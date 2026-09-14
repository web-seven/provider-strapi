/*
Copyright 2025 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backups

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupsv2alpha1 "github.com/web-seven/provider-strapi/apis/backups/v2alpha1"
	s3client "github.com/web-seven/provider-strapi/internal/clients/s3"
	strapiclient "github.com/web-seven/provider-strapi/internal/clients/strapi"
)

type fakeTransfer struct {
	manifest strapiclient.DumpManifest
	err      error
	calls    int
}

func (f *fakeTransfer) Dump(_ context.Context, _ string, _ bool) (strapiclient.DumpManifest, error) {
	f.calls++
	return f.manifest, f.err
}

type fakeBucket struct {
	putErr    error
	puts      map[string]int64
	objects   []s3client.Object
	listErr   error
	deleted   []string
	deleteErr error
}

func newFakeBucket() *fakeBucket {
	return &fakeBucket{puts: map[string]int64{}}
}

func (f *fakeBucket) PutFile(_ context.Context, key, _ string) (int64, error) {
	if f.putErr != nil {
		return 0, f.putErr
	}
	const size = 10
	f.puts[key] = size
	return size, nil
}

func (f *fakeBucket) ListObjects(_ context.Context, _ string) ([]s3client.Object, error) {
	return f.objects, f.listErr
}

func (f *fakeBucket) DeleteObjects(_ context.Context, keys []string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, keys...)
	return nil
}

func newCR(schedule string) *backupsv2alpha1.Backup {
	return &backupsv2alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: backupsv2alpha1.BackupSpec{
			ForProvider: backupsv2alpha1.BackupParameters{
				Schedule: schedule,
				Destination: backupsv2alpha1.BackupDestination{
					Bucket: "backups",
					Path:   "prod",
				},
			},
		},
	}
}

func TestObserve_NoExternalName(t *testing.T) {
	e := &external{transfer: &fakeTransfer{}, bucket: newFakeBucket()}
	cr := newCR("0 2 * * *")

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if obs.ResourceExists {
		t.Fatal("expected ResourceExists=false before Create has run")
	}
}

func TestObserve_InvalidSchedule(t *testing.T) {
	e := &external{transfer: &fakeTransfer{}, bucket: newFakeBucket()}
	cr := newCR("not a schedule")
	meta.SetExternalName(cr, cr.GetName())

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected error for invalid schedule")
	}
}

func TestObserve_DueWhenNeverRun(t *testing.T) {
	e := &external{transfer: &fakeTransfer{}, bucket: newFakeBucket()}
	cr := newCR("* * * * *") // due every minute
	meta.SetExternalName(cr, cr.GetName())
	cr.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Minute))

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.ResourceExists {
		t.Fatal("expected ResourceExists=true")
	}
	if obs.ResourceUpToDate {
		t.Fatal("expected a backup to be due")
	}
	if cr.Status.AtProvider.NextBackupTime == nil {
		t.Fatal("expected NextBackupTime to be populated")
	}
}

func TestObserve_NotDueAfterRecentBackup(t *testing.T) {
	e := &external{transfer: &fakeTransfer{}, bucket: newFakeBucket()}
	cr := newCR("0 2 * * *") // daily at 02:00
	meta.SetExternalName(cr, cr.GetName())
	cr.Status.AtProvider.LastBackupTime = &metav1.Time{Time: time.Now()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.ResourceUpToDate {
		t.Fatal("expected no backup due right after the last one completed")
	}
}

func TestCreate_SetsExternalName(t *testing.T) {
	e := &external{transfer: &fakeTransfer{}, bucket: newFakeBucket()}
	cr := newCR("0 2 * * *")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if meta.GetExternalName(cr) != cr.GetName() {
		t.Fatalf("expected external-name=%q, got %q", cr.GetName(), meta.GetExternalName(cr))
	}
}

func TestUpdate_PerformsBackupAndRecordsStatus(t *testing.T) {
	ft := &fakeTransfer{manifest: strapiclient.DumpManifest{
		Entities: strapiclient.DumpFile{Path: "", Key: "entities.jsonl.gz", Count: 3},
	}}
	fb := newFakeBucket()
	e := &external{transfer: ft, bucket: fb}
	cr := newCR("0 2 * * *")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if ft.calls != 1 {
		t.Fatalf("expected Dump to be called once, got %d", ft.calls)
	}
	if cr.Status.AtProvider.LastBackupTime == nil {
		t.Fatal("expected LastBackupTime to be set")
	}
	if cr.Status.AtProvider.LastBackupKey == "" {
		t.Fatal("expected LastBackupKey to be set")
	}
	if cr.Status.AtProvider.LastBackupEntities != 3 {
		t.Fatalf("expected LastBackupEntities=3, got %d", cr.Status.AtProvider.LastBackupEntities)
	}
	// Only the manifest is uploaded: the dump files in this test have empty
	// Path (as if produced by a fake with no local staging), which upload
	// skips.
	if _, ok := fb.puts[cr.Status.AtProvider.LastBackupKey+"/manifest.json"]; !ok {
		t.Fatalf("expected manifest.json to be uploaded under %q, got %v", cr.Status.AtProvider.LastBackupKey, fb.puts)
	}
}

func TestUpdate_DumpError(t *testing.T) {
	ft := &fakeTransfer{err: errors.New("boom")}
	e := &external{transfer: ft, bucket: newFakeBucket()}
	cr := newCR("0 2 * * *")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("expected error to propagate from a failed dump")
	}
}

func TestUpdate_AppliesRetentionWhenConfigured(t *testing.T) {
	fb := newFakeBucket()
	old := time.Now().Add(-60 * 24 * time.Hour)
	fb.objects = []s3client.Object{
		{Key: "prod/old-backup/manifest.json", LastModified: old},
		{Key: "prod/old-backup/entities.jsonl.gz", LastModified: old},
	}
	e := &external{transfer: &fakeTransfer{}, bucket: fb}
	cr := newCR("0 2 * * *")
	cr.Spec.ForProvider.RetentionDays = 30

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if len(fb.deleted) != 2 {
		t.Fatalf("expected the stale backup's 2 objects to be deleted, got %v", fb.deleted)
	}
}

func TestUpdate_NoRetentionByDefault(t *testing.T) {
	fb := newFakeBucket()
	fb.objects = []s3client.Object{
		{Key: "prod/old-backup/manifest.json", LastModified: time.Now().Add(-365 * 24 * time.Hour)},
	}
	e := &external{transfer: &fakeTransfer{}, bucket: fb}
	cr := newCR("0 2 * * *")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if len(fb.deleted) != 0 {
		t.Fatalf("expected no deletions when retentionDays is unset, got %v", fb.deleted)
	}
}

func TestDelete_DoesNotRemoveBackups(t *testing.T) {
	fb := newFakeBucket()
	e := &external{transfer: &fakeTransfer{}, bucket: fb}
	cr := newCR("0 2 * * *")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if len(fb.deleted) != 0 {
		t.Fatal("Delete must not remove backups already written to the bucket")
	}
}

func TestBackupPrefix(t *testing.T) {
	at := time.Date(2026, 9, 14, 2, 0, 0, 0, time.UTC)
	cases := []struct {
		path string
		want string
	}{
		{"", "20260914T020000Z"},
		{"production", "production/20260914T020000Z"},
		{"/production/", "production/20260914T020000Z"},
	}
	for _, c := range cases {
		if got := backupPrefix(c.path, at); got != c.want {
			t.Errorf("backupPrefix(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

func TestProviderConfigRef_DefaultsToClusterProviderConfig(t *testing.T) {
	cr := newCR("0 2 * * *")
	name, kind := providerConfigRef(cr)
	if name != "default" || kind != "ClusterProviderConfig" {
		t.Fatalf("got (%q, %q), want (\"default\", \"ClusterProviderConfig\")", name, kind)
	}
}
