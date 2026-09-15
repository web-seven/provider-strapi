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
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupsv2alpha1 "github.com/web-seven/provider-strapi/apis/backups/v2alpha1"
	s3client "github.com/web-seven/provider-strapi/internal/clients/s3"
	strapiclient "github.com/web-seven/provider-strapi/internal/clients/strapi"
)

// fakeTransfer writes objects to the sink the way the real client streams a
// dump: create, write, close.
type fakeTransfer struct {
	manifest strapiclient.DumpManifest
	objects  map[string]string
	// partial is a key Dump opens and writes to but never closes, as when a
	// dump fails midway.
	partial string
	err     error
	calls   int
}

func (f *fakeTransfer) Dump(ctx context.Context, sink strapiclient.DumpSink, _ bool) (strapiclient.DumpManifest, error) {
	f.calls++
	for key, body := range f.objects {
		w, err := sink.Create(ctx, key)
		if err != nil {
			return f.manifest, err
		}
		if _, err := io.WriteString(w, body); err != nil {
			return f.manifest, err
		}
		if err := w.Close(); err != nil {
			return f.manifest, err
		}
	}
	if f.partial != "" {
		w, err := sink.Create(ctx, f.partial)
		if err != nil {
			return f.manifest, err
		}
		_, _ = io.WriteString(w, "partial")
	}
	return f.manifest, f.err
}

type fakeBucket struct {
	mu        sync.Mutex
	putErr    error
	puts      map[string]string
	abandoned []string
	objects   []s3client.Object
	listErr   error
	deleted   []string
	deleteErr error
}

func newFakeBucket() *fakeBucket {
	return &fakeBucket{puts: map[string]string{}}
}

func (f *fakeBucket) Upload(_ context.Context, key string, r io.Reader) (int64, error) {
	if f.putErr != nil {
		return 0, f.putErr
	}
	b, err := io.ReadAll(r)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil {
		f.abandoned = append(f.abandoned, key)
		return 0, err
	}
	f.puts[key] = string(b)
	return int64(len(b)), nil
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

func TestUpdate_StreamsBackupToBucket(t *testing.T) {
	ft := &fakeTransfer{
		objects: map[string]string{
			"entities.jsonl.gz":  "entities",
			"assets/1_image.png": "image",
		},
		manifest: strapiclient.DumpManifest{
			Entities: strapiclient.DumpFile{Key: "entities.jsonl.gz", Size: 8, Count: 3},
			Assets:   []strapiclient.DumpFile{{Key: "assets/1_image.png", Size: 5}},
		},
	}
	fb := newFakeBucket()
	e := &external{transfer: ft, bucket: fb}
	cr := newCR("0 2 * * *")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if ft.calls != 1 {
		t.Fatalf("expected Dump to be called once, got %d", ft.calls)
	}
	at := cr.Status.AtProvider
	if at.LastBackupTime == nil || at.LastBackupKey == "" {
		t.Fatalf("expected LastBackupTime and LastBackupKey to be set, got %+v", at)
	}
	if at.LastBackupEntities != 3 || at.LastBackupAssets != 1 {
		t.Fatalf("expected 3 entities and 1 asset, got %+v", at)
	}
	for key, want := range ft.objects {
		if got := fb.puts[at.LastBackupKey+"/"+key]; got != want {
			t.Fatalf("object %q = %q, want %q (puts=%v)", key, got, want, fb.puts)
		}
	}
	manifest, ok := fb.puts[at.LastBackupKey+"/manifest.json"]
	if !ok {
		t.Fatalf("expected manifest.json to be uploaded under %q, got %v", at.LastBackupKey, fb.puts)
	}
	if want := int64(8 + 5 + len(manifest)); at.LastBackupSizeBytes != want {
		t.Fatalf("LastBackupSizeBytes = %d, want %d", at.LastBackupSizeBytes, want)
	}
}

func TestUpdate_DumpErrorAbandonsPartialObjects(t *testing.T) {
	ft := &fakeTransfer{
		objects: map[string]string{"entities.jsonl.gz": "entities"},
		partial: "links.jsonl.gz",
		err:     errors.New("boom"),
	}
	fb := newFakeBucket()
	e := &external{transfer: ft, bucket: fb}
	cr := newCR("0 2 * * *")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("expected error to propagate from a failed dump")
	}
	if len(fb.abandoned) != 1 || fb.abandoned[0] == "" {
		t.Fatalf("expected the partial object's upload to be abandoned, got %v", fb.abandoned)
	}
	for key := range fb.puts {
		if key == fb.abandoned[0] {
			t.Fatalf("partial object %q must not be stored", key)
		}
	}
	if cr.Status.AtProvider.LastBackupTime != nil {
		t.Fatal("a failed backup must not be recorded as successful")
	}
}

func TestUpdate_UploadErrorFailsBackup(t *testing.T) {
	ft := &fakeTransfer{objects: map[string]string{"entities.jsonl.gz": "entities"}}
	fb := newFakeBucket()
	fb.putErr = errors.New("bucket unavailable")
	e := &external{transfer: ft, bucket: fb}
	cr := newCR("0 2 * * *")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("expected upload error to fail the backup")
	}
	if cr.Status.AtProvider.LastBackupTime != nil {
		t.Fatal("a failed backup must not be recorded as successful")
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

type fakeAdmin struct {
	tokens      []strapiclient.TransferToken
	created     int
	regenerated int
	deleted     int
}

func (f *fakeAdmin) ListTransferTokens(_ context.Context) ([]strapiclient.TransferToken, error) {
	return f.tokens, nil
}

func (f *fakeAdmin) CreateTransferToken(_ context.Context, name, _ string, permissions []string) (strapiclient.TransferToken, error) {
	f.created++
	tok := strapiclient.TransferToken{ID: len(f.tokens) + 1, Name: name, Permissions: permissions}
	f.tokens = append(f.tokens, tok)
	tok.AccessKey = "created-key"
	return tok, nil
}

func (f *fakeAdmin) RegenerateTransferToken(_ context.Context, _ int) (string, error) {
	f.regenerated++
	return "regenerated-key", nil
}

func (f *fakeAdmin) DeleteTransferToken(_ context.Context, id int) error {
	f.deleted++
	for i, t := range f.tokens {
		if t.ID == id {
			f.tokens = append(f.tokens[:i], f.tokens[i+1:]...)
			return nil
		}
	}
	return errors.New("not found")
}

func newKube(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func tokenSecret(t *testing.T, kube client.Client, cr *backupsv2alpha1.Backup) (*corev1.Secret, error) {
	t.Helper()
	s := &corev1.Secret{}
	err := kube.Get(context.Background(), types.NamespacedName{Namespace: cr.GetNamespace(), Name: transferTokenSecretName(cr)}, s)
	return s, err
}

func TestEnsureTransferToken_ProvisionsTokenAndSecret(t *testing.T) {
	kube := newKube(t)
	admin := &fakeAdmin{}
	cr := newCR("0 2 * * *")

	token, err := ensureTransferToken(context.Background(), kube, admin, cr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "created-key" || admin.created != 1 {
		t.Fatalf("expected a token to be created, got token=%q created=%d", token, admin.created)
	}
	if got := admin.tokens[0]; got.Name != transferTokenName(cr) || len(got.Permissions) != 1 || got.Permissions[0] != strapiclient.TransferTokenPermissionPull {
		t.Fatalf("unexpected token: %+v", got)
	}
	s, err := tokenSecret(t, kube, cr)
	if err != nil {
		t.Fatal(err)
	}
	if string(s.Data[transferTokenSecretKey]) != "created-key" {
		t.Fatalf("expected token stored in secret, got %v", s.Data)
	}
	if refs := s.GetOwnerReferences(); len(refs) != 1 || refs[0].Kind != backupsv2alpha1.BackupKind || refs[0].Name != cr.GetName() {
		t.Fatalf("expected secret to be owned by the Backup, got %v", refs)
	}
}

func TestEnsureTransferToken_ReusesStoredToken(t *testing.T) {
	cr := newCR("0 2 * * *")
	kube := newKube(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: cr.GetNamespace(), Name: transferTokenSecretName(cr)},
		Data:       map[string][]byte{transferTokenSecretKey: []byte("stored-key")},
	})
	admin := &fakeAdmin{}

	token, err := ensureTransferToken(context.Background(), kube, admin, cr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "stored-key" || admin.created != 0 || admin.regenerated != 0 {
		t.Fatalf("expected stored token to be reused, got token=%q created=%d regenerated=%d", token, admin.created, admin.regenerated)
	}
}

func TestEnsureTransferToken_RegeneratesOrphanedToken(t *testing.T) {
	cr := newCR("0 2 * * *")
	kube := newKube(t)
	admin := &fakeAdmin{tokens: []strapiclient.TransferToken{{ID: 7, Name: transferTokenName(cr)}}}

	token, err := ensureTransferToken(context.Background(), kube, admin, cr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "regenerated-key" || admin.regenerated != 1 || admin.created != 0 {
		t.Fatalf("expected existing token to be regenerated, got token=%q created=%d regenerated=%d", token, admin.created, admin.regenerated)
	}
	s, err := tokenSecret(t, kube, cr)
	if err != nil {
		t.Fatal(err)
	}
	if string(s.Data[transferTokenSecretKey]) != "regenerated-key" {
		t.Fatalf("expected regenerated token stored in secret, got %v", s.Data)
	}
}

func TestUpdate_RejectedTokenDropsSecret(t *testing.T) {
	cr := newCR("0 2 * * *")
	kube := newKube(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: cr.GetNamespace(), Name: transferTokenSecretName(cr)},
		Data:       map[string][]byte{transferTokenSecretKey: []byte("revoked-key")},
	})
	ft := &fakeTransfer{err: fmt.Errorf("connect: %w", strapiclient.ErrTransferUnauthorized)}
	e := &external{kube: kube, admin: &fakeAdmin{}, transfer: ft, bucket: newFakeBucket()}

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("expected the dump error to propagate")
	}
	if _, err := tokenSecret(t, kube, cr); !kerrors.IsNotFound(err) {
		t.Fatalf("expected rejected token secret to be deleted, got err=%v", err)
	}
}

func TestDeletion_RemovesProvisionedToken(t *testing.T) {
	cr := newCR("0 2 * * *")
	meta.SetExternalName(cr, cr.GetName())
	now := metav1.Now()
	cr.SetDeletionTimestamp(&now)
	admin := &fakeAdmin{tokens: []strapiclient.TransferToken{{ID: 1, Name: transferTokenName(cr)}}}
	e := &external{kube: newKube(t), admin: admin}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.ResourceExists {
		t.Fatal("expected deletion to wait for the provisioned token to be removed")
	}
	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if admin.deleted != 1 {
		t.Fatalf("expected the token to be deleted, got %d deletions", admin.deleted)
	}
	obs, err = e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if obs.ResourceExists {
		t.Fatal("expected deletion to complete once the token is gone")
	}
}

func TestDeletion_UserSuppliedTokenCompletesImmediately(t *testing.T) {
	cr := newCR("0 2 * * *")
	meta.SetExternalName(cr, cr.GetName())
	now := metav1.Now()
	cr.SetDeletionTimestamp(&now)
	e := &external{transfer: &fakeTransfer{}, bucket: newFakeBucket()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if obs.ResourceExists {
		t.Fatal("expected a Backup without a provisioned token to have nothing to clean up")
	}
}

func TestProviderConfigRef_DefaultsToClusterProviderConfig(t *testing.T) {
	cr := newCR("0 2 * * *")
	name, kind := providerConfigRef(cr)
	if name != "default" || kind != "ClusterProviderConfig" {
		t.Fatalf("got (%q, %q), want (\"default\", \"ClusterProviderConfig\")", name, kind)
	}
}
