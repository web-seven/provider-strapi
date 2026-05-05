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

package rolepermissions

import (
	"context"
	"reflect"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	permv1alpha1 "github.com/web-seven/provider-strapi/apis/permissions/v1alpha1"
	strapiclient "github.com/web-seven/provider-strapi/internal/clients/strapi"
)

type fakeClient struct {
	roles      []strapiclient.Role
	updated    *strapiclient.Role
	listErr    error
	updateErr  error
	listCalls  int
	updateCals int
}

func (f *fakeClient) ListRoles(_ context.Context) ([]strapiclient.Role, error) {
	f.listCalls++
	return f.roles, f.listErr
}

func (f *fakeClient) UpdateRole(_ context.Context, id int, role strapiclient.Role) error {
	f.updateCals++
	if f.updateErr != nil {
		return f.updateErr
	}
	role.ID = id
	f.updated = &role
	// Reflect the update back into the in-memory state so subsequent
	// observes see the new permissions.
	for i := range f.roles {
		if f.roles[i].ID == id {
			f.roles[i].Permissions = role.Permissions
			return nil
		}
	}
	return nil
}

func newCR(role string, perms []string) *permv1alpha1.RolePermissions {
	return &permv1alpha1.RolePermissions{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
		Spec: permv1alpha1.RolePermissionsSpec{
			ForProvider: permv1alpha1.RolePermissionsParameters{
				Role:        role,
				Permissions: perms,
			},
		},
	}
}

func builtInRoles() []strapiclient.Role {
	return []strapiclient.Role{
		{ID: 1, Name: "Authenticated", Type: "authenticated"},
		{
			ID: 2, Name: "Public", Type: "public",
			Permissions: strapiclient.ExpandPermissions([]string{
				"api::article.article.find",
			}),
		},
	}
}

func TestObserve_DriftDetected(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{
		"api::article.article.find",
		"api::article.article.findOne",
	})

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.ResourceExists {
		t.Fatal("expected ResourceExists=true")
	}
	if obs.ResourceUpToDate {
		t.Fatal("expected drift (spec has 2 perms, role has 1)")
	}
	if got := meta.GetExternalName(cr); got != "2" {
		t.Fatalf("external-name=%q, want 2", got)
	}
	if cr.Status.AtProvider.RoleID != 2 || cr.Status.AtProvider.Type != "public" {
		t.Fatalf("status not populated: %+v", cr.Status.AtProvider)
	}
}

func TestObserve_InSync(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{"api::article.article.find"})
	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.ResourceUpToDate {
		t.Fatalf("expected up-to-date, status=%+v", cr.Status.AtProvider)
	}
}

func TestObserve_RoleNotFound(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("nonexistent", nil)
	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected error for unknown role")
	}
}

func TestObserve_DeletedShortCircuits(t *testing.T) {
	fc := &fakeClient{}
	e := &external{client: fc}

	now := metav1.Now()
	cr := newCR("public", nil)
	cr.SetDeletionTimestamp(&now)

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if obs.ResourceExists {
		t.Fatal("expected ResourceExists=false on deleted CR")
	}
	if fc.listCalls != 0 {
		t.Fatal("expected no list call when deleted")
	}
}

func TestUpdate_PutsPermissions(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{
		"api::article.article.find",
		"api::article.article.findOne",
	})

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if fc.updated == nil {
		t.Fatal("UpdateRole was not called")
	}
	got := strapiclient.FlattenPermissions(fc.updated.Permissions)
	want := []string{
		"api::article.article.find",
		"api::article.article.findOne",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PUT permissions:\n got  %v\n want %v", got, want)
	}
}

func TestCreate_BehavesAsUpdate(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{"api::article.article.findOne"})
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if fc.updateCals != 1 {
		t.Fatalf("expected 1 update call, got %d", fc.updateCals)
	}
}

func TestDelete_NoOp(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", nil)
	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if fc.updateCals != 0 {
		t.Fatal("Delete should not call UpdateRole")
	}
}
