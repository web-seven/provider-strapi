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

func TestObserve_NonNumericExternalNameFallsThroughToSelector(t *testing.T) {
	// crossplane-runtime's default NameAsExternalName initializer writes the
	// CR's metadata.name as external-name on first reconcile. For
	// RolePermissions that means "public" / "authenticated", which Atoi
	// cannot parse. The previous implementation errored out; we now ignore
	// non-numeric external-names and fall through to selector resolution.
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{"api::article.article.find"})
	meta.SetExternalName(cr, "public") // simulates the initializer

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !obs.ResourceExists {
		t.Fatal("expected ResourceExists=true")
	}
	if got := meta.GetExternalName(cr); got != "2" {
		t.Fatalf("Observe should have overwritten external-name with the role ID; got %q", got)
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

func TestObserve_DeletionLifecycle(t *testing.T) {
	// During deletion the "external resource" is the configured permission
	// set, not the role itself. Observe must keep reporting ResourceExists=true
	// until Delete has actually run (and recorded the Cleared flag in status),
	// so the reconciler invokes Delete at least once. After Delete records
	// success, Observe returns false so the finalizer can drain.
	now := metav1.Now()

	t.Run("Cleared flag unset → exists=true so Delete runs", func(t *testing.T) {
		fc := &fakeClient{roles: builtInRoles()}
		e := &external{client: fc}

		cr := newCR("public", nil)
		cr.SetDeletionTimestamp(&now)

		obs, err := e.Observe(context.Background(), cr)
		if err != nil {
			t.Fatal(err)
		}
		if !obs.ResourceExists {
			t.Fatal("expected ResourceExists=true on first deletion reconcile (Delete must still run)")
		}
	})

	t.Run("Cleared flag unset and role already empty → still exists=true", func(t *testing.T) {
		// Important: an empty live permission set must NOT short-circuit
		// the deletion path on its own — Delete is what writes the flag,
		// and the user wants Delete to be invoked unconditionally.
		roles := builtInRoles()
		roles[1].Permissions = nil
		fc := &fakeClient{roles: roles}
		e := &external{client: fc}

		cr := newCR("public", nil)
		cr.SetDeletionTimestamp(&now)

		obs, err := e.Observe(context.Background(), cr)
		if err != nil {
			t.Fatal(err)
		}
		if !obs.ResourceExists {
			t.Fatal("expected ResourceExists=true while Cleared flag is unset")
		}
	})

	t.Run("Cleared flag set → exists=false drains finalizer", func(t *testing.T) {
		fc := &fakeClient{roles: builtInRoles()}
		e := &external{client: fc}

		cr := newCR("public", nil)
		cr.SetDeletionTimestamp(&now)
		cr.Status.AtProvider.Cleared = true

		obs, err := e.Observe(context.Background(), cr)
		if err != nil {
			t.Fatal(err)
		}
		if obs.ResourceExists {
			t.Fatal("expected ResourceExists=false once Delete has recorded the Cleared flag")
		}
	})
}

func TestDelete_SetsClearedFlag(t *testing.T) {
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{"api::article.article.find"})
	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if !cr.Status.AtProvider.Cleared {
		t.Fatal("Delete must record Cleared=true so the next Observe drains the finalizer")
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

func TestDelete_ClearsPermissions(t *testing.T) {
	// Delete clears the role's managed permissions in Strapi (PUT empty
	// tree). It does NOT delete the role itself — this MR doesn't manage
	// role lifecycle, and built-in roles can't be deleted anyway.
	//
	// The Crossplane reconciler only invokes Delete when the user's
	// managementPolicies include the Delete action. Skipping Delete on
	// MR removal is achieved at that level (e.g. by setting policies to
	// ["Observe","Create","Update"]), not in this code path.
	fc := &fakeClient{roles: builtInRoles()}
	e := &external{client: fc}

	cr := newCR("public", []string{"api::article.article.find"})
	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if fc.updated == nil {
		t.Fatal("Delete should call UpdateRole to clear permissions")
	}
	if got := strapiclient.FlattenPermissions(fc.updated.Permissions); len(got) != 0 {
		t.Fatalf("expected empty permissions, got %v", got)
	}
}
