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

package strapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestExpandThenFlatten(t *testing.T) {
	in := []string{
		"api::article.article.find",
		"api::article.article.findOne",
		"plugin::users-permissions.auth.callback",
	}
	got := FlattenPermissions(ExpandPermissions(in))
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("roundtrip mismatch:\n want %v\n got  %v", in, got)
	}
}

func TestFlattenSkipsDisabled(t *testing.T) {
	tree := PermissionsByResource{
		"api::article.article": {
			Controllers: map[string]map[string]Action{
				"article": {
					"find":   {Enabled: true},
					"create": {Enabled: false},
				},
			},
		},
	}
	got := FlattenPermissions(tree)
	want := []string{"api::article.article.find"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExpandIgnoresMalformed(t *testing.T) {
	tree := ExpandPermissions([]string{
		"api::article.article.find",
		"too.short",
		"oneword",
		"",
		".empty.controller",
	})
	if _, ok := tree["api::article.article"]; !ok {
		t.Fatal("valid entry missing")
	}
	if len(tree) != 1 {
		t.Fatalf("expected 1 resource, got %d: %v", len(tree), tree)
	}
}

func TestFindRole(t *testing.T) {
	roles := []Role{
		{ID: 1, Name: "Authenticated", Type: "authenticated"},
		{ID: 2, Name: "Public", Type: "public"},
		{ID: 3, Name: "Editor", Type: "editor"},
	}
	cases := map[string]int{
		"public":        2,
		"PUBLIC":        2,
		"authenticated": 1,
		"editor":        3,
		"Editor":        3, // matches by name (case-insensitive)
	}
	for sel, wantID := range cases {
		t.Run(sel, func(t *testing.T) {
			r, ok := FindRole(roles, sel)
			if !ok {
				t.Fatalf("not found")
			}
			if r.ID != wantID {
				t.Fatalf("got id=%d, want %d", r.ID, wantID)
			}
		})
	}
	if _, ok := FindRole(roles, "doesNotExist"); ok {
		t.Fatal("expected miss")
	}
}

// rolesServer mounts /admin/login + /users-permissions/* on a test server.
// Captures the most recent PUT body for assertion.
type rolesServer struct {
	server  *httptest.Server
	lastPUT struct {
		ID   int
		Body map[string]any
	}
}

func newRolesServer(t *testing.T) *rolesServer {
	t.Helper()
	rs := &rolesServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/login", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"token": "tok"},
		})
	})
	mux.HandleFunc("/users-permissions/roles", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"roles": []Role{
				{ID: 1, Name: "Authenticated", Type: "authenticated"},
				{ID: 2, Name: "Public", Type: "public", Permissions: PermissionsByResource{
					"api::article.article": {Controllers: map[string]map[string]Action{
						"article": {"find": {Enabled: true}},
					}},
				}},
			},
		})
	})
	mux.HandleFunc("/users-permissions/roles/2", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		rs.lastPUT.ID = 2
		rs.lastPUT.Body = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	rs.server = httptest.NewServer(mux)
	t.Cleanup(rs.server.Close)
	return rs
}

func TestClient_ListAndUpdateRole(t *testing.T) {
	srv := newRolesServer(t)
	c, err := New(Config{
		Endpoint:    srv.server.URL,
		Credentials: Credentials{Email: "admin@example.com", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}

	roles, err := c.ListRoles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(roles) != 2 {
		t.Fatalf("got %d roles", len(roles))
	}

	pub, ok := FindRole(roles, "public")
	if !ok {
		t.Fatal("public role missing")
	}
	pub.Permissions = ExpandPermissions([]string{"api::article.article.findOne"})
	if err := c.UpdateRole(context.Background(), pub.ID, pub); err != nil {
		t.Fatal(err)
	}
	if srv.lastPUT.ID != 2 {
		t.Fatalf("PUT hit role %d", srv.lastPUT.ID)
	}
	if _, ok := srv.lastPUT.Body["permissions"]; !ok {
		t.Fatal("PUT body missing permissions")
	}
}
