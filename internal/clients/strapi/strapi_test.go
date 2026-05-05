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
	"sync/atomic"
	"testing"
)

func TestParseCredentials(t *testing.T) {
	cases := map[string]struct {
		in      string
		wantErr bool
	}{
		"valid":           {`{"email":"a@b","password":"x"}`, false},
		"missing email":   {`{"password":"x"}`, true},
		"missing pwd":     {`{"email":"a@b"}`, true},
		"empty email":     {`{"email":"","password":"x"}`, true},
		"not json":        {`hello`, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseCredentials([]byte(tc.in))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// fakeStrapi spins up a test server simulating the small surface we use:
// POST /admin/login returns a token; other paths require Bearer auth and
// either return 200 or a configurable 401 (for testing JWT rotation).
type fakeStrapi struct {
	server     *httptest.Server
	loginCalls atomic.Int32
	calls      atomic.Int32
	// expireAfter: respond 401 on the first N authed requests after each login.
	// Used to exercise the 401 → re-login → retry path.
	expireAfter atomic.Int32
	tokenSeq    atomic.Int32
}

func newFakeStrapi(t *testing.T) *fakeStrapi {
	t.Helper()
	f := &fakeStrapi{}
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/login", func(w http.ResponseWriter, r *http.Request) {
		f.loginCalls.Add(1)
		var body Credentials
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if body.Email != "admin@example.com" || body.Password != "secret" {
			http.Error(w, "nope", http.StatusUnauthorized)
			return
		}
		// Reset the per-token expiry counter when a new token is minted.
		f.expireAfter.Store(0)
		seq := f.tokenSeq.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"token": "tok-" + itoa(seq)},
		})
	})
	mux.HandleFunc("/admin/users/me", func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		if n := f.expireAfter.Load(); n > 0 {
			f.expireAfter.Add(-1)
			http.Error(w, "expired", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 1}})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func itoa(n int32) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 8)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

func newClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	c, err := New(Config{
		Endpoint:    endpoint,
		Credentials: Credentials{Email: "admin@example.com", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClient_LazyLoginAndCache(t *testing.T) {
	f := newFakeStrapi(t)
	c := newClient(t, f.server.URL)

	for range 3 {
		resp, err := c.Do(context.Background(), http.MethodGet, "/admin/users/me", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", resp.StatusCode)
		}
	}
	if got := f.loginCalls.Load(); got != 1 {
		t.Fatalf("expected 1 login, got %d", got)
	}
	if got := f.calls.Load(); got != 3 {
		t.Fatalf("expected 3 authed calls, got %d", got)
	}
}

func TestClient_RotateOn401(t *testing.T) {
	f := newFakeStrapi(t)
	c := newClient(t, f.server.URL)

	// First call: succeeds, caches token.
	resp, err := c.Do(context.Background(), http.MethodGet, "/admin/users/me", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Force the server to return 401 once: simulates JWT expiry.
	f.expireAfter.Store(1)

	resp, err = c.Do(context.Background(), http.MethodGet, "/admin/users/me", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected eventual 200, got %d", resp.StatusCode)
	}
	if got := f.loginCalls.Load(); got != 2 {
		t.Fatalf("expected 2 logins (initial + rotation), got %d", got)
	}
}

func TestClient_LoginFailureSurfaced(t *testing.T) {
	f := newFakeStrapi(t)
	c, err := New(Config{
		Endpoint:    f.server.URL,
		Credentials: Credentials{Email: "admin@example.com", Password: "wrong"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(context.Background(), http.MethodGet, "/admin/users/me", nil)
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected error from bad credentials")
	}
}

func TestClient_DoJSON_DecodeAndError(t *testing.T) {
	f := newFakeStrapi(t)
	c := newClient(t, f.server.URL)

	var out struct {
		Data struct {
			ID int `json:"id"`
		} `json:"data"`
	}
	if err := c.DoJSON(context.Background(), http.MethodGet, "/admin/users/me", nil, &out); err != nil {
		t.Fatal(err)
	}
	if out.Data.ID != 1 {
		t.Fatalf("got id=%d", out.Data.ID)
	}

	// Non-existent path → 404 → DoJSON returns error.
	if err := c.DoJSON(context.Background(), http.MethodGet, "/admin/nope", nil, nil); err == nil {
		t.Fatal("expected error on 404")
	}
}

func TestNew_Validation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := New(Config{Endpoint: "http://x"}); err == nil {
		t.Fatal("expected error for missing creds")
	}
}
