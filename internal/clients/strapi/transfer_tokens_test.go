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
	"strconv"
	"sync"
	"testing"
)

// fakeTransferTokens simulates Strapi's /admin/transfer/tokens API, including
// its unique-name constraint and access keys only being returned on create
// and regenerate.
type fakeTransferTokens struct {
	mu     sync.Mutex
	tokens map[int]TransferToken
	nextID int
	keySeq int
}

func newTransferTokenClient(t *testing.T) (*Client, *fakeTransferTokens) {
	t.Helper()
	f := &fakeTransferTokens{tokens: map[int]TransferToken{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/login", func(w http.ResponseWriter, _ *http.Request) {
		encodeData(w, map[string]any{"token": "jwt"})
	})
	mux.HandleFunc("GET /admin/transfer/tokens", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		list := []TransferToken{}
		for _, tok := range f.tokens {
			tok.AccessKey = ""
			list = append(list, tok)
		}
		encodeData(w, list)
	})
	mux.HandleFunc("POST /admin/transfer/tokens", func(w http.ResponseWriter, r *http.Request) {
		var body TransferToken
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, tok := range f.tokens {
			if tok.Name == body.Name {
				http.Error(w, `{"error":{"message":"Name already taken"}}`, http.StatusBadRequest)
				return
			}
		}
		f.nextID++
		body.ID = f.nextID
		body.AccessKey = f.newKey()
		f.tokens[body.ID] = body
		encodeData(w, body)
	})
	mux.HandleFunc("POST /admin/transfer/tokens/{id}/regenerate", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id, _ := strconv.Atoi(r.PathValue("id"))
		tok, ok := f.tokens[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		tok.AccessKey = f.newKey()
		f.tokens[id] = tok
		encodeData(w, map[string]any{"id": id, "accessKey": tok.AccessKey})
	})
	mux.HandleFunc("DELETE /admin/transfer/tokens/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id, _ := strconv.Atoi(r.PathValue("id"))
		if _, ok := f.tokens[id]; !ok {
			http.NotFound(w, r)
			return
		}
		delete(f.tokens, id)
		encodeData(w, map[string]any{"id": id})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c, err := New(Config{Endpoint: srv.URL, Credentials: Credentials{Email: "admin@example.com", Password: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	return c, f
}

func (f *fakeTransferTokens) newKey() string {
	f.keySeq++
	return "key-" + strconv.Itoa(f.keySeq)
}

func encodeData(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func TestTransferTokenLifecycle(t *testing.T) {
	ctx := context.Background()
	c, f := newTransferTokenClient(t)

	created, err := c.CreateTransferToken(ctx, "backup", "desc", []string{TransferTokenPermissionPull})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == 0 || created.AccessKey == "" {
		t.Fatalf("expected ID and access key, got %+v", created)
	}
	if got := f.tokens[created.ID].Permissions; len(got) != 1 || got[0] != TransferTokenPermissionPull {
		t.Fatalf("expected pull permission to be sent, got %v", got)
	}

	if _, err := c.CreateTransferToken(ctx, "backup", "desc", []string{TransferTokenPermissionPull}); err == nil {
		t.Fatal("expected duplicate name to fail")
	}

	list, err := c.ListTransferTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "backup" || list[0].AccessKey != "" {
		t.Fatalf("unexpected listing: %+v", list)
	}

	key, err := c.RegenerateTransferToken(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if key == "" || key == created.AccessKey {
		t.Fatalf("expected a new access key, got %q", key)
	}

	if err := c.DeleteTransferToken(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if len(f.tokens) != 0 {
		t.Fatalf("expected token to be deleted, got %v", f.tokens)
	}
	if err := c.DeleteTransferToken(ctx, created.ID); err == nil {
		t.Fatal("expected deleting a missing token to fail")
	}
}
