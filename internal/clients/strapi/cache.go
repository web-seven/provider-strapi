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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// Cache memoizes Client instances keyed by their effective config. Without
// it, every managed-resource reconcile triggers a fresh /admin/login call
// — multiple MRs sharing one ProviderConfig hammer Strapi's login rate
// limiter (HTTP 429). With the cache, a single Client per (endpoint,
// credentials, TLS-skip) tuple is reused across reconciles, and the
// Client's internal JWT cache (refreshed reactively on 401) keeps logins
// to one per provider lifetime under steady state.
//
// Stale Clients linger in the map after credentials change. For the
// expected scale (one or a few ProviderConfigs per cluster) this is fine;
// add eviction if it ever becomes a problem.
type Cache struct {
	mu      sync.Mutex
	clients map[string]*Client
}

// NewCache returns an empty Cache.
func NewCache() *Cache {
	return &Cache{clients: map[string]*Client{}}
}

// Get returns a cached Client for cfg, or constructs and caches a new one
// if none exists. The mutex is held for the duration of construction, so
// concurrent Get calls with the same cfg only build one Client.
func (c *Cache) Get(cfg Config) (*Client, error) {
	key := cacheKey(cfg)
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[key]; ok {
		return cl, nil
	}
	cl, err := New(cfg)
	if err != nil {
		return nil, err
	}
	c.clients[key] = cl
	return cl, nil
}

// cacheKey is a stable hash of the credential-and-endpoint fingerprint.
// Using SHA-256 means the password never appears in error messages,
// metric labels, or anything else that might log the key.
func cacheKey(cfg Config) string {
	h := sha256.New()
	// hash.Hash.Write is documented never to return an error, but errcheck
	// can't see that — discard explicitly.
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s\x00%t",
		cfg.Endpoint, cfg.Credentials.Email, cfg.Credentials.Password, cfg.InsecureSkipTLSVerify)
	return hex.EncodeToString(h.Sum(nil))
}
