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

import "testing"

func TestCache_ReusesSameClient(t *testing.T) {
	cache := NewCache()
	cfg := Config{
		Endpoint:    "http://strapi.example.com",
		Credentials: Credentials{Email: "admin@example.com", Password: "secret"},
	}

	c1, err := cache.Get(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := cache.Get(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatal("expected the same Client instance for identical configs")
	}
}

func TestCache_DifferentForDifferentConfigs(t *testing.T) {
	cache := NewCache()
	base := Config{
		Endpoint:    "http://strapi.example.com",
		Credentials: Credentials{Email: "admin@example.com", Password: "secret"},
	}
	other := base
	other.Credentials.Password = "other"

	c1, err := cache.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := cache.Get(other)
	if err != nil {
		t.Fatal(err)
	}
	if c1 == c2 {
		t.Fatal("expected distinct Client instances for different credentials")
	}
}

func TestCache_PropagatesNewError(t *testing.T) {
	cache := NewCache()
	if _, err := cache.Get(Config{}); err == nil {
		t.Fatal("expected error from invalid config")
	}
}
