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

// Package strapi is an HTTP client for Strapi's admin API. Managed-resource
// controllers obtain an authenticated *Client via New, then call Do/DoJSON
// for /admin/* operations. The client exchanges admin email+password for a
// JWT lazily, caches it, and re-logs in once on a 401 response.
package strapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	loginPath      = "/admin/login"
	defaultTimeout = 30 * time.Second
)

// Credentials is the JSON payload expected in the ProviderConfig secret.
type Credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// ParseCredentials decodes JSON-shaped admin credentials and validates them.
func ParseCredentials(data []byte) (Credentials, error) {
	var c Credentials
	if err := json.Unmarshal(data, &c); err != nil {
		return c, errors.Wrap(err, "credentials secret is not valid JSON")
	}
	if c.Email == "" || c.Password == "" {
		return c, errors.New(`credentials secret must contain non-empty "email" and "password"`)
	}
	return c, nil
}

// Config is everything needed to construct a Client.
type Config struct {
	Endpoint              string
	Credentials           Credentials
	InsecureSkipTLSVerify bool
}

// Client is an authenticated HTTP client for Strapi's admin API.
type Client struct {
	endpoint string
	creds    Credentials
	http     *http.Client

	mu    sync.Mutex
	token string
}

// New constructs a Client. It does not perform login — the JWT is fetched
// lazily on first request.
func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	if cfg.Credentials.Email == "" || cfg.Credentials.Password == "" {
		return nil, errors.New("credentials are required")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.InsecureSkipTLSVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in
	}

	return &Client{
		endpoint: strings.TrimRight(cfg.Endpoint, "/"),
		creds:    cfg.Credentials,
		http:     &http.Client{Timeout: defaultTimeout, Transport: transport},
	}, nil
}

// Endpoint returns the configured Strapi base URL.
func (c *Client) Endpoint() string { return c.endpoint }

// Do executes an HTTP request against Strapi with a valid admin JWT attached.
// path is a leading-slash path under the Strapi endpoint, e.g. "/admin/users".
// body is JSON-marshaled if non-nil. On a 401 response the cached JWT is
// dropped and the request is retried exactly once.
func (c *Client) Do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	// JWT likely expired. Drop it, re-login on the next attempt.
	resp.Body.Close() //nolint:errcheck
	c.invalidate()
	return c.do(ctx, method, path, body)
}

// DoJSON is a convenience wrapper that decodes a 2xx JSON response body into out.
// On non-2xx it returns an error containing the status and (truncated) body.
func (c *Client) DoJSON(ctx context.Context, method, path string, body, out any) error {
	resp, err := c.Do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return errors.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.Wrapf(err, "decode %s %s response", method, path)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	token, err := c.tokenLocked(ctx)
	if err != nil {
		return nil, err
	}

	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, errors.Wrap(err, "marshal request body")
		}
		rdr = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, rdr)
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

// tokenLocked returns a cached JWT, performing a fresh login if absent.
func (c *Client) tokenLocked(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" {
		return c.token, nil
	}
	tok, err := c.login(ctx)
	if err != nil {
		return "", err
	}
	c.token = tok
	return tok, nil
}

func (c *Client) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = ""
}

func (c *Client) login(ctx context.Context) (string, error) {
	body, err := json.Marshal(c.creds)
	if err != nil {
		return "", errors.Wrap(err, "marshal login body")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+loginPath, bytes.NewReader(body))
	if err != nil {
		return "", errors.Wrap(err, "build login request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "POST /admin/login")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", errors.Errorf("login failed: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}

	var out struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", errors.Wrap(err, "decode login response")
	}
	if out.Data.Token == "" {
		return "", errors.New("login response did not contain a token")
	}
	return out.Data.Token, nil
}
