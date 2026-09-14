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
	"net/http"
	"strconv"

	"github.com/pkg/errors"
)

const transferTokensPath = "/admin/transfer/tokens"

// TransferTokenPermissionPull is the transfer token scope that allows
// pulling a project dump from the remote data-transfer endpoint.
const TransferTokenPermissionPull = "pull"

// TransferToken is a data-transfer token as returned by Strapi's admin API.
// AccessKey is only populated by CreateTransferToken — Strapi never returns
// it again, so callers must persist it.
type TransferToken struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
	AccessKey   string   `json:"accessKey,omitempty"`
}

// ListTransferTokens fetches all transfer tokens, without their access keys.
func (c *Client) ListTransferTokens(ctx context.Context) ([]TransferToken, error) {
	var out struct {
		Data []TransferToken `json:"data"`
	}
	if err := c.DoJSON(ctx, http.MethodGet, transferTokensPath, nil, &out); err != nil {
		return nil, errors.Wrap(err, "list transfer tokens")
	}
	return out.Data, nil
}

// CreateTransferToken creates a non-expiring transfer token. Token names are
// unique in Strapi; creating a duplicate name fails.
func (c *Client) CreateTransferToken(ctx context.Context, name, description string, permissions []string) (TransferToken, error) {
	body := map[string]any{
		"name":        name,
		"description": description,
		"permissions": permissions,
		"lifespan":    nil,
	}
	var out struct {
		Data TransferToken `json:"data"`
	}
	if err := c.DoJSON(ctx, http.MethodPost, transferTokensPath, body, &out); err != nil {
		return TransferToken{}, errors.Wrapf(err, "create transfer token %q", name)
	}
	if out.Data.AccessKey == "" {
		return TransferToken{}, errors.Errorf("create transfer token %q: response did not contain an access key", name)
	}
	return out.Data, nil
}

// RegenerateTransferToken issues a new access key for an existing token,
// invalidating the previous one, and returns it.
func (c *Client) RegenerateTransferToken(ctx context.Context, id int) (string, error) {
	var out struct {
		Data TransferToken `json:"data"`
	}
	if err := c.DoJSON(ctx, http.MethodPost, transferTokensPath+"/"+strconv.Itoa(id)+"/regenerate", nil, &out); err != nil {
		return "", errors.Wrapf(err, "regenerate transfer token %d", id)
	}
	if out.Data.AccessKey == "" {
		return "", errors.Errorf("regenerate transfer token %d: response did not contain an access key", id)
	}
	return out.Data.AccessKey, nil
}

// DeleteTransferToken deletes a transfer token by ID.
func (c *Client) DeleteTransferToken(ctx context.Context, id int) error {
	if err := c.DoJSON(ctx, http.MethodDelete, transferTokensPath+"/"+strconv.Itoa(id), nil, nil); err != nil {
		return errors.Wrapf(err, "delete transfer token %d", id)
	}
	return nil
}
