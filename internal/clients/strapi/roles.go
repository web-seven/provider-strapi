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
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Role is a users-permissions role as returned by Strapi v4.
//
// The Permissions field uses Strapi's nested controller/action shape:
//
//	{
//	  "<api or plugin>": {
//	    "controllers": {
//	      "<controller>": {
//	        "<action>": { "enabled": true, "policy": "" }
//	      }
//	    }
//	  }
//	}
//
// Use FlattenPermissions / ExpandPermissions to convert to/from a flat
// list of "source.controller.action" strings.
type Role struct {
	ID          int                   `json:"id"`
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Type        string                `json:"type,omitempty"`
	Permissions PermissionsByResource `json:"permissions,omitempty"`
}

// PermissionsByResource maps a permission resource ("api::article.article",
// "plugin::users-permissions") to its controllers map.
type PermissionsByResource map[string]ResourcePermissions

// ResourcePermissions wraps the controllers map under a resource.
type ResourcePermissions struct {
	Controllers map[string]map[string]Action `json:"controllers"`
}

// Action represents the enabled/policy state of a single action.
type Action struct {
	Enabled bool   `json:"enabled"`
	Policy  string `json:"policy"`
}

// ListRoles fetches all users-permissions roles, including their permission
// trees.
func (c *Client) ListRoles(ctx context.Context) ([]Role, error) {
	var out struct {
		Roles []Role `json:"roles"`
	}
	if err := c.DoJSON(ctx, http.MethodGet, "/users-permissions/roles", nil, &out); err != nil {
		return nil, errors.Wrap(err, "list users-permissions roles")
	}
	return out.Roles, nil
}

// GetRole fetches a single role by ID with its permission tree.
func (c *Client) GetRole(ctx context.Context, id int) (Role, error) {
	var out struct {
		Role Role `json:"role"`
	}
	if err := c.DoJSON(ctx, http.MethodGet, "/users-permissions/roles/"+strconv.Itoa(id), nil, &out); err != nil {
		return Role{}, errors.Wrapf(err, "get role %d", id)
	}
	return out.Role, nil
}

// UpdateRole replaces a role's name, description and/or permission tree.
// Strapi's PUT semantics are full-replace: anything not present in the body
// is cleared.
func (c *Client) UpdateRole(ctx context.Context, id int, role Role) error {
	body := map[string]any{
		"name":        role.Name,
		"description": role.Description,
		"type":        role.Type,
		"permissions": role.Permissions,
	}
	if err := c.DoJSON(ctx, http.MethodPut, "/users-permissions/roles/"+strconv.Itoa(id), body, nil); err != nil {
		return errors.Wrapf(err, "update role %d", id)
	}
	return nil
}

// FindRole returns the role whose type or name matches the given selector.
// Built-in roles are matched first by type ("public" / "authenticated"),
// case-insensitively. Custom roles fall through to a case-insensitive name
// match. Returns false if no role matches.
func FindRole(roles []Role, selector string) (Role, bool) {
	want := strings.ToLower(selector)
	for _, r := range roles {
		if strings.EqualFold(r.Type, want) {
			return r, true
		}
	}
	for _, r := range roles {
		if strings.EqualFold(r.Name, want) {
			return r, true
		}
	}
	return Role{}, false
}

// FlattenPermissions turns Strapi's nested permission tree into a sorted
// list of action strings matching Strapi's flat storage convention:
//
//   - api content types → "api::<api>.<contentType>.<action>". The Strapi
//     controller name conventionally equals the content type, so it is
//     implicit and omitted from the flat form.
//   - plugins → "plugin::<plugin>.<controller>.<action>". The controller
//     name varies per plugin, so it is explicit.
//
// Actions whose Enabled flag is false are skipped. The result is sorted
// for stable diffs.
func FlattenPermissions(p PermissionsByResource) []string {
	out := make([]string, 0)
	for resource, rp := range p {
		for controller, actions := range rp.Controllers {
			for action, a := range actions {
				if !a.Enabled {
					continue
				}
				if implicitController(resource, controller) {
					out = append(out, resource+"."+action)
				} else {
					out = append(out, resource+"."+controller+"."+action)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// implicitController reports whether the given controller name is implicit
// for the given resource UID under Strapi's naming convention. For
// "api::<api>.<contentType>" resources the controller equals contentType
// and is omitted from the flat form.
func implicitController(resource, controller string) bool {
	if !strings.HasPrefix(resource, "api::") {
		return false
	}
	last := strings.LastIndex(resource, ".")
	return last >= 0 && resource[last+1:] == controller
}

// ExpandPermissions turns a flat list of permission action strings into
// Strapi's nested permission tree, with all listed actions marked enabled.
// Malformed entries are silently dropped.
//
// Accepted forms:
//
//	api::<api>.<contentType>.<action>
//	plugin::<plugin>.<controller>.<action>
func ExpandPermissions(flat []string) PermissionsByResource {
	out := make(PermissionsByResource)
	for _, p := range flat {
		resource, controller, action, ok := splitFlat(p)
		if !ok {
			continue
		}
		rp, ok := out[resource]
		if !ok {
			rp = ResourcePermissions{Controllers: map[string]map[string]Action{}}
		}
		ctrl, ok := rp.Controllers[controller]
		if !ok {
			ctrl = map[string]Action{}
		}
		ctrl[action] = Action{Enabled: true}
		rp.Controllers[controller] = ctrl
		out[resource] = rp
	}
	return out
}

// splitFlat parses a flat permission string. Returns ok=false on malformed
// inputs (unknown source prefix, wrong segment count, empty parts).
func splitFlat(p string) (resource, controller, action string, ok bool) {
	lastDot := strings.LastIndex(p, ".")
	if lastDot <= 0 || lastDot == len(p)-1 {
		return "", "", "", false
	}
	action = p[lastDot+1:]
	before := p[:lastDot]

	switch {
	case strings.HasPrefix(before, "api::"):
		resource, controller, ok = splitAPI(before)
	case strings.HasPrefix(before, "plugin::"):
		resource, controller, ok = splitPlugin(before)
	default:
		return "", "", "", false
	}
	if !ok || resource == "" || controller == "" {
		return "", "", "", false
	}
	return resource, controller, action, true
}

// splitAPI parses the "<resource>" prefix of an api flat string. Accepts
// both the canonical "api::<api>.<contentType>" (controller implicit, equals
// contentType) and the explicit "api::<api>.<contentType>.<controller>".
func splitAPI(before string) (resource, controller string, ok bool) {
	afterPrefix := before[len("api::"):]
	firstDot := strings.Index(afterPrefix, ".")
	if firstDot <= 0 {
		return "", "", false
	}
	tail := afterPrefix[firstDot+1:]
	if dot := strings.Index(tail, "."); dot >= 0 {
		return "api::" + afterPrefix[:firstDot+1+dot], tail[dot+1:], true
	}
	if tail == "" {
		return "", "", false
	}
	return before, tail, true
}

// splitPlugin parses the "<resource>.<controller>" prefix of a plugin flat
// string ("plugin::<plugin>.<controller>").
func splitPlugin(before string) (resource, controller string, ok bool) {
	afterPrefix := before[len("plugin::"):]
	firstDot := strings.Index(afterPrefix, ".")
	if firstDot <= 0 || firstDot == len(afterPrefix)-1 {
		return "", "", false
	}
	return "plugin::" + afterPrefix[:firstDot], afterPrefix[firstDot+1:], true
}
