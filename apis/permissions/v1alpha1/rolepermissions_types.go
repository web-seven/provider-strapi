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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// RolePermissionsParameters are the configurable fields of a RolePermissions.
type RolePermissionsParameters struct {
	// Role identifies the users-permissions role to manage.
	// Built-in values are "public" and "authenticated", matched case-insensitively
	// against Strapi's role.type field. Other values are matched against role.name.
	// The role must already exist in Strapi; this resource manages the role's
	// permission set, not its lifecycle.
	// +kubebuilder:validation:MinLength=1
	Role string `json:"role"`

	// Permissions is the full set of action strings granted to the role,
	// e.g. "api::article.article.find" or
	// "plugin::users-permissions.auth.callback". Strapi's PUT semantics replace
	// the entire permission set, so this list is authoritative — anything not
	// listed is revoked on the next reconcile.
	// +optional
	Permissions []string `json:"permissions,omitempty"`
}

// RolePermissionsObservation are the observable fields of a RolePermissions.
type RolePermissionsObservation struct {
	// RoleID is the numeric identifier of the resolved role in Strapi.
	RoleID int `json:"roleID,omitempty"`

	// Type is the type field of the resolved role (e.g. "public").
	Type string `json:"type,omitempty"`

	// Permissions is the current permission action set as observed on the role.
	Permissions []string `json:"permissions,omitempty"`

	// Cleared is set by Delete after wiping the role's permissions in Strapi.
	// On the next deletion reconcile Observe uses it to report the external
	// resource as gone, so the finalizer drains instead of looping on Delete
	// (the role itself is never removed by this MR).
	Cleared bool `json:"cleared,omitempty"`
}

// A RolePermissionsSpec defines the desired state of a RolePermissions.
type RolePermissionsSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              RolePermissionsParameters `json:"forProvider"`
}

// A RolePermissionsStatus represents the observed state of a RolePermissions.
type RolePermissionsStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          RolePermissionsObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// RolePermissions manages the permission set of a single Strapi
// users-permissions role.
// +kubebuilder:printcolumn:name="ROLE",type="string",JSONPath=".spec.forProvider.role"
// +kubebuilder:printcolumn:name="ROLE-ID",type="integer",JSONPath=".status.atProvider.roleID"
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,strapi}
type RolePermissions struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RolePermissionsSpec   `json:"spec"`
	Status RolePermissionsStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RolePermissionsList contains a list of RolePermissions.
type RolePermissionsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RolePermissions `json:"items"`
}
