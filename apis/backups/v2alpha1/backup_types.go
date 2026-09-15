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

package v2alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
)

// BucketCredentials selects the identity used to write to the destination
// bucket. "Secret" reads a JSON payload (accessKeyId, secretAccessKey,
// optional sessionToken) from the referenced key. "InjectedIdentity" uses
// the AWS SDK's ambient credential chain (e.g. IRSA on EKS, workload
// identity federation), so no secret is required.
type BucketCredentials struct {
	// Source of the bucket credentials.
	// +kubebuilder:validation:Enum=InjectedIdentity;Secret
	Source xpv1.CredentialsSource `json:"source"`

	xpv1.CommonCredentialSelectors `json:",inline"`
}

// BackupDestination is the S3-compatible bucket a backup is written to. Only
// the S3 API is supported, which covers AWS, MinIO, Cloudflare R2 and GCS
// (via its S3-compatible XML API) through a single implementation.
type BackupDestination struct {
	// Bucket is the name of the destination bucket. It must already exist;
	// this resource does not manage bucket lifecycle.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Path is a key prefix under which backup objects are written, e.g.
	// "production". Defaults to the bucket root.
	// +optional
	Path string `json:"path,omitempty"`

	// Region is the bucket's region, e.g. "us-east-1". Required by the S3
	// API even for S3-compatible services that don't use AWS regions.
	// +optional
	Region string `json:"region,omitempty"`

	// Endpoint overrides the default AWS S3 endpoint, e.g.
	// "https://minio.example.com" or an R2/GCS S3-compatible endpoint.
	// Leave empty to use AWS S3.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// ForcePathStyle addresses the bucket as
	// "<endpoint>/<bucket>/<key>" instead of "<bucket>.<endpoint>/<key>".
	// Most S3-compatible services other than AWS require this.
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`

	// Credentials used to write to the bucket.
	Credentials BucketCredentials `json:"credentials"`
}

// BackupParameters are the configurable fields of a Backup.
type BackupParameters struct {
	// Schedule is a standard 5-field cron expression (minute hour
	// day-of-month month day-of-week) in UTC, e.g. "0 2 * * *" for daily at
	// 02:00. A backup is taken the first time it comes due after the
	// resource is created or after the previous backup completed.
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`

	// Destination is the bucket a backup is written to.
	Destination BackupDestination `json:"destination"`

	// TransferTokenSecretRef references a Secret containing a Strapi
	// transfer token scoped to "pull". Strapi's remote data-transfer
	// endpoint authenticates with a transfer token, not an admin session.
	//
	// Optional: when omitted, the provider creates a pull-scoped transfer
	// token through Strapi's admin API using the ProviderConfig credentials,
	// stores it in a Secret named "<backup name>-transfer-token" owned by
	// this Backup, and deletes the token when the Backup is deleted.
	// +optional
	TransferTokenSecretRef *xpv1.SecretKeySelector `json:"transferTokenSecretRef,omitempty"`

	// IncludeAssets additionally pulls uploaded media files into the
	// backup. Disabled by default, since self-hosted Strapi instances
	// commonly already store media in object storage directly (via the
	// upload provider), making a second copy through this resource
	// redundant.
	// +optional
	IncludeAssets bool `json:"includeAssets,omitempty"`

	// RetentionDays prunes backups older than this many days from the
	// destination after each successful backup. Zero (the default) keeps
	// every backup indefinitely; use bucket lifecycle rules instead if
	// that's a better fit for your storage backend.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RetentionDays int32 `json:"retentionDays,omitempty"`
}

// BackupObservation are the observable fields of a Backup.
type BackupObservation struct {
	// LastBackupTime is when the most recent successful backup completed.
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// LastBackupKey is the destination key prefix the most recent backup
	// was written under.
	LastBackupKey string `json:"lastBackupKey,omitempty"`

	// LastBackupSizeBytes is the total size of the objects written by the
	// most recent backup.
	LastBackupSizeBytes int64 `json:"lastBackupSizeBytes,omitempty"`

	// LastBackupEntities is the number of entity records captured by the
	// most recent backup.
	LastBackupEntities int64 `json:"lastBackupEntities,omitempty"`

	// LastBackupAssets is the number of media assets captured by the most
	// recent backup, when includeAssets is enabled.
	LastBackupAssets int64 `json:"lastBackupAssets,omitempty"`

	// NextBackupTime is when the next backup is due.
	NextBackupTime *metav1.Time `json:"nextBackupTime,omitempty"`
}

// A BackupSpec defines the desired state of a Backup.
type BackupSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              BackupParameters `json:"forProvider"`
}

// A BackupStatus represents the observed state of a Backup.
type BackupStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          BackupObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// Backup dumps a Strapi project's entities, relations and configuration (and
// optionally its media assets) to an S3-compatible bucket on a cron
// schedule, using Strapi's remote data-transfer "pull" endpoint.
// +kubebuilder:printcolumn:name="SCHEDULE",type="string",JSONPath=".spec.forProvider.schedule"
// +kubebuilder:printcolumn:name="LAST-BACKUP",type="date",JSONPath=".status.atProvider.lastBackupTime"
// +kubebuilder:printcolumn:name="NEXT-BACKUP",type="date",JSONPath=".status.atProvider.nextBackupTime"
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,strapi}
type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupSpec   `json:"spec"`
	Status BackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupList contains a list of Backup.
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}
