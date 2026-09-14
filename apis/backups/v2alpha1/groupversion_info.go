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
	"reflect"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// Package type metadata.
const (
	Group   = "backups.strapi.crossplane.io"
	Version = "v2alpha1"
)

var (
	// SchemeGroupVersion is group version used to register these objects.
	SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: SchemeGroupVersion}
)

// Backup type metadata.
var (
	BackupKind             = reflect.TypeOf(Backup{}).Name()
	BackupGroupKind        = schema.GroupKind{Group: Group, Kind: BackupKind}.String()
	BackupKindAPIVersion   = BackupKind + "." + SchemeGroupVersion.String()
	BackupGroupVersionKind = SchemeGroupVersion.WithKind(BackupKind)
)

func init() {
	SchemeBuilder.Register(&Backup{}, &BackupList{})
}
