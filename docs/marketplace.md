# Crossplane Strapi Provider

Manage [Strapi v4](https://strapi.io) resources declaratively from Kubernetes via [Crossplane](https://crossplane.io).

## Features

- **RolePermissions** — declare the full permission set for a Strapi
  users-permissions role. Built-in `Public` and `Authenticated` roles are
  resolved by `type`; custom roles by name. Strapi's `PUT` is full-replace,
  so the spec's `permissions` list is always authoritative.
- **ProviderConfig / ClusterProviderConfig** — supply Strapi endpoint and
  admin credentials (email + password) via a Kubernetes `Secret`. Supports
  `insecureSkipTLSVerify` for self-signed certificates.
- **Crossplane composition** — compose Strapi resources together with any
  other Crossplane provider.

## Install

```bash
crossplane xpkg install provider xpkg.upbound.io/web7/provider-strapi:v0.1.0
```

Or as a manifest:

```yaml
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-strapi
spec:
  package: xpkg.upbound.io/web7/provider-strapi:v0.1.0
```

## Prerequisites

- A Kubernetes cluster with Crossplane installed.
- A reachable Strapi v4 instance with an admin account whose credentials
  the provider can use to call the admin API.

## Configure

Create a `Secret` holding the admin credentials as a JSON payload, then a
`ClusterProviderConfig` (cluster-scoped) or `ProviderConfig` (namespaced)
that points at it. See [`examples/provider`](https://github.com/web-seven/provider-strapi/tree/main/examples/provider).

### Credentials secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: strapi-creds
  namespace: crossplane-system
type: Opaque
stringData:
  credentials: |
    {"email": "admin@example.com", "password": "..."}
```

### ClusterProviderConfig

```yaml
apiVersion: strapi.crossplane.io/v1alpha1
kind: ClusterProviderConfig
metadata:
  name: default
spec:
  endpoint: https://strapi.example.com
  credentials:
    source: Secret
    secretRef:
      namespace: crossplane-system
      name: strapi-creds
      key: credentials
```

If the managed resource omits `providerConfigRef`, Crossplane v2 defaults
to `name: default`, `kind: ClusterProviderConfig`.

## Usage

### RolePermissions

Manages the permission set on an existing users-permissions role.
Permissions are declared in the flat Strapi v4 form
(`api::<api>.<contentType>.<action>` for content APIs,
`plugin::<plugin>.<controller>.<action>` for plugins).

```yaml
apiVersion: permissions.strapi.crossplane.io/v1alpha1
kind: RolePermissions
metadata:
  name: public-read
  namespace: default
spec:
  forProvider:
    roleType: public            # or "authenticated", or use roleName: <custom>
    permissions:
      - api::article.article.find
      - api::article.article.findOne
      - plugin::users-permissions.auth.callback
```

`Delete` clears the configured permissions from the role; the role
itself is never removed (built-in roles cannot be deleted, and custom
role lifecycle is intentionally out of scope).

## Examples

See the [`examples/`](https://github.com/web-seven/provider-strapi/tree/main/examples) directory for sample `ProviderConfig` and `RolePermissions` manifests.

## Source

- Source: [github.com/web-seven/provider-strapi](https://github.com/web-seven/provider-strapi)
- Issues: [github.com/web-seven/provider-strapi/issues](https://github.com/web-seven/provider-strapi/issues)
- License: [Apache-2.0](https://github.com/web-seven/provider-strapi/blob/main/LICENSE)
