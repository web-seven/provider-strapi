# Crossplane Strapi Provider

Manage [Strapi v4](https://strapi.io) resources declaratively from Kubernetes via [Crossplane](https://crossplane.io).

## Features

- **Declarative Strapi management** — control Strapi instances from
  Kubernetes manifests, reconciled continuously by Crossplane.
- **Admin API auth** — point the provider at any reachable Strapi v4
  endpoint with admin credentials (email + password) supplied via a
  Kubernetes `Secret`. `insecureSkipTLSVerify` is available for
  self-signed certificates.
- **Namespaced and cluster-scoped configuration** — `ProviderConfig`
  (namespaced) and `ClusterProviderConfig` (cluster-scoped) cover both
  multi-tenant and shared-instance topologies.
- **Composable** — combine Strapi resources with any other Crossplane
  provider via Compositions.

## Available resources

The provider currently ships:

- `RolePermissions` (`permissions.strapi.crossplane.io/v1alpha1`) — manage
  the permission set on a users-permissions role.

Additional managed resources are planned.

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

## Example

Declare a managed resource referencing the `ClusterProviderConfig` above.
The example uses `RolePermissions` (the resource shipped today); other
Strapi resources will follow the same pattern.

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

More samples live in [`examples/`](https://github.com/web-seven/provider-strapi/tree/main/examples).

## Source

- Source: [github.com/web-seven/provider-strapi](https://github.com/web-seven/provider-strapi)
- Issues: [github.com/web-seven/provider-strapi/issues](https://github.com/web-seven/provider-strapi/issues)
- License: [Apache-2.0](https://github.com/web-seven/provider-strapi/blob/main/LICENSE)
