# provider-strapi

A [Crossplane](https://crossplane.io/) provider for [Strapi](https://strapi.io/).
Manages Strapi resources declaratively as Kubernetes custom resources, against
a running Strapi v4 instance.

> Status: alpha. Targets Strapi v4 (`@strapi/strapi 4.20.x`). Strapi v5 support
> will land when the upstream API delta is known.

## Resources

| API group | Kind | What it does |
|---|---|---|
| `strapi.crossplane.io/v1alpha1` | `ProviderConfig` / `ClusterProviderConfig` | Endpoint + admin email/password used to authenticate against a Strapi instance. |
| `permissions.strapi.crossplane.io/v1alpha1` | `RolePermissions` | Manages the permission set of a users-permissions role (built-in `Public` / `Authenticated` or any existing custom role). |

See `examples/` for ready-to-apply manifests.

## How auth works

The provider does **not** bootstrap a Strapi admin user. The cluster operator
(or your Helm chart) is responsible for ensuring a Strapi admin exists and
storing their credentials in a Kubernetes Secret. The Secret payload is JSON:

```json
{"email": "admin@example.com", "password": "..."}
```

`ProviderConfig` references that Secret. On first use the provider POSTs to
`/admin/login`, caches the returned JWT, and re-logs in once on a 401
response (handles JWT expiry transparently).

## Quick start

```yaml
# 1. Secret with admin credentials (JSON-encoded)
apiVersion: v1
kind: Secret
metadata: { name: strapi-admin, namespace: default }
type: Opaque
stringData:
  credentials: '{"email":"admin@example.com","password":"change-me"}'
---
# 2. ProviderConfig pointing at the Strapi instance + secret
apiVersion: strapi.crossplane.io/v1alpha1
kind: ProviderConfig
metadata: { name: example, namespace: default }
spec:
  endpoint: https://strapi.example.com
  credentials:
    source: Secret
    secretRef: { namespace: default, name: strapi-admin, key: credentials }
---
# 3. Manage the Public role's permissions
apiVersion: permissions.strapi.crossplane.io/v1alpha1
kind: RolePermissions
metadata: { name: public, namespace: default }
spec:
  providerConfigRef: { name: example }
  forProvider:
    role: public
    permissions:
      - api::article.article.find
      - api::article.article.findOne
      - plugin::users-permissions.auth.callback
```

`RolePermissions` replaces the role's entire permission tree on each reconcile —
anything not listed is revoked.

## Development

The build pipeline relies on `crossplane/build` as a git submodule. **First
clone:**

```sh
make submodules
```

Day-to-day loops:

| Command | What it does |
|---|---|
| `make serve` | Creates an Overlock-managed Crossplane environment (`overlock env create $(PROJECT_NAME)`) and serves the provider with hot-reload on source changes (`overlock provider serve`). Best dev loop. |
| `make serve-clean` | Tears down the Overlock environment. |
| `make dev` | Alternative: creates a `kind` cluster, applies CRDs, runs `go run cmd/provider/main.go --debug` in foreground. |
| `make dev-clean` | Deletes the `kind` cluster created by `make dev`. |
| `make run` | Runs the built provider binary against your current kubeconfig (no cluster bootstrap). |
| `make reviewable` | Code generation + linters + unit tests. Pre-commit gate. |
| `make test` | Go unit tests only. |
| `make generate` | Regenerate CRDs and methodsets after editing `apis/` types. |

Add a new managed-resource type:

```sh
make provider.addtype provider=Strapi group=<lower> kind=<CamelKind>
```

After running, register the new API group in `apis/strapi.go` and the new
controller in `internal/controller/strapi.go`.

## License

Apache 2.0.
