# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

Crossplane provider for Strapi v4. Module path `github.com/web-seven/provider-strapi`. Currently ships:

- `strapi.crossplane.io/v1alpha1` — `ProviderConfig` (namespaced) and `ClusterProviderConfig` (cluster-scoped). Spec: `endpoint`, `credentials` (Secret with JSON `{"email","password"}`), optional `insecureSkipTLSVerify`.
- `permissions.strapi.crossplane.io/v1alpha1` — `RolePermissions`. Manages a users-permissions role's permission set against an existing role (built-in `Public`/`Authenticated` resolved by `type`, custom roles by name). Strapi's `PUT` is full-replace, so the spec's `permissions` list is authoritative.
- `internal/clients/strapi/` — HTTP client. `Client.Do/DoJSON` attaches the admin JWT, drops the cache on 401, re-logs in once. `roles.go` adds `ListRoles`, `UpdateRole`, `FindRole`, plus `FlattenPermissions`/`ExpandPermissions` between Strapi's nested tree and the flat user-facing form.

## Common commands

The build pipeline relies on `crossplane/build` as a git submodule under `build/`. **Run `make submodules` first** on a fresh clone — without it, the included makefiles are missing and any `make` target that uses `$(INFO)`/`$(KIND)` etc. will fail.

- `make submodules` — initialize/refresh the `build/` submodule.
- `make reviewable` — code generation + linters + unit tests; canonical pre-commit gate.
- `make build` — build the provider binary and OCI/xpkg artifacts.
- `make test` — Go unit tests.
- `make lint` — `golangci-lint` (`.golangci.yml`; goimports local-prefix `github.com/web-seven/provider-strapi`).
- `make generate` — re-run code generators after editing anything under `apis/`. Wired through `apis/generate.go`: `controller-gen` (deepcopy + CRDs into `package/crds/`) and `angryjet` (managed/PC methodsets — `zz_generated.managed.go`, `zz_generated.pc*.go`).
- `make serve` — `overlock env create $(PROJECT_NAME)` + `overlock provider serve` (hot-reload). Primary dev loop. Uses plain `echo`, so works without `make submodules`.
- `make serve-clean` — `overlock env delete $(PROJECT_NAME)`.
- `make dev` / `make dev-clean` — `kind`-based alternative (requires `make submodules` because it depends on `$(KIND)`/`$(KUBECTL)`).
- `make run` — built binary against current kubeconfig.

Run a single Go test:

```sh
go test ./internal/controller/<pkg> -run TestName -v
```

Add a new managed-resource type via the still-active scaffolder:

```sh
make provider.addtype provider=Strapi group=<lower> kind=<CamelKind> [apiversion=v1alpha1]
```

After running:
1. Register the new group's `SchemeBuilder.AddToScheme` in `apis/strapi.go` (`AddToSchemes`).
2. Register the new controller's `SetupGated` in `internal/controller/strapi.go`.

The one-shot `make provider.prepare` (template rename) has been removed — the rename has already happened.

## Architecture

Standard Crossplane v2 provider, three layers:

**1. Entrypoint — `cmd/provider/main.go`**
Builds a `controller-runtime` Manager, wires feature gates (`EnableBetaManagementPolicies`, `EnableAlphaChangeLogs`), MR metrics, change-logs gRPC client, calls `customresourcesgate.Setup` then `controller.SetupGated`. Flags: `--debug`, `--leader-election`, `--sync` (drift recheck, default 1h), `--poll` (per-resource poll, default 1m), `--max-reconcile-rate`, `--enable-management-policies`, `--enable-changelogs`.

**2. APIs — `apis/`**
Two-tier layout:

- `apis/v1alpha1/` — provider-level types. `ProviderConfig` (namespaced) and `ClusterProviderConfig` (cluster-scoped) plus their `*Usage` companions. `ProviderConfigSpec` is shared between the two. Credentials use `xpv1.CommonCredentialSelectors` (Source: `None|Secret|InjectedIdentity|Environment|Filesystem`); the secret payload is JSON parsed by `internal/clients/strapi.ParseCredentials`.
- `apis/<group>/v1alpha1/` — managed-resource types. Currently `apis/permissions/v1alpha1/` (`RolePermissions`). Add new groups via `make provider.addtype`. Each `<Kind>Spec` embeds `xpv2.ManagedResourceSpec` + a `ForProvider` parameters struct; `<Kind>Status` embeds `xpv1.ResourceStatus` + an `AtProvider` observation struct.
- `apis/strapi.go` is the aggregating `SchemeBuilder` — every API group must be added to its `AddToSchemes`.

**3. Controllers — `internal/controller/`**

- `strapi.go` — `SetupGated(mgr, opts)` registers each controller. Currently: `config.Setup`, `rolepermissions.SetupGated`.
- `config/config.go` — reconciles both `ProviderConfig` and `ClusterProviderConfig` via `crossplane-runtime`'s `providerconfig.NewReconciler`. Both wired unconditionally.
- `rolepermissions/rolepermissions.go` — managed-resource controller for `RolePermissions`. Per-MR-kind shape:
  - `SetupGated` registers `Setup` with `o.Gate` keyed by GVK so it only starts after the CRD is present (safe-start).
  - `connector.Connect` resolves credentials by switching on `cr.GetProviderConfigReference().Kind`. `"ProviderConfig"` is namespaced (must use `cr.GetNamespace()` in the `kube.Get`); `"ClusterProviderConfig"` is cluster-scoped. Tracks usage via `resource.NewProviderConfigUsageTracker`, extracts creds via `resource.CommonCredentialExtractor`, parses JSON with `strapiclient.ParseCredentials`, builds a `*strapiclient.Client`.
  - `external` implements `managed.TypedExternalClient` against a small `strapiClient` interface (so tests substitute a fake without httptest). `Observe` resolves the role by external-name (numeric ID) or selector, computes drift via `FlattenPermissions`. `Create`/`Update` `PUT` the role with `ExpandPermissions(spec)`. `Delete` is a no-op (built-in roles can't be removed; we don't manage role lifecycle).
  - `WithEventFilter(resource.DesiredStateChanged())` on the controller builder so spec-only changes drive reconciliation.

**Strapi client (`internal/clients/strapi/`)**
- `strapi.go`: `Client` with mutex-cached JWT, lazy login on first request, `Do`/`DoJSON` helpers. On 401 the JWT cache is invalidated and the request retried once (handles 30-day default JWT expiry).
- `roles.go`: users-permissions role types (`Role`, nested `PermissionsByResource`/`ResourcePermissions`/`Action`), CRUD helpers, `FindRole` selector, and the flat-vs-nested permission-format converters.
- Permission action format follows Strapi v4 storage: `api::<api>.<contentType>.<action>` (controller implicit, equals contentType) and `plugin::<plugin>.<controller>.<action>` (controller explicit). The flat string IS what the user writes; the nested tree is internal.

**Resource scope.** `ProviderConfig` is namespace-scoped; `ClusterProviderConfig` is cluster-scoped. `RolePermissions` is namespace-scoped. New managed resources should generally be namespace-scoped; their connector must use `cr.GetNamespace()` when resolving a namespaced `ProviderConfig`.

## Code generation invariants

- All `zz_generated.*.go` files come from `make generate` — never edit by hand. Lint and goimports both ignore them.
- After editing kubebuilder markers, struct fields, or adding/removing types under `apis/`, re-run `make generate` (or `make reviewable`) before commit; CRDs in `package/crds/` and managed-resource methodsets must stay in sync with the Go types.
- `hack/boilerplate.go.txt` is prepended to every generated file.

## Pre-push checklist

CI's `lint` and `check-diff` jobs are strict. Before pushing, run locally:

- `gofmt -l .` (must be empty).
- `go mod tidy` (commit any go.mod/go.sum changes; `go build`/`go test` don't enforce direct/indirect classification).
- `go vet ./...`
- `go test ./...`
- For `apis/` changes: `go generate ./apis/...` and commit the resulting CRD/deepcopy files.

## Linter conventions

`.golangci.yml` enables a broad set (errorlint, gosec, gocyclo, prealloc, etc.). Notable settings: `gocyclo` complexity max 10 (split helpers if you exceed), goimports local-prefix `github.com/web-seven/provider-strapi`, test files relax `dupl/errcheck/gocyclo/gosec/unparam`. Generated files and `examples/` are excluded.
