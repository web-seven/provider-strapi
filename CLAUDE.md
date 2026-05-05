# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

Crossplane provider for Strapi, scaffolded from `crossplane/provider-template` and renamed via `make provider.prepare provider=Strapi`. The Go module path is `github.com/web-seven/provider-strapi`. The placeholder `MyType` example resource and its `apis/sample` group have been removed; no Strapi-specific managed resources have been added yet — `apis/strapi.go` and `internal/controller/strapi.go` only wire up `ProviderConfig`/`ClusterProviderConfig` (the `config` controller). Use `make provider.addtype` to scaffold the first real resource.

## Common commands

The build system is driven by `crossplane/build` as a git submodule under `build/`. **Run `make submodules` before anything else** on a fresh clone — without it, the included makefiles are missing.

- `make submodules` — initialize/refresh the `build/` submodule.
- `make reviewable` — code generation + linters + unit tests; the canonical pre-commit gate.
- `make build` — build the provider binary and OCI/xpkg artifacts.
- `make test` — Go unit tests (provided by `build/makelib/golang.mk`).
- `make lint` — `golangci-lint` (config in `.golangci.yml`; local-prefix `github.com/web-seven/provider-strapi`).
- `make generate` — re-run code generators; required after editing anything under `apis/`. Wired through `apis/generate.go` and runs `controller-gen` (deepcopy + CRDs into `package/crds/`) and `angryjet` (crossplane-runtime methodsets — `zz_generated.managed.go`, `zz_generated.pc*.go`, etc.).
- `make run` — `go build` then run the provider out-of-cluster against your current kubeconfig.
- `make dev` — create a local `kind` cluster (`provider-strapi-dev`), apply CRDs from `package/crds`, and run the provider against it via `go run cmd/provider/main.go --debug`.
- `make dev-clean` — delete the kind cluster created by `make dev`.
- `make e2e.run` / `make test-integration` — run `cluster/local/integration_tests.sh` against a kind cluster (uses `KIND`, `KUBECTL`, `CROSSPLANE_CLI`, `HELM3` from the build submodule).

Run a single Go test:

```sh
go test ./internal/controller/<pkg> -run TestName -v
```

Add a new managed-resource type:

```sh
make provider.addtype provider=Strapi group=<lower> kind=<CamelKind> [apiversion=v1alpha1]
```

After running it you must:
1. Add the new API group's `SchemeBuilder.AddToScheme` to `apis/strapi.go` (`AddToSchemes`).
2. Register the new controller's `SetupGated` in `internal/controller/strapi.go`.

Note: `make provider.prepare` has already been run for this repo and is one-shot — do not run it again.

## Architecture

Standard Crossplane v2 provider skeleton, three layers:

**1. Entrypoint — `cmd/provider/main.go`**
Builds a `controller-runtime` Manager, wires feature gates (`EnableBetaManagementPolicies`, `EnableAlphaChangeLogs`), MR metrics, change-logs gRPC client, and calls `customresourcesgate.Setup` followed by `controller.SetupGated`. Flags worth knowing: `--debug`, `--leader-election`, `--sync` (drift recheck, default 1h), `--poll` (per-resource poll, default 1m), `--max-reconcile-rate`, `--enable-management-policies`, `--enable-changelogs`.

**2. APIs — `apis/`**
Two-tier layout, important to keep straight:

- `apis/v1alpha1/` — **provider-level** types: `ProviderConfig` (namespaced) + `ClusterProviderConfig` (cluster-scoped), plus their `*Usage` companions. Both reference credentials via `xpv1.CommonCredentialSelectors` (Source enum: `None|Secret|InjectedIdentity|Environment|Filesystem`). Generated `zz_generated.pc.go` / `pcu.go` / `pculist.go` come from angryjet.
- `apis/<group>/v1alpha1/` (none yet — added via `make provider.addtype`) — **managed-resource** types. Use the standard split: `<Kind>Spec` embeds `xpv2.ManagedResourceSpec` and a `ForProvider` parameters struct; `<Kind>Status` embeds `xpv1.ResourceStatus` and an `AtProvider` observation struct. Generated `zz_generated.managed.go` / `managedlist.go` come from angryjet.
- `apis/strapi.go` is the aggregating scheme builder — every API group must be added to its `AddToSchemes`.

**3. Controllers — `internal/controller/`**

- `strapi.go` — `SetupGated(mgr, opts)` registers each controller's setup function. Currently only `config.Setup`; new types must be added here.
- `config/config.go` — provides reconcilers for both `ProviderConfig` and `ClusterProviderConfig` using `crossplane-runtime`'s `providerconfig.NewReconciler`. Both are wired up unconditionally; do not duplicate.
- `<resource>/<resource>.go` (none yet) — one directory per managed-resource kind. The standard shape:
  - `SetupGated` registers the real `Setup` with `o.Gate` keyed by GVK so it only starts after the CRD is present (safe-start). Always use this from `strapi.go`, not direct `Setup`.
  - `connector.Connect` resolves credentials by switching on `cr.GetProviderConfigReference().Kind` — `"ProviderConfig"` is namespaced (must use `cr.GetNamespace()`), `"ClusterProviderConfig"` is cluster-scoped. It tracks usage via `resource.NewProviderConfigUsageTracker` and extracts creds via `resource.CommonCredentialExtractor`.
  - `external` implements `managed.TypedExternalClient[*v1alpha1.<Kind>]` — `Observe`, `Create`, `Update`, `Delete`, `Disconnect`. Holds the Strapi API client.
  - `WithEventFilter(resource.DesiredStateChanged())` is applied to the controller builder so spec-only changes drive reconciliation.

**Resource scope.** The namespaced `ProviderConfig` is namespace-scoped; `ClusterProviderConfig` is cluster-scoped. New managed resources should generally be namespace-scoped (matching `MyType`'s previous template); for namespace-scoped MRs the namespaced `ProviderConfig` lookup must include the resource's namespace (see `cr.GetNamespace()` usage in connector code).

## Code generation invariants

- All `zz_generated.*.go` files are produced by `make generate` — never edit by hand. The lint config and goimports both ignore them.
- Adding/removing kubebuilder markers, struct fields, or new types in `apis/` requires re-running `make generate` (or `make reviewable`) before commit; the CRDs in `package/crds/` and the managed-resource methodsets must stay in sync with the Go types.
- `hack/boilerplate.go.txt` is prepended to every generated file.

## Linter conventions

`.golangci.yml` enables a broad set (errorlint, gosec, gocyclo, prealloc, etc.). Notable settings: `gocyclo` complexity 10, `goimports` local prefix `github.com/web-seven/provider-strapi`, test files relax `dupl/errcheck/gocyclo/gosec/unparam`. Generated files and `examples/` are excluded.
