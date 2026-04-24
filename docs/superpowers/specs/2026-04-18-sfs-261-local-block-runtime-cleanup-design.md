# SFS-261 Local Block Runtime Cleanup Design

## Context

`SFS-256` through `SFS-260` established the new production data-plane runtime around a file-per-block local backend, persistent index, simplified read/write paths, index-driven upload recovery, and one shared cache budget. `SFS-261` is the cleanup step that removes superseded production code, tests, docs, and operator-facing terminology so the repository teaches and exposes one active runtime model.

The canonical rewrite contract remains [docs/juicefs-local-first-rust-contract.md](../../juicefs-local-first-rust-contract.md). This cleanup must preserve the current on-disk subtree shape under `<cache-root>/v3/**` as an implementation detail while removing `cache-v3` as the product/runtime identity in public surfaces.

## Goal

Leave one canonical local-block runtime in the repository by:

- keeping `crates/studiofs-cache/src/runtime.rs` plus `block_*` and `startup_recovery.rs` as the only active implementation;
- deleting the duplicated `crates/studiofs-cache/src/v3/**` module tree;
- renaming public and internal exposed runtime surface from `CacheV3*`/`cache-v3` to `LocalPayload*` or neutral local-runtime wording where appropriate;
- rewriting active docs and tests so they describe the runtime as a local payload runtime whose current disk subtree happens to be `v3/`.

## Non-Goals

- no on-disk format migration away from `v3/`;
- no change to startup recovery, read/write ordering, upload lifecycle, or mounted behavior;
- no broad architectural refactor outside runtime naming cleanup and duplicate-code removal;
- no full-workspace verification sweep in this task.

## Accepted Constraints

### Compatibility

- The mounted runtime must continue to detect and provision `v3/LAYOUT_V1`.
- The payload and index locations remain `payload/objects/**` and `index/cache.redb`.
- Existing mounts and restart/recovery flows must continue to work without cache migration.

### Vocabulary

- User-facing and operator-facing wording should prefer `local payload runtime`, `local cache runtime`, `local backend`, or similarly neutral terms.
- The literal string `cache-v3` may remain only where it describes the current disk subtree or legacy compatibility details that are still true.
- Public API names should stop advertising `CacheV3` when the API is the canonical current runtime.

## Design

### 1. Canonical Implementation Boundary

`crates/studiofs-cache/src/runtime.rs` is the canonical runtime implementation and owns:

- backend detection;
- backend kind reporting;
- runtime config and errors;
- active local payload backend implementation;
- startup reconciliation wiring.

`crates/studiofs-cache/src/payload_layout.rs`, `payload_store.rs`, `payload_index.rs`, `payload_eviction.rs`, `payload_platform.rs`, and `startup_recovery.rs` remain the implementation modules beneath that runtime.

The duplicated `crates/studiofs-cache/src/v3/**` module tree is removed. Any remaining callers must use `studiofs_cache::runtime` or top-level re-exports from `studiofs_cache`.

### 2. Naming Cleanup

The public `studiofs-cache` surface is renamed from `CacheV3*` to `LocalPayload*` wherever the type refers to the active runtime rather than to the disk subtree detail.

Representative targets:

- `CacheV3LocalPayloadBackend` -> `LocalPayloadBackendRuntime`
- `CacheV3LocalPayloadBackendConfig` -> `LocalPayloadBackendConfig`
- `CacheV3LocalPayloadBackendError` -> `LocalPayloadBackendOpenError`
- `CacheV3PayloadSyncMode` -> `LocalPayloadSyncMode`
- `PreparedCacheV3Runtime` -> `PreparedLocalPayloadRuntime`
- `PrepareCacheV3RuntimeError` -> `PrepareLocalPayloadRuntimeError`
- `prepare_cache_v3_runtime()` -> `prepare_local_payload_runtime()`

Where compatibility aliases materially reduce churn inside the current change set, they may remain temporarily inside `studiofs-cache::runtime` or `studiofs-cache::lib`, but the rest of the tree should stop depending on them.

### 3. Detection and Status Surface

Detection continues to inspect the current disk subtree, but the API should describe it generically:

- `CacheBackendKind` becomes a neutral runtime kind type or a single current-runtime label;
- detection predicates stop centering `has_cache_v3_contract()` and instead use neutral names such as `has_runtime_layout()` or `has_local_payload_layout()`;
- `status`, `janitor`, mount bootstrap, and VFS diagnostics stop reporting the backend version as `cache-v3`.

The user-visible message should become “current local payload runtime” or similar, with an optional implementation-detail line noting the `v3/` subtree when useful.

### 4. Docs Cleanup

Active documentation should describe one runtime model:

- keep [docs/architecture.md](../../architecture.md) aligned with the canonical contract and current implementation;
- replace or rewrite [docs/local-block-runtime-layout.md](../../local-block-runtime-layout.md) so it no longer presents `cache-v3` as the runtime identity;
- keep explicit mention that the current disk layout still lives under `v3/` as an implementation detail;
- preserve real operational guidance for readonly inspection, startup reconciliation scope, and stale-artifact remediation.

If the old document title itself becomes misleading, the content should move to a new local-runtime document and the stale file should be removed.

### 5. Tests

Targeted tests should guard the cleanup:

- `studiofs-cache` tests prove the renamed runtime API still provisions and opens the current layout;
- CLI/VFS tests use the neutral runtime names and wording;
- vocabulary regression tests keep active docs/CLI surfaces free of superseded runtime identity wording while allowing the retained disk-layout detail where needed.

No test should assert that the product runtime identity is `cache-v3`.

## Implementation Slices

1. Rename the canonical `studiofs-cache` API and move remaining callers onto it.
2. Remove the duplicate `src/v3/**` module tree and any imports that still depend on it.
3. Update CLI/VFS detection, diagnostics, and tests to the neutral runtime vocabulary.
4. Rewrite active docs and vocabulary guards to match the cleaned-up architecture.

## Risks

### Risk: overly aggressive wording cleanup hides a necessary disk-layout detail

Mitigation:

- keep `v3/` called out explicitly in layout/documentation code that describes disk paths;
- distinguish “runtime identity” from “current subtree path”.

### Risk: delete duplicated modules before all callers are moved

Mitigation:

- perform caller migration first;
- run targeted crate tests after each slice;
- use compile errors as the final sweep for stale imports.

### Risk: regression tests still intentionally encode `cache-v3` tags

Mitigation:

- update observability/test fixtures in the same slice as runtime naming cleanup;
- narrow vocabulary tests so they permit implementation-detail mentions only in the few files that must retain them.

## Verification Strategy

Run only targeted commands in this task:

- focused `cargo test` invocations for `studiofs-cache`, `studiofs`, and selected `studiofs-vfs` tests that cover runtime bootstrap/status/janitor;
- `cargo fmt`;
- a narrow `cargo clippy` invocation only if needed for touched crates and only if it stays reasonably scoped.

The user should run the full workspace validation afterwards.
