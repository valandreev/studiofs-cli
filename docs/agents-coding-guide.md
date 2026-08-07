# StudioFS Agent Implementation Guide

This file stores detailed project instructions that do not belong in the root `AGENTS.md`.

## Repository structure

- Workspace root: `studiofs-cli/`
- Core crates: `studiofs`, `studiofs-meta`, `studiofs-data`, `studiofs-cache`, `studiofs-vfs`, `studiofs-macfuse`, `studiofs-winfsp`

## Naming conventions

- Crates: `studiofs-*`
- Modules: snake_case (`cli/mount.rs`)
- Types: PascalCase (`StudioVfs`, `VfsConfig`)
- Functions/variables: snake_case (`parse_size`, `max_dirty_bytes`)
- Constants: SCREAMING_SNAKE_CASE (`DYNAMIC_L1_LIMIT_CAP_BYTES`)

## Rust conventions

- Use Rust 2024 conventions.
- Workspace lint policy lives in the root `[workspace.lints]` tables in
  `Cargo.toml`. Every workspace member manifest must opt in with:
  ```toml
  [lints]
  workspace = true
  ```
- Keep the shared lint baseline conservative and low-noise. Do not turn on
  broad Clippy groups such as `all`, `pedantic`, `unwrap_used`, or
  `expect_used` across the workspace unless that cleanup is planned and
  validated separately.
- Production no-panic policy is enforced incrementally by
  `crates/studiofs/tests/production_lint_policy_test.rs`. It scans scoped
  production runtime files, strips `#[cfg(test)]` items, and requires any
  remaining `unwrap`, `expect`, or `panic!` site to carry a local
  `#[expect(clippy::<lint>, reason = "...")]` rationale. Add new production
  files to that test as they are cleaned up; do not expand it to test helpers
  or fixture-only sources.
- Crates without unsafe requirements should use `#![forbid(unsafe_code)]`.
  Bridge crates that are platform or FFI boundaries must keep unsafe scopes
  narrow and document each boundary with `SAFETY:` comments when unsafe code is
  introduced.
- Prefer domain errors with `thiserror` for library code:
  - `pub enum <ErrorName> { ... }`
- Use `Result<T, E>` for recoverable failures, avoid panics in library logic.
- Prefer `Arc<T>` for shared state in async boundaries.
- Prefer `parking_lot` synchronization primitives.
- Use `tokio` for async workloads.
- Prefer `async fn` (where possible) over trait-based async wrappers.
- Use explicit `Send + Sync` bounds for cross-thread types.
- Prefer `Release`/`Acquire` atomics for correctness-critical state transitions; use `Relaxed` elsewhere.

## Error and async examples

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VfsError {
    #[error("file not found: {0}")]
    NotFound(String),

    #[error("permission denied")]
    AccessDenied,
}
```

Do not use `anyhow` as the primary error type in library code.

## Platform-specific patterns

Use the narrowest target conditions possible:

```rust
#[cfg(windows)]
const PLATFORM: &str = "windows";

#[cfg(target_os = "macos")]
const PLATFORM: &str = "macos";

#[cfg(all(unix, not(target_os = "macos")))]
const PLATFORM: &str = "linux";
```

Prefer `#[cfg(windows)]`/`#[cfg(target_os = "linux")]`/`#[cfg(target_os = "macos")]` when behavior is OS-specific.

## Commit / PR conventions

- Commit message format: `<type>: <subject>`
  - `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `ci`
- PR checklists and quality gates are defined in `docs/contributing.md`.

## VFS state decomposition

`VfsState` is decomposed into explicit subsystems (example groups):

- `writer`
- `cached_store`
- `upload`
- `file_session`
- `namespace`
- `acl`
- `incarnation`
- `read`
- `file_lock`
- `shutdown`
- `metrics`

Access pattern: use `self.state.<subsystem>.<field>` instead of direct monolithic fields.

### Lock ordering

Higher priority (lower tier number) must be acquired first:

1. `ShutdownState`
2. Writer/cache dirty-state owners
3. `NamespaceState`
4. `IncarnationState`
5. `UploadState`
6. `AclState`
7. `FileLockState`
8. `MetricsState`

## Local-only durability contracts

- `write` accepts bytes into file/chunk/slice writer state and does not wait for
  upload or PostgreSQL replication.
- `flush` admits the dirty state to the bounded volatile worker and does not
  wait for a redb durability barrier.
- `fsync` is the strict caller-requested local sync API and must not imply remote publication.
- WinFSP `cleanup`:
  - WinFSP `FlushAndPurgeOnCleanup` is disabled by default; write-capable
    handles use `Create.Opened.DisableCache` so writes bypass the OS file cache.
    Read-only handles normally keep WinFSP cache-manager behavior. Remote
    same-inode content changes advance the WinFSP file identity via committed
    content epoch so cached file data is not reused for a newer published
    payload.
  - delete + `FspCleanupDelete` finalizes deletion.
  - non-delete StudioFS cleanup callback handling is observational; it should
    not be the ownership point for dirty local persistence.
- `overwrite`, WinFSP `set_file_size`, and size-changing FUSE paths must persist new EOF/attrs before returning.
- `release` (FUSE) and `close`/`release_fh` (WinFSP) admit remaining local
  writer state without moving remote publication or redb durability to the
  foreground; strict callers use `fsync` or shutdown.
- Treat repeated/non-delete cleanup foreground barriers in sequential-write Windows validation (`cleanup_skipped_clean`, `cleanup_flush_fh`) as signs of avoidable callback work (SFS-192 follow-up signal).
