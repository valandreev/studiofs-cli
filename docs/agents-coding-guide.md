# StudioFS Agent Implementation Guide

This file stores detailed project instructions that do not belong in the root `AGENTS.md`.

## Repository structure

- Workspace root: `studiofs-cli/`
- Core crates: `studiofs`, `studiofs-meta`, `studiofs-data`, `studiofs-cache`, `studiofs-vfs`, `studiofs-fuse`, `studiofs-winfsp`

## Naming conventions

- Crates: `studiofs-*`
- Modules: snake_case (`cli/mount.rs`)
- Types: PascalCase (`StudioVfs`, `VfsConfig`)
- Functions/variables: snake_case (`parse_size`, `max_dirty_bytes`)
- Constants: SCREAMING_SNAKE_CASE (`DYNAMIC_L1_LIMIT_CAP_BYTES`)

## Rust conventions

- Use Rust 2024 conventions.
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

## VFS state decomposition (SFS-96)

`VfsState` is decomposed into explicit subsystems (example groups):

- `upload`
- `file_session`
- `namespace`
- `acl`
- `incarnation`
- `local_dirty`
- `read`
- `file_lock`
- `shutdown`
- `metrics`

Access pattern: use `self.state.<subsystem>.<field>` instead of direct monolithic fields.

### Lock ordering

Higher priority (lower tier number) must be acquired first:

1. `ShutdownState`
2. `local_dirty`
3. Reserved `DirtyBufferState`
4. Reserved legacy `IngressState`
5. `NamespaceState`
6. `IncarnationState`
7. `UploadState`
8. `AclState`
9. `FileLockState`
10. `MetricsState`

## Local-only durability contracts (SFS-250)

- `flush` remains the explicit caller-requested local-durability barrier for dirty unpublished state.
- `fsync` is the strict caller-requested local sync API and must not imply remote publication.
- WinFSP `cleanup`:
  - delete + `FspCleanupDelete` finalizes deletion.
  - non-delete cleanup is observational; it should not be the ownership point for dirty local persistence.
- `overwrite`, WinFSP `set_file_size`, and size-changing FUSE paths must persist new EOF/attrs before returning.
- `release` (FUSE) and `close`/`release_fh` (WinFSP) are release-path checks; they must reach strict local durability when required without moving remote publication to the foreground.
- Treat repeated/non-delete cleanup foreground barriers in sequential-write Windows validation (`cleanup_skipped_clean`, `cleanup_flush_fh`) as signs of avoidable callback work (SFS-192 follow-up signal).