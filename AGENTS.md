# Repository Guidelines

## Project Structure & Module Organization

StudioFS is a Rust workspace with the following structure:

```
studiofs-cli/
├── Cargo.toml              # Workspace root with dependency overrides
├── AGENTS.md               # This file
└── crates/
    ├── studiofs/           # CLI binary (main.rs)
    ├── studiofs-meta/      # Metadata engine (PostgreSQL + Redb)
    ├── studiofs-data/      # S3 object storage layer
    ├── studiofs-cache/     # Block caching layer
    ├── studiofs-vfs/       # Virtual File System implementation
    ├── studiofs-fuse/      # Unix FUSE bridge
    └── studiofs-winfsp/    # Windows WinFSP bridge
```

### Naming Conventions

- **Crates**: `studiofs-*` prefix (e.g., `studiofs-meta`, `studiofs-vfs`)
- **Modules**: snake_case (e.g., `cli/mount.rs`, `engine/postgres.rs`)
- **Types**: PascalCase (e.g., `StudioVfs`, `VfsConfig`)
- **Functions/Variables**: snake_case (e.g., `parse_size`, `max_dirty_bytes`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `DYNAMIC_L1_LIMIT_CAP_BYTES`)

## Build, Test, and Development Commands

### Build

```bash
# Debug build (default)
cargo build

# Release build (optimized)
cargo build --release

# Build specific crate
cargo build -p studiofs
```

### Test

```bash
# Run all tests in workspace
cargo test --workspace

# Run tests for specific crate
cargo test -p studiofs

# Run only library tests
cargo test --workspace --lib

# Build tests without running (for debugging)
cargo test --workspace --no-run
```

### Linting & Formatting

```bash
# Check formatting (run in CI)
cargo fmt --check

# Apply formatting
cargo fmt

# Run Clippy linter
cargo clippy --workspace -- -D warnings

# Run Clippy on specific crate
cargo clippy -p studiofs
```

## Coding Style & Naming Conventions

### General Principles

- **Use `thiserror` for domain errors** (not `anyhow` for library code)
- **Prefer `Result<T, E>` over panics** for recoverable errors
- **Use `Arc<T>` for shared state** (e.g., `Arc<dyn AsyncMetaEngine>` at async boundaries, `Arc<dyn MetaEngine>` for sync metadata internals)
- **Use ` parking_lot` for synchronization primitives** (faster than std)
- **Follow Rust edition 2024** conventions

### Error Handling

```rust
// Good: Domain-specific error types
#[derive(thiserror::Error, Debug)]
pub enum VfsError {
    #[error("file not found: {0}")]
    NotFound(String),
    #[error("permission denied")]
    AccessDenied,
}

// Bad: Using anyhow for library errors
pub fn read_file() -> anyhow::Result<String> { /* ... */ }
```

### Async Patterns

- **Use `tokio` runtime** for async operations
- **Prefer `async fn` over `async-trait`** where possible
- **Use `Send + Sync` bounds** explicitly for cross-thread types

### Platform-Specific Code

```rust
// Windows-specific code
#[cfg(windows)]
mod winfsp_bootstrap;

// Unix-specific code
#[cfg(unix)]
mod mount_profile;

// Platform detection
#[cfg(windows)]
const PLATFORM: &str = "windows";

#[cfg(target_os = "macos")]
const PLATFORM: &str = "macos";

#[cfg(all(unix, not(target_os = "macos")))]
const PLATFORM: &str = "linux";
```

## Testing Guidelines

### Test Structure

Tests are organized by crate:

- **Unit tests**: In `src/` alongside source files
- **Integration tests**: In `tests/` directory per crate
- **Ignored tests**: Marked with `#[ignore]` (require S3 + PostgreSQL)

### Running Tests

```bash
# Run fast-only tests
cargo test -p studiofs --test destroy_test

# Run infra-backed tests (requires S3 + PostgreSQL)
cargo test -p studiofs --test destroy_test -- --ignored --nocapture --test-threads=1
```

### Test Naming

- **Test functions**: `test_<what>_<condition>` (e.g., `test_create_write_read_file`)
- **Integration tests**: `<feature>_test.rs` (e.g., `destroy_test.rs`, `vfs_test.rs`)

## Commit & Pull Request Guidelines

### Commit Message Format

```
<type>: <subject>

[optional body]
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Test additions/changes
- `perf`: Performance improvements
- `ci`: CI/CD changes

**Examples**:
```
feat: add runtime S3 endpoint override for mount command (SFS-95)
fix: restore prefetch acquire wait default to 75ms
docs: update docs for transit memory budget behavior
test: add destroy tests fixes
```

### Pull Request Requirements

1. **Title**: Follow commit message format
2. **Description**: Include context, changes, and testing notes
3. **Issues**: Link related GitHub issues
4. **Screenshots**: Include for UI/UX changes
5. **Tests**: Add/modify tests for new/changed functionality

### Pull Request Checklist

- [ ] Code compiles without warnings (`cargo clippy -- -D warnings`)
- [ ] Tests pass (`cargo test --workspace`)
- [ ] Formatting applied (`cargo fmt`)
- [ ] Documentation updated (README, AGENTS.md, inline docs)
- [ ] Changelog entry added (if user-facing)

## Architecture Overview

### High-Level Components

```
┌─────────────────────────────────────────────────────────────┐
│                      CLI (studiofs)                         │
│  Commands: init, discover, mount, umount, status, janitor   │
└─────────────────────────────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │  StudioVFS  │ │  FUSE/Bridge│ │ WinFSP      │
    │  (VFS impl) │ │  (Unix)     │ │  (Windows)  │
    └─────────────┘ └─────────────┘ └─────────────┘
            │               │               │
            └───────────────┼───────────────┘
                            ▼
            ┌──────────────────────────────┐
            │  Metadata Engine             │
            │  - PostgreSQL (shared)       │
            │  - Redb (local cache)        │
            └──────────────────────────────┘
                            │
                            ▼
            ┌──────────────────────────────┐
            │  Data Store (S3)             │
            │  - Object storage layer      │
            │  - Block caching             │
            └──────────────────────────────┘
```

### Data Flow

1. **Write Path**: Application → VFS → Block staging → Ingress journal → S3 upload
2. **Read Path**: Application → VFS → Cache lookup → S3 fetch (if miss) → Response
3. **Sync Path**: VFS → PostgreSQL delta batches → Remote clients invalidate cache

### L2 Read Priority Policy (SFS-110)

- `L2IoPriority::Foreground`: no artificial 1MB throttling in the block read hot path.
- `L2IoPriority::Background`: keeps 1MB chunked reads to reduce queue starvation risk for foreground I/O.
- `L2DiskCache::get_sync` defaults to `Background` for safer generic sync usage.
- `BlockCache` user-facing sync fast paths explicitly request `Foreground` via
  `get_sync_with_priority(..., L2IoPriority::Foreground)`.

### Environment Variables

Tunable at runtime (see `crates/studiofs/src/cli/mount.rs` for full list):

- `STUDIOFS_MAX_DIRTY_BYTES`: Memory budget for dirty blocks
- `STUDIOFS_MAX_SPILL_BYTES`: Disk budget for spill-backed writes
- `STUDIOFS_S3_ENDPOINT`: S3 endpoint URL
- `STUDIOFS_S3_BUCKET`: S3 bucket name
- `STUDIOFS_PRINCIPAL`: ACL principal for access control
- `STUDIOFS_CACHE_SAFETY_MODE`: `fast` or `durable` (default: `fast`)

## Security & Configuration Tips

### Environment Variables

- **Never commit `.env` files** (add to `.gitignore`)
- **Use `dotenvy` for local development** only
- **Prefer explicit configuration** for production deployments

### S3 Security

- Use **IAM roles** where possible (avoid hardcoded credentials)
- Enable **bucket encryption** (SSE-S3 or SSE-KMS)
- Configure **versioning** for crash recovery

### PostgreSQL Security

- Use **SSL/TLS connections** (`?sslmode=require`)
- Restrict **network access** to application servers
- Enable **audit logging** for compliance

## Agent-Specific Instructions

### For AI Code Assistants

1. **Read AGENTS.md first** before making changes
2. **Follow existing patterns** in `crates/studiofs/src/`
3. **Use `cargo clippy -- -D warnings`** to catch common mistakes
4. **Run `cargo fmt`** before committing
5. **Test platform-specific code**:
   - Windows: `cargo test -p studiofs-winfsp`
   - Unix: `cargo test -p studiofs-fuse`
6. **Update AGENTS.md** when adding new conventions or patterns

## VfsState Decomposition (SFS-96)

### Overview

The monolithic `VfsState` struct (68 fields) was decomposed into 11 cohesive, independently lockable subsystems to improve maintainability, reduce lock contention, and clarify ownership boundaries.

### Subsystems

```
VfsState (composed of 11 subsystems)
├── ingress: IngressState          # Ingress WAL, spill tracking, segment management
├── dirty_buffer: DirtyBufferState # Dirty memory buffer, spill state, drain coordination
├── upload: UploadState            # Upload queue, cancellation, pending evictions
├── namespace: NamespaceState       # Dentry cache, path shortcuts, attr cache
├── acl: AclState                  # ACL state (minimal, placeholder)
├── incarnation: IncarnationState  # Inode incarnation tracking
├── write_buffer: WriteBufferState # Pending blocks for writes
├── read: ReadState                # Foreground read tracking, P90 latency, prefetch hints
├── file_lock: FileLockState       # File locking state
├── shutdown: ShutdownState        # Shutdown flags, tokens, task handles
└── metrics: MetricsState          # Runtime metrics and counters
```

### Lock Ordering Policy

To prevent deadlocks, a strict lock ordering is enforced (Tier 0 = highest priority):

1. **Tier 1**: `ShutdownState` — shutdown flags are checked before any other operation
2. **Tier 2**: `WriteBufferState` — pending write blocks
3. **Tier 3**: `DirtyBufferState` — dirty memory buffer and spill state
4. **Tier 4**: `IngressState` — ingress WAL and spill entries
5. **Tier 5**: `NamespaceState` — dentry/attr caches
6. **Tier 6**: `IncarnationState` — inode incarnation tracking
7. **Tier 7**: `UploadState` — upload queue and cancellations
8. **Tier 8**: `AclState` — ACL state
9. **Tier 9**: `FileLockState` — file locks
10. **Tier 10**: `MetricsState` — metrics (rarely needs locking)

### VfsState::new Pattern

The `VfsState::new()` constructor accepts `&VfsConfig` for cache/limit parameters rather than individual arguments:

```rust
fn new(
    recovered_spill: RecoveredSpillMap,
    recovered_spill_bytes: usize,
    active_segment_id: u64,
    recovered_spill_seq: u64,
    initial_checkpoint_meta: Option<(u64, u64, u64)>,
    cfg: &VfsConfig,
) -> Self
```

### Key Patterns

1. **State access**: Use `self.state.subsystem.field` instead of direct fields on `VfsState`
2. **Lock release before IO**: Always release mutex locks before performing IO operations (file deletions, WAL appends)
3. **Batch operations**: Use batch methods like `remove_spill_entries_batch()` for efficiency
4. **Atomic ordering**: Use `Release`/`Acquire` for critical data (throttling metrics), `Relaxed` for non-critical counters
5. **notify_waiters outside lock**: Always call `notify_waiters()` after releasing the mutex to avoid lock contention
