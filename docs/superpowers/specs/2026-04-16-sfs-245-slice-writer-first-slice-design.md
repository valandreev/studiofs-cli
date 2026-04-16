# SFS-245 Slice Writer First Slice Design

## Goal

Replace the current `WriteBuffer`-driven RAM write path with Rust-native per-handle writer state that:

- makes same-mount reads resolve from active handle state first
- materializes durable local unpublished slices on `flush`, `fsync`, and last-handle release
- reuses the `SFS-244` chunk/slice/block metadata API directly instead of preserving the old flat write pipeline

This document defines the first implementation slice for `SFS-245`. It is intentionally narrow enough to land safely while still changing the architectural source of truth for foreground writes.

## Why This Slice

`SFS-244` is now in place. The repo already has:

- chunk/slice/block metadata lookups through `lookup_regular_file_slices`
- atomic overlay writes through `flush_regular_file_overlay_slices_with_attr`
- read-path resolution that can already merge local unpublished and published metadata

The remaining architectural problem is that accepted writes still enter the system through [`crates/studiofs-vfs/src/write_buffer.rs`](../../../crates/studiofs-vfs/src/write_buffer.rs), and same-mount read-after-write is partially preserved by flushing buffered dirty data at read start instead of consulting explicit writer state.

## Approaches Considered

### 1. Recommended: per-handle writer state plus explicit materialization bridge

Introduce handle-owned slice-writer state, teach read-path to consult it first, and materialize it into durable local unpublished slices only at the documented barriers.

Why this wins:

- aligns with the canonical contract in `docs/juicefs-local-first-rust-contract.md`
- removes the biggest semantic mismatch first: read-after-write from implicit flushing
- lets `SFS-244` metadata APIs become the durable state boundary immediately

Trade-off:

- some transitional code remains while `WriteBuffer` callers are retired

### 2. Per-inode shared writer state

Keep one mutable writer owner per inode and let all handles write into it.

Why not now:

- simpler wiring, but weaker handle isolation
- increases ambiguity around append ordering, close semantics, and future concurrent-handle rules

### 3. Full write-path rewrite in one cutover

Delete `WriteBuffer`, upload handoff glue, and barrier plumbing in one pass.

Why not now:

- too much surface area for one safe step
- harder to preserve Windows/FUSE barrier guarantees while the new writer semantics settle

## Scope

### In scope

- new handle-scoped writer runtime in `studiofs-vfs`
- read-path precedence: active handle writer state before durable local unpublished state
- barrier-driven materialization to `flush_regular_file_overlay_slices_with_attr`
- `flush`, `fsync`, and last-handle `release_fh` semantics expressed through the new writer state
- targeted retirement of read-start eager draining that exists only to preserve same-mount visibility

### Out of scope for this slice

- upload pipeline redesign
- compaction redesign
- full deletion of every helper in `write_buffer.rs`
- cross-handle merge policy beyond deterministic append/overwrite correctness for one active writer handle

## Proposed Architecture

### 1. `HandleWriterRegistry`

Add a VFS-owned registry keyed by file handle:

- owns the active writer object for each writable handle
- supports lookup by `fh` for write, flush, fsync, release, and same-handle reads
- supports inode-scoped iteration so read paths can gather all active writer spans for the current mount

This runtime object becomes the authoritative owner of RAM-only accepted writes. `WriteBuffer` stops being the canonical source of pending foreground state.

### 2. `HandleSliceWriter`

Each writable handle gets a state machine with:

- logical file size view
- ordered pending slices keyed by chunk-relative range
- immutable block payload fragments staged in memory
- append/overwrite/truncate bookkeeping
- a `materialize()` operation that turns pending handle slices into durable local unpublished `SliceRecord`s

The writer state is Rust-native:

- explicit enums for `Dirty`, `Materializing`, `Clean`, and `StickyError`
- no goroutine-shaped commit topology copied from Go
- ownership stays local to the handle registry plus explicit materialization calls

### 3. Materialization Boundary

`materialize()` performs the local durability boundary:

1. freeze current dirty slices
2. allocate object ids for any new immutable payload blocks
3. persist payloads to local block storage
4. call `flush_regular_file_overlay_slices_with_attr`
5. mark the writer state clean, or sticky-error on failure

After step 4 succeeds, the bytes live in durable local unpublished state and normal metadata-backed reads can find them even after the handle closes.

### 4. Read Resolution

Foreground reads become:

1. active handle writer spans
2. other current-mount active writer spans for the same inode, ordered newest-first
3. durable local unpublished slices from metadata
4. published base slices
5. holes

For this first slice, the key behavior change is that `read_ordered()` no longer needs to trigger `flush_dirty_write_buffer_for_read_start()` just to preserve same-mount visibility.

## Data Flow

### Write

`write_fh_with_local_sync_policy(fh, ino, offset, data, policy)`:

1. resolve the writer for `fh`
2. split data into chunk-local pending slices
3. update writer-local logical size and dirty spans
4. if `policy == Strict`, materialize immediately
5. otherwise return once writer state reflects the accepted bytes

### Flush / Fsync

`flush_fh`, `flush_inode_barrier_with_outcome`, and `fsync_with_outcome`:

- materialize all dirty writer state for the inode
- persist attrs as they do today
- preserve the existing rule that success means local durability, not remote publication

### Release

Last-handle release:

- materializes any remaining dirty writer state first
- then runs existing sticky-error and orphan cleanup logic

This keeps the WinFSP/FUSE guarantee that the final release path owns the fallback local durability barrier when explicit flush did not already do so.

## Error Handling

- writer-local failures become sticky per handle and surface on flush/fsync/release
- materialization failures must not silently drop accepted RAM-only bytes
- once materialization succeeds, later upload/publication failures remain upload-lifecycle concerns, not writer-state concerns

## Testing Strategy

Targeted tests only. No full-suite run in this task.

### New regression coverage

- same-handle read-after-write succeeds without a read-start flush
- reopened reader after `flush_fh` sees bytes through metadata-backed local unpublished slices
- last-handle release materializes dirty writer state when no explicit flush occurred
- append, overwrite, sparse overwrite, and cross-chunk writes produce correct `SliceRecord` ranges
- sticky materialization error is preserved and reported by the next barrier

## Exit Criteria For This Slice

This first slice is complete when:

- writable handles use explicit writer state instead of `WriteBuffer` as the foreground source of truth
- read-after-write no longer depends on `flush_dirty_write_buffer_for_read_start`
- flush/fsync/release materialize writer state through `flush_regular_file_overlay_slices_with_attr`
- targeted tests pass for same-mount visibility and barrier durability semantics

## Follow-on Work

After this lands, the remaining `SFS-245` work should:

- remove the superseded `WriteBuffer` data path entirely
- simplify eager flush/backpressure plumbing around the old buffer model
- align upload handoff to the new writer/materialization lifecycle
