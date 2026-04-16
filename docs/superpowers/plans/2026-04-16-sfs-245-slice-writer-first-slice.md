# SFS-245 Slice Writer First Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce per-handle slice-writer state for foreground writes and barrier-driven local materialization on top of the `SFS-244` chunk/slice/block metadata API.

**Architecture:** Add a handle-owned writer registry inside `studiofs-vfs`, route write/read/barrier paths through it, and materialize durable local unpublished slices through `flush_regular_file_overlay_slices_with_attr`. Keep the first slice narrow: replace the foreground source of truth first, then retire the read-start `WriteBuffer` dependency and last-handle barrier behavior around it.

**Tech Stack:** Rust 2024, Tokio, parking_lot, StudioFS VFS runtime, `studiofs-meta` regular-file slice APIs.

---

### Task 1: Lock The First Regression

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn same_handle_read_after_buffered_write_does_not_require_read_start_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-same-handle-read");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sfs-245.txt", 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"slice-writer-visible";

        vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();

        let bytes = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs-vfs same_handle_read_after_buffered_write_does_not_require_read_start_flush -- --exact`

Expected: FAIL because the current read path still depends on the old buffered flush behavior instead of explicit handle writer state.

- [ ] **Step 3: Write minimal implementation**

```rust
// Wire the read path to consult active handle writer state before metadata
// resolution. The first green version can be narrow: only cover the active
// writable handle case used by the test above.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs-vfs same_handle_read_after_buffered_write_does_not_require_read_start_flush -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/tests.rs crates/studiofs-vfs/src/vfs/io_ops.rs crates/studiofs-vfs/src/vfs/read_path.rs
git commit -m "test: lock same-handle writer visibility for SFS-245"
```

### Task 2: Add Handle Writer Runtime

**Files:**
- Create: `crates/studiofs-vfs/src/vfs/handle_writer.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/handle_table.rs`
- Modify: `crates/studiofs-vfs/src/vfs/file_session.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn handle_writer_registry_tracks_dirty_and_clean_handle_state() {
    let registry = HandleWriterRegistry::new(512 * 1024);
    let fh = 42;
    let ino = 7;

    registry.open_writer(fh, ino);
    registry.write(fh, 0, b"abc").unwrap();
    assert!(registry.inode_has_dirty_writers(ino));

    let materialized = registry.take_dirty_materialization(fh).unwrap();
    assert_eq!(materialized.logical_size, 3);
    assert!(!materialized.pending_slices.is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs-vfs handle_writer_registry_tracks_dirty_and_clean_handle_state -- --exact`

Expected: FAIL with missing `HandleWriterRegistry`.

- [ ] **Step 3: Write minimal implementation**

```rust
pub(crate) struct HandleWriterRegistry {
    block_size: u32,
    writers: parking_lot::Mutex<std::collections::HashMap<u64, HandleSliceWriter>>,
}

pub(crate) struct MaterializeHandleSnapshot {
    pub(crate) ino: Ino,
    pub(crate) logical_size: u64,
    pub(crate) pending_slices: Vec<PendingWriterSlice>,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs-vfs handle_writer_registry_tracks_dirty_and_clean_handle_state -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/handle_writer.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat: add handle writer runtime for SFS-245"
```

### Task 3: Route Handle Writes Through The New Runtime

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/io_ops.rs`
- Modify: `crates/studiofs-vfs/src/vfs/sync_ops.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn write_fh_populates_handle_writer_state_before_barrier() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-write-fh");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"writer-state.txt", 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh(fh, attr.ino, 5, b"xyz").await.unwrap();

        assert!(vfs.handle_writer_registry().inode_has_dirty_writers(attr.ino));
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs-vfs write_fh_populates_handle_writer_state_before_barrier -- --exact`

Expected: FAIL because `write_fh` still writes into `OrderedWriteSession`.

- [ ] **Step 3: Write minimal implementation**

```rust
pub async fn write_fh_with_local_sync_policy(...) -> VfsResult<u32> {
    self.handle_writers.write(fh, ino, offset, data, local_sync_policy)?;
    if matches!(local_sync_policy, LocalSyncPolicy::Strict) {
        self.materialize_handle_writer_for_fh(fh).await?;
    }
    Ok(data.len() as u32)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs-vfs write_fh_populates_handle_writer_state_before_barrier -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/io_ops.rs crates/studiofs-vfs/src/vfs/sync_ops.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat: route handle writes through slice writer state"
```

### Task 4: Materialize Writer State Into SFS-244 Metadata

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/handle_writer.rs`
- Modify: `crates/studiofs-vfs/src/write_buffer.rs`
- Modify: `crates/studiofs-vfs/src/runtime.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn flush_fh_materializes_handle_writer_state_into_local_unpublished_slices() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-flush-materialize");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"flush-materialize.txt", 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"flush-me";

        vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();
        vfs.flush_fh(fh).await.unwrap();

        let spans = meta.lookup_regular_file_slices(attr.ino, 0, payload.len() as u64).unwrap();
        assert!(!spans.is_empty());
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs-vfs flush_fh_materializes_handle_writer_state_into_local_unpublished_slices -- --exact`

Expected: FAIL because `flush_fh` only knows how to drain the old write buffer.

- [ ] **Step 3: Write minimal implementation**

```rust
async fn materialize_handle_writer_for_fh(&self, fh: u64) -> VfsResult<()> {
    let snapshot = self.handle_writers.take_dirty_materialization(fh)?;
    let chunk_slices = build_slice_records_from_snapshot(&snapshot)?;
    run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
        engine.flush_regular_file_overlay_slices_with_attr(snapshot.ino, &chunk_slices, None)
    }).await?;
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs-vfs flush_fh_materializes_handle_writer_state_into_local_unpublished_slices -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/handle_writer.rs crates/studiofs-vfs/src/write_buffer.rs crates/studiofs-vfs/src/runtime.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat: materialize handle writer state into overlay slices"
```

### Task 5: Retire Read-Start Flush Dependence

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/io_ops.rs`
- Modify: `crates/studiofs-vfs/src/vfs/read_path.rs`
- Modify: `crates/studiofs-vfs/src/write_buffer.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn read_ordered_does_not_call_write_buffer_flush_for_active_handle_visibility() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-read-no-flush");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"read-no-flush.txt", 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh(fh, attr.ino, 0, b"abcdef").await.unwrap();

        let bytes = vfs.read(attr.ino, 0, 6).await.unwrap();
        assert_eq!(bytes.as_ref(), b"abcdef");
        assert!(vfs.pending_write_buffer_blocks_for_test(attr.ino).is_empty());
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs-vfs read_ordered_does_not_call_write_buffer_flush_for_active_handle_visibility -- --exact`

Expected: FAIL until read precedence switches to handle writer spans.

- [ ] **Step 3: Write minimal implementation**

```rust
// Remove read-start eager draining for this path and let build_read_plan_with()
// prepend active handle writer spans ahead of metadata-backed local unpublished
// spans.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs-vfs read_ordered_does_not_call_write_buffer_flush_for_active_handle_visibility -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/io_ops.rs crates/studiofs-vfs/src/vfs/read_path.rs crates/studiofs-vfs/src/write_buffer.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "refactor: remove read-start flush dependence for SFS-245"
```

### Task 6: Preserve Last-Handle Barrier Semantics

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/sync_ops.rs`
- Modify: `crates/studiofs-vfs/src/commit.rs`
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn last_handle_release_materializes_dirty_handle_writer_state() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-release-materialize");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"release-materialize.txt", 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"release-barrier";

        vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();
        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();

        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);
        let spans = meta.lookup_regular_file_slices(attr.ino, 0, payload.len() as u64).unwrap();
        assert!(!spans.is_empty());
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs-vfs last_handle_release_materializes_dirty_handle_writer_state -- --exact`

Expected: FAIL because `release_fh` still reasons only about `WriteBuffer` dirty state.

- [ ] **Step 3: Write minimal implementation**

```rust
async fn local_sync_barrier_with_outcome(...) -> VfsResult<LocalSyncBarrierOutcome> {
    let flushed_handle_writers = self.inode_has_dirty_handle_writers(ino);
    if flushed_handle_writers {
        self.materialize_all_handle_writers_for_inode(ino, local_sync_policy).await?;
    }
    // keep existing dirty_attr persistence and sticky-error reporting
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs-vfs last_handle_release_materializes_dirty_handle_writer_state -- --exact`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/sync_ops.rs crates/studiofs-vfs/src/commit.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "fix: preserve release barrier semantics with handle writers"
```

### Task 7: Targeted Verification Sweep

**Files:**
- Modify: `docs/superpowers/specs/2026-04-16-sfs-245-slice-writer-first-slice-design.md`
- Modify: `docs/superpowers/plans/2026-04-16-sfs-245-slice-writer-first-slice.md`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`
- Test: `crates/studiofs-meta/src/redb_engine/tests/chunk_state.rs`

- [ ] **Step 1: Run the targeted VFS writer tests**

Run: `cargo test -p studiofs-vfs same_handle_read_after_buffered_write_does_not_require_read_start_flush write_fh_populates_handle_writer_state_before_barrier flush_fh_materializes_handle_writer_state_into_local_unpublished_slices read_ordered_does_not_call_write_buffer_flush_for_active_handle_visibility last_handle_release_materializes_dirty_handle_writer_state -- --exact`

Expected: PASS for all listed tests.

- [ ] **Step 2: Run the targeted metadata graph tests**

Run: `cargo test -p studiofs-meta test_flush_regular_file_overlay_slices_with_attr_is_atomic test_lookup_regular_file_slices_cross_chunk_and_ordered_base_resolution test_lookup_regular_file_slices_boundary_spanning_write_is_split_per_chunk -- --exact`

Expected: PASS for all listed tests.

- [ ] **Step 3: Update docs if design drifted during implementation**

```markdown
Update the design and plan docs only if the landed code changed writer naming,
module boundaries, or barrier semantics from the approved design.
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-04-16-sfs-245-slice-writer-first-slice-design.md docs/superpowers/plans/2026-04-16-sfs-245-slice-writer-first-slice.md crates/studiofs-vfs/src/vfs/tests.rs crates/studiofs-meta/src/redb_engine/tests/chunk_state.rs
git commit -m "docs: finalize first-slice plan for SFS-245"
```
