# SFS-246 Data Reader First Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the planner-shaped foreground read engine with a reader-shaped flow that preserves Local-First precedence and keeps adaptive prefetch working on top of the merged read view.

**Architecture:** Keep `ReadMetadataResolver`, metadata slice lookup, and local unpublished helpers. Replace `ReadPipeline` with a request-scoped `DataReader` in `read_path.rs`, route prefetch through the same reader path, and delete planner-only read-path types and docs that no longer describe the active design.

**Tech Stack:** Rust 2024, Tokio, StudioFS VFS, chunk/slice/block metadata in `studiofs-meta`, targeted `cargo test` runs only

---

### Task 1: Introduce reader-first tests and `DataReader` scaffolding

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/read_path.rs`
- Test: `crates/studiofs-vfs/src/vfs/read_path.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn data_reader_prefers_local_unpublished_over_published_base() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "data-reader-local-first");
    rt.block_on(async {
        // Build a published base block and then overlay newer local-unpublished bytes.
        // Expect the foreground reader to return the local bytes.
    });
}

#[test]
fn data_reader_zero_fills_sparse_gaps() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "data-reader-zero-fill");
    rt.block_on(async {
        // Build a sparse file window and expect the uncovered bytes to read as zeroes.
    });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p studiofs-vfs data_reader_ --lib`
Expected: FAIL because `DataReader` tests reference behavior not yet implemented

- [ ] **Step 3: Add minimal `DataReader` scaffolding**

```rust
struct DataReader<'a> {
    vfs: &'a StudioVfs,
    ino: Ino,
    offset: u64,
    length: u64,
    pending_content_epoch: Option<u64>,
    resolved_regular_file_slices: Option<&'a [ResolvedSliceSpan]>,
}

impl<'a> DataReader<'a> {
    fn new(/* fields */) -> Self { /* ... */ }
}
```

- [ ] **Step 4: Re-run the targeted tests**

Run: `cargo test -p studiofs-vfs data_reader_ --lib`
Expected: FAIL on missing behavior rather than missing type

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/read_path.rs
git commit -m "test: add SFS-246 data reader regression coverage"
```

### Task 2: Move foreground resolution from `ReadPipeline` to `DataReader`

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/read_path.rs`
- Modify: `crates/studiofs-vfs/src/vfs/overlay.rs`
- Modify: `crates/studiofs-vfs/src/write_buffer.rs`
- Test: `crates/studiofs-vfs/src/vfs/read_path.rs`

- [ ] **Step 1: Write the next failing tests for foreground execution**

```rust
#[test]
fn execute_prepared_foreground_read_uses_data_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "data-reader-foreground");
    rt.block_on(async {
        // Execute a prepared read and assert the bytes match the effective merged view.
    });
}
```

- [ ] **Step 2: Run the focused test**

Run: `cargo test -p studiofs-vfs execute_prepared_foreground_read_uses_data_reader --lib`
Expected: FAIL while `execute_prepared_foreground_read()` still depends on `ReadPipeline`

- [ ] **Step 3: Implement minimal foreground cutover**

```rust
let data_reader = DataReader::new(
    self,
    ino,
    offset,
    prepared.read_len as u64,
    prepared.context.pending_content_epoch,
    Some(prepared.resolved_slices.as_slice()),
);
let written = data_reader.read_into(buffer).await?;
```

- [ ] **Step 4: Move ordered source resolution into `DataReader`**

```rust
let handle_spans = self.vfs.snapshot_handle_writer_read_spans(...);
let dirty_spans = self.vfs.snapshot_pending_dirty_slice_read_spans(...);
let overlay_source = self.vfs.pending_overlay_block_source(...);
let resolved = self.resolved_regular_file_slices.unwrap_or(&[]);
```

- [ ] **Step 5: Re-run the focused tests**

Run: `cargo test -p studiofs-vfs data_reader_ --lib`
Expected: PASS for the new data-reader tests

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/read_path.rs crates/studiofs-vfs/src/vfs/overlay.rs crates/studiofs-vfs/src/write_buffer.rs
git commit -m "feat: switch SFS-246 foreground reads to data reader flow"
```

### Task 3: Route prefetch through `DataReader` and retire planner-only types

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/read_path.rs`
- Modify: `crates/studiofs-vfs/src/vfs/prefetch.rs`
- Test: `crates/studiofs-vfs/src/vfs/read_path.rs`

- [ ] **Step 1: Write the failing prefetch regression test**

```rust
#[test]
fn read_prefetch_batch_uses_data_reader_prefetch_path() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "data-reader-prefetch");
    rt.block_on(async {
        // Prefetch a contiguous batch and assert the expected local/object fetch path runs.
    });
}
```

- [ ] **Step 2: Run the focused test**

Run: `cargo test -p studiofs-vfs read_prefetch_batch_uses_data_reader_prefetch_path --lib`
Expected: FAIL while prefetch still constructs `ReadPipeline`

- [ ] **Step 3: Cut prefetch over to `DataReader::prefetch_only()`**

```rust
let data_reader = DataReader::new(
    self,
    ino,
    block_start,
    block_len,
    context.pending_content_epoch,
    None,
);
data_reader.prefetch_only(true).await?;
```

- [ ] **Step 4: Delete planner-only read-path types that are no longer active**

```rust
// Remove ReadPipeline and collapse helper names around DataReader.
// Keep only reader-internal segment helpers that are still used by foreground and prefetch paths.
```

- [ ] **Step 5: Re-run the focused tests**

Run: `cargo test -p studiofs-vfs read_prefetch_batch_uses_data_reader_prefetch_path --lib`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/read_path.rs crates/studiofs-vfs/src/vfs/prefetch.rs
git commit -m "refactor: retire SFS-246 planner-first prefetch flow"
```

### Task 4: Update active docs and run targeted verification

**Files:**
- Modify: `docs/vfs.md`
- Modify: `docs/juicefs-local-first-rust-contract.md` (only if wording needs a code-status clarification)
- Modify: `docs/superpowers/specs/2026-04-16-sfs-246-data-reader-first-design.md`

- [ ] **Step 1: Update read-path docs to describe the new active architecture**

```md
- foreground reads use a request-scoped `DataReader`
- the reader resolves active writer state, durable local unpublished slices, published slices, and holes in precedence order
- prefetch batches call the same reader path without buffer assembly
```

- [ ] **Step 2: Run targeted verification**

Run: `cargo test -p studiofs-vfs data_reader_ --lib`
Expected: PASS

Run: `cargo test -p studiofs-vfs read_prefetch_batch_uses_data_reader_prefetch_path --lib`
Expected: PASS

Run: `cargo fmt --check`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add docs/vfs.md docs/juicefs-local-first-rust-contract.md docs/superpowers/specs/2026-04-16-sfs-246-data-reader-first-design.md crates/studiofs-vfs/src/vfs/read_path.rs crates/studiofs-vfs/src/vfs/prefetch.rs
git commit -m "docs: align SFS-246 read path docs with data reader architecture"
```

## Self-Review

- Spec coverage: the plan covers reader precedence, zero-fill, prefetch cutover, planner retirement, and doc updates.
- Placeholder scan: no `TODO`/`TBD` placeholders remain; each task names exact files and commands.
- Type consistency: `DataReader` is the only new top-level read engine named in all tasks, and both foreground and prefetch tasks refer to the same `read_into()` / `prefetch_only()` API.
