# SFS-246 Data Reader First Design

## Goal

Replace the current read planner with a Rust-native reader flow that:

- resolves data from chunk/slice/block metadata directly
- preserves Local-First precedence for same-mount reads
- handles holes and sparse ranges as zero-fill through one reader model
- keeps adaptive readahead and prefetch, but hangs them off reader sessions instead of the old planner/classifier architecture

## Why This Slice

`SFS-244` and `SFS-245` already moved the rewrite onto chunk/slice/block metadata and handle writer state. The remaining mismatch is that foreground reads still route through [`crates/studiofs-vfs/src/vfs/read_path.rs`](../../../crates/studiofs-vfs/src/vfs/read_path.rs) as a `ReadPipeline` planner that expands a request into temporary `ReadPlanSpan` entries before the real IO work begins.

That planner is not the target architecture described in [`docs/juicefs-local-first-rust-contract.md`](../../../docs/juicefs-local-first-rust-contract.md). The contract requires a `dataReader`-style merged reader over:

1. active writer state
2. durable local unpublished state
3. published state
4. holes

## Approaches Considered

### 1. Recommended: reader session plus explicit data sources

Introduce a foreground `DataReader` session that owns one request and resolves bytes by walking ordered sources directly.

Why this wins:

- matches the canonical rewrite contract
- reuses the useful parts of the current code: metadata resolution, pending overlay lookup, and block IO helpers
- makes readahead and prefetch consume effective reader ranges instead of planner-specific span structs

Trade-off:

- requires a careful cutover because `ReadPipeline`, `read_prefetch_batch()`, and tests currently assume planner objects

### 2. Keep the planner, but rename it to sound like a reader

Keep most of the current `ReadPipeline` logic and wrap it in new types.

Why not now:

- preserves the old architecture under new names
- keeps prefetch tied to planner-only behavior
- does not actually simplify the read path

### 3. Rewrite reads and readahead in one large refactor

Delete `ReadPipeline`, `access.rs`, `readahead.rs`, and `vfs/prefetch.rs` together in a single cutover.

Why not now:

- too much surface area for one safe step
- makes it harder to preserve proven read visibility behavior while deleting planner-only layers

## Scope

### In scope

- foreground reader rewrite in `crates/studiofs-vfs/src/vfs/read_path.rs`
- new reader-owned source ordering over handle state, local unpublished state, published overlay/base slices, and holes
- prefetch worker cutover to the new reader flow
- targeted simplification of read-path classifier/planner logic that only exists for the old engine
- documentation updates for the active read architecture

### Out of scope for this slice

- full operator tuning cleanup
- cache/object-store pipeline rewrite covered by `SFS-247`
- bridge-specific tuning work covered by `SFS-250`
- deleting every access classification metric if it still carries real runtime value outside the old planner

## Proposed Architecture

### 1. `DataReader`

Add a reader object in `read_path.rs` that owns:

- inode
- request offset and length
- `ReadRequestContext`
- resolved metadata slices for the request window

It exposes:

- `read_into(&mut [u8])`
- `prefetch_only()`
- internal source iteration for effective spans

The object is request-scoped and has no long-lived background ownership.

### 2. Reader Sources

The reader resolves effective data in this exact order:

1. active handle writer spans from `snapshot_handle_writer_read_spans`
2. durable local unpublished spans from `snapshot_pending_dirty_slice_read_spans`
3. pending local extent-backed blocks from `pending_overlay_block_source`
4. resolved metadata slices with `ResolvedSliceSource::Overlay`
5. resolved metadata slices with `ResolvedSliceSource::Base`
6. zero-fill holes

This keeps the already-correct Local-First precedence, but removes the idea that the read path must first become a planner.

### 3. Reader-native segment assembly

Instead of building `ReadPlanSpan { range, source }` values and sorting them, the reader will build smaller effective segments while it walks unresolved ranges:

- each segment knows how to satisfy bytes
- the segment type remains local to the reader implementation
- foreground reads and prefetch use the same segment resolution path

The key design rule is that the reader owns resolution and assembly together.

### 4. Session-oriented readahead

Keep handle-aware stream tracking, but make it a lightweight reader-session concern:

- foreground read records a stream observation
- tracker returns next block ranges worth prefetching
- prefetch worker calls the same `DataReader::prefetch_only()` path over those ranges

This preserves adaptive readahead and in-flight deduplication, but removes the idea that prefetch depends on the old read planner as an architectural layer.

### 5. Deletion target

At the end of this slice, the active architecture must no longer describe:

- `ReadPipeline` as the canonical read engine
- `ReadPlanSource` / `ReadPlanSpan` as the target model
- planner-specific docs that say foreground reads are built around a read planner

## Data Flow

### Foreground read

`prepare_foreground_read()`:

1. resolves attr and metadata slices
2. builds `ReadRequestContext`
3. computes the bounded read length

`execute_prepared_foreground_read()`:

1. records stream access
2. constructs `DataReader`
3. resolves the request through ordered reader sources
4. enqueues readahead hints from the effective reader session

### Prefetch read

`read_prefetch_batch()`:

1. validates requested block range against current read context
2. constructs `DataReader` for each contiguous batch
3. runs `prefetch_only()` to warm blocks without copying payload into a caller buffer

## Error Handling

- local unpublished extent missing from local backend: zero-fill that range and emit a warning, preserving current local durability fallback behavior
- published block missing from object storage during prefetch: keep best-effort behavior and do not fail the request solely due to background prefetch
- published block missing during foreground read: preserve current `NotFound` handling semantics for the caller-visible read path
- metadata `InodeNotFound`: clear stale runtime state exactly as the current resolver does

## Testing Strategy

Targeted tests only. No full-suite run in this task.

### New or updated regression coverage

- foreground reads still prefer active handle state over durable local unpublished state
- durable local unpublished spans still beat published base spans
- sparse gaps still zero-fill through the new reader
- prefetch batches use the new reader path instead of `ReadPipeline`
- planner-only types are gone or reduced to reader-internal implementation details

## Exit Criteria

This slice is complete when:

- foreground reads run through `DataReader` rather than `ReadPipeline`
- prefetch batches use the same reader resolution logic
- Local-First precedence remains intact
- old planner-first read-path docs are updated
- targeted tests prove the new flow without running the full workspace suite

## Follow-on Work

After this lands, follow-on work can:

- simplify or remove any remaining access classification policy that exists only to support old reader heuristics
- fold reader metrics around reader sessions instead of legacy planner concepts
- continue the cached-store integration under `SFS-247`
