# Shared OS-Cache Coherence and Handle-Lifetime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move macOS's proven generation-aware OS-cache behavior, writer/foreground lifetime, and FUSE inode liveness into shared deep modules without changing macOS decisions or Windows behavior.

**Architecture:** `studiofs-vfs::vfs::os_cache` owns exact content-generation identity, bounded observations, active participating writers, open decisions, decision metrics, and lifecycle tokens attached to `OpenFile`. VFS release paths own those tokens through synchronous and deferred local barriers. `studiofs-bridge` owns lookup-reference counts, liveness-gated invalidation, and a small per-inode serializer for the FUSE `prepare → reply` boundary. macFUSE becomes the first adapter migrated to these shared seams; Linux consumption is intentionally deferred to the Linux integration plan.

**Tech Stack:** Rust 2024, existing VFS handle/release machinery, `lru`, `parking_lot`, `dashmap`, macFUSE low-level bridge, Cargo tests.

---

## Dependency and execution order

Execute this plan after `2026-07-14-linux-fuse-async-write-correctness.md`, because both plans touch VFS metrics. This plan must finish with macOS behavior preserved before Linux is allowed to consume the shared cache decision.

## Scope and invariants

- The adapter supplies only whether page-cache reuse is enabled for this open; VFS derives handle, inode, access mode, attributes, dirty state, writer state, and generation.
- `prepare_os_cache_open` never fails an otherwise valid open. Unknown local facts return `Replace(UnknownLocalState)`.
- Generation identity is exact: content epoch, logical size, and the complete ordered slice graph. Publication status is not part of identity.
- First observation replaces; only a repeated identical stable observation reuses.
- Mutating opens and the final writer release clear remembered identity. A read rejects both an already-prepared writer lifecycle and any `active_open_overlay` visible in `HandleTable`, including `O_RDONLY|O_TRUNC`, closing the open-to-prepare race.
- Every adapter that calls `prepare_os_cache_open` gets shared foreground/writer lifetime even when page-cache participation is disabled. This is required for Linux Direct-mode QoS later.
- Lifecycle ends exactly once and remains active through any final local release barrier.
- Full-data invalidation and confirmed final inode retirement clear identity before any platform notification and without holding the cache lock during the callback.
- Same-inode FUSE cache decisions are serialized only from shared preparation
  through the synchronous platform reply. A rejected reply clears the prepared
  observation before the next same-inode opener can decide, so it can never
  turn a required `Replace` into stale `Reuse`.
- Lookup liveness counts inode references, handles hardlinks, and invokes external notification with no map/shard guard held.
- Liveness membership can be inspected without inventing a lookup reference, and monotonic event counters make forwarding/suppression auditable.
- WinFSP does not call the new prepare API in this plan; its behavior remains unchanged.
- Tests place temporary state under the repository `target/` tree and clean it after use.

## File map

- Create: `crates/studiofs-bridge/src/fuse_liveness.rs` — lookup counts and liveness-checked invalidator.
- Create: `crates/studiofs-bridge/src/fuse_open_reply.rs` — bounded-lifetime per-inode `prepare → reply` serialization.
- Modify: `crates/studiofs-bridge/src/lib.rs` — Unix-only liveness and reply-serializer exports.
- Modify: `crates/studiofs-bridge/Cargo.toml` — Unix `dashmap` and `parking_lot` dependencies.
- Create: `crates/studiofs-vfs/src/vfs/os_cache.rs` — decisions, exact generation, bounded store, active writer state, lifecycle cleanup, tests.
- Modify: `crates/studiofs-vfs/src/vfs.rs` — module/export, `StudioVfs` state, prepare API, shared metrics.
- Modify: `crates/studiofs-vfs/src/vfs/bootstrap.rs` — initialize shared OS-cache state in the single VFS assembly path.
- Modify: `crates/studiofs-vfs/src/lib.rs` — public decision/stat exports.
- Modify: `crates/studiofs-vfs/src/vfs/file_session.rs` — idempotent per-handle preparation/lifecycle state.
- Modify: `crates/studiofs-vfs/src/vfs/config.rs` — carry lifecycle in final release context.
- Modify: `crates/studiofs-vfs/src/vfs/sync_ops.rs` — finish lifecycle after local release work on every path.
- Modify: `crates/studiofs-vfs/src/vfs/namespace_ops.rs` — retire generation state only after final unlink or final rename displacement.
- Modify: `crates/studiofs-vfs/src/vfs/workers.rs` — clear shared generation before full invalidation callback.
- Modify: `crates/studiofs-meta/src/redb_engine/fs_ops.rs` — avoid deferred-delete state when unlinking an open inode that still has another hardlink.
- Modify: `crates/studiofs-meta/src/redb_engine/tests/fs_ops.rs` — cover open-handle unlink with a surviving hardlink.
- Modify: `crates/studiofs-vfs/src/vfs/metrics.rs` — platform-neutral OS-cache counters and legacy decode compatibility.
- Modify: `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs` — export active writer/foreground cache-lifecycle gauges.
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs` — lifecycle, release, invalidation, and Windows-neutral regression tests.
- Modify: `crates/studiofs-macfuse/src/adapter.rs` — consume shared decision/liveness and delete duplicated ownership.
- Modify: `crates/studiofs-macfuse/src/lib.rs` — retarget cache behavior tests to shared decisions.
- Modify: `crates/studiofs-macfuse/Cargo.toml` — remove `lru` after the adapter snapshot store is deleted.

## Task 1: Extract the two small shared FUSE coordination primitives

- [ ] **Step 1: Add the Unix module and dependencies**

In `crates/studiofs-bridge/src/lib.rs` add:

```rust
#[cfg(unix)]
/// Lookup-reference liveness shared by FUSE-style Unix adapters.
pub mod fuse_liveness;

#[cfg(unix)]
/// Per-inode serialization for FUSE open preparation and reply.
pub mod fuse_open_reply;
```

In the Unix dependency section of `crates/studiofs-bridge/Cargo.toml` add:

```toml
dashmap.workspace = true
parking_lot.workspace = true
```

- [ ] **Step 2: Move the current liveness tests first**

Create `crates/studiofs-bridge/src/fuse_liveness.rs` and move/adapt the macFUSE tests to require:

```rust
lookup_references_close_only_after_final_forget
zero_nlookup_is_a_noop
root_inode_is_always_observed
ensure_observed_reports_only_a_new_reference
is_observed_never_creates_a_reference
unknown_inode_suppresses_notification
hardlink_lookup_references_accumulate_by_inode
close_does_not_wait_for_external_notification
notification_can_reenter_close
notification_does_not_block_unrelated_inode_updates
liveness_stats_count_observed_forgotten_retired_forwarded_and_suppressed_events
```

Use one-second channel timeouts for concurrency tests.

- [ ] **Step 3: Verify RED**

Run:

```bash
cargo test -p studiofs-bridge fuse_liveness -- --nocapture
```

Expected: compilation fails because the shared types do not exist.

- [ ] **Step 4: Move the concurrent implementation**

Use the current macFUSE `DashMap<u64, Arc<Mutex<u64>>>` algorithm and expose:

```rust
#[derive(Debug)]
pub struct FuseInodeLiveness {
    observed: DashMap<u64, Arc<Mutex<u64>>>,
}

impl Default for FuseInodeLiveness {
    fn default() -> Self {
        let observed = DashMap::new();
        observed.insert(ROOT_INO, Arc::new(Mutex::new(u64::MAX)));
        Self { observed }
    }
}

impl FuseInodeLiveness {
    /// Count one kernel lookup reference.
    pub fn observe(&self, ino: u64);
    /// Ensure an already-open inode has at least one live reference.
    /// Ensure at least one reference; return true only when this call added it.
    pub fn ensure_observed(&self, ino: u64) -> bool;
    /// Retire every reference after final inode removal.
    pub fn close(&self, ino: u64);
    /// Retire `nlookup` references supplied by the kernel.
    pub fn forget(&self, ino: u64, nlookup: u64);
    /// Test liveness without changing the kernel reference count.
    pub fn is_observed(&self, ino: u64) -> bool;
    /// Invoke `notify` only while the inode is still known.
    pub fn notify_if_observed(&self, ino: u64, notify: impl FnOnce()) -> bool;
    /// Return monotonic operation counters for bridge observability.
    pub fn stats(&self) -> FuseInodeLivenessStats;
}
```

`ensure_observed` returns whether it changed a zero/absent inode to one, allowing an adapter to roll back a rejected open reply without decrementing a pre-existing lookup reference. `notify_if_observed` must compute the boolean under the per-inode lock, release both the lock and DashMap guard, then invoke `notify`. Return whether it invoked the callback.

`FuseInodeLivenessStats` contains `snapshot_generation` plus monotonic `observed_references`, `forgotten_references`, `retired_inodes`, `notifications_forwarded`, and `notifications_suppressed` counters. Increment the generation with every counter mutation. Count only actual reference changes: clamp an oversized forget to the current reference count, do not count root seeding, and count `close` only when it retires a live non-root inode. These counters are later published by each platform adapter; the shared module does not depend on a metrics backend.

Add the shared wrapper:

```rust
pub struct LivenessCheckedKernelInvalidator {
    liveness: Arc<FuseInodeLiveness>,
    inner: Arc<dyn studiofs_vfs::KernelInvalidator>,
}

impl LivenessCheckedKernelInvalidator {
    pub fn new(
        liveness: Arc<FuseInodeLiveness>,
        inner: Arc<dyn studiofs_vfs::KernelInvalidator>,
    ) -> Self {
        Self { liveness, inner }
    }
}
```

Its `inval_inode` and `inval_inode_attrs` implementations call the inner invalidator only through `notify_if_observed`.

- [ ] **Step 5: Add the per-inode reply serializer**

Create `crates/studiofs-bridge/src/fuse_open_reply.rs` with one public
`FuseOpenReplySerializer`. Its `with_inode(ino, closure)` method uses a
`parking_lot::Mutex<HashMap<u64, Arc<Mutex<()>>>>`: briefly clone/create the
per-inode lock under the map lock, release the map, hold only the inode lock
while invoking the closure, then remove the map entry when the map and current
call are its only remaining owners. Do not hold the map lock across platform
reply or VFS work.

Add tests proving:

```text
same_inode_prepare_reply_sections_do_not_overlap
different_inode_prepare_reply_sections_can_progress_independently
idle_inode_lock_entries_are_removed
```

Use channels and one-second bounds, not sleeps. This is deliberately only a
keyed critical section; do not add cache epochs, pending states, async queues,
or platform behavior to the bridge.

- [ ] **Step 6: Migrate macFUSE liveness**

Replace `KernelInodeLiveness` in `adapter.rs` with `FuseInodeLiveness`, replace `LiveKernelInvalidator` with `LivenessCheckedKernelInvalidator`, and preserve the existing wrapper order:

```text
platform invalidator -> liveness check -> read-window/invalidation gate
```

Delete only the moved liveness implementation/tests. Do not touch cache snapshots or writer lifetime yet.

- [ ] **Step 7: Verify GREEN**

Run:

```bash
cargo test -p studiofs-bridge fuse_liveness -- --nocapture
cargo test -p studiofs-bridge fuse_open_reply -- --nocapture
cargo test -p studiofs-macfuse kernel_inode_liveness -- --nocapture
cargo test -p studiofs-macfuse kernel_invalidator_does_not_retain_entire_adapter_shared_state -- --nocapture
```

Expected: shared tests pass; the macFUSE filter may report zero moved tests, while the remaining invalidator test passes.

- [ ] **Step 8: Commit**

```bash
git add crates/studiofs-bridge crates/studiofs-macfuse/src/adapter.rs Cargo.lock
git commit -m "refactor: share fuse inode liveness"
```

## Task 2: Implement exact bounded generation identity

- [ ] **Step 1: Declare the shared decision vocabulary**

Create `crates/studiofs-vfs/src/vfs/os_cache.rs` with these exact public types:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsCacheParticipation {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsCacheDisposition {
    Reuse,
    Replace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsCacheReason {
    PolicyDisabled,
    WriteCapable,
    UnknownHandle,
    UnknownLocalState,
    NotRegularFile,
    LocalDirtyState,
    ActiveWriter,
    FirstObservation,
    GenerationChanged,
    StableGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OsCacheOpenDecision {
    disposition: OsCacheDisposition,
    reason: OsCacheReason,
}
```

Add private `replace(reason)` and `reuse()` constructors plus public
`disposition(self)` and `reason(self)` accessors. Callers can inspect a
decision but cannot construct contradictory disposition/reason pairs.

- [ ] **Step 2: Add pure generation tests**

Tests in `os_cache.rs` must prove each equality component independently:

```rust
first_observation_replaces_and_identical_second_observation_reuses
content_epoch_change_replaces
logical_size_change_replaces
span_order_change_replaces
span_range_or_chunk_change_replaces
slice_identity_or_order_change_replaces
source_change_replaces
payload_object_offset_length_or_flags_change_replaces
publication_only_transition_reuses
generation_store_evicts_to_its_fixed_capacity
```

For tests use capacity 8; production capacity is 8192, matching the current macFUSE bound.

- [ ] **Step 3: Verify RED**

Run:

```bash
cargo test -p studiofs-vfs os_cache::tests -- --nocapture
```

Expected: compilation fails on the missing generation store.

- [ ] **Step 4: Implement exact generation and store**

Use:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
struct OsCacheGeneration {
    content_epoch: u64,
    size: u64,
    graph: Vec<OsCacheGraphSpan>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OsCacheGraphSpan {
    chunk_id: u64,
    range_start: u32,
    range_end: u32,
    source: u8,
    slice_id: Option<u64>,
    slice_order: Option<u64>,
    payload: Option<OsCachePayload>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OsCachePayload {
    object_id: u64,
    offset: u32,
    length: u32,
    flags: u8,
}
```

Do not add upload/publication state. Convert every result from `lookup_regular_file_slices(ino, 0, size)` in its returned order, including tombstones and holes.

The state is:

```rust
pub(super) struct OsCacheState {
    generations: Mutex<LruCache<Ino, OsCacheGeneration>>,
    active_writers: Mutex<HashMap<Ino, usize>>,
}
```

Expose private methods for observe, clear, begin/end writer, and active-writer query. Never invoke VFS metadata or a platform callback while `generations` or `active_writers` is locked.

- [ ] **Step 5: Wire the module and state**

Add `mod os_cache;` and public re-exports in `vfs.rs`, re-export the four public types from `crates/studiofs-vfs/src/lib.rs`, add `os_cache: OsCacheState` to `StudioVfs`, and initialize it in every `StudioVfs` constructor.

- [ ] **Step 6: Verify GREEN**

Run:

```bash
cargo test -p studiofs-vfs os_cache::tests -- --nocapture
```

Expected: every exact-identity and eviction test passes.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-vfs/src/lib.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/bootstrap.rs crates/studiofs-vfs/src/vfs/os_cache.rs
git commit -m "feat(vfs): own exact os cache generations"
```

## Tasks 3–4 execution rule

Execute Tasks 3 and 4 as one implementation and review unit. Preparation can
race a cloned `OpenFile` against handle-table removal, so Task 3 is not allowed
to land while release still lacks the terminal state transition and lifecycle
ownership added by Task 4. Keep the prepare-versus-release tests red through
the intermediate Task 3 implementation, finish Task 4, then run the combined
GREEN suite and create one commit.

## Task 3: Attach idempotent cache participation to `OpenFile`

- [ ] **Step 1: Add prepare-policy tests**

Using `ContractVfsFixture`, add focused tests named:

```rust
os_cache_prepare_unknown_handle_replaces_without_error
os_cache_prepare_disabled_still_tracks_foreground_read_lifetime
os_cache_prepare_write_handle_clears_generation_and_tracks_writer
os_cache_prepare_non_regular_replaces
os_cache_prepare_dirty_inode_replaces
os_cache_prepare_active_writer_replaces_read_open
os_cache_prepare_unprepared_write_handle_replaces_read_open
os_cache_prepare_unprepared_truncating_handle_replaces_read_open
os_cache_prepare_unknown_graph_replaces
os_cache_prepare_is_idempotent_and_enrolls_lifecycle_once
os_cache_aborted_open_clears_observation_for_next_replace
os_cache_release_before_prepare_is_terminal_without_lifecycle_leak
os_cache_prepare_racing_release_finishes_lifecycle_exactly_once
os_cache_prepared_writer_keeps_foreground_qos_active_before_write_classification
```

The disabled test must assert that foreground I/O is active until release even though the decision is `Replace(PolicyDisabled)`. This is the Direct-mode CPU/QoS contract.

The prepared-writer QoS test must exercise a write-capable handle before any
read/write access classifier marks it as a writer, and include an
`O_RDONLY|O_TRUNC` case. Wire the real `StudioVfs` foreground predicate used by
upload backpressure to OR in shared active-writer state; the existing
`AccessClass::SequentialWriter` unit test is not evidence for this lifecycle.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-vfs os_cache_prepare_ -- --nocapture
```

Expected: compilation fails because `prepare_os_cache_open` and handle state do not exist.

- [ ] **Step 3: Add shared handle state**

In `file_session.rs` add an `Arc<Mutex<OsCacheHandleState>>` to `OpenFile`:

```rust
#[derive(Debug)]
pub(super) enum OsCacheHandleLifecycle {
    ForegroundRead,
    Writer { ino: Ino },
}

#[derive(Debug, Default)]
pub(super) enum OsCacheHandleState {
    #[default]
    Unprepared,
    Prepared {
        decision: OsCacheOpenDecision,
        lifecycle: Option<OsCacheHandleLifecycle>,
    },
    Released,
}
```

All clones of one `OpenFile` share this state. Add private operations that return an existing decision or atomically install one, plus a release operation that replaces either prior state with terminal `Released` while taking a prepared lifecycle at most once. If a stale clone prepares after release won the mutex, return conservative `Replace(UnknownHandle)` without enrolling lifetime. If prepare wins, release observes and takes that lifecycle. Test both orders with channels/barriers, not sleeps.

- [ ] **Step 4: Implement the non-fallible VFS seam**

Add:

```rust
pub fn prepare_os_cache_open(
    &self,
    fh: u64,
    participation: OsCacheParticipation,
) -> OsCacheOpenDecision

pub fn abort_os_cache_open(&self, fh: u64)
```

Execute under the handle-local preparation mutex so two calls cannot double-enroll lifetime. The exact decision order is:

1. unknown `fh` -> `Replace(UnknownHandle)`;
2. existing prepared decision -> return it without incrementing metrics;
3. current handle has `active_open_overlay` -> begin shared writer, clear generation, `Replace(WriteCapable)`;
4. read handle -> begin shared foreground-read lifetime;
5. disabled policy -> `Replace(PolicyDisabled)`;
6. missing/open attr observation failure -> clear, `Replace(UnknownLocalState)`;
7. non-regular -> clear, `Replace(NotRegularFile)`;
8. local dirty -> clear, `Replace(LocalDirtyState)`;
9. `HandleTable::has_active_open_overlay(ino)` or an active writer lifecycle -> clear, `Replace(ActiveWriter)`;
10. graph observation failure -> clear, `Replace(UnknownLocalState)`;
11. first generation -> remember, `Replace(FirstObservation)`;
12. changed generation -> replace, `Replace(GenerationChanged)`;
13. identical generation -> `Reuse(StableGeneration)`.

The handle-table check covers every mutating/truncating handle that opened but has not yet reached `prepare_os_cache_open`; the shared lifecycle count covers a writer after handle-table removal while its final local barrier is still running. The begin-foreground call is intentionally independent of `OsCacheParticipation`; only WinFSP, which does not call this API, remains unprepared.

`abort_os_cache_open` is the narrow failed-platform-reply hook. While the VFS
handle still exists, resolve its inode and clear that inode's remembered
generation; do not release the handle or finish lifecycle here. It is a no-op
for an unknown handle. FUSE adapters call it only after `prepare_os_cache_open`
when their synchronous open/create reply is rejected, while still holding the
shared per-inode reply serializer. They then release the VFS handle through the
normal local-only release path. A successful reply needs no acknowledgement.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p studiofs-vfs os_cache_prepare_ -- --nocapture
cargo test -p studiofs-vfs foreground_io_active_includes_active_writers -- --nocapture
cargo test -p studiofs-vfs handle_read_access_tracking_promotes_active_class_counts -- --nocapture
```

Expected at this intermediate point: pure prepare-policy tests pass. The two
prepare-versus-release tests remain red until Task 4 installs the terminal
release transition; do not commit this partial state.

- [ ] **Step 6: Continue directly into Task 4**

Keep the worktree uncommitted and complete Task 4 in the same implementation
and review unit.

## Task 4: Own lifecycle through every release path

- [ ] **Step 1: Add failing lifecycle tests**

Add:

```rust
os_cache_nonfinal_reader_release_finishes_foreground_once
os_cache_nonfinal_writer_release_decrements_writer_once
os_cache_last_writer_remains_active_through_sync_release_barrier
os_cache_sync_release_error_still_finishes_lifecycle
os_cache_deferred_release_keeps_lifecycle_until_barrier_completion
os_cache_duplicate_or_unknown_release_does_not_underflow
os_cache_last_writer_release_leaves_next_read_in_replace_state
winfsp_unprepared_open_release_keeps_existing_counters
```

Use the existing deferred-release barrier test controls rather than sleeps to prove lifetime remains active until the task finishes.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-vfs os_cache_ -- --nocapture
```

Expected: lifecycle tests fail because table removal currently discards no shared token and deferred release has no ownership.

- [ ] **Step 3: Carry lifecycle in release context**

Add to `HandleReleaseContext` in `config.rs`:

```rust
pub(super) os_cache_lifecycle: Option<OsCacheHandleLifecycle>,
```

In `close_handle_for_release`, immediately after `HandleTable::close` call the handle-state operation that atomically transitions the shared clone state to terminal `Released` and takes any lifecycle:

- if another handle remains, finish it immediately because this close owns no final barrier;
- if it is the final handle, move it into `HandleReleaseContext`.

- [ ] **Step 4: Finish synchronous lifecycle after local work**

Introduce a private borrowed guard whose `Drop` calls `finish_os_cache_handle_lifecycle`. Construct it immediately after receiving final context, before ignored-entry/orphan/session branches. It must live through the awaited `execute_last_handle_release_action` call and therefore runs on success, error, and early return.

For writer completion, decrement `active_writers`; when the count reaches zero, clear the inode generation. For foreground read completion, call the existing `end_foreground_read_handle` exactly once.

- [ ] **Step 5: Move lifecycle into deferred release**

For `release_fh_with_deferred_last_handle_barrier`, create an owned guard containing `Arc<StudioVfs>` plus the lifecycle and move it into the spawned task. Drop it only after `execute_deferred_last_handle_release_action` and file-session cleanup. RAII must also clean up if the task is cancelled or panics.

Clean/ignored final actions that do not spawn a task finish before the API returns.

- [ ] **Step 6: Verify GREEN**

Run:

```bash
cargo test -p studiofs-vfs os_cache_ -- --nocapture
cargo test -p studiofs-vfs release_fh_with_outcome -- --nocapture
cargo test -p studiofs-vfs deferred_release -- --nocapture
cargo check -p studiofs-winfsp
```

Expected: lifecycle tests and existing release tests pass; Windows compiles without calling prepare.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-vfs/src/vfs/config.rs crates/studiofs-vfs/src/vfs/file_session.rs crates/studiofs-vfs/src/vfs/os_cache.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/sync_ops.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat(vfs): own os cache handle lifetime"
```

## Task 5: Clear generation on invalidation and final inode retirement

- [ ] **Step 1: Add invalidation-order tests**

Add tests:

```rust
full_kernel_invalidation_clears_generation_before_callback
full_kernel_invalidation_deduplicates_and_clears_all_before_first_callback
full_invalidation_clears_generation_without_platform_invalidator
full_kernel_invalidation_clears_generation_when_callback_is_suppressed_for_active_open
invalidation_callback_can_reenter_generation_observation
attribute_only_invalidation_preserves_generation
os_cache_generation_retirement_final_unlink_without_open_handles_clears_generation
os_cache_generation_retirement_final_unlink_with_open_handles_clears_before_last_close
os_cache_generation_retirement_unlink_one_hardlink_without_open_handles_preserves_generation
os_cache_generation_retirement_unlink_one_hardlink_with_open_handles_preserves_generation_and_payload_after_release
os_cache_generation_retirement_rename_overwrite_final_target_clears_only_target
os_cache_generation_retirement_rename_overwrite_surviving_hardlink_preserves_generation_and_payload
os_cache_generation_retirement_rename_overwrite_open_final_target_defers_cleanup_until_release
unlink_open_handle_with_remaining_hardlink_does_not_record_deferred_delete
```

The re-entrant mock invalidator must use a fresh unprepared read handle (preferably
for the second of two invalidated inodes) so it really enters
`OsCacheState::observe`. Run invalidation on a spawned thread and require a
channel result within one second so a lock-order regression fails boundedly
instead of hanging the suite.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-vfs full_kernel_invalidation_ -- --nocapture
cargo test -p studiofs-vfs attribute_only_invalidation_preserves_generation -- --nocapture
cargo test -p studiofs-vfs os_cache_generation_retirement -- --nocapture
cargo test -p studiofs-meta unlink_open_handle_with_remaining_hardlink_ -- --nocapture
```

Expected: generation remains reusable after a full invalidation or ordering assertion fails.

- [ ] **Step 3: Add one VFS-owned retirement operation**

Expose a crate-private `retire_os_cache_inode(ino)` that removes the shared generation without calling platform code. Call it from `namespace_ops.rs` as soon as successful metadata mutation confirms that the inode itself, not merely one pathname, is gone:

- both open-handle and no-open-handle unlink paths only when returned
  `attr.nlink == 0`; when an open unlink changes `nlink` from two to one, the
  redb namespace operation must not create a deferred-delete record and VFS
  must not mark the inode deleted/orphaned;
- rename-overwrite only when a post-`rename_atomic`
  `get_attr(displaced_ino)` returns exactly
  `Err(MetaError::InodeNotFound(found)) if found == displaced_ino`; `Ok(attr)`
  means a hardlink survives and the whole deleted-inode graph/accounting
  cleanup branch, not only cache retirement, is skipped;
- never for a remaining hardlink, the moved source inode, an ordinary rename, or a failed namespace mutation.

For a confirmed displaced inode with open handles, mark it orphaned and defer
payload cleanup to the final release, but clear its generation immediately.
Clear before deferred storage cleanup so a cleanup error after committed
metadata cannot leave a reusable observation for a retired inode. Direct
adapter calls must not own this rule; macFUSE and Linux receive the same VFS
behavior. Hardlink-preservation tests must release every handle and then prove
the remaining pathname's exact payload as well as StableGeneration reuse.

- [ ] **Step 4: Change full invalidation ordering**

In `invalidate_kernel_inodes`/the full-data invalidation helper in `workers.rs`:

1. normalize and deduplicate every input inode before callback eligibility is considered;
2. clear each unique shared OS-cache generation, even if no external invalidator is installed or `should_skip_kernel_invalidation_for_active_open` later suppresses its callback;
3. release all OS-cache locks;
4. obtain the invalidator and call `KernelInvalidator::inval_inode` only for non-skipped inodes.

Do not clear generations from `invalidate_kernel_inode_attrs`; attribute-only invalidation must keep page identity.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p studiofs-vfs full_kernel_invalidation_ -- --nocapture
cargo test -p studiofs-vfs invalidation_callback_can_reenter_generation_observation -- --nocapture
cargo test -p studiofs-vfs attribute_only_invalidation_preserves_generation -- --nocapture
cargo test -p studiofs-vfs os_cache_generation_retirement -- --nocapture
cargo test -p studiofs-meta unlink_open_handle_with_remaining_hardlink_ -- --nocapture
```

Expected: all pass without deadlock.

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-meta/src/redb_engine/fs_ops.rs crates/studiofs-meta/src/redb_engine/tests/fs_ops.rs crates/studiofs-vfs/src/vfs/namespace_ops.rs crates/studiofs-vfs/src/vfs/workers.rs crates/studiofs-vfs/src/vfs/os_cache.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "fix(vfs): retire os cache generations at shared seams"
```

## Task 6: Generalize cache-decision observability

- [ ] **Step 1: Add serialization and counter tests**

In `metrics.rs` and VFS tests require one counter for every `OsCacheReason`, aggregate `reuse` and `replace`, idempotent prepare counting once, and deserialization of the existing `macfuse_open_cache` field.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-vfs os_cache_stats -- --nocapture
cargo test -p studiofs-vfs macfuse_open_cache_stats_deserializes -- --nocapture
```

Expected: the new neutral stats type is missing.

- [ ] **Step 3: Add platform-neutral stats**

Add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct VfsOsCacheStats {
    pub total: u64,
    pub reuse: u64,
    pub replace: u64,
    pub policy_disabled: u64,
    pub write_capable: u64,
    pub unknown_handle: u64,
    pub unknown_local_state: u64,
    pub not_regular_file: u64,
    pub local_dirty_state: u64,
    pub active_writer: u64,
    pub first_observation: u64,
    pub generation_changed: u64,
    pub stable_generation: u64,
    pub active_writers: u64,
    pub active_foreground_handles: u64,
}
```

Record the decision inside the first successful execution of `prepare_os_cache_open`, never in an adapter. Populate the two gauges from current shared lifecycle state. Add `os_cache: VfsOsCacheStats` to the write-path observability snapshot. Reset baseline-subtracts only monotonic decision counters; current gauges are copied unchanged.

Keep `VfsMacFuseOpenCacheStats` and its serialized snapshot field readable for one compatibility cycle so older saved reports still deserialize; mark both deprecated in Rust docs. New snapshots leave that legacy field at its default and publish live data only through `VfsOsCacheStats`. Do not copy Linux counters into a field named macFUSE, and do not keep a second live decision recorder.

- [ ] **Step 4: Export and verify GREEN**

Export `VfsOsCacheStats` through `vfs.rs` and `lib.rs`, plus these exact runtime gauges:

```text
os_cache.active_writers
os_cache.active_foreground_handles
```

Then run:

```bash
cargo test -p studiofs-vfs os_cache_stats -- --nocapture
cargo test -p studiofs-vfs macfuse_open_cache_stats_ -- --nocapture
cargo test -p studiofs-vfs runtime_stats_surface_reports_os_cache_lifecycle -- --nocapture
```

Expected: both neutral and legacy compatibility tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-vfs/src/lib.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs/os_cache.rs crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat(vfs): report shared os cache decisions"
```

## Task 7: Migrate macFUSE to the shared cache seam

- [ ] **Step 1: Retarget behavior tests before deleting state**

Update existing macFUSE tests to assert `OsCacheOpenDecision` and adapter flags for:

```text
first read -> Replace/keep_cache=false
identical second read -> Reuse/keep_cache=true
publication-only transition -> Reuse
size/epoch/graph change -> Replace
dirty state -> Replace
active writer -> Replace
write-capable open -> Replace
final writer release -> next read Replace
final unlink -> shared observation removed
unlink one remaining hardlink -> shared observation retained
rename-overwrite final target -> shared observation removed
bounded observations -> no more than shared capacity
two same-inode replies cannot publish Reuse before the preceding Replace reply
rejected Replace reply -> next open replaces again
failed open/create reply -> handle and shared lifecycle return to baseline
```

Keep the existing tests in `macfuse/src/lib.rs` around the current generation, foreground, and active-writer cases; change their inspection seam instead of deleting coverage.

- [ ] **Step 2: Verify the characterization tests pass before migration**

Run:

```bash
cargo test -p studiofs-macfuse open_cache -- --nocapture
cargo test -p studiofs-macfuse foreground_read -- --nocapture
cargo test -p studiofs-macfuse active_writer -- --nocapture
```

Expected: current behavior characterization passes.

- [ ] **Step 3: Serialize shared preparation through the platform reply**

Refactor ordinary-file `open` and `create` so their adapter methods return a
small private value containing `ino`, `fh`, and `MacFuseOpenCachePolicy`, but do
not call `prepare_os_cache_open` yet. The synthetic runtime-stats file keeps its
existing ready-made direct-I/O reply and does not enter the shared cache seam.

In `studiofs_macfuse_open` and `studiofs_macfuse_create`, use the adapter's
`FuseOpenReplySerializer` for the returned inode. Inside that one short
same-inode critical section:

```rust
let participation = if flag_policy.keep_cache {
    OsCacheParticipation::Enabled
} else {
    OsCacheParticipation::Disabled
};
let decision = self.vfs.prepare_os_cache_open(fh, participation);
let keep_cache = flag_policy.keep_cache
    && decision.disposition() == OsCacheDisposition::Reuse;
let open_reply = OpenReply::new(fh)
    .with_direct_io(flag_policy.direct_io)
    .with_keep_cache(keep_cache);
```

Call `Reply::open` or `Reply::create` before releasing the serializer. Writers
are prepared even though their adapter flag cannot keep cache. For create,
reserve the lookup reference immediately before `Reply::create`; roll it back
if that reply fails. For open, preserve macOS's existing `ensure_observed`
behavior but remember whether this call added the reference and roll back only
that addition on reply failure.

Treat a missing owned request or an `io::Error` returned by `Reply::open` or
`Reply::create` as a rejected platform reply. Still inside the same serializer:

1. call `vfs.abort_os_cache_open(fh)` so the next opener cannot reuse an
   observation the kernel never accepted;
2. roll back any lookup reference added for this reply;
3. synchronously call `release_fh_with_outcome(fh)` through the adapter runtime;
4. normalize only close-time `ENOENT` and log every other cleanup error.

The rejected request cannot receive another reply. This cleanup removes the
VFS handle and completes shared foreground/writer lifecycle without adding a
cache commit-token protocol.

- [ ] **Step 4: Test ordering and rejected replies**

Use a fake `Reply` seam that can block and return success or failure. Prove that
a second same-inode callback cannot prepare/reply while the first reply is
blocked, a different inode can progress, a failed `Replace` makes the next
same-generation open `Replace` again, and rejected open/create replies return
handle/lifecycle/liveness gauges to baseline. Use channels and one-second
bounds, not sleeps.

- [ ] **Step 5: Delete duplicated adapter ownership**

Remove:

- `MacFuseOpenCacheSnapshot`, graph/payload structs, source conversion, and adapter LRU;
- `WriteCapableState` and released-write structs;
- `foreground_read_handles` and its begin/end helpers;
- `keep_cache_for_current_generation`, `open_cache_graph_signature`, clear/seed helpers;
- cache snapshot removal from `ReadWindowKernelInvalidator`;
- adapter release cleanup for shared writer/foreground state.

Keep read-window invalidation and file-flag maps; they are separate platform behavior. Remove `lru.workspace` from `studiofs-macfuse/Cargo.toml` after `rg '\bLruCache\b' crates/studiofs-macfuse` returns no matches.

- [ ] **Step 6: Verify macOS equivalence**

Run:

```bash
cargo test -p studiofs-macfuse --lib -- --nocapture
cargo test -p studiofs-vfs os_cache_ -- --nocapture
```

Expected: all tests pass, including first/second generation, publication-only reuse, active writer, foreground lifetime, and release cases.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-macfuse crates/studiofs-vfs
git commit -m "refactor(macos): consume shared os cache coherence"
```

## Task 8: Cross-platform focused verification

- [ ] **Step 1: Run shared and macOS suites**

```bash
cargo test -p studiofs-bridge --lib -- --nocapture
cargo test -p studiofs-vfs --lib os_cache -- --nocapture
cargo test -p studiofs-vfs --lib release -- --nocapture
cargo test -p studiofs-macfuse --lib -- --nocapture
```

- [ ] **Step 2: Prove Windows remains an unprepared consumer**

```bash
cargo check -p studiofs-winfsp
cargo test -p studiofs-vfs winfsp_unprepared_open_release_keeps_existing_counters -- --nocapture
```

Expected: Windows compiles and neutral lifecycle counters remain unchanged for unprepared handles.

- [ ] **Step 3: Run formatting and focused linting**

```bash
cargo fmt --all -- --check
cargo clippy -p studiofs-bridge -p studiofs-vfs -p studiofs-macfuse --all-targets -- -D warnings
git diff --check
```

Expected: all commands exit zero.

## Self-review

- Public cache API contains participation, a non-fallible decision, and one
  non-fallible failed-reply abort hook; platform policy stays out of VFS.
- Disabled page-cache participation still enrolls foreground/writer lifetime for adapters using the seam.
- Exact identity excludes publication state but includes every ordered graph component.
- Unknown state replaces cache and never converts an open success into an error.
- Active writer lifetime outlives handle-table removal and ends after the local barrier.
- Terminal per-handle state serializes prepare versus release, preventing late lifecycle enrollment.
- Mutating overlay detection includes truncation-only opens, not only write access bits.
- Deferred tasks own cleanup through cancellation/error via RAII.
- Full invalidation clears state before, and never under, the external callback.
- Final unlink/rename retirement clears shared state in VFS; removing one hardlink does not.
- The small per-inode serializer covers preparation through reply; a rejected
  reply clears observation and releases the VFS handle before that section ends.
- Shared liveness statistics count actual reference changes and notification outcomes without coupling to VFS metrics.
- macFUSE inspects reply status and uses the shared decision; no adapter
  snapshot/writer/foreground map remains.
- WinFSP is neither forced into FUSE liveness nor changed implicitly.
