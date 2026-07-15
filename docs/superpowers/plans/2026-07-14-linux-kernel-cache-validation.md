# Linux Kernel-Cache Integration and Performance-Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect Linux libfuse3 to the shared OS-cache/liveness seams, publish honest callback and negotiated-geometry evidence, prove cache coherence with mounted tests, and select the Linux default only through a repeated throughput/CPU/tail-latency gate.

**Architecture:** Linux keeps platform ownership of FUSE replies, flags, connection negotiation, and mounted lifecycle. It consumes `StudioVfs::prepare_os_cache_open` for every regular-file handle, maps only a shared `Reuse` decision to `keep_cache`, and wraps invalidation with shared inode liveness. VFS callback snapshots gain fixed latency and request-size histograms. A Linux-only benchmark driver exercises an already-mounted process, captures mount CPU and runtime-stat deltas, and feeds a deterministic median/MAD analyzer. Direct mode remains the rollback; Kernel becomes the default only in a final evidence commit if every gate passes.

**Tech Stack:** Rust 2024, libfuse3 low-level API and C shim, shared StudioFS VFS metrics, Bash, Python 3 standard library, bundled `target/bench/studiofs-bench`, cgroup v2, `pidstat`, Cargo tests.

---

## Dependency and execution order

Execute this plan only after both prerequisite plans are green:

1. `docs/superpowers/plans/2026-07-14-linux-fuse-async-write-correctness.md`;
2. `docs/superpowers/plans/2026-07-14-shared-os-cache-coherence.md`.

The first prerequisite supplies handle-scoped barriers, safe teardown, and the Direct `ASYNC_DIO` candidate. The second supplies `FuseInodeLiveness`, `LivenessCheckedKernelInvalidator`, `prepare_os_cache_open`, VFS-owned handle lifetime, and neutral cache-decision metrics. Do not duplicate either implementation in the Linux adapter.

## Scope and invariants

- `forget` always replies with `fuse_reply_none`, even when callback state is unavailable.
- Lookup liveness reserves one reference immediately before an entry/create reply and rolls it back when libfuse rejects the reply; this prevents a concurrent `forget` from overtaking post-reply observation.
- An `open` reply never creates a lookup reference: the request already names a kernel-live inode, and synthesizing another count would have no matching `forget`. Unknown membership records an invariant counter but does not mutate liveness.
- Linux lookup liveness is owned only by successful entry/create replies and kernel `forget`; namespace removal never fabricates a forget or force-drops a still kernel-owned reference. Shared VFS retirement still clears OS-cache generation state.
- Direct opens call the shared prepare API with `Disabled`; Kernel opens use `Enabled`. Both therefore get shared foreground/writer lifetime.
- `keep_cache` is true only for Kernel mode plus `Reuse(StableGeneration)`; it is false for all Direct opens and all conservative `Replace` decisions.
- Requested FUSE tuning and actual negotiated connection values are different fields and are never presented as equivalent.
- Libfuse-header support for `parallel_direct_writes` is reported separately from kernel acceptance of `ASYNC_DIO`; Rocky Linux 9's libfuse 3.10 path remains buildable.
- Libfuse before 3.12 uses one receive thread rather than its unbounded legacy
  multithread loop; current headers use the configured worker cap, and the
  selected loop is observable.
- Callback latency is measured from Rust callback entry through the libfuse reply call.
- Interval percentiles are rebuilt from raw histogram bucket deltas; cumulative quantiles or maxima are never subtracted.
- Warm-cache proof uses unchanged read-callback counters, not elapsed time alone.
- Every measured run starts only after local dirty state and the upload queue are idle.
- Profiling runs are diagnostic and excluded from throughput medians.
- Kernel policy remains opt-in unless every correctness, throughput, CPU, RSS, geometry, error, and tail gate passes.
- Final memory evidence uses a fresh cgroup v2 per repetition to distinguish daemon anonymous/RSS growth from file-backed page-cache growth.
- Tests and performance artifacts live under the repository `target/` tree; temporary workload files are removed after their evidence is captured.

## File map

- Modify: `crates/studiofs-linux-fuse/csrc/low_level_ops.c` — `forget` wiring and negotiated-connection callback.
- Modify: `crates/studiofs-linux-fuse/Cargo.toml` — only the test dependencies required by the mounted PostgreSQL/S3 fixture.
- Modify: `crates/studiofs-linux-fuse/src/low_level.rs` — Linux liveness, cache decisions, invalidator wrapper, reply mapping, geometry state, callback timing, unit and mounted tests.
- Modify: `crates/studiofs-linux-fuse/src/lib.rs` — any public connection snapshot type needed by mount wiring.
- Modify: `crates/studiofs-bridge/src/fuse_liveness.rs` — keep Linux unknown-open observations in the same coherent liveness snapshot generation.
- Modify: `crates/studiofs-vfs/src/lib.rs` — export the extended neutral liveness snapshot.
- Modify: `crates/studiofs-vfs/src/vfs/metrics.rs` — callback size/latency histograms and barrier snapshots.
- Modify: `crates/studiofs-vfs/src/vfs.rs` — atomic recorders, snapshot/reset behavior, exports.
- Modify: `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs` — separate bridge callback/barrier histograms and negotiated gauges.
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs` — histogram serialization/reset/runtime-surface tests.
- Modify: `crates/studiofs/src/cli/status.rs` — negotiated versus requested geometry and callback tails.
- Modify: `crates/studiofs/src/cli/benchmark_profile_common.rs` — include new bridge counters/histograms in benchmark snapshots.
- Modify: `crates/studiofs/src/cli/perf.rs` — print and serialize new runtime-stat fields.
- Modify: `crates/studiofs/src/cli/mount/mod.rs` — remove the stale statement that Linux mount is unsupported.
- Create: `tools/perf/linux-cache-gate.sh` — one-mode mounted evidence collector.
- Create: `tools/perf/linux-cache-matrix.sh` — fresh mount/cache/cgroup orchestration with balanced run order and strict cleanup.
- Create: `tools/perf/linux_cache_gate_summary.py` — median/MAD comparison and promotion decision.
- Create: `tools/perf/test_linux_cache_gate.py` — dry-run, parser, and threshold tests.
- Modify: `tools/perf/README.md` — exact three-mode workflow and artifact contract.
- Modify: `docs/development.md`, `docs/configuration.md`, `docs/tests.md`, `docs/platform-bridges.md`, `docs/local-first-validation-matrix.md` — supported Linux behavior and validation commands.
- Modify: `docs/perf/benchmark-methodology.md`, `docs/perf/benchmark-validation.md`, `docs/perf/runtime-stats-schema.md` — CPU, histogram, cold/warm, and gate definitions.
- Create after real runs: `docs/perf/linux-cache-performance-result.md` — committed evidence summary and final policy decision.
- Create only if Kernel passes: `docs/adr/0011-linux-kernel-cache-default-after-evidence.md` — supersede ADR 0002 without rewriting history.
- Reference only: `target/bench/README.md` and `target/bench/studiofs-bench` — bundled benchmark contract.
- Reference only: `/home/val/dev/studiofs-bench` — modify only if the bundled binary cannot express a required workload.
- Reference only: `.rwx/rocky.yml` and `.github/workflows/rust.yml` — real Rocky/libfuse 3.10, macOS, and Windows verification runners; Linux-hosted cfg-elided commands are not cross-platform evidence.

## Task 1: Wire Linux `forget` and inode liveness

- [ ] **Step 1: Add failing callback-table and liveness tests**

In Linux low-level tests add these exact cases:

```text
linux_low_level_ops_installs_forget_callback
linux_forget_without_callback_state_still_replies_none
linux_forget_decrements_exact_lookup_count
linux_forget_zero_count_is_a_noop
linux_entry_reply_reserves_before_reply_and_rolls_back_failure
linux_create_reply_reserves_before_reply_and_rolls_back_failure
linux_open_reply_does_not_change_lookup_reference_count
linux_open_of_unobserved_inode_records_invariant_without_creating_reference
linux_unlink_does_not_force_drop_lookup_reference
linux_rename_overwrite_does_not_force_drop_lookup_reference
linux_unlinked_open_inode_remains_observed_until_forget
linux_invalidator_suppresses_unknown_or_forgotten_inode
linux_liveness_runtime_stats_publish_reference_and_notification_deltas
```

Use a fake reply seam returning `0` or `-ENOENT` for the two reply-order tests. The seam asserts the reference is already visible while the reply function runs, and the failed case asserts it is removed afterward. Do not depend on a mounted kernel for this ordering.

- [ ] **Step 2: Verify RED**

```bash
cargo test -p studiofs-linux-fuse linux_forget_ -- --nocapture
cargo test -p studiofs-linux-fuse linux_entry_reply_reserves_ -- --nocapture
```

Expected: tests fail because Linux has no `forget` callback or liveness field.

- [ ] **Step 3: Add the raw reply and Rust callback**

Declare `fuse_reply_none` in the Linux raw bindings. Factor the body through a
small closure-based helper so a test can prove the reply is unconditional even
without callback state; opaque fake `fuse_req_t` values must not be passed to
the exported callback. The production callback remains:

```rust
#[unsafe(no_mangle)]
unsafe extern "C" fn studiofs_linux_fuse_forget(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    nlookup: u64,
) {
    if let Some(state) = callback_state(req) {
        state.inode_liveness.forget(ino, nlookup);
        state.publish_liveness_stats();
    }
    unsafe { raw::fuse_reply_none(req) };
}
```

Add `inode_liveness: Arc<FuseInodeLiveness>` to `SessionState`, initialize it once per mount, and share that exact `Arc` with the invalidator wrapper.

- [ ] **Step 4: Install `.forget` in the C operation table**

Add this declaration:

```c
extern void studiofs_linux_fuse_forget(fuse_req_t req, fuse_ino_t ino,
                                       uint64_t nlookup);
```

Add `.forget = studiofs_linux_fuse_forget` immediately after `.lookup` in `STUDIOFS_LOW_LEVEL_OPS`. Add `studiofs_linux_fuse_low_level_ops_has_forget_callback` and bind it in the Rust test module.

- [ ] **Step 5: Reserve and reconcile entry replies centrally**

Change `reply_entry_result` and `reply_create` to reserve with `state.inode_liveness.observe(attr.ino)` immediately before `fuse_reply_entry`/`fuse_reply_create`. Inspect the returned `c_int`; on nonzero return, immediately call `forget(attr.ino, 1)` to roll back the rejected entry. This ordering prevents a kernel `forget` dispatched on another loop thread from arriving before the reference exists.

For `open`, call only `is_observed(ino)`. A false result calls a new tracker
method that increments `open_unknown_inode` under the existing
`FuseInodeLiveness` stats mutex and advances that same `snapshot_generation`;
do not use a separate Linux atomic that could publish conflicting values for
one generation. Emit a rate-limited invariant warning. Successful or failed
`fuse_reply_open` never changes lookup counts. The existing failed-reply handle
cleanup in Task 2 is independent of lookup liveness.

Do not observe `getattr` or directory reads, and do not retain a reference after a failed entry reply.

- [ ] **Step 6: Leave Linux lookup retirement to `forget`**

Do not call `FuseInodeLiveness::close` from Linux unlink, rmdir, or rename. A successful entry/create reply created a kernel reference, and only a matching `forget` removes it; this also correctly covers an open-but-unlinked file. Namespace callbacks keep using their existing VFS operations with no pre-operation target capture or new outcome abstraction.

The shared VFS retirement operation from the prerequisite clears generation identity after final unlink/rename commit. Publish Linux liveness after observe, rollback, and forget only. This small separation removes reply-versus-retirement resurrection races without an epoch/tombstone protocol.

- [ ] **Step 7: Wrap the Linux invalidator**

In `Session::kernel_invalidator`, construct the platform invalidator first, wrap it with the shared check, and then with a Linux-local publishing wrapper:

```rust
let state = self.state.as_ref().expect("mounted session owns callback state");
let checked = Arc::new(LivenessCheckedKernelInvalidator::new(
    Arc::clone(&state.inode_liveness),
    platform_invalidator,
));
Arc::new(PublishingLinuxKernelInvalidator::new(
    checked,
    Arc::clone(&state.inode_liveness),
    Arc::clone(&state.vfs),
))
```

The outer wrapper publishes after forwarded or suppressed notification attempts. It may retain only the VFS plus liveness tracker, not the whole session state.

Add `open_unknown_inode` to the shared `FuseInodeLivenessStats` and neutral
`VfsFuseLivenessSnapshot`. Export them from `.studiofs-runtime-stats`; the
benchmark gate uses deltas to prove lookup/forget and notification
forwarding/suppression actually occurred. Include `snapshot_generation`, update
it in the tracker with every counter change, and reject a concurrently
published snapshot older than the current generation so monotonic values
cannot move backward.

- [ ] **Step 8: Verify GREEN**

```bash
cargo test -p studiofs-bridge fuse_liveness -- --nocapture
cargo test -p studiofs-linux-fuse linux_ -- --nocapture
cargo test -p studiofs-vfs runtime_stats_surface_reports_fuse_liveness -- --nocapture
```

Expected: all tests pass; external invalidation remains outside liveness locks.

- [ ] **Step 9: Commit**

```bash
git add crates/studiofs-bridge/src/fuse_liveness.rs crates/studiofs-linux-fuse/csrc/low_level_ops.c crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs-vfs/src/lib.rs crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat(linux): track fuse inode liveness"
```

## Task 2: Consume shared cache decisions at the serialized reply boundary

- [ ] **Step 1: Add a pure flag-mapping truth table**

Add table-driven tests for `linux_fuse_open_cache_policy_for`:

| Mount policy | Async accepted | Shim field | Shared decision | Direct I/O | Keep cache | Parallel direct writes |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Direct | true | true | Replace | true | false | true |
| Direct | true | true | Reuse | true | false | true |
| Direct | true | false | Reuse | true | false | false |
| Direct rollback | false | true | Reuse | true | false | false |
| Kernel | false | true | FirstObservation/Replace | false | false | false |
| Kernel | false | true | GenerationChanged/Replace | false | false | false |
| Kernel | false | true | StableGeneration/Reuse | false | true | false |

Also require open and create to use the same mapper. Initialize both
`cache_policy: LinuxFuseCachePolicy` and `requested_async_dio: bool` in
`SessionState` from the environment at mount construction. Derive requested
writeback-cache negotiation from `cache_policy`, and use these stored fields
for init negotiation, preparation, flag mapping, metrics, and status;
callbacks must not reread the environment. Add an internal constructor taking
explicit cache settings for parallel-safe tests rather than mutating the
process environment. `Session::mount` must also build argv through
`argv_strings_for_cache_policy(volume_name, state.cache_policy)`; production
must not fall back to `argv_strings(volume_name)` and reread policy indirectly.

- [ ] **Step 2: Verify RED**

```bash
cargo test -p studiofs-linux-fuse linux_cache_decision_flags_ -- --nocapture
```

Expected: the Kernel reuse case fails because Linux currently derives `keep_cache` only from flags.

- [ ] **Step 3: Return an opened-handle value from session methods**

Add private values:

```rust
#[derive(Clone, Copy, Debug)]
struct LinuxFuseOpenedFile {
    fh: u64,
    ino: u64,
}

#[derive(Clone, Debug)]
struct LinuxFuseCreatedFile {
    attr: InodeAttr,
    opened: LinuxFuseOpenedFile,
}
```

After VFS open/create succeeds, return the handle and inode without preparing
the shared cache state. Add the prerequisite's `FuseOpenReplySerializer` to
`SessionState`; preparation belongs in the synchronous reply helper so its
decision cannot be overtaken by another same-inode reply.

- [ ] **Step 4: Map the decision only at the adapter boundary**

Validate `fuse_file_info` with the existing `require_file_info(fi)` before the
VFS open/create because its flags are needed to open the handle. Remove the
unreachable post-open null-fi cleanup branch and do not add a reply-outcome
struct.

Use one closure-based completion helper shared by open/create tests and
production. Inside the inode's `FuseOpenReplySerializer` section it derives
participation from `state.cache_policy`, calls
`prepare_os_cache_open(fh, participation)`, maps flags, and invokes the reply
closure before releasing the section. Production alone mutates opaque
`fuse_file_info`; tests can block or fail the closure safely. The decision is
non-fallible; an observation problem cannot turn a successful VFS open into an
open error. The shared pure mapper receives `state.cache_policy`,
`state.requested_async_dio`, the C-shim support bit, and
`OsCacheOpenDecision`; Task 3 replaces the requested async input with actual
negotiated acceptance without changing this boundary:

```rust
let keep_cache = cache_policy == LinuxFuseCachePolicy::Kernel
    && decision.disposition() == OsCacheDisposition::Reuse;
LinuxFuseOpenCachePolicy {
    direct_io: cache_policy == LinuxFuseCachePolicy::Direct,
    keep_cache,
    parallel_direct_writes: cache_policy == LinuxFuseCachePolicy::Direct
        && async_dio_enabled
        && supports_parallel_direct_writes,
}
```

Do not reconstruct generation, dirty, access-mode, or writer facts in Linux.

For create, reserve the lookup reference immediately before the combined reply,
inside the same serialized section, and roll it back if libfuse returns
nonzero. Open still only checks `is_observed` and never mutates lookup counts.

If a prepared raw reply returns nonzero, perform create-reference rollback,
call `vfs.abort_os_cache_open(fh)`, and synchronously call
`release_fh_with_outcome(fh)` through the mount runtime before leaving the
serializer. Normalize only close-time `ENOENT`, and log any other cleanup error
because the kernel request can no longer be replied. This local-only cleanup
removes the VFS handle and finishes shared foreground/writer lifecycle.

- [ ] **Step 5: Add Linux integration tests over the shared seam**

Using `ContractVfsFixture`, require:

```text
linux_kernel_first_stable_read_replaces_cache
linux_kernel_second_identical_read_reuses_cache
linux_kernel_changed_generation_replaces_cache
linux_kernel_dirty_inode_replaces_cache
linux_kernel_active_writer_replaces_cache
linux_writer_open_never_keeps_cache
linux_direct_open_enrolls_foreground_lifetime_without_keep_cache
linux_open_prepare_is_counted_once
linux_same_inode_prepare_reply_is_serialized
linux_different_inode_reply_progresses_while_one_reply_is_blocked
linux_rejected_replace_reply_makes_next_open_replace_again
linux_failed_open_reply_releases_handle_and_lifecycle
linux_failed_create_reply_releases_handle_and_lifecycle
```

Use a blocking fake-reply seam and channels for the concurrency cases. For
every case release the returned handle and assert the shared
foreground/writer gauges return to their baseline.

- [ ] **Step 6: Verify GREEN and rollback**

```bash
cargo test -p studiofs-vfs os_cache_ -- --nocapture
cargo test -p studiofs-linux-fuse linux_ -- --nocapture
```

Expected: all tests pass and Direct never sets `keep_cache`.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs
git commit -m "feat(linux): consume shared os cache decisions"
```

## Task 3: Publish actual negotiated FUSE geometry

- [ ] **Step 1: Add failing configured-versus-negotiated tests**

Add these exact tests:

```text
negotiated_geometry_preserves_requested_and_effective_values
negotiated_geometry_rejected_async_disables_parallel_direct_writes
negotiated_geometry_missing_shim_field_disables_parallel_direct_writes
fuse_connection_snapshot_preserves_pre_init_absence
runtime_stats_surface_reports_fuse_connection_geometry
mounted_status_formats_configured_and_negotiated_fuse_geometry
```

Require independent configured and negotiated fields for background,
congestion, `ASYNC_DIO`, and `WRITEBACK_CACHE`. Max read/write/readahead have
only negotiated fields because StudioFS does not currently configure them; do
not invent requested values that are always absent. One fixture sets configured
and negotiated background/congestion differently and proves the snapshot
preserves both. A second requests Direct async while recording
`negotiated_async_dio = false` and asserts the open reply leaves
`parallel_direct_writes` false. A third records accepted async on a shim without
the per-open field and also expects false.
The fixture also reports whether this header selected the pre-3.12 single
receive loop; it is support state, not a requested geometry value.

- [ ] **Step 2: Verify RED**

```bash
cargo test -p studiofs-linux-fuse negotiated_geometry_ -- --nocapture
cargo test -p studiofs-vfs fuse_connection_snapshot_preserves_pre_init_absence -- --nocapture
```

Expected: compilation fails because the neutral connection snapshot does not exist.

- [ ] **Step 3: Add the adapter-owned negotiated snapshot**

Use a connection-owned, mode-neutral type rather than putting geometry into async-write state:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LinuxFuseNegotiatedGeometry {
    max_read: u32,
    max_write: u32,
    max_readahead: u32,
    max_background: u32,
    congestion_threshold: u32,
    async_dio: bool,
    writeback_cache: bool,
    parallel_direct_writes_supported: bool,
    single_thread_receive_loop: bool,
}
```

Store it as `OnceLock<LinuxFuseNegotiatedGeometry>` in `SessionState`; init
writes it once and every open reads it without a mutex. An unset lock means the
init callback has not reported a real connection yet and must never be rendered
as negotiated zeroes. Before negotiation, the open/create mapper
conservatively treats async acceptance as false. Add
`VfsFuseConnectionSnapshot` with:

```rust
pub configured_max_background: Option<u32>,
pub configured_congestion_threshold: Option<u32>,
pub requested_async_dio: Option<bool>,
pub requested_writeback_cache: Option<bool>,
pub negotiated_max_read_bytes: Option<u32>,
pub negotiated_max_write_bytes: Option<u32>,
pub negotiated_max_readahead_bytes: Option<u32>,
pub negotiated_max_background: Option<u32>,
pub negotiated_congestion_threshold: Option<u32>,
pub negotiated_async_dio: Option<bool>,
pub negotiated_writeback_cache: Option<bool>,
pub parallel_direct_writes_supported: Option<bool>,
pub single_thread_receive_loop: Option<bool>,
```

Retain the old geometry fields inside `VfsFuseAsyncWriteSnapshot` only for one decode-compatibility cycle and leave them at defaults in new snapshots; publish live connection values only through the neutral snapshot.

- [ ] **Step 4: Record values after feature negotiation in C**

Declare:

```c
extern void studiofs_linux_fuse_record_negotiated_connection(
    void *userdata,
    uint32_t max_read,
    uint32_t max_write,
    uint32_t max_readahead,
    uint32_t max_background,
    uint32_t congestion_threshold,
    int async_dio,
    int writeback_cache,
    int parallel_direct_writes_supported,
    int single_thread_receive_loop);
```

At the end of `studiofs_linux_fuse_init`, capture the return value from each of
the two prerequisite capability-request helper calls and pass those exact
accepted booleans together with the five connection fields,
`studiofs_linux_fuse_supports_parallel_direct_writes()`, and the prerequisite's
single-loop probe. Do not call `fuse_set/get_feature_flag` directly: those
symbols are unavailable on the supported libfuse 3.10 path. The Rust callback
sets the previously empty `OnceLock` and republishes VFS observability.
Once this state exists, change the open/create mapper input from the requested
Direct async decision to
`geometry.get().is_some_and(|value| value.async_dio)` plus
`parallel_direct_writes_supported`, so the per-open bit is false before init,
when the kernel rejected `FUSE_CAP_ASYNC_DIO`, or when the installed header
lacks the field.

Add a small C-shim init callback fixture that exercises this wiring and checks
the exact accepted booleans; Rust-only synthetic snapshot tests are not enough
to prove the real init path records them.

- [ ] **Step 5: Render configured and negotiated values distinctly**

Add these gauges to `.studiofs-runtime-stats`:

```text
fuse_negotiated_max_read_bytes
fuse_negotiated_max_write_bytes
fuse_negotiated_max_readahead_bytes
fuse_negotiated_max_background
fuse_negotiated_congestion_threshold
fuse_negotiated_async_dio
fuse_negotiated_writeback_cache
fuse_parallel_direct_writes_supported
fuse_single_thread_receive_loop
```

In status output label real inputs `configured` and connection results `negotiated`; show max-read only on the negotiated line. Do not silently repurpose serialized fields. Absence of negotiated writeback skips only writeback-specific mounted assertions; it never skips the Kernel cache-coherence matrix.

- [ ] **Step 6: Verify GREEN**

```bash
cargo test -p studiofs-linux-fuse negotiated_geometry_ -- --nocapture
cargo test -p studiofs-vfs fuse_connection_snapshot_preserves_pre_init_absence -- --nocapture
cargo test -p studiofs-vfs runtime_stats_surface_reports_fuse_connection_geometry -- --nocapture
cargo test -p studiofs mounted_status_formats_configured_and_negotiated_fuse_geometry -- --nocapture
```

Expected: all tests pass and a mismatched requested/actual fixture remains visibly mismatched.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-linux-fuse/csrc/low_level_ops.c crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs crates/studiofs-vfs/src/vfs/tests.rs crates/studiofs/src/cli/status.rs
git commit -m "feat(linux): report negotiated fuse geometry"
```

## Task 4: Add callback size, latency, and barrier histograms

- [ ] **Step 1: Add failing histogram tests**

Require:

```text
fuse_read_size_histogram_places_boundary_values_once
fuse_write_size_histogram_places_overflow_values_once
fuse_callback_latency_reports_p50_p95_p99_and_max
fuse_flush_fsync_release_histograms_are_independent
fuse_histogram_reset_baseline_subtracts_monotonic_buckets
fuse_latency_snapshot_exports_raw_monotonic_buckets_and_saturation
runtime_stats_surface_exports_each_fuse_callback_histogram
runtime_stats_surface_exports_latency_overflow_bucket
linux_read_callback_metrics_cover_reply_and_use_requested_size
linux_barrier_callback_metrics_cover_reply_and_count_reply_rejection
```

Use exact observations at every bucket boundary plus one overflow value.

- [ ] **Step 2: Verify RED**

```bash
cargo test -p studiofs-vfs fuse_read_size_histogram_ -- --nocapture
cargo test -p studiofs-vfs fuse_flush_fsync_release_histograms_ -- --nocapture
```

Expected: histogram fields are missing.

- [ ] **Step 3: Add one fixed request-size histogram**

Use inclusive upper bounds:

```rust
const FUSE_SIZE_BUCKET_UPPER_BYTES: [u64; 6] = [
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
];
```

The private recorder owns seven `AtomicU64` counters, with the final counter as overflow. Publish:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct VfsFuseSizeHistogram {
    pub bucket_upper_bounds_bytes: Vec<u64>,
    pub bucket_counts: Vec<u64>,
}
```

Add `size_histogram` and `latency: VfsWritePathStageLatencyStats` to `VfsFuseIoStats` under `#[serde(default)]`. The latency snapshot must retain fixed upper bounds, raw cumulative bucket counts, overflow count, and saturation flags in addition to display quantiles; later interval analysis is impossible from quantiles alone. For every read callback, record the requested callback argument `size` in this histogram on both success and error. Returned bytes stay in the existing byte counter and never define request-size buckets.

For the reused `LatencyHistogramSnapshot`, `bucket_counts` are mutually
exclusive monotonic buckets and `bucket_counts.len() ==
bucket_upper_bounds_ns.len() + 1`; the final count is overflow. Diagnostics
reset must capture per-histogram baselines and saturating-subtract them in
`write_path_observability_snapshot`. It must not call `.reset()` on live FUSE
histograms because interval collectors require monotonic raw counters. Test
observations before reset, immediately after reset, and after new observations.

- [ ] **Step 4: Add barrier snapshots and recorders**

Add:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct VfsFuseBarrierStats {
    pub calls: u64,
    pub errors: u64,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
    pub latency: VfsWritePathStageLatencyStats,
}
```

Extend `VfsFuseCallbackStats` with `flush`, `fsync`, and `release`. Internally reuse the existing fixed `LatencyHistogram` implementation for all five callback kinds.

Expose:

```rust
pub enum FuseBarrierKind {
    Flush,
    Fsync,
    Release,
}

pub fn record_fuse_barrier_callback(
    &self,
    kind: FuseBarrierKind,
    callback_elapsed: Duration,
    errored: bool,
)
```

- [ ] **Step 5: Measure through the reply boundary**

For read, flush, fsync, and release: make the narrow Rust reply helpers return
libfuse's `c_int` and create `Instant` as the first instruction in each exported
Rust callback, before callback-state validation. Execute the session operation,
call the libfuse reply, then record elapsed time with `errored =
operation_failed || reply_status != 0` on every synchronous success/error path.
The async-write prerequisite already measures through `reply_write`; route it
into the histogram without double-counting. Synchronous writes use the same
recorder once.

Under `cfg(test)`, extend the existing narrow request/reply seam (for example
`FuseReqPtr`) with buffer and error reply closures. Use it to prove read metrics
cover a blocked reply and bucket the requested size, and that a rejected
flush/fsync/release reply increments errors. Do not pass opaque fake requests
to exported callbacks.

- [ ] **Step 6: Export separate runtime histograms**

Add `insert_latency_with_buckets` beside `insert_latency` and export:

```text
read_callback
write_callback
flush_callback
fsync_callback
release_callback
```

`insert_latency_with_buckets` zips the normal bounds/counts, then explicitly
appends `RuntimeLatencyBucket { le_ns: u64::MAX, count:
counts[bounds.len()] }`. A runtime-surface test must assert that exact final
bucket so 5s+ latency cannot disappear from gate evidence.

Preserve aggregate `callback` for one compatibility cycle, deriving it from snapshots instead of maintaining a second live recorder. Update benchmark-profile, perf, and status outputs with p50/p95/p99/cumulative-max, size buckets, raw latency buckets, overflow, and saturation.

Document the interval rule used by the gate: require identical bounds, subtract cumulative bucket counts, reject any negative delta, overflow, saturation, or zero-sample interval, and recompute p50/p95/p99 from the delta buckets. Never subtract two precomputed quantiles or cumulative maxima. Define `interval_max_upper_ms` as the upper bound of the last nonzero delta bucket; if that bucket is overflow, the run fails instead of guessing a maximum.

- [ ] **Step 7: Verify GREEN**

```bash
cargo test -p studiofs-vfs fuse_ -- --nocapture
cargo test -p studiofs-vfs runtime_stats_surface_exports_each_fuse_callback_histogram -- --nocapture
cargo test -p studiofs-linux-fuse linux_read_callback_metrics_ -- --nocapture
cargo test -p studiofs-linux-fuse linux_barrier_callback_metrics_ -- --nocapture
cargo test -p studiofs benchmark_profile -- --nocapture
cargo test -p studiofs perf_ -- --nocapture
```

Expected: exact counts, sorted inclusive bounds, reset snapshots without underflow, and enough raw data to reconstruct interval quantiles and a bounded interval maximum.

- [ ] **Step 8: Commit**

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs crates/studiofs-vfs/src/vfs/tests.rs crates/studiofs/src/cli/status.rs crates/studiofs/src/cli/benchmark_profile_common.rs crates/studiofs/src/cli/perf.rs
git commit -m "feat: publish fuse callback latency histograms"
```

## Task 5: Add the mounted cache-coherence and rollback matrix

- [ ] **Step 1: Extend the mounted fixture with observable snapshots**

Add helpers to the existing ignored Linux mounted-test fixture to:

- capture `write_path_observability_snapshot` before and after an operation;
- wait until local dirty bytes, pending uploads, and active uploads are zero with a bounded timeout;
- inspect negotiated geometry recorded during mount;
- unmount and join the session without using sleeps as correctness assertions.

The in-process mounted fixture reads snapshots directly from its `Arc<StudioVfs>`;
it must not read `.studiofs-runtime-stats` through the mount because that read
would itself change Linux callback counters. Before asserting negotiated
geometry, perform one real operation through the mount and wait with a bounded
timeout for the init callback to publish `Some(...)`.

Also adapt only the minimum setup from the existing two-client PostgreSQL/S3
fixture in `crates/studiofs-vfs/tests/vfs_test.rs`: add direct Linux test
dependencies in `crates/studiofs-linux-fuse/Cargo.toml`, create a small local
volume/setup helper in the Linux test module, mount the reader VFS through
Linux Kernel mode, and keep a second writer VFS on the same volume. Do not
extract a new shared fixture abstraction, duplicate unrelated VFS test helpers,
or add a test-only "force invalidation" hook that bypasses the production
metadata listener. Observe the production metadata listener through the
existing runtime and kernel-invalidation counters.

- [ ] **Step 2: Add one deterministic coherence matrix test**

Add ignored `mounted_kernel_cache_coherence_matrix` with fresh paths for:

```text
write, fsync, close, reopen, exact read
append and exact final length/content
truncate smaller and larger
same-size overwrite
sparse write and zero-filled hole
reader open while writer changes content
rename while open
unlink/delete while open
hardlink update and final-link retirement
first stable reopen replaces and second identical reopen reuses
second stable warm read adds zero FUSE read callbacks
```

Every subcase asserts bytes and relevant cache-decision/liveness counter deltas. The warm subcase requires an exact checksum, a positive read-callback delta on its first fresh read, cache decisions `Replace` then `Reuse`, and zero FUSE/VFS read-callback delta on the second fresh read; zero callbacks without the first-read proof is not a pass. After the first read, wait for live-read gauges to reach zero and callback counters to remain stable across consecutive bounded polls before taking the second-read baseline, so late readahead cannot contaminate that interval.

- [ ] **Step 3: Prove production remote invalidation with two clients**

Add ignored `mounted_kernel_remote_listener_invalidation` requiring the existing
PostgreSQL/S3 test environment. Publish v1, wait for idle, and read v1 through
the mount until the reader records stable reuse. Capture this inode's reader
metadata payload IDs and a liveness baseline. Then publish a same-inode,
same-size v2. Require the production listener to change the payload IDs for
that exact inode, increment forwarded invalidation by the exact expected delta,
and leave suppressed invalidation unchanged. A fresh read through the mount
must return v2's exact bytes/checksum, and the cache decision must replace the
stale generation before later reuse. This baseline prevents an initial
full-snapshot invalidation or an old cumulative counter from passing the test.

If the environment is absent, print an explicit skip reason and do not record a
pass. When `STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST=1`, absence of the
environment or any attempted skip is a test failure. The final correctness
preflight sets this variable, so a zero cargo status proves the production
listener path actually executed rather than silently skipping.

- [ ] **Step 4: Add ordering and teardown tests shared with Direct**

Retain prerequisite same-file Direct parallel-write and forced-unmount tests. Add:

```text
mounted_cross_file_fsync_and_close_do_not_share_async_watermark
mounted_kernel_writeback_flush_and_release_are_locally_durable
mounted_direct_rollback_preserves_reopen_visibility
```

Exact cross-handle timing proof stays in deterministic ledger unit tests; mounted isolation proves bounded progress and exact content.

Move every mounted-test path used by the correctness allowlist under `target/`
and route it through one bounded teardown helper. Replace the existing smoke
sleep with an observable readiness condition. On every outcome, attempt the
normal unmount, fall back to the bounded forced unmount when needed, join the
session, prove `findmnt` no longer sees the mount, and assert pending
reply/write plus OS-cache lifecycle gauges are zero.

- [ ] **Step 5: Assert actual negotiation**

Only `mounted_kernel_writeback_flush_and_release_are_locally_durable` skips when `negotiated_writeback_cache == Some(false)` and emits that unsupported capability as the explicit reason. The rest of the Kernel coherence and remote-listener matrix still runs. Direct async requires `negotiated_async_dio == Some(true)`; on headers with `parallel_direct_writes_supported == false`, same-file parallelism is reported unsupported rather than passed. Direct rollback requires negotiated async false.

- [ ] **Step 6: Run all three modes serially and record correctness provenance**

Run an explicit allowlist; never use the broad `mounted_` filter because the
three modes intentionally contain mutually exclusive negotiation assertions.
The matrix runner executes each listed test as its own cargo command and fails
if the filter executes anything other than one test:

```text
Direct serial (`cache=direct`, `async=0`):
  mounted_smoke_mounts_and_unmounts_cleanly
  mounted_regular_file_io_is_visible_across_reopen
  mounted_namespace_mutations_are_visible_through_readdir
  mounted_direct_rollback_preserves_reopen_visibility

Direct async (`cache=direct`, `async=1`):
  mounted_same_file_parallel_direct_writes_preserve_disjoint_regions
  mounted_forced_unmount_drains_admitted_and_waiting_raw_requests
  mounted_cross_file_fsync_and_close_do_not_share_async_watermark

Kernel (`cache=kernel`):
  mounted_kernel_cache_coherence_matrix
  mounted_kernel_writeback_flush_and_release_are_locally_durable

Kernel remote (`cache=kernel`, `STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST=1`):
  mounted_kernel_remote_listener_invalidation
```

Expected: every supported case passes with zero leaked mounts, pending replies, or lifecycle gauges.

The matrix runner added in Task 6 executes these commands once per exact
revision before performance runs, with
`STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST=1` on the remote-listener command,
and writes `<output-root>/correctness-manifest.json`. The manifest contains the
git revision/dirty state, StudioFS and benchmark SHA-256 values, each required
test's full name, mode/environment, command, start/end timestamps, exit status,
log path and SHA-256, plus explicit `executed` and `skipped` booleans. It is
valid only when every required test has `exit_status = 0`, `executed = true`,
and `skipped = false`. Derive those booleans by matching each full test name's
`... ok` line and the harness summary in the captured log; a cargo command that
matched zero tests is a failure even when cargo exits zero. Capability-specific
cases may be marked `applicable = false` only from the recorded negotiated
capability, never as a substitute for the required coherence, rollback, or
remote-listener tests. Do not hand-edit this file.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-linux-fuse/Cargo.toml crates/studiofs-linux-fuse/src/low_level.rs Cargo.lock
git commit -m "test(linux): prove mounted cache coherence"
```

## Task 6: Build the repeatable Linux cache/CPU gate

- [ ] **Step 1: Add failing CLI and analyzer tests**

Create `tools/perf/test_linux_cache_gate.py` with standard-library `unittest` cases for:

```text
help lists mode, workload, mount point, mount pid, runs, cgroup, and output root
dry-run writes environment.json, manifest.json, summary.md, and decision.md
real run rejects a target whose findmnt type is not fuse.studiofs
mode/environment mismatch is rejected
idle parser requires three consecutive zero snapshots
idle parser includes async reservations and foreground/writer lifecycles
diagnostics attach snapshot must match mount pid/path/schema and has no mounted-file fallback
median and MAD are correct for odd and even samples
5 percent throughput boundary passes and a larger regression fails
Direct async rejection makes Direct serial the Kernel baseline
CPU regression beyond combined MAD fails
foreground average and sampled-peak core regressions fail
callback, async, cancellation, or cleanup error fails
histogram interval rejects changed bounds, negative deltas, overflow, saturation, and zero samples
histogram applicability allows zero reads only for the Kernel second warm pass
Kernel warm proof requires first-read callbacks, checksum, Replace-to-Reuse, then zero second-read callbacks
negotiated geometry mismatch fails
fresh cgroup is required for final evidence
missing, skipped, checksum-mismatched, or wrong-revision correctness manifest fails
daemon RSS/anonymous and cgroup file-cache thresholds fail independently
manifest text cannot waive a numeric memory failure
workload manifests reject wrong byte or file counts
bench report rejects stopped, cleanup error, duplicate/missing passes, wrong processed bytes, or extra JSON
python client report rejects incomplete work or checksum mismatch
matrix dry-run rotates mode/workload order and uses one mount per repetition
matrix records unmount timeout or nonzero mount exit as cleanup failure
matrix rejects missing, duplicate, or extra scheduled tuples and reused cgroups
promotion requires exactly five repetitions; diagnostic three-run output is ineligible
final provenance rejects dirty source trees or a mount executable checksum mismatch
cgroup preflight requires empty counters, memory controller, and writable required files
```

- [ ] **Step 2: Verify RED**

```bash
python3 -m unittest tools.perf.test_linux_cache_gate -v
```

Expected: import/file errors because the gate and analyzer do not exist.

- [ ] **Step 3: Implement the one-mode collector**

Create `tools/perf/linux-cache-gate.sh` with:

```text
linux-cache-gate.sh \
  --mode direct-serial|direct-async|kernel \
  --workload large-single|fixed-1m|small-128k|warm-reopen|same-file-parallel|cross-file-barriers \
  --mount-point PATH \
  --mount-pid PID \
  --runs N \
  --cgroup PATH \
  --bench PATH \
  --studiofs PATH \
  --correctness-manifest PATH \
  --output-root PATH \
  [--workload-bytes N] \
  [--dry-run]
```

Defaults: one run, `target/bench/studiofs-bench`, `target/release/studiofs`, `4000000000` bytes, and `target/perf/linux-cache-gate`. `--workload` and `--correctness-manifest` are required for real runs. Follow `linux-juicefs-parity-gate.sh` argument and error style. More than one run against one mount is diagnostic only; the final analyzer rejects it because every final mode/workload repetition requires fresh mount/cache/cgroup state.

Before real I/O, poll `findmnt` for up to 60 seconds while also checking `kill -0`; then require:

```bash
[[ "$(uname -s)" == Linux ]]
[[ "$(findmnt -n -T "$mount_point" -o FSTYPE)" == fuse.studiofs ]]
kill -0 "$mount_pid"
```

Read `/proc/$mount_pid/environ` internally but persist only an explicit allowlist (`STUDIOFS_LINUX_FUSE_CACHE_POLICY`, `STUDIOFS_FUSE_ASYNC_DIO`, FUSE geometry/profile knobs, allocator/profile switches). Never write database, object-store, token, or unrelated environment values into artifacts. Validate:

- `direct-serial`: `STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct` and `STUDIOFS_FUSE_ASYNC_DIO=0`;
- `direct-async`: `STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct` and `STUDIOFS_FUSE_ASYNC_DIO=1`;
- `kernel`: `STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel` and an explicit reproducibility-only `STUDIOFS_FUSE_ASYNC_DIO=0`.

Implement the allowlist as literal variable names, never a `STUDIOFS_*`
prefix. Record the mount process's captured stdout/stderr. Require the actual
mount path to end in the requested volume name; the CLI otherwise mounts at
`<supplied-root>/<volume>` and a collector could validate the wrong directory.
Record normal/fallback unmount output and status plus the mount process exit.

For real final evidence, require a newly created delegated cgroup v2 directory
on a cgroup2 filesystem with an active memory controller, writable
`cgroup.procs`, and readable `memory.current`, `memory.peak`, and `memory.stat`.
Before migration require empty `cgroup.procs`, `memory.current == 0`, and
`memory.peak == 0`; there is no bootstrap allowance. Move the collector shell
and mount PID into it, verify both memberships via `/proc/<pid>/cgroup`, and let
collector children inherit it. A missing/unwritable prerequisite is a hard
failure outside `--dry-run`.

Require clean StudioFS and `/home/val/dev/studiofs-bench` source trees for
promotion evidence. Record both source revisions and dirty states, `uname -a`,
CPU count, memory, `findmnt`, the backing filesystem/device for `--cache-dir`,
allowlisted mount environment, StudioFS/benchmark binary checksums, cgroup
path/baseline, correctness-manifest path and SHA-256, and first runtime snapshot
in `environment.json`. Verify `/proc/$mount_pid/exe` has the supplied StudioFS
binary checksum and record `/proc/$mount_pid/stat` start time to prevent PID
reuse. Reject the manifest before I/O unless its revision, clean state, both
binary checksums, and required non-skipped test results match this run.

In the same task, create `tools/perf/linux-cache-matrix.sh` accepting volume,
mount-root, delegated cgroup-root, repetitions, output-root, benchmark path,
StudioFS path, and an explicit `--diagnostic` flag. Before any performance mount it runs the Task 5 correctness
commands and emits the machine-owned correctness manifest. It then enumerates
all six workloads and three modes, invoking the collector with `--runs 1` and
that manifest for every `(repetition, workload, mode)` tuple. For every tuple:

1. create a unique empty mount directory, `--cache-dir`, artifact directory,
   and delegated cgroup beneath `target/`/the supplied cgroup root;
2. start one mount with explicit mode variables and record only the allowlisted
   environment;
3. wait for collector preflight, move mount plus collector into the fresh
   cgroup, and run exactly one workload;
4. request unmount, bound unmount and process exit to 30 seconds each, record
   any timeout/nonzero status as failure, then send TERM and finally KILL only
   to that still-live test mount;
5. remove workload/cache/mount temporary directories only after artifacts are
   durable, and remove the cgroup only after it is empty.

Rotate mode order as a three-row Latin square and workload order by repetition
so drift is not aligned with one candidate. `--dry-run` emits the complete
correctness commands, schedule, and cleanup actions without mounting. Never
mask cleanup status with `|| true`.

Write exactly this aggregation layout:

```text
<matrix>/correctness-manifest.json
<matrix>/schedule.json
<matrix>/<mode>/<workload>/rep-NNN/...
```

Normal promotion runs require exactly five repetitions. Three exploratory
repetitions are accepted only with `--diagnostic`; their manifests and outputs
are marked promotion-ineligible. Validate the schedule against the filesystem
and reject missing, duplicate, or extra tuples or any reused cgroup. The
summary analyzer receives the three `<matrix>/<mode>` directories.

- [ ] **Step 4: Implement bounded idle and mount-process sampling**

Never obtain evidence by reading `.studiofs-runtime-stats` through the mount:
that read is itself a Linux FUSE callback and contaminates the counters being
measured. Every snapshot and idle poll uses the existing out-of-band local
diagnostics transport:

```bash
"$studiofs" internal diagnostics-attach "$mount_point" \
  --recent-events-limit 1 --log-level error
```

Parse and persist only `.runtime_snapshot`. Hard-fail when `.session.pid` does
not equal `mount_pid`, normalized `.session.mount_point` does not equal the
target mount, or `.runtime_snapshot` is null or lacks the expected schema.
Final evidence has no mounted-file fallback.

Poll every 500 ms until these JSON values are zero for three consecutive
snapshots:

```text
runtime_snapshot.ingress.dirty_mem_bytes
runtime_snapshot.upload.pending_uploads
runtime_snapshot.upload.active_uploads
runtime_snapshot.write_path.fuse_async_write.pending_writes
runtime_snapshot.write_path.fuse_async_write.pending_bytes
runtime_snapshot.write_path.fuse_async_write.pending_handles
runtime_snapshot.write_path.os_cache.active_writers
runtime_snapshot.write_path.os_cache.active_foreground_handles
```

Fail after 600 seconds and retain the final snapshots. Measure each external workload as two explicitly marked phases:

1. capture the out-of-band runtime snapshot, `/proc/$mount_pid/{stat,status,smaps_rollup,io}`, `/proc/meminfo`, and cgroup `memory.current`, `memory.peak`, and `memory.stat` at `foreground_start`, then start `LC_ALL=C pidstat -urd -h -p "$mount_pid" 1` plus a 100 ms sampler that records cgroup memory and the mount process's `VmRSS`/`RssAnon` from `/proc/$mount_pid/status`;
2. run the client workload, record `foreground_end`, and capture another runtime/proc snapshot;
3. keep `pidstat` running while the same idle poll drains local dirty state and uploads;
4. record `drain_end`, capture the final runtime/proc/cgroup snapshot, then stop and reap both samplers;
5. record command, exit status, all three timestamps, and cleanup status.

This split is mandatory even when the drain is empty. It prevents background CPU from disappearing into the idle wait before the next run.

Require all idle gauges to be zero only at `foreground_start` and `drain_end`. Preserve `foreground_end` backlog as evidence; dirty bytes or uploads there are expected and must not fail the run.

`pidstat` absence, an unusable/empty sample stream, or locale-dependent parse
failure is a hard failure for real evidence and only an explicit warning in
`--dry-run`. Define p95 over sampled core equivalents with nearest rank
`ceil(0.95 * n)` after sorting, using one-second pidstat samples; sampled peak
is the maximum. Boundary-only `/proc` snapshots are not used as RSS/anonymous
peaks.

- [ ] **Step 5: Execute exactly one selected workload per collector run**

Run commands from each run directory so saved reports land beside inputs.

Large single file:

```bash
"$bench" --scripted --target "$mount_point" \
  --workload-bytes "$workload_bytes" --run-mode mounted \
  --mode read-write --layout single-file --cache enabled --keep-files --save-report
```

Fixed 1 MiB files:

```bash
"$bench" --scripted --target "$mount_point" \
  --workload-bytes "$workload_bytes" --run-mode mounted \
  --mode read-write --file-size-mb 1 --cache enabled --keep-files --save-report
```

Exact small files use an embedded Python standard-library client, not the multi-phase `studiofs devbench` command. Create exactly 1,000 unique 128 KiB files, fsync/close them, reopen every file, verify size and checksum, and report exactly 131,072,000 logical bytes written plus the same bytes read:

```bash
python3 "$run_dir/small-files-client.py" \
  --target "$mount_point" --files 1000 --file-bytes 131072 \
  --report "$run_dir/client-report.json"
```

Use embedded Python 3 standard-library programs, following the existing gate style, for:

- stable warm reopen: create a unique file for that run, run a Python write/fsync preparation phase, return to Bash for the same three-snapshot idle gate, then run a Python read phase with two fresh opens and per-pass counter snapshots; delete it only after the second pass and final metrics snapshot;
- same-file parallel writes: `min(os.cpu_count() or 1, 8)` threads use `os.pwrite` for equal disjoint 64 MiB regions, fsync once, reopen, and verify deterministic bytes; record the selected parallelism;
- cross-file barriers: write two 64 MiB files concurrently, fsync/close each, record completion times, reopen, and verify exact bytes.

Kernel records the same-file workload as correctness/CPU evidence but does not compare its direct-write speed to Direct. Direct serial and Direct async compare it directly. Require exactly one generated benchmark JSON, copy it to stable `client-report.json`, and retain the original. Before real evidence, rebuild from clean `/home/val/dev/studiofs-bench`, record its source revision (currently `34b28dc`), source dirty state and binary SHA-256 (currently `b7f4b25752eb051378216819f02d356f92409fb751c65cc5c4c4d910bfabbae2`), copy the binary to `target/bench/studiofs-bench`, and refresh the bundled README so its report schema includes `run.file_count`.

For every benchmark report require exact `run.workload_bytes` and
`run.file_count`, `run.stopped == false`, `run.cleanup_error == null`, mounted
run mode and expected target/layout, and exactly one write plus one read pass.
Each pass has exact `bytes_processed` and `stopped == false`. `large-single`
uses 4,000,000,000 bytes and one file; `fixed-1m` uses 4,000,000,000 bytes and
4,000 files.

All Python clients emit one minimal common report: workload, exact file count,
exact bytes written/read, expected and actual aggregate checksum, completion
boolean, and cleanup error. Require checksum equality and completion. Measured
totals are: small `131,072,000` written plus read; warm preparation is separate
and excluded, with zero measured write bytes and `2 * workload_bytes` read;
same-file is `parallelism * 64 MiB` written plus read; cross-file is two
64 MiB files written plus read. Tool rounding, partial completion, an extra
phase/report, or configured bytes substituted for processed bytes is a hard
harness failure.

Because benchmark runs keep their files, perform deletion only after the
measured drain and final snapshot. Record a separate cleanup status and cleanup
drain; exclude deletion work from foreground/drain CPU and throughput math.

- [ ] **Step 6: Write a stable artifact schema**

Each collector invocation owns one matrix repetition directory and writes:

```text
<rep>/environment.json
<rep>/manifest.json
<rep>/command.txt
<rep>/runtime-foreground-start.json
<rep>/runtime-foreground-end.json
<rep>/runtime-drain-end.json
<rep>/runtime-cleanup-end.json
<rep>/proc-foreground-start/{stat,status,smaps_rollup,io}.txt
<rep>/proc-foreground-end/{stat,status,smaps_rollup,io}.txt
<rep>/proc-drain-end/{stat,status,smaps_rollup,io}.txt
<rep>/cgroup-foreground-{start,end}.json
<rep>/cgroup-drain-end.json
<rep>/cgroup-memory-samples.jsonl
<rep>/process-memory-samples.jsonl
<rep>/pidstat.txt
<rep>/client-report.json
<rep>/cleanup.json
<rep>/derived.json
```

The matrix root owns `correctness-manifest.json`, `schedule.json`, and the final
`comparison/` output; each repetition references the manifest's absolute path
and checksum instead of copying or rewriting it.

`derived.json` contains verified bytes written/read, foreground wall seconds, end-to-end foreground throughput, client-reported phase throughput, foreground and drain mount CPU seconds, total lifecycle CPU seconds/GiB, foreground/drain average, nearest-rank p95, and sampled-peak core equivalents, drain duration, sampled daemon RSS/anonymous peaks, cgroup total/file/anon peaks and deltas, `/proc` backing-I/O deltas, callback/barrier interval p50/p95/p99/max-upper-bound, errors/cancellations, queue peaks, negotiated geometry, liveness deltas, and warm callback/cache-decision deltas. Cgroup total peak remains diagnostic; daemon `VmRSS`/`RssAnon` and cgroup file memory own the independent gates because total also includes unsynchronized kernel/slab memory.

Define `processed_gib = (verified_bytes_written + verified_bytes_read) / 2^30` and `CPU-seconds/GiB = mount_user_plus_system_cpu_seconds / processed_gib`; cleanup/deletion bytes are excluded. Compute mount CPU from `/proc/<pid>/stat` fields 14+15 divided by `getconf CLK_TCK`; the client process CPU is never substituted. Rebuild callback/barrier interval statistics from raw bucket deltas as specified in Task 4.

- [ ] **Step 7: Implement deterministic summary thresholds**

Create `tools/perf/linux_cache_gate_summary.py` with:

```text
--direct-serial ARTIFACT
--direct-async ARTIFACT
--kernel ARTIFACT
--correctness-manifest PATH
--output DIRECTORY
```

Use `statistics.median`; MAD is the median absolute deviation from the median. The primary throughput is verified processed bytes divided by measured foreground wall time, not the benchmark's lifecycle-excluding phase average. Primary cold workloads are the 4 GB single file, 4,000 fixed 1 MB files, and the exact 1,000 x 128 KiB client. First decide Direct async against Direct serial. If Direct async passes every applicable gate, use it as Kernel's baseline; otherwise use Direct serial and label that choice in every output. Compare the same-file parallel workload only between Direct serial and Direct async.

For promotion, require the complete scheduled set with exactly five
repetitions per mode/workload and reject missing, duplicate, or extra tuples.
Diagnostic three-repetition inputs are reported but never promotion-eligible.
Direct serial must itself pass correctness, provenance, geometry, histogram,
cleanup, CPU, and memory eligibility; an invalid Direct-serial baseline makes
every promotion decision ineligible. Direct async rejection may select Direct
serial for Kernel only when that baseline is fully eligible.

Define applicable histogram kinds per workload. Require a nonempty interval
only where that callback is expected. In particular, zero read callbacks on
Kernel's second stable-warm pass is the success condition, not a missing-sample
failure; its first pass must still provide the positive callback/control proof.

Fail a candidate when any condition holds:

- any correctness result, command, callback, task, cancellation, or cleanup failed;
- actual geometry contradicts the mode label;
- primary median throughput is below 95% of its applicable baseline;
- total lifecycle CPU-seconds/GiB (foreground plus upload drain) exceeds baseline by more than `max(2 * (baseline_mad + candidate_mad), 0.01 * baseline_median)`;
- foreground average or p95 core equivalents exceed baseline by more than `max(2 * (baseline_mad + candidate_mad), 0.10 * baseline_median, 0.25 cores)`; report sampled peak separately and apply the same guard as a conservative saturation check;
- callback/barrier p95 or p99 exceeds baseline by more than `max(2 * (baseline_mad + candidate_mad), 0.10 * baseline_median)`;
- callback/barrier interval max upper bound exceeds baseline by more than `max(2 * (baseline_mad + candidate_mad), 0.25 * baseline_median)`;
- daemon RSS or anonymous-memory peak exceeds baseline by more than `max(2 * (baseline_mad + candidate_mad), 0.10 * baseline_median)`;
- Direct async cgroup file-memory peak exceeds Direct serial by more than that same robust noise threshold;
- Kernel's incremental cgroup file-memory peak over its selected Direct
  baseline exceeds one verified logical working set plus
  `max(2 * (baseline_mad + candidate_mad), 0.10 * workload_bytes)`; this
  accounts for one intentional kernel-cache copy but rejects additional
  unexplained copies;
- Kernel's stable second warm read adds a FUSE or VFS read callback; Direct modes report their callback deltas as controls without this reuse gate;
- any required idle gauge is nonzero at `foreground_start` or `drain_end` (the `foreground_end` backlog is evidence, not a failure).

Report an improvement only when its median difference exceeds `baseline_mad + candidate_mad`. Always report foreground and upload-drain CPU separately. Before any statistics, validate the explicit correctness manifest and its checksum against every artifact. A candidate with histogram overflow/saturation, invalid interval buckets, a missing/skipped/mismatched required remote-invalidation result, or a reused/non-v2 cgroup is ineligible regardless of throughput.

- [ ] **Step 8: Verify GREEN**

```bash
python3 -m unittest tools.perf.test_linux_cache_gate -v
bash tools/perf/linux-cache-gate.sh --help
bash tools/perf/linux-cache-matrix.sh --help
bash tools/perf/linux-cache-gate.sh \
  --dry-run --mode direct-serial --workload large-single --mount-point target \
  --mount-pid "$$" --runs 1 --output-root target/perf/linux-cache-gate-dry-run
bash tools/perf/linux-cache-matrix.sh --dry-run \
  --volume test --mount-root target/perf/mounts \
  --cgroup-root target/perf/fake-cgroup --repetitions 1 \
  --output-root target/perf/linux-cache-matrix-dry-run
```

Expected: tests pass; dry-run prints a unique artifact directory and creates the complete skeleton under `target/`.

- [ ] **Step 9: Commit**

```bash
git add tools/perf/linux-cache-gate.sh tools/perf/linux-cache-matrix.sh tools/perf/linux_cache_gate_summary.py tools/perf/test_linux_cache_gate.py tools/perf/README.md
git commit -m "perf(linux): add cache and cpu evidence gate"
```

## Task 7: Run exploratory, profiling, and final evidence

- [ ] **Step 1: Build once and record the candidate revision**

```bash
cargo build --release --locked
git rev-parse HEAD
sha256sum target/release/studiofs target/bench/studiofs-bench
```

Do not rebuild between compared modes. If executable behavior changes after exploratory runs, discard earlier comparisons and restart here.

- [ ] **Step 2: Run three balanced exploratory repetitions**

Use the matrix orchestrator so every mode/workload repetition receives a fresh mount, empty local payload cache, and fresh cgroup:

```bash
bash tools/perf/linux-cache-matrix.sh \
  --diagnostic \
  --volume "$VOLUME" \
  --mount-root target/perf/linux-cache-exploratory/mounts \
  --cgroup-root "$DELEGATED_CGROUP_ROOT" \
  --repetitions 3 \
  --output-root target/perf/linux-cache-exploratory
```

The orchestrator supplies explicit variables for Direct serial, Direct async, and Kernel, records the balanced schedule, and marks this three-run output promotion-ineligible. Stop and fix correctness, instrumentation, or harness defects. Do not tune thresholds after seeing a failing performance result.

- [ ] **Step 3: Take one separate profile per viable candidate**

Mount with `--profile`, run one representative 4 GB single-file workload, wait for upload drain, unmount, and preserve pprof plus matching runtime/pidstat artifacts. Exclude profiles from analyzer inputs. Attribute CPU among bridge, VFS, local cache, materialization, encryption, upload, and runtime scheduling before considering any new controller.

- [ ] **Step 4: Close only measured CPU gaps before final evidence**

Run the analyzer over exploratory artifacts. If a candidate exceeds a CPU/core gate or its profile shows an avoidable hotspot, stop before final runs and write a narrow addendum plan naming the measured stack, expected counter/profile movement, focused RED/GREEN test, and rollback. Classify ownership before editing:

- shared VFS/upload QoS changes require Linux evidence plus real macOS and Windows CI regression signal;
- Linux callback/copy/session-loop work stays in `studiofs-linux-fuse`;
- backing-cache/page-cache duplication requires a separate cache-design decision, not an ad-hoc `O_DIRECT`/`fadvise` toggle.

Implement only the smallest measured fix, rebuild, and restart exploratory runs from Step 1. Never lower a threshold to make a candidate pass and do not add a Linux-only scheduler merely because total CPU is high. If profiles show no safe avoidable Linux hotspot, preserve that evidence and reject any candidate that still fails the objective gates.

- [ ] **Step 5: Run five clean final repetitions**

Repeat Step 2 without `--diagnostic`, with `--repetitions 5` and:

```text
target/perf/linux-cache-final
```

Verify every `environment.json` has the same git revision and binary checksums.

- [ ] **Step 6: Produce the final comparison**

```bash
python3 tools/perf/linux_cache_gate_summary.py \
  --direct-serial target/perf/linux-cache-final/direct-serial \
  --direct-async target/perf/linux-cache-final/direct-async \
  --kernel target/perf/linux-cache-final/kernel \
  --correctness-manifest target/perf/linux-cache-final/correctness-manifest.json \
  --output target/perf/linux-cache-final/comparison
```

Expected: `comparison.json`, `summary.md`, and `decision.md`; exit zero means every promotion gate passed, exit one means retain the applicable prior default.

- [ ] **Step 7: Commit a human-readable evidence record**

Create `docs/perf/linux-cache-performance-result.md` with candidate revision and host/kernel/libfuse details, artifact/checksum/cgroup provenance, balanced schedule, all medians/MADs, cold/warm results, foreground/drain CPU and core utilization, daemon anon/RSS plus cgroup file-memory data, backing I/O, callback/barrier interval tails, liveness and remote-listener evidence, errors/cancellations, negotiated geometry/header support, profile hotspots, Direct async verdict, and Kernel verdict with exact failed gates.

```bash
git add docs/perf/linux-cache-performance-result.md
git commit -m "docs(perf): record linux cache gate evidence"
```

## Task 8: Select the default in a separate evidence commit

- [ ] **Step 1: Apply the Direct async verdict**

If Direct async fails, restore disabled-by-default and retain `STUDIOFS_FUSE_ASYNC_DIO=1` as an experiment. If it passes, retain enabled-by-default and `STUDIOFS_FUSE_ASYNC_DIO=0` rollback. Rerun focused Direct tests after either change.

- [ ] **Step 2: Branch explicitly on the Kernel decision**

If any Kernel gate failed:

- keep `linux_fuse_cache_policy_from_env` defaulting to Direct;
- document Kernel as opt-in;
- do not create ADR 0011;
- list every failed gate in the evidence document.

If every Kernel gate passed:

- change the pure default-policy test to expect Kernel and verify RED;
- change only the absent-environment fallback to Kernel;
- preserve `STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct` rollback;
- add `docs/adr/0011-linux-kernel-cache-default-after-evidence.md`, superseding ADR 0002 and linking the evidence;
- rerun mounted default-Kernel and explicit-Direct tests.

- [ ] **Step 3: Update operational and user documentation**

Distinguish stable configuration from benchmark overrides, current default from rollback, local durability from remote publication, configured from negotiated geometry, cold from stable-warm results, and foreground from upload-drain CPU. Correct stale Linux support in mount help/examples.

- [ ] **Step 4: Verify selected policy**

```bash
cargo test -p studiofs-linux-fuse cache_policy_ -- --nocapture
cargo test -p studiofs-linux-fuse async_dio_policy_ -- --nocapture
STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct STUDIOFS_FUSE_ASYNC_DIO=0 \
  cargo test -p studiofs-linux-fuse mounted_direct_rollback_preserves_reopen_visibility \
  -- --ignored --nocapture --test-threads=1
cargo test -p studiofs --test architecture_vocabulary_test -- --nocapture
```

Additionally run exactly one unfiltered-environment mounted test matching the
selected branch (Direct serial rollback visibility, Direct async same-file
parallelism, or Kernel coherence matrix). Do not use `mounted_`: it includes
mutually exclusive mode assertions. Expected: selected default and both
explicit rollback paths match docs and evidence.

- [ ] **Step 5: Commit exactly one policy branch**

Retained Direct:

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs/src/cli/mount/mod.rs docs tools/perf/README.md
git commit -m "docs(linux): retain validated direct cache default"
```

Promoted Kernel:

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs/src/cli/mount/mod.rs docs tools/perf/README.md
git commit -m "feat(linux): promote validated kernel cache default"
```

## Task 9: Final focused verification and review handoff

- [ ] **Step 1: Run affected crate tests**

```bash
cargo test -p studiofs-bridge --lib -- --nocapture
cargo test -p studiofs-vfs --lib -- --nocapture
cargo test -p studiofs-linux-fuse --lib -- --nocapture
cargo test -p studiofs --lib -- --nocapture
python3 -m unittest tools.perf.test_linux_cache_gate -v
```

- [ ] **Step 2: Run formatting, linting, and docs checks**

```bash
cargo fmt --all -- --check
cargo clippy -p studiofs-bridge -p studiofs-vfs -p studiofs-linux-fuse -p studiofs-macfuse -p studiofs-winfsp -p studiofs --all-targets --locked -- -D warnings
cargo test -p studiofs --test architecture_vocabulary_test -- --nocapture
git diff --check
```

- [ ] **Step 3: Obtain real platform compatibility signal**

Run/record the existing Rocky Linux 9 job from `.rwx/rocky.yml` so the C shim compiles and tests against system libfuse 3.10 and asserts its single receive loop, then run the same focused Linux suite on the current libfuse 3.18 host and assert the bounded multithread loop. Because shared bridge/VFS and macFUSE code changed, require the existing hosted jobs in `.github/workflows/rust.yml` to complete:

```text
macOS:  cargo test -p studiofs-macfuse --lib -- --nocapture
Windows: cargo test -p studiofs-winfsp --lib -- --nocapture --test-threads=1
```

Record job URLs/revisions in the evidence document. Linux cfg-elided invocations of those crates are not acceptable substitutes.

On the current host the local libfuse 3.18 path, cgroup v2 delegation,
`pidstat`, and `findmnt` are available, but no `rwx` CLI is installed. Rocky,
macOS, and Windows therefore require configured remote runners. Keep final
completion pending until those real jobs have run and their revision-matched
URLs are recorded; absence of the runner is not a passing signal.

- [ ] **Step 4: Audit evidence against measured binaries**

```bash
cargo build --release --locked
git show --stat --oneline HEAD
git status --short
sha256sum target/release/studiofs target/bench/studiofs-bench
```

The release build after the policy commit is mandatory; record its revision and
new checksum as the final selected-policy binary. The evidence document records
both the performance-candidate revision/checksum and this final revision/checksum.
Inspect `git diff "$MEASURED_REVISION"..HEAD`: if executable changes are limited
to the tested absent-environment fallback plus CLI help/tests, the default-mode
mounted run and explicit Direct rollback from Task 8 are sufficient because all
performance modes used explicit policy variables. If any other I/O, cache,
scheduler, metric, or bridge behavior changed, rerun the full five-pass matrix
with the rebuilt binary before claiming completion, then update and commit the
evidence document.

- [ ] **Step 5: Request correctness and spec review**

Use requesting-code-review against the accepted design and all three plans. Review handle/reply lifetime, cache/invalidation correctness, Linux FFI/liveness, benchmark statistics/provenance, macOS equivalence, Windows non-regression, and whether selected defaults follow actual gate results. Address findings and rerun affected evidence.

## Self-review

- Linux consumes the shared cache seam rather than cloning macOS generation logic.
- `forget` is installed before Kernel can become a default and always receives a no-payload reply.
- Entry reference reservation precedes replies, failed replies roll it back, and Linux retires refs only through kernel `forget`.
- Open replies never synthesize lookup counts, and liveness event deltas are published.
- Direct participates in foreground QoS without retaining kernel pages.
- Negotiated values remain distinct from real configured/requested settings; unsupported header fields are explicit.
- Read/write size and latency plus flush/fsync/release latency are independently observable.
- Warm reuse proof requires first-read work/checksum and Replace-to-Reuse before zero second-read callbacks.
- Comparisons use one binary, idle boundaries, repeated runs, median/MAD, and applicable baselines.
- Mount-process CPU and upload drain are measured; client CPU is not substituted.
- Fresh cgroup file/anon memory proves cache growth separately from daemon RSS; prose cannot waive the gate.
- Direct rollback is mounted and verified before any Kernel promotion.
- Default selection is a separate commit and cannot occur on partial evidence.
- No benchmark rewrite, Linux-only scheduler, geometry tuning, or backing-cache direct I/O is introduced without measured need.
