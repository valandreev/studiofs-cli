# Linux Memory-Pressure Adaptation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep a Linux StudioFS mount responsive and CPU-efficient when host or cgroup memory headroom shrinks, without changing macOS or Windows behavior.

**Architecture:** Add one mount-owned Linux sampler which publishes a small immutable pressure snapshot through the existing `StudioVfs` object. Existing VFS atomics and admission paths consume the limits; the Linux FUSE async queue receives the current queue limit and continues to block on its condition variable. The kernel remains responsible for page-cache LRU and clean-page reclaim.

**Tech Stack:** Rust 2024, Tokio, `tokio_util::sync::CancellationToken`, existing StudioFS runtime stats, Bash/Python performance gates, Linux cgroup v2.

---

## File map

- Create `crates/studiofs-vfs/src/memory_pressure.rs`: platform-neutral formula, snapshot types, hysteresis, and shared controller handle.
- Modify `crates/studiofs-vfs/src/lib.rs`: export the controller types used by the mount and Linux bridge.
- Modify `crates/studiofs-vfs/src/vfs.rs`: store the controller, expose admitted-byte gauges, and apply published dirty/transit/cache limits through existing admission mechanisms.
- Modify `crates/studiofs-vfs/src/vfs/bootstrap.rs`: accept an optional controller without changing existing callers' defaults.
- Modify `crates/studiofs-vfs/src/vfs/metrics.rs` and `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs`: add the stable `memory_pressure` stats section.
- Create `crates/studiofs/src/cli/mount/memory_pressure.rs`: read Linux host/cgroup inputs and run the one 500 ms mount-owned sampler; return disabled behavior elsewhere.
- Modify `crates/studiofs/src/cli/mount.rs` and `crates/studiofs/src/cli/mount/session.rs`: construct the initial snapshot, wire sampler lifetime, and join it during unmount.
- Modify `crates/studiofs-linux-fuse/src/async_writes.rs` and `crates/studiofs-linux-fuse/src/low_level.rs`: replace the fixed active byte limit with the current pressure limit while keeping the existing 256 MiB ceiling and condition-variable wait.
- Modify `tools/perf/linux-cache-gate.sh` and `tools/perf/linux_cache_gate_summary.py`: add delegated-cgroup pressure validation and report memory/CPU evidence.
- Modify `tools/perf/test_linux_cache_gate.py`: deterministic collector/summary coverage.
- Modify `docs/linux-cache-performance-result.md`: record the constrained-memory promotion evidence.

### Task 1: Pure envelope and hysteresis model

**Files:**
- Create: `crates/studiofs-vfs/src/memory_pressure.rs`
- Modify: `crates/studiofs-vfs/src/lib.rs`

- [ ] **Step 1: Write failing formula tests**

Add table-driven tests for `(8 GiB total, 3 GiB available)`, finite cgroup lower than host, unlimited cgroup, zero allocatable memory, rounding, and minimum overcommit. Use these exact public inputs:

```rust
let input = MemoryPressureInput {
    host_total_bytes: gib(8),
    host_available_bytes: gib(3),
    cgroup_limit_bytes: None,
    cgroup_current_bytes: None,
};
let ceilings = MemoryComponentLimits {
    memory_cache_bytes: gib(2),
    dirty_bytes: mib(800),
    transit_bytes: mib(800),
    async_write_bytes: mib(256),
};
let snapshot = calculate_snapshot(input, ceilings, mib(16), mib(1));
assert_eq!(snapshot.reserve_bytes, gib(8) * 15 / 100);
assert!(snapshot.envelope_bytes <= snapshot.available_bytes - snapshot.reserve_bytes);
assert!(snapshot.limits.managed_total() <= snapshot.envelope_bytes);
```

Also assert that minimum limits are `64/64/32/32 MiB`, adjusted upward for block size and negotiated max write, and set `overcommitted_minimum` when their sum exceeds 90% of the envelope.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cargo test -p studiofs-vfs memory_pressure::tests --lib`

Expected: FAIL because `memory_pressure` and its types do not exist.

- [ ] **Step 3: Implement the minimal pure model**

Define:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MemoryPressureSource { Host, Cgroup, HostAndCgroup, Fallback, Disabled }

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MemoryPressureState { Normal, Constrained, Disabled }

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryComponentLimits {
    pub memory_cache_bytes: u64,
    pub dirty_bytes: u64,
    pub transit_bytes: u64,
    pub async_write_bytes: u64,
}

impl MemoryComponentLimits {
    pub fn managed_total(self) -> u64 {
        self.memory_cache_bytes
            .saturating_add(self.dirty_bytes)
            .saturating_add(self.transit_bytes)
            .saturating_add(self.async_write_bytes)
    }
}
```

Use integer math: reserve is `max(1 GiB, effective_limit * 15 / 100)`, envelope is `min(ceil(configured_total * 10 / 9), allocatable * 75 / 100)`, and managed shares are 40/25/15/10 percent of the envelope. Clamp each share to its configured ceiling; apply minima only after calculating shares.

- [ ] **Step 4: Add deterministic state-machine tests**

Test immediate entry into `Constrained`, retention after one healthy sample, exit after two healthy samples, and recovery growth capped at `ceiling / 4` per sample. Test that a failed sample preserves the last valid limits and increments `sampling_failures`.

- [ ] **Step 5: Implement `MemoryPressureModel::observe`**

```rust
pub fn observe(
    &mut self,
    input: Result<MemoryPressureInput, MemoryPressureReadError>,
    admitted_managed_bytes: u64,
) -> MemoryPressureSnapshot
```

The first error returns the minimum fallback snapshot. Later errors clone the last snapshot, increment generation and failure count, and retain its limits. A healthy sample is constrained when `available < reserve` or `envelope < admitted_managed_bytes`.

- [ ] **Step 6: Run, format, and commit**

Run: `cargo fmt --check && cargo test -p studiofs-vfs memory_pressure::tests --lib`

Expected: PASS.

```bash
git add crates/studiofs-vfs/src/memory_pressure.rs crates/studiofs-vfs/src/lib.rs
git commit -m "feat(vfs): model memory pressure envelope"
```

### Task 2: Shared snapshot and VFS dynamic limits

**Files:**
- Modify: `crates/studiofs-vfs/src/memory_pressure.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/bootstrap.rs`
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] **Step 1: Write failing controller/VFS tests**

Construct a `MemoryPressureController::new(initial)` and a test VFS, publish a smaller snapshot, then assert:

```rust
controller.publish(constrained.clone());
assert_eq!(vfs.max_dirty_bytes(), constrained.limits.dirty_bytes as usize);
assert_eq!(vfs.memory_pressure_snapshot(), constrained);
```

Add a write test that admits one write before the shrink, proves it completes, and proves a new writer waits until dirty bytes drain. Use existing `set_max_dirty_bytes` test scaffolding and a Tokio timeout; do not add polling sleeps.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p studiofs-vfs memory_pressure --lib`

Expected: FAIL because `MemoryPressureController` and VFS wiring are absent.

- [ ] **Step 3: Implement the shared handle with standard synchronization**

Use the existing dependency set only:

```rust
#[derive(Clone)]
pub struct MemoryPressureController {
    snapshot: Arc<parking_lot::RwLock<MemoryPressureSnapshot>>,
}

impl MemoryPressureController {
    pub fn snapshot(&self) -> MemoryPressureSnapshot { self.snapshot.read().clone() }
    pub fn publish(&self, snapshot: MemoryPressureSnapshot) { *self.snapshot.write() = snapshot; }
}
```

Do not introduce `arc-swap`, callbacks, or a second controller abstraction.

- [ ] **Step 4: Wire the controller through bootstrap**

Add `memory_pressure: MemoryPressureController` to `StudioVfs`. Existing constructors create `MemoryPressureController::disabled(configured_limits)` so macOS, Windows, tests, and library callers preserve current budgets. Add only one explicit Linux-facing constructor parameter to `new_with_prepared_cache_runtime`.

Change `max_dirty_bytes()` to return the smaller of the configured atomic ceiling and the current controller limit. Apply the same clamp at the existing transit reservation and memory-cache admission points; when the memory-cache limit shrinks, invoke the cache's existing reclaim/eviction operation and never evict pinned entries. Do not build a new LRU.

- [ ] **Step 5: Publish admitted managed bytes**

Add one method which sums existing gauges:

```rust
pub fn admitted_managed_bytes(&self) -> u64 {
    self.memory_cache_resident_bytes()
        .saturating_add(self.total_pending_write_buffer_bytes() as u64)
        .saturating_add(self.transit_reserved_bytes())
        .saturating_add(self.fuse_async_pending_bytes())
}
```

Implement `memory_cache_resident_bytes`, `transit_reserved_bytes`, and `fuse_async_pending_bytes` as private accessors over the existing cache-state, ingress, and bridge gauges. They must not own or increment counters; `admitted_managed_bytes` is only an aggregate view.

- [ ] **Step 6: Run focused VFS tests and commit**

Run: `cargo fmt --check && cargo test -p studiofs-vfs memory_pressure --lib`

Expected: PASS, including admitted writes completing across a limit shrink.

```bash
git add crates/studiofs-vfs/src/memory_pressure.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/bootstrap.rs crates/studiofs-vfs/src/vfs/tests.rs
git commit -m "feat(vfs): apply dynamic memory admission limits"
```

### Task 3: Linux host/cgroup probe and mount-owned sampler

**Files:**
- Create: `crates/studiofs/src/cli/mount/memory_pressure.rs`
- Modify: `crates/studiofs/src/cli/mount.rs`
- Modify: `crates/studiofs/src/cli/mount/session.rs`

- [ ] **Step 1: Write failing parser tests**

Test `/proc/meminfo` parsing with `MemTotal` and `MemAvailable`, `memory.max` values `1073741824` and `max`, `memory.current`, malformed input, and cgroup-path resolution from `/proc/self/cgroup`. All readers receive paths or string slices in tests; tests must not depend on the test host's actual cgroup.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p studiofs cli::mount::memory_pressure --lib`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the Linux probe**

Define one internal trait for deterministic tests:

```rust
trait MemoryPressureProbe: Send + Sync {
    fn sample(&self) -> anyhow::Result<MemoryPressureInput>;
}
```

`LinuxMemoryPressureProbe` reads `/proc/meminfo`, `/proc/self/cgroup`, and the active cgroup v2 `memory.max`/`memory.current`. Treat `max`, missing files, and absent cgroup controllers as no finite cgroup limit. Treat malformed host data as a failed sample. Under `#[cfg(not(target_os = "linux"))]`, construct a disabled controller and spawn no task.

- [ ] **Step 4: Write failing sampler lifecycle tests**

Use a fake probe and `tokio::time::pause`/`advance` to assert one sample every 500 ms, last-snapshot retention after a read error, and cancellation plus join during shutdown.

- [ ] **Step 5: Implement sampler ownership**

```rust
struct MemoryPressureSampler {
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl MemoryPressureSampler {
    async fn stop(self) {
        self.cancellation.cancel();
        let _ = self.task.await;
    }
}
```

At `MountPlan::prepare`, take the first synchronous sample before building `StudioVfs`; use its memory-cache limit instead of `initial_cache_bytes()` on Linux, pass the controller into VFS, then spawn exactly one sampler on the mount runtime. Store it in `PreparedForegroundRuntime`. Stop it before `vfs.shutdown(...)` and before `runtime.shutdown_timeout(...)`.

- [ ] **Step 6: Run focused tests and commit**

Run: `cargo fmt --check && cargo test -p studiofs cli::mount::memory_pressure --lib && cargo test -p studiofs runtime_shutdown --lib`

Expected: PASS; non-Linux code compiles with disabled behavior.

```bash
git add crates/studiofs/src/cli/mount.rs crates/studiofs/src/cli/mount/memory_pressure.rs crates/studiofs/src/cli/mount/session.rs
git commit -m "feat(mount): sample Linux memory headroom"
```

### Task 4: Dynamic Linux async-write admission without spin

**Files:**
- Modify: `crates/studiofs-linux-fuse/src/async_writes.rs`
- Modify: `crates/studiofs-linux-fuse/src/low_level.rs`

- [ ] **Step 1: Write failing queue tests**

Add tests proving: the active byte limit can shrink below already pending bytes without cancelling their tickets; a new admission blocks; releasing a reservation wakes it; shutdown wakes it with `EIO`; wait count increments once and cumulative wait time grows. Assert no repeated observer calls while blocked.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p studiofs-linux-fuse async_writes --lib`

Expected: FAIL because the byte limit is immutable.

- [ ] **Step 3: Add the minimal mutable active limit**

Keep `max_pending_writes` fixed. Replace only the byte field with `AtomicUsize`, capped at construction-time ceiling:

```rust
pub(crate) fn set_active_max_pending_bytes(&self, bytes: usize) {
    self.active_max_pending_bytes
        .store(bytes.min(self.max_pending_bytes_ceiling), Ordering::Release);
    self.changed.notify_all();
}
```

In `admit`, load the active limit inside the existing condition-variable loop. Permit one request up to negotiated `max_write` when the minimum profile is overcommitted; otherwise retain `E2BIG` for requests larger than the fixed ceiling. Do not add a timer, retry loop, or busy polling.

- [ ] **Step 4: Feed the limit from `SessionState`**

Before each admission, call `set_active_max_pending_bytes(vfs.memory_pressure_snapshot().limits.async_write_bytes as usize)`. Publish pending async bytes through the existing bridge gauge so `StudioVfs::admitted_managed_bytes()` includes it.

- [ ] **Step 5: Run tests and commit**

Run: `cargo fmt --check && cargo test -p studiofs-linux-fuse async_writes --lib && cargo test -p studiofs-linux-fuse low_level --lib`

Expected: PASS with condition-variable wakeups and clean shutdown.

```bash
git add crates/studiofs-linux-fuse/src/async_writes.rs crates/studiofs-linux-fuse/src/low_level.rs
git commit -m "feat(linux-fuse): adapt async write queue to memory pressure"
```

### Task 5: Stable runtime statistics

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/metrics.rs`
- Modify: `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs`

- [ ] **Step 1: Write failing serialization tests**

Add `memory_pressure` to `RUNTIME_STATS_SECTIONS` and assert JSON/text round trips preserve source, state, generation, effective/available/reserve/envelope bytes, four limits, transitions, failures, overcommitted flag, admission wait count, and wait milliseconds. Assert older JSON with no section still deserializes to an empty section.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p studiofs-vfs runtime_stats_snapshot --lib`

Expected: FAIL because `RuntimeStatsSnapshot` has no `memory_pressure` section.

- [ ] **Step 3: Add and populate the section**

Add `pub memory_pressure: RuntimeStatsSection` to the snapshot and its default/section iteration. Populate it in `StudioVfs::runtime_stats_snapshot()` from the controller snapshot and existing async-write wait counters. Encode enum values as stable numeric gauges plus documented names in JSON only if the existing stats representation supports strings; do not redesign `RuntimeStatsSection`.

- [ ] **Step 4: Run tests and commit**

Run: `cargo fmt --check && cargo test -p studiofs-vfs runtime_stats_snapshot --lib && cargo test -p studiofs-vfs runtime_stats_surface --lib`

Expected: PASS and backward-compatible parsing.

```bash
git add crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs
git commit -m "feat(metrics): expose memory pressure state"
```

### Task 6: Deterministic mounted pressure gate

**Files:**
- Modify: `tools/perf/linux-cache-gate.sh`
- Modify: `tools/perf/linux_cache_gate_summary.py`
- Modify: `tools/perf/test_linux_cache_gate.py`

- [ ] **Step 1: Write failing shell and Python tests**

Extend fixture data with `memory.events`, runtime-stats samples, holder start/release timestamps, foreground CPU, drain CPU, and hashes. Assert the summary rejects: any `oom`/`oom_kill` delta, a managed-total limit above the envelope, mismatched bytes, failed drain/unmount, or missing recovery. Assert kernel-cache mode reports the same correctness failures but remains diagnostic for throughput tails.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tools.perf.test_linux_cache_gate`

Expected: FAIL because pressure evidence is not collected or evaluated.

- [ ] **Step 3: Add one pressure scenario to the existing gate**

Reuse the gate's current mount lifecycle. In a delegated cgroup v2, set a finite `memory.max`, launch a separate memory-holder process, run `small-128k` and `large-single`, release the holder, repeat the recovery phase, verify byte hashes, drain, and unmount. Record JSON with Python `json.dump()`; do not add configurable repetition counts or a second runner.

The holder must allocate/touch memory and wait for a signal; keep its temporary source/binary/output under `target/` and remove them in the gate trap.

- [ ] **Step 4: Evaluate promotion criteria**

Direct + negotiated ASYNC_DIO fails promotion on correctness, OOM events, envelope violation, unbounded CPU, failed drain, or absent throughput recovery. Kernel-cache mode records the same evidence and correctness status but its performance tail remains diagnostic.

- [ ] **Step 5: Run deterministic tests and commit**

Run: `python3 -m unittest tools.perf.test_linux_cache_gate`

Expected: PASS.

```bash
git add tools/perf/linux-cache-gate.sh tools/perf/linux_cache_gate_summary.py tools/perf/test_linux_cache_gate.py
git commit -m "test(perf): gate Linux mounts under memory pressure"
```

### Task 7: Focused integration validation and evidence

**Files:**
- Modify: `docs/linux-cache-performance-result.md`

- [ ] **Step 1: Run focused workspace validation**

Run:

```bash
cargo fmt --check
cargo clippy -p studiofs-vfs -p studiofs-linux-fuse -p studiofs --all-targets -- -D warnings
cargo test -p studiofs-vfs memory_pressure
cargo test -p studiofs-linux-fuse async_writes
cargo test -p studiofs cli::mount::memory_pressure
python3 -m unittest tools.perf.test_linux_cache_gate
```

Expected: every command exits 0 with no warnings.

- [ ] **Step 2: Run the real mounted diagnostic gate**

Use the existing README/options in `target/bench` and the gate's real mode. Run once on a fresh mount for Direct + ASYNC_DIO and once for kernel-cache diagnostic mode. Preserve the generated evidence under `target/` until the results are summarized.

Expected: both modes pass byte verification, drain, and unmount; cgroup `oom` and `oom_kill` deltas are zero; Direct satisfies CPU/recovery promotion criteria; kernel mode produces diagnostic performance data.

- [ ] **Step 3: Record exact evidence**

In `docs/linux-cache-performance-result.md`, append the command, git commit, host/cgroup limits, initial occupied memory, per-phase envelope/limits, OOM deltas, CPU, throughput, hash verification, drain/unmount result, and Direct promotion decision. Do not copy raw logs into the document.

- [ ] **Step 4: Remove temporary benchmark artifacts**

Delete only files created by this run under `target/`; preserve the checked-in/externally supplied `target/bench` binary and README.

- [ ] **Step 5: Commit evidence**

```bash
git add docs/linux-cache-performance-result.md
git commit -m "docs: record Linux memory pressure validation"
```

### Task 8: Final review

**Files:** all files changed by Tasks 1-7.

- [ ] **Step 1: Review against the approved design**

Confirm: Linux-only probing; macOS/Windows unchanged; no kernel LRU; no `DONTNEED`; no memory-driven inode invalidation; one sampler; 500 ms interval; two-sample recovery; 25% growth cap; admitted work is never cancelled; no public tuning flag.

- [ ] **Step 2: Review for minimality**

Run `git diff ce940f2e --stat` and `git diff ce940f2e`. Remove unused flexibility, duplicate gauges, configurable run counts, manual JSON assembly, and any new dependency that can be replaced by the standard library or existing workspace dependencies.

- [ ] **Step 3: Run the final focused suite**

Repeat the exact commands from Task 7 Step 1.

Expected: all PASS.

- [ ] **Step 4: Request two-stage code review**

Use `superpowers:requesting-code-review`, then address only verified correctness/spec findings. Run the affected focused test after every correction and commit the corrections with a scoped message.
