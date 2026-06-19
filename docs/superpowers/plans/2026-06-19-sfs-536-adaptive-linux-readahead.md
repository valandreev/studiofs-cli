# SFS-536 Adaptive Linux Readahead Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first Linux-default JuiceFS-style adaptive readahead increment by growing sequential prefetch windows, shrinking them on sparse access, and surfacing native metrics.

**Architecture:** Reuse the existing VFS `ReadAheadTracker` as the authoritative per-stream prefetch planner and keep the Linux native bridge as the FUSE-local session and metrics layer. Avoid a second prefetch engine: native `advise_read_ahead` continues to enqueue work into the existing VFS prefetch worker.

**Tech Stack:** Rust 2024, `studiofs-vfs`, `studiofs-fuse`, Cargo tests.

---

### Task 1: Adaptive VFS Readahead Window

**Files:**
- Modify: `crates/studiofs-vfs/src/readahead.rs`

- [x] **Step 1: Write the failing tests**

Add tests that require the VFS tracker to widen sequential streams and shrink after a random jump:

```rust
#[test]
fn test_sequential_reads_grow_window_toward_max_width() {
    let tracker = ReadAheadTracker::new_with_bounds(2, 16, 2);
    let mut t = 1_000_000u64;

    let _ = tracker.record_read_at_micros(1, 0, 0, t);
    t = t.saturating_add(500_000);
    let _ = tracker.record_read_at_micros(1, 1, 1, t);
    assert_eq!(tracker.debug_width(1), Some(2));

    for block in 2..=8 {
        t = t.saturating_add(500_000);
        let _ = tracker.record_read_at_micros(1, block, block, t);
    }

    assert_eq!(tracker.debug_width(1), Some(16));
}

#[test]
fn test_random_seek_shrinks_adaptive_window_to_base_width() {
    let tracker = ReadAheadTracker::new_with_bounds(2, 16, 2);
    let mut t = 1_000_000u64;

    for block in 0..=8 {
        let _ = tracker.record_read_at_micros(1, block, block, t);
        t = t.saturating_add(500_000);
    }
    assert_eq!(tracker.debug_width(1), Some(16));

    let random = tracker.record_read_at_micros(1, 1000, 1000, t);

    assert!(random.is_empty());
    assert_eq!(tracker.debug_width(1), Some(2));
}
```

- [x] **Step 2: Run tests to verify RED**

Run: `cargo test -p studiofs-vfs test_sequential_reads_grow_window_toward_max_width test_random_seek_shrinks_adaptive_window_to_base_width -- --nocapture`

Expected: FAIL because the current tracker keeps a fixed configured window.

- [x] **Step 3: Implement minimal adaptive growth/shrink**

Update `ReadState` with a small sequential streak counter. On contiguous forward reads, grow `current_width` by doubling until `max_width`, starting at `fixed_width`. On random/sparse reads or idle reset, shrink to `base_width` and suppress immediate prefetch.

- [x] **Step 4: Run tests to verify GREEN**

Run: `cargo test -p studiofs-vfs test_sequential_reads_grow_window_toward_max_width test_random_seek_shrinks_adaptive_window_to_base_width -- --nocapture`

Expected: PASS.

### Task 2: Native Metrics For Adaptive Sessions

**Files:**
- Modify: `crates/studiofs-fuse/src/native/cache_policy.rs`
- Modify: `crates/studiofs-fuse/src/native/vfs_adapter.rs`

- [x] **Step 1: Write the failing test**

Add a Linux native bridge test proving a long trusted sequential read stream records prefetches and nonzero amplification while a later sparse seek records suppression:

```rust
#[test]
fn read_metrics_snapshot_reports_adaptive_prefetch_and_sparse_suppression() {
    let vfs = Arc::new(TestNativeVfs::default());
    let bridge = LinuxNativeBridge::new(vfs.clone());
    bridge.remember_trusted_open_cache_state(
        20,
        &NativeInodeCacheState {
            content_epoch: 9,
            size: 2 * 1024 * 1024,
            mtime_ns: 100,
            invalidation_generation: 0,
            trusted: None,
            writer_active: false,
            dirty: false,
            local_materialized: true,
        },
    );

    for block in 0..8u64 {
        let _ = bridge.handle_read(read_request_for_offset_and_size(block * 8192, 8192));
    }
    let sequential = bridge.read_metrics_snapshot();
    assert!(sequential.read_ahead_prefetches > 1);
    assert!(sequential.read_ahead_amplification_bytes > 0);

    let _ = bridge.handle_read(read_request_for_offset_and_size(1024 * 1024, 8192));
    let sparse = bridge.read_metrics_snapshot();
    assert!(sparse.read_ahead_suppressed > sequential.read_ahead_suppressed);
}
```

- [x] **Step 2: Run test to verify RED**

Run: `cargo test -p studiofs-fuse read_metrics_snapshot_reports_adaptive_prefetch_and_sparse_suppression -- --nocapture`

Expected: FAIL until native metrics expose the needed adaptive-session behavior.

- [x] **Step 3: Implement minimal metric wiring**

If needed, extend `NativeReadMetricsSnapshot` and `observe_read_ahead` with counters for adaptive prefetch observations. Keep native range calculation bounded by `NativeReadAheadPolicy` and preserve existing suppression behavior for untrusted or dirty cache state.

- [x] **Step 4: Run test to verify GREEN**

Run: `cargo test -p studiofs-fuse read_metrics_snapshot_reports_adaptive_prefetch_and_sparse_suppression -- --nocapture`

Expected: PASS.

### Task 3: Focused Validation

**Files:**
- No new files.

- [x] **Step 1: Run focused VFS tests**

Run: `cargo test -p studiofs-vfs readahead -- --nocapture`

Expected: PASS.

- [x] **Step 2: Run focused native bridge tests**

Run: `cargo test -p studiofs-fuse read_ahead -- --nocapture`

Expected: PASS.

- [x] **Step 3: Inspect diff**

Run: `git diff -- crates/studiofs-vfs/src/readahead.rs crates/studiofs-fuse/src/native/cache_policy.rs crates/studiofs-fuse/src/native/vfs_adapter.rs docs/superpowers/plans/2026-06-19-sfs-536-adaptive-linux-readahead.md`

Expected: Only the adaptive readahead plan, tests, implementation, and metrics wiring changed.

Focused validation run on 2026-06-19:
- `cargo test -p studiofs-vfs readahead -- --nocapture`
- `cargo test -p studiofs-fuse readahead -- --nocapture`
- `cargo test -p studiofs-fuse read_metrics_snapshot_reports_adaptive_prefetch_and_sparse_suppression -- --nocapture`
- `cargo test -p studiofs-fuse local_cache -- --nocapture`
- `cargo test -p studiofs-vfs write_path_observability_reports_native_bridge_snapshot -- --nocapture`
