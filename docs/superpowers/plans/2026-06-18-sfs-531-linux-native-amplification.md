# SFS-531 Linux Native Amplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development for every behavior change. Do not create a PR for this plan.

**Goal:** Finish SFS-531 by making `LinuxNativeBridge` observable and conservative for metadata-heavy and sparse-random workloads.

**Architecture:** Keep the work inside the native Linux bridge boundaries already defined by `docs/linux-native-bridge-contract.md`: `vfs_adapter` owns FUSE operation amplification counters and bridge-owned cache invalidation, while `cache_policy` owns sparse-random readahead decisions. Avoid live fio in unit tests; export enough metrics for mounted benchmark gates to diagnose remaining bottlenecks.

**Tech Stack:** Rust 2024, `cargo`, `studiofs-fuse`, native FUSE protocol tests.

---

### Task 1: Native Amplification Counters

**Files:**
- Modify: `crates/studiofs-fuse/src/native/vfs_adapter.rs`
- Test: `crates/studiofs-fuse/src/native/vfs_adapter.rs`

- [x] **Step 1: Write the failing test**

Add `amplification_metrics_snapshot_counts_metadata_churn` under `native::vfs_adapter::tests`.

- [x] **Step 2: Verify RED**

Run: `cargo test -p studiofs-fuse amplification_metrics_snapshot_counts_metadata_churn -- --nocapture`
Expected: FAIL because `LinuxNativeBridge::amplification_metrics_snapshot` does not exist.

- [x] **Step 3: Implement minimal code**

Add `NativeAmplificationMetrics`, `NativeAmplificationMetricsSnapshot`, `scaled_ratio_micros`, and record lookup/getattr/readdir/readdirplus/open/release/read/write in the native handlers.

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p studiofs-fuse amplification_metrics_snapshot_counts_metadata_churn -- --nocapture`
Expected: PASS.

### Task 2: Mutation Invalidation

**Files:**
- Modify: `crates/studiofs-fuse/src/native/vfs_adapter.rs`
- Modify: `docs/linux-native-bridge-contract.md`
- Test: `crates/studiofs-fuse/src/native/vfs_adapter.rs`

- [x] **Step 1: Write the failing test**

Add `setattr_truncate_invalidates_trusted_open_cache_state_before_followup_read`.

- [x] **Step 2: Verify RED**

Run: `cargo test -p studiofs-fuse setattr_truncate_invalidates_trusted_open_cache_state_before_followup_read -- --nocapture`
Expected: FAIL because a follow-up read can still use the old trusted local-cache range.

- [x] **Step 3: Implement minimal code**

Add `invalidate_metadata_cache_for_mutation` and call it on successful `mkdir`, `create`, `unlink`, `rmdir`, `rename`, successful write/overwrite, and size-changing `setattr`.

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p studiofs-fuse setattr_truncate_invalidates_trusted_open_cache_state_before_followup_read -- --nocapture`
Expected: PASS.

- [x] **Step 5: Audit overwrite coverage**

Add `handle_write_overwrite_invalidates_trusted_open_cache_state_before_followup_read`.

- [x] **Step 6: Verify RED/GREEN**

Run: `cargo test -p studiofs-fuse handle_write_overwrite_invalidates_trusted_open_cache_state_before_followup_read --lib -- --nocapture`
Expected RED before the write-path invalidation fix because the follow-up read still returns a local-cache range; expected GREEN after the fix.

- [x] **Step 7: Audit remote-refresh coverage**

Add `handle_read_falls_back_to_bytes_when_remote_refresh_generation_changed` and expose stale invalidation generation fallbacks in native/VFS telemetry.

- [x] **Step 8: Verify RED/GREEN**

Run: `cargo test -p studiofs-fuse handle_read_falls_back_to_bytes_when_remote_refresh_generation_changed --lib -- --nocapture`
Expected RED before generation fallback telemetry/validation exists; expected GREEN after local-cache ranges carry an invalidation generation and the bridge rejects mismatched generations.

### Task 3: Per-Opcode Latency Snapshot

**Files:**
- Modify: `crates/studiofs-fuse/src/native/vfs_adapter.rs`
- Test: `crates/studiofs-fuse/src/native/vfs_adapter.rs`

- [x] **Step 1: Write the failing test**

Add `amplification_metrics_snapshot_records_opcode_latency`.

- [x] **Step 2: Verify RED**

Run: `cargo test -p studiofs-fuse amplification_metrics_snapshot_records_opcode_latency -- --nocapture`
Expected: FAIL because the snapshot has no latency fields.

- [x] **Step 3: Implement minimal code**

Record total and max latency nanoseconds for lookup/getattr/readdir/read/write/open/release operations using `Instant::now()` in each handler.

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p studiofs-fuse amplification_metrics_snapshot_records_opcode_latency -- --nocapture`
Expected: PASS.

### Task 4: Metadata Invalidation Coverage

**Files:**
- Modify: `crates/studiofs-fuse/src/native/vfs_adapter.rs`
- Test: `crates/studiofs-fuse/src/native/vfs_adapter.rs`

- [x] **Step 1: Write failing tests**

Add tests proving metadata churn paths have bridge-owned invalidation coverage; the implemented RED path is `lookup_missing_name_returns_negative_entry_with_conservative_ttl`, which covers SFS-531 negative lookup TTL behavior.

- [x] **Step 2: Verify RED**

Run: `cargo test -p studiofs-fuse lookup_missing_name_returns_negative_entry_with_conservative_ttl -- --nocapture`
Expected: FAIL with `reply_errno == -ENOENT`.

- [x] **Step 3: Implement minimal code**

Add `negative_entry_payload` and return success entry replies with `nodeid=0` and conservative TTL for `lookup` `ENOENT`.

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p studiofs-fuse lookup_missing_name_returns_negative_entry_with_conservative_ttl -- --nocapture`
Expected: PASS.

### Task 5: Sparse-Random Controls Telemetry

**Files:**
- Modify: `crates/studiofs-fuse/src/native/cache_policy.rs`
- Modify: `crates/studiofs-fuse/src/native/vfs_adapter.rs`
- Test: `crates/studiofs-fuse/src/native/cache_policy.rs`
- Test: `crates/studiofs-fuse/src/native/vfs_adapter.rs`

- [x] **Step 1: Write the failing test**

Add `read_metrics_snapshot_reports_sparse_random_readahead_suppression`.

- [x] **Step 2: Verify RED**

Run: `cargo test -p studiofs-fuse read_metrics_snapshot_reports_sparse_random_readahead_suppression -- --nocapture`
Expected: FAIL because `NativeReadMetricsSnapshot` has no read-ahead telemetry fields.

- [x] **Step 3: Implement minimal code**

Add `read_ahead_prefetches`, `read_ahead_suppressed`, and `read_ahead_amplification_bytes` to `NativeReadMetricsSnapshot` and record them from `observe_read_ahead`.

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p studiofs-fuse read_metrics_snapshot_reports_sparse_random_readahead_suppression -- --nocapture`
Expected: PASS.

### Task 6: Documentation And Focused Verification

**Files:**
- Modify: `docs/linux-native-bridge-contract.md`
- Optionally modify: `docs/tests.md`

- [x] **Step 1: Update docs**

Document which counters satisfy SFS-531 and which mounted fio gates remain operator-run checks.

- [x] **Step 2: Run focused verification**

Run:

```bash
cargo fmt
cargo test -p studiofs-fuse native::vfs_adapter --lib -- --nocapture
cargo test -p studiofs-fuse native::cache_policy --lib -- --nocapture
cargo test -p studiofs-fuse --lib -- --nocapture
cargo clippy -p studiofs-fuse --lib -- -D warnings
```

Expected: all commands exit 0.

Additional verification run for the completed implementation:

```bash
cargo test -p studiofs-vfs write_path_observability_reports_native_bridge_snapshot --lib -- --nocapture
cargo test -p studiofs-vfs --lib -- --nocapture
cargo test -p studiofs-fuse --test mount_sanity --no-run
cargo clippy -p studiofs-fuse --all-targets -- -D warnings
cargo clippy -p studiofs-vfs --lib -- -D warnings
```

Expected: all commands exit 0.
