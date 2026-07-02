# SFS-595 JuiceFS Writer Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure StudioFS completed writer blocks stage and drain continuously, JuiceFS-style, before `flush`, `fsync`, or `release`.

**Architecture:** Reuse the existing `MountedWriterStack`, deferred materialization task, and `CachedStore`; do not add a second writer pipeline. Tighten worker scheduling, bounded drain behavior, and observability only where focused tests show a gap.

**Tech Stack:** Rust 2024, Tokio, existing `studiofs-vfs` unit tests, local JuiceFS reference at `/home/val/dev/juicefs`.

---

## File Map

- Modify: `crates/studiofs-vfs/src/vfs/tests.rs` for focused TDD coverage.
- Modify if a test fails: `crates/studiofs-vfs/src/vfs/sync_ops.rs` for per-inode deferred drain behavior.
- Modify if worker-level continuous drain is the gap: `crates/studiofs-vfs/src/vfs/workers.rs`.
- Modify if observability is missing: `crates/studiofs-vfs/src/vfs/metrics.rs` and `crates/studiofs-vfs/src/vfs.rs`.
- Reference only: `/home/val/dev/juicefs/pkg/vfs/vfs.go`, `/home/val/dev/juicefs/pkg/chunk/cached_store.go`, `/home/val/dev/juicefs/pkg/chunk/disk_cache.go`.

## Task 1: Verify The Existing Continuous Drain Surface

- [x] **Step 1: Run the focused existing tests**

Run:

```bash
cargo test -p studiofs-vfs deferred_materialization_ -- --nocapture
```

Result: PASS on 2026-07-02. Seven `deferred_materialization_` tests passed, including idle-tail drain before close, hot open writer linger, and in-flight reschedule.

- [x] **Step 2: Run completed-block and observability tests**

Run:

```bash
cargo test -p studiofs-vfs completed_block_materialization_ -- --nocapture
cargo test -p studiofs-vfs write_path_observability_reports_writer_cache_drain_lag_without_landing_journal -- --nocapture
cargo test -p studiofs-vfs write_path_observability_reports_active_tuning_knobs -- --nocapture
```

Result: PASS on 2026-07-02. Completed-block materialization, object-id prefetch, drain-lag observability, and active tuning knob tests passed.

## Task 2: Add A Regression Test Only For The Missing Behavior

- [x] **Step 1: If Task 1 passes, add no new behavior test**

No code change is needed when existing tests already prove SFS-595 acceptance. Keep the branch focused on documentation and verification evidence.

- [x] **Step 2: If completed blocks still wait for a later write/barrier, add this test to `crates/studiofs-vfs/src/vfs/tests.rs`**

Skipped because Task 1 proved completed blocks already drain before `flush`, `fsync`, or `release`.

```rust
#[test]
fn deferred_materialization_continuously_drains_multiple_completed_blocks_before_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-595-continuous-drain");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-595-continuous-drain.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;

        for index in 0..4usize {
            let payload = vec![index as u8 + 1; block_size];
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                (index * block_size) as u64,
                &payload,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && vfs.total_pending_persisted_local_dirty_slice_bytes()
                        >= block_size * 4
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("completed blocks should drain to local staging before flush");

        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "local staging should enqueue upload work without waiting for flush"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}
```

- [x] **Step 3: Verify RED**

Skipped because there was no missing behavior to drive with a new RED test.

Run:

```bash
cargo test -p studiofs-vfs deferred_materialization_continuously_drains_multiple_completed_blocks_before_flush -- --nocapture
```

Expected if a production gap exists: FAIL by timing out or by leaving dirty writer-stack bytes. If it passes, do not modify production code for this behavior.

## Task 3: Minimal Production Fix If RED Exposes A Gap

- [x] **Step 1: Keep the fix inside `start_deferred_materialization_task`**

Skipped because no RED failure exposed a production gap.

Change only the loop condition or linger scheduling in `crates/studiofs-vfs/src/vfs/sync_ops.rs`. The desired shape is:

```rust
let completed_blocks_remain = vfs.should_continue_deferred_materialization(ino);
if made_progress {
    no_progress_retries = 0;
} else if no_progress_retries < super::DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES
    && vfs.should_linger_deferred_materialization(ino)
{
    no_progress_retries += 1;
    tokio::time::sleep(Duration::from_millis(
        super::DEFERRED_MATERIALIZATION_LINGER_WAIT_MS,
    ))
    .await;
    continue;
}
if !made_progress || !completed_blocks_remain {
    break;
}
if completed_blocks_remain {
    tokio::task::yield_now().await;
    continue;
}
```

Use this only if the current loop exits while completed blocks remain. Do not
reset retry accounting just because completed blocks remain; no-progress
iterations must still back off or exit.

- [x] **Step 2: Verify GREEN**

Skipped because no production code was changed.

Run:

```bash
cargo test -p studiofs-vfs deferred_materialization_continuously_drains_multiple_completed_blocks_before_flush -- --nocapture
```

Expected: PASS.

## Task 4: Observability Gap Fix If Needed

- [x] **Step 1: If counters are missing, extend the existing snapshot instead of adding a new metrics type**

Use existing fields in `write_path_observability_snapshot()` where possible. Add only the missing field that a failing test requires.

Result: skipped. Existing observability tests passed.

- [x] **Step 2: Verify the focused observability tests**

Run:

```bash
cargo test -p studiofs-vfs write_path_observability_reports_writer_cache_drain_lag_without_landing_journal -- --nocapture
cargo test -p studiofs-vfs write_path_observability_reports_active_tuning_knobs -- --nocapture
```

Result: PASS on 2026-07-02.

## Task 5: Final Focused Verification

- [x] **Step 1: Run the local writer/cache SFS-595 set**

Run:

```bash
cargo test -p studiofs-vfs deferred_materialization_ -- --nocapture
cargo test -p studiofs-vfs completed_block_materialization_ -- --nocapture
cargo test -p studiofs-vfs write_admission_pressure_samples_cached_store_pending_upload_bytes -- --nocapture
cargo test -p studiofs-vfs opportunistic_deferred_write_rechecks_hard_pressure_without_inline_fallback -- --nocapture
cargo test -p studiofs-vfs write_path_observability_reports_writer_cache_drain_lag_without_landing_journal -- --nocapture
cargo test -p studiofs-vfs write_path_observability_reports_active_tuning_knobs -- --nocapture
```

Expected: PASS.

Result: PASS on 2026-07-02 via focused commands for `deferred_materialization_`,
`completed_block_materialization_`, write admission pressure, opportunistic
hard-pressure recheck, drain-lag observability, and active tuning knobs.

- [x] **Step 2: Run rustfmt on touched Rust files**

Run:

```bash
cargo fmt --all
```

Expected: exit 0.

Result: PASS on 2026-07-02.

## Self-Review

- The plan maps SFS-595 to existing writer-stack, cached-store, and deferred drain code.
- No new writer abstraction is introduced.
- Production edits are conditional on a failing focused test.
- JuiceFS reference files are named for every planning comparison.
