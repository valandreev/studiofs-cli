# New Meta Engine Refactor Roadmap

## Rule Of Execution

This roadmap is strict.

1. Work proceeds only in order: `PR-01 -> PR-10`.
2. The next PR must not start before the current PR meets all readiness gates.
3. If a gate fails, the phase is not complete.
4. The contract in
   [file-visibility-and-incarnation-model.md](E:/dev/studiofs-cli/docs/refactor/file-visibility-and-incarnation-model.md)
   overrides any legacy behavior in the current branch.

## Main Target

The final system must have these properties:

1. the writing client reads accepted data immediately after local durable accept
2. local filesystem latency does not correlate with S3 upload latency
3. other clients see only coarse remote status: `uploading`, `updating`,
   `ready`
4. remote visibility is file-version atomic
5. large-file delete, patch, and rename behave correctly without global stalls

## PR-01: Freeze Contract And Gate Suite

### Goal

Turn the target behavior into repository docs and explicit gate tests.

### Files

Existing:

1. [architecture.md](E:/dev/studiofs-cli/docs/architecture.md)
2. [release-readiness.md](E:/dev/studiofs-cli/docs/release-readiness.md)
3. [append-only-ingress-segments.md](E:/dev/studiofs-cli/docs/append-only-ingress-segments.md)
4. [vfs.md](E:/dev/studiofs-cli/docs/vfs.md)
5. [vfs_test.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

New:

1. [file-visibility-and-incarnation-model.md](E:/dev/studiofs-cli/docs/refactor/file-visibility-and-incarnation-model.md)
2. [new-meta-engine-refactor-roadmap.md](E:/dev/studiofs-cli/docs/refactor/new-meta-engine-refactor-roadmap.md)

### Changes

1. Document `inode`, `incarnation`, `extent_record`, and `upload_state`.
2. Record the final read-after-write and remote-visibility contract.
3. Record the three critical edge cases.
4. Define the mandatory gate-test names for future PRs.

### Done when

1. The contract is documented in one place and is internally consistent.
2. The state machine for `inode/incarnation/upload_state` is fixed in writing.
3. The roadmap lists all 10 PRs and their order.
4. Test names for new scenarios are reserved in `vfs_test.rs` comments or
   helper scaffolding.

### Expected tests

1. `cargo check -p studiofs-vfs -p studiofs-meta`
2. `cargo test -p studiofs-vfs --test vfs_test -- --list`
3. `cargo test -p studiofs-meta --lib -- --list`

## PR-02: Runtime Isolation And Bounded Upload Pool

### Goal

Remove current stop-the-world execution paths before changing the data model.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [upload.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/upload.rs)
3. [lib.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/lib.rs)
4. [engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/engine.rs)

New:

1. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/runtime.rs`

### Changes

1. Move sync metadata calls off Tokio worker threads.
2. Move sync ingress/checkpoint filesystem I/O off Tokio worker threads.
3. Replace bursty `spawn-per-upload` behavior with a bounded worker pool.
4. Remove any foreground participation in dirty drain.

### Done when

1. Foreground read/write/flush no longer performs sync metadata work on async
   workers.
2. Foreground path no longer performs the heavy sync `std::fs` ingress path in
   the async hot path.
3. Upload backlog cannot create thousands of runnable tasks.
4. Existing ingress regressions are green and stable.

### Expected tests

1. `test_ingress_prefers_spill_before_backpressure`
2. `test_ingress_backpressure_wait_time_accumulates_until_drain`
3. `test_ingress_multi_writer_pressure_prefers_spill_without_wait`
4. `test_ingress_multi_writer_spill_recovers_after_crash`
5. `test_concurrent_flush_does_not_deadlock`

## PR-03: Acceptance WAL And Local Serving Index

### Goal

Make local durable acceptance the only condition for writer-visible success.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [lib.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/lib.rs)
3. [vfs_test.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

New:

1. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/ingress.rs`
2. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/serving.rs`

### Changes

1. Introduce append-only ingress WAL as the owner of accepted local data.
2. Introduce a live serving index over accepted records.
3. Make `flush()` and `close()` mean local durable accept, not remote progress.
4. Reduce `dirty_mem` to cache semantics only.

### Done when

1. After `write + flush/close`, the same mounted client reads the file fully
   without S3.
2. Slow or offline S3 does not break local readability of accepted data.
3. Restart restores accepted data from WAL and checkpoint replay.
4. `dirty_mem` is no longer the logical owner of file content.

### Expected tests

1. `writer_reads_full_file_before_any_remote_commit`
2. `writer_recovers_local_file_after_restart_before_upload_complete`
3. `test_crash_recovery_replays_ingress_spill_before_metadata_flush`
4. `test_crash_recovery_replays_ingress_spill_before_remote_fetch`

## PR-04: Inode And Incarnation Model In VFS

### Goal

Introduce explicit `pending` and `committed` file versions in the VFS layer.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [lib.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/lib.rs)
3. [vfs_test.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

New:

1. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/incarnation.rs`

### Changes

1. Add `committed_incarnation` and `pending_incarnation` to VFS file state.
2. For updates, derive pending state from committed base.
3. Keep at most one pending incarnation per inode.
4. Teach the local read path to apply pending overlay over committed base.

### Done when

1. Each inode has at most one pending and one committed incarnation.
2. A brand-new file maps to `upload_state = uploading`.
3. Updating an existing committed file maps to `upload_state = updating`.
4. Local reads prefer pending data without exposing it remotely yet.

### Expected tests

1. `new_file_enters_uploading_state`
2. `committed_file_mutation_enters_updating_state`
3. `local_reads_prefer_pending_overlay_over_committed_base`
4. `single_inode_never_has_two_pending_incarnations`

## PR-05: Extent Supersession, Unlink Cancel, Rename Neutrality

### Goal

Handle delete, partial overwrite, and rename correctly at data-identity level.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [upload.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/upload.rs)
3. [vfs_test.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

### Changes

1. Introduce `extent_record_id` and liveness/invalidation semantics.
2. Cancel a whole pending incarnation on `unlink()`.
3. Invalidate only touched extents on overwrite.
4. Keep `rename()` namespace-only.

### Done when

1. `write 50GB -> unlink` stops useful upload work for that file.
2. `write 50GB -> patch 2 blocks` re-uploads only touched extents.
3. Stale upload completions after unlink or patch cannot resurrect old state.
4. `rename()` does not restart upload or invalidate data.

### Expected tests

1. `large_file_unlink_cancels_pending_uploads`
2. `large_file_partial_overwrite_reuploads_only_touched_extents`
3. `stale_upload_completion_is_ignored_after_extent_invalidate`
4. `large_file_rename_does_not_restart_or_duplicate_uploads`

## PR-06: Materializer And Upload Intent Pipeline

### Goal

Fully decouple foreground write semantics from object materialization and upload.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [upload.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/upload.rs)
3. [lib.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/lib.rs)
4. [vfs_test.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

New:

1. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/materialize.rs`

### Changes

1. Introduce a `Materializer` actor reading accepted records.
2. Build pending manifests from accepted extents.
3. Reuse untouched extents from committed manifest during updates.
4. Keep upload completion batching internal only.

### Done when

1. Foreground write path no longer directly materializes or submits uploads.
2. Pending update manifests reuse unchanged committed extents.
3. Restart replays only live pending extents.
4. Internal batch size changes do not affect visibility semantics.

### Expected tests

1. `update_manifest_reuses_untouched_committed_extents`
2. `restart_replays_only_live_pending_extents`
3. `offline_s3_does_not_break_local_read_after_write`
4. `upload_completion_batching_does_not_change_visibility_contract`

## PR-07: Metadata Schema Becomes Committed-Only Plus upload_state

### Goal

Make `studiofs-meta` store committed manifests and coarse upload state only.

### Files

Existing:

1. [engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/engine.rs)
2. [types.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/types.rs)
3. [schema.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/schema.rs)
4. [events.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/events.rs)
5. [redb_engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/redb_engine.rs)
6. [pg_engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine.rs)

### Changes

1. Remove staged file-block ownership from metadata.
2. Store committed incarnation and committed manifest.
3. Store coarse `upload_state`.
4. Replace per-block publish semantics with file-level publish/switch semantics.

### Done when

1. Metadata is not required for local read-after-write.
2. Only `uploading`, `updating`, and `ready` are remote upload statuses.
3. Per-block remote visibility is no longer part of metadata API behavior.
4. redb passes unit tests on the new schema and transition rules.

### Expected tests

1. `redb_manifest_publish_sets_committed_head`
2. `redb_upload_state_transitions_follow_inode_fields`
3. `redb_partial_update_does_not_expose_new_blocks_before_publish`
4. `redb_rename_keeps_same_inode_and_incarnation_ids`

## PR-08: Atomic Publish And Remote Client Semantics

### Goal

Implement final remote-visible behavior for new files and updates.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/engine.rs)
3. [events.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/events.rs)
4. [redb_engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/redb_engine.rs)
5. [pg_engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine.rs)
6. [vfs_test.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

New:

1. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/commit.rs`

### Changes

1. New files become remotely visible as `uploading`, not as partial readable
   files.
2. Existing files remain remotely readable in their old committed version during
   updates.
3. Add atomic publish of fully built incarnations.
4. Switch `upload_state` to `ready` only after publish succeeds.

### Done when

1. Remote clients see `uploading` for a new file without exact progress.
2. Remote read/open on an uploading new file does not return partial content.
3. Remote clients see the old committed version while an update is in progress.
4. New content becomes visible remotely only after atomic incarnation switch.

### Expected tests

1. `remote_clients_see_uploading_marker_without_precise_progress`
2. `remote_open_on_uploading_new_file_returns_transient_unavailable`
3. `remote_clients_keep_previous_committed_version_during_update`
4. `atomic_switch_to_new_incarnation_after_full_commit`

## PR-09: PostgreSQL Becomes Pure Background Replication

### Goal

Remove PostgreSQL from the foreground file-data path.

### Files

Existing:

1. [pg_engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine.rs)
2. [engine.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/engine.rs)
3. [events.rs](E:/dev/studiofs-cli/crates/studiofs-meta/src/events.rs)

### Changes

1. Stop serializing foreground file-data behavior behind PG flush/pull.
2. Replicate only namespace changes, committed manifest switches, and upload
   status.
3. Move PG sync activity to background replication workers.
4. Preserve local accepted reads even while PG is lagging or busy.

### Done when

1. Foreground file reads, writes, and flushes do not wait for PG.
2. Heavy PG replication work is no longer inside the critical serialized path.
3. Upload-state replication and publish events happen asynchronously.
4. Remote convergence still works without staged file blocks in PG.

### Expected tests

1. `test_pg_remote_uploading_marker_replication`
2. `test_pg_remote_update_keeps_old_committed_version_until_switch`
3. `test_pg_publish_switch_is_atomic_across_peers`
4. `test_pg_foreign_events_do_not_block_local_accepted_reads`

## PR-10: QoS, Fairness, Legacy Cleanup, Soak

### Goal

Finish the architecture and remove transitional code paths.

### Files

Existing:

1. [vfs.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs)
2. [upload.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/upload.rs)
3. [fetch.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/fetch.rs)
4. [aimd.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/aimd.rs)
5. [lib.rs](E:/dev/studiofs-cli/crates/studiofs-vfs/src/lib.rs)
6. [release-readiness.md](E:/dev/studiofs-cli/docs/release-readiness.md)

New:

1. `E:/dev/studiofs-cli/crates/studiofs-vfs/src/qos.rs`

### Changes

1. Add separate credits for acceptance, materialize, upload, commit, and fetch.
2. Add fairness so one giant file cannot starve all other I/O.
3. Remove legacy staged-block and transitional drain logic.
4. Run long soak and fault-injection scenarios.

### Done when

1. The only valid writer backpressure reason is local durable-budget
   exhaustion.
2. Upload backlog cannot consume all execution capacity and stall the
   filesystem.
3. There is only one write pipeline left in the code.
4. Slow or offline S3 no longer produces a global filesystem stall in soak
   tests.

### Expected tests

1. `slow_s3_does_not_stall_local_reads_and_writes`
2. `backpressure_only_on_local_budget_exhaustion`
3. `single_large_file_upload_does_not_starve_small_foreground_io`
4. `twenty_four_hour_soak_no_global_fs_stall`
5. `recovery_after_offline_s3_preserves_local_accepted_view`

## Mandatory Final Acceptance Criteria

The refactor is complete only when all of the following are true:

1. The writing client can read accepted data immediately after `flush()` or
   `close()`.
2. Background S3 upload cannot freeze foreground local reads and writes.
3. `unlink()` cancels the whole pending incarnation.
4. Partial overwrite of a large file re-uploads only touched extents.
5. `rename()` does not affect data identity or upload progress.
6. Other clients see only coarse upload state and never a partial new version.
7. Remote visibility switches atomically at committed-incarnation granularity.
