# Pack random-write fragments into shared payload objects

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

SFS-900 reduces local materialization and upload work after random 4 KiB writes.
Several file segments share one immutable payload of at most the configured
block size. Reads retain each segment's file offset, object offset and sequence.
A sustained random writer must also feed background upload before fsync.

## Progress

- [x] (2026-10-25) Record checkout, clean status and existing SFS-900 branch; read issue and trace writer, drain, reattach, replay and truncate paths.
- [x] (2026-10-25) Reproduce per-fragment materialization, incorrect upload length and missing aged drain; add stale-epoch refusal regression.
- [x] (2026-10-25) Pack finished slices across the entire inode, retain reads/retry ownership and persist once per object.
- [x] (2026-10-25) Correct commit/replay/truncate geometry and refuse contiguous reattach for scattered or incomplete mappings.
- [x] (2026-10-25) Bound dirty residency through the existing deferred worker; age regression passes.
- [x] (2026-10-25) Register mounted-random-write-packing acceptance with byte readback, object bound, pre-fsync upload samples and retained fsync timing.
- [x] (2026-10-25) Focused tests, all-target checks/Clippy, formatting and final diff review passed; rebuilt mounted scenario passed with cleanup.

## Surprises & Discoveries

The upload queue is memory-only and startup rebuilds it from local metadata.
Both local-dirty commit and truncate requeue infer object length from file
coordinates. Startup emits one descriptor per resolved span and must aggregate
shared objects before removing them from the dirty-object set. The existing
idle-tail worker waits for file inactivity, which sustained random writes prevent.
Linear's advertised get_agent_context endpoint returned Tool not found.

## Decision Log

- Decision: preserve separate overlay slices and existing object storage format.
  Rationale: PayloadRef already represents offsets into shared objects; no data
  migration or remote synchronous callback is needed. Date: 2026-10-25.
- Decision: represent a contiguous object's file origin as Option<u64>; None
  disables contiguous stale reattachment for scattered objects.
  Rationale: inferring a file mapping from packed bytes can corrupt data. Existing
  metadata references still allow marking/rebasing a live object. Date: 2026-10-25.
- Decision: reuse the per-inode deferred materialization task and local durability
  boundary. Rationale: no new worker, service, dependency or tuning flag is needed.
  Date: 2026-10-25.

## Context and Orientation

`crates/studiofs-vfs/src/vfs/writer_stack.rs` owns buffered slice writers. Finish
first detaches bytes, retains read spans during persistence, and later installs
`FinishedWriterSlice` into `local_dirty.rs`. A slice maps a file range to bytes
inside an immutable object; its sequence chooses the newest overlapping write.
`vfs/sync_ops.rs` drives strict and deferred materialization. `studiofs-cache`
CachedStore persists payloads and pins PendingUpload queue entries until terminal.
`vfs/upload_lifecycle/worker.rs` marks uploaded objects and can reattach a missing
object to uncovered file ranges. `vfs/startup_replay.rs` reconstructs uploads;
`vfs/namespace_ops.rs` retains uploads over truncation/growth.

## Plan of Work and Milestones

First reproduce the per-fragment object count with scattered writes and verify
bytes via each returned object's offset. Pack newly detached slices before
exposing materializing state; retain full shared bytes for retries and persist
one entry per object. Existing pending materialization must keep its identity.
Keep full-block streaming on its existing fast path.

Next make PendingUpload.object_file_offset optional throughout callers and tests.
Aggregate object length in object coordinates and obtain actual local object
length where available. All segments must agree on a checked file origin to
permit contiguous reattachment. Update commit, startup replay and truncate paths
and test scattered segments, partial supersede and stale epochs.

Finally allow the existing idle-tail worker to finish sufficiently aged writer
state even while writes continue. Test with explicit timestamp control and
observable materialization rather than scheduling sleeps. Verify object usage
and retention through overwrite/truncate regressions. Document focused commands
and mounted prerequisites in docs/tests.md.

## Concrete Steps and Validation and Acceptance

Work from `/home/val/dev/studiofs-cli`. Run focused `cargo test -p studiofs-vfs
--lib <filter> -- --nocapture` and cache queue tests after the type change. New
regressions must fail before the corresponding fix and pass afterwards. Run
`cargo check -p studiofs-vfs -p studiofs-cache`, `cargo clippy -p studiofs-vfs
-p studiofs-cache --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and
`git diff --check`. Do not run workspace-wide or full ignored suites.

Mounted acceptance requires a rebuilt studiofs binary, usable /dev/fuse and
local PostgreSQL/object storage, and the registered studiofs-e2e runner. Retain
artifacts under target. Measure 256 MiB random 4 KiB writes during 15 seconds,
observe active upload with a pending backlog, and compare fsync time and object
count to dirty bytes divided by block size. Report unrun live gates explicitly.

## Idempotence and Recovery

Focused tests use isolated target temporary directories. Preserve unrelated
changes. Pending payloads stay pinned until upload completion; any materialization
failure keeps detached bytes and sticky errors for the next strict barrier.
No persistent schema or object encoding changes are planned.

## Interfaces and Dependencies

Reuse Bytes/BytesMut, CachedStore and existing overlay metadata APIs. Optional
contiguous origin is an in-memory queue contract; no dependency is added.

## Outcomes & Retrospective

Initial red regressions: five objects instead of two for 20 bytes, and inferred upload length 36 instead of actual 12. First broad run: 133 passed, one retention assertion required a slice-write setup because the ordinary block-write path correctly copies all surviving bytes into a replacement object. Subsequent focused runs passed 185 VFS tests, 52 cache/queue tests, one E2E file-count test, and 35 VFS drain/packed/deferred-release tests after the live-discovered drain correction. All-target checks for VFS/cache and E2E passed. Final Clippy passed for all targets of VFS, cache and E2E. The rebuilt live rerun passed with successful cleanup: 8,451 random 4 KiB writes, 11 physical payload objects, exact readback, six samples with simultaneous pending and active uploads, and fsync 2,147.8 ms. Metadata commit histogram sum increased by 2,490.5 ms across three concurrent/background and foreground commits during fsync; this aggregate is diagnostic, not an isolated redb transaction timing. Both failed and passed runner artifacts remain under target/e2e-runs. Full workspace/ignored suites and Windows/macOS mounted gates were not run.

Revision: initial plan records traced geometry hazards and the minimum complete
packing, retry, replay and background materialization scope.

Revision: independent review found per-chunk packing missed sparse per-inode
packing; gather across the entire inode and scatter the regression across chunks.
Replay geometry is aggregated across all resolved spans before the dirty-object
set consumes the first reference. Refuse reattachment for incomplete mappings as
well as scattered ones: a surviving segment cannot prove absent payload bytes'
original file positions. Added a registered E2E scenario because no existing
scenario exercised sustained random writes and sampled background upload.

Revision: first live scenario proved packed byte correctness (11,165 writes,
15 payload objects, 1,409 ms fsync) but failed the upload-during-writes gate.
The deferred task loops after idle/aged materialization and only commits local
metadata when that loop exits; continuous writes kept refilling it. Complete
the pass after aged materialization so its existing local metadata drain queues
uploads, then let the existing task registry reschedule remaining dirty bytes.
This mounted failure is the regression for the missing drain boundary.

Revision: Clippy identified the expanded optional-origin batch tuple as overly
complex; share the existing positional submission shape through UploadOp, reused
by CachedStore and startup replay. No lint exception or dependency was added.

## Final command evidence

All commands ran from `/home/val/dev/studiofs-cli`; Cargo used `--offline` after
the initial dependency fetch. Commands and nonzero test results were:

    cargo test --offline -p studiofs-vfs --lib -- packed_ writer_stack startup_replay completed_block_materialization process_upload_ explicit_flush_sparse local_dirty:: --nocapture
    185 passed; 0 failed
    cargo test --offline -p studiofs-vfs --lib -- packed_ deferred_materialization deferred_release writer_stack_retries_transient_materialization_failure --nocapture
    35 passed; 0 failed
    cargo test --offline -p studiofs-cache --lib -- upload_queue:: cached_store::tests::sfs752_ submit_uploads_batch --nocapture
    52 passed; 0 failed
    cargo test --offline -p studiofs-e2e --lib -- random_write_packing scenario::registry --nocapture
    1 passed; 0 failed
    cargo check --offline -p studiofs-vfs -p studiofs-cache --all-targets --message-format short
    cargo check --offline -p studiofs-e2e --all-targets --message-format short
    cargo clippy --offline -p studiofs-vfs -p studiofs-cache -p studiofs-e2e --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check
    cargo build --offline -p studiofs -p studiofs-e2e
    target/debug/studiofs-e2e run mounted-random-write-packing --studiofs-bin target/debug/studiofs --run-id sfs900-random-packing-green --retain always --timeout 180s
    status=Passed; cleanup=passed; exit_code=0

The selected Linux scenario proves local byte mapping, bounded payload count and
upload progress during sustained writes. It does not replace full scenario,
crash-restart, cold-peer, cross-platform or release-performance gates.

Revision: record final focused command evidence, live metrics and remaining
validation boundaries after independent review and successful native rerun.
