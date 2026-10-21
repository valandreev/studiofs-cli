# Recover WinFSP inode invalidations after queue overflow

## Purpose and Context

SFS-873: individual inode requests beyond the 8192-target WinFSP queue are lost.
The foreground producer must remain nonblocking at capacity, and recovery must
retain bounded memory. Queue acceptance never proves native cache invalidation.
The current branch is `feature/sfs-873-winfsp-invalidation-queue-overflow-permanently-drops`
in `E:/dev/studiofs-cli`; the starting worktree was clean.

## Progress

- [x] (2026-10-21) Trace `inval_inode`, queue admission, draining, and VFS retirement.
- [x] (2026-10-21) Add regression requiring recovery without another mutation.
- [x] (2026-10-21) Add bounded local inode-ID pages and overflow reconciliation.
- [x] (2026-10-21) Focused tests and native mounted cached-handle check.
- [x] (2026-10-21) Clippy for all three changed crates, formatting, final diff review.
- [x] (2026-10-21) Implementation ready for commit/push and Linear review handoff.

## Surprises & Discoveries

VFS retirement clears OS-cache generations before queue admission. Re-scanning
only runtime caches can miss the lost inode. Root notifications are not a proven
whole-volume purge; Reset explicitly remains unsupported.

The red regression failed at `overflow must retain reconciliation work` (one
test executed). Metadata pagination passed its sparse/base-local/MAX-ID test.
The first green attempt exposed missing forwarding in RuntimeMetaHandle; add
`list_inode_ids_after` to `crates/studiofs-vfs/src/runtime.rs` as well.
Refill must happen before draining: filling the just-freed queue would compete
with requeueing historical notifications deferred for active writers.

The native test initially failed twice: two notifications were submitted, but
the same cached handle kept old bytes and its read callback count stayed at one.
The locked winfsp `set_name` convenience method appends a NUL and counts it in
the notification name size. Switching the shared helper to counted UTF-16 via
`set_name_raw` made the native test pass in 1.67 seconds. This is necessary for
the recovered inode requests to produce the intended native cache effect.

## Decision Log

- Decision: retain an overflow timestamp, cursor and generation, then scan local
  metadata candidate IDs in bounded pages through the existing notification path.
  Rationale: a full-volume snapshot or overflow list grows with inode count;
  numeric ID iteration performs badly with sparse allocator namespaces.
  Date: 2026-10-21.
- Decision: fix the shared counted-name encoding rather than weaken the native
  acceptance assertion. Rationale: the native regression reproduced ineffective
  purges independently of queue acceptance. Date: 2026-10-21.

## Plan of Work and Milestones

First add `MetaInodes::list_inode_ids_after(after, limit)` in
`crates/studiofs-meta/src/engine.rs`, with bounded redb table ranges in
`redb_engine/inodes.rs` and local delegation in `pg_engine/local_traits.rs` and
`read_only.rs`. Candidate IDs may be tombstoned; notification preparation already
resolves current attributes and skips removed inodes.
`crates/studiofs-vfs/src/runtime.rs` forwards the method through RuntimeMetaHandle.

Then update `crates/studiofs-winfsp/src/context/invalidation.rs` so overflow
retains reconciliation state. A timer drain refills bounded work outside the queue
lock. Concurrent overflow restarts the scan, preserving its oldest timestamp.
Existing writer deferral and native submission behavior remain authoritative.
Update queue regressions in `context.rs` and the coherence contract.

## Concrete Steps and Validation

Run from `E:/dev/studiofs-cli`, sequentially:

    cargo test -p studiofs-winfsp --lib winfsp_invalidation_queue -- --test-threads=1
    cargo test -p studiofs-meta --lib list_inode_ids_after -- --test-threads=1
    cargo test -p studiofs-winfsp --lib invalidation -- --test-threads=1
    cargo test -p studiofs-winfsp --lib semantic_ -- --test-threads=1
    cargo test -p studiofs-winfsp --lib build_notify_info -- --test-threads=1
    cargo test -p studiofs-winfsp --lib overflow_reconciliation_preserves_writer_deferral -- --test-threads=1
    cargo clippy -p studiofs-winfsp -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

The new overflow regression must fail before implementation and pass afterwards.
Verify sparse IDs, page limits, repeated overflow, queue closure, and old delivery
age. Use an existing local-only WinFSP mounted harness if feasible; preserve logs
under target. Full suites remain operator-run per user instructions.

## Interfaces, Dependencies, and Recovery

No dependency or remote service is added. Page enumeration is read-only. Unsupported
engines fail explicitly and retain reconciliation for retry. Shutdown closes the
queue. All checks are repeatable; preserve unrelated work and do not reset Git.

## Outcomes & Retrospective

Focused checks passed: metadata pagination (1), WinFSP invalidation (14 plus
1 explicitly ignored mount test), semantic invalidation (11), notification-name
encoding/path limits (2), and overflow writer deferral (1). The ignored test was
also run explicitly on the unused Z: drive with a real WinFSP mount and passed.
It proves an admitted cached read-only handle reloads overwritten local bytes
after its notification overflows. Logs, including both failed attempts, are in
`target/sfs873-native-overflow*.log`. The drive was unmounted afterwards.
The final native rerun additionally required a new read callback on the same
handle and passed in 1.65 seconds. Clippy for all three changed crates, rustfmt
check, and `git diff --check` passed. Independent review found no remaining
actionable issue. Delivery uses the existing feature branch; no PR is requested.

Native submission remains asynchronous and delivered_generation stays unknown.
This focused native observation does not establish general kernel acknowledgement,
full integration, multi-client promotion, Linux/macOS, or performance acceptance.
Full suites remain operator-run, including the issue's full crate command:

    cargo test -p studiofs-winfsp --lib -- --test-threads=1

Revision: initial plan records the bounded local-page design and validation scope.
Revision: record runtime forwarding, writer-safe refill ordering, native red/green
evidence, and the counted-name correction required by the observed kernel behavior.
