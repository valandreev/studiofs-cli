# Recover WinFSP inode invalidations after queue overflow

## Purpose and Context

SFS-873: individual inode requests beyond the 8192-target WinFSP queue are lost.
The foreground producer must remain nonblocking at capacity, and recovery must
retain bounded memory. Queue acceptance never proves native cache invalidation.
The current branch is `feature/sfs-873-winfsp-invalidation-queue-overflow-permanently-drops`
in `<repo-root>`; the starting worktree was clean.

## Progress

- [x] (2026-10-21) Trace `inval_inode`, queue admission, draining, and VFS retirement.
- [x] (2026-10-21) Add regression requiring recovery without another mutation.
- [x] (2026-10-21) Add bounded local inode-ID pages and overflow reconciliation.
- [x] (2026-10-21) Focused tests and native mounted cached-handle check.
- [x] (2026-10-21) Clippy for all three changed crates, formatting, final diff review.
- [x] (2026-10-21) Implementation ready for commit/push and Linear review handoff.
- [x] (2026-10-21) Reproduce open-unlinked candidate and rejected-path-retry defects.
- [x] (2026-10-21) Block health promotion and live cache admission during recovery.
- [x] (2026-10-21) Validate deep-review corrections and dispositions; ready for commit/push.

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

Run from `<repo-root>`, sequentially:

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

## Review follow-up

Removed the redundant refill metrics snapshot, reused atomic queue extension in
the native test, and shared the existing bounded wait helper. Replaced the accepted
counter with a last-cursor check that explicitly accepts an empty terminal page;
without the empty-page case, a completed full page would never finish reconciliation.
A focused regression covers this terminal state.

Retained `overflow_epoch`: a short metadata page read outside the queue lock can
become stale while its cursor remains `None`. Producers can fill the queue with
all IDs from that old page, then reject a newly created inode. Without the epoch
check, refill coalesces the old page and clears overflow, losing the new inode's
purge request. Cursor equality alone cannot distinguish those snapshots.

Review validation: the invalidation group passed 15 tests (native test ignored
there), and the explicit native cached-handle test passed in 1.67 seconds; log:
`target/sfs873-native-overflow-review.log`. The overflow group also passed six
tests before the equivalent final spelling of the completion predicate. Full
suites remain operator-run. The concurrent snapshot schedule was checked against
the source; no new concurrency hook was added for this review cleanup.
WinFSP all-target Clippy with warnings denied, rustfmt, and `git diff --check`
also passed.

## Deep-review follow-up

Source: a local deep-review report against `38e3a671` (kept out of the
repository by `.gitignore`). Each finding and its disposition is recorded below,
so this plan does not depend on that report.

- 2.1 confirmed by a failing regression: an open-unlinked file has `nlink == 0`
  but still has metadata. Preparation now reads authoritative attributes first,
  skips removed/unlinked candidates, and reuses the kind without a fallback.
- 2.2 raw tombstone candidates are intentional: filtering before `take(limit)`
  would violate bounded scanned work, while filtering a bounded raw page without
  returning its scanned cursor would break progress. Deleted candidates now take
  one authoritative lookup, without a redundant path-resolution attempt. Keep the
  bounded raw-page API; add metadata pagination coverage with tombstones.
  No measured multi-second latency or transaction-cost acceptance is claimed.
- 2.3 retain refill-before-drain: the freed slots must remain available for
  requeued historical writer targets. A full initial queue needs another timer
  pass; repeated pages per callback are not required for eventual recovery.
- 3.1 confirmed for legacy individual inode requests, which do not carry semantic
  generation gaps. Add an explicit recovery signal blocking health promotion
  until the sweep and prepared notification retries finish, followed by fresh
  catch-up coverage. Cache admission also checks the live adapter signal because
  health observations can lag; its focused regression failed before that check.
  Native consumption remains unobserved. The report's claim
  of permanent degradation without external changes overlooks fallback polling.
- 3.2 confirmed by a failing regression: unpublished inode notifications become
  Path retries. Shared admission now starts reconciliation for rejected known
  inode paths too; lost historical names still remain observable failures.
- 3.3 document current single-path hardlink limitation and promotion gap, rather
  than add unbounded alias enumeration to a bounded recovery queue.
- 4.1 partially addressed by reading attributes before path resolution and reusing
  kind. Keep authoritative deletion classification instead of silently defaulting
  failed lookups to a regular file. Further resolver/cache optimization is deferred.
- 4.2/4.3 defer streaming merge and stack encoding: bounded existing allocations
  have no measured bottleneck, and neither change is needed for correctness.
- 4.4 remove duplicate admission checks by sharing `push_locked` with path retries.
- 5 reuse the existing wait helper for mount readiness; add focused regressions
  for the confirmed defects. Do not enforce a same-tick recovery requirement.

Deep-review validation:

- Metadata `list_inode_ids_after`: 2 passed, including a real deleted base inode
  followed by a live inode across the page boundary.
- VFS `coherence::tests`: 8 passed; adapter-recovery admission regression: 1 passed.
- WinFSP `overflow_`: 8 passed, 1 native test ignored in that group.
- WinFSP `invalidation`: 17 passed, 1 native test ignored; `semantic_`: 11 passed.
- Final open-unlinked preparation regression: 1 passed. Both original preparation
  and path-retry regressions failed before their corrections; the new admission
  assertion likewise failed before adding the live signal check.
- Explicit native cached-handle overflow test: 1 passed in 1.68 seconds. Artifact:
  `target/sfs873-native-overflow-deep-review.log`.
- Strict all-target Clippy for metadata, VFS and WinFSP, rustfmt check, and
  `git diff --check` passed. Independent final VFS review found no further issue.

No full suites, multi-client alias acceptance, throughput benchmarks, or native
kernel acknowledgement were established. Full VFS/WinFSP suites remain operator-run:

    cargo test -p studiofs-vfs -p studiofs-winfsp --lib -- --test-threads=1
