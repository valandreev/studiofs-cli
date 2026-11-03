# Keep local unlink authoritative during pending replay

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

SFS-963 blocks SFS-960: a returning create/rename followed by pending journal replay must not restore a locally unlinked name. Preserve SFS-234: canceling an unpublished update retains the prior peer-visible file and its published payload.

## Progress

- [x] (2026-11-03) Recorded root `/home/val/dev/studiofs-cli`, clean worktree and branch `feature/sfs-960-linux-fuse-unlink-returns-enoent-right-after-a-successful`; read the issue and metadata/replay contracts.
- [x] (2026-11-03) Reproduced committed regression: one test fails at the expected pending-replay lookup assertion.
- [x] (2026-11-03) Isolated missing local name intent and implemented explicit local removal plus durable DeletedBeforePublish reconciliation protection.
- [x] (2026-11-03) Validated echo, repeated pending replay, rollback acknowledgement, journal pruning, exclusive recreation, peer retention, foreign replacement and open-unlink/write/close.
- [x] (2026-11-03) Read-only review identified a real foreign-unlink compatibility regression; restored the original base-dentry publication decision. Focused live peer-unlink test now passes.
- [x] (2026-11-03) Completed final crate checks, rebuilt release, repeated mounted smoke and generic/241, and independently verified cleanup.

## Surprises & Discoveries

The committed reproduction actually journals PublishRegularFileBaseSlices + SetAttr (rollback of a committed version), not RemoveAttr. Its missing local namespace event exposes the echoed name already before pending replay. The issue description/comment overstated the RemoveAttr case. Adding the name event fixes the original failure, but an added rollback-ack assertion failed until the durable DeletedBeforePublish lifecycle also prevented inode reconciliation from reinstalling old base names. Explicit post-pull dentry reconciliation needs the same deletion authority after journal pruning.

Event tags 16/17 are retired snapshot tags and cannot be reused. New local event tag 18 is covered by exact bytes/roundtrip and remains excluded from PostgreSQL projection.

## Decision Log

- Decision: retain the SFS-960 branch and commits as the baseline, without rewriting history. This task depends on its namespace authority correction. No new persistent table is needed.
- Decision: keep the existing deleted_base_dentry remote-publication decision and use explicit RemoveLocalEntry for the previously missing local-only deletion rather than infer it from neighboring rollback events; this remains safe when publication windows split. The event is local-only and uses the existing envelope with new tag 18. Older binaries cannot reopen a journal containing it; peer clients see no new event type.
- Decision: reuse DeletedBeforePublish to keep inode reconciliation from reinstalling names after rollback acknowledgement, and make dentry reconciliation hide only base bindings of those deleted inodes. Different foreign replacement inodes remain eligible for convergence.
- Decision: follow the repository's crate-focused validation limits over skill requests for workspace-wide runs; no full ignored suite.

## Outcomes & Retrospective

Original regression was red before the change and green afterward. Further red/green coverage exposed inode reconciliation after rollback acknowledgement. The corrected design records the missing local name intent without changing the existing remote deletion decision: an already-known base dentry still journals RemoveEntry; a cancellation with no base binding journals RemoveLocalEntry. Both feed the same namespace reducer. DeletedBeforePublish protects local serving authority after acknowledgement and pruning.

Intermediate validation: replay 150 passed before removing the extra policy-expansion fixture; current final suite is being rerun with open-unlink coverage. Redb 391 passed/1 ignored, backlog 79 passed/6 ignored, event codec 11 passed. A live ordinary remote-unlink test passed. The extra live foreign-uploading unlink test initially failed (Updating versus Ready), identifying the unrequested publication-policy expansion; after restoring the original decision it passed one test. Intermediate release smoke and generic/241 passed, but final rebuild/rerun is required.

## Context and Orientation

`crates/studiofs-meta/src/redb_engine/fs_ops.rs::execute_unlink` removes local names and records lifecycle state. `append_unlink_events` avoids deleting an old published version remotely. `redb_engine/replication.rs` separately applies base, serving mirror and pending local journal state. `pg_engine/remote_apply.rs` reduces pending namespace state and rebases it after a self batch. Base rows hold the remote version; serving rows and tombstones hold local overrides. Removing a mirror can expose base again.

## Plan of Work and Milestones

First use the committed `pending_replay_of_local_unlink_hides_echoed_rename_destination` test to isolate whether self replay or subsequent pending replay exposes the name. Inspect every caller of any modified helper. Correct pending local deletion semantics at the shared replay boundary and reuse the existing pending namespace reducer if sufficient. Extend focused regressions for name reuse, repeated replay and preservation of published peer content/cleanup. Finally run the remote-apply and relevant redb suites, cargo check, Clippy, formatting and diff review. Mounted validation requires rebuilding the product and host FUSE/services; report it separately from unit evidence.

## Concrete Steps

From `/home/val/dev/studiofs-cli`:

    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::pending_replay_of_local_unlink_hides_echoed_rename_destination -- --exact
    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::
    cargo test -p studiofs-meta --lib redb_engine::tests::
    cargo check -p studiofs-meta
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo fmt -p studiofs-meta -- --check
    git diff --check

## Validation and Acceptance

The committed regression must change from one expected failure to one pass. A just-unlinked name stays absent after self echo and repeated pending replay and can be recreated exclusively. The existing published-update cancellation regression must preserve the old peer payload and queue only unpublished uploaded objects for cleanup. Preserve existing foreign-base unlink behavior rather than expanding SFS-234 publication policy to echoed writer entries. The last-close metadata cleanup path and journal pruning must leave the removed name absent. Every reported test command must execute nonzero tests.

## Idempotence and Recovery

Unit fixtures use disposable redb databases. Retain temporary logs under `target/sfs963/`; do not load `.env` unless a live test needs it. Preserve unrelated files and never reset commits.

## Artifacts and Notes

Initial red result: `replaying the local unlink must not expose the echoed base destination` at `pg_engine/tests/remote_apply.rs`.

## Interfaces and Dependencies

Reuse `PendingLocalNamespaceState`, indexed dentry lookup and existing replay setters. No dependency, configuration or new persisted table is introduced. MetaEvent adds RemoveLocalEntry with local-journal tag 18, retaining the prior tags and envelope.

Initial revision: document the red regression and investigate local versus mirror removal before implementation.

Investigation/implementation revision: correct the observed rollback event sequence, record explicit local intent and acknowledgement/pruning boundaries, and reserve retired tags.

Review revision: retain the original remote deletion policy after the live foreign-unlink regression, add open-unlink/write/close coverage, and distinguish intermediate mounted evidence from the pending final binary.

## Final focused validation

On the final implementation, `cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::` passed 150 tests, with 45 infrastructure tests ignored; `redb_engine::tests::` passed 391 with one ignored; the open-unlink and closed-unlink focused filter passed two. The codec selection passed 11, including reserved-tag rejection and exact tag-18 bytes. Two selected live PostgreSQL tests passed individually: `test_pg_remote_unlink_removes_file_from_namespace_immediately` and, after restoring the old publication decision, `test_pg_peer_can_unlink_foreign_visible_uploading_entry`. The independent review's confirmed compatibility finding is corrected; its open-unlink concern is covered through metadata set_attr, last-close remove_attr, self echo, pending replay, acknowledgement, pruning and recreation. This test models the metadata cleanup calls, not a full VFS handle close.

The final release rebuild and new mounted runs remain pending. Intermediate mounted runs passed under binary SHA-256 `713fc2cf45784994cd966bef25d5d741495bf8bd87b57fd055d8ad2c6e41cfcc`: smoke report `target/e2e-runs/sfs963-smoke-20261103/report.json`; generic/241 artifacts `target/xfstests/sfs963-replay-20261103T182213Z/generic241/` report exit 0, 78 seconds, `Passed all 1 tests`. These are retained but do not establish final acceptance after the remote-policy correction.

Validation revision: record the final local counts and live-test repair, and explicitly limit the metadata open-unlink model and intermediate binary evidence.

## Final mounted acceptance and delivery

Implementation commit `d4790195` is on `feature/sfs-963-metadata-self-replay-resurrects-a-locally-unlinked`, preserving the original SFS-960 history. The working tree was clean after that commit. No push, PR, merge or issue closure was requested or performed.

Final `cargo build -p studiofs --release -j 8` passed (4m33s). Product SHA-256: `b0bb9ccdcd8cb3e62e7b09692919daab611f787e4bae7fee87d367f85cd4319e`. With that binary, ran:

    target/debug/studiofs-e2e run basic-multi-client-smoke --infra env --studiofs-bin target/release/studiofs --run-id sfs963-d4790195-smoke --seed 963 --retain always --timeout 180s

The registered smoke passed validation and cleanup; report `target/e2e-runs/sfs963-d4790195-smoke/report.json`.

Reused the existing xfstests setup script with new dedicated volume names and all temporary mount/client roots under target. Its final command was:

    tools/xfstests/run.sh /home/val/dev/studiofs-cli/target/xfstests/tools/xfstests /home/val/dev/studiofs-cli/target/xfstests/sfs963-replay-20261103T183017Z/generic241 generic/241

The selected original dbench case passed, exit 0, 80 seconds, `Passed all 1 tests`. Artifacts: `target/xfstests/sfs963-replay-20261103T183017Z/generic241/manifest.tsv`, `metadata.tsv`, `cases/241/check.log`, and `cases/241/results/result.xml`. Independent host inspection found zero remaining mounts and zero live recorded daemon PIDs for the final smoke/xfstests roots. This final acceptance supersedes the intermediate binary results.

Final focused results: replay 150 passed/45 ignored; redb 391 passed/1 ignored; backlog 79 passed/6 ignored; codec 11 passed; two selected live PostgreSQL tests each passed one test. `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt -p studiofs-meta -- --check`, and `git diff --check` passed. All logs are retained under `target/sfs963-*.log`. The initial live foreign-unlink failure is explained above and repaired; it is not an outstanding failure.

Outcome: the committed deterministic SFS-963 regression is green through echo, repeated pending replay, rollback acknowledgement, pruning and exclusive recreation. Published peer content and pending cleanup eligibility retain their existing semantics. A different foreign inode can replace the locally hidden old binding. Open-unlink/write/last-close is covered at the metadata-call level. No full workspace, full ignored suite, full generic suite, Windows/macOS mount, or object-store GC end-to-end gate was run. The new local-only journal tag prevents downgrade to an older binary while such events remain in local state; it never reaches peer clients through PostgreSQL.

Completion revision: record final exact binary, commands, mounted evidence, independent cleanup and the local-journal downgrade limitation.
