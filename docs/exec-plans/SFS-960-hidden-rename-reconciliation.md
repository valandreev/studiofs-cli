# Preserve local uploading files during self replay

This ExecPlan follows `.agent/PLANS.md` and remains a living record.

## Purpose / Big Picture

A successful local rename must not lose its destination when this client's PostgreSQL batch returns. A file still uploading can be absent from the remote base (the peer-visible metadata tables), while its local redb name remains authoritative. SFS-960 option B covers that ENOENT only; delete-before-publication resurrection and unconditional rename-source cleanup are separate work.

## Progress

- [x] (2026-11-03) Recorded checkout `/home/val/dev/studiofs-cli`, task branch, clean status, and existing regression commit `07bc399b`.
- [x] (2026-11-03) Read metadata, mounted IO, replay contracts and ADR 0012; traced projection, publication exclusions, replay and recent-flush reconciliation.
- [x] (2026-11-03) Observed the new regression fail with EntryNotFound after the post-pull protection snapshot and local Ready commit, then pass after the correction.
- [x] (2026-11-03) Removed the redundant inode pass; retained dentry reconciliation. Transactional stale-attr regression and two live PostgreSQL publication/unlink tests pass.
- [x] (2026-11-03) Ran focused replay/backlog/layout tests, check, Clippy, formatting and diff review; independent review found no important issues. Mounted validation is gated by missing /dev/fuse.
- [ ] Create the separate EEXIST/EIO Linear issue after explicit approval: automatic approval review rejected its creation as an external disclosure. A local draft preserves the exact requested content.

## Surprises & Discoveries

`RecentFlush.overlay.attr_inodes` includes rename targets even when projection hides their SetAttr and AddEntry. The first fixture with only an uploading inode passed: lifecycle protection worked until publication. Scheduling a Ready attribute commit after the overlay snapshot exposed the actual race deterministically. The normal replay inode candidate collector intentionally considers regular-file attrs, publication and removal, not rename alone. Post-pull reconciliation nevertheless uses the broader conflict set. Local event ranges marked published are excluded from ordinary journal protection.

## Decision Log

The user chose option B on 2026-11-03. The proven smallest correction removes the second inode pass, which had no accurate batch candidate set and sampled protection outside its write transaction. Retain the dentry pass to avoid expanding scope; normal replay already reconciles inode state under its transaction. Preserve the existing red delete-before-publication regression without fixing that independent behavior. Prefer deleting redundant reconciliation over adding persistent state or changing the wire format, but first prove the exact boundary with a regression. The repository contract takes precedence over skill requests for workspace-wide tests and an additional plan approval: run crate-focused checks and continue under the user's authorization.

## Outcomes & Retrospective

The production correction deletes the broad inode pass from `reconcile_recent_flush`; no new authority state or protocol changes were needed. The dentry pass remains. The new test proves the destination survives replay and a later Ready commit, then unlink succeeds. It models the local publication boundary with `set_attr`, not the full uploader. The old stale-serving-attr test now exercises `apply_replay_page_atomically` to demonstrate that transactional replay owns the necessary inode reconciliation. The unused test helper was removed.

Validation on 2026-11-03: replay module 145 passed, one known pre-existing failure, 45 ignored; backlog module 79 passed, six ignored; layout three passed; new regression separately one passed. The known red test is `pending_replay_of_local_unlink_hides_echoed_rename_destination`, preserved unchanged for the separate EEXIST/EIO work. `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, crate formatting and `git diff --check` passed. Independent read-only review found no important issues.

Two targeted live PostgreSQL tests each passed one test after running outside the network sandbox: `test_pg_nested_temp_rename_then_publish_visible_on_remote_peer` and `test_pg_remote_unlink_removes_file_from_namespace_immediately`. The first initially failed to connect inside the sandbox; that attempt is not a behavior failure. There is no `/dev/fuse` in this session, so the product was not rebuilt for a mounted rerun and neither dbench nor generic/241 is claimed fixed by mounted evidence. The workspace/full ignored suites were not run.

Automatic approval review rejected creation of the separate Linear issue because its payload would export internal code/commit/bug details to an external service without payload-specific authorization. No retry or alternative external write was attempted. The issue remains uncreated, with a reviewable local draft under `target/sfs960/`.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/remote_apply.rs` converts the local journal (`MetaEvent` records) to peer-visible batches and applies returning batches. A self batch is an echo of this client's publication. Hidden files have no complete peer-visible version yet. `replication_flush.rs::flush_pending_claimed_batch` excludes published ranges from local overlay conflicts (keys that block replacement by remote state). `metadata_sync.rs::pull_and_reconcile_foreign_events_locked` runs replay and a second reconciliation afterward. `tests/remote_apply.rs` exercises real redb transactions without PostgreSQL. Existing `pending_replay_of_local_unlink_hides_echoed_rename_destination` is a known red test from the earlier investigation and is outside this change.

## Plan of Work and Milestones

First add a regression alongside recent-flush tests, creating a regular file, making it uploading, renaming it, projecting and marking the selected journal range published, then applying its own batch and recent-flush reconciliation. Assert the destination remains resolvable and can be unlinked. This milestone establishes which reconciliation first damages serving state, without timing sleeps.

Next change only the responsible shared reconciliation path. Compare the normal replay candidate/protection behavior against the later pass before deleting any code. Keep newer local names private and retain foreign namespace convergence. No public API, dependency, configuration, migration or platform adapter change is intended.

Finally run the relevant replay and backlog modules and crate checks, inspect the diff, and update this plan with exact results. Mounted validation needs a rebuilt product, configured PostgreSQL/object storage, FUSE, and a registered scenario runner; unit tests alone do not establish that gate.

## Concrete Steps and Validation

Run from `/home/val/dev/studiofs-cli`:

    cargo test -p studiofs-meta --lib hidden_uploading_rename
    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::
    cargo test -p studiofs-meta --lib pg_engine::tests::replication_backlog::
    cargo check -p studiofs-meta
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo fmt -p studiofs-meta -- --check
    git diff --check

The new regression must fail before the fix and pass after. Report the pre-existing delete-before-publication failure explicitly rather than treating it as a new regression or hiding it in a broad success claim. Verify existing foreign-removal and stale-serving reconciliation tests still pass with nonzero counts.

## Idempotence and Recovery

Tests use disposable redb databases. Keep temporary diagnostics under `target/sfs960/`. Preserve the existing commit and avoid destructive checkouts. Retry failed checks after fixing the actual failure; do not add sleeps or broad retry loops.

## Artifacts and Notes

The prior mounted reproducer and diagnostic patches remain in `target/sfs960/`; the old test mount is unmounted. They are investigation artifacts, not evidence for the new binary.

## Interfaces and Dependencies

Reuse `RemoteApplyCoordinator`, `RecentFlush`, `OverlayConflictSets`, `LocalEventRange`, `DecodedReplayBatch` and existing unit helpers. All changes remain inside studiofs-meta plus this plan.

Initial revision: scope option B and record the exact publication/reconciliation boundary before production edits.

Completion revision (2026-11-03): record the actual Ready-publication race, minimal deletion, independent review, exact local/live validation, mounted prerequisite and rejected issue creation.
