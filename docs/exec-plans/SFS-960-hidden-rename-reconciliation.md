# Preserve local uploading files during self replay

This ExecPlan follows `.agent/PLANS.md` and remains a living record.

## Purpose / Big Picture

A successful local rename must not lose its destination when this client's PostgreSQL batch returns. A file still uploading can be absent from the remote base (the peer-visible metadata tables), while its local redb name remains authoritative. SFS-960 option B covers that ENOENT only; delete-before-publication resurrection and unconditional rename-source cleanup are separate work.

## Progress

- [x] (2026-11-03) Recorded checkout `/home/val/dev/studiofs-cli`, task branch, clean status, and existing regression commit `07bc399b`.
- [x] (2026-11-03) Read metadata, mounted IO, replay contracts and ADR 0012; traced projection, publication exclusions, replay and recent-flush reconciliation.
- [x] (2026-11-03) Observed the new regression fail with EntryNotFound after the post-pull protection snapshot and local Ready commit, then pass after the correction.
- [x] (2026-11-03) Removed the redundant inode pass; retained dentry reconciliation. Transactional stale-attr regression and two live PostgreSQL publication/unlink tests pass.
- [x] (2026-11-03) Ran focused replay/backlog/layout tests, check, Clippy, formatting and diff review; independent review found no important issues. Initial sandbox-only FUSE check was incomplete; the later host-level mounted rerun below supersedes that limitation.
- [x] (2026-11-03) After explicit payload approval, created separate EEXIST/EIO issue SFS-963.
- [x] (2026-11-03) Rebuilt release product and ran registered mounted smoke plus pinned generic/241 outside sandbox. Smoke passed; generic/241 reproduced the original unlink ENOENT.
- [ ] SFS-960 remains unresolved: trace the remaining mounted ENOENT before proposing another correction.

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

## Mounted verification follow-up (2026-11-03)

The user pointed out that FUSE exists outside the sandbox and requested verification. Host inspection confirmed `/dev/fuse`, fusermount3, dbench, sudo, ext4 and the existing pinned xfstests prerequisites. The earlier absence claim described only the sandbox view and is superseded by this host evidence.

Ran `cargo build -p studiofs --release -j 8` successfully, then:

    target/debug/studiofs-e2e run basic-multi-client-smoke --infra env --studiofs-bin target/release/studiofs --run-id sfs960-06fd7355-smoke --seed 960 --retain always --timeout 180s

The registered smoke passed, including final mounted validation and cleanup. Its report is `target/e2e-runs/sfs960-06fd7355-smoke/report.json`.

On two fresh, dedicated volumes initialized from the configured test services, ran only `generic/241` through `tools/xfstests/run.sh`. The upstream revision was `22348afe338c0f6d540c0d7ef0db749eaa51b218`, with the existing three-line common/rc FUSE subtype patch. No full suite was run. The product was commit `06fd7355fb108542a4e0442f3baf3bf14e1ea7fd`, binary SHA-256 `9761838dafc664880528e7fa5c321ff74f523e72e2a8b043a0dcdf17a8ce5a5b`.

The test failed, exit 1, harness wall time 30 seconds. Dbench reached 13 seconds before:

    [4114] unlink .../clients/client2/~dmtmp/PWRPNT/NEWPCB.PPT failed (No such file or directory) - expected NT_STATUS_OK
    ERROR: child 2 failed at line 4114
    Child failed with status 1

Artifacts: `target/xfstests/sfs960-06fd7355-20261103T170712Z/generic241/manifest.tsv`, `metadata.tsv`, `cases/241/check.log`, `cases/241/results/generic/241.full`, and mount logs in the parent run's `mounts/`. The helper script/config and init logs are retained under target. Host checks after the run found neither test mount active and both recorded daemon PIDs exited.

This reproduces the original SFS-960 symptom, so the passing unit regression and smoke do not establish that 06fd7355 fixes the issue. The retained post-pull dentry reconciliation still samples protection before acquiring its write transaction and only checks local payload overlays for newly encountered bindings. This is a candidate remaining race, not a proven explanation of this particular mounted failure; no additional production patch was made during verification.

The separate EEXIST/EIO issue was created after explicit user approval as [SFS-963](https://linear.app/valandreev/issue/SFS-963/metadata-self-replay-resurrects-a-locally-unlinked-unpublished-rename). The earlier rejection entry above remains historical evidence, not a current blocker.

Verification revision: correct the sandbox-only environment conclusion, record actual mounted red evidence and cleanup, and explicitly reopen SFS-960 rather than claiming the incomplete correction is sufficient.
