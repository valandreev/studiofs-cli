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
- [x] (2026-11-03) Traced the remaining mounted ENOENT to destructive own-rename reconciliation and reproduced hidden-interior self-ack loss deterministically.
- [x] (2026-11-03) Corrected hidden local namespace authority through self-ack and both reconciliation passes; the rebuilt product passed the original generic/241 case.

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

## Remaining ENOENT root-cause investigation (2026-11-03)

The user requested finding the remaining cause. Started from clean `ae51b80c` on the same task branch. Temporarily instrumented exact NEWPCB.PPT/NEWTIPS.PPT serving/base writes with the previous inode binding and call stack, rebuilt the release product, and reran only pinned generic/241 on fresh volumes. The diagnostic run reproduced the same line-4114 unlink ENOENT after 10 seconds of measured dbench execution. Evidence is under `target/xfstests/sfs960-diag-20261103T171618Z/`.

The trace identifies the actual destructive call: `apply_visible_self_batch_in_txn` -> `reconcile_own_serving_renames_from_base_in_txn` -> `reconcile_serving_rename_from_base_in_txn` -> `remove_dentry_mirror_in_txn`. At 17:16:37.519990Z it removed NEWPCB.PPT under parent 549755813948 while serving bound it to newly created inode 549755814178 and base had no source binding. Three more such removals occurred under parents 549755813944, 549755813954 and 549755813950. NEWPCB.PPT had been recreated with those new inode bindings between 17:16:34.970107Z and 17:16:35.176232Z. No source-name restoration followed. The later post-pull removals found those names already absent. Thus the retained post-pull pass is not the first destructive writer in this run. The four exact destructive stacks are extracted into `target/sfs960/newpcb-removal-evidence.log`.

The deterministic test `self_ack_preserves_recreated_draft_hidden_inside_published_batch` models a genuine publication window: create/publish old file, rename it away, recreate its source as a new empty draft, then append another visible namespace operation. Canonical projection hides the new draft, while `matching_local_prefix` confirms the entire journal window matches the projected batch. The test records that exact published prefix and uses `PgMetaEngine::apply_replay_page_atomically`. It fails on current production code when looking up the recreated NEWPCB.PPT after self replay, without any concurrency or sleeps. This explains why the earlier `self_replay_of_rename_away_keeps_recreated_source_and_unlinked_destination` test passed: it placed recreation in the unacknowledged suffix, which pending namespace rebase can restore. A hidden create inside the acknowledged prefix is no longer in that suffix.

Root cause: an own rename unconditionally removes the source even when a newer local inode now owns it; self acknowledgement consumes hidden local-only journal events together with remotely represented events, and subsequent pending replay has no remaining event to restore that newer source binding. The absence of the recreated file in base is intentional until its first publication and must not authorize removal of the local name. This is an observed ENOENT mechanism, so the earlier classification of unconditional own-rename cleanup as only an unobserved hazard is superseded.

An additional experiment completed a hidden-create range before replay: transactional replay retained the name, but post-pull reconciliation then removed it. That red fixture is saved only in `target/sfs960/root-cause-regressions.patch`; it is not the production path demonstrated by the mounted run and is not included in the task diff. It demonstrates why changing just the unconditional remove would be insufficient without preserving local namespace authority through final and post-pull reconciliation.

The earlier snapshot-race hypothesis also needs qualification: ordinary `PgMetaEngine::mutate_with_hint` holds `pending_state` while committing local writes, and `pull_and_reconcile_foreign_events_locked` holds the same mutex across its protection snapshot and reconciliation. The earlier Ready-after-snapshot unit fixture invokes the coordinator directly and therefore does not prove that scheduling is reachable through those ordinary engine methods. No production correction was inferred from that schedule in this investigation.

The diagnostic source edits were reversed exactly from `target/sfs960/diag-newpcb.patch`; production source is unchanged. The diagnostic binary is retained at `target/sfs960/studiofs-diag-newpcb`. `target/release/studiofs` was restored from the preserved clean 06fd7355 product and its SHA-256 matches the prior non-diagnostic artifact. Host checks confirmed zero remaining diagnostic mounts and both recorded mount daemon PIDs exited. The sole new task test intentionally remains red as root-cause evidence; a fix and mounted green result are still outstanding.

Investigation revision: replace the unproven post-pull-race explanation with the observed own-rename deletion stack and the faithful hidden-interior publication regression, retaining the exact evidence and separating experiments from the demonstrated mounted path.

## Approved implementation: preserve hidden local namespace authority

On 2026-11-03 the user selected approach 2: preserve hidden local name ownership until publication, rather than adding separate deletion guards. Extend the existing local-binding snapshot/rebase in `remote_apply.rs` with currently served regular-file names whose durable lifecycle and replicated base still identify them as unpublished. Discover only keys touched by the replay page, using the existing candidate collector and by-inode indexes; do not scan every inode or add a table. Carry these bindings as dentry conflicts across self acknowledgement and rebase them using the existing reducer output. The post-pull dentry pass must consult the same unpublished ownership predicate. Stop preserving a binding when its file is actually publishable in replicated base, so later foreign unlink/replace still converges, including a later batch in the same page. No new dependencies, public APIs, wire formats or platform changes are intended.

Milestone 1 is the existing red hidden-interior regression (rerun and confirmed). Milestone 2 adds page-boundary/publication-release regressions, implements shared protection, and checks both successful local unlink and later foreign deletion. Milestone 3 runs focused replay/backlog/layout suites, check, Clippy and formatting, obtains read-only review, rebuilds release and reruns generic/241 outside sandbox on fresh volumes. Preserve the separate SFS-963 failure and report it explicitly if it still occurs; do not treat EEXIST/EIO as an ENOENT pass.

- [x] (2026-11-03) Implemented shared durable-lifecycle namespace ownership, captured before each raw/canonicalized batch, reusing the existing local-binding map/rebase; post-pull uses the same predicate.
- [x] (2026-11-03) Root regression and boundary regression pass: protection survives self-ack/post-pull/next foreign page and ends after publication before same-page foreign unlink. Alias regression failed before canonical capture and passes for existing and same-page aliases.
- [x] (2026-11-03) Completed focused checks and read-only review (canonical-parent finding repaired red/green); rebuilt product passed registered smoke and generic/241 on fresh volumes, with cleanup verified.

Implementation-plan revision: record the user's selected approach and its publication/foreign-deletion boundary before editing production code.

## Implementation validation before mounted rerun (2026-11-03)

The production predicate reads the existing durable inode lifecycle and identifies regular-file namespace ownership by `!has_published_base`, excluding DeletedBeforePublish. Before each batch can mutate a name, the existing raw and canonicalized key collector identifies its effective keys; current served unpublished bindings join the existing completed-binding protection map. Self-ack cannot discard that snapshot. The existing final namespace rebase restores its exact bindings. Later pages reconstruct ownership from redb lifecycle rather than retaining process-local state. The post-pull pass consults the same predicate. The pending-state mutex prevents local publication from changing lifecycle midway through the replay page; files already published locally use ordinary event/suffix protection and do not gain this unpublished-name protection.

TDD evidence: reran the committed hidden-interior regression red before production edits, then green. The new `acknowledged_unpublished_namespace_stays_local_until_publication` failed at post-pull lookup with only the snapshot implementation; with shared post-pull protection it passes through subsequent foreign replacement, publication and same-page foreign unlink. The old deferred-base-entry regression now uses an actually published file: manually advancing the cursor over an empty private draft never establishes namespace publication.

Read-only review found one concrete issue: collecting only raw keys misses a canonical parent after directory-alias rewriting. `unpublished_name_protection_uses_canonical_parents` reproduced the replacement of a local draft by a foreign directory. The capture now occurs per batch after alias resolution, using both raw and canonical keys and the existing collector; this also removes the duplicate whole-page prepass. Both existing-alias and alias-created-earlier-in-page variants pass.

Current focused results: replay module 148 passed, one unchanged pre-existing SFS-963 failure (`pending_replay_of_local_unlink_hides_echoed_rename_destination`), 45 ignored; backlog module 79 passed, six ignored; layout three passed. Cargo check and Clippy --all-targets -D warnings passed. Two focused live PostgreSQL tests passed outside the sandbox: reopening an unpublished local create and nested temp rename/publication on a peer. Formatting and diff checks passed. The full workspace and full ignored suites were not run. Release rebuild and fresh mounted verification remain in progress; the preceding failed 06fd7355 run is not superseded until those results are recorded.

Implementation revision: record common ownership semantics, red/green evidence, corrected fixture, canonical-parent review repair, and exact focused validation before the mounted acceptance check.

## Final mounted acceptance (2026-11-03)

Implementation commit `40c3002c8f19206a9499d00a09183fbfb1cdbbc1` preserves hidden local namespace authority using the existing durable lifecycle and binding/rebase mechanisms. The final release product was rebuilt after the canonical-parent repair with `cargo build -p studiofs --release -j 8`; binary SHA-256 is `6ae9b830e2ee2ba5437900a3db0fb02bb21dea8ae89f3d81444d1ae4d349d2c0`.

Registered `basic-multi-client-smoke` passed, including mounted-tree validation and cleanup:

    target/debug/studiofs-e2e run basic-multi-client-smoke --infra env --studiofs-bin target/release/studiofs --run-id sfs960-40c3002c-smoke --seed 960 --retain always --timeout 180s

Report: `target/e2e-runs/sfs960-40c3002c-smoke/report.json`.

The original pinned generic/241 was run outside sandbox on two fresh dedicated volumes through `tools/xfstests/run.sh`. It completed the full `dbench -t 60 ... 4` workload and passed, exit 0 (harness wall time 83 seconds). The check log reports `Passed all 1 tests`. This supersedes the earlier mounted ENOENT failures on 06fd7355 and its diagnostic build, without claiming a full generic-suite pass.

Artifacts: `target/xfstests/sfs960-authority-20261103T173821Z/generic241/manifest.tsv`, `metadata.tsv`, `cases/241/check.log`, `cases/241/results/generic/241.full`, `cases/241/results/result.xml`, and parent `mounts/` logs. The metadata records exact product hash, upstream xfstests revision `22348afe338c0f6d540c0d7ef0db749eaa51b218`, adaptive Linux cache policy and mmap opt-in disabled. Host checks found zero remaining run mounts and both recorded mount daemon PIDs exited. The upstream scratch-unmount message in the full log did not affect the case verdict; cleanup was checked independently.

Final local evidence remains: replay module 148 passed, one unchanged SFS-963 failure, 45 ignored; backlog 79 passed, six ignored; layout three passed; focused `unpublished_` selection 17 passed, three ignored; cargo check, Clippy --all-targets -D warnings, formatting and diff checks passed. Live PostgreSQL reopen-before-publication and nested rename/publication tests passed. The full workspace, full ignored suite and full generic suite were not run. SFS-963 (`pending_replay_of_local_unlink_hides_echoed_rename_destination`) remains outside the selected ENOENT correction; one mounted pass does not establish that separate bug is fixed.

Outcome: the selected approach is implemented and the original mounted acceptance case is now green. No new persisted table, configuration, public API or event format was introduced. No PR or external issue status change was made as part of this local delivery.

Completion revision: record exact final release/mounted evidence, independently verified cleanup and remaining SFS-963 limitation; close the implementation milestones without overstating suite coverage.

## SFS-963 integration into SFS-960 (2026-11-03)

The user requested importing the completed SFS-963 correction and verifying both fixes together. Started at clean SFS-963 head `15da9845` in `/home/val/dev/studiofs-cli`; fetched both remote branches and verified SFS-960 `e8c3d66c` is an ancestor. Switched to the requested SFS-960 branch and fast-forwarded it to SFS-963, retaining all original commits: `d4790195` (explicit local unlink intent and deletion authority), `91e92455` (mounted evidence), and `15da9845` (allow a matching peer inode removal over a local DeletedBeforePublish marker). There were no conflicts or implementation edits in this integration.

The combined behavior must preserve a recreated unpublished source through self-ack (SFS-960), keep a locally unlinked destination hidden through echo/ack/pruning (SFS-963), retain the old published peer payload when canceling a new version (SFS-234), and still apply genuine foreign removals and replacements. The existing replay regression set covers these paths together. The new local-only RemoveLocalEntry journal event uses tag 18 and is not projected to PostgreSQL; older binaries cannot reopen local journals containing this tag.

- [x] Imported all three SFS-963 commits into SFS-960 without rewriting either history.
- [x] Combined replay module: 150 passed, zero failed, 45 infrastructure tests ignored; previously red SFS-963 test is green.
- [x] Redb module: 391 passed, zero failed, one ignored. Event codec: 11 passed. Layout: three passed. Cargo check and Clippy --all-targets -D warnings passed.
- [x] (2026-11-03) Backlog 79 passed; live peer-unlink test passed. Rebuilt integrated release product and passed fresh mounted smoke plus generic/241 outside sandbox.
- [x] (2026-11-03) Recorded combined validation and updated PR #939 to 80fbe177; hosted CI is monitored separately.

Integration revision: preserve the source history and define paired validation, superseding the earlier outstanding-SFS-963 limitation once the combined checks complete.

### Combined acceptance results

On combined implementation head `15da984575a7ee76bc9b458445bed2866acd8bef`, replay passed 150/0 failed/45 ignored, redb 391/0 failed/1 ignored, backlog 79/0 failed/6 ignored, events 11/0 failed, and layout 3/0 failed. The formerly CI-failing `pending_replay_of_local_unlink_hides_echoed_rename_destination` and SFS-960 hidden-interior/release/alias regressions are all included in that green replay module. The targeted live PostgreSQL test `test_pg_peer_can_unlink_foreign_visible_uploading_entry` passed one test outside sandbox. Cargo check, Clippy --all-targets -D warnings, formatting and diff checks passed. Local logs are under `target/sfs960/integrated-15da9845/`.

The release rebuild succeeded (3m48s), yielding binary SHA-256 `cadb42fb95b7e947215d126c35108a5e4e5403f47bb30be5243781f8c2f96f73`. The registered mounted smoke passed both validation and cleanup, report `target/e2e-runs/sfs960-963-15da9845-smoke/report.json`. The original generic/241 then passed the full 60-second four-client dbench workload, exit 0, harness wall time 78 seconds. Artifacts are under `target/xfstests/sfs960-963-pair-20261103T185032Z/generic241/`, including manifest.tsv, metadata.tsv, cases/241/check.log, results/generic/241.full and results/result.xml. Host inspection confirmed zero remaining test mounts and both recorded mount daemon PIDs exited.

This combined verification supersedes the earlier known-red SFS-963 limitation for SFS-960. No new implementation edits were necessary: the SFS-963 source branch was already based on the SFS-960 authority correction. The full workspace, full ignored suite, full generic suite, and native Windows/macOS mounts were not run locally. Ubuntu/Rocky hosted CI results remain separate from the local evidence until the updated PR head is checked.

Integration acceptance revision: record the exact integrated code/binary, green joint regression set, original mounted workload and independently verified cleanup. Keep the local-journal tag-18 downgrade restriction visible.

## Ubuntu CI test-fixture follow-up (2026-11-03)

The hosted Ubuntu workspace job on `80fbe177` failed only `vfs::tests::test_release_fh_reports_sticky_error_after_persisting_current_changes`: `release_fh` returned Ok while the test unconditionally expected a sticky error. The metadata replay tests were green. The failing test is unchanged in main and uses `make_test_vfs`, which starts background workers. Those workers may drain pending writer state before release selects its action, or successfully recover an upload and clear a manually injected error. The explicit clean-close contract is already covered by `test_release_fh_clean_skip_does_not_reenter_sticky_error_barrier`; changing production to report that error on a clean close would violate that behavior. An isolated local run of the old test passed, consistent with its scheduler-dependent precondition.

The three adjacent unit tests that manually inject and retain sticky errors now use the existing unstarted VFS fixture, so background persistence/recovery cannot consume their test state. The release regression also asserts its actual pending-local-state precondition before injection. This is a test-only correction: production release, metadata replication and publication behavior are unchanged. No sleeps, retries, ignored tests or weakened expected-error assertions were added.

The focused `release_fh` selection passed 20 tests, including both sticky-error reporting and the complementary clean-skip path. The final sticky-error selection passed seven tests; `cargo check -p studiofs-vfs`, `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`, crate formatting and diff checks passed. Retained CI log: `target/sfs960/integrated-15da9845/ubuntu-ci-failed.log`; local focused outputs: `vfs-release.log` and `vfs-sticky-final.log` in that directory. Mounted tests are not repeated for fixture-only edits; earlier integrated production-binary evidence remains separate.

CI-fixture revision: remove competing background actors from tests that assume their manually injected error and dirty state cannot change; preserve the intentionally different clean-close production behavior.
