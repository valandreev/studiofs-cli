# Preserve replacement entries during namespace repair

This ExecPlan is maintained under `.agent/PLANS.md`. It covers the verified
findings in `docs/code-reviews/2026-09-30-sfs-759-concurrent-namespace-races.md`
and the follow-up review
`docs/code-reviews/2026-10-01-sfs-759-atomic-namespace-repair-review.md`.

## Purpose / Big Picture

Raw dentry repair must remove only the inode binding it diagnosed as broken. A healthy
replacement can arrive after a VFS cache observation, an fsck scan, or startup
enumeration. Passing that earlier inode identity to one atomic metadata
operation prevents repair from deleting the replacement. Ordinary raw removal
by name should return the entry actually deleted by its write transaction.

## Progress

- [x] (2026-10-01) Read the review, current source, contracts, and caller paths.
- [x] (2026-10-01) Confirm the longer repair race is outside the original internal comparison.
- [x] (2026-10-01) Observe failing real-engine regressions for fsck and VFS stale observations.
- [x] (2026-10-01) Add the atomic conditional metadata API and forward it through existing wrappers.
- [x] (2026-10-01) Update repair callers and preserve replacement visibility in VFS caches.
- [x] (2026-10-01) Move the test marker to test support and own race-test threads on failure.
- [x] (2026-10-01) Run focused tests, affected-package check/Clippy, formatting, and diff checks.
- [x] (2026-10-01) Verify all six follow-up claims against committed HEAD `10f91bff` and current caller contracts.
- [x] (2026-10-01) Observe RED and fix first-lookup replacement resolution and retained-directory counting.
- [x] (2026-10-01) Observe RED for skipped-heal stale state and add a live-inode preservation guard.
- [x] (2026-10-01) Finish follow-up focused tests, check/Clippy, formatting, and final diff review.
- [x] (2026-10-01) Locate PR 731 checkout, verify clean HEAD `5da0de1e` matches GitHub, and read all four attached unresolved threads.
- [x] (2026-10-01) Confirm metric undercount RED and explicitly abort all three ordinary skipped namespace mutations.
- [x] (2026-10-01) Scope the negative-cache rule to ordinary healing and preserve the hidden platform-root policy.
- [x] (2026-10-01) Reproduce spurious rmdir ENOTEMPTY and repair it through the existing bounded settle window; the exact regression is GREEN.
- [x] (2026-10-01) Preserve orphan-directory context at both fsck readdir sites; the diagnostic regression and all six fsck tests pass.
- [x] (2026-10-01) Run PR repair validation, inspect the final diff, and prepare the verified changes for delivery to PR 731.
- [x] (2026-10-01) Verify the next three live PR threads against clean head `64628f05`; reproduce repeated skipped-heal waiting and armed-hook leakage during unwinding.
- [x] (2026-10-01) Allow one immediate skipped-heal recheck, add stable-live-child latency coverage, and scope the snapshot hook with a test-only Drop guard.
- [x] (2026-10-01) Finish focused follow-up validation and prepare the next PR repair commit.

## Surprises & Discoveries

The original fix compared two reads inside `remove_entry`, while actual repair
callers already hold an older inode identity. All seven VFS healing call sites
supply an inode hint, including a cached positive lookup that has no file type.
Fsck's dangling report stores the inode, and startup cleanup enumerates entries
for one known inode. An inode expectation therefore fits every real caller.

The reviewed `expect` is compiled only under `cfg(test)` and its marker is
thread-local. It cannot panic in a production binary or contaminate another
test thread's marker. Receiver disconnection and unwinding thread ownership
are still worth correcting within test support.

Fsck can skip a child because its binding changed or because it has a live
hardlink. Removing the orphan parent afterward would leave the retained child
under a missing directory inode. A focused hardlink regression reproduced that
existing parent-cleanup defect; retain a populated parent when child deletion
was skipped. This also handles an existing `DirNotEmpty` result from recursive
directory repair.

The separate fsck directory-child branch uses name-based `MetaFsOps::rmdir`
after enumeration. Its scanner expectation race predates this change and is
outside SFS-759's raw `MetaNamespace` scope. This plan does not claim that all
recursive fsck repair is safe against concurrent writers.

## Decision Log

Use a required `MetaNamespace::remove_entry_if_matches(parent, name,
expected_ino) -> MetaResult<Option<DirEntryValue>>`. `Some` reports the matched
entry removed; `None` reports a skipped absent or changed binding. This result
does not claim that the slot is empty and needs no new error variant. All
checks, deletion, and event append share one write transaction. No default
read-then-delete implementation is permitted on the trait.

Keep `remove_entry` as an atomic removal by current name, using one lookup and
returning the actual deleted value. Repair uses the new conditional operation.
The old internal stale-read regression must be replaced by coverage of the
explicit expected-inode contract and actual returned raw-removal identity.

Reuse one small transaction helper for the two removal modes. Preserve base,
overlay, tombstone, visibility, event, and invalidation behavior. Do not change
the safe mounted `NamespaceDeleteTxn` mutation path or remote publication.

## Outcomes & Retrospective

All repair callers of raw namespace removal now pass their diagnosed inode.
Ordinary raw removal uses one write lookup and returns the current entry.
Skipped VFS healing evicts the stale dentry observation without negative caching
or clearing another inode's state. Test-only synchronization uses scoped threads
and tolerates a disconnected receiver. No dependency was added.

The first review's focused tests, affected-package check/Clippy, and hygiene
checks passed. The user subsequently committed that implementation as
`10f91bff`. The new review started with a clean tree on that commit. Follow-up
changes passed final verification and were later committed by the user as
`5da0de1e`. Existing commits and supplied review documents are preserved.

The user committed the second review changes as `5da0de1e` and now requests
repair of PR 731's four attached outstanding comments. The selected checkout
and live PR head match that commit. All four PR comments are confirmed and
addressed by the narrow changes described below. Current validation passed;
earlier review validation above is historical evidence. Publish the verified
repair commit to the existing PR branch and record thread-delivery evidence
under `target/` without changing the earlier commits.

## Context and Orientation

`crates/studiofs-meta/src/engine.rs` defines the metadata capability. The redb
implementation is `redb_engine/namespace.rs`; PostgreSQL and read-only wrappers
are `pg_engine/local_traits.rs` and `read_only.rs`. The VFS runtime wrapper in
`crates/studiofs-vfs/src/runtime.rs` forwards the capability. VFS repair lives
in `vfs/namespace_ops.rs` and `vfs/startup_replay.rs`; CLI scan repair lives in
`crates/studiofs/src/cli/fsck.rs`.

The expected inode is the identity observed before diagnosis. A write
transaction serializes namespace checks and mutation. A skipped operation
must preserve the replacement and append no deletion event.

## Plan of Work

First add real-engine tests that replace the diagnosed name before repair and
watch them fail on HEAD `10f71a9e`. Implement the capability and existing
wrappers, then update each actual repair caller. Check VFS cache visibility:
a skipped repair cannot insert a negative cache entry for a live replacement.
Finally isolate the transaction-attempt marker in test support and use scoped
threads with the held transaction inside the scope closure so unwinding drops
the lock before automatic thread joining.

## Concrete Steps

Work from `/Volumes/Storage/dev/studiofs`. Use `RUSTFLAGS='-D warnings'` for
focused tests and compile checks. Run the fsck regression with
`cargo test -p studiofs --bin studiofs repair_preserves_replacement_for_stale_dangling_report`.
Run the VFS regression under
`cargo test -p studiofs-vfs --lib stale_positive_lookup_does_not_delete_or_hide_replacement_entry`.
Then run local redb tests, focused fsck/VFS repair tests, and
`cargo check -p studiofs-meta -p studiofs-vfs -p studiofs` followed by
`cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings`.
Finish with `cargo fmt --all -- --check` and `git diff --check`.

## Validation and Acceptance

The old diagnosed inode must never authorize deletion of a new inode at the
same name, whether replacement occurs before repair starts or while repair
waits for the writer. The replacement remains resolvable through VFS caches.
Missing and changed conditional deletions return `None` with no event;
matching removal returns the actual removed entry and preserves base tombstones.
Raw removal returns the current entry from its write transaction. Test-marker
receiver cancellation does not panic. Every reported test filter must execute
nonzero tests. Live PostgreSQL and mounted drivers are separate coverage and
will not be implied by local tests.

## Idempotence and Recovery

Fixtures use isolated directories under `target/`. Test and check commands can
be rerun safely. Preserve the user's existing committed work and review
document. Do not reset history or remove unrelated changes.

## Artifacts and Notes

Retain red and green command output under `target/sfs-759-*.log`. Record exact
results and the disposition of each review claim here at completion.

### Review disposition

1. **Incomplete repair race protection: confirmed and fixed.** Real fsck scan
   and stale positive VFS cache regressions deleted the replacement on HEAD.
   Caller-supplied inode expectations close the observed gap. Metadata coverage
   also checks replacement while conditional removal waits for the writer.
2. **Duplicate merged lookup: confirmed and removed.** Both removal modes use
   a single merged lookup in the write transaction. The claimed HIGH performance
   severity is unmeasured; raw repair removal is not the mounted `unlink` path.
3. **Misleading missing-entry result: confirmed and fixed.** Conditional
   `None` means absent or changed; fsck reports "changed or gone". No new error
   variant is needed. Raw removal reports `EntryNotFound` only for absence.
4. **Cow allocation: confirmed and fixed.** `into_owned()` reuses an owned
   lossy conversion on the existing insertion error path.
5. **Test hook panic/coupling: partly confirmed and fixed.** Dropping the
   receiver reproduced a test-only panic; the send is now best effort and the
   marker lives in `test_support`. The production panic and cross-thread marker
   contamination claims are disproven by `cfg(test)` and thread-local storage.
   Test-hook overhead was not measured and does not justify more machinery.
6. **Detached race-test worker on panic: confirmed and fixed.** Scoped threads
   own workers; the held writer transaction is inside the scope closure so it
   drops on unwind before automatic joining.

### Validation evidence

All following Cargo tests/checks use `RUSTFLAGS='-D warnings'`.

| Command after `cargo` | Result | Retained log under `target/` |
| --- | --- | --- |
| `test -p studiofs-meta --lib redb_engine::tests` | 307 passed, 1 ignored | `sfs-759-review-redb-tests.log` |
| `test -p studiofs-meta --lib read_only_meta_rejects_conditional_namespace_removal` | 1 passed | `sfs-759-read-only.log` |
| `test -p studiofs --bin studiofs cli::fsck::tests` | 5 passed | `sfs-759-fsck-green.log` |
| `test -p studiofs-vfs --lib stale_positive_lookup_does_not_delete_or_hide_replacement_entry` | 1 passed | `sfs-759-vfs-green.log` |
| `test -p studiofs-vfs --lib dangling` | 5 passed | `sfs-759-vfs-dangling.log` |
| `test -p studiofs-vfs --lib startup_replay` | 31 passed | `sfs-759-vfs-startup-replay.log` |
| `check -p studiofs-meta -p studiofs-vfs -p studiofs` | passed | `sfs-759-review-check.log` |
| `clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings` | passed | `sfs-759-review-clippy.log` |

`cargo fmt --all -- --check` and `git diff --check` passed. Final read-only
review found no introduced regression. Clippy's first attempt found one needless
borrow in the new read-only fixture; the fixture was corrected and the complete
affected-package Clippy command passed on retry.

RED was observed for stale fsck repair, stale VFS healing, raw returned-entry
identity, dropped test receiver, and retained-parent cleanup before their fixes.
Live PostgreSQL/service and native mounted tests were not run; the local results
do not imply that coverage.

### Follow-up review decisions and milestones

For finding 1, a stale positive cache observation cannot justify ENOENT for an
operation that began after a healthy replacement existed. The strengthened
`stale_positive_lookup_does_not_delete_or_hide_replacement_entry` test failed
on the first lookup with `EntryNotFound`, then passed after removing that cached
branch's immediate return. It now reaches the existing local metadata lookup,
including its access checks and observation-token cache admission.

Finding 2 correctly observes that a changed child may be deferred during this
repair invocation. It does not establish a permanent leak. `run` rescans and
reports remaining issues for a fresh repair pass. Removing the processed marker
would authorize destructive cleanup from the original orphan report after the
child binding changed; that report's link count may now be stale. Retain the
conservative skip. Concurrent online orphan reclamation would require fresh,
atomic reachability validation rather than deleting this marker.

Finding 3's missing-parent error is not produced by the current backends:
Redb `readdir` enumerates dentry tables without validating the parent attribute;
Pg `readdir` forwards to it. The CLI opens its local database exclusively with
`background_poll: false`, completes PostgreSQL sync before scanning, and invokes
repair synchronously. A concurrent writer during repair is outside that CLI
lifecycle. If repair were exposed online, raw `remove_attr` would indeed lack an
atomic empty-directory guard; the proposed extra `readdir` would leave the same
check/delete gap. Do not introduce a partial guard or a speculative new public
capability for unsupported online fsck. The previously recorded recursive
directory-name race also remains outside this raw dentry repair scope.

For finding 4, retain the existing summary field but feed it an actual count.
`repair_orphan_directory` now receives the counter through its existing callers
and increments it when retained children keep a directory alive. The real-engine
hardlink fixture failed with count 0 versus expected 1 and passed after this
increment. It still exercises the full repair and checks preserved bindings.

Finding 5 is confirmed for callers that diagnose missing attributes directly;
the cached-getattr path already clears stale state in `ReadMetadataResolver`.
The private healing regression failed because dirty state for the missing old
inode survived a conditional miss. Clear only `expected_ino` after a successful
matched removal, or after direct metadata confirms it absent on a skipped or
failed removal. Preserve the replacement's state and a still-live expected inode.
This fresh check matters because access enforcement can synthesize
`InodeNotFound` for `AccessLevel::NoAccess`; the review's unconditional cleanup
would erase valid local state. A second real-engine fixture covers a live inode
renamed to another binding and requires its state to survive.

Finding 6 is cosmetic. `DirEntryValue` contains two scalar fields; cloning it
does not allocate. Adding a public `Copy` contract has no demonstrated behavior
or performance benefit for this issue, so leave the type unchanged.

From `/Volumes/Storage/dev/studiofs`, rerun focused follow-up tests with
`RUSTFLAGS='-D warnings'`: `cargo test -p studiofs-vfs --lib
vfs::namespace_ops::tests::`, `cargo test -p studiofs-vfs --lib
stale_positive_lookup_does_not_delete_or_hide_replacement_entry`,
`cargo test -p studiofs-vfs --lib dangling`, `cargo test -p studiofs-vfs --lib
getattr_unchecked`, and `cargo test -p studiofs --bin studiofs cli::fsck::tests`.
Check and lint the changed packages with `cargo check -p studiofs-vfs -p studiofs`
and `cargo clippy -p studiofs-vfs -p studiofs --all-targets -- -D warnings`, then
run `cargo fmt --all -- --check` and `git diff --check`. Focused command evidence
is retained as `target/sfs-759-followup-*.log`.

Final follow-up results: the healing module ran 2 tests, first-lookup regression
1, dangling namespace filter 5, getattr filter 3, and fsck module 5: 16 distinct
focused tests passed. Check and all-targets Clippy for `studiofs-vfs` and
`studiofs` passed with warnings denied. Formatting and diff checks passed.
Independent final reviews found no introduced regression and confirmed the
current-backend and CLI-lifecycle evidence for finding 3. The initial fsck RED
command used the old name after the test was renamed and selected zero tests;
it was discarded, and the exact renamed test produced the recorded count
failure before the increment. Service-backed PostgreSQL and mounted E2E gates
were not run for this follow-up; no native or remote proof is inferred.

### PR 731 repair decisions and milestones

The selected repository is `github.com/valandreev/studiofs-cli`, PR 731,
`feature/sfs-759-make-raw-redb-namespace-add-and-remove-atomic-with-their` into
`main`. Live GitHub review-thread inspection found the four attached comments
unresolved and current. GitHub CLI PR commands explicitly use `--repo`; API
commands, which do not accept that flag, use explicit owner/repository endpoints
or GraphQL variables. Preserve existing commits and stage only this task's files.

Comment 1 is confirmed: dropping a skipped `RedbWriteTransaction` releases its
guards but does not record mutation latency. Use the existing explicit abort
convention for absent conditional removal, mismatching conditional removal,
and duplicate insertion. The real-engine metric regression failed with zero
mutation samples versus one; after explicit aborts all three assertions pass.
Do not introduce a global Drop accounting change or alter commit metrics.

Comment 2 describes a reachable mounted VFS boundary. A dangling-child snapshot
can become absent before the conditional heal, after the failed emptiness
transaction released its namespace barrier. Preserve matched-removal accounting
but let observed dead children request the existing bounded retry/settle path.
Use deterministic test synchronization at that snapshot boundary, real redb
mutations, and no scheduling sleep as race proof. A stable live replacement
must still cause ENOTEMPTY, and failed healing must not introduce an unbounded
retry loop.

The regression removes the snapshotted binding and inode before the attribute
batch using a one-shot test-only hook. It failed with `DirNotEmpty(2)` and now
passes. The healing result reports observed missing children separately from
actual removals; that observation enables the existing settle branch, whose
80-attempt cap remains unchanged. A later live replacement observation again
takes the immediate ENOTEMPTY branch.

Comment 3 concerns storage-error diagnostics, unlike the earlier disproven
claim that current backends return parent `InodeNotFound` from `readdir`.
Use one small mapper at both orphan-directory readdir sites so actual errors
include the directory inode and an absent-directory result remains benign.
Exercise the pure error conversion directly; do not invent a parent-missing
behavior in the real backend or build a whole metadata mock.

The diagnostic test failed with a raw `FsckError::Meta(Storage(...))` before the
mapper supplied `readdir failed for orphan dir 42` context. It now passes along
with the existing five fsck tests. Missing-directory conversion is also covered
directly without claiming a current backend produces it.

Comment 4 is confirmed documentation overreach. Ordinary skipped healing
invalidates cached dentry observations. The root platform-trash policy hides
those names and intentionally inserts a negative entry even if no repair occurs.
Document that exception without changing the product's hidden-root behavior.

Run each focused regression with `RUSTFLAGS='-D warnings'`, followed by local
redb namespace tests, affected VFS rmdir/healing tests and fsck tests. Finish with
`cargo check -p studiofs-meta -p studiofs-vfs -p studiofs`,
`cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings`,
`cargo fmt --all -- --check`, and `git diff --check`. Retain evidence under
`target/sfs-759-pr731-*.log`. After all checks pass, update the PR branch and
reply in the four original inline threads with concrete behavior and validation.

First PR repair validation, all with `RUSTFLAGS='-D warnings'` for Cargo
test/check/Clippy commands:

- `cargo test -p studiofs-meta --lib redb_engine::tests`: 308 passed, 1 ignored.
- `cargo test -p studiofs-vfs --lib rmdir`: 8 passed, including the new race regression.
- `cargo test -p studiofs-vfs --lib dangling`: 5 passed, including hidden root-trash policy; one test also appears in the rmdir filter.
- `cargo test -p studiofs --bin studiofs cli::fsck::tests`: 6 passed.
- The metric and rmdir regressions each ran alone and passed after their recorded RED failures.
- Affected-package `cargo check` and all-targets Clippy with `-D warnings` passed.
- `cargo fmt --all -- --check` and `git diff --check` passed.

Independent final reviews found no actionable issue in the metadata, VFS, or
fsck changes. GitHub still reported the selected baseline head `5da0de1e`
before publication. The worktree contains only the six intended task files.
No PostgreSQL service-backed, native mount, or Windows gate was run for this
PR repair; local test and compile evidence does not imply those gates passed.

### Additional PR 731 review: skipped-heal latency and hook lifetime

The first repair was published as `64628f05`, and all four original threads
were answered and resolved. The next request attaches three current threads;
the clean checkout and live PR head both match `64628f05`.

Repeated skipped healing could consume all 80 timer waits even without an
observed in-flight delete. Keep the existing settle window for observed delete
operations, but allow only one immediate metadata recheck for an observed
missing child when no delete was observed. A second unsuccessful heal returns
ENOTEMPTY without sleeping. Reuse the existing attempt counter; no new timer,
configuration, or dependency is required.

The real-redb regression repeatedly changes a dangling binding after each
snapshot so conditional removal always skips. Before the fix the first future
poll returned Pending at the timer; afterward it returns ENOTEMPTY after exactly
two snapshots. This proves the shared no-progress retry boundary, which also
handles a healing storage error; it does not inject a backend storage error.
The previous disappearing-child regression continues to prove successful
removal after the immediate recheck.

A live-child regression polls rmdir once under its runtime and requires a ready
ENOTEMPTY result with the child retained. This asserts no async wait directly,
without an elapsed-time threshold. Current behavior already satisfies that
contract; this review adds missing coverage rather than changes live-child
semantics.

The live-child test also failed under an intentional mutation that removed the
first-attempt return guard. That mutation was restored before final validation;
this is test-sensitivity evidence, not a claim that live-child behavior was
already broken on the selected head.

Use a small test-only guard for snapshot-hook ownership. Its Drop clears any
unconsumed or rearmed hook. A catch_unwind regression failed before that Drop
implementation and passes afterward. This verifies cleanup on the same thread
directly, without depending on a claim that libtest reuses worker threads under
`--test-threads=1`.

Run `RUSTFLAGS='-D warnings' cargo test -p studiofs-vfs --lib
vfs::namespace_ops::tests:: -- --test-threads=1` and
`RUSTFLAGS='-D warnings' cargo test -p studiofs-vfs --lib rmdir`, followed by
`RUSTFLAGS='-D warnings' cargo check -p studiofs-vfs` and
`RUSTFLAGS='-D warnings' cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
Finish with formatting and diff checks. Retain evidence as
`target/sfs-759-pr731-*.log` and reply in the three original threads after the
verified commit is published to the existing branch.

Final follow-up results: the namespace-ops module passed 6 tests with
`--test-threads=1`, and the rmdir filter passed 10 tests; three tests appear in
both commands, so these are 13 distinct passing tests. VFS package check and
all-targets Clippy passed with warnings denied. Formatting and diff checks
passed. Final source inspection confirms the fast-return guard is present and
the only production change is the single immediate recheck for skipped healing.
Only namespace_ops.rs and this living plan changed. PostgreSQL, mounted E2E,
and Windows gates were not run; no native acceptance is inferred.

## Interfaces and Dependencies

No dependency is added. The required capability is implemented by RedbMetaEngine,
PgMetaEngine, ReadOnlyMetaEngine, and RuntimeMetaHandle. The existing merged
write lookup remains the authority for namespace visibility. Standard-library
channels and scoped threads provide deterministic, owned test synchronization.

Plan created after verification showed that the actual repair observation
predates the original method-local comparison.

Plan extended on 2026-10-01 for the second review: retain same-call replacement
visibility, count skipped directories, and reclaim confirmed stale VFS state
without discarding live inode state. Scope decisions avoid treating the external
review's unsupported online-fsck assumptions as new product requirements.

Plan extended for PR 731 on 2026-10-01 to track the four live attached threads,
their targeted regression evidence, and delivery of the reviewed fixes.
