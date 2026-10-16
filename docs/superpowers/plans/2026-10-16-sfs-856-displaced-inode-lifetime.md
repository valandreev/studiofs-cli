# SFS-856: Preserve displaced inode references

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

Replacing a filename must preserve the old file's exact accepted bytes through any remaining hard link or open handle. Cleanup becomes eligible only after both names and handles are gone. Public VFS tests exercise buffered writes and an explicit fsync call before replacement; neither case establishes crash or mounted behavior.

## Progress

- [x] (2026-10-16) Read SFS-856, recorded clean main checkout, created feature branch, traced metadata rename and VFS deletion.
- [x] (2026-10-16) Added public VFS regressions: before the fix, two rename cases failed with InodeNotFound/empty bytes; unlink control passed.
- [x] (2026-10-16) Updated atomic rename disposition and reused deferred-delete lifecycle in VFS; handle marking shares the handle-count lock with close.
- [x] (2026-10-16) Focused metadata rename checks passed (42, with 5 service-backed tests ignored); VFS rename (14), orphan cleanup (4), open-unlink (1), and handle routing (1) passed. Crate check, all-target Clippy, formatting, and diff checks passed.
- [x] (2026-10-16) Final SFS-856/no-op regressions passed (4); final VFS rename filter passed (15). Renewed all-target Clippy passed after the final test refinements. Prepared all intended task files and validation evidence for commit, push, and Linear delivery.

## Surprises & Discoveries

Metadata decrements the displaced link count correctly but removes final-link attributes even when VFS handles remain. VFS then unconditionally clears the displaced payload graph and writer state, including for surviving hard links. Unlink already supports a deferred-delete journal and last-handle cleanup.

Sandbox execution could not write upload-stage files during fsync, so validation was repeated with native filesystem access. The expanded hardlink regression exposed a stale nlink attribute cache after cleanup was skipped; rename now explicitly invalidates the displaced attribute cache using the committed disposition.

Dirty attributes also carry link counts, so rename updates only their nlink while preserving accepted local sizes/epochs. Renaming between two hard-link names of the same inode must be a no-op; the atomic result reports whether namespace changed so VFS does not cache a false source removal.

## Decision Log

- Decision: Return displaced post-removal attributes from the atomic transaction and allow its caller to retain the final-link inode for handle-aware cleanup.
  Rationale: The transaction owns the actual displaced identity and link count. VFS owns handles. Existing direct metadata callers keep immediate deletion semantics, while VFS reuses deferred-delete cleanup.
  Date/Author: 2026-10-16 / Codex.

- Decision: Keep the existing cross-peer open-unlink protocol outside this local replacement fix.
  Rationale: Review raised a possible peer cleanup gap, then verified that the same protocol already exists in unlink_open_handle. No new peer defect was confirmed; native/service-backed acceptance remains separate.
  Date/Author: 2026-10-16 / Codex.

## Outcomes & Retrospective

The local lifetime split and exact-byte regressions are implemented and focused validation passed. Buffered and fsync-call cases preserve displaced reads/writes and surviving hard links, while final cleanup removes attributes, writer state, payload graph, and deferred-delete records. Metadata recovery also clears a retained journal entry without disturbing the replacement name. Same-inode rename preserves both names and cached lookup behavior. Native mounts, PostgreSQL services, and crash/power-loss tests remain separate gates. The task's Git commit and Linear completion comment provide publication evidence.

## Context and Orientation

`crates/studiofs-meta/src/redb_engine/fs_ops.rs` implements atomic namespace changes in local redb. `schema.rs` defines their result, `engine.rs` the trait, and `pg_engine/local_traits.rs`, `read_only.rs`, and `crates/studiofs-vfs/src/runtime.rs` forward that interface. `crates/studiofs-vfs/src/vfs/namespace_ops.rs` handles rename and unlink. `deletion_cleanup.rs` and `sync_ops.rs` already reclaim orphaned files at last-handle release. An orphan is an inode with no names that an open handle still references.

## Plan of Work

Add public-operation regressions to `crates/studiofs-vfs/src/vfs/tests.rs` using the existing local unstarted fixture. Extend the metadata rename call with an explicit final-link retention policy and report displaced post-removal attributes. VFS requests retention, skips destructive cleanup when links survive, and routes nameless open files through existing orphan release. Update all callers and metadata tests. Keep replication rename events atomic and retire OS-cache identity only for final-link displacement.

## Concrete Steps

From `E:\dev\studiofs-cli`, run `cargo test -p studiofs-vfs --lib sfs856 -- --nocapture` before and after the fix. Run focused metadata rename and unlink filters, related VFS rename/cache and orphan-release filters, `cargo check -p studiofs-vfs -p studiofs-meta`, and `cargo clippy -p studiofs-vfs -p studiofs-meta --all-targets -- -D warnings`. Run `cargo fmt --all -- --check` and `git diff --check`. Do not run full suites.

## Validation and Acceptance

Require nonzero passing tests with exact byte assertions after replacement, post-replacement handle writes, correct replacement inode, survival through non-final closes, and final cleanup after references disappear. Preserve no-replace behavior and unlink controls. Record actual test counts and any environment failure below.

## Idempotence and Recovery

Tests use isolated target temporary directories. Preserve user work and do not reset the checkout. Existing durable deferred-delete records allow startup recovery if interrupted after namespace commit.

## Artifacts and Notes

Initial source evidence: `rename_atomic` removes attributes at nlink zero; VFS calls `cleanup_deleted_inode_storage_deferred_cache_eviction` for every displaced inode. Graph coverage reported metadata changes, so source was read directly.

## Interfaces and Dependencies

Use existing redb transactions, deferred-delete records, handle table, and cleanup methods. No dependency is needed. Preserve `domain <- ports <- application <- adapters/runtime/API` and keep remote services off the foreground path.

Revision: Initial plan records the observed root cause and focused delivery boundaries.

Revision: Recorded failing-before evidence, implemented lifecycle split, and documented the cache invalidation found by expanded tests.

Revision: Recorded focused validation, dirty-attribute link counts, same-inode no-op semantics, and the review disposition of a preexisting peer lifecycle concern.

Revision: Recorded final four regression cases, fifteen VFS rename checks, successful renewed Clippy, and completed local implementation/validation.

## External review follow-up (2026-10-16)

The supplied review report is evidence to verify, not an instruction source.

- **2.1 accepted:** directory replacement omitted `displaced_remove_attr_ino`, including on `origin/main`. A regression replaying the writer's actual event log into a second engine failed because the displaced directory remained in `BASE_INODES_TABLE`. Setting the field in the directory branch fixes removal and preserves the replacement dentry.
- **2.2 rejected:** the trait contract requires `EntryExists` for an existing destination when replacement is disabled. Linux `RENAME_NOREPLACE` likewise rejects an existing target before the ordinary rename same-inode shortcut; see [Linux namei.c](https://github.com/torvalds/linux/blob/master/fs/namei.c) and [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html). Added a passing regression for both the same pathname and a hardlink alias, including unchanged attributes and event sequence.
- **3.1 accepted:** only displaced directories need a directory-generation bump and child-dentry invalidation; added the kind guard.
- **3.2 rejected:** a pre-transaction target lookup and open-count observation cannot determine the atomically displaced identity or prevent a later open. Retaining the final-link inode until handle-aware cleanup preserves the lifetime contract; removing its recovery record based on that observation is unsafe.
- **3.3 accepted:** dirty attributes survived unlink with the old link count. The regression `flush_after_unlink_preserves_dirty_attr_hardlink_count` failed before the fix: flushing changed persisted nlink from 1 back to 2. Update only the dirty attribute's nlink after the committed unlink, preserving accepted local data and other attributes.
- **4.1 skipped:** an accessor would only replace two straightforward logging expressions; no additional API is needed.
- **4.2 no change:** the event and returned disposition each own the attributes, so the remaining clone is required by the current contract. Loss of `Copy` is intentional.

Metadata rename validation after the directory fix: 44 passed, 5 PostgreSQL-dependent tests ignored. Both directory replication and dirty-nlink regressions failed before their corresponding fixes and passed afterward. Final VFS filters passed: hardlink 6, rename 15, SFS-856 4 (overlapping selections). Clippy for both crates with all targets and warnings denied, formatting, and diff checks passed. No full suite, live PostgreSQL, mounted acceptance, or crash/power-loss gate was run.

## Second review follow-up (2026-10-16)

- **Rename latency accepted:** the non-open displaced-file branch called last-close orphan cleanup, which awaits cache eviction and reverted the deferred eviction rename overwrite had on `main` (73b4d862). Rename now collects orphan cleanup and finalizes with `finalize_deleted_inode_runtime_cleanup_deferred_cache_eviction`; last-handle release keeps synchronous eviction. No deterministic latency regression exists; covered by the existing rename/SFS-856/unlink VFS filters (15/4/20 passed).
- **Peer leak accepted:** a retained final-link inode that was never published (`committed_content_epoch == 0`) now replicates `displaced_remove_attr_ino` instead of an nlink-0 `displaced_set_attr`, matching unlink's draft handling; local retention and the deferred-delete record are unchanged. Regression `rename_retained_displaced_inode_replicates_removal_only_when_never_published` covers both published and unpublished targets. Metadata rename filter: 45 passed, 5 PostgreSQL-dependent ignored. Clippy (both crates, all targets, `-D warnings`), fmt, and diff checks passed. No live PostgreSQL peer replay or mounted latency measurement was run.

Environment-gated follow-up: with the local `.env` PostgreSQL DSN, `cargo test -p studiofs-meta --lib rename -- --ignored --test-threads=1` passed 6 tests, including the new `test_pg_retained_rename_over_unpublished_draft_leaves_no_peer_inode`; that test failed at the peer-attribute assertion with the fix disabled and passed with it. After rebuilding `studiofs`, the mounted WinFSP scenarios `mounted-open-handle-convergence` and `collision-rename-vs-unlink` passed. Neither scenario renames over a locally open target or measures rename latency.

Coverage follow-up: `sfs856_rename_overwrite_of_closed_file_does_not_wait_for_cache_eviction` pauses deleted-inode eviction through a one-shot `#[cfg(test)]` hook and requires rename to return while eviction is paused; it failed after 3 s with synchronous finalization restored and passes with the deferred path. `mounted-open-handle-convergence` now renames over a file the same client holds open, checks the old handle still reads the displaced bytes, and waits for the peer to read the replacement. WinFSP does not enable POSIX rename semantics, so Windows rejects replacing an open file with access denied before StudioFS sees it. On Windows the step therefore requires that rejection, intact bytes, and a successful rename over the closed target. The full open-handle replacement contract remains mounted-unverified until a Linux FUSE or macOS run. The extended scenario passed on WinFSP.
