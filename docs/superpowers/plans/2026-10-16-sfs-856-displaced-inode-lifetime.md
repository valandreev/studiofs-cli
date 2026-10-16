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
