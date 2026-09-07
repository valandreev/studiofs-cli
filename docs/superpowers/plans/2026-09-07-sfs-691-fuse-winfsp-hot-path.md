# SFS-691 WinFSP hot-path attribute reuse

This ExecPlan is a living document. It follows `.agent/PLANS.md`; the
`Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes &
Retrospective` sections must stay current until the issue is complete.

## Purpose / Big Picture

WinFSP asks StudioFS to resolve the same positive Windows path from more than
one callback, especially `get_security_by_name` and read-only `open`. Today the
full-path shortcut remembers only the inode number, so a shortcut hit performs
another terminal attribute resolution. After this change, a stable positive
path also carries the attribute-cache observation that was valid when the path
was resolved. A hit may reuse the already cached attributes when that
observation, namespace generations, and local-first state are still valid;
otherwise it follows the existing component-resolution path.

The observable result is unchanged metadata and access behavior, while the
native SFS-690 benchmark can show fewer repeated `metadata_getattr` operations
and improved `stat`/`open_close` timing when the Windows mount prerequisites are
available. Existing negative lookup, enumeration, rename, delete, dirty,
deferred, and invalidation behavior remains covered by focused tests.

## Progress

- [x] (2026-09-07) Retrieved SFS-691, confirmed it is already `In Progress`, and recorded its SFS-690 benchmark dependency.
- [x] (2026-09-07) Traced WinFSP `get_security_by_name` and `open` to the shared `resolve_path` and traced the positive path-shortcut terminal `getattr`.
- [x] (2026-09-07) Read the WinFSP contract, local-first metadata/coherence contract, SFS-690 instrumentation contract, and execution-plan rules.
- [x] (2026-09-07) Confirmed the relevant graph index coverage has no recorded parse gaps; the source is metadata-changed and must remain the source of truth.
- [x] (2026-09-07) Added the first red focused test for observation-bound positive path reuse; before implementation it failed to compile because the observation-aware value and helpers did not exist.
- [x] (2026-09-07) Implemented the observation-aware cache fusion and preserved access, dirty, deferred, orphan, symlink, and invalidation fallbacks in the shared VFS resolver.
- [ ] Add focused safety tests for stale observations and local-first states, then run focused VFS/WinFSP validation.
- [ ] Run formatting, diff checks, and targeted clippy/checks without invoking workspace-wide suites.
- [ ] Run the release native interleaved SFS-690 A/B benchmark if the configured WinFSP mount and `.env` prerequisites are available; retain its artifact path and report whether the required workloads improved.
- [ ] Update SFS-691 in Linear with the implementation, validation, and benchmark evidence; do not close it without the final native evidence or an explicit environment-gated report.
- [ ] Commit all intended task files with an English `<type>: <subject>` commit message.

## Surprises & Discoveries

- The WinFSP `open` callback does not call a second explicit `getattr`; both
  callbacks call `context::resolve_path`, and the redundant attribute work is
  inside `NamespaceResolver::resolve_components_cached` after a positive path
  cache hit.
- `lookup_component_impl` already returns an `InodeAttr` and applies ignored,
  deferred, hidden-path, orphan-healing, and access rules. The positive path
  cache currently discards that attribute and stores only its inode.
- SFS-690 already measures `path_shortcut_cache_*`, `attr_read_cache_*`, and
  `metadata_getattr` at the real boundaries. The implementation will reuse
  these existing attribution counters and add a trace message for fused-hit
  versus fallback reasoning only if the focused validation shows the existing
  counters cannot explain the path.
- The previous SFS-680 sequential comparison was not decisive under changing
  host load, so performance acceptance must use the existing interleaved
  baseline/candidate harness rather than sequential timing claims.

## Decision Log

- Decision: Store an `AttrReadObservation` beside the positive path-shortcut
  inode, not a copied `InodeAttr`.
  Rationale: The attribute cache has a one-second TTL and explicit targeted and
  reset observation retirement. Storing the full attribute would duplicate a
  separately expiring cache entry and would make freshness harder to prove.
  The observation token binds reuse to the existing invalidation protocol.
  Date/Author: 2026-09-07 / Codex.
- Decision: Reuse the attr-cache entry only through a helper that validates the
  expected observation while holding the cache lock, rejects dirty/deferred
  local state, rechecks the local metadata refresh decision, and enforces read
  access before returning.
  Rationale: A raw cache lookup could return a stale pending attribute or cross
  a concurrent invalidation. Returning `None` for an unsafe fusion lets the
  existing full resolver preserve all local-first and healing behavior.
  Date/Author: 2026-09-07 / Codex.
- Decision: Keep negative path entries and namespace epoch/parent-generation
  validation unchanged.
  Rationale: SFS-691 is a hot positive-path optimization; changing negative
  lookup semantics or the bounded cache policy would expand the task and risk
  unrelated regressions.
  Date/Author: 2026-09-07 / Codex.
- Decision: Do not change replicated metadata semantics or add a global path
  cache. Keep the existing bounded two-tier cache and local redb foreground
  authority.
  Rationale: These are explicit issue and StudioFS contract boundaries.
  Date/Author: 2026-09-07 / Codex.

## Outcomes & Retrospective

This section is updated at each completed milestone. At completion it will
state whether the focused tests passed, which native workloads changed versus
the SFS-690 baseline, whether negative lookup and enumeration remained safe,
and which environment prerequisites, if any, prevented native confirmation.

## Context and Orientation

The repository root is `E:\dev\studiofs-cli`, and the working branch is
`feature/sfs-691-fuse-winfsp-path-resolution-and-attribute-lookup-for-hot`.
The worktree was clean when this plan was started.

`crates/studiofs-winfsp/src/context.rs` contains the WinFSP adapter. Its
`get_security_by_name` and `open` methods both call the local `resolve_path`
helper, which delegates to `NamespaceResolver::resolve_path`.

`crates/studiofs-vfs/src/vfs/namespace.rs` owns path normalization and the
bounded path-shortcut cache. `resolve_components_cached` walks components on a
miss, receives terminal `InodeAttr` values from `lookup_component`, and stores
the positive inode. On a hit it currently calls `StudioVfs::getattr` again.
`crates/studiofs-vfs/src/vfs/namespace_ops.rs` is the component lookup path and
must remain the authority for ignored, deferred, hidden, orphaned, and access
semantics.

`crates/studiofs-vfs/src/vfs/read_path.rs` owns read attribute resolution.
`AttrReadObservation` is a pair of a mount-wide generation and an inode-local
generation. `AttrReadCache::put_observed` admits a read only when the captured
observation is still current, and invalidation retires observations. The same
module's `MetadataRefreshDecision` determines whether a cached regular-file
attribute is safe to use locally or must be refreshed from authoritative
metadata.

The foreground mount uses local redb-backed VFS state. PostgreSQL and object
storage are bootstrap/replication/payload services, not ordinary foreground
path-read dependencies. A "fallback" in this plan means the existing
component lookup plus attribute-resolution path is used because the fused
observation is absent, expired, stale, or unsafe.

## Plan of Work

First add a focused test in the VFS namespace/read-path tests that proves a
positive path result is paired with the same current attribute observation and
that the observation-bound helper returns the cached attribute. Run only that
test and record the expected pre-implementation compile/test failure.

Then extend `AttrReadCache` with an observation-bound lookup that checks the
expected token before and after the cache read under its existing mutex. Add a
small `StudioVfs` helper in `read_path.rs` that returns an optional cached
attribute only when no deferred or dirty local attribute exists, the existing
`MetadataRefreshDecision` says local use is safe, the observation-bound cache
lookup succeeds, and `enforce_access(..., AccessLevel::ReadOnly)` succeeds.
An absent or unsafe result must be `Ok(None)`, not an error, so the namespace
resolver can use its normal component path. An inode-not-found access result
must retain the current positive-hit healing behavior by falling back to the
full resolver; other access/metadata errors must propagate as before.

Change `PathShortcutCacheValue::Positive` to hold `{ ino, attr_observation }`.
Capture the terminal inode's current observation when inserting a positive full
path entry. On a positive cache hit, call the new helper and invoke the current
`do_resolve` closure when it returns `None`; leave the negative branch and
namespace freshness checks untouched. Update the cache charge tests and direct
cache fixtures for the larger value.

Add focused tests for stable positive reuse, dirty and deferred local state,
expired/retired observations, and concurrent invalidation boundaries. Existing
rename, delete-subtree, negative-lookup, ignored/blocked path, Windows/Posix
normalization, bounded-cache, and semantic-reset tests must remain green. If a
test exposes a race, synchronize on the actual observation/cache condition with
a bounded wait; never add sleeps or retry loops as proof.

Finally run the narrow Rust validation, inspect the diff, rebuild and run the
registered release native SFS-690 harness with interleaved baseline/candidate
ordering when WinFSP and configured `.env` prerequisites are present, and
record the raw artifact identity and workload medians. If native prerequisites
are unavailable, report that exact gate as unavailable rather than claiming a
performance result.

## Concrete Steps

All commands below run from `E:\dev\studiofs-cli` in PowerShell. Do not run a
workspace-wide test suite.

    git status --short --branch
    cargo test -p studiofs-vfs path_shortcut --lib
    cargo test -p studiofs-vfs read_path --lib
    cargo check -p studiofs-vfs -p studiofs-winfsp
    cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings
    cargo fmt -- --check
    git diff --check

The native gate, only after rebuilding the product/test executable and only
when `.env` is configured with a free WinFSP mount point, is:

    powershell -ExecutionPolicy Bypass -File .\tools\perf\sfs-680-windows-metadata-ab.ps1 -MountPoint Z: -Runs 3

The drive letter is illustrative and must match
`STUDIOFS_WINFSP_TEST_MOUNT_POINT`. The harness must produce fresh artifacts
with exactly the four workloads (`stat`, `open_close`, `negative_lookup`, and
`enumeration`), three measured passes, interleaved run order, and attribution
columns. A focused test or compile check does not substitute for this native
gate.

## Validation and Acceptance

The focused VFS tests must pass with a nonzero number of executed tests and
must prove that stable positive paths can reuse a still-valid attr observation.
Dirty attributes, deferred namespace attributes, attr-cache expiry, targeted
invalidation, semantic reset, stale parent generations, rename/delete, hidden
and ignored paths, and access checks must continue to return the same result as
the existing resolver. No test may rely on an unbounded cache, a fixed sleep,
or a remote foreground read.

The native SFS-690 artifact must show the candidate binary identity used for
execution, successful fresh reports, and interleaved baseline/candidate timing.
Acceptance is a measured improvement in the targeted `stat` and/or
`open_close` wall-time median/tail together with reduced or explainable
`metadata_getattr` work, while `negative_lookup` and 64-entry `enumeration`
show no material regression. If repeated native evidence does not improve a
metric, record that honestly and keep the correctness change separate from the
performance claim.

## Idempotence and Recovery

The code and test edits are additive and safe to rerun. Re-running focused
tests does not alter repository state beyond `target/`. Native benchmark
artifacts are written under unique `target/perf/` directories. If a native run
fails because the mount is busy, WinFSP is unavailable, or required `.env`
values are absent, preserve the failure diagnostics, do not change the cache
semantics to accommodate the environment, and report the gate as unavailable.
Before committing, stage only the SFS-691 plan, source, test, and directly
related documentation files; preserve any unrelated worktree changes.

## Artifacts and Notes

The primary implementation files are:

    crates/studiofs-vfs/src/vfs/namespace.rs
    crates/studiofs-vfs/src/vfs/read_path.rs
    crates/studiofs-vfs/src/vfs.rs

The WinFSP callback path in
`crates/studiofs-winfsp/src/context.rs` is expected to require no semantic
change because it already shares the resolver; it must still be checked in the
final diff and focused compile validation. Native evidence, when available,
belongs under the generated `target/perf/sfs-680-windows-metadata-ab-*`
directory and must not contain credentials or full connection strings.

## Interfaces and Dependencies

At the end of implementation, the following internal interfaces must exist.

`AttrReadCache` in `crates/studiofs-vfs/src/vfs.rs` or its existing module
must provide an observation-bound lookup equivalent to:

    fn get_if_observed(
        &mut self,
        ino: Ino,
        expected: AttrReadObservation,
        now: Instant,
    ) -> Option<InodeAttr>;

`StudioVfs` in `crates/studiofs-vfs/src/vfs/read_path.rs` must provide an
internal helper equivalent to:

    pub(super) fn cached_attr_for_path_shortcut(
        &self,
        ino: Ino,
        expected: AttrReadObservation,
    ) -> VfsResult<Option<InodeAttr>>;

`PathShortcutCacheValue::Positive` in
`crates/studiofs-vfs/src/vfs/namespace.rs` must contain both the target `Ino`
and its `AttrReadObservation`. Its existing parent-generation and namespace
epoch metadata continues to be the freshness boundary for the path itself.

The implementation uses the existing `TwoTierCache`, `parking_lot` locks,
`MetadataRefreshDecision`, `AccessLevel`, `MetaError`, and local VFS metadata
interfaces. It must not add a dependency, a network call, a replicated schema
change, or a second global cache.

Plan revision note (2026-09-07): created after tracing the shared WinFSP path
and reviewing the SFS-690 measurement contract; the plan intentionally keeps
native benchmark work as a final acceptance milestone because the current
worktree has no code changes yet.
