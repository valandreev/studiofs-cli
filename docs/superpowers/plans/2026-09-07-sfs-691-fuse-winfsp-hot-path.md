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
- [x] (2026-09-07) Added focused safety tests for stale observations and local-first states; the 9 path-shortcut, 5 namespace-resolver, 3 `getattr_unchecked`, 6 semantic-invalidation, and 3 WinFSP unit tests passed.
- [x] (2026-09-07) Formatting, diff checks, targeted `cargo check`, and targeted `cargo clippy --all-targets -- -D warnings` passed without invoking workspace-wide suites.
- [x] (2026-09-07) Ran the release native interleaved SFS-690 A/B benchmark with baseline `a99fb44d` and candidate `44ff791f`; retained fresh artifacts and measured the required workloads.
- [x] (2026-09-07) Added a red test for direct normalized path-key encoding, then implemented borrowed path-cache lookup with an inline `SmallVec` key; full component strings remain fallback-only.
- [x] (2026-09-07) Re-ran the focused path, namespace, invalidation, metadata, WinFSP, check, and clippy validation after the key-preparation change.
- [x] (2026-09-07) Ran the fresh release native interleaved SFS-690 A/B benchmark with baseline `44ff791f` and candidate `885e7eda`; recorded latency gains, resource-cost signals, and the remaining `metadata_getattr` work.
- [x] (2026-09-07) Updated SFS-691 in Linear with the implementation, validation, and benchmark evidence; left it `In Progress` for review rather than closing it on median evidence alone.
- [x] (2026-09-07) Updated SFS-691 in Linear with the follow-up key-preparation implementation and five-run native benchmark evidence; left it `In Progress` because CPU/working-set and residual-getattr signals still need review.
- [x] (2026-09-07) Committed the initial SFS-691 implementation as `44ff791f` with `perf: reuse observed attrs on WinFSP hot paths`.
- [x] (2026-09-07) Committed the follow-up key-preparation optimization as `885e7eda` with `perf: avoid hot path key allocations`.
- [x] (2026-09-07) Verified the attached code review against the current source: the borrowed-key clone, duplicated normalization, root-path, observation-check, documentation, and boundary-test recommendations are applicable; the residual handle-based `metadata_getattr` is a separate WinFSP concern.
- [x] (2026-09-07) Applied the confirmed review fixes, added clone-count and path-boundary coverage, and rejected an initial two-pass normalization implementation after its native regression.
- [x] (2026-09-07) Reworked normalization to one pass and completed fresh three-run interleaved native benchmarks against `885e7eda`; the focused final check and Clippy pass.
- [x] (2026-09-07) Completed a five-run interleaved native benchmark for the final working tree; medians were directionally lower, but `stat` was effectively flat and enumeration p95 increased, so no universal tail-performance claim is made.

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
  these existing attribution counters; no new hot-path trace target was added
  because the harness already attributes the relevant cache and getattr work,
  while an extra trace event would add work to the optimized path.
- The previous SFS-680 sequential comparison was not decisive under changing
  host load, so performance acceptance must use the existing interleaved
  baseline/candidate harness rather than sequential timing claims.
- The SFS-690 harness completed all six timed processes and one untimed
  attribution process. Timed medians changed by `-12.0%` for `stat`, `-6.5%`
  for `open_close`, `-6.4%` for `negative_lookup`, and `-1.7%` for
  `enumeration`; candidate enumeration reported two IQR outliers, and no
  workload process failed.
- The first SFS-691 optimization still rebuilt `Vec<String>` components and an
  owned encoded cache key before every shortcut lookup. The second optimization
  moves normalization and encoding to an inline borrowed lookup key; a miss
  pays the existing component parse only after the cache has been checked.
- The five-run follow-up reduced timed medians by `-6.5%` for `stat`, `-2.9%`
  for `open_close`, `0.0%` for `negative_lookup`, and `-1.4%` for
  `enumeration`; all four p95 values also decreased. The candidate process CPU
  median was `1.0%` higher and max working set was about `14%` higher, while
  candidate attribution still showed `1000` `metadata_getattr` events for
  `stat` and `open_close`.
- The review's root-path allocation description is slightly overstated: an
  empty `Vec` has no element allocation, but bypassing normalization still
  removes the temporary parsing and bookkeeping path. The review's remaining
  `metadata_getattr` attribution is consistent with the handle-based
  `StudioFsContext::get_file_info` fallback and is not caused by a path-cache
  miss.
- The first shared-normalizer implementation stored component slices and then
  walked them again to encode the key. Its native A/B regressed `stat` by
  `+5.7%`, `open_close` by `+7.2%`, and enumeration by `+4.5%`, with higher
  candidate CPU; it was not retained. Building the encoded key and component
  slices in the same normalization pass removed that hot-path regression.
- The retained one-pass implementation's fresh interleaved A/B artifact
  `target/perf/sfs-680-windows-metadata-ab-20260907-164101` improved medians by
  `-11.2%` for `stat`, `-14.9%` for `open_close`, `-4.2%` for
  `negative_lookup`, and `-4.3%` for enumeration. Candidate p95 also improved
  for every workload; candidate CPU was lower in this run, while max working
  set remained higher (`24.012` vs `22.094` MiB).
- The final five-run interleaved artifact
  `target/perf/sfs-680-windows-metadata-ab-20260907-165415` was more stable for
  medians: `stat -0.8%`, `open_close -3.1%`, `negative_lookup -10.6%`, and
  enumeration `-3.7%`. `open_close` p95 improved, while `stat` was effectively
  flat and enumeration p95 increased; candidate CPU was lower and max working
  set was about `5%` higher. These results support keeping the correctness and
  maintainability fixes, but not claiming a universal latency-tail win.

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
- Decision: Use the existing SFS-690 cache and `metadata_getattr` counters for
  attribution and keep the benchmark harness/schema unchanged.
  Rationale: The native attribution run reported the candidate's path-shortcut
  hits, attr-cache hits, and remaining `metadata_getattr` work without adding
  a new per-hit counter or production trace overhead. The timing comparison is
  already interleaved and binary-identified.
  Date/Author: 2026-09-07 / Codex.
- Decision: Represent one parsed path as borrowed normalized component slices
  plus its encoded shortcut key, and reuse that representation on a fallback.
  Rationale: The hot hit path keeps the existing inline `SmallVec` behavior,
  while a miss or stale observation converts the already parsed slices into
  owned component strings instead of parsing separators and Windows suffixes a
  second time. The same parser becomes the single normalization authority for
  `resolve_path` and `resolve_parent`.
  Date/Author: 2026-09-07 / Codex.
- Decision: Add owned promotion/touch helpers to `TwoTierCache` and use them
  only from `get_borrowed`; retain the existing borrowed helpers for `get`.
  Rationale: The borrowed lookup already owns the stored key for stale cleanup
  and can transfer it directly into the ordering deque, removing one redundant
  `K::clone` without changing cache policy or ordering semantics.
  Date/Author: 2026-09-07 / Codex.
- Decision: Keep the residual handle-based `metadata_getattr` outside this
  follow-up.
  Rationale: `StudioFsContext::get_file_info` resolves by inode and has no path
  key to reuse. Changing it would be a separate WinFSP handle-attribute design,
  not a review cleanup of SFS-691's path shortcut.
  Date/Author: 2026-09-07 / Codex.

## Outcomes & Retrospective

The implementation is complete and committed in `44ff791f`. The shared VFS
resolver now records the attr observation with positive path shortcuts and
reuses the attr-cache entry only after observation, TTL, local-first, and read
access checks. The focused tests and targeted checks passed.

The native artifact
`target/perf/sfs-680-windows-metadata-ab-20260907-152048` contains fresh
interleaved results comparing baseline
`a99fb44d5d1f0000aee7b7a20508983060c2b360` with candidate
`44ff791fc2c27e807bff369de00deafec7938ebf`. Timed medians were
`46.550 -> 40.965 ms` for `stat` (-12.0%), `45.002 -> 42.085 ms` for
`open_close` (-6.5%), `24.569 -> 22.996 ms` for `negative_lookup` (-6.4%), and
`311.285 -> 305.912 ms` for 64-entry `enumeration` (-1.7%). The `open_close`
p95 improved slightly (`72.453 -> 72.057 ms`), while `stat` p95 was higher
(`73.337 -> 78.048 ms`) and candidate enumeration had two IQR outliers. The
artifact has successful fresh reports, exact binary identities, and the
  required candidate attribution run. The Linear issue remains open for its
  owner's review; it is not closed here.

The follow-up key-preparation optimization is implemented and has been measured
against the previous SFS-691 candidate.

That comparison is now recorded in
`target/perf/sfs-680-windows-metadata-ab-20260907-155249`: it improved the
latency distributions, including `stat` p95, but retained a small aggregate
resource-cost signal and did not remove the residual attribute work. The Linear
issue remains `In Progress` for review.

The review follow-up is now measured in
`target/perf/sfs-680-windows-metadata-ab-20260907-164101`. The one-pass shared
normalizer preserves the previous shortcut attribution while avoiding the
two-pass hot-path cost. The remaining `metadata_getattr` events are still the
expected inode-based `get_file_info` reads and are not changed by this path
work.

The final five-run repeat is retained in
`target/perf/sfs-680-windows-metadata-ab-20260907-165415`; its binary identity
and interleaved run order are recorded in `environment.json` and `command.txt`.
The plan is complete for the review follow-up, while SFS-691 remains open in
Linear for owner review and the separate handle-based `metadata_getattr`
optimization.

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

For the review follow-up, first add a clone-count regression for
`TwoTierCache::get_borrowed` in `crates/studiofs-vfs/src/vfs/tests.rs`. The
regression must prove that a probationary hit and a protected hit each clone
the stored key once after the initial insertion. Then add a borrowed
`NormalizedPath` representation in
`crates/studiofs-vfs/src/vfs/namespace.rs`. It owns only inline component
references and the encoded key during normal paths, shares separator,
`.`/`..`, and Windows trailing-dot/space handling for all callers, and converts
to `Vec<String>` only when the existing component resolver needs owned names.
Use it in `resolve_path` so cache misses and unsafe positive hits reuse the
already parsed components, in `resolve_parent`, and in the existing direct-key
unit test. Add tests for negative resolver hits, root normalization, more than
16 components, and encoded paths longer than 256 bytes. The refactor removes
the old delimiter-truncation calculation entirely, so no explanatory comment
is needed for that code. Finally add rustdoc for the two internal attribute
helpers and simplify `AttrReadCache::get_if_observed` without changing its
lock or observation contract.

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

The review follow-up used the same harness from `E:\dev\studiofs-cli` with
`-BaselineCommit 885e7eda -CandidateCommit HEAD -Runs 3`. It produced six
successful timed CSVs, one candidate attribution CSV, and the retained
artifact `target/perf/sfs-680-windows-metadata-ab-20260907-164101`. The
candidate binary was built from the current working tree; its commit identity
in the artifact is `578958618d6dc02be8b4980d51eddb23babe3469`.

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

Plan revision note (2026-09-07): recorded the red-green focused test cycle,
the committed implementation, targeted validation, and the successful native
interleaved artifact. The timing result is reported with its p95 and outlier
qualification rather than treating median improvement as universal tail
improvement.

Plan revision note (2026-09-07): added the attached-review follow-up, rejected
the measured two-pass normalization regression, retained the one-pass shared
representation, and recorded the final five-run result with its mixed tail and
memory signals.
