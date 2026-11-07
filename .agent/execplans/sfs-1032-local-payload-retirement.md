# Retain local payload ownership after upload completion

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

Heavy overwrites leave uploaded cache objects and canceled dirty payloads until the next mount. Preserve their inode ownership after upload queue completion so local maintenance can reclaim objects once their readers, rollback and recovery owners are gone. Foreground callbacks must not perform remote work. Remote objects and their snapshot retention are unchanged.

## Progress

- [x] (2026-11-07) Traced overlay replacement, truncate cancellation, cached-store terminal transitions and inode session gates.
- [x] Reproduced both classes: initial run had 2 expected assertion failures after clean shutdown.
- [x] Retained terminal CachedStore ownership; added owned local maintenance and final shutdown sweep.
- [x] Focused validation: 3 retirement, 42 shutdown, 63 prefetch, 35 startup replay, 2 recovery and 56 cached-store tests passed; check, Clippy and format/diff checks passed.
- [x] Final release rebuilt; registered overwrite/read, open-handle and sparse-range mounted scenarios passed.
- [x] All six fsx legs completed 100000 operations each; clean remount and ext4 byte comparison passed, no repair. Startup orphan dirty and unreferenced cleanup counts were both zero; runtime reclamation removed 34973 objects without errors.

## Surprises & Discoveries

`CachedStore::cancel_upload` and `complete_upload` release queue accounting and pins but retain no inode ownership for later payload retirement. Overlay replacement legitimately drops slices without touching cached-store objects. Startup cleanup is the only exhaustive orphan collector. The uploader's inode gate is `FileSessionOperation::Upload`, which excludes reads and materialization as well as truncate rollback.

## Decision Log

Retain terminal object identities in the existing cached-store owner rather than changing slice mutation APIs or introducing a persistent garbage journal. Immutable object IDs and local metadata provide authority; after crashes the existing startup collector remains responsible. Keep candidates while handles, pending uploads, live extents or metadata/recovery roots can own them. Local cache eviction does not delete remote snapshot payloads; no new remote calls are needed.

## Outcomes & Retrospective

Both initial regressions passed after the first implementation. Three safety/retry tests passed, and 56 cached-store tests plus crate check passed. Independent review found PG-backed reachability under the inode gate and repeated scans; both were corrected with an explicit local-only metadata capability and a shared prefilter scan. All focused validation and the three registered mounted scenarios passed. Full six-leg fsx acceptance passed on Linux with the stock 120-second mount helper. All buffered/direct reads, remount hashes and ext4 references match. Runtime reclaimed 34973 obsolete local objects in 10 sweeps; remount removed zero dirty or warm orphans. No repair or reclamation failure occurred. Final shutdown reports zero pending uploads, successful metadata/payload sync and recoverability. Windows/macOS mounted gates were not run.

## Context and Orientation

`crates/studiofs-cache/src/cached_store.rs` owns queue terminal transitions and local payload storage. `crates/studiofs-vfs/src/vfs/deletion_cleanup.rs` owns local reclamation; `vfs.rs` starts owned workers and `upload_lifecycle.rs` drains them during shutdown. `get_local_referenced_object_ids` includes local base, overlay and pending recovery roots; the existing global GC method additionally queries PostgreSQL on PG-backed mounts. Metadata can restore removed overlay slices on a failed truncate; the exclusive Upload session prevents that restoration racing deletion. Existing open handles are preserved conservatively. RuntimeMetaHandle must forward the local-only metadata capability; without that forwarding the fail-closed default preserves payloads and the original regression tests fail.

## Milestones

First reproduce both orphan classes through real VFS writes and truncation, observing the two clean-shutdown assertions fail. Next retain cached-store ownership and demonstrate reclamation while preserving current graph, open-handle and prefetch readers, and failed stage cleanup retry. Finally validate local recovery and shutdown behavior, build the release product and run registered mounted scenarios and the exact six-leg fsx workload with a clean remount and ext4 comparison.

## Plan of Work

Add regression tests in `crates/studiofs-vfs/src/vfs/tests.rs`: overwrite while uploads are queued then truncate, and overwrite after a completed upload. Verify the old local payload is reclaimed on clean shutdown and the surviving payload remains readable. Add retained ownership to successful cached-store terminal transitions. Maintenance snapshots candidates grouped by inode, takes the exclusive inode Upload session, checks current local reachability and runtime owners, removes upload-stage links before payloads in bounded batches, and forgets only successfully removed candidates. Worker ownership and shutdown must join before stores close. Failed cleanup retains candidates for retry and startup fallback.

## Concrete Steps

Work in `/home/val/dev/studiofs-cli` on `fix/sfs-1032-local-payload-retirement`. Run focused tests using `cargo test -p studiofs-vfs --lib <test-filter>` before and after implementation. Run `cargo check -p studiofs-cache -p studiofs-meta -p studiofs-vfs`, `cargo clippy -p studiofs-cache -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`, `cargo fmt --all -- --check` and `git diff --check`.

## Validation and Acceptance

Both regression tests must fail on unchanged production code because old objects remain, then pass. Tests must retain referenced objects, open-handle objects and queued uploads. Existing startup replay and upload terminal accounting tests must pass. Unit tests do not prove mounted fsx acceptance: Linux needs FUSE, PostgreSQL, object storage, ext4 reference, a rebuilt binary and the six-leg runner under `target/xfstests/sfs1026` with the stock 120-second mount helper. Record unavailable prerequisites explicitly.

## Idempotence and Recovery

All cleanup is local and idempotent; keep ownership on failure. Crash recovery continues through existing startup replay. No remote deletion, new dependency, persistent format or foreground durability contract changes.

## Artifacts and Notes

Initial checkout: clean `main`; branch creation required Git write escalation because `.git` is read-only in the sandbox.

## Interfaces and Dependencies

Use existing CachedStore, local payload batch removal and VFS FileSession gates. Expose narrow candidate snapshot/forget methods between cache and VFS and add an explicitly local-only reachability method to MetaGc, forwarded through PostgreSQL, read-only and runtime facades.

Revision: initial plan records the shared ownership loss and conservative reclamation boundary.

Revision: recorded red/green evidence and review-driven local-only reachability, shared scan prefilter and runtime-facade forwarding. FUSE is available outside the sandbox; PostgreSQL/RustFS were started using the issue's isolated compose project.

Revision: protect in-flight prefetch snapshots with a reader lifetime gate, conservatively skip reclamation while that gate is occupied, and drain admitted volatile overlay batches under the inode gate before the final reference check. Final release binary and mounted results must be regenerated after these changes.

Revision: recorded focused-suite counts and final mounted scenario acceptance. Final binary SHA-256 is ec29da2baf06e3e4947da002f27ef29bf25f123f26df263ee9d743408641eea0. Full fsx is running under target/xfstests/sfs1026/sfs1032-20261107173244; registered report is target/e2e-runs/sfs1032-final-mounted-20261107/report.json. Validation command output is target/sfs1032-focused-validation.log.

Revision: full acceptance completed. Evidence: target/xfstests/sfs1026/sfs1032-20261107173244/legs/acceptance.json and payload-reclamation.json. Six leg durations were 18, 1466, 1467, 2794, 2796 and 2971 seconds; each exited zero after 100000 operations. The derived target-only runner removed silent remount retries, and its verifier accepts the rebuilt binary path rather than the old baseline binary. Test infrastructure is stopped without deleting volumes; client metadata and diagnostics remain retained.
