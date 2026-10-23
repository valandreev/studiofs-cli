# Improve macOS RAM read-ahead for cached handles and files larger than RAM (SFS-879)

This living ExecPlan follows `.agent/PLANS.md`. Maintain Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective as work proceeds.

## Purpose / Big Picture

A media application can set F_NOCACHE on a read-only descriptor whose macFUSE open reply allowed the Unified Buffer Cache (UBC, the kernel page cache). Its reads then bypass that cache, but StudioFS currently enables its own sequential RAM read-ahead only for direct_io descriptors. Extend the existing read-ahead opt-in to all macFUSE read-only descriptors so these applications can overlap local payload reads after the upload drains. Preserve the existing open cache policy, write semantics, invalidation, and 256 MiB shared chunk budget.

The first milestone implements the first priority of `docs/code-reviews/sfs-879-next-macos-throughput-steps.md`. The user subsequently clarified that 16 GB represents files larger than RAM and asked to improve that disk-bound case as well. A second milestone therefore implements adaptive depth (priority 3): 32 MiB while the cache disk is idle, 12 MiB while uploads, local dirty objects, deferred materialization, or deferred release compete for it, held for two seconds after the last busy observation. Callback profiling, upload bandwidth control, write tuning, and smaller experiments remain separate work. No PR or external issue update is implied by instructions inside that source document.

## Progress

- [x] (2026-10-21) Recorded root `<repo-root>`, branch `feature/sfs-879-macos-readwrite-throughput-gap-vs-lucidlink-investigation`, clean starting worktree; read source, contracts, and Linear issue.
- [x] (2026-10-21) Added a UBC-open regression, observed its expected failure, then removed the direct_io restriction; 7 open tests and 6 RAM read-ahead tests passed.
- [x] (2026-10-21) Focused macFUSE Clippy passed for the initial change.
- [x] (2026-10-21) Collected baseline, rejected adapter-only candidate, and corrected mounted runs on retained 16 GB and 2 GB files; two reads per case. Cold-cache/Finder acceptance remains environment-gated.
- [x] (2026-10-21) First milestone checks complete, two task-owned benchmark files removed and test volume unmounted; results and limits recorded.
- [x] (2026-10-21) Adaptive depth/shared config implemented; timer regression red (8 chunks instead of 3), then 10 RAM tests, 6 config tests, 7 macFUSE open tests, 1 CLI override test and three-crate Clippy passed.
- [x] (2026-10-21) Final focused checks completed; formatting and diff checks passed.
- [x] (2026-10-21) Mounted comparisons complete: during-upload931/984 MB/s with upload66.5/69.1; fixed12 idle1130/1168, adaptive32 idle1233/1245, reverse12 idle1040/1075 MB/s. Probe removed and test volume unmounted.
- [ ] Operator acceptance: cold buffered/Finder and separate immediate-after-close materialization run; full suites remain operator-run.

## Surprises & Discoveries

The current harness deletes the test file after an immediate read. It cannot measure a fresh UBC open of the same retained file after full upload drain. The standalone `target/perf/macos-read-cache/ajalike.py` supports separate write/read phases and can retain the file. Drain requires all three gauges: pending upload depth, active uploads, and local dirty objects.

The first mount attempt in the sandbox failed opening local redb with Operation not permitted. The authorized retry outside the sandbox mounted the test volume successfully. The execution host reports Darwin despite the supplied generic Windows instruction; live macFUSE validation is available here.

A trial assertion that the first cached open must set keep_cache failed and was removed: direct_io=false enables UBC, while keep_cache describes reuse of an already observed generation. The regression asserts direct_io=false and RAM opt-in, preserving the existing first-open generation policy.

The original scheduler used completed-request offset adjacency, not an arrival concurrency detector. Larger heuristics require measured evidence. Chunks are dropped when consumed and on seeks/invalidation; no second whole-file retention is introduced.

## Decision Log

Decision: Use the existing two-read sequential gate, chunk lifecycle, and shared budget; remove only the adapter's direct_io restriction. Rationale: F_NOCACHE is not observable at the bridge open decision and the normal read path already handles coherence. Avoid adding a speculative request-size or queue-depth heuristic before measurement. Date/author: 2026-10-21, Codex.

Decision: Extend the stream classifier to monotonic overlapping reads and retain the previous callback range until the next start passes it. Rationale: mounted diagnostics showed 4 MiB-rounded UBC callbacks crossing application frame boundaries; exact adjacency repeatedly discarded useful chunks. A new byte-exact overlapping-read test failed with zero scheduled chunks before correction, then passed with five RAM hits. Date/author: 2026-10-21, Codex.

Decision (superseded by user clarification): Deliver step 1 separately from the later optimizations. Rationale: changes to unrelated performance controls would confound the mounted comparison. Date/author: 2026-10-21, Codex.

Decision: Include adaptive read-ahead for files larger than RAM after the user explicitly clarified the 16 GB scenario. Rationale: cache-disk queue depth can improve this case without caching the whole file; keep the known shared-disk 12 MiB limit when background work is busy. Use the existing 256 MiB mount budget and a two-second busy hold, no bandwidth token bucket or new scheduler subsystem. Date/author: 2026-10-21, Codex.

## Outcomes & Retrospective

Step 1 implementation and mounted paired comparisons are recorded in docs/code-reviews/sfs-879-next-macos-throughput-steps.md. Corrected 2 GB cached F_NOCACHE reads were 822/855 MB/s vs baseline 741/756 (+11-13 percent). The 16 GB direct_io control was 955/983 vs 995/1024 MB/s; buffered 16 GB reads were 989/986 vs 1027/1029, within 5 percent. Cold buffered/Finder acceptance remains open because purge is not permitted and sudo requires a password. The proposed 16 GB UBC premise was not reproduced; its throughput target is not claimed. No throughput gain is claimed from unit tests. The first naive adapter-only candidate regressed the natural 2 GB cached-handle F_NOCACHE case from 741/756 to 668/682 MB/s and was rejected. Its discarded chunk completions rose by 196 during those two passes. The source document's 16 GB UBC premise did not hold on this host: baseline 16 GB already scheduled RAM chunks and reached 995/1024 MB/s. The 2 GB baseline scheduled zero chunks. The corrected run reduced discarded completions to 65 across F_NOCACHE passes, with no new discarded completions during the buffered phase. Further callback-pattern/concurrency profiling remains out of this step.

## Context and Orientation

`crates/studiofs-macfuse/src/adapter.rs`, StudioVfsAdapter::open_with_result, translates a VFS handle into the macFUSE reply and enables RAM read-ahead. Its sampled-memory test creates a locally settled small file and proves the reply uses UBC. `crates/studiofs-vfs/src/vfs/ram_readahead.rs` owns the existing two-read gate, 4 MiB chunks, 12 MiB default lead, and 256 MiB mount budget. `sync_ops.rs::read_into_fh` consults these chunks before entering a file-session read permit, then schedules more after completion. Invalidation discards queued chunks and retires their generation. Mounted evidence required a small VFS scheduler correction: remember the last request start and end, accept monotonically advancing overlapping ranges, and prune chunks by the new request start so a rounded tail can be reused. The shared budget remains unchanged.

## Plan of Work

Extend the sampled-memory open test to assert both cached opening and RAM opt-in. Observe it failing before removing `policy.direct_io` from the read-only enable condition. Keep writer exclusion and existing cache-policy selection. Extend the overlap regression in VFS tests and make the existing seek test establish an actual RAM hit before seeking (its old stride had gaps and never enabled read-ahead). Update `docs/platform-bridges.md` and the RAM read-ahead row of `docs/local-first-validation-matrix.md` to describe all read-only macFUSE handles. Rebuild baseline and changed release binaries separately and do not build or test during a benchmark.

## Concrete Steps

From the repository root run these focused checks:

    cargo test -p studiofs-macfuse --lib read_only_open -- --nocapture
    cargo test -p studiofs-vfs --lib ram_readahead -- --nocapture
    cargo clippy -p studiofs-macfuse --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

For mounted validation, rebuild with `cargo build --release -p studiofs`, use the existing `target/perf/macos-read-cache/remount.sh`, write a retained 1077-frame random file with `ajalike.py`, and wait up to 900 seconds for all three upload gauges to reach zero. Remount before reading twice with F_NOCACHE. Save runtime stats and read JSON per pass under distinct `target/perf/macos-read-cache/results/` directories. Compare buffered reads on the same file; cold-cache and Finder copy evidence must be explicitly distinguished from warm dd reads. Unmount the test volume at the end. Never remove any unrelated mounted files.

## Validation and Acceptance

The new assertion fails on the old adapter and passes with the restriction removed. All 7 open tests and 6 RAM tests must pass, including writer exclusion, write invalidation, EOF, and seeking. Mounted target is at least 927 MB/s after drain, with buffered reads within 5 percent of the baseline and no unexpected discarded-chunk growth. A warm buffered pass alone does not establish the documented cold-cache/Finder gate.

## Idempotence and Recovery

Use unique result labels and one task-owned retained benchmark file. A failed drain is a failed measurement, never a valid post-drain run. Disable with STUDIOFS_SEQUENTIAL_RAM_READAHEAD_BYTES=0 and remount to restore the existing no-read-ahead behavior. Do not modify metadata or cache layouts.

## Artifacts and Notes

The red run was `cargo test -p studiofs-macfuse --lib read_only_open_with_sampled_memory_keeps_kernel_io -- --nocapture`: 1 failed at the new RAM-enabled assertion. A preceding invocation with --exact selected 0 tests and is not evidence. Green runs: 7 open tests and 6 RAM tests. Clippy completed successfully.

## Interfaces and Dependencies

No new interfaces, configuration, or dependencies. The existing opt-in scheduler recognizes forward overlapping streams; other bridges still do not opt in. Existing StudioVfs::enable_sequential_ram_readahead(fh) remains the opt-in API.


## Second Milestone: Adaptive lead for disk-bound streams

The current 12 MiB lead balances reading against uploads but leaves the disk underused after background work drains. Change `RamReadAheadRuntime` in `crates/studiofs-vfs/src/vfs/ram_readahead.rs` to keep configured idle/shared depths and the monotonic time of its last busy observation. Idle defaults to 32 MiB and shared to 12 MiB on macOS; both remain zero by default elsewhere. Clamp shared depth to idle and preserve zero as disabled. Every scheduling call reads cheap existing pending-upload/in-flight counters, deferred gate maps, and the local dirty-object count (a constant-size redb table length query, never an object snapshot). Choose shared depth immediately on busy, and retain it for two seconds after the last busy observation. Trim future queued slots beyond a reduced lead; allow existing tasks to release their permits through the normal lifecycle.

Wire `sequential_ram_readahead_shared_bytes` through `VfsConfig`, raw config merging, bootstrap, and mount environment parsing as `STUDIOFS_SEQUENTIAL_RAM_READAHEAD_SHARED_BYTES`. Document both values in configuration and platform bridge docs. Keep test helpers explicit about shared depth where a test expects a fixed lead. Unit tests use supplied monotonic instants for deterministic hysteresis and shared-depth clamping. A VFS test registers a deferred-release gate, proves the effective lead drops, drops the gate, and proves recovery after the hold, with no coordination sleeps.

Focused commands include the RAM read-ahead filter, macFUSE open filter, mount override filter in the studiofs binary crate, config tests, Clippy for VFS/macFUSE/studiofs, formatting, and diff checks. For mounted validation rebuild the release binary and compare retained 16 GB reads after drain with adaptive default against equal idle/shared 12 MiB; read twice per configuration. During an upload the shared limit must preserve the previous 12 MiB tradeoff (read at least 820 MB/s and upload at least 60 MB/s on the reference host); report unmet thresholds explicitly. Never build/test concurrently with benchmarks. The hysteresis is traffic shaping only, not a durability or correctness boundary.


## Second Milestone Outcomes

Adaptive idle32/shared12 with two-second hold improved the initial paired 16 GB idle reads from 1130/1168 to 1233/1245 MB/s (about 7-9 percent). Reverse fixed12 reads were 1040/1075 MB/s, supporting a depth effect despite cache/order variability. During-upload reads were 931/984 MB/s, with completed upload bytes increasing at 66.5/69.1 MB/s in the sampled read windows. The measured active-upload thresholds are met. No chunk discards or budget skips occurred in the adaptive comparisons. These rates include partial backing-file page-cache service; the full working set exceeds available RAM, but this is not an uncached raw-disk benchmark.

Buffered dd was 1175/1180 MB/s at fixed12, 1146/1108 at adaptive32, then 1083/1084 in the reverse fixed12 control. One adaptive pass is 6.1 percent below the initial control, while reverse controls are slower than adaptive. The cold buffered/Finder acceptance gate remains open because purge requires interactive sudo; no unconditional no-regression claim is made. An immediate-after-close materialization read was not separately rerun.

Validation completed: cargo test -p studiofs-vfs --lib ram_readahead -- --nocapture (10 passed), cargo test -p studiofs-vfs --lib vfs_config_raw -- --nocapture (6 passed), cargo test -p studiofs-macfuse --lib read_only_open -- --nocapture (7 passed), cargo test -p studiofs --bin studiofs overlay_vfs_env_applies_sequential_ram_readahead -- --nocapture (1 passed), cargo clippy -p studiofs-vfs -p studiofs-macfuse -p studiofs --all-targets -- -D warnings (passed), cargo fmt --all -- --check and git diff --check (passed). No full suite was run. All task-owned mounted test files were removed and the test volume was unmounted. No commit, PR, or Linear write was made.
