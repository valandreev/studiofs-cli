# Repair Windows read failures after replacing an uploading file

This ExecPlan is maintained under `.agent/PLANS.md`. Its Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective sections record the investigation and the validation boundary.

## Purpose / Big Picture

A Windows AJA workload succeeds initially, but replacing its file before S3 upload finishes leaves old uploads running, can fail the next read, and reportedly reduces subsequent warm reads from about 4 GB/s to 2 GB/s. Replacing a file must cancel obsolete work without removing locally authoritative bytes for the replacement. Warm read behavior must be measured on the mounted Windows path.

## Progress

- [x] (2026-10-17) Recorded checkout `<repo-root>`, branch `bug/winfsp-bughunt-01`, initially clean worktree, and release mount `test` on `Z:` (PID 41604).
- [x] (2026-10-17) Read relevant VFS/cache/WinFSP contracts and inspected recent scheduler PRs #813/#814; delegated scheduler and WinFSP read-path investigations.
- [x] (2026-10-17) Preserved live write-path counters under `target/perf/aja-restart-2026-10-17/`; no production metadata or existing payload was modified.
- [x] (2026-10-17) Ran the authorized three-pass 4 GiB native probe on the original release mount. Delete/recreate and truncate canceled old pending bytes; all six verified reads passed. Warm reads were 1.51–1.54 GiB/s. The original AJA read failure was not reproduced.
- [x] (2026-10-17) Added red/green regressions and corrected two independently confirmed defects: a canceled upload's staging-read failure poisoned inode error state; deferred WinFSP notifications permanently increased the failure count and disabled the Windows file cache.
- [x] (2026-10-17) Passed 29 upload-worker, 5 notification, 8 coherence, and 1 queue-owner tests; affected-crate all-target Clippy, formatting, and diff checks passed.
- [x] (2026-10-17) Built the candidate release binary under `target/perf/aja-restart-2026-10-17/studiofs-candidate.exe`; fresh independent review found no confirmed code defect.
- [x] (2026-10-17) Investigated raw versus resolved overlay references and rejected a membership-only change: publication intentionally requires raw references uploaded, so changing that predicate alone would break publication. No metadata changes retained.
- [x] (2026-10-17) Restored the user's preferred first candidate on Z: with INFO logging after the diagnostic build still failed AJA and reportedly lost about 30% throughput. Changing AJA's workload size is a supported operation.
- [x] (2026-10-17) Added focused red/green corrections for pure active-writer notification deferral and writer-stack handle ownership across truncate; these newest changes are not yet in the restored mounted candidate.
- [x] (2026-10-17) Reproduced a native failure automatically with buffered write-only overwrite, two byte-verified reads per round, and approximately 1 GB, 1 GB, 2 GB rounds: third-round first read passed, second reader's CreateFile returned FileNotFound. Failure cleanup was disabled before repeating; the path was already absent before cleanup.
- [x] (2026-10-17) Gracefully unmounted, preserved the production metadata database as `target/perf/aja-restart-2026-10-17/probe-failed-meta.db`, and remounted the same preferred candidate. Read-only inspection shows both the owned probe and AJA's actual file absent from serving and base dentry tables, with durable local `Created` markers and no dentry tombstones.
- [ ] Rebuild and validate the relevant native mounted sequence; record separate correctness and throughput results.

## Surprises & Discoveries

The mount uses today's release binary (2026-10-17 13:28 local time). Its existing counters show cancellation does occur: 1094 deleted operations and 1142 stale operations, including one stale operation after PUT. This does not establish whether the reported AJA replacement transition cancels at the right time.

The synthetic `.studiofs-runtime-stats` read did not finish promptly and was canceled. The existing cache-root `write_path_observability.json` remains available without entering the mount. No inference about the AJA cause is made from the synthetic read.

The Codebase Memory MCP transport was closed. Native CLI verified project `E-dev-studiofs-cli` at this checkout, generation `2026-10-17T09:59:13Z`; exact source is inspected for material conclusions. Local Git history, rather than unavailable authenticated GitHub queries, supplies recent PR diffs.

Diagnostics on the original mount show `invalidation_failed`, 19 delivery failures, no pending invalidations, and PostgreSQL caught up. The failure counter included retried batches: a transient WinFSP `NotifyBegin` refusal poisoned a cumulative health baseline permanently. The native probe's read opens all disabled the OS file cache. Correcting the failure metric preserves pending-age and real-failure protection; restored mounted cache admission and throughput still require measurement.

The upload cancellation regression pauses a real VFS upload at the staging-read boundary, cancels its owner and removes staging bytes, then resumes it. The original code stores that cancellation-induced read error as an inode-wide sticky error. The correction treats the canceled owner as stale (or requeues an authoritative retained owner) while preserving real I/O failures and shutdown behavior. This proves the lifecycle defect, not that it caused the original AJA read dialog.

Preserved console events confirm the user's observation: pending bytes rose from about 2.9 GB to 5.8 GB before draining. The authorized probe canceled promptly, so the exact original transition remains under investigation.

## Decision Log

- Decision: Investigate upload cancellation and Windows read caching separately until a shared cause is demonstrated.
  Rationale: Direct payload throughput cannot prove WinFSP cache behavior; previous healthy AJA runs did not reproduce this failure.
  Date/Author: 2026-10-17 / Codex.
- Decision: Use uniquely named disposable probe files and retain artifacts under `target/`; leave existing user files and metadata intact.
  Rationale: The active mounted volume contains unrelated files and provides valuable failure evidence.
  Date/Author: 2026-10-17 / Codex.

## Outcomes & Retrospective

Mounted E2E scenarios 11 and 18 passed against the rebuilt candidate (`target/e2e-runs/aja-restart-20261017/report.json`). The mount was restarted with the candidate; normal buffered warm reads of an existing MKV reached 4.42–4.59 GiB/s with cache admission enabled. A separate SequentialScan control reached about 1.97 GiB/s; open flags must be held constant when comparing throughput.

The user reproduced AJA's failure on the candidate: first run normal, subsequent run fails. The actual AJA workload opens `ajadisktest.dat` with overwrite/truncate on the same inode; it deletes only a separate 8-byte preflight file. Captured old-upload cancellation drained the first pending queue before the replacement 4 GiB queue. No VFS read error was logged. A second traced attempt passed twice and failed on the third write stage; its callbacks accepted all approximately 8 GiB of writes and two 64 KiB boundary reads, then no successful read-only open appeared. First two traced passes wrote approximately 4 GiB each. Exact underlying error remains under investigation; targeted WinFSP error conversion logging is enabled in the rebuilt release.

The candidate still developed two permanent delivery failures after AJA. A second invalidation classification defect is confirmed in source: active-writer deferral returns Failed for incomplete coverage and increments the permanent failure counter, although VFS explicitly retries that case without counting it as a delivery failure. A focused correction must retain incomplete coverage and exclude only pure writer deferral from the failure metric.

The latest traced 4 GB AJA attempt accepted the full write and full paging read, including EOF, and successfully opened its read-only context. This supersedes the earlier missing-read-only-open hypothesis for that attempt. No VFS read error was recorded. Heavy callback tracing is disabled again; throughput comparisons across trace settings do not isolate a source-code performance effect.

The retained native failure is a namespace loss, rather than a failed payload-byte check. The probe's catch records FileNotFound at `SafeFileHandle.CreateFile`, `File.Exists == false`, and no matching directory entry before any cleanup. Both repeated larger probes failed after the first verified read of the grown file. The immutable copied database confirms absent serving/base names while local `Created` authority remains. Normal WinFSP non-delete Cleanup/Close contains no unlink. Replay's final dentry reconciliation can remove an absent-base mirror after self-ack clears the event overlay, whereas inode reconciliation separately protects unpublished local payloads. A deterministic regression and an identity-preserving correction are in progress; the snapshot alone does not establish the exact triggering replay event.

The first metadata correction protects exact dentry reconciliation while local payload rows remain, with a regression continued through publication and self-ack. Adjacent dentry (2), foreign unlink (6), foreign rename (3), semantic notification (11), and writer-stack (57) tests passed, as did affected-crate Clippy and release build. Native 1/1/2 GB still failed, so this correction alone is insufficient. A second native run with temporary name-filtered caller diagnostics identifies the actual removal at `redb_engine/replication.rs:1962`: the `Some(base regular-file attr)` branch invokes `remove_stale_dentries_for_base_entry`, not the missing-base-inode branch. The temporary diagnostic source must be removed before delivery.

Publication currently derives `was_remote_visible` using a namespace-visibility predicate that treats zero-length `Updating` attrs as visible. Investigation now targets a never-published file truncated to zero: this predicate may suppress its first `AddEntry`, leaving a base inode without a base name, which inode reconciliation then treats as a stale alias. This producer hypothesis is being tested before another production correction.

A focused retry-age regression also failed before a three-line queue correction: when older in-flight notification A retries after new pending B arrives, the queue discarded A's older age. The correction preserves the minimum timestamp, including coalescing an identical key. Its controlled-clock regression passes; no coordination sleep was added.

The original AJA failure is not fixed yet. Whole-workspace and full ignored suites are outside the authorized validation scope.

## Context and Orientation

`crates/studiofs-winfsp/src/context.rs` maps Windows overwrite, set-file-size and cleanup into VFS operations and decides file-cache admission at open. `crates/studiofs-vfs/src/vfs/namespace_ops.rs` owns size changes and retained overlay uploads. `crates/studiofs-vfs/src/vfs/deletion_cleanup.rs` and `upload_lifecycle.rs` mark deleted inodes, cancel tokens and retain open-unlink bytes. `crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs` schedules and executes background uploads. `crates/studiofs-cache/src/upload_queue.rs` owns pending identity and scheduler paging. These layers must preserve local reads while obsolete remote work is canceled.

## Plan of Work

First preserve live counters, determine whether AJA deletes or truncates its existing inode, and exercise replacement using an owned file while upload bytes are pending. Compare expected payload bytes and record the first read error and native code. Inspect the corresponding operation identity, epoch, local payload pin and cancellation transition. Add the smallest deterministic regression at that boundary before changing production behavior. Avoid changing cache policy solely to improve a direct VFS benchmark.

After the failing test identifies the defect, correct the shared owner and its required callers. Inspect all call sites of changed functions and preserve cancellation, stale-completion and open-unlink contracts. Record exact files and test filters here when established. Validate correctness before measuring warm reads.

## Concrete Steps

Run all commands from `<repo-root>`. Preserve probe results in `target/perf/aja-restart-2026-10-17/`. Read existing counters from `D:/cache/8d16df8d-1dae-445a-b5b5-a4217f863ea2/write_path_observability.json`; do not print secret-bearing configuration. Use `cargo test -p <affected-crate> <specific-filter>` and require nonzero test execution. Cargo commands run sequentially. Before native acceptance rebuild the product; do not count the currently running old binary as fixed validation.

## Validation and Acceptance

A deterministic regression must fail on the original source and pass after the correction. The native replacement sequence must keep replacement bytes readable before and after upload completion and show obsolete pending work canceled. Repeat mounted reads and record throughput and callback counters separately from unit tests. A healthy native rerun means the issue was not reproduced there; it does not alone prove every intermittent AJA condition fixed.

Run `cargo fmt --all -- --check`, affected-crate `cargo check` and `cargo clippy -p <affected-crate> --all-targets -- -D warnings`, plus `git diff --check`. Do not run full suites. Document unavailable native or service-backed prerequisites explicitly.

## Idempotence and Recovery

Probe files have unique task-owned names. Remove only those files after collecting evidence. Do not reset the checkout, delete volume/cache roots, alter credentials, or rewrite production metadata. Temporary binaries, source probes and snapshots stay under `target/`.

## Artifacts and Notes

`target/perf/aja-restart-2026-10-17/write-path-before.json` contains the initial available live snapshot. The user reported AJA's exact dialog as `Disk Test Failed`, without an underlying Windows error code.

## Interfaces and Dependencies

Reuse existing VFS, upload-queue and mounted-scenario test helpers. Add no dependency or public configuration unless diagnosis proves it necessary. Preserve `domain <- ports <- application <- adapters/runtime/API`, local foreground metadata, and asynchronous remote publication.

Revision 2026-10-17: Created the living investigation plan after initial live and source discovery; implementation details remain conditional on a reproduced defect.
