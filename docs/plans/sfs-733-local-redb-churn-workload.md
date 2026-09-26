# Add a deterministic local redb churn workload

This ExecPlan is a living document and follows `.agent/PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as the work proceeds.

## Purpose / Big Picture

After this change, a developer can run one focused `studiofs-meta` test to create, update, rename, and delete local metadata in repeatable cycles. The test will report redb file and page measurements, retained event-log span, logical table row counts, and optional explicit compaction results. It will not connect to S3 or PostgreSQL and will not enforce a platform-specific size threshold.

## Progress

- [x] (2026-09-26 11:47Z) Read SFS-733 and its SFS-731/SFS-732 dependencies; confirm the issue is already In Progress and both prerequisites are Done.
- [x] (2026-09-26 11:47Z) Trace the current `RedbMetaEngine` diagnostic and metadata mutation paths; identify existing redb metrics and churn-test patterns.
- [x] (2026-09-26 12:00Z) Add the deterministic ignored churn workload and focused report in `crates/studiofs-meta/src/redb_engine/tests/local_storage_churn.rs`, included from `tests.rs`.
- [x] (2026-09-26 12:00Z) Document default, compaction, and larger soak invocations in `docs/perf/local-redb-growth-workload.md`.
- [x] (2026-09-26 12:00Z) Run the named workload with compaction disabled and enabled; both modes pass.
- [x] (2026-09-26 12:02Z) Run the focused workload, crate check/clippy, formatting, and diff checks; record their outcome here.

## Surprises & Discoveries

- SFS-732 already exposes physical bytes, redb page statistics, local event-log bounds, and compaction outcome/timing through `LocalStorageDiagnostics`; SFS-733 can consume that view without changing production code.
- The current event log retains local events. Therefore the report must show its sequence span separately from live inode/dentry/xattr/ACL table counts; a growing event span is not itself evidence that the visible namespace grew.
- The existing `studiofs perf` CLI only observes a mounted filesystem. A core local-storage workload belongs in the `studiofs-meta` test harness and can run without a mount or remote payload service.
- The Windows default run kept four visible files and empty temporary directories while event-log span rose from 18 to 2,450 and ACL tombstones from 0 to 128. Those counts are observable logical state, so the workload reports them rather than asserting that every table returns to baseline.
- The review pass added lifecycle and pending-compaction row counts. The churn files have local overlay chunk state and changed size, so unlink intentionally retains `DeletedBeforePublish` lifecycle rows without a remote cleanup worker; that count is reported rather than constrained to the four survivors.
- `LocalStorageDiagnostics` page statistics are optional best-effort samples. The report keeps those fields as `Option` values instead of treating an unavailable sample as a workload failure.

## Decision Log

- Decision: Put the workload beside the `RedbMetaEngine` tests and keep it ignored by default. Rationale: it is a manually invoked measurement scenario and must not add long-running work to the routine crate test run.
- Decision: Use deterministic cycle/slot names and a configurable file count. Rationale: no pseudo-random generator or seed is needed when inputs are derived from loop indexes.
- Decision: Keep compaction opt-in through an environment variable and avoid asserting byte-for-byte sizes or mandatory shrink. Rationale: the workload records redb 4.3 behavior for comparison, while filesystem allocation and compaction results can vary by platform and live state.
- Decision: Read redb `Table::len()` only in the test report for logical row counts. Rationale: this gives bounded diagnostics without adding full-table scans to production paths.
- Decision: Assert the visible survivor count and empty temporary directories, but report rather than assert a plateau for event and tombstone rows. Rationale: the observed ACL tombstone count grows with churn and must remain measurable for later retention work.

## Outcomes & Retrospective

The workload passes on Windows with compaction disabled and enabled. With 32 files per cycle, four cycles, and four fixed survivor files, the physical file stayed at 1,056,768 bytes during churn; explicit compaction reduced it to 167,936 bytes in about 21 ms. The workload also reported a retained event span of 2,450 and 128 ACL tombstones. These are measurements from one environment, not portable thresholds. `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` all passed. Only the named ignored workload ran; no full suite was run.

## Context and Orientation

`crates/studiofs-meta/src/redb_engine.rs` declares the engine modules and the test module. `crates/studiofs-meta/src/redb_engine/tests.rs` already contains local redb diagnostics tests and imports the engine's layout definitions. `RedbMetaEngine::local_storage_diagnostics()` reports file size, redb page statistics, retained local event sequences, and compaction counters; `Meta` operations create the local overlay state, while `apply_replica_events_to_base` seeds or retires peer-visible base rows. The workload will use the existing temporary-engine fixture, so every run starts from a fresh file and cleans it up when the test ends.

## Plan of Work

Add one ignored test that creates a fixed live directory plus two temporary parent directories. In each cycle it creates bounded, index-derived directories and files, seeds base inode/dentry/xattr/ACL state and regular-file chunk rows, changes attributes, updates and removes xattr/ACL rows, renames within and across parents, unlinks the files, applies base removal events, and removes the temporary directories. At every cycle boundary it verifies the visible live set is unchanged and prints physical/page/event/table-row diagnostics. The optional compaction mode calls the existing `compact_local_storage()` hook once after the final no-compaction checkpoint and reports its result.

Add a short perf note with the exact PowerShell and shell commands for the default workload, compaction mode, and a larger manual soak profile. Explain that the report is evidence for later policy work, not a production threshold, and that exact physical byte counts are not comparable across operating systems.

## Concrete Steps

Run from `E:\dev\studiofs-cli` in PowerShell:

    cargo test -p studiofs-meta local_metadata_churn_redb_growth_workload -- --ignored --nocapture

The unset file-count variable selects the small default. To include explicit compaction, run in PowerShell:

    $env:STUDIOFS_REDB_CHURN_COMPACT = "1"
    cargo test -p studiofs-meta local_metadata_churn_redb_growth_workload -- --ignored --nocapture
    Remove-Item Env:STUDIOFS_REDB_CHURN_COMPACT

Set `STUDIOFS_REDB_CHURN_FILES=1024` for the larger soak profile, using `$env:STUDIOFS_REDB_CHURN_FILES = "1024"` in PowerShell or `STUDIOFS_REDB_CHURN_FILES=1024` before the test command on Linux/macOS. The report emits `SFS-733 checkpoint=...` lines with visible entry counts, table rows, `LocalStorageDiagnostics`, and optional compaction data. Final checks are `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`.

## Validation and Acceptance

The named workload passed with compaction disabled and enabled. Its output includes physical database bytes, redb page/stored/fragmented statistics where available, first/next/retained event sequence information, logical row counts for local/base metadata tables, visible entry counts, and compaction duration/bytes when attempted. It proves that temporary directories and files are removed while the fixed survivor count remains constant. It does not assert exact sizes or a universal compaction shrink threshold. Only this named test was run; workspace-wide suites remain outside this task.

## Idempotence and Recovery

Each invocation uses the existing temporary redb fixture, whose temporary directory is dropped at the end of the test. Re-running the command therefore starts with a fresh database and deterministic operation names. Invalid or zero scale input should fail clearly rather than silently selecting another workload size.

## Artifacts and Notes

The test prints one `SFS-733` line for the initial state, each cycle, and the optional post-compaction state. Preserve those lines when comparing commits. Event-log retention is reported separately because future pruning work is not part of SFS-733.

## Interfaces and Dependencies

The workload uses existing `RedbMetaEngine` metadata APIs, `LocalStorageDiagnostics`, and `redb::Table::len()`. It adds no production API, dependency, telemetry path, or remote-service requirement.
