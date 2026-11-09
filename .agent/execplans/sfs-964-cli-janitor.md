# SFS-964: Reproduce and instrument the CLI janitor

## Purpose

The standalone `studiofs janitor` command can spend most of a run listing remote object keys before its deletion cap permits only a small number of deletes. This work makes that cost measurable without changing which objects are examined or deleted. A deterministic local S3 HTTP fixture exercises OpenDAL's real S3 listing, HEAD, and DELETE requests, while a local metadata fixture covers the command's metadata setup when practical. The mounted background janitor remains outside this task.

## Progress

- [x] Confirmed the clean checkout was fast-forwarded to `37c33f3f` and created branch `feature/sfs-964-reproduce-and-instrument-the-cli-janitor`.
- [x] Captured pre-instrumentation baseline: `cargo test -p studiofs --bin studiofs janitor_ -- --nocapture` passed 5/5 in 1.08 seconds; the package-wide filtered command could not link an integration-test executable (`LNK1104`).
- [x] Added and ran a real-OpenDAL local S3 characterization test for the eager listing and low-cap case. It observed three pages before the first HEAD, 2,505 listed/scanned entries, two HEADs, two fragment DELETE calls, and 121 ms total object-work time in the final run.
- [x] Added CLI phase markers and timings for metadata open, global lock wait, protection reads, initial listing, HEAD, DELETE, object work, and whole lock-held cycle. The listing remains eager; no protection or deletion decisions changed.
- [x] Added a local redb metadata fixture for the full CLI `run` path and a dry-run assertion that no DELETE requests are sent.
- [x] Documented the reproducible fixture, test command, timing limitations, safe live `--dry-run` command, and the fact that the reported live slowdown remains unverified.
- [x] Focused Windows CLI tests passed 8/8; `cargo check -p studiofs --bin studiofs`, `cargo clippy -p studiofs --bin studiofs -- -D warnings`, `cargo fmt --check`, and `git diff --check` passed.
- [ ] Root will stage, commit, and push the requested branch because this delegated environment cannot write Git metadata; no PR is requested.

## Plan

The relevant production path is `crates/studiofs/src/cli/janitor.rs`: `run` opens a metadata engine selected by the volume format, acquires the global janitor lock, loads snapshot pins and object references, then calls `run_one_cycle`. That cycle currently materializes all recursive S3 results with `list_with` before checking candidates. It calls `stat` for candidate age and deletes each fragment sequentially until successful deletes reach the configured cap. Instrument the existing boundaries and keep this control flow unchanged. The lock is PostgreSQL-backed only for PostgreSQL metadata formats. This CLI path does not call per-object claim or claim-finish methods, so report those as not applicable instead of adding new deletion semantics.

Use the co-located unit test's loopback HTTP server as the S3 fixture. It returns 2,505 old object keys over three S3 list pages and counts page requests, HEADs and DELETEs. With a successful-delete cap of two, the baseline expectation is three listing pages and all 2,505 scanned entries, two HEADs, and two objects' fragment DELETEs. This test must pass before adding timing/counter instrumentation; it intentionally records current eager behavior rather than a desired future algorithm.

Validation is limited to focused Windows CLI tests, formatting, and diff checks. Do not run the live cleanup command, contact an actual PostgreSQL or S3 service, or run full test suites. No mounted janitor behavior is changed.

## Decision Log

- Keep the current eager list operation and serial delete flow in this issue stage so the baseline and instrumentation describe the shipped algorithm. Rationale: performance changes require a reproducible baseline first.
- Count remote S3 list pages in the loopback fixture. OpenDAL's public eager `list_with` result exposes entries but not a runtime page count; do not claim an inferred page count in production telemetry.
- Report claim and claim-finish as not applicable for this CLI flow. Its direct deletes do not use the VFS background janitor's per-object claim protocol.

## Outcomes and Notes

The final loopback run observed three list pages before the first HEAD, 2,505 listed and peak entries, first candidate at 109 ms, two HEAD calls, two DELETE calls, and 121 ms total object-work time. The local redb CLI fixture completed metadata open, lock, protection, listing, HEAD and DELETE phases and verified the global lock can be acquired again after the run. No real PostgreSQL or S3 service was contacted. Production output cannot expose S3 page counts through the current public OpenDAL eager-list API, so it reports that field as unavailable. The user-reported 2 minute 30 second live behavior remains unverified.
