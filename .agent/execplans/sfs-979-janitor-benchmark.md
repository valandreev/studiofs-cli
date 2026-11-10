# SFS-979: benchmark and soak-test the CLI janitor

This ExecPlan is a living document maintained under `.agent/PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current.

## Purpose / Big Picture

`studiofs janitor <volume>` deletes S3 objects that no filesystem metadata references. Its redesign (SFS-964 to SFS-978) replaced an eager listing with a streaming, checkpointed, rate-limited pipeline. Before changing production defaults, operators need evidence that the new command makes predictable progress on large buckets, keeps memory flat, does not saturate a small PostgreSQL server, never deletes referenced objects, and leaves foreground traffic responsive. After this work anyone can rerun two ignored tests and get a JSON report under `target/janitor-benchmark/`, and `docs/perf/sfs-979-janitor-benchmark.md` records the measured before/after matrix and the chosen defaults.

## Progress

- [x] (2026-11-10) Read SFS-979, its relations (blockers SFS-975 and SFS-978 are Done), `docs/janitor-performance.md`, the CLI janitor and its live adversarial fixture.
- [x] (2026-11-10) Built the eager baseline CLI (main commit `2c39e212`, SFS-964 instrumentation on the pre-redesign pipeline) in a detached worktree under `target/janitor-baseline/`.
- [x] (2026-11-10) Added JSONL cycle fields the text output already printed: `first_entry_ms`, `first_candidate_ms`, `peak_buffered_candidates`, `head_attempted`, `delete_errors`, `head_errors`, `s3_request_interval_ms`.
- [x] (2026-11-10) Added the synthetic benchmark `janitor_synthetic_listing_benchmark` and periodic 503/429 injection plus a lexicographic ID stride in the loopback S3 fixture.
- [x] (2026-11-10) Added the live benchmark `live_cli_janitor_benchmark` (`crates/studiofs/src/cli/janitor/tests/adversarial_live/benchmark.rs`).
- [x] (2026-11-10) Recorded the synthetic matrix: 100K and 1M keys, and 1M keys with 503 or 429 on every 20th request.
- [x] (2026-11-10) Recorded the live matrix: 1K keys at 0 and 100 ms PostgreSQL RTT, and 100K keys at 50 ms, each for both baseline and candidate. Wrote `docs/perf/sfs-979-janitor-benchmark.md` and updated `docs/janitor-performance.md`.
- [x] (2026-11-10) Filed SFS-1064 for the PostgreSQL round-trip bottleneck.
- [ ] macOS/Linux smoke runs; only Windows 11 with Docker Desktop was measured.

## Surprises & Discoveries

- `docs/janitor-performance.md` cites `janitor_streaming_listing_processes_first_page_before_later_pages`, which no longer exists; the closest current test is `janitor_delete_cap_stops_opendal_listing_after_first_page`.
- Throughput is bound by PostgreSQL RTT. A 500-deletion cycle took 443 s at about 0 ms and 1,323 s at 100 ms. That is about 18 extra round trips per deletion and about 20 per 64-key eligibility batch, because each unprepared tokio-postgres call costs a prepare plus an execute round trip. PostgreSQL CPU stayed at or below 12%. Filed as SFS-1064.
- The baseline never deletes legacy keys, yet still counts them as deleted. With grace 0 it also treats objects referenced after its start-up snapshot as orphan candidates: 266 of them in the 100K run.
- The first layout (even index = legacy) correlated with the 1% orphan pattern (`i * 37 % 100 < 1` means `i % 100 == 0`), so every orphan was legacy. The layout now uses `index % 3`. The 100K candidate run predates that fix, which does not change candidate cost.
- Concurrent `cargo test` processes fail with LNK1104 when one relinks an executable another is running. Run the built test executable directly to run configurations in parallel.
- The loopback S3 fixture formats IDs `1..=N` with the shard prefix `id & 0xffff`, so above 65,535 objects its listing is not lexicographic and the pipeline correctly rejects it. The synthetic benchmark multiplies IDs by 65,536 to keep one ordered shard.

## Decision Log

- Decision: Baseline is main commit `2c39e212`, not `37c33f3f`. Rationale: it is the eager pipeline plus SFS-964's timing output, so both sides print comparable phase data; behavior is otherwise identical to `37c33f3f`. Date: 2026-11-10.
- Decision: 1M keys are synthetic (loopback S3 + fake metadata) while 1K and 100K are live (RustFS + PostgreSQL in Docker). Rationale: seeding 1M real objects is slower than the measurement and the issue allows a labeled synthetic fixture. Date: 2026-11-10.
- Decision: PostgreSQL latency is injected by an in-test TCP proxy that delays each direction by half the configured round trip; constraints use `docker update --cpus/--memory` after seeding. Rationale: portable on Windows Docker Desktop, needs no `tc`/`NET_ADMIN`, and keeps seeding outside the measured window. The same proxy in front of RustFS adds S3 latency and counts connections. Date: 2026-11-10.
- Decision: Live S3 429/503 injection is covered only by the synthetic benchmark. Rationale: a TCP proxy cannot fault single requests on pooled HTTP/1.1 connections without an HTTP parser. Date: 2026-11-10.

- Decision: Keep `--s3-requests-per-second 4` and `--max-deletes 500`. Rationale: deletion stayed below the 2 objects/s that the 4 requests/s gate allows (1.1 objects/s at about 0 ms RTT, 0.38 at 100 ms RTT), so PostgreSQL RTT, not S3 pacing, sets throughput. A larger cap only lengthens the global-lock hold, about 22 minutes per capped cycle at 100 ms RTT. Date: 2026-11-10.
- Decision: Measure the baseline but do not gate it. Rationale: its losses (legacy orphans left behind, candidates taken from a stale snapshot) are the "before" result. Date: 2026-11-10.

## Outcomes & Retrospective

Delivered a repeatable benchmark (two ignored tests plus the report). Every candidate run kept every referenced object and reached complete coverage. Peak RSS stayed flat (16–17 MiB live, ≤ 29 MiB at 1M synthetic keys). The candidate used one PostgreSQL connection and one S3 request in flight. Foreground p99 stayed at or below 9 ms for PostgreSQL and 94 ms for S3. Defaults are unchanged. The open performance problem is PostgreSQL round trips (SFS-1064). No speedup over the baseline is claimed: the baseline is 5–14× faster in wall time, but unsafe and incomplete.

## Context and Orientation

The CLI cycle lives in `crates/studiofs/src/cli/janitor.rs` (argument parsing, metadata session, JSONL/text output) and `crates/studiofs/src/cli/janitor/pipeline.rs` (listing, eligibility batches of 64, claim/delete/finish one object at a time). `studiofs-data/src/janitor.rs` owns the OpenDAL S3 store and its request gate (`--s3-requests-per-second`, default 4, range 1–32). Each orphan costs one DELETE per canonical key (legacy key plus `CLOUD_FRAGMENT_COUNT` = 1 fragment key), so deletion throughput is bounded at roughly `rps / 2` objects per second.

The live disposable stack (`DisposableStack` in `adversarial_live.rs`) starts PostgreSQL 18 and RustFS through `docker/compose.e2e.yml` on free loopback ports, and tears them down on drop, keeping artifacts under `target/janitor-adversarial/<run>/`.

## Plan of Work and Concrete Steps

From the repository root on Windows (PowerShell or Git Bash), build the release CLI and test binary, then run:

    cargo build --release -p studiofs --bin studiofs
    STUDIOFS_JANITOR_SYNTHETIC_KEYS=1000000 cargo test -p studiofs --bin studiofs --release janitor_synthetic_listing_benchmark -- --ignored --nocapture
    STUDIOFS_JANITOR_BENCH_OBJECTS=1000 cargo test -p studiofs --bin studiofs --release live_cli_janitor_benchmark -- --ignored --nocapture

For the baseline, build `2c39e212` in a worktree and pass its binary:

    git worktree add --detach target/janitor-baseline/src 2c39e212
    cd target/janitor-baseline/src && CARGO_TARGET_DIR=../target cargo build --release -p studiofs --bin studiofs
    STUDIOFS_JANITOR_BENCH_BASELINE_BINARY=target/janitor-baseline/target/release/studiofs.exe cargo test ... live_cli_janitor_benchmark ...

The full knob list and the recorded matrix are in `docs/perf/sfs-979-janitor-benchmark.md`.

## Validation and Acceptance

Each live run ends with zero missing referenced objects (seeded live references and foreground objects); the candidate binary must also reach `complete` with no orphans left. The synthetic run asserts full coverage, the exact orphan count deleted, at most 64 buffered candidates, and one S3 request in flight.

## Idempotence and Recovery

Every live run uses a fresh Compose project, bucket and volume; reruns never share state. The baseline worktree lives under ignored `target/` and can be removed with `git worktree remove target/janitor-baseline/src`.
