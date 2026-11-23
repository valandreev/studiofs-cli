# Prove adaptive compression through real mounted clients

This ExecPlan is maintained according to `.agent/PLANS.md`. It is a living document; update Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective after each milestone.

## Purpose / Big Picture

Users need proof that compressible studio assets remain correct after upload, cold random reads, and disabling compression. The registered `mounted-compression` scenario writes USDA-like text, repetitive float scanlines, and deterministic high-entropy bytes through a real mount. It inspects encrypted object-store payloads and reads from independent clients whose local metadata and payload caches did not exist before discovery. Linux FUSE and macFUSE are acceptance platforms; WinFSP is a separate native gate. There is no performance threshold.

## Progress

- [x] (2026-11-23) Read ADR 0015, runner lifecycle and cold-peer patterns, and scoped contracts.
- [x] (2026-11-23) Implement the registered scenario, Linux full selection, focused fixture/format tests, and command documentation.
- [x] (2026-11-23) Initial `cargo check -p studiofs-e2e` passed on Windows.
- [x] (2026-11-23) Final Windows focused tests: two fixture/format tests, one registration test, and one full-selection contract test passed. Clippy, formatting, and diff whitespace checks passed.
- [x] (2026-11-23) Rebuilt Linux product/runner and passed final mounted run `sfs1094-linux-fuse-3`; retained all phases and cleanup evidence.
- [ ] Run macFUSE acceptance when a native macOS host with the driver is available.
- [ ] Record separate native WinFSP acceptance.

## Surprises & Discoveries

The DataStore returns decrypted STFS bytes, so proving the outer SFEC encryption format requires a read-only object-store client. The E2E crate directly uses the workspace's existing OpenDAL dependency to list and read this scenario volume's payload keys. It authenticates the envelope with the volume cipher and uses the production bounded decoder, without exposing encryption keys in reports.

The `sync_safety` runtime state is the observable publication barrier. The scenario polls this state with a 120-second deadline and retains the latest snapshot. It also requires the effective policy in that snapshot to be zstd level 3 or off as appropriate. A poll interval paces observable-state checks; no elapsed sleep is accepted as publication proof.

The first Linux mounted run passed with fully repetitive fixtures. Its compressed objects fitted in one SFEC frame, which did not exercise later encryption frames. The scanline fixture now distributes 12.5% deterministic entropy through the rows; remote inspection requires multiple SFEC frames. The second Linux run proved the new scanline layout (267804 stored bytes, five 64 KiB frames) but rejected a cache insertion during a cold mounted range. Inspection of `crates/studiofs-vfs/src/vfs/read_path.rs` and `crates/studiofs-vfs/src/vfs/access.rs` explains this: the first stream read is not a confirmed seek and may fetch a complete object. The scenario now records cache insertion without making it a failure. Request sizes refer to mounted reads, not object-store range transfer or bandwidth limits. The final corrected run, `sfs1094-linux-fuse-3`, passed all phases and cleanup after rebuilding the runner.

## Decision Log

- Decision: Use a new client sandbox for every range size and a separate client for complete readback.
  Rationale: Complete readback before seeking warms the local payload cache and would invalidate cold-range acceptance. Every range must perform its own S3 GET; each compressed file must advance decode time. Cache insertion counts are evidence, not a performance or internal range-strategy gate.
  Date/Author: 2026-11-23, Codex.
- Decision: Use distinct compressible bytes after disabling compression.
  Rationale: Remote inspection must distinguish the new raw object from the previous compressed object by exact decoded content.
  Date/Author: 2026-11-23, Codex.
- Decision: Preserve normal product read scheduling and allow full-object fetches during cold mounted range requests.
  Rationale: Correct byte delivery from an initially cold client is the acceptance contract. Forcing the partial-fetch policy would change product semantics and create an unrequested transfer/performance gate. Per-file remote GET/decode evidence proves the requested reads were not served from an already warm local cache.
  Date/Author: 2026-11-23, Codex.

## Outcomes & Retrospective

Implementation, final focused fixture/format tests (two passed), named registration and Linux full-selection contract tests (one each passed), Windows clippy, formatting, and whitespace checks are complete. The final corrected Linux FUSE mounted run `sfs1094-linux-fuse-3` passed preflight, setup, run, validation, scenario cleanup, and runner cleanup (exit 0). This proves byte correctness, remote formats and savings, cold mounted request sizes, and policy-switch compatibility on Linux. macFUSE is unavailable in the current Windows/WSL environment and remains pending; native WinFSP is a separate unrun gate. Neither is covered by Linux or compile-only checks.

## Context and Orientation

`crates/studiofs-e2e/src/scenario/workloads/compression.rs` owns the workload and evidence. Its `MountedCompression` implementation follows the runner's setup/run/validate/cleanup lifecycle. `crates/studiofs-e2e/src/scenario/registry.rs` registers the stable name. `crates/studiofs-e2e/src/scenario/workloads.rs` connects the module. `.github/workflows/linux-e2e.yml` explicitly selects it in the manual full gate, and `crates/studiofs-e2e/tests/runner_contract.rs` checks selection completeness. ADR 0015 defines STFS v3 compressed payloads, 128 KiB independent zstd chunks, 64 KiB SFEC encryption frames, and unchanged raw STFS v2 objects with 1 MiB frames.

## Plan of Work

The first milestone writes three 2 MiB fixtures under zstd level 3 and waits for safe publication. Read-only remote inspection authenticates SFEC, parses the STFS header, decodes the complete payload, and matches it to the fixture. Compressible fixtures must use v3 with 64 KiB frames and total encrypted stored length at most half the logical length. High-entropy bytes must use v2 with 1 MiB frames.

The second milestone unmounts the writer and discovers four independent peers in sequence. Each peer starts with no metadata database and an empty cache, then reads one seeded unaligned range from each file at one size: 4, 16, 64, or 256 KiB. Ranges cross a 128 KiB chunk boundary. Before/after runtime snapshots prove each file independently caused remote GETs and, for v3 files, decompression. Complete payload-cache insertions are recorded; a cold read may populate cache while serving the request. A fifth fresh peer compares complete files and their sizes.

The third milestone changes the shared policy to off while the writer is unmounted, remounts, writes distinct compressible data, and repeats publication and remote inspection expecting v2. A final fresh peer reads all old and new bytes, proving old v3 objects remain readable with the current policy off. All controllers remain owned for graceful cleanup, with emergency kill for bounded mounted-I/O timeouts. The existing runner retains process logs and failure reports.

## Concrete Steps

Run from the repository root, `E:\dev\studiofs-cli` on Windows or the matching checkout on Linux/macOS. Do not copy the Windows executable into WSL.

    cargo run -p studiofs-e2e -- list
    cargo test -p studiofs-e2e --lib compression_ -- --nocapture
    cargo test -p studiofs-e2e --test runner_contract compression_scenario_is_registered_and_selected_by_name -- --exact
    cargo test -p studiofs-e2e --test runner_contract full_e2e_selects_every_registered_scenario -- --exact
    cargo check -p studiofs-e2e
    cargo clippy -p studiofs-e2e --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

Build product and runner before mounted acceptance:

    cargo build -p studiofs -p studiofs-e2e
    cargo run -p studiofs-e2e -- run mounted-compression --studiofs-bin target/debug/studiofs --platform linux-fuse --run-id sfs1094-linux --retain always

On macOS replace the platform with `macos-fuse` and use `sfs1094-macos`. On Windows use `target/debug/studiofs.exe`, `windows-winfsp`, and `sfs1094-winfsp`. Docker Compose is the default infrastructure; `--infra env` uses the repository's configured S3/PostgreSQL services instead. Native driver and service preflight must succeed.

## Validation and Acceptance

Focused fixture tests must run nonzero tests and prove the text and scanlines select compression while entropy triggers the probe fallback. The format test rejects wrong STFS versions, encryption frame sizes, and insufficient compression. Registration checks must select the scenario once in the full Linux gate.

Mounted acceptance requires the runner to report passed and retain `compression.json`, the per-phase object inspection JSON files, per-peer read evidence, cold-start evidence, and before/after runtime snapshots under the scenario artifact directory. The object rows contain file identity, object key, SFEC marker, STFS version, frame size, logical/stored lengths, and SHA-256. Cold read evidence contains offsets, lengths, hashes, GET counts, complete-cache insertion counts, and decode time counters. Do not substitute focused unit or runner-contract tests for native mounted evidence. macFUSE and WinFSP results are recorded separately from Linux FUSE.

## Idempotence and Recovery

Use a new run ID for retries so absent metadata and empty-cache assertions remain meaningful. Keep failed sandboxes for diagnosis. The runner owns client teardown; do not delete cache paths while a mount is active. No existing user volume, remote object, or unrelated worktree edit is changed by this scenario. No full suites or performance gates are run by this task.

## Artifacts and Notes

Windows evidence on 2026-11-23: `cargo check -p studiofs-e2e` passed; `cargo test -p studiofs-e2e --lib compression_ -- --nocapture` ran two tests and passed; each named `runner_contract` command above ran one test and passed. `cargo clippy -p studiofs-e2e --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed. `cargo run -p studiofs-e2e -- list` exposed the compression scenario as entry 48. No full test suite ran. The parent also ran the existing production `payload_range_v3_reads_only_selected_chunks_with_three_requests` test in `studiofs-data` (one passed), which is separate from mounted correctness acceptance.

Final Linux artifacts are retained in `/home/valen/dev/studiofs/target/e2e-runs/sfs1094-linux-fuse-3/`, with the report/summary copied to `target/e2e-runs/sfs1094-linux-fuse-3/` in the Windows checkout. The product SHA-256 is `22c13b998ff6e3d40973de86d1c301b74e9e13e8893d3bd41c33b754fb4043e6`; runner SHA-256 is `f8547b8a025be8fd242c459dc6b88b0ff06eb5cd314d12510870d1287a190a96`. Native macFUSE/WinFSP and full suites were not run.

## Interfaces and Dependencies

The scenario implements the existing `Scenario` trait and uses `MultiClientController<SystemClientProcessExecutor>` for bootstrap, discovery, mounting, CLI policy updates, runtime snapshots, and teardown. It uses `run_bounded_mounted_io` for filesystem operations. `opendal::Operator` performs read-only S3 inspection, `ObjectCipher` authenticates/decrypts SFEC, `ObjectHeader` parses STFS, and `decode_object` verifies and decodes fixture bytes. No production API or on-disk format changes are required.

Revision note (2026-11-23): Created the self-contained implementation and validation plan; final acceptance remains explicitly pending.
Revision note (2026-11-23): Strengthened scanlines to exercise multiple SFEC frames; recorded initial mounted evidence and removed an invalid complete-cache insertion restriction after tracing actual first-read scheduling. Final corrected mounted acceptance remains pending.
Revision note (2026-11-23): Recorded final nonzero focused checks and passing corrected Linux FUSE mounted acceptance, while retaining separate macFUSE and WinFSP boundaries.
