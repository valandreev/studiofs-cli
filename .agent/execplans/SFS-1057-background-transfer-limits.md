# Add configurable background transfer limits

This living ExecPlan follows `.agent/PLANS.md` and records SFS-1057 in the current worktree.

## Purpose / Big Picture

Mounts can set aggregate upload and download ceilings in decimal Mbps. Zero means unlimited. Mount flags and TOML defaults expose the same limits, while `studiofs settings` updates discovered running mounts through authenticated diagnostics. Status reports configured limits and cumulative successful throttle-wait time.

## Progress

- [x] (2026-11-17) Inspected root/scoped contracts, VFS architecture, config, transfer schedulers and diagnostics paths; Codebase Memory coverage reported no recorded gaps for the task transfer/settings files (best-effort evidence).
- [x] (2026-11-17) Added one shared upload/download limiter per mount across DataStore profile clones, bounded public values, status and live control.
- [x] (2026-11-17) Preserved original OpenDAL range reads and outer request timeout; ranges charge requested bytes plus the fixed encrypted-header allowance before foreground AIMD/QoS admission.
- [x] (2026-11-17) Uploads precharge encrypted bytes before AIMD/request timeout; bounded full fragment GETs charge measured bytes after GET and release AIMD/QoS before waiting. Prefetch charge occurs only after admission.
- [x] (2026-11-17) Isolated settings TOML helper from live discovery; all failed live updates return an error stating TOML was saved.
- [x] (2026-11-17) Added paused-time limiter tests and upload timeout separation, settings bounds/outcome tests, existing IPC and config coverage.
- [x] (2026-11-17) Added foreground-priority limiter queue and regressions for range wait ordering, skipped prefetch, full GET timeout separation and prepare-error metrics; added mounted fallback status line.
- [x] (2026-11-17) Ran focused transfer/settings/diagnostics/status tests, package check, all-target Clippy, formatting and diff checks; inspected final diff.

## Surprises & Discoveries

- Uploads encrypt complete objects before sending. A transport-specific client and chunked pacing complicated signed requests and timeout semantics unnecessarily; neither remains in this revision.
- An object can take longer than one second at a finite rate. The queue therefore waits for its complete encrypted size rather than truncating the charge to a bucket capacity.
- Tokio paused-time tests must yield after advancing time before inspecting task completion. Test byte counts must match decimal Mbps, not binary MiB.
- Foreground budget waits must not hold AIMD/QoS admission; skipped prefetch must not wait for or charge download budget. The shared queue places foreground waiters ahead of queued prefetches.

## Decision Log

- Decision (superseded by the 2026-11-17 re-review decision below): use FIFO whole-object precharge, with a drop guard that removes canceled waiters and wakes the next waiter.
  Rationale: initial implementation preserved original HTTP request behavior without a custom transport. Re-review added foreground priority because FIFO alone could delay foreground reads behind queued prefetch.
  Date/Author: 2026-11-17 / Codex.
- Decision (superseded for full downloads and range reads): uploads prepare ciphertext once and acquire budget before AIMD/request timeout on each retry. The initial design used HEAD/stat precharge for downloads; the re-review decision below removes it and charges measured full-read bytes after GET.
  Rationale: upload byte size is known before dispatch; full-download metadata probes and custom per-range timeout helpers introduced unwanted request behavior.
  Date/Author: 2026-11-17 / Codex.
- Decision: preserve the original full fragment GET and outer timeout. After GET, charge its measured encrypted length outside the timeout and release AIMD/QoS first. For ranges, charge requested bytes plus fixed SFEC header allowance before foreground admission; prefetch waits only after AIMD/QoS admission. Queued foreground waiters precede queued prefetch waiters.
  Rationale: avoids metadata HEAD calls and custom range helpers, keeps foreground scheduling responsive, and does not charge prefetch work that is skipped.
  Date/Author: 2026-11-17 / Codex.
- Decision: settings saves first, then performs live discovery only from the public run entry point. No discovered mounts is successful; all discovered updates failing returns nonzero and states the saved TOML result.
  Rationale: tests remain isolated from real local mount state and operators can distinguish persistence from live application.
  Date/Author: 2026-11-17 / Codex.

## Outcomes & Retrospective

Revision package-scoped check and all-target Clippy passed. Focused results are recorded below. Historical transport/chunk test claims have been removed because they do not describe the current implementation. No full suite, live S3, or mounted acceptance is claimed. No commit, push, or PR is authorized for this work.

## Context and Orientation

`crates/studiofs-data/src/payload_throttle.rs` owns shared upload/download budgets, foreground-priority queueing, live rate changes and counters. `store.rs` prepares upload ciphertext and charges full-read/range bytes. `crates/studiofs-vfs/src/upload.rs` separates upload budget waits from request timeouts; `fetch.rs` keeps the original timeout around GET and handles foreground/prefetch admission. VFS config/bootstrap/status and shutdown connect the shared state. CLI runtime flags/settings persist defaults, and diagnostics protocol/client/service/live source update a mount under its controller lease.

## Plan of Work

Keep unlimited OpenDAL paths unchanged. Limit values are 0 through 1,000,000 Mbps; one Mbps is 125,000 bytes per second. Uploads charge each complete encrypted object before dispatch. Full fragment downloads charge the measured encrypted response after GET. Range reads charge the requested range plus a fixed encrypted-header allowance before foreground admission; prefetch budget waits occur after AIMD/QoS admission. Each mount shares a direction budget across its workers; independent mounts have independent budgets. In-flight requests are not repaced by live updates. Short-term bursts may contain one object per concurrent transfer; this is an average transfer ceiling rather than packet pacing.

## Concrete Steps

From `E:\dev\studiofs-cli`, run focused limiter, upload budget timeout, config, CLI/IPC, full-read and range-read tests; then package-scoped check and all-target Clippy. Run formatting and diff checks. Record exact outcomes below. Never run a full workspace or ignored suite.

## Validation and Acceptance

Paused-time tests demonstrate aggregate serial budget, objects larger than a one-second allowance, dropped-wait refund, unlimited behavior, finite live change and shutdown cancellation. The upload fake-store test waits ten seconds for budget despite a five-second request timeout, proving budget waits are excluded. CLI tests verify bounds/persistence and live-outcome handling. Diagnostics tests verify authenticated lease updates and unchanged unspecified direction. Focused tests must run nonzero cases.

## Idempotence and Recovery

Settings is safe to repeat. Saved defaults apply on the next mount even if no endpoint is available. Live settings override explicit mount-time flags. Cancellation removes the queued wait without leaving a future reservation. No destructive cleanup or repository reset is part of this plan.

## Artifacts and Notes

Initial implementation checks (retained as historical evidence):

- `cargo test -p studiofs-data --lib payload_throttle::tests` - 6 passed.
- `cargo test -p studiofs-data --lib prepared_upload_budget_is_charged_once` - 1 passed.
- `cargo test -p studiofs-data --lib limited_large_download_charges_complete_encrypted_object` - 1 passed.
- `cargo test -p studiofs-data --lib shutdown_cancels_a_waiting_download_reader` - 1 passed.
- `cargo test -p studiofs-data --lib payload_range` - 9 passed after restoring original concurrent head/target reads. Initial revision run had 8 passed and 1 failed, identifying the serialized-read regression.
- `cargo test -p studiofs-vfs upload_budget_wait_is_outside_request_timeout` - 1 passed in the library target; other filtered targets ran zero cases and are not acceptance evidence.
- `cargo test -p studiofs payload_limit` - 4 passed in the binary unit target; other filtered targets ran zero cases and are not acceptance evidence. An initial `--lib` selector was invalid because studiofs has no library target; the corrected command above supplied the evidence.
- `cargo check -p studiofs-data -p studiofs-vfs -p studiofs` - passed on final source after original range concurrency was restored.
- `cargo clippy -p studiofs-data -p studiofs-vfs -p studiofs --all-targets -- -D warnings` - passed on final revised source.
- `cargo test -p studiofs-vfs --lib vfs_config_raw_validate` - 5 passed, including the new upper-bound regression.
- `cargo fmt --all -- --check` and `git diff --check` - passed; Git printed only LF-to-CRLF conversion warnings.

Re-review checks on revised source:

- `cargo test -p studiofs-data --lib payload_throttle::tests` - 7 passed, including foreground priority over queued prefetch.
- `cargo test -p studiofs-data --lib payload_range` - 9 passed; original concurrent target/head range reads remain intact.
- `cargo test -p studiofs-vfs --lib fetch::tests` - 31 passed, including skipped-prefetch budget, full-download wait outside the 1-second request timeout, and foreground range wait before AIMD/request timeout.
- `cargo test -p studiofs-vfs --lib upload::tests` - 7 passed, including budget wait outside request timeout and prepare-error metrics.
- `cargo test -p studiofs payload_limit` - 4 passed; `cargo test -p studiofs payload_limit_updates_require_auth_and_lease_over_ipc` - 1 passed.
- `cargo test -p studiofs mounted_status_formats_payload_limit_fallback` - 1 passed.
- `cargo check -p studiofs-data -p studiofs-vfs -p studiofs --all-targets` and `cargo clippy -p studiofs-data -p studiofs-vfs -p studiofs --all-targets -- -D warnings` - passed on revised source.
- `cargo fmt --all -- --check` and `git diff --check` - passed. No full test suite or mounted rerun was performed.

## Interfaces and Dependencies

Use existing Tokio synchronization/time and OpenDAL. No new production dependency is needed; Tokio test-util enables deterministic paused-time tests. No custom HTTP client, multipart writer, or limited chunk transfer path remains. The original full/range request timeout wraps GET work; rate waits occur outside it.

Revision note: replaced obsolete chunk-pacing/custom-client claims with the whole-object precharge implementation and reset validation claims to the actual revision evidence.

Revision note (2026-11-17): re-review restored original range GET logic and outer timeout, removed full-read HEAD/stat charging, moved full download charge to measured post-GET bytes, prioritized foreground budget waiters, and corrected range accounting to requested bytes plus fixed header allowance. Mounted measurements below remain historical and were not rerun for this review. The prior mounted-status fallback gap is closed by printing limits and cumulative waits from the existing write-path snapshot.

## Mounted evidence (2026-11-17, Windows/WinFSP, volume `test`, release build of this branch)

Volume `test` was upgraded from format 5 to 6 first (`studiofs upgrade test`).
It was mounted at `R:` with an isolated cache under `target/sfs1057-e2e/cache`.
Upload time is measured until `pending_upload_bytes`/`pending_uploads` in
`write_path_observability.json` reach zero. Read time is measured for cold-cache file reads.

| Case | Cap | Data | Result |
|---|---|---|---|
| Upload, mount flag | 16 Mbps | 48 MiB | 25.8 s, 15.6 Mbps |
| Upload, live `settings --upload-limit 64` | 64 Mbps | 96 MiB | 14.0 s, 57.5 Mbps |
| Upload, live unlimited | 0 | 96 MiB | about 1.7 s, about 480 Mbps (link baseline) |
| Upload below request timeout (4 MiB object > 15 s) | 2 Mbps | 12 MiB | 51.3 s, 2.0 Mbps; no timeout/retry/WARN in mount log |
| Sequential cold read, mount flag | 32 Mbps | 48 MiB | 13.4 s, 30.0 Mbps |
| 16 random 1 MiB cold reads (ranged path) | 32 Mbps | 16 MiB useful | 13.6 s (throttled; includes readahead amplification) |
| Live `settings --download-limit 0`, cold sequential | 0 | 96 MiB | 0.5 s |

`settings` reported `1 mount(s) updated, 0 failed` for each live change. Known gap:
`studiofs status` against a mounted volume (locked redb fallback) does not print the
payload-limit line; it only appears in the full status path.

### Rerun after the second review fix pass (rebuilt release binary)

The mount used a fresh cache with `--upload-limit 16 --download-limit 32`.
Uploading 24 MiB took 13.9 s, about 14.5 Mbps; the short sample makes polling
granularity more visible. A cold sequential read of 48 MiB took 12.9 s, about
31.1 Mbps. Sixteen random 1 MiB cold reads took 12.2 s. Plain `studiofs status`
on the mounted volume now prints
`Payload limits upload/download Mbps: 16/32; throttle wait ...`. It no longer
needs `--verbose`.
