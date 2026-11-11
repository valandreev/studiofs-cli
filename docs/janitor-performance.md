# CLI janitor performance baseline

## Progress output (SFS-977)

The CLI emits a readable progress line every five seconds, beginning before the first local metadata open. It reports the active phase and its elapsed time. Counts are `unknown` until work updates them; while a blocking provider call is pending they represent completed work and attempted calls, not a fabricated completion. Phase values include `metadata_open`, `storage_setup`, `waiting_for_cycle`, `global_lock`, `protection`, `metadata_eligibility`, `listing`, `head`, `metadata_claim`, `delete`, `metadata_finish`, `object_scan`, and `cycle_summary`.

Pass `--output jsonl` to emit newline-delimited JSON events (`--output text` is the default). Each event has `schema_version: 1` and an `event` field. `progress` events contain `phase`, `phase_elapsed_ms`, `scanned`, `listed_entries`, `orphan_candidates`, `protected`, `skipped_young`, `skipped_unknown`, `head_attempted`, `delete_attempted`, `delete_confirmed`, `would_delete`, and `s3_rate_limit_per_second`. `cycle` events report `outcome`, `partial`, `coverage`, `scanned`, `listed_entries`, `protected`, `abandoned_uploads`, `orphan_candidates`, skip counts, delete attempted/confirmed/would-delete counts, phase timings, S3 throttle/retry counts, configured S3 rate limit and current `s3_request_interval_ms`, nullable `first_entry_ms` and `first_candidate_ms`, `peak_buffered_candidates`, `head_attempted`, `delete_errors`, and `head_errors`. Exactly one final `summary` event reports `outcome`, `coverage`, and `exit_code`; backend/unsafe/partial/lock-busy summaries also include a safe `message`. Null means not yet observed. In-flight requests, retryable queue depth and actual request throughput are not exposed by current interfaces, so no event reports them. `s3_rate_limit_per_second` is the configured cap, not measured throughput. S3 attempt counters are logical OpenDAL calls, not underlying HTTP retries. Listing page count is unavailable.

The process exit codes are 0 for complete success, 2 for a partial budget or cancellation, 3 when the single cycle cannot acquire the global lock, 4 when metadata safety cannot be established, and 5 for backend, schema and other failures, including a single cycle that ended with transient S3 errors or an open overload circuit. A second Ctrl+C exits immediately with 130. JSONL summaries include the matching `exit_code`.

Text mode remains suitable for redirected output and terminals on Windows. JSONL mode writes progress, cycle, and final-summary objects to stdout; phase diagnostics and safe error labels stay on stderr. A clean cycle can still have limited coverage when a scan, runtime, or delete budget stops admission. Dry-run opens local metadata read-only, reads remote protection/checkpoint/GC-candidate state, and takes a session-scoped PostgreSQL advisory lock to avoid racing a destructive sweep. It does not persist the lock, write the GC queue, reset/save a scan checkpoint, prune deletion fences, or send DELETE; it can list objects and HEAD untimed entries.

`studiofs janitor` logs the duration of metadata open, global lock acquisition, protection metadata reads, initial object listing, fallback HEAD requests, batch deletion work, object work, and the full lock-held cycle. It also logs the listed entry count, peak buffered candidates, scanned count, time to the first listed entry, time to the first orphan candidate, and OpenDAL HEAD/DELETE call counts. These are logical OpenDAL calls; retries can create additional HTTP requests.

## Independent request budgets (SFS-974)

The command accepts `--s3-requests-per-second` (default 4, range 1–32). The pipeline issues PostgreSQL commands and S3 requests one at a time, so the earlier `--pg-concurrency` and `--s3-concurrency` options had no effect and were removed. Request starts (LIST pagination, stat/HEAD, each canonical fragment DELETE) are paced at OpenDAL's HTTP request layer.

The provider-neutral pipeline deletes up to 64 claimed objects in one batch and buffers at most one eligibility batch (1,000 candidates); the rate and transport limits therefore remain effective if provider internals add parallel requests later. Existing deadlines bound listing to 60 seconds and stat/HEAD to 20 seconds and a claimed delete batch to 40 seconds; PostgreSQL janitor commands use the metadata session's command timeout. These are per-process budgets, not cluster-wide coordination. The mounted VFS janitor is unchanged.

## Retries, backoff and overload (SFS-975)

OpenDAL's `RetryLayer` sits outside the request gate, so each retry attempt passes the in-flight limit and rate pacing again. It retries only temporary errors, at most 3 times per request, with jittered exponential backoff. The request gate is adaptive: an HTTP 429/503 response or a transport failure doubles the spacing between request starts (up to 8 s) and pauses every request for the provider's `Retry-After` or the new spacing (at most 30 s); every other response shrinks the spacing by 1/8 back toward `--s3-requests-per-second`. The cycle summary prints per-cycle deltas for `s3_throttled` and `s3_retries`, and the current `s3_request_interval_ms` (initialized to the configured interval before the first request), plus `head errors` for objects skipped because their age probe failed transiently.

OpenDAL marks every error leaving its retry layer as persistent, so the janitor classifies by error kind: `NotFound` is a vanished key, `PermissionDenied`, `ConfigInvalid`, `Unsupported` and directory-kind errors are terminal, and everything else is transient. A failed key leaves its object retryable and defers its queue row. Already submitted batches drain before the cycle stops. Five transiently failed objects in a row stop the cycle as overloaded; `--loop` backs off and gives up after five failed cycles in a row.

The public OpenDAL `Lister` does not expose how many S3 pages it fetched. Production output therefore marks `list_pages=unavailable`. The test fixture below counts actual S3-compatible HTTP list requests. It uses the real OpenDAL S3 operator pointed at a loopback TCP server and never contacts external services.

## Streaming listing (SFS-969)

The CLI consumes the remote listing through OpenDAL `lister_with(prefix).recursive(true)`. Entries are evaluated as pages arrive and grouped into eligibility batches of `MAX_JANITOR_ELIGIBILITY_BATCH` (1,000). Memory holds one provider page plus one batch, independent of the number of keys. Keys of one object are merged only when adjacent, which matches S3's lexicographic listing order. A file key listed out of order fails the cycle, because merged object age depends on seeing every key of an object together; batches processed before that key keep their results.

Each listing request, including the first, is bounded by a 60-second timeout. A failed or stalled page fails the cycle with the number of entries listed so far: batches processed earlier keep their confirmed deletions, and the partially buffered batch is not acted on. The listing is never treated as proof of deletion safety; PostgreSQL eligibility and claims decide that.

`initial_listing_ms` is the time spent waiting on the listing, not wall time from start to the last page, because processing now overlaps listing.

From the repository root on Windows, run:

    cargo test -p studiofs --bin studiofs janitor_delete_cap_stops_opendal_listing_after_first_page -- --nocapture

The fixture serves 20,005 old object keys in 21 1,000-key S3 pages through the real OpenDAL S3 operator on a loopback server, with `max_deletes=2`. Reaching the cap in the first eligibility batch ends the cycle, so only the first page is requested; peak buffered candidates are 64, no HEAD is sent because every listed key carries `LastModified`, and two objects are deleted in one DeleteObjects request containing every canonical key.

`janitor_stalled_opendal_page_times_out_after_first_page_was_processed` uses the same loopback S3 with a second page that never arrives. The first page's 15 full batches are evaluated before the timeout ends the cycle.

## Object age (SFS-970)

Keys are accepted only when `canonical_object_delete_keys` reformats the parsed object ID back to the exact listed key (legacy key or one fragment key under the configured prefix); any other key is counted as `skipped_non_canonical` and never touched. Adjacent keys of one object are merged into one candidate, and its age is the newest `LastModified` across those keys, so a young fragment protects an old legacy key. Only keys listed without `LastModified` get a HEAD, each bounded by a 20-second timeout. A missing, failed, or future-dated timestamp makes the age unknown and the object is skipped. When PostgreSQL has a pending-deletion timestamp, the stricter of that age and the listed age decides, and no HEAD is sent. CLI and mounted janitor share the grace rule in `studiofs_data::janitor` (`janitor_age`, `pending_deletion_age`).

The earlier eager baseline (SFS-964, main commit `37c33f3f`) fetched all three pages of a 2,505-key fixture before the first HEAD and materialized all 2,505 entries.

## Standalone janitor ownership

The standalone command coordinates its phases in `crates/studiofs/src/cli/janitor.rs` and the provider-neutral cycle in `crates/studiofs/src/cli/janitor/pipeline.rs`. `studiofs-data` owns OpenDAL S3-compatible listing, last-modified reads, and fragment deletion; `studiofs-meta` owns the protected-reference snapshot and recovery reconciliation. This split keeps provider calls and metadata safety independently fakeable while preserving the existing protection policy. The mounted janitor remains in `crates/studiofs-vfs/src/vfs/workers.rs` and is not part of this CLI pipeline.

For a real volume, a non-deleting measurement can be started with `studiofs janitor <volume> --dry-run --skip-newer-minutes 1 --max-deletes 1000`. Dry-run still reads metadata, lists remote objects, and HEADs keys listed without `LastModified`; it suppresses DELETE calls. The recorded test does not run this command or connect to a live service.

## Reading a live run

The command prints phase-start messages before blocking metadata, lock, protection, and listing operations, then completion durations when each phase returns. The listing also prints `first_entry` once the first page delivers an entry; OpenDAL exposes no per-page boundaries through this API. `cycle_ms` includes lock acquisition, protection metadata reads, object work, and lock release. `object_work_ms` and HEAD/DELETE durations are nested measurements and must not be added together as independent wall time.

## Resuming bounded scans

The CLI persists scan progress in PostgreSQL under its per-volume janitor lock. Repeated `studiofs janitor <volume> --max-scan-entries 10000` invocations continue the prior scan. Output includes the next scan position and whether coverage completed; reaching the end wraps to the beginning so newly inserted keys behind the cursor are revisited.

S3 operators advertising `start_after` resume after the last listed key. An unfinished object group is retained in the checkpoint and all its canonical keys are statted again before deciding age. The position is saved after every completed eligibility batch, so a failed cycle resumes there. A provider that ignores `start_after` is rejected by the listing order check; the earlier hexadecimal prefix fallback was unreachable with the S3 service and was removed. Entry budgets count listing entries, while age probes retain their separate bounded operation deadlines.

`--full-scan` ignores the saved position for the first cycle and saves new progress; with `--loop` it applies once, and subsequent cycles resume. `--dry-run` reads progress and applies the flag only in memory: it never saves checkpoints.

Checkpoints bind the volume UUID, endpoint, bucket, normalized data prefix, and listing strategy. A supported scope change starts at the beginning. Missing tables, malformed state, and unknown checkpoint versions fail closed; an older binary cannot reset or overwrite a newer version.

## Schema version, deployment and recovery (SFS-980)

The payload GC queue (`snapshot_payload_gc_candidates`), sweep checkpoint (`snapshot_payload_sweep_checkpoints`) and upload fences share one schema version, recorded as `studiofs_schema_components(component = 'payload_gc')`. The current version is `PAYLOAD_GC_SCHEMA_VERSION = 1` in `crates/studiofs-meta/src/pg_engine/schema.rs`. Volumes created before this version are not supported and are not converted.

- **Deploy.** `studiofs init` and `studiofs load` apply the canonical, repeatable DDL in one PostgreSQL transaction, and the version marker is its last statement. An interrupted or failed application therefore leaves either the previous schema or the complete new one, never a marker without its tables. To deploy onto an existing metadata database, rerun `studiofs init` with the new binary: it applies the schema before it reports that the volume already exists. That report exits nonzero and suggests `studiofs destroy`; in this case it is expected and must not be followed. The CLI janitor performs no runtime DDL and there is no second migration runner.
- **Retry.** Reapplying the DDL is idempotent. It keeps existing queue, checkpoint and fence rows, recreates dropped guard triggers, and never lowers the recorded version.
- **Refusal.** After taking its lock, `studiofs janitor` checks that the recorded version equals the version it was built for and that the six deletion guard triggers on `event_log`, `snapshot_source_checkpoints` and `snapshot_payload_upload_watermarks` exist and are enabled. A missing table, missing marker, other version, or missing or disabled guard releases the lock and stops with exit code 5 (`schema_unavailable`) before any listing or deletion. Dry runs are refused the same way.
- **Rollback limit.** There is no down-migration. A binary built for an older version refuses a newer schema rather than relabelling it; roll back by redeploying the matching binary, not by editing the marker. Do not lower the marker by hand: an older janitor would then run against tables it does not understand.
- **Recovery.** Rerun `studiofs init` to repair a dropped guard or an incomplete schema. A disabled watermark guard is not re-enabled by the DDL, because that would take a table lock on every ensure; re-enable it with `ALTER TABLE snapshot_payload_upload_watermarks ENABLE TRIGGER USER`.
- **Dump and load.** A dump carries no GC queue, checkpoint or upload ledger. `studiofs load` refuses a target volume that still holds rows in any of those tables, and a loaded volume starts with an empty queue and a fresh scan, so every object is again judged by the normal protections and grace window.

## Large-volume benchmark (SFS-979)

The measured 1K/100K live and 1M synthetic matrix, the reasons for keeping the current defaults, and the reproduction commands are in `docs/perf/sfs-979-janitor-benchmark.md`. In short, memory stays flat and S3 and PostgreSQL load stay bounded. SFS-1064 cut the PostgreSQL round trips that limited throughput.

## PostgreSQL round trips (SFS-1064)

The janitor session prepares its statements once per connection and sends the statements of one call together; PostgreSQL still runs them in order. An eligibility batch of up to 1,000 candidates costs one round trip, plus one for the checkpoint save. Deletions are claimed in batches of up to 64 objects with one set-based statement, sent only after the `metadata_state` share lock in the same transaction is granted (two round trips: a claim queued behind a waiting lock would still commit after the client died), and settled with one statement that finishes each claim under its fencing token and acknowledges or defers its GC queue row. A batch starts its deletes only while its 60-second claim lease still covers its 40-second S3 work bound; otherwise claims are released without deferring their queue rows and claimed again. A stop releases every unattempted claim the same way. Measurements are in `docs/perf/sfs-979-janitor-benchmark.md`.

## SFS-1065: S3 multi-object deletion

The CLI submits all canonical keys of an admitted claim batch through OpenDAL's batched deleter, respecting the provider capability and S3's 1,000-key limit. At the current 64-object claim size this is 128 keys per healthy request. Every actual HTTP request, including retries and single-key recovery, passes the existing adaptive rate gate. `delete_attempted` continues counting canonical keys submitted to the adapter; it is not an HTTP request count.

OpenDAL 0.55 exposes only the number of successful keys. A complete successful flush confirms every key. A partial result is independently confirmed with idempotent single DELETEs, because the count cannot identify which object's keys failed. Recovery has a shared 35-second deadline; unconfirmed keys remain retryable. Failed batch calls also use bounded single-key confirmation; unconfirmed keys remain retryable. A batch that fails with 429 or after any 429/503 throttle leaves every key retryable without single-key DELETEs, so overload is not multiplied and the overload breaker still counts each object. HTTP 405/501 or typed Unsupported switches that store and its clones to single deletes for subsequent work. No unsupported result counts as success.

The remaining deletion allowance bounds each claim batch, so the cap cannot overshoot. Cancellation during an admitted batch drains all its keys and settles every claim before returning; cancellation before submission releases the claims. A successful object requires confirmation of every key and its fenced PostgreSQL finish.

Local loopback HTTP tests exercise batched partial failure, unsupported-provider fallback, throttle retries and a sustained throttle that sends no single-key DELETEs. The Backblaze B2 live adapter smoke passed with one batch POST for four keys, zero single-key fallback DELETEs, and the protected sentinel intact. Cleanup removed all nine scratch version/marker entries; none remain. Live PostgreSQL/RustFS throughput and adversarial evidence remain pending because the configured Docker daemon was unavailable. The historical SFS-979 measurements above are not measurements of multi-delete.

Backblaze B2 was selected as the production-provider class. B2 buckets are always versioned: a name-only delete adds a delete marker and retains older versions. The janitor's existing key-based deletion contract therefore confirms logical absence, not reclamation of historical versions. A tiny disposable B2 batch smoke must inspect version history as well as HEAD/list absence; explicit version deletion or lifecycle cleanup is outside SFS-1065. See [B2 bucket versions](https://www.backblaze.com/docs/cloud-storage-s3-compatible-api-bucket-versions) and [B2 Delete Objects](https://www.backblaze.com/apidocs/s3-delete-objects). Repeated name-only confirmations preserve logical absence but may add markers.

## Release gate (SFS-981)

The CLI janitor deletes by default, without `--dry-run`. Do not tag a release
that contains it until all three gates below pass on the release commit. Run
them from the repository root; every command works in PowerShell. The Docker
gates start their own disposable Compose project and never read `.env`. The
schema gate needs `STUDIOFS_TEST_PG_DSN` pointing at a disposable PostgreSQL.

| Gate | Proves | Command |
| --- | --- | --- |
| Safety (SFS-978) | Referenced, snapshot-pinned, uploading and delayed-publication payloads survive; dry run changes nothing; budgets, checkpoints, PG restart, kill/restart and lock exclusion recover | `cargo test -p studiofs --bin studiofs sfs978_ -- --nocapture`, then `cargo build -p studiofs --bin studiofs` and `cargo test -p studiofs --bin studiofs live_cli_disposable_ -- --ignored --nocapture --test-threads=1` |
| Scale (SFS-979) | Flat memory, bounded S3/PG load, completed sweeps without losing referenced objects | `cargo build --release -p studiofs --bin studiofs --tests`, then `cargo test -p studiofs --bin studiofs --release janitor_synthetic_listing_benchmark -- --ignored --nocapture` and `... live_cli_janitor_benchmark -- --ignored --nocapture` |
| Migration (SFS-980) | Atomic, repeatable schema deployment; janitor refuses missing, interrupted, drifted or newer schema | `cargo test -p studiofs-meta --lib payload_gc_version_marker_is_the_last_statement_and_never_downgrades` and `cargo test -p studiofs-meta --lib janitor_refuses_missing_interrupted_drifted_or_unsupported_gc_schema -- --ignored --nocapture` |

Deployment: build and publish the release, apply the schema once with
`studiofs init` (expected `already exists` exit, never followed by `destroy`),
then run `--dry-run --full-scan` and a capped run before scheduling `--loop`.
Rollback: stop running the CLI janitor; mounted clients do not depend on it.
There is no schema downgrade, and an older binary refuses the newer schema.

### Recorded run: 2026-11-11, Windows 11, Docker 29.9.3

- Safety: 2 deterministic and 6 disposable PostgreSQL 18/RustFS tests passed
  (92 s). This closes the SFS-978 Windows Docker gap.
- Migration: both schema tests passed against a disposable PostgreSQL 18.
- Scale, synthetic: 1M keys with 1,000 orphans completed in 90 s, peak RSS
  32 MiB, 1,000 list and 1,000 batch delete requests, no HEAD.
- Scale, live (1,000 objects, 50% orphans, 100 ms PostgreSQL RTT, 1 PG vCPU,
  4 S3 requests/s): the first run deleted 500 objects in 13.5 s (exit 2,
  capped), the second completed with nothing left (exit 0). No live or
  foreground object was missing; peak RSS 17 MiB, peak PostgreSQL CPU 7%.
  This is the first live RustFS measurement with SFS-1065 multi-delete; the
  same cycle took 268 s after SFS-1064 alone. Artifacts:
  `target/janitor-benchmark/live-sfs981-windows-1000-o50-pg100ms-rps4.json`,
  `synthetic-1000000-fault0.json`.
- Not covered: Windows console Ctrl+C delivery (the SIGINT drain step is
  Unix-only; hard kill and restart ran), a live mounted writer against the CLI
  janitor, and the dump/load GC-row refusal test, which needs S3 test
  configuration.
