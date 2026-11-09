# CLI janitor performance baseline

## Progress output (SFS-977)

The CLI emits a readable progress line every five seconds, beginning before the first local metadata open. It reports the active phase and its elapsed time. Counts are `unknown` until work updates them; while a blocking provider call is pending they represent completed work and attempted calls, not a fabricated completion. Phase values include `metadata_open`, `storage_setup`, `waiting_for_cycle`, `global_lock`, `protection`, `metadata_eligibility`, `listing`, `head`, `metadata_claim`, `delete`, `metadata_finish`, `object_scan`, and `cycle_summary`.

Pass `--output jsonl` to emit newline-delimited JSON events (`--output text` is the default). Each event has `schema_version: 1` and an `event` field. `progress` events contain `phase`, `phase_elapsed_ms`, `scanned`, `listed_entries`, `orphan_candidates`, `protected`, `skipped_young`, `skipped_unknown`, `head_attempted`, `delete_attempted`, `delete_confirmed`, `would_delete`, `retryable_queue_depth`, `s3_rate_limit_per_second`, `s3_in_flight`, and `pg_in_flight`. `cycle` events report `outcome`, `partial`, `coverage`, `scanned`, `listed_entries`, `protected`, `orphan_candidates`, skip counts, delete attempted/confirmed/would-delete counts, nullable retryable queue depth, phase timings, S3 throttle/retry counts, and configured S3 rate limit. Exactly one final `summary` event reports `outcome`, `coverage`, and `exit_code`; backend/unsafe/partial/lock-busy summaries also include a safe `message`. Null means unobservable: in-flight requests, retryable queue depth, and actual request throughput are not exposed by current interfaces. `s3_rate_limit_per_second` is the configured cap, not measured throughput. S3 attempt counters are logical OpenDAL calls, not underlying HTTP retries. Listing page count is unavailable.

The process exit codes are 0 for complete success, 2 for partial budget/cancellation/retryable work, 3 when the single cycle cannot acquire the global lock, 4 when metadata safety cannot be established, and 5 for backend failures. JSONL summaries include the matching `exit_code`.

Text mode remains suitable for redirected output and terminals on Windows. JSONL mode writes progress, cycle, and final-summary objects to stdout; phase diagnostics and safe error labels stay on stderr. A clean cycle can still have limited coverage when a scan, runtime, or delete budget stops admission. Dry-run opens local metadata read-only, reads remote protection/checkpoint/GC-candidate state, and takes a session-scoped PostgreSQL advisory lock to avoid racing a destructive sweep. It does not persist the lock, write the GC queue, reset/save a scan checkpoint, prune deletion fences, or send DELETE; it can list objects and HEAD untimed entries.

`studiofs janitor` logs the duration of metadata open, global lock acquisition, protection metadata reads, initial object listing, fallback HEAD requests, fragment DELETE requests, object work, and the full lock-held cycle. It also logs the listed entry count, peak buffered candidates, scanned count, time to the first listed entry, time to the first orphan candidate, and OpenDAL HEAD/DELETE call counts. These are logical OpenDAL calls; retries can create additional HTTP requests.

## Independent request budgets (SFS-974)

The command accepts `--pg-concurrency` (default 1, range 1–8), `--s3-concurrency` (default 2, range 1–32), and `--s3-requests-per-second` (default 4, range 1–32). The PG pipeline currently issues claim and finish commands serially, so its observed peak is one even when the configured upper bound is higher. S3 concurrency is enforced by OpenDAL's HTTP request layer and includes LIST pagination, stat/HEAD, and each canonical fragment DELETE. Request starts are paced at that same transport boundary.

The provider-neutral pipeline admits no more than one candidate at a time for destructive claim/delete/finish and buffers at most one eligibility page (64 candidates); the rate and transport limits therefore remain effective if provider internals add parallel requests later. Existing deadlines bound listing to 60 seconds and stat/HEAD and DELETE to 20 seconds; PostgreSQL janitor commands use the metadata session's command timeout. These are per-process budgets, not cluster-wide coordination. The mounted VFS janitor is unchanged.

## Retries, backoff and overload (SFS-975)

OpenDAL's `RetryLayer` sits outside the request gate, so each retry attempt passes the in-flight limit and rate pacing again. It retries only temporary errors, at most 3 times per request, with jittered exponential backoff. The request gate is adaptive: an HTTP 429/503 response or a transport failure doubles the spacing between request starts (up to 8 s) and pauses every request for the provider's `Retry-After` or the new spacing (at most 30 s); every other response shrinks the spacing by 1/8 back toward `--s3-requests-per-second`. The cycle summary prints per-cycle deltas for `s3_throttled` and `s3_retries`, and the current `s3_request_interval_ms` (initialized to the configured interval before the first request), plus `head errors` for objects skipped because their age probe failed transiently.

OpenDAL marks every error leaving its retry layer as persistent, so the janitor classifies by error kind: `NotFound` is a vanished key, `PermissionDenied`, `ConfigInvalid`, `Unsupported` and directory-kind errors are terminal, and everything else is transient. A DELETE failure stops that object's remaining key deletes, finishes its claim as retryable, and defers its queue row. Five transiently failed objects in a row stop the cycle as overloaded; `--loop` backs off and gives up after five failed cycles in a row.

The public OpenDAL `Lister` does not expose how many S3 pages it fetched. Production output therefore marks `list_pages=unavailable`. The test fixture below counts actual S3-compatible HTTP list requests. It uses the real OpenDAL S3 operator pointed at a loopback TCP server and never contacts external services.

## Streaming listing (SFS-969)

The CLI consumes the remote listing through OpenDAL `lister_with(prefix).recursive(true)`. Entries are evaluated as pages arrive and grouped into eligibility batches of `MAX_JANITOR_ELIGIBILITY_BATCH` (64). Memory holds one provider page plus one batch, independent of the number of keys. Keys of one object are merged only when adjacent, which matches S3's lexicographic listing order. A file key listed out of order fails the cycle, because merged object age depends on seeing every key of an object together; batches processed before that key keep their results.

Each listing request, including the first, is bounded by a 60-second timeout. A failed or stalled page fails the cycle with the number of entries listed so far: batches processed earlier keep their confirmed deletions, and the partially buffered batch is not acted on. The listing is never treated as proof of deletion safety; PostgreSQL eligibility and claims decide that.

`initial_listing_ms` is the time spent waiting on the listing, not wall time from start to the last page, because processing now overlaps listing.

From the repository root on Windows, run:

    cargo test -p studiofs --bin studiofs janitor_streaming_listing_processes_first_page_before_later_pages -- --nocapture

The fixture serves 20,005 old object keys in 21 1,000-key S3 pages through the real OpenDAL S3 operator on a loopback server, with `max_deletes=2`. Expected stable counts:

- The first DELETE request is sent after only the first list page was fetched.
- 20,005 entries are listed and scanned; peak buffered candidates are 64.
- No HEAD requests are sent, because every listed key carries `LastModified`; two objects are deleted (one request per canonical key).
- The remaining 20,003 candidates are skipped by the cap. The cap does not yet stop the listing early.

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

S3 operators advertising `start_after` resume after the last listed key. An unfinished object group is retained in the checkpoint and all its canonical keys are statted again before deciding age. Other operators use deterministic hexadecimal prefix subdivision; a dense prefix is split down to one object ID, then its bounded canonical key set is statted before advancing. Entry budgets count listing entries, while age probes retain their separate bounded operation deadlines.

`--full-scan` ignores the saved position for the first cycle and saves new progress if bounded. `--reset-scan` clears the saved position before that first cycle. These flags conflict; with `--loop` they apply once, and subsequent cycles resume. `--dry-run` reads progress and applies either flag only in memory: it never saves or resets checkpoints.

Checkpoints bind the volume UUID, endpoint, bucket, normalized data prefix, and listing strategy. A supported scope change starts at the beginning. Missing tables, malformed state, and unknown checkpoint versions fail closed; an older binary cannot reset or overwrite a newer version.

Schema deployment must include the repeatable canonical DDL in `studiofs-meta/src/pg_engine/schema.rs`, including `snapshot_payload_sweep_checkpoints`, before running the command. The CLI performs no runtime DDL. Coordinate this schema ensure step with the shared schema rollout; do not introduce a second migration runner. The checkpoint schema version is independent of the volume format version.
