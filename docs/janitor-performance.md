# CLI janitor performance baseline

`studiofs janitor` logs the duration of metadata open, global lock acquisition, protection metadata reads, initial object listing, candidate HEAD requests, fragment DELETE requests, object work, and the full lock-held cycle. It also logs the materialized listing size, peak listing entries, scanned count, time to the first orphan candidate, and OpenDAL HEAD/DELETE call counts. These are logical OpenDAL calls; retries can create additional HTTP requests.

The public OpenDAL `list_with` result does not expose how many S3 pages it fetched. Production output therefore marks `list_pages=unavailable`. The test fixture below counts actual S3-compatible HTTP list requests. It uses the real OpenDAL S3 operator pointed at a loopback TCP server and never contacts external services.

## Reproduce the eager-listing baseline

From the repository root on Windows, run:

    cargo test -p studiofs --bin studiofs janitor_eager_listing_baseline_reads_all_pages_before_low_delete_cap -- --nocapture

The pre-change checkout was clean at main commit `37c33f3f` on 2026-11-09. Its existing CLI janitor unit tests passed 5/5 in 1.08 seconds. The fixture was then added and run before production instrumentation. On the final instrumented rerun, the unchanged eager-list/delete algorithm took 109 ms to list, 109 ms to its first candidate, and 121 ms for total object work. These timings describe the local fixture only and do not confirm the reported live slowdown.

The fixture provides 2,505 old object keys in three 1,000-key S3 pages and uses `max_deletes=2`. Expected stable counts are:

- All three list pages arrive before the first HEAD request.
- 2,505 entries are materialized and scanned; peak listing entries are 2,505.
- Two candidates receive HEAD requests and two objects are deleted (one request per fragment).
- The remaining 2,503 candidates are skipped after the successful-delete cap is reached.
- The test prints observed elapsed time to the first candidate and total object-work time. Timing values vary by host; counts and ordering are deterministic.

This characterization records current behavior before any future listing or deletion algorithm change. It is not a live S3/PostgreSQL benchmark. The CLI uses PostgreSQL metadata only when the volume format selects that backend. Its standalone path does not use the mounted janitor's per-object claim/finish protocol, so claim and finish timings are not applicable. The mounted background janitor is unchanged.

## Standalone janitor ownership

The standalone command coordinates its phases in `crates/studiofs/src/cli/janitor.rs` and the provider-neutral cycle in `crates/studiofs/src/cli/janitor/pipeline.rs`. `studiofs-data` owns OpenDAL S3-compatible listing, last-modified reads, and fragment deletion; `studiofs-meta` owns the protected-reference snapshot and recovery reconciliation. This split keeps provider calls and metadata safety independently fakeable while preserving the eager listing order and existing protection policy. The mounted janitor remains in `crates/studiofs-vfs/src/vfs/workers.rs` and is not part of this CLI pipeline.

For a real volume, a non-deleting measurement can be started with `studiofs janitor <volume> --dry-run --skip-newer-minutes 1 --max-deletes 1000`. Dry-run still reads metadata and lists/stat-checks remote objects; it suppresses DELETE calls. The recorded test does not run this command or connect to a live service.

## Reading a live run

The command prints phase-start messages before blocking metadata, lock, protection, and eager-list operations, then completion durations when each phase returns. If an eager remote listing is canceled, the start marker is retained but OpenDAL exposes no partial-page progress through this API. `cycle_ms` includes lock acquisition, protection metadata reads, object work, and lock release. `object_work_ms` and HEAD/DELETE durations are nested measurements and must not be added together as independent wall time.
