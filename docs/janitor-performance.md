# CLI janitor performance baseline

`studiofs janitor` logs the duration of metadata open, global lock acquisition, protection metadata reads, initial object listing, fallback HEAD requests, fragment DELETE requests, object work, and the full lock-held cycle. It also logs the listed entry count, peak buffered candidates, scanned count, time to the first listed entry, time to the first orphan candidate, and OpenDAL HEAD/DELETE call counts. These are logical OpenDAL calls; retries can create additional HTTP requests.

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
