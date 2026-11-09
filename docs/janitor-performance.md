# CLI janitor performance baseline

`studiofs janitor` logs the duration of metadata open, global lock acquisition, protection metadata reads, initial object listing, candidate HEAD requests, fragment DELETE requests, object work, and the full lock-held cycle. It also logs the listed entry count, peak buffered candidates, scanned count, time to the first listed entry, time to the first orphan candidate, and OpenDAL HEAD/DELETE call counts. These are logical OpenDAL calls; retries can create additional HTTP requests.

The public OpenDAL `Lister` does not expose how many S3 pages it fetched. Production output therefore marks `list_pages=unavailable`. The test fixture below counts actual S3-compatible HTTP list requests. It uses the real OpenDAL S3 operator pointed at a loopback TCP server and never contacts external services.

## Streaming listing (SFS-969)

The CLI consumes the remote listing through OpenDAL `lister_with(prefix).recursive(true)`. Entries are evaluated as pages arrive and grouped into eligibility batches of `MAX_JANITOR_ELIGIBILITY_BATCH` (64). Memory holds one provider page plus one batch, independent of the number of keys. Duplicate keys of one object are collapsed only when adjacent, which matches S3's lexicographic listing order; a provider that returns an object's keys apart re-checks that object, which stays safe because claims and `NotFound` deletes are idempotent.

Each listing step is bounded by a 60-second timeout. A failed or stalled page fails the cycle: batches processed earlier keep their confirmed deletions, and the partially buffered batch is not acted on. The listing is never treated as proof of deletion safety; PostgreSQL eligibility and claims decide that.

`initial_listing_ms` is the time spent waiting on the listing, not wall time from start to the last page, because processing now overlaps listing.

From the repository root on Windows, run:

    cargo test -p studiofs --bin studiofs janitor_streaming_listing_processes_first_page_before_later_pages -- --nocapture

The fixture serves 20,005 old object keys in 21 1,000-key S3 pages through the real OpenDAL S3 operator on a loopback server, with `max_deletes=2`. Expected stable counts:

- The first HEAD request is sent after only the first list page was fetched.
- 20,005 entries are listed and scanned; peak buffered candidates are 64.
- Two candidates receive HEAD requests and two objects are deleted (one request per canonical key).
- The remaining 20,003 candidates are skipped by the cap. The cap does not yet stop the listing early.

The earlier eager baseline (SFS-964, main commit `37c33f3f`) fetched all three pages of a 2,505-key fixture before the first HEAD and materialized all 2,505 entries.

## Standalone janitor ownership

The standalone command coordinates its phases in `crates/studiofs/src/cli/janitor.rs` and the provider-neutral cycle in `crates/studiofs/src/cli/janitor/pipeline.rs`. `studiofs-data` owns OpenDAL S3-compatible listing, last-modified reads, and fragment deletion; `studiofs-meta` owns the protected-reference snapshot and recovery reconciliation. This split keeps provider calls and metadata safety independently fakeable while preserving the existing protection policy. The mounted janitor remains in `crates/studiofs-vfs/src/vfs/workers.rs` and is not part of this CLI pipeline.

For a real volume, a non-deleting measurement can be started with `studiofs janitor <volume> --dry-run --skip-newer-minutes 1 --max-deletes 1000`. Dry-run still reads metadata and lists/stat-checks remote objects; it suppresses DELETE calls. The recorded test does not run this command or connect to a live service.

## Reading a live run

The command prints phase-start messages before blocking metadata, lock, protection, and listing operations, then completion durations when each phase returns. The listing also prints `first_entry` once the first page delivers an entry; OpenDAL exposes no per-page boundaries through this API. `cycle_ms` includes lock acquisition, protection metadata reads, object work, and lock release. `object_work_ms` and HEAD/DELETE durations are nested measurements and must not be added together as independent wall time.
