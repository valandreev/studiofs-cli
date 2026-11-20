# Run the local cache scrub worker on each StudioFS mount

## Purpose / Big Picture

SFS-1119 implements the background worker from ADR 0017 in
`docs/adr/0017-background-local-cache-scrub.md`. A mount scans checksummed
payloads and restores damaged objects through the existing fetch scheduler.
Operators can save the scrub configuration with `studiofs settings` or override
it with `studiofs mount`.

## Progress

- [x] Trace the cache index, existing corruption eviction, and prefetch scheduler.
- [x] Wire the scrub worker, cursor, rate limits, QoS idle notification, and configuration.
- [x] Add focused tests for repair, cursor persistence, rate limits, QoS, and shutdown.
- [x] Pass the required VFS and cache tests, formatting, and Clippy.
- [x] Address PR review findings and validate page pacing, restart, rewrite, and refetch reporting.

## Surprises & Discoveries

A repair fetch can race a cache writer after eviction. The persistent backend
checks for an existing row under the mutation stripe before admitting repaired
bytes.

## Decision Log

Persist the last object ID and pass timestamps in one cache index transaction.
Checkpoint after a 32-row page that verified a payload, or after 60 seconds
of scanning pages without checksums. Save pending progress on cooperative
shutdown and completion time at the end of each pass. A crash can replay
progress after the most recent checkpoint.

Track the task through the existing VFS background task owner. Observe the
foreground semaphore through a notification issued when all permits are
returned. Reserve one operation before each index page read. Reserve bytes and
operations before the optimistic payload read and again before a corruption
recheck. Count refetch errors separately from prefetch admission skips.

## Validation and Acceptance

From the repository root, run `cargo test -p studiofs-vfs cache_scrub`,
`cargo test -p studiofs-cache` and
`cargo clippy -p studiofs-vfs -p studiofs-cache -p studiofs --all-targets -- -D warnings`.
Run `cargo fmt --check` and `git diff --check`. The repair test seeds encrypted
objects into an in-memory object store, damages cache files on disk, and checks
the repaired bytes. Run the existing cache race regression for the mutation
stripe that protects a rewrite from eviction.

## Outcomes & Retrospective

On Windows, the focused VFS run passed 3 tests and the cache run passed 290
tests. Clippy passed for all targets of the three changed crates. The VFS
tests use an in-memory object store. Mounted acceptance and status reporting
are tracked separately from this worker.

The PR review follow-up reproduced the pacing and skip-accounting failures
before the fix. The Windows rerun passed 5 scrub tests, 290 cache tests, and
the focused mount-tuning test. Clippy, formatting, and diff checks passed.
