# Report cache scrub health and prove mounted repair

## Purpose / Big Picture

SFS-1120 exposes progress and repair counters from the background cache scrub
defined in ADR 0017. Operators can see the last completed pass in status.
The mounted scenario verifies automatic restoration before any mounted read
can repopulate the cache.

## Progress

- [x] Trace the scrub worker, diagnostics v5, status, and mounted runner.
- [x] Add counters and pass progress, v5 compatibility, and status output.
- [x] Register the mounted scenario in the runner and Linux CI, and document the gate.
- [x] Run mounted WinFSP and Linux FUSE acceptance on rebuilt binaries.
- [x] Finish focused Windows checks.

## Surprises & Discoveries

SFS-1119 already logs corruption and refetch failure. Its cursor clears the
completion marker at the start of the next pass, so the most recent completion
is also retained in the same transaction for diagnostics.

## Decision Log

Keep counters mount-local and stage progress in the worker. Keep the existing
refetch failure counter on the previous metrics surface for consumers of that
field. The mounted scenario stops S3 for an offline pass over a cache with
an unpublished payload, then restores S3 before damaging uploaded objects.

## Validation and Acceptance

Run the background local cache scrub gate in
`docs/local-first-validation-matrix.md`. Rebuild `studiofs` and
`studiofs-e2e` before mounted runs and retain their artifacts under `target/`.

## Outcomes & Retrospective

The WinFSP and WSL Linux FUSE runs each reported one checksum, one missing,
and one size finding, three successful refetches, and zero refetch failures.
Both runs preserved the unpublished payload across an offline pass. The
reports are retained under `target/` as `sfs-1120-winfsp-final` and
`sfs-1120-linux-final`. The macFUSE gate needs a macOS host with macFUSE.
On Windows, all 15 focused tests passed. The four changed crates passed
`cargo check` and Clippy on all targets with warnings denied. Formatting
and the diff check passed.
