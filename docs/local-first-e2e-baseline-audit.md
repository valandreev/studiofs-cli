# Local First E2E Baseline Audit

## Starting Point

This audit treats commit `f4e5e831252f265b16bba569dc7b8f274645d26e` as the
accepted E2E baseline starting point. Later changes may exist on the current
branch, but this document classifies the accepted Contract/Core and real E2E
work before more mounted scenarios are added.

Reviewed Linear scope:

- SFS-309 through SFS-316: Contract/Core harness, namespace integrity,
  chunk/slice resolution, Local First durability boundaries, upload replay,
  publication visibility, metadata authority/headroom, and reachability/GC.
- SFS-317 through SFS-329: client-root isolation, real E2E runner,
  Docker-backed infrastructure, multi-client control, bootstrap/discovery,
  mounted-tree validation, artifacts, smoke, outage, crash/restart, collision,
  and manual RWX E2E workflow.

Artifact-backed repository checks:

- `docs/juicefs-local-first-rust-contract.md` defines `flush`, `fsync`, and
  last-handle close as local boundaries, not peer visibility boundaries.
- `docs/e2e-runner-contract.md` keeps mounted validation on public mount trees
  and keeps scenario orchestration outside VFS, metadata, cache, FUSE, and
  WinFSP internals.
- `docker/compose.e2e.yml` uses PostgreSQL 18 via `postgres:18-alpine` and the
  floating RustFS image `rustfs/rustfs:latest`.

## Local First contract conflicts

No accepted Contract/Core issue in SFS-309 through SFS-316 conflicts with the
current Local First model. Those issues distinguish active writer state,
locally durable state, uploaded payloads, published metadata, and peer-visible
state.

Potential conflict area in the accepted real E2E set:

- SFS-327 crash/restart wording must be read as recovery of data that crossed a
  documented local durability boundary. It must not require preserving bytes
  that existed only in volatile OS cache after an unclean crash.
- SFS-325 and SFS-326 must keep temporary peer invisibility during PostgreSQL or
  RustFS/S3 outage as expected behavior until publication and convergence are
  possible.
- SFS-324, SFS-325, SFS-326, SFS-327, and SFS-328 must not turn every close or
  remount into a forced remote publication boundary.

## Failure Classification

Product bugs:

- Local writes that crossed the documented local durability boundary are not
  readable after restart.
- Published versions reference payload objects that are not durable in object
  storage.
- Peers converge to internally inconsistent namespace state, duplicate paths,
  broken parents, stale payloads, or resurrected deleted content.

Harness bugs:

- A scenario inspects private VFS, cache, redb, PostgreSQL, or S3 internals to
  decide pass/fail instead of validating mounted paths and public artifacts.
- Multiple clients share one local metadata database or one
  `STUDIOFS_CLIENT_ROOT`.
- Cleanup/reporting hides the primary scenario failure behind teardown noise.

Environment drift:

- PostgreSQL image drift away from PostgreSQL 18 changes startup, healthcheck,
  or SQL behavior.
- RustFS `latest` drift can change S3 compatibility or healthcheck behavior and
  should be classified as environment drift before it is treated as a product
  regression.
- Docker, FUSE, macFUSE, WinFSP, mount-manager, or Windows process cleanup
  assumptions differ from the documented preflight gates.

Incorrect test expectations:

- Requiring peer visibility while PostgreSQL is stopped.
- Requiring remote fetches while RustFS/S3 is stopped.
- Requiring remote durability or publication from ordinary close/remount.
- Requiring crash recovery for bytes that never crossed the documented local
  durability boundary and only lived in volatile OS cache.

## Retained Lessons

Runner zombie-process cleanup and stale resource recovery are retained lessons,
not canceled investigation noise. Real E2E cleanup must continue to:

- record child process IDs in the manifest when known;
- unmount before deleting mount directories or explicit client roots;
- kill only recorded child processes or direct descendants;
- recover runner-owned stale mount locks and run-owned resources;
- preserve the primary scenario failure in `report.json` while recording
  cleanup errors in `cleanup.json`;
- treat unknown mounted paths, drive letters, and process ownership as
  preflight or cleanup failures instead of force-cleaning them.

Windows process cleanup remains a first-class runner contract because mounted
WinFSP scenarios can leave foreground `studiofs.exe` mount processes, helpers,
or descendants behind when a scenario times out or is interrupted.

## Environment Assumptions

The accepted baseline depends on:

- PostgreSQL 18, represented by `postgres:18-alpine` in
  `docker/compose.e2e.yml`;
- RustFS image policy, currently `rustfs/rustfs:latest`;
- Docker Compose health waiting and project-scoped teardown with volumes
  removed;
- Linux FUSE or macFUSE support for Unix mounted runs;
- WinFSP installed and loadable for Windows mounted runs;
- safe cleanup of only runner-owned sandboxes, explicit client roots, mount
  points, containers, networks, volumes, and child processes.

## Follow-up rewrites

Before rerunning broad Windows mounted sweeps, rewrite scenario contracts where
needed so they state the Local First expectation directly:

- SFS-403: PostgreSQL outage must validate writer recovery first, then
  publication after PostgreSQL returns.
- SFS-404: crash/restart must validate recovered writer checksum after a local
  durability boundary, not bytes that only lived in volatile OS cache.
- SFS-406: runner diagnostics should stay minimal and preserve the first
  failure while the Local First baseline is being stabilized.
- SFS-407: deterministic simulation should be replanned from accepted Local
  First contracts rather than from canceled post-baseline assumptions.

The next implementation issues can proceed without reopening the canceled
post-baseline plan as long as they keep product bugs, harness bugs,
environment drift, and incorrect test expectations separated.
