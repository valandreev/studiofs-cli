# Refactor the CLI janitor into testable phases

This ExecPlan is a living document governed by `.agent/PLANS.md`.

## Purpose / Big Picture

After this change, the standalone `studiofs janitor` command still observes and deletes exactly the same objects, while its argument conversion, object discovery, metadata protection, and deletion decisions can be tested independently with fakes. S3-specific OpenDAL calls live behind `studiofs-data`; metadata safety snapshots live behind `studiofs-meta`. The mounted background janitor in `crates/studiofs-vfs/src/vfs/workers.rs` is out of scope and remains unchanged.

## Progress

- [x] (2026-11-09) Confirmed clean `main`, read task contracts, traced the CLI cycle and metadata safety calls, and switched to `feature/sfs-965-refactor-cli-janitor-into-a-testable`.
- [x] (2026-11-09) Implemented the provider-neutral CLI pipeline and data/meta seams while retaining eager listing, instrumentation, protection order, and deletion behavior.
- [x] (2026-11-09) Added fake-driven pipeline/protection tests and ran focused Windows CLI/VFS checks; recorded evidence below.

## Surprises & Discoveries

- Observation: `run_one_cycle` currently combines eager listing, object parsing, protection checks, age checks, HEAD, and fragment deletes in `crates/studiofs/src/cli/janitor.rs`.
  Evidence: the existing 2,505-object eager-listing test calls this function directly and asserts all pages arrive before HEAD.
- Observation: the current metadata protection sequence is pins, optional pending-recovery reconciliation for non-dry-run, current references, then pending timestamps, all under the CLI-acquired global janitor lock.
  Evidence: `run` at `crates/studiofs/src/cli/janitor.rs` lines 533-550.
- Observation: object work remains ordered and single-pass after the eager listing returns; no second candidate vector is needed.
  Evidence: `pipeline.rs` calls `discover_object`, `is_protected`, then `process_orphan` for each listed entry.

## Decision Log

- Decision: Keep the provider-neutral cycle and its test seam in `crates/studiofs/src/cli/janitor/pipeline.rs`; expose S3-compatible configuration/list/stat/delete operations through `studiofs-data` and safety snapshot collection through `studiofs-meta`.
  Rationale: this avoids making the data adapter own application orchestration or adding a new crate, while keeping OpenDAL and metadata backend details in their owning crates.
  Date/Author: 2026-11-09 / Codex.

## Outcomes & Retrospective

The CLI now separates command parsing/setup from the cycle pipeline, with S3 OpenDAL operations in `studiofs-data` and metadata protection/recovery reads in `studiofs-meta`. Existing eager-listing, dry-run, local metadata, and cache-inspection CLI tests pass unchanged; fake tests cover protected IDs and listing/HEAD/delete failures including NotFound fragment handling. Targeted mounted VFS janitor regression coverage passed; `vfs/workers.rs` was not modified. No streaming listing, new safety policy, full suite, or live service-backed run was included.

## Context and Orientation

The CLI is the application coordinator: `JanitorArgs` converts command-line values, `run` opens metadata and owns the global lock and cycle loop, and `run_one_cycle` currently performs all object work. `studiofs-data` owns payload object key layout and OpenDAL storage operations. `studiofs-meta` owns current references, global snapshot pins, pending timestamps, recovery reconciliation, and global lock operations. The refactor must preserve the eager `list_with(...).recursive(true)` baseline recorded in `docs/janitor-performance.md`; streaming changes and new safety policy are separate follow-ups.

## Plan of Work

Extract the object-cycle decision flow into a provider-neutral CLI pipeline interface and adapt the real OpenDAL implementation through `studiofs-data`. Extract metadata safety collection into a `studiofs-meta` capability/helper while leaving lock acquisition and release at the existing CLI boundary. Keep the CLI responsible for arguments, banners, metrics presentation, and looping. Add fake-backed tests for arguments, listing/discovery, metadata protection, and deletion outcomes. Do not edit `workers.rs`.

## Concrete Steps

From `E:\dev\studiofs-cli`, the completed focused validation was:

    cargo fmt --all -- --check
    git diff --check
    cargo check -p studiofs -p studiofs-data -p studiofs-meta
    cargo clippy -p studiofs-data -p studiofs-meta -p studiofs --all-targets -- -D warnings
    cargo test -p studiofs --bin studiofs janitor_ -- --nocapture
    cargo test -p studiofs --bin studiofs fake_store -- --nocapture
    cargo test -p studiofs-meta janitor::tests --lib
    cargo test -p studiofs-vfs janitor_deletion_queue_bounds_parallelism_and_caps_only_successes --lib

## Validation and Acceptance

Acceptance requires the existing eager-list baseline and dry-run behavior to pass, fake tests to show protected IDs never reach age checks or deletion, delete failures and NotFound fragments to retain current behavior, metadata collection failures to prevent object work, and Windows CLI/VFS targeted tests to compile and pass. These checks do not prove mounted WinFSP/FUSE behavior.

## Idempotence and Recovery

The refactor is additive/movable code only; preserve unrelated worktree state. The current global janitor lock remains released after either success or protection/object-cycle failure.

## Artifacts and Notes

No S3 pagination, safety-policy, mounted-worker, or SFS-964 instrumentation changes are planned.

## Interfaces and Dependencies

Use existing workspace dependencies only. Keep all cross-phase interfaces private to the CLI unless the data or metadata adapter needs a small public operation to own its provider/backend-specific I/O.
