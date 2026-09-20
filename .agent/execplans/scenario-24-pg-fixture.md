# Make scenario 24 bootstrap from a PostgreSQL history fixture

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Scenario 24 (`mounted-large-history-cold-bootstrap-convergence`) currently spends
most of its time creating and deleting 45,000 files through the mounted
WinFSP path before it can test PostgreSQL catch-up. After the PostgreSQL sync
path was made faster, that setup workload still exceeded the outer five-minute
scenario-child timeout. The scenario should instead start from a deterministic
PostgreSQL event-log fixture containing the large history and many visible
files, then open the volume with the StudioFS clients. The scenario will still
perform a mounted moving-head workload, restart the cold client during catch-up,
write a final marker, and validate mounted convergence.

The observable result is that the registered scenario completes inside its
runner timeout and its retained summary reports more than one sync page,
catch-up, restart, moving-head convergence, and final mounted-tree convergence.

## Progress

- [x] (2026-09-19 17:30Z) Reproduced the supplied timeout and inspected the
  retained process diagnostics and scenario artifacts.
- [x] (2026-09-19 18:30Z) Fixed the local root-event acknowledgement and the
  repeated local-only pending-prefix projection in the PostgreSQL metadata
  engine; added focused metadata and VFS regressions.
- [x] (2026-09-19 19:35Z) Confirmed the production path no longer falls back to
  the long-history projection, but the mounted 45,000-pair setup remains too
  slow for the five-minute child limit.
- [x] (2026-09-19) Exposed the persisted event encoder and added the
  single-transaction PostgreSQL fixture module.
- [x] (2026-09-19) Reworked the fixture to build a compact serving checkpoint
  by importing one `RedbSnapshot` transaction, then append a bounded replay
  tail. The 4,096-file fixture seeds in about 1.1 seconds in the retained
  native runs instead of about 42.8 seconds with per-event checkpoint replay.
- [x] (2026-09-19) Built the affected binaries and passed the fixture unit
  tests; the metadata backlog and VFS cleanup regressions also passed earlier
  in this working state.
- [x] (2026-09-19) Ran exploratory native fixture-backed scenario-24 attempts
  and retained their artifacts. The warm and cold mounts reached `Live`, but
  the final mounted marker probe timed out and the short-tail runs did not
  produce the required observable `CatchingUp`/restart evidence.
- [x] (2026-09-19) Stopped the latest tail-32 attempt at the requested
  checkpoint and removed only its exact Compose containers/network; its
  artifacts and volumes were retained.
- [x] (2026-09-20) Replaced the timing race with a deterministic PostgreSQL
  replay gate, moved the 10,000-mutation moving head into a second PG fixture
  append, and made the large fixture namespace sharded and metadata-only for
  mounted convergence scans.
- [x] (2026-09-20) Passed the exact native scenario twice with retained runs
  `20260920T-s24-pg-scan60-r14` and `20260920T-s24-pg-repeat-r15`; both
  summaries report catch-up, restart, moving-head, marker, and tree convergence.
- [x] (2026-09-20) Inspected the final diff and ran focused tests and clippy for
  the affected crates. Temporary diagnostic runs remain under `analysis-s24`
  as retained evidence and are intentionally untracked.

## Surprises & Discoveries

- Observation: the original failure was not a PostgreSQL outage or a slow PG
  catch-up; it timed out during `bulk-create-delete:45000`.
  Evidence: the supplied run stopped at `scenario_progress=24/26` after 300
  seconds, with the helper still reporting `bulk-create-delete:45000`.
- Observation: the local redb metadata write itself was fast after the pending
  prefix fix, but the mounted helper still achieved only roughly 120 pairs per
  second on the native WinFSP environment.
  Evidence: the retained native run reached about 37,700 pairs before it was
  stopped, while no repeated PG suffix-classifier fallback warnings appeared.
- Observation: a static Docker PostgreSQL data directory would be brittle
  because each run has a dynamic volume name and the database stores generated
  identifiers. The fixture will seed the current run's volume through the
  persisted event format instead.
- Observation: the event-log `version` is a batch version, not an event-row
  count. The fixture must group many wire events into a bounded number of
  batches so pagination remains observable without requiring one SQL round trip
  per mutation.
- Observation: generating the serving checkpoint by replaying each fixture
  event through the public replica-apply path was still too slow for a setup
  helper. Importing the same deterministic metadata as one `RedbSnapshot`
  transaction and exporting the serving checkpoint reduced fixture generation
  to roughly 1.1 seconds for 4,096 visible files.
- Observation: the compact fixture is sufficient to get both warm and cold
  clients to `Live`, but the retained `...pg-checkpoint-fast` and
  `...pg-checkpoint-pages` runs timed out while probing the final mounted
  marker. Their status artifacts also contain no `CatchingUp` sample, so they
  are not acceptance runs. The latest `...pg-checkpoint-tail32` run was
  interrupted immediately after setup and has no scenario result.
- Observation: a PostgreSQL `ACCESS EXCLUSIVE` lock on `event_log` makes the
  cold client's `CatchingUp` lifecycle deterministic, but ordinary mount
  readiness cannot require a write while that gate is held. A scenario-only
  lifecycle-ready mount probe is needed until the gate is released.
- Observation: the remaining mounted `bulk-create-delete:5000` helper could
  still block for more than five minutes on WinFSP. Appending the logical
  moving-head mutations directly to PostgreSQL preserves the sync race while
  removing the unrelated mounted generation cost.
- Observation: even checksum-free snapshots remain slow when they issue a
  getattr/file-type callback per fixture file. Sharding plus file-only shard
  enumeration validates all 4,096 names while avoiding per-file callbacks.

## Decision Log

- Decision: keep the existing mounted moving-head workload and final marker, but
  replace only the initial 90,000-mutation setup with a PG fixture.
  Rationale: replacing the whole scenario with a database snapshot would stop
  testing mounted writes and ordered publication; replacing only setup removes
  the unrelated WinFSP generation cost while preserving the catch-up contract.
  Date/Author: 2026-09-19 / Codex.
- Decision: supersede the mounted moving-head helper with a second PostgreSQL
  fixture append, while retaining a real mounted final-marker write/read and
  full cross-client namespace-name convergence.
  Rationale: the scenario targets PG checkpoint/tail synchronization; mounted
  generation of 10,000 mutations reproduced the same unrelated WinFSP cost as
  the original 90,000-mutation setup.
  Date/Author: 2026-09-20 / Codex.
- Decision: use two explicit roles (`client-a` writer and `client-d` cold
  observer) and hold an E2E-only table lock until `CatchingUp` is observed.
  Rationale: extra warm clients repeated the same bootstrap without adding a
  sync invariant, while the gate synchronizes on the real replay boundary.
  Date/Author: 2026-09-20 / Codex.
- Decision: generate the fixture at scenario start and insert it in one
  PostgreSQL transaction using `MetaEvent`'s persisted wire encoder.
  Rationale: this is repeatable, fast, compatible with the current schema, and
  avoids committing a platform-specific PostgreSQL data directory or copying
  storage internals into the E2E crate.
  Date/Author: 2026-09-19 / Codex.
- Decision: use non-empty, ready regular-file attributes with no payload graph
  for fixture files.
  Rationale: the remote projection intentionally hides brand-new empty drafts;
  a one-byte ready file is visible to all clients and reads as a deterministic
  hole, so the mounted tree can validate all clients without requiring S3
  payload objects for the bootstrap fixture.
  Date/Author: 2026-09-19 / Codex.
- Decision: use a bounded multi-batch page setting rather than one batch per
  page for the fixture run.
  Rationale: the scenario must prove pagination (`observed_page_count > 1`)
  and expose a real catch-up window. The current exploratory configuration
  uses one batch per page, a 32-batch replay tail, and a bounded 90-second
  mount-ready timeout. This is intentionally slower than the final target and
  remains pending native acceptance.
  Date/Author: 2026-09-19 / Codex.
- Decision: keep the PostgreSQL fixture as event-log rows plus a published
  serving checkpoint rather than copying a PostgreSQL data directory.
  Rationale: the checkpoint is run-specific and wire-compatible with the
  current schema while avoiding dynamic Docker volume names and platform-
  specific database files. It also makes the expensive mounted file creation
  disappear without removing the real client bootstrap path.
  Date/Author: 2026-09-19 / Codex.

## Outcomes & Retrospective

The fixture-backed scenario is complete and passed twice on native WinFSP. The
initial 90,000 logical mutations seed in about 1.2 seconds, the deterministic
gate exposes `CatchingUp`, the cold client restarts, a second PG append moves the
remote head by 10,000 logical mutations, and a mounted marker proves read/write
convergence. The retained passing runs finished in roughly 162 and 181 seconds,
both below the original five-minute timeout, with no namespace differences.

## Context and Orientation

The scenario runner is in
`crates/studiofs-e2e/src/scenario/workloads/advanced_mounted/runner.rs`.
`run_large_history_cold_bootstrap_body` initializes client-a, seeds the
PostgreSQL fixture, discovers and mounts the warm clients, waits for client-a
to apply the fixture, and then discovers/mounts cold client-d. The scenario's
finalization is in `finish.rs`; it scans all mounted trees, so fixture files
must be valid visible metadata entries.

The shared PostgreSQL metadata schema is defined in
`crates/studiofs-meta/src/pg_engine/schema.rs`. The `event_log` table stores
one row per wire event and a `batch_version` shared by rows in the same logical
batch. `metadata_state.version` is the remote head batch version and
`checkpoint_version=0` makes a fresh client replay the event history. The
metadata event wire format is implemented in
`crates/studiofs-meta/src/events.rs`; the E2E fixture must call that encoder so
future wire-format changes remain centralized.

`crates/studiofs-e2e` already depends on `tokio-postgres`, and
`snapshot_v2.rs` demonstrates the repository's current-thread Tokio connection
pattern. The scenario receives the current DSN and volume name through
`ScenarioContext.client_controller_config`. The fixture must never print or
persist the DSN, credentials, or connection error text containing secrets. The
current runner seeds the fixture before warm-client discovery, sets one-batch
catch-up pagination, and gives the cold mount a bounded 90-second readiness
window; those choices are exploratory until the retained status evidence
proves the intended race window.

## Plan of Work

The encoder and fixture module are now implemented. The module builds
deterministic `SetAttr` and `AddEntry` events for one fixture directory and
4,096 one-byte regular files, imports the checkpoint metadata into a temporary
redb snapshot in one transaction, appends 8,192 replay events in 32 batches,
and inserts the tail with `UNNEST` chunks in one PostgreSQL transaction. It
verifies that the volume has an empty history before seeding and returns a
redacted serializable fixture summary.

Update `advanced_mounted.rs` to register and re-export the fixture module.
Update the scenario constants to define the file count, fixture event count,
events per batch, one-batch catch-up pages, and the bounded mount-ready timeout.
In `run_large_history_cold_bootstrap_body`, seed after client-a initialization
but before discovering the warm peers. Wait for client-a's status to show the
remote head applied using bounded observable polling, record a fixture
operation and artifact, and remove the mounted initial bulk helper. Keep the
cold discovery, restart, moving writer, final marker, and existing summary
checks unchanged while the native evidence is investigated.

The planned operation names and fixture-backed runner are updated, and focused
unit tests cover fixture shape and batch grouping. The next implementation step
is evidence-driven: inspect the retained final-marker timeout and status
artifacts, then make the smallest timing/ordering correction needed to expose
catch-up and allow the final mounted probe to converge. Do not add sleeps or
retry loops as a substitute for an observable state condition.

## Concrete Steps

Run all commands from `E:\dev\studiofs-cli`.

1. Add the public encoder and fixture module with `apply_patch`; do not edit
   files with shell redirection. (Completed.)
2. Format and compile the affected crates. (Completed.)

    cargo fmt --all
    cargo check --target-dir E:\dev\studiofs-cli\analysis-s24\target -p studiofs-meta -p studiofs-e2e

3. Run focused tests with nonzero test counts, including the existing metadata
   replication backlog tests, the VFS cleanup regression, and the new fixture
   unit tests. The fixture unit command currently reports `2 passed`.
4. Rebuild the product and E2E binaries before the native scenario. (Completed
   for the current exploratory configuration.)

    cargo build --target-dir E:\dev\studiofs-cli\analysis-s24\target -p studiofs -p studiofs-e2e

5. Run only scenario 24 with the registered runner and retained artifacts after
   the remaining runner correction. The run must use the native mounted
   environment and a fresh run ID; do not run a workspace-wide or full ignored
   test suite. The exploratory runs are `20260919T-e24-pg-checkpoint-fast`,
   `20260919T-e24-pg-checkpoint-pages`, and the interrupted
   `20260919T-e24-pg-checkpoint-tail32`.
6. Inspect `large-history-cold-bootstrap-summary.json`,
   `large-history-status.ndjson`, `operations.ndjson`, and the validation
   artifact. Confirm `observed_page_count > 1`, `saw_catching_up`,
   `restarted_during_catchup`, `moving_head_observed`, and
   `final_read_write_converged` are all true.
7. Remove only the self-created `analysis-s24` scratch directory and temporary
   inspection example after evidence is no longer needed. It is intentionally
   retained at this checkpoint so tomorrow's investigation can start from the
   current artifacts.

## Validation and Acceptance

The metadata backlog test module must report 29 or more passed tests and zero
ignored tests. The focused VFS cleanup regression must pass. The fixture unit
module currently reports 2 passed and the affected E2E/meta crates compile with
the fixture module. These are not the native acceptance gate.

The native scenario is accepted only if its registered `run-one` invocation
returns success, its summary's `matched` validation is true, the final mounted
trees converge, and the retained status shows more than one PostgreSQL page,
`saw_catching_up`, `restarted_during_catchup`, `moving_head_observed`, and
`final_read_write_converged`. No current run meets all of these conditions.
A compile-only result or a test command reporting `running 0 tests` is not
acceptance evidence. If Docker, WinFSP, or the native mount environment is
unavailable, retain the diagnostic artifact and report that gate separately.

## Idempotence and Recovery

Every E2E run uses a fresh run-specific volume name, so generating the fixture
again is safe. The fixture refuses to modify a volume whose `metadata_state`
version is not zero, preventing accidental append/overwrite of an existing
history. If a scenario fails, use its retained artifacts and let the runner
perform its normal client and compose cleanup; do not delete unrelated
`target/e2e-runs` directories. The latest interrupted run's exact containers
and network were removed after verifying their run label; its volumes were not
removed.

## Artifacts and Notes

The fixture summary contains only counts, batch versions, payload sizes, the
logical fixture root, and elapsed time. It does not contain the PostgreSQL DSN
or credential-bearing error text. The final report should cite the exact
focused commands and scenario run IDs, and clearly distinguish completed local
validation from the still-pending native acceptance gate.

## Interfaces and Dependencies

In `crates/studiofs-meta/src/events.rs`, expose:

    pub fn encode_event(event: &MetaEvent) -> MetaResult<Vec<u8>>

In the new E2E fixture module, define a scenario-visible function with the
following behavior:

    pub(in crate::scenario) fn seed_large_history_fixture(
        config: &ClientControllerConfig,
        volume_name: &str,
        fixture_root: &str,
        scenario_dir: &Path,
    ) -> Result<LargeHistoryFixtureSummary, ScenarioError>

Use `tokio_postgres::NoTls`, the existing `ScenarioError`, and the existing
`studiofs_meta::MetaEvent`/schema types. Keep the insertion SQL local to the
fixture module; do not expose PostgreSQL storage internals from the metadata
crate merely for the test.

## Plan Revision Note

(2026-09-19) Created after confirming that the production PG fast path was
fixed but the original mounted history generator still exceeded the outer
five-minute child timeout. The plan deliberately keeps the moving-head mounted
workload and replaces only the setup history with a direct, wire-compatible PG
fixture.

(2026-09-19) Revised after the first fixture-backed native attempts. The
fixture now uses a direct serving checkpoint plus a compact replay tail; seed
time is no longer the bottleneck. The plan remains open because the native
scenario has not yet demonstrated the required catch-up/restart flags and the
final mounted marker probe still times out.
