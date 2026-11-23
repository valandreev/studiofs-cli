# Move Docker-dependent janitor tests to `.env` services and `studiofs-e2e`

This ExecPlan is a living document maintained per `.agent/PLANS.md`.

## Purpose / Big Picture

`cargo test --workspace --all-targets -- --ignored` used to start private Docker
Compose stacks from the `studiofs` janitor unit tests. After this change, those tests
need only the shared test services (`STUDIOFS_TEST_PG_DSN`, `STUDIOFS_S3_*`). Tests that
must control the services themselves run as registered `studiofs-e2e` scenarios.

## Progress

- [x] (2026-11-23) Inventoried every Docker-dependent ignored test: 7 tests, all under
  `crates/studiofs/src/cli/janitor/tests/adversarial_live*`.
- [x] (2026-11-23) Moved five tests to a `LiveStack` that reads `.env`. They are
  `live_cli_disposable_adversarial_protection_cleanup_dry_run_and_budget`,
  `live_cli_disposable_retires_abandoned_upload_but_protects_delayed_publication`,
  `live_cli_disposable_kill_restart_and_signal_drain`,
  `live_cli_disposable_concurrent_snapshot_cuts_wait_for_janitor`, and
  `live_cli_disposable_scan_budget_resumes_checkpoint_across_processes`.
- [x] (2026-11-23) Moved the PostgreSQL restart test to the E2E scenario
  `janitor-postgres-restart-recovery` and the SFS-979 benchmark to the opt-in
  scenario `janitor-live-benchmark`. Removed the `studiofs` dev-dependency on
  `studiofs-e2e`.
- [x] (2026-11-23) Updated docs, the full E2E workflow list, and the runner-contract
  opt-in list.
- [x] (2026-11-23) Reviewed against `main`. The CLI scenarios publish redacted
  logs even when the child times out. The benchmark counts only proxy connections
  and round trips from the measured runs, and its timer excludes log processing.

## Surprises & Discoveries

- Observation: a volume created with `studiofs init` is refused by the janitor with exit
  4 (`snapshot reachability bootstrap is incomplete`). The old synthetic fixture
  inserted the bootstrap row by hand. Evidence: the first scenario run,
  `target/e2e-runs/janitor-pg-restart-1`. The scenarios now run `studiofs fsck <volume>`
  after init. Its PostgreSQL sync completes the bootstrap through the product path,
  without a mount.

## Decision Log

- Decision: the E2E scenarios use a real `studiofs init` volume, not a copy of the
  unit-test fixture. Rationale: there is no duplicated fixture, and the CLI is exercised
  against product-created metadata. Date: 2026-11-23.
- Decision: test names keep the `live_cli_disposable_` prefix. Rationale: existing
  plans and docs select tests by that prefix. Date: 2026-11-23.
- Decision: the benchmark does not restore its `docker update` limits. It is opt-in and
  documented to run alone. Date: 2026-11-23.

## Validation and Acceptance

- `cargo test -p studiofs --bin studiofs live_cli_disposable_ -- --ignored --nocapture --test-threads=1`
  against `.env` services: 5 passed.
- `cargo run -p studiofs-e2e -- run janitor-postgres-restart-recovery --studiofs-bin target/debug/studiofs.exe --infra compose`:
  passed. The outage run exited with 5; after restart one deletion was confirmed and
  the sweep completed.
- `janitor-live-benchmark` with 100 objects, 20 ms/4 ms latency and RPS 32: passed. It
  deleted 50 of 50 orphans, with no live or foreground object missing.
- `cargo test -p studiofs-e2e --test runner_contract`: 40 passed.
  `cargo test -p studiofs --test architecture_vocabulary_test`: 53 passed.
- Environment-gated: the full Linux E2E target was not run locally (Windows host).
- Review validation: the timeout regression failed before the fix and passed with
  both benchmark helper tests. The focused full-target selection contract, E2E
  Clippy, and formatting checks passed. Both individual Compose scenarios passed
  on Windows. The 20-object debug benchmark removed all 11 orphans and preserved
  every referenced object.
- The five shared-service tests passed in 30 seconds against isolated local
  PostgreSQL and RustFS selected through environment overrides. The earlier
  attempt with the workspace `.env` failed to connect to its configured S3
  endpoint.
