# SFS-663 PostgreSQL command-pool size implementation plan

This ExecPlan is a living document. It follows `E:\dev\studiofs-cli\.agent\PLANS.md`; the `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` sections must be updated as the work advances.

**Goal:** Allow each StudioFS client to choose its PostgreSQL command-pool capacity at runtime, retaining a default of four connections, while keeping the persisted volume schema unchanged.

**Architecture:** `MetaOpenOptions` carries the local runtime value into `PgOpenOptions`. A single resolver in `crates/studiofs-meta/src/pg_engine.rs` applies the precedence `STUDIOFS_PG_POOL_SIZE` environment override, then the explicit option field, then the default value, and rejects zero or malformed values. `PgConnectionPool` receives the resolved positive value and uses it for both its connection limit and semaphore; the standalone snapshot backend uses the same resolver.

**Tech Stack:** Rust 2024, Cargo, `tokio::sync::Semaphore`, `tokio-postgres`, redb-backed metadata fixtures, ignored PostgreSQL integration tests, and Markdown configuration documentation.

## Purpose / Big Picture

Today every PostgreSQL-backed client is forced to use a four-connection command pool, even when a deployment needs a different per-client budget. After this change, a caller can set `MetaOpenOptions::pg_pool_size` or `PgOpenOptions::pg_pool_size`, and an operator can override that local value with `STUDIOFS_PG_POOL_SIZE`. The effective value is validated before the PostgreSQL pool is created, applies equally to normal metadata engines and standalone `PgSnapshotBackend`, and is never written into `FormatInfo` or `PostgresConfig`.

The behavior is visible through focused unit tests for default/override/validation and an ignored PostgreSQL test that opens a one-slot pool, proves a second command waits, then proves successful repeated commands reuse the pooled connection. A PostgreSQL deployment must budget the sum of all client pool sizes against the server's `max_connections`.

## Progress

- [x] (2026-08-29 09:00 +03:00) Retrieved and read the current SFS-663 Linear contract, including its corrected scope and acceptance criteria.
- [x] (2026-08-29 09:05 +03:00) Confirmed the current branch is `feature/sfs-663-make-postgresql-command-pool-size-a-per-client-runtime` and the worktree is clean.
- [x] (2026-08-29 09:15 +03:00) Mapped `MetaOpenOptions`, `PgOpenOptions`, `PgMetaEngine::open_with_options`, `PgSnapshotBackend::open`, and `PgConnectionPool::new` with codebase-memory and source checks.
- [x] (2026-08-29 09:20 +03:00) Chosen interface and environment precedence: positive `usize` option field; `STUDIOFS_PG_POOL_SIZE` overrides the option; default is four; invalid and zero values return a typed metadata error.
- [x] (2026-08-29 09:17 +03:00) Added failing unit tests for default/explicit/environment pool-size resolution, zero/malformed-value rejection, and zero pool capacity; the focused test command failed for the expected missing API/signature reasons.
- [x] (2026-08-29 09:40 +03:00) Propagated the new field through all existing option literals, added the one-slot capacity regression, and extended connection-reuse assertions.
- [x] (2026-08-29 10:15 +03:00) Updated metadata/configuration documentation and `studiofs init` help; focused unit, ignored PostgreSQL, compile-only, formatting, check, clippy, and diff validation passed.
- [x] (2026-08-29 10:20 +03:00) Committed the complete SFS-663 change set with message `feat: make postgres pool size a client runtime setting`.

## Surprises & Discoveries

- Observation: `PgConnectionPool::new` hardcodes `DEFAULT_MAX_SIZE = 4` twice, once in `max_size` and once in `Semaphore::new`, so changing only a field or only the semaphore would leave the pool inconsistent.
  Evidence: `crates/studiofs-meta/src/pg_engine/runtime.rs:121-138`.
- Observation: `MetaOpenOptions` and `PgOpenOptions` currently carry only `background_poll`; adding a field requires updating every struct literal across metadata, CLI, VFS, and tests.
  Evidence: repository search found literals in `crates/studiofs-meta`, `crates/studiofs`, and `crates/studiofs-vfs`.
- Observation: the existing runtime file has a parse-partial graph range at lines 275-276, outside the pool implementation, so source inspection remains authoritative for that file.
  Evidence: codebase-memory `index_status` reported `crates/studiofs-meta/src/pg_engine/runtime.rs` range `275-276`.
- Observation: connection reuse and application-error discard already have ignored PostgreSQL tests; the new capacity test can extend the existing lifecycle coverage instead of inventing a second fixture system.
  Evidence: `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs:976-1081`.
- Observation: the red test run failed at compilation because `DEFAULT_PG_POOL_SIZE`, `resolve_pg_pool_size`, `PgOpenOptions::pg_pool_size`, and the four-argument fallible pool constructor do not exist yet.
  Evidence: `cargo test -p studiofs-meta pg_pool_size --lib -- --nocapture` exited with code 1 and reported those exact missing symbols/signature errors.
- Observation: the first capacity-test implementation returned a tuple from `PgMetaEngine::run_async`, whose contract requires `MetaResult<T>`; wrapping the tuple in `Ok(...)` restored the intended test boundary.
  Evidence: the focused ignored test compile failed on the future output type, then passed after the bounded checkout test returned `Ok((first_result, second_result, observed_wait))`.

## Decision Log

- Decision: Use `usize` for `pg_pool_size` in both public runtime option structs, validate it at the shared resolver and again at `PgConnectionPool::new`, and return `MetaError::Storage` for invalid values.
  Rationale: existing option structs use simple copyable scalar fields, callers can construct them without `NonZeroUsize` boilerplate, and the pool constructor must remain safe when used directly by internal tests.
  Date/Author: 2026-08-29 / Codex.
- Decision: Give `STUDIOFS_PG_POOL_SIZE` precedence over the explicit option field, and make malformed or zero environment values errors rather than silently falling back.
  Rationale: the issue calls for a documented override and positive validation; silently converting a deployment typo into an unintended connection budget hides an operational configuration error.
  Date/Author: 2026-08-29 / Codex.
- Decision: Keep pool-size policy out of `PostgresConfig`, `MetadataConfig`, and persisted `FormatInfo`.
  Rationale: SFS-663 explicitly defines pool size as per-client runtime policy, while those structures are volume authority configuration.
  Date/Author: 2026-08-29 / Codex.

## Outcomes & Retrospective

The runtime option field and shared resolver now provide a default pool size of
four, apply a positive-integer `STUDIOFS_PG_POOL_SIZE` override, reject zero and
malformed values, and feed the resolved value into both the command-pool limit
and semaphore. `PgMetaEngine` and standalone `PgSnapshotBackend` use the same
resolver, while persisted `PostgresConfig` remains unchanged. The focused unit
tests passed for defaults, explicit values, environment precedence, validation,
snapshot-backend propagation, and zero-capacity construction. The PostgreSQL
capacity regression observed a blocked second checkout with a one-slot pool;
the existing cancellation tests and repeated-query connection-reuse regression
also passed. `STUDIOFS_TEST_PG_DSN` was available, so both ignored PostgreSQL
pool regressions ran; no environment-dependent validation gap remains. No
review follow-up is required within SFS-663 after the invalid environment-value
diagnostic was corrected.

## Context and Orientation

`crates/studiofs-meta/src/lib.rs` exposes `MetaOpenOptions` and the generic `open_meta_engine_with_options` factory used by the CLI. When a local redb `FormatInfo` selects PostgreSQL, that factory constructs `PgOpenOptions` and calls `PgMetaEngine::open_with_options`.

`crates/studiofs-meta/src/pg_engine.rs` owns the PostgreSQL engine and the standalone `PgSnapshotBackend`. Its `PgOpenOptions` controls engine startup. The file already reads per-client `STUDIOFS_PG_*` values through `pg_env_var`, which has a thread-local test override so unit tests do not mutate the process environment.

`crates/studiofs-meta/src/pg_engine/runtime.rs` owns `PgConnectionPool`. A checkout first acquires an owned semaphore permit, then reuses an idle `tokio_postgres::Client` or creates one until `state.total` reaches `max_size`. The existing cancellation and connection-timeout tests prove reserved slots are released; the new capacity test must exercise the same checkout path.

`crates/studiofs-meta/src/pg_engine/tests/mod.rs` contains the shared redb/PG fixtures. `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` contains ignored tests that require `STUDIOFS_TEST_PG_DSN`, including current pool reuse and discard tests.

`crates/studiofs-meta/src/schema.rs` defines persisted `PostgresConfig` with only the DSN and logical volume ID. It must not gain a pool-size field. `docs/metadata.md`, `docs/configuration.md`, and the `init` command's runtime-policy help are the live documentation surfaces for per-client PostgreSQL settings.

## Plan of Work

First add red tests in the metadata crate. The resolver tests will assert default four, environment override precedence over an explicit option, and rejection of zero/malformed values. A direct pool-constructor test will assert that zero cannot create a semaphore. Run those tests before adding the implementation and record the expected missing-function or signature failures.

Then add `DEFAULT_PG_POOL_SIZE = 4`, `pg_pool_size: usize` to both runtime option structs, and a private resolver that reads `STUDIOFS_PG_POOL_SIZE`, parses a positive `usize`, or uses the option value after the same positive check. The resolver must be called by `PgMetaEngine::open_with_options` and `PgSnapshotBackend::open`; `open_meta_engine_with_options` must copy the field from `MetaOpenOptions` into `PgOpenOptions`. `PgConnectionPool::new` will accept the resolved size, reject zero, return `MetaResult<Self>`, and use that value for both `max_size` and `Semaphore::new`. Its test stats will expose the configured maximum so the integration test can verify propagation without adding production observability.

Update every existing `MetaOpenOptions` and `PgOpenOptions` literal with `..Default::default()` or an explicit field where the test needs a non-default size. Keep all existing runtime behavior unchanged for callers that use defaults. Update the shared unit fixture and direct pool constructors for the fallible constructor signature.

Add the ignored lifecycle regression using `pg_pool_size: 1`: start two `with_postgres_client_timeout` operations concurrently, hold the first checkout briefly, and assert the second completes only after the first releases the sole slot and `checkout_waits` increments. Change the reuse test to use an explicit pool size and assert the stats report that capacity while retaining its existing reuse assertions. Keep current cancellation, timeout, and application-error tests intact, adjusting only constructor calls and adding capacity assertions where useful.

Finally document the default, variable name, precedence, invalid-value behavior, and aggregate connection-budget warning. Explicitly state that the setting is client-local and is not persisted in the volume metadata schema. Run formatting, focused unit tests, focused ignored PostgreSQL pool tests when the DSN is available, clippy/checks for affected crates, and `git diff --check`; do not run the workspace-wide suites.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli` in PowerShell.

1. Create the failing tests in `crates/studiofs-meta/src/pg_engine.rs`/`runtime.rs` and run:

    `cargo test -p studiofs-meta pg_pool_size --lib -- --nocapture`

   Expected red result: the new resolver/default/override/zero tests do not compile or fail because the option field/resolver and pool-size constructor parameter do not exist yet. Correct any test-only mistake before implementing production code.

2. Implement the resolver/options/pool plumbing and update all struct literals. Run:

    `cargo fmt -- crates/studiofs-meta/src/lib.rs crates/studiofs-meta/src/pg_engine.rs crates/studiofs-meta/src/pg_engine/runtime.rs`

    `cargo test -p studiofs-meta pg_pool_size --lib -- --nocapture`

   Expected result: all resolver and zero-rejection tests pass, including default four and environment-over-explicit precedence.

3. Run the existing focused metadata unit tests that cover pool lifecycle and adjacent engine behavior:

    `cargo test -p studiofs-meta command_pool --lib -- --nocapture`

    `cargo test -p studiofs-meta --lib --no-run`

   Expected result: the selected tests pass and the metadata test binary compiles with no missing option fields or constructor errors.

4. When `STUDIOFS_TEST_PG_DSN` is configured, run the exact ignored pool regressions sequentially:

    `cargo test -p studiofs-meta command_pool_capacity_is_bounded_by_configured_pool_size --lib -- --ignored --nocapture --test-threads=1`

    `cargo test -p studiofs-meta command_pool_reuses_connection_for_repeated_remote_metadata_queries --lib -- --ignored --nocapture --test-threads=1`

   Expected result: the capacity test observes a one-slot pool and at least one checkout wait; the reuse test observes two successful checkouts, no discard, and at most one new connection. If the DSN is unavailable, the command is not counted as passing evidence and the gap is reported.

5. Validate the affected code and documentation:

    `cargo check -p studiofs-meta -p studiofs`

    `cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings`

    `git diff --check`

   Expected result: exit code zero for each command. No full workspace test suite is part of this task.

6. Review the final diff, update this plan's progress/outcomes with actual test counts and any unavailable environment checks, then commit all SFS-663 files with an English message such as `feat: make postgres pool size a client runtime setting`.

## Validation and Acceptance

The change is accepted when a fresh unit run proves `MetaOpenOptions::default()` and `PgOpenOptions::default()` resolve to four, a valid `STUDIOFS_PG_POOL_SIZE` overrides an explicit option, zero and malformed values return errors, and the pool constructor cannot create a zero-capacity pool.

The metadata engine and standalone snapshot backend must both pass the same resolved positive size to the command pool. The ignored one-slot PostgreSQL test must show observable checkout blocking without sleeps used as synchronization; it should hold a real checkout, use bounded async timing only, and assert the pool's wait counter. The existing reuse test must continue to prove serial commands reuse a client.

The persisted schema acceptance is source-level: `PostgresConfig` remains limited to URL and volume ID, and the new setting appears only in runtime options/resolution and documentation. Documentation must state `STUDIOFS_PG_POOL_SIZE`, default `4`, precedence over the option field, rejection of non-positive/malformed values, and that total pool sizes across clients consume the PostgreSQL connection budget.

## Idempotence and Recovery

The source edits are additive and safe to repeat after checking the exact field names and current literals. Tests use existing target-scoped temporary directories and the existing thread-local environment override; no repository `.env` or persisted metadata is modified by unit tests. Ignored PostgreSQL fixtures use unique names and existing fixture cleanup. If a focused command fails because a Windows process locks `target`, rerun it with the repository's isolated target guidance after closing the holder; do not delete broad directories or reset the worktree.

## Artifacts and Notes

The primary artifacts are the living plan at `docs/superpowers/plans/2026-08-29-sfs-663-postgresql-pool-size.md`, the runtime-option/pool implementation, focused metadata tests, and updated PostgreSQL configuration documentation. The final notes must include the actual test commands and concise pass/skip evidence rather than claiming a PostgreSQL-backed test passed when no DSN was available.

## Interfaces and Dependencies

At completion, `crates/studiofs-meta/src/lib.rs` must expose:

    pub struct MetaOpenOptions {
        pub background_poll: bool,
        pub pg_pool_size: usize,
    }

`crates/studiofs-meta/src/pg_engine.rs` must expose the same runtime field through:

    pub struct PgOpenOptions {
        pub background_poll: bool,
        pub pg_pool_size: usize,
    }

The default for both structs is `pg_pool_size = 4`. The private resolver in `pg_engine.rs` must accept the explicit `usize` and return `MetaResult<usize>`, applying `STUDIOFS_PG_POOL_SIZE` first and rejecting values that are not positive integers. `PgConnectionPool::new` in `runtime.rs` must accept `max_size: usize`, return `MetaResult<Self>`, reject zero, set `max_size` to that value, and initialize `active_slots` with `Semaphore::new(max_size)`.

`PgMetaEngine::open_with_options` and `PgSnapshotBackend::open` remain the existing public entry points; neither adds pool size to persisted `PostgresConfig`. The implementation continues to use `tokio-postgres` clients and the existing `tokio` runtime, with no new dependency.

## Revision Note

2026-08-29 09:20 +03:00 — Created this living ExecPlan after reading SFS-663, current source, graph coverage, existing pool tests, and repository test/documentation guidance. The plan records the explicit option field, environment precedence, fail-fast validation, unchanged persisted schema, and focused validation boundary.
2026-08-29 09:17 +03:00 — Added the first red tests. The expected compile failures confirmed the new option field, resolver, default constant, and fallible pool-constructor contract are not present in the baseline.
2026-08-29 10:15 +03:00 — Completed implementation and focused verification. Documentation, CLI help, option propagation, resolver tests, pool-capacity blocking, cancellation, and connection reuse all have recorded passing evidence; the complete change set is ready to commit.
2026-08-29 10:20 +03:00 — Committed the complete change set with message `feat: make postgres pool size a client runtime setting`.
2026-08-29 — Verified the attached code review against current callers and contracts. Corrected the zero-value environment error to identify STUDIOFS_PG_POOL_SIZE; deferred options-API, type, builder, and polling suggestions as out of scope or contrary to the existing concurrency test contract.
