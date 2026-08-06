# SFS-642: PostgreSQL lifecycle ownership and schema provisioning

This is a living ExecPlan for SFS-642. It records the accepted design, the
implementation sequence, validation evidence, and any deviations discovered
while implementing the change. Update `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` at each meaningful milestone.

## Purpose and big picture

Make PostgreSQL schema ownership and background-worker lifetime explicit in the
shared metadata engine.

Today `PgMetaEngine` can create the `file_lock_leases` table from a foreground
file-lock operation, while `studiofs init` creates the rest of the PostgreSQL
schema. This makes startup permissions and operational behavior implicit: a
foreground lock can unexpectedly issue DDL, and an existing volume with a
missing table is repaired only when that path is exercised. The engine also
stores four worker `JoinHandle`s and joins them through a timeout helper that
can leave a second helper thread waiting on the real worker after the timeout.

After this change:

* `studiofs init` is the only application path that provisions PostgreSQL
  schema, including `file_lock_leases`.
* `PgMetaEngine` performs a read-only startup validation of the required tables
  and fails clearly when provisioning is incomplete.
* Foreground lock operations execute DML only; they do not create or alter
  tables.
* A private `PgBackgroundWorkers` value owns worker handles, cancellation
  state, and wake-up state. Normal shutdown cancels, wakes, and waits on the
  actual worker handles under one deadline. Repeated shutdown is safe, and
  `Drop` only performs best-effort non-blocking signalling.
* The shared `MetaReplication::prepare_for_close` contract reports the first
  meaningful shutdown error to its caller. `StudioVfs::shutdown` records that
  error while continuing its existing local durability barrier.
* Lock-worker laziness and the shared cross-platform behavior remain
  unchanged; no platform-specific schema or lifecycle branch is introduced.

## Progress

- [x] 2026-08-06: Read SFS-642, its completed SFS-641 dependency, repository
  contracts, current lifecycle/schema code, and affected tests.
- [x] 2026-08-06: Compared the minimal canonical-init design with adding a new
  migration command and with retaining runtime auto-creation.
- [x] 2026-08-06: User approved the minimal design: canonical `studiofs init`,
  startup validation, DML-only foreground lock paths, and explicit worker
  ownership.
- [ ] Add regression tests first and observe their expected failures.
- [ ] Implement schema ownership and startup validation.
- [ ] Implement owned worker shutdown and update the shared close contract.
- [ ] Run focused tests, compile/lint checks, and available infrastructure
  validation.
- [ ] Record final evidence and update SFS-642 in Linear in English.

## Surprises and discoveries

The following facts were verified before implementation:

* `PgMetaEngine` currently stores four optional worker handles directly:
  PostgreSQL polling, metadata notification, lock notification, and lock
  renewal. Cancellation and wake-up fields are separate from those handles.
* `prepare_for_close` currently cancels workers and joins each handle through
  `join_background_thread_with_timeout`. That helper spawns another thread to
  call `JoinHandle::join`, so a timeout returns while the real worker may still
  be joined asynchronously.
* `acquire_file_lock_lease`, `release_file_lock_lease`,
  `renew_file_lock_lease`, `current_file_lock_lease`, and monitoring setup all
  pass through the runtime schema-ensure helper before their DML or worker
  setup.
* `studiofs init` provisions `volumes`, `counters`, `event_log`,
  `metadata_state`, `acl_rules`, and `snapshots`, but not `file_lock_leases`.
* An existing ignored PostgreSQL test currently expects opening an engine and
  acquiring a lock to recreate a dropped `file_lock_leases` table. That test
  must become a startup-validation regression test.
* The local redb store remains the foreground metadata durability store. The
  PostgreSQL boundary is still used for bootstrap, replication/checkpoint,
  and shared file-lock authority; this task does not move foreground metadata
  writes to PostgreSQL.
* The current runner is macOS with macFUSE available, but `/dev/fuse` and the
  MinGW `x86_64-w64-mingw32-gcc` compiler are unavailable. The test PostgreSQL
  DSN is not present in the environment observed during planning. Those gaps
  must be reported rather than treated as passing evidence.

## Decision log

1. **Use `studiofs init` as the sole schema owner.** This is the smallest
   change that makes provisioning explicit because the CLI already owns the
   canonical schema transaction. Add `file_lock_leases` there with
   idempotent DDL.
2. **Validate required tables during PostgreSQL engine startup.** Validation
   is read-only and happens before normal background activity or foreground
   lock use. A missing table is an operational provisioning error, not a
   reason for the runtime engine to repair schema implicitly.
3. **Do not add a migration command in SFS-642.** Existing deployments need an
   explicit provisioning/migration operation, but a new command would expand
   the issue beyond the accepted ownership boundary. `studiofs init` remains
   the available provisioning path; a dedicated migration workflow can be a
   separate issue if needed.
4. **Remove runtime schema creation from foreground lock paths.** The
   correctness contract is DML-only operation after startup validation. This
   also allows a PostgreSQL role used for foreground work to lack DDL
   privileges.
5. **Introduce one private worker owner rather than new public traits.** The
   requested lifecycle boundary is internal to `PgMetaEngine`; a private
   `PgBackgroundWorkers` value can own handles and cancellation without
   widening visibility or adding an abstraction with one implementation.
6. **Wait on the real handles under one overall deadline.** Cancellation and
   wake-up are performed once, then each owned handle is joined with the
   remaining deadline. A timed-out worker is reported, not hidden behind a
   detached join helper. The exact existing join policy and error wording are
   retained where they remain compatible with the new owner.
7. **Keep `Drop` best-effort and non-blocking.** Destructors cannot return
   errors and must not wait indefinitely. `Drop` signals cancellation and
   wake-up; explicit shutdown is the path that observes join failures and
   timeout errors.
8. **Make close idempotence explicit.** The owner records enough terminal
   state to make repeated explicit shutdown calls safe. Since `MetaError` is
   not cloneable, stored shutdown diagnostics will be replayed as new
   `MetaError::Storage` values rather than storing an error object for reuse.
9. **Preserve lazy lock-worker start.** Opening and passive lock monitoring
   must not start the renewal worker early. Ownership changes handle storage
   and shutdown only; it does not change lock-worker activation semantics.
10. **Return a close result through the shared trait.**
    `MetaReplication::prepare_for_close` will return the metadata result type,
    with the default implementation remaining successful. `StudioVfs` will
    include the result in its existing shutdown error combination while still
    attempting the local flush and other cleanup.

## Context and orientation

The relevant flow is:

1. `studiofs/src/cli/init.rs::init_pg_schema_and_volume` opens a PostgreSQL
   transaction and executes `init_pg_schema_sql`. This is the canonical schema
   provisioning boundary.
2. `studiofs-meta/src/lib.rs::open_meta_engine_with_options` selects
   `PgMetaEngine::open_with_options` for PostgreSQL volumes.
3. `PgMetaEngine::open_with_options` opens local redb, reads configuration,
   bootstraps/catches up PostgreSQL state, and may start the poll and metadata
   notification workers. Startup table validation belongs here and must be
   read-only.
4. `studiofs-meta/src/pg_engine/engine_core.rs` contains file-lock DML and
   currently calls the schema-ensure helper before those operations. Those
   calls must be removed or replaced with no-op-free validation assumptions;
   no foreground DDL helper remains.
5. `studiofs-meta/src/pg_engine/lock_workers.rs` lazily starts lock
   notification and renewal workers. Their handles must be registered with the
   single owner without changing the lazy start contract.
6. `studiofs-meta/src/pg_engine/replication_api.rs::prepare_for_close`
   currently performs the cancellation and joins. It becomes the adapter for
   the owned-worker shutdown result and local redb close barrier.
7. `studiofs-vfs/src/vfs/upload_lifecycle.rs::StudioVfs::shutdown` calls the
   shared close hook after VFS workers stop. It must propagate/aggregate the
   new metadata close result without skipping its existing local sync path.

The required PostgreSQL tables used by the runtime are `volumes`, `counters`,
`event_log`, `metadata_state`, `acl_rules`, `snapshots`, and
`file_lock_leases`. The CLI also provisions `volume_crypto_keys` for volume
encryption metadata; it remains part of the canonical init schema even though
it is not a runtime `studiofs-meta` lock/replication table.

## Plan of work

### 1. Add red tests before production changes

Write the smallest tests that express the accepted contracts:

* A fresh init schema contains `file_lock_leases`.
* A PostgreSQL engine opening against a volume with the table removed fails at
  startup with a clear schema/provisioning error, before lock DML can repair
  it.
* A foreground lock operation does not attempt `CREATE TABLE` or `ALTER TABLE`.
  Prefer a deterministic restricted-role or SQL-observation test already
  supported by the fixture; do not assert absence from scheduler timing.
* Repeated explicit close is safe and does not re-join or restart workers.
* A close timeout applies to the actual worker handle and leaves the owner in a
  defined terminal state; it does not create a detached helper join path.
* Dropping an engine signals its workers, and a subsequent open does not
  inherit live workers from the dropped engine. Use deterministic state
  checkpoints rather than sleeps as synchronization where possible.

Run each new focused test individually and capture the expected failure before
editing production code. PostgreSQL tests remain ignored/infrastructure-backed
when they require `STUDIOFS_TEST_PG_DSN`.

### 2. Make schema ownership explicit

Update `init_pg_schema_sql` with the existing `FILE_LOCK_LEASES_SCHEMA_SQL`
contents, preserving idempotence and transaction behavior. Make the init test
assert the new table as part of the canonical schema.

Add a private startup validator for the required runtime tables. It should use
the existing PostgreSQL client/runtime and a read-only catalog query, return a
specific `MetaError` naming the missing table(s), and run from
`open_with_options` before the engine can be used. Do not invoke
`batch_execute`, `CREATE`, `ALTER`, or `DROP` from the validator.

Delete the runtime schema-creation and retry bookkeeping that only existed to
support foreground auto-repair. Remove the calls from all lock/monitoring
foreground paths, then retain only the DML statements and worker-start logic.

Keep test fixture setup able to create the complete schema for isolated tests;
fixture DDL is test provisioning, not runtime schema ownership.

### 3. Introduce owned worker lifecycle

Move the four optional worker handles and their related cancellation/wake state
behind a private `PgBackgroundWorkers` value. The owner must support:

* registration of lazily created lock-worker handles;
* one cancellation/wake operation shared by all workers;
* explicit shutdown that cancels, wakes, and joins the actual handles using one
  overall deadline;
* first meaningful error retention for repeated explicit shutdown calls;
* non-blocking `Drop` signalling only.

Preserve the existing worker loop stop conditions and thread names unless a
minimal change is required to register ownership. Remove the helper thread
based timeout join mechanism once the owner joins the actual handles.

### 4. Update the close contract and caller

Change `MetaReplication::prepare_for_close` to return the established metadata
result type, with the default implementation returning success. Update the
PostgreSQL implementation to combine the local redb durability barrier with
the worker-owner result in the existing order.

Update `StudioVfs::shutdown` to capture the metadata close error and combine it
with its existing local synchronization errors. The caller must continue
cleanup after a metadata worker timeout; a timeout must not silently become a
successful shutdown.

### 5. Verify and document the boundary

Run focused tests for schema validation, lock behavior, worker lifecycle, and
VFS shutdown. Run formatting, warnings-as-errors compilation, and diff checks.
Run available PostgreSQL integration tests with one test thread. If platform
or database infrastructure is unavailable, record the exact missing
capability in the final Linear update and response.

## Concrete steps

All commands below are run from `/Volumes/Storage/dev/studiofs`. The test-first
rule applies to each new regression test: add one test, run its exact filter
and observe the failure, then make the smallest production change that turns
it green.

1. Confirm the clean branch and read the plan before editing:
   `git status --short --branch`
2. After adding the init-schema regression test, run:
   `cargo test -p studiofs init_pg_schema_provisions_file_lock_lease_schema -- --nocapture`
   Expected before the implementation change: the assertion that
   `file_lock_leases` is included fails.
3. After adding the startup-validation regression test, run its single filter
   with the PostgreSQL test setup and one thread:
   `cargo test -p studiofs-meta test_pg_open_rejects_missing_file_lock_lease_table -- --ignored --nocapture --test-threads=1`
   Expected before the implementation change: the engine opens or the old
   runtime-repair behavior succeeds instead of returning the new startup
   error. If no test DSN is available, record the skip and use non-integration
   contract coverage for the local red/green loop.
4. After adding the worker timeout/idempotence regression test, run its exact
   filter:
   `cargo test -p studiofs-meta prepare_for_close -- --nocapture`
   Expected before the implementation change: the existing timeout test can
   return while a helper thread still owns the actual join, or the new state
   assertion fails.
5. Run the focused metadata lock-worker tests individually as applicable:
   `cargo test -p studiofs-meta pg_engine::tests::test_pg_passive_lock_monitoring_starts_notify_listener_but_defers_renew_worker -- --ignored --nocapture --test-threads=1`
6. Run the focused VFS shutdown tests individually after the trait/caller
   change:
   `cargo test -p studiofs-vfs shutdown -- --nocapture`
7. Run the complete metadata PostgreSQL ignored slice once the focused tests
   pass:
   `cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1`
8. Run repository checks:
   `cargo fmt --all -- --check`

   `RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs -p studiofs`

   `git diff --check`

9. Inspect the final diff and status, then update SFS-642 in Linear with the
   implementation summary, verification commands/results, and explicit
   unavailable-platform/database gaps.

The exact test filters may be adjusted only when the repository's existing
module/test names require it; record the replacement command and reason here
in `Progress` and `Surprises & Discoveries`.

## Validation and acceptance

The change is accepted only when all applicable evidence supports these
claims:

* `studiofs init` provisions the complete required schema, including
  `file_lock_leases`, and remains safe to rerun.
* Engine startup rejects a missing required table with a clear error; no
  foreground lock path issues schema DDL.
* Repeated explicit shutdown is idempotent, and the normal shutdown path waits
  on actual worker handles under the documented deadline policy.
* `Drop` does not block indefinitely and cannot silently report a successful
  explicit shutdown after a worker timeout.
* Lazy lock-worker activation tests still pass.
* Shared metadata/VFS tests compile and pass without platform-specific
  branches.
* PostgreSQL-backed tests pass when the configured test database is available;
  otherwise the exact missing DSN/infrastructure is reported.
* macFUSE, Linux/FUSE, and Windows/WinFSP coverage is reported separately.
  The current environment lacks `/dev/fuse` and MinGW, so those unavailable
  portions are not marked as validated.

## Idempotence and recovery

The init DDL uses the existing idempotent provisioning pattern and executes in
the existing init transaction. Startup validation only reads catalog state and
does not alter an existing volume. A missing table therefore requires an
explicit provisioning/migration action; SFS-642 does not silently mutate
production schema from a foreground request.

Explicit engine shutdown may be called more than once. The first call owns the
join attempt; later calls return the retained terminal result without taking
ownership of already-consumed handles. `Drop` performs only cancellation and
wake-up, so it remains safe during unwinding and cannot wait on a worker.

Test database changes must remain isolated to the existing test fixture and
must not be run against an unrelated production DSN. No destructive repository
operation is required for implementation or recovery.

## Interfaces and dependencies

The intended interface changes are deliberately narrow:

* `MetaReplication::prepare_for_close(&self) -> MetaResult<()>`, with the
  default implementation returning `Ok(())`.
* A private `PgBackgroundWorkers` owned by `PgMetaEngine`; it is not exported
  and does not add a public lifecycle trait.
* A private read-only PostgreSQL schema validator used only during
  `PgMetaEngine::open_with_options`.
* Existing `StudioVfs::shutdown` error aggregation extended to include the
  metadata close result.

No new production dependency is expected. The dependency direction remains
`domain <- ports <- application <- adapters/runtime/API`; schema catalog
access and worker ownership remain in the PostgreSQL adapter layer, while the
shared close result crosses the existing metadata port.

## Outcomes and retrospective

Implementation is not complete yet. Update this section after verification
with the final changed files, commands and results, known infrastructure
gaps, and any design deviation that was required by the current code.
