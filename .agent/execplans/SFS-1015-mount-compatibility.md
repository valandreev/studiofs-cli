# Apply volume compatibility to mounted write authority

This ExecPlan is maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

An existing mount should continue to serve safe local reads when PostgreSQL is unavailable or the volume policy changes. A client that can still read but cannot publish must stop accepting new shared mutations. Its existing local event journal must survive until publication is safe again.

## Progress

- [x] (2026-11-27) Fetch `origin/main` and confirm the Linear issue and dependencies.
- [x] (2026-11-27) Reproduce the false writable status on a PostgreSQL policy change in an isolated schema.
- [x] (2026-11-27) Connect evaluator decisions to startup, replay, publication, and Janitor checks and the mounted lifecycle.
- [x] (2026-11-27) Run focused verification and prepare the change for review.

## Surprises & Discoveries

The publication session already rejected stale epochs and unsupported writer capabilities. On the original code, the isolated regression failed because the mount still allowed new mutations. The configured database also contains a shared schema with a newer core component. Existing catch-up and legacy-format tests now use disposable schemas so the baseline tests leave that schema intact.

The requested branch has no unique commits and is checked out in the primary worktree. This worktree uses its T3 branch and started at the fetched `origin/main` commit.

## Decision Log

Keep the existing atomic PostgreSQL publication fence. Use the compatibility evaluator to assess the shared format and per-operation policy, and project the result into the local lifecycle without PostgreSQL calls from foreground read or mutation admission.

The existing offline authority rules still apply to an established local allocator reservation. A connected incompatible writer enters `ReadOnlyCompatible` when replay remains safe. A read policy refusal, UUID mismatch, or stale writer fence degrades the mount and clears the frozen catch-up target. The local view can still serve reads in those cases. Recovery returns through catch-up, UUID, and allocator validation. A locally unusable store continues to fail startup or enter the existing fatal state. Preferred checkpoint encoding is independent of the authority to publish ordinary metadata events.

The synchronization head query reads the policy even without new events. Replay pages check their read policy with the same statement that reads their format and retained cursor, and publication still stamps its session and epoch inside the transaction. Expected-format locks and predicates on format adoption and artifact decoding retain their separate invariants.

## Plan of Work

Start in `crates/studiofs-meta/src/pg_engine/volume_protocol.rs`, which reads the policy and uses `VolumeProtocol::check_operations` to evaluate required capabilities. Connect the result to remote validation and allocator transitions in `pg_engine.rs`. Apply the read policy before selecting a replay page. Keep session and transaction fencing in place so concurrent policy changes cannot authorize stale publication. Update the CLI mount status mapping if a lifecycle state changes. Audit remaining shared format checks and preserve independent local and backup artifact validation.

## Validation and Acceptance

Run `cargo test -p studiofs-meta --lib mount_policy_change_freezes_local_writer -- --ignored` with the configured `STUDIOFS_TEST_PG_DSN`. The test should show that unsupported write requirements freeze the mount, preserve existing local reads and cached events, and permit recovery after a compatible policy returns. Run focused existing offline, lifecycle, and publication tests, then package checks, format checks, and `git diff --check`.

## Idempotence and Recovery

The regression owns a disposable PostgreSQL schema. A failed publication must leave the journal cursor intact so retries can publish after the policy becomes compatible.

## Outcomes & Retrospective

The new regression failed on the baseline at the mounted write-authority assertion and passed after the change. Eleven focused PostgreSQL tests passed, including catch-up, legacy-format fences, sessions, pending-event recovery, and Janitor. Forty-four focused baseline unit tests and twenty tests with `test-successor` passed. Package check, all-target package Clippy, formatting, and the staged diff check passed. The tests exercised the configured PostgreSQL service in isolated schemas. Mounted WinFSP or FUSE I/O, RustFS I/O, and the full workspace suites are outside this validation run.
