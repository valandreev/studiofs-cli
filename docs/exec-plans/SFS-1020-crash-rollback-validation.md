# Validate crash recovery and rollback through the 0.2.5 baseline

This ExecPlan follows `.agent/PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture

Operators need to be able to repeat an interrupted `studiofs upgrade` or restart a crashed client without losing already accepted writes. A supported pre-activation rollback advances the policy epoch and restores baseline-client access. A published legacy client is fenced after adoption into format 7. The compiled test successor represents a future supported release without expanding support to pre-0.2.5 clients.

## Progress

- [x] (2026-11-27 21:28Z) Verified the baseline, test successor, and published macOS v0.2.4 binaries. Started disposable PostgreSQL and the registered mounted scenario.
- [x] (2026-11-27 21:33Z) Added crash and cancellation cases to the existing matrix and extended the PostgreSQL prune regression.
- [x] (2026-11-27 21:34Z) Passed nine focused PostgreSQL tests and two focused redb tests.
- [x] (2026-11-27 21:35Z) Passed the registered Janitor restart and mounted snapshot takeover scenarios on macOS.
- [x] (2026-11-27 21:40Z) Added a format-5 adoption cell using a verified published v0.2.2 binary.
- [x] (2026-11-27 21:48Z) Counted SQL statements in real v0.2.4 and 0.2.5 binary publications and retained the failing comparison as a blocking gate.
- [x] (2026-11-27 21:50Z) Passed four additional focused PostgreSQL tests covering contract DDL and snapshot cleanup, and removed the dedicated PostgreSQL stack.
- [x] (2026-11-27 21:53Z) Passed all nine fault-injected macOS cells and the runner cleanup.
- [x] (2026-11-27 21:54Z) Passed all eight ordinary macOS cells and verified removal of the run containers, volumes, and mounts.
- [x] (2026-11-27 22:00Z) Combined the lease stamp with the first version bump or the existing retained-publication row lock. Passed the live PostgreSQL tests for the fused fence.
- [x] (2026-11-27 22:01Z) Rebuilt both binaries and verified the five declared successor changes. Re-pinned the format-7 header and captured and restored a v5 dump against isolated services.
- [x] (2026-11-27 22:07Z) Passed the rebuilt macOS forward matrix (8/8), crash matrix (9/9), and maintenance run (2/2). Verified 14 SQL statements per comparable payload batch for each release and removed the run containers, volumes, and mounts.

## Surprises & Discoveries

The first macOS run passed cells 1, 2, and 8, then checked the wrong mount path in cell 3. On directory-based mounts the product adds the volume name. The log showed the mounted child path while the runner polled and attempted to unmount the parent. The matrix now uses the existing `product_mount_path` helper for both readiness and teardown. The first failed artifact is retained under `target/e2e-runs/sfs1020-macos-forward/`.

The crash run with the original 15-second tree-snapshot limit verified checkpoint rollback and prune retry, then timed out reading the cold multi-chunk file. The cold full-tree checksum now has a 60-second attempt limit within the existing 90-second convergence deadline.

After the baseline was force-killed with PostgreSQL stopped, its old lease and the replacement mount lease could both be active. The first complete crash attempt reached cell 5, where the runner incorrectly required a unique baseline lease. It now verifies that activation refused an observed active baseline lease. The retained crash-v4 report has six passing cells and one failed cell. The crash-v5 retry reached the rollback and refused activation for a different reason. The successor cache had stopped before the new baseline write, so the real cutover preflight found local replay lag. The runner now waits for publication and checksum convergence before stopping the successor for the activation attempt.

Compose's default ports are exclusive. Parallel runs with the defaults failed preflight before mounting anything. Independent runs below use disjoint port ranges.

PostgreSQL truncates long statements in `pg_stat_activity`. The SFS-1015 test now identifies the fused statement by its leading CTE and still waits for the activation transaction to block the publisher before committing.

## Decision Log

On 2026-11-27, the implementation reused the registered `mounted-forward-compat-matrix` with a wrapper flag so the normal compatibility run remains independently runnable. Crash mode runs on disposable Compose services. PostgreSQL triggers inject SQLSTATE 57014 inside transactions. Nontransactional PostgreSQL sequences let the runner wait for cancelled background work even though the transaction rolls back. The redb process-exit hook is compiled only into `test-successor`.

On 2026-11-27, the publication fix used the existing session lock, transaction-local settings, and version-bump predicate. The first bump consumes the epoch produced by a materialized lease stamp and still checks format and write requirement. PostgreSQL rechecks the stored publication epoch after a conflicting row update. Rust negotiates all event encodings against the lease's validated policy before a write. Retained publication stamps while taking its existing metadata row lock, then looks up the receipt in the following statement so it can see any newly committed receipt.

The format-7 `FormatInfo` came from the existing baseline fixture generator. The candidate CLI captured a v5 dump from a fresh format-7 volume and restored it into separate PostgreSQL. The published format-5 and format-6 adoption fixtures and the v4 dump refusal fixture keep their original bytes.

## Outcomes & Retrospective

The rebuilt macOS crash matrix passed nine of nine cells with no skips. It records recovery of both legacy adoption chains, schema and local revision cancellation, checkpoint and prune retry, snapshot checksum, a force-killed baseline with an unpublished write, supported deactivate, and irreversible downgrade refusal. The rebuilt ordinary forward matrix passed eight of eight cells and the maintenance run passed both scenarios. PostgreSQL statement counts match v0.2.4 for the measured file-payload batch. The format-7 header and v5 dump fixture seals pass the drift gate. The build and fixture provenance uses `7fe972358f9e48655249739ae7c8415bcac5504d` with uncommitted changes. Windows remains pending SFS-1135, Linux remains pending SFS-1136, and final release evidence comes from the clean candidate.

## Context and Orientation

`crates/studiofs-e2e/src/scenario/workloads/forward_compat_matrix.rs` runs native mounts and compiled `studiofs` commands against one disposable PostgreSQL and RustFS stack per run. Each mount has independent local metadata and cache storage. A policy epoch identifies the current shared capability policy. A serving checkpoint holds the metadata used by cold clients, and the prune floor marks the newest event history removed after publication. The PostgreSQL registry advances a component version only when its schema change commits. The redb revision marker tells an interrupted local conversion which stage to resume.

## Plan of Work

The fault-enabled matrix interrupts the successor schema migration after its registry update, the local redb revision after the irreversible marker commits, and v0.2.4 adoption before format 7 commits. It cancels checkpoint publication and event-log pruning after the affected statements run, kills the successor before checkpoint retry, and force-kills the baseline after it accepts a synced write while PostgreSQL is stopped. It reuses the existing lease-expiry and pre-activation rollback cases and cancels an activation update before retrying it in a new CLI process. The original matrix verifies cross-client replay, snapshot bytes, and fail-closed old-client behavior. The registered Janitor and snapshot scenarios exercise maintenance separately.

## Concrete Steps

Run from the repository root with a native mount driver, Cargo, Python, Docker Compose, and a verified v0.2.4 binary for the host. Pass the verified native v0.2.4 and v0.2.2 binaries as `<v024>` and `<v022>` below. Use the native executable suffix on Windows. Use fresh run IDs on retries.

    python tools/test_successor/build.py --require-complete
    cargo build -p studiofs-e2e --locked
    STUDIOFS_E2E_COMPOSE_FILE="$PWD/target/sfs1020-fused-compose.e2e.yml" python3 tools/test_successor/run_compat_matrix.py --skip-build --run-id sfs1020-macos-forward-fused --port-range 36200-36219 --v024-bin <v024>
    python3 tools/test_successor/run_compat_matrix.py --skip-build --crash-rollback --run-id sfs1020-macos-crash-fused --port-range 36100-36119 --v024-bin <v024> --v022-bin <v022>
    target/debug/studiofs-e2e run janitor-postgres-restart-recovery --scenario snapshot-materializer-takeover --studiofs-bin target/debug/studiofs --infra compose --retain always --run-id sfs1020-macos-maintenance-fused

For the PostgreSQL tests, start an isolated PostgreSQL and set `STUDIOFS_TEST_PG_DSN` to that disposable service. For the fused-query evidence the command was `STUDIOFS_E2E_POSTGRES_PORT=35432 docker compose -f docker/compose.e2e.yml -p studiofs-sfs1020-fused up -d --wait postgres`. Each focused test below provisions its own schema. Keep `.env` untouched.

For the statement-count run, copy `docker/compose.e2e.yml` to the path given in `STUDIOFS_E2E_COMPOSE_FILE`. Add `command: ["postgres", "-c", "log_statement=all", "-c", "log_parameter_max_length=0", "-c", "log_parameter_max_length_on_error=0"]` under `services.postgres` so logging starts before the test clients connect. Run the checker on the retained Compose log.

To regenerate the two pending fixture pins, use the baseline header generator and the candidate dump and load commands below. Start disposable source PostgreSQL and RustFS and a second disposable restore PostgreSQL. The source ports for the recorded run were 35432 and 35436 and the restore port was 35433. Use the default throwaway credentials from the Compose fixture for the variables in the commands.

    STUDIOFS_REGENERATE_BASELINE_FIXTURES=1 cargo test --locked -p studiofs-meta --lib regenerate_baseline_0_2_5_fixtures -- --ignored
    export STUDIOFS_CLIENT_ROOT="$PWD/target/sfs1020-fused-dump/source"
    target/debug/studiofs init sfs1020-dump-baseline --endpoint http://127.0.0.1:35436 --bucket sfs1020-dump-baseline --access-key "$TEST_S3_ACCESS_KEY" --secret-key "$TEST_S3_SECRET_KEY" --meta "$TEST_SOURCE_PG_DSN"
    target/debug/studiofs dump sfs1020-dump-baseline --path-to-dump target/sfs1020-fused-dump/dump-v5-0.2.5.json
    STUDIOFS_CLIENT_ROOT="$PWD/target/sfs1020-fused-dump/restored" target/debug/studiofs load --path-to-dump target/sfs1020-fused-dump/dump-v5-0.2.5.json --postgresql-address "$TEST_RESTORE_PG_DSN"
    cargo test --locked -p studiofs-meta --test baseline_fixture_manifest

The header generator also writes the baseline redb and checkpoint fixtures. Preserve their sealed bytes when refreshing only `format-info.json`. Copy the verified v5 dump to `crates/studiofs/tests/fixtures/dump-v5-0.2.5.json` and update both digests in the manifest.

## Validation and Acceptance

In the table, the command prefix `PG` is `cargo test --locked -p studiofs-meta --lib`, with `-- --ignored --nocapture` after the test name. `Crash` and `Maintenance` are the corresponding exact commands in Concrete Steps. The migration and maintenance rows retain the earlier focused evidence. The rebuilt installation ran the 13 PostgreSQL tests listed below, the two mounted matrices, and both mounted maintenance scenarios. The earlier runner check `cargo test -p studiofs-e2e --lib --locked scenario::workloads::forward_compat_matrix -- --nocapture` passed four tests.

| Crash point or invariant | Test or mounted evidence | Command | Result |
| --- | --- | --- | --- |
| PostgreSQL schema migration rollback and retry | `migration_registry_applies_resumes_and_refuses_drift_on_live_postgres`, matrix `core_migration_cancellation_and_retry` | PG, Crash cell 1 | PG passed (1), mounted operation passed in crash-fused |
| Local redb irreversible revision marker and pending journal | `baseline_dirty_cache_local_revision_resumes_at_every_boundary`, matrix `local_revision_restart_after_irreversible_marker` | `cargo test -p studiofs-meta --lib --locked --features test-successor baseline_dirty_cache_local_revision_resumes_at_every_boundary -- --nocapture`, Crash cells 1 and 7 | redb passed (1), marker exit, CLI restart and downgrade refusal passed in crash-fused |
| Legacy adoption, format 5→6→7 and 6→7 | `published_format_adoption_retries_and_fences_legacy_publication`, `published_format6_upgrade_resumes_after_each_local_stage`, matrix `format7_adoption_cancellation` | PG, `cargo test -p studiofs-meta --lib --locked published_format6_upgrade_resumes_after_each_local_stage -- --nocapture`, Crash cells 8 and 9 | PG passed (1), redb passed (1), both compiled-binary adoption chains passed in crash-fused |
| Session renewal failure, lease expiry, and fenced restart | `client_session_leases_failed_heartbeat_preserves_local_reads_and_recovers`, `client_session_leases_fence_expired_generations_on_live_postgres`, matrix cells 4 and 5 | PG for each, Crash | PG passed (2), including cancelled renewal, mounted pending write recovery and lease expiry passed in crash-fused |
| Serving checkpoint publication | `pg_checkpoint_lease_takeover_advances_fence_and_rejects_old_owner`, matrix `checkpoint_cancellation_and_restart` | PG, Crash cell 2 | PG passed (1), mounted operation passed in crash-fused |
| Feature transition, fenced write, reversible deactivate, and downgrade refusal | `cutover_activate_and_publication_race_fences_stale_writer`, `two_volumes_retire_feature_in_turn_then_contract_ddl_passes`, `contract_ddl_step_refused_while_any_volume_needs_feature_and_allowed_once_retired_or_available`, matrix `activation_cancellation_and_retry`, `reversible_deactivate_keeps_baseline_rw`, and old-binary refusal | PG for each, Crash cells 5–8 | PG passed (3), mounted cancellation, deactivate, activation and downgrade refusal passed in crash-fused |
| Event-log prune and client replay | `pg_event_log_prune_deletes_complete_batches_in_bounded_transactions`, matrix `prune_floor_preserved_after_cancellation` and cold checksum convergence | PG, Crash cell 2 | PG passed (1), including six event rows preserved before retry, mounted cold replay passed in crash-fused |
| Janitor/GC checkpoint and snapshot maintenance | `pg_janitor_sweep_checkpoint_restart_scope_dry_run_and_version`, `pg_maintenance_stale_claim_is_rejected`, `pg_maintenance_policy_preserves_snapshot_and_replay_history`, `mount_renewal_is_fenced_by_active_cleanup_claim`, `janitor-postgres-restart-recovery`, `snapshot-materializer-takeover` | PG for each, Maintenance | PG passed (4), both macOS scenarios passed |
| No extra publication round trip per successful single-batch transaction | `check_publication_statements.py` on the rebuilt ordinary matrix with PostgreSQL statement logging | `python3 tools/test_successor/check_publication_statements.py target/e2e-runs/sfs1020-macos-forward-fused/artifacts/docker/compose.log` | Pass, exit 0. v0.2.4 executed 14 statements per measured batch (2 transactions) and 0.2.5 executed 14 (1 transaction) |
| Inactive lease, stale validated epoch, concurrent activation, write requirement and successful fenced publication | `event_publication_fence`, `client_session_leases_fence_expired_generations_on_live_postgres`, `client_session_leases_validate_policy_epoch_and_role_capabilities`, `compatible_epoch_change_during_publication_keeps_local_writer` | PG for each filter | Pass (6), including a concurrent renewal and a bump blocked by activation |
| Candidate fixture seal | FormatInfo 7, v5 dump, and manifest digests | Fixture generator, dump and load commands in Concrete Steps. `cargo test --locked -p studiofs-meta --lib baseline_fixtures` and the same filter with `--features test-successor` | Generator passed (1), fixture coverage passed (11 baseline, 13 successor), manifest passed (2), and the pinned v5 dump restored a published format-7 volume |

The other passing live PostgreSQL filters were `client_session_leases_register_renew_and_close_pg_engines`, `client_session_leases_failed_heartbeat_preserves_local_reads_and_recovers`, `mount_policy_change_freezes_local_writer`, `cutover_activate_and_publication_race_fences_stale_writer`, `published_format_adoption_retries_and_fences_legacy_publication`, `test_pg_retained_publication_response_loss_retry`, and `volume_protocol_policy_and_write_requirement_on_live_postgres` (one each). The CLI filters `dump_load_roundtrip_restores_the_published_baseline` with `-- --ignored --nocapture` and `load_refuses_v4_dump_with_upgrade_instruction` each passed with `cargo test --locked -p studiofs --bin studiofs`. The static SQL check `checkpoint_freshness_sql_uses_persisted_scalar_bookkeeping` passed (1). `cargo clippy --locked -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.

## Idempotence and Recovery

Each matrix starts with a fresh Compose database, bucket, and client roots. The runner removes its own containers and mounts and retains reports. A cancelled transaction leaves the previous committed registry, checkpoint, prune floor, or policy available. A normal retry reuses the stopped local database and removes its upgrade marker when local completion commits. Remove the dedicated test stacks with `docker compose -f docker/compose.e2e.yml -p studiofs-sfs1020-fused down -v` and the same command with `-p studiofs-sfs1020-dump-restore`.

## Artifacts and Notes

Mounted results are under `target/e2e-runs/<run-id>/`. The matrix writes `artifacts/scenarios/mounted-forward-compat-matrix/forward-compat-matrix.json` with binary hashes, observed protocol state, and each operation result. Focused PostgreSQL logs are under `target/sfs1020-pg/`. The native legacy archives passed `shasum -a 256 -c studiofs-0.2.4-aarch64-apple-darwin.tar.gz.sha256` and `shasum -a 256 -c studiofs-0.2.2-aarch64-apple-darwin.tar.gz.sha256` before extraction. Use the matching release archive and checksum on each platform. The macOS executable paths were `target/sfs1020-v024/studiofs-0.2.4-aarch64-apple-darwin/studiofs` and `target/sfs1020-v022/studiofs-0.2.2-aarch64-apple-darwin/studiofs`.

The pre-fix log in `sfs1020-macos-forward-v4` yielded 14 statements for v0.2.4 and 15 for 0.2.5 (exit 1). The rebuilt log in `sfs1020-macos-forward-fused` yielded 14 for both (exit 0). The JSON counts are retained at `target/sfs1020-publication-statements.json` and `target/sfs1020-fused-publication-statements.json`. The count covers BEGIN through COMMIT and excludes PostgreSQL wire-protocol messages and lease refresh outside the transaction.

The verified successor build manifest is `target/test-successor/manifest.json`, SHA-256 `635e469c18d1b97e4e1fb514d2320e8ac5fb49532f02a8db51a300b58da66142`. The [fixture manifest](../../crates/studiofs-meta/tests/fixtures/baseline-0.2.5/manifest.json) records this hash, both binary hashes and both fixture checksums. The dump restore state and CLI logs are under `target/sfs1020-fused-dump/`.

## Interfaces and Dependencies

The wrapper adds `--crash-rollback` and selects the existing scenario. The only product code used exclusively by the test successor is the local revision interruption in `crates/studiofs-meta/src/redb_engine/format.rs`. It exits after the stage-3 transaction commits. The ordinary and crash suites require real Docker PostgreSQL and RustFS, the baseline executable, the compiled test successor executable, and the published legacy executables for cells 8 and 9.

Revision note, 2026-11-27. Added the crash gates and recorded the snapshot mount-path and cutover precondition fixes. Repeated the acceptance runs after fusing the publication stamp and recorded statement parity and the fixture seal.
