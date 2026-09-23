use super::*;
use crate::engine::{
    CheckpointDescriptorFailure, MetaAcl, MetaError, MetadataCheckpointRefreshReason,
    PgSyncFailure, PgSyncPageDiagnostics,
};
use crate::redb_engine::RedbSnapshot;
use sha2::{Digest, Sha256};

#[test]
fn checkpoint_coordinator_coalesces_requests_until_consumed() {
    let state = super::checkpoint_coordinator::CheckpointCoordinatorState::new();

    assert!(state.request());
    assert!(!state.request());
    assert!(state.take_request());
    assert!(!state.take_request());
    assert!(state.request());
}

#[test]
fn checkpoint_lease_sql_is_fenced_and_short_lived() {
    let claim = super::sql::CLAIM_BOOTSTRAP_CHECKPOINT_LEASE;
    assert!(claim.contains("lease_expires_at"));
    assert!(claim.contains("fencing_token"));
    assert!(claim.contains("clock_timestamp()"));
    assert!(claim.contains("RETURNING"));

    let publish = super::sql::UPSERT_FENCED_METADATA_CHECKPOINT;
    assert!(publish.contains("fencing_token"));
    assert!(publish.contains("lease_expires_at > clock_timestamp()"));
    assert!(publish.contains("RETURNING"));
}

#[test]
fn checkpoint_coordination_miss_fails_closed_when_freshness_observation_fails() {
    let (_dir, engine) = temp_pg_engine_for_unit();

    assert!(
        !engine.refresh_checkpoint_freshness_after_coordination_miss(),
        "a failed freshness observation must not report a fresh checkpoint"
    );
}

async fn with_pg_test_client<T, F, Fut>(dsn: &str, f: F) -> Result<T, MetaError>
where
    F: FnOnce(tokio_postgres::Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, MetaError>>,
{
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let result = f(client).await;
    conn_task.abort();
    let _ = conn_task.await;
    result
}

fn open_pg_publisher(volume: &PgTestVolume) -> (tempfile::TempDir, Arc<PgMetaEngine>) {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("publisher.redb");
    seed_local(&db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata client");
    (dir, publisher)
}

#[derive(Debug, Clone)]
struct PgCheckpointData {
    snapshot: Vec<u8>,
    sha256: [u8; 32],
}

fn read_pg_checkpoint(dsn: &str, volume_id: i64) -> PgCheckpointData {
    run_async(with_pg_test_client(dsn, move |client| async move {
        let row = client
            .query_one(
                "SELECT snapshot, checkpoint_sha256 FROM metadata_state WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let snapshot: Vec<u8> = row
            .try_get("snapshot")
            .map_err(|error| MetaError::Storage(format!("invalid snapshot column: {error}")))?;
        let sha256: Vec<u8> = row.try_get("checkpoint_sha256").map_err(|error| {
            MetaError::Storage(format!("invalid checkpoint checksum column: {error}"))
        })?;
        let sha256: [u8; 32] = sha256.try_into().map_err(|bytes: Vec<u8>| {
            MetaError::Storage(format!(
                "invalid checkpoint checksum length: expected 32, got {}",
                bytes.len()
            ))
        })?;
        Ok::<PgCheckpointData, MetaError>(PgCheckpointData { snapshot, sha256 })
    }))
    .expect("read checkpoint")
}

fn read_pg_event_batch(dsn: &str, volume_id: i64, batch_version: i64) -> (i64, Vec<MetaEvent>) {
    run_async(with_pg_test_client(dsn, move |client| async move {
        let state = client
            .query_one(
                "SELECT version FROM metadata_state WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let rows = client
            .query(
                "SELECT event_type, event_payload FROM event_log WHERE vol = $1 AND batch_version = $2 ORDER BY seq_id",
                &[&volume_id, &batch_version],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let events = rows
            .into_iter()
            .map(|row| crate::events::decode_event_with_type(row.get(0), row.get(1)))
            .collect::<MetaResult<Vec<_>>>()?;
        Ok::<(i64, Vec<MetaEvent>), MetaError>((state.get(0), events))
    }))
    .expect("read event batch from postgres")
}

#[test]
fn pg_sync_lifecycle_raw_values_keep_fatal_explicit_and_unknown_fail_closed() {
    let cases = [
        (0, PgSyncLifecycle::LocalReady),
        (1, PgSyncLifecycle::CatchingUp),
        (2, PgSyncLifecycle::TargetReached),
        (3, PgSyncLifecycle::AllocatorPending),
        (4, PgSyncLifecycle::Live),
        (5, PgSyncLifecycle::Degraded),
        (6, PgSyncLifecycle::Fatal),
    ];

    for (raw, expected) in cases {
        assert_eq!(PgMetaEngine::pg_sync_lifecycle_from_raw(raw), expected);
    }
    assert_eq!(
        PgMetaEngine::pg_sync_lifecycle_from_raw(u8::MAX),
        PgSyncLifecycle::Fatal
    );
}

#[test]
fn pg_sync_diagnostics_reports_frozen_target_and_remaining_distance() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local_version
        .store(4, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(12, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(10, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );

    let diagnostics = engine.pg_sync_diagnostics();

    assert_eq!(diagnostics.lifecycle, PgSyncLifecycle::CatchingUp);
    assert_eq!(diagnostics.local_applied_version, Some(4));
    assert_eq!(diagnostics.startup_target_version, Some(10));
    assert_eq!(diagnostics.remote_head_version, Some(12));
    assert_eq!(diagnostics.remaining_version_distance, Some(6));
}

#[test]
fn rebootstrap_required_is_distinct_and_records_checkpoint_coordinates() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Live);
    let error = MetaError::RebootstrapRequired {
        local_version: 5,
        retained_floor: 8,
        checkpoint_version: 8,
        target_version: 12,
    };

    let returned = engine.record_remote_operation_result::<bool>(Err(error.clone()));

    assert!(matches!(
        returned,
        Err(MetaError::RebootstrapRequired {
            local_version: 5,
            retained_floor: 8,
            checkpoint_version: 8,
            target_version: 12,
        })
    ));
    assert_eq!(
        PgSyncFailure::from_error(&error),
        PgSyncFailure::RebootstrapRequired {
            local_version: Some(5),
            retained_floor: Some(8),
            checkpoint_version: Some(8),
            target_version: Some(12),
        }
    );
    let diagnostics = engine.pg_sync_diagnostics();
    assert!(diagnostics.rebootstrap_required);
    assert_eq!(
        diagnostics.last_sync_error,
        Some(PgSyncFailure::RebootstrapRequired {
            local_version: Some(5),
            retained_floor: Some(8),
            checkpoint_version: Some(8),
            target_version: Some(12),
        })
    );
    assert_eq!(
        diagnostics
            .last_sync_error
            .expect("rebootstrap diagnostic")
            .to_string(),
        "checkpoint rebootstrap required (local cursor 5, retained floor 8, checkpoint 8, target 12)"
    );
    assert!(diagnostics.source_connected);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
}

#[test]
fn pg_sync_diagnostics_reports_distance_from_uninitialized_cursor() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local_version
        .store(-1, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(100, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );

    let diagnostics = engine.pg_sync_diagnostics();

    assert_eq!(diagnostics.local_applied_version, None);
    assert_eq!(diagnostics.startup_target_version, Some(100));
    assert_eq!(diagnostics.remaining_version_distance, Some(101));
}

#[test]
fn pg_sync_diagnostics_prefers_newer_checkpoint_head_observation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .metadata_remote_head_version
        .store(10, std::sync::atomic::Ordering::Release);
    engine.record_checkpoint_freshness_observation(
        super::super::metadata_io::CheckpointFreshnessObservation {
            head_version: 14,
            checkpoint_version: 10,
            total_event_payload_bytes: 0,
            format_info_present: true,
            event_payload_bytes: 0,
            checkpoint_age: Duration::ZERO,
        },
    );

    assert_eq!(engine.pg_sync_diagnostics().remote_head_version, Some(14));
}

#[test]
fn pg_sync_diagnostics_tracks_redacted_error_and_page_completion() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let failure = MetaError::RemoteUnavailable("test-postgres-dsn".into());
    engine.record_pg_sync_failure(&failure);

    let failed = engine.pg_sync_diagnostics();
    assert_eq!(
        failed.last_sync_error,
        Some(PgSyncFailure::RemoteUnavailable)
    );
    assert!(
        !serde_json::to_string(&failed)
            .unwrap()
            .contains("test-postgres-dsn")
    );

    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: 4,
        first_available_version: 8,
        target_version: 12,
    });
    let gap = engine.pg_sync_diagnostics();
    assert!(gap.rebootstrap_required);
    assert!(matches!(
        gap.last_sync_error,
        Some(PgSyncFailure::ReplayGap { .. })
    ));

    let first_page = PgSyncPageDiagnostics {
        first_batch_version: Some(1),
        last_batch_version: Some(4),
        batch_count: 2,
        event_count: 3,
        response_bytes: 64,
        has_more: true,
        oversized: false,
        ..Default::default()
    };
    let next_page = PgSyncPageDiagnostics {
        first_batch_version: Some(5),
        last_batch_version: Some(8),
        has_more: false,
        ..first_page
    };

    assert!(engine.record_pg_sync_page_selected(first_page));
    engine.record_pg_sync_failure(&MetaError::RemoteUnavailable("test-postgres-dsn".into()));
    assert_eq!(engine.pg_sync_diagnostics().current_page, None);

    assert!(!engine.record_pg_sync_page_selected(first_page));
    assert!(!engine.record_pg_sync_page_selected(first_page));
    assert!(engine.record_pg_sync_page_selected(next_page));
    assert_eq!(engine.pg_sync_diagnostics().current_page, Some(next_page));

    engine.record_pg_sync_page_applied(next_page);
    assert_eq!(engine.pg_sync_diagnostics().current_page, None);
    assert_eq!(engine.pg_sync_diagnostics().last_page, Some(next_page));

    engine.record_pg_sync_success();
    assert_eq!(engine.pg_sync_diagnostics().last_sync_error, None);
}

#[test]
fn pg_sync_success_clears_rebootstrap_requirement() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: -1,
        first_available_version: 1,
        target_version: 3,
    });
    assert!(engine.pg_sync_diagnostics().rebootstrap_required);

    engine.record_pg_sync_success();

    assert!(!engine.pg_sync_diagnostics().rebootstrap_required);
}

#[test]
fn idle_degraded_sync_success_keeps_rebootstrap_requirement() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: 1,
        first_available_version: 2,
        target_version: 3,
    });

    engine.record_pg_sync_success();

    assert!(engine.pg_sync_diagnostics().rebootstrap_required);
}

#[test]
fn pg_sync_failure_classifies_nested_and_oversized_errors() {
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::EventTooLarge {
            estimated: 10,
            limit: 5,
        }),
        PgSyncFailure::Storage
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::RepairRequired("repair".into())),
        PgSyncFailure::Storage
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::Serialization("decode".into())),
        PgSyncFailure::Storage
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::RegularFileOverlayBatchOperation {
            index: 0,
            source: Box::new(MetaError::RemoteUnavailable("test-postgres-dsn".into(),)),
        }),
        PgSyncFailure::RemoteUnavailable
    );
}

#[test]
fn pg_sync_failure_preserves_uninitialized_versions() {
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::ReplayGap {
            local_version: -1,
            first_available_version: 1,
            target_version: 3,
        }),
        PgSyncFailure::ReplayGap {
            local_version: None,
            first_available_version: Some(1),
            target_version: Some(3),
        }
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::CatchUpStalled {
            local_version: -1,
            target_version: 3,
        }),
        PgSyncFailure::CatchUpStalled {
            local_version: None,
            target_version: Some(3),
        }
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::CatchUpBudgetExceeded {
            local_version: -1,
            target_version: 3,
            pages: 0,
        }),
        PgSyncFailure::CatchUpBudgetExceeded {
            local_version: None,
            target_version: Some(3),
            pages: 0,
        }
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::RebootstrapRequired {
            local_version: -1,
            retained_floor: -1,
            checkpoint_version: -1,
            target_version: -1,
        }),
        PgSyncFailure::RebootstrapRequired {
            local_version: None,
            retained_floor: None,
            checkpoint_version: None,
            target_version: None,
        }
    );
}

#[test]
fn startup_catch_up_drain_replays_pages_until_frozen_target() {
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        3,
        || local_version.get(),
        || true,
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect("startup catch-up should drain each bounded page");

    assert!(changed);
    assert_eq!(local_version.get(), 3);
    assert_eq!(pulls.get(), 3);
}

#[test]
fn startup_rebootstrap_requires_snapshot_import_before_refreshing_target() {
    let rebootstrap_error = MetaError::RebootstrapRequired {
        local_version: 5,
        retained_floor: 8,
        checkpoint_version: 8,
        target_version: 12,
    };
    let target_calls = std::cell::Cell::new(0_usize);

    let error = rebootstrap_startup_target(
        rebootstrap_error.clone(),
        || Ok(false),
        || {
            target_calls.set(target_calls.get() + 1);
            Ok(15)
        },
    )
    .expect_err("a missing snapshot must keep recovery failed");

    assert!(matches!(error, MetaError::RebootstrapRequired { .. }));
    assert_eq!(target_calls.get(), 0);

    let target = rebootstrap_startup_target(
        rebootstrap_error,
        || Ok(true),
        || {
            target_calls.set(target_calls.get() + 1);
            Ok(15)
        },
    )
    .expect("an imported snapshot should allow a fresh target");

    assert_eq!(target, 15);
    assert_eq!(target_calls.get(), 1);
}

#[test]
fn startup_rebootstrap_propagates_snapshot_import_error_without_refreshing_target() {
    let rebootstrap_error = MetaError::RebootstrapRequired {
        local_version: 5,
        retained_floor: 8,
        checkpoint_version: 8,
        target_version: 12,
    };
    let target_calls = std::cell::Cell::new(0_usize);

    let error = rebootstrap_startup_target(
        rebootstrap_error,
        || Err(MetaError::Storage("io failure".to_string())),
        || {
            target_calls.set(target_calls.get() + 1);
            Ok(15)
        },
    )
    .expect_err("snapshot import errors must be returned unchanged");

    assert!(matches!(
        error,
        MetaError::Storage(message) if message == "io failure"
    ));
    assert_eq!(target_calls.get(), 0);
}

#[test]
fn startup_catch_up_drain_yields_at_page_budget_and_resumes_from_cursor() {
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);

    let error = drain_startup_catch_up_pages(
        17,
        || local_version.get(),
        || true,
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect_err("a startup session must yield before monopolizing replication");

    assert!(matches!(
        error,
        MetaError::CatchUpBudgetExceeded {
            local_version: 16,
            target_version: 17,
            pages: 16
        }
    ));
    assert_eq!(local_version.get(), 16);
    assert_eq!(pulls.get(), 16);

    let resumed_pulls = std::cell::Cell::new(0_usize);
    let changed = drain_startup_catch_up_pages(
        17,
        || local_version.get(),
        || true,
        || {
            resumed_pulls.set(resumed_pulls.get() + 1);
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect("the next session must resume from the durable cursor");

    assert!(changed);
    assert_eq!(local_version.get(), 17);
    assert_eq!(resumed_pulls.get(), 1);
}

#[test]
fn startup_catch_up_drain_rejects_page_without_cursor_progress() {
    let error = drain_startup_catch_up_pages(1, || 0, || true, || Ok(false))
        .expect_err("a stalled startup catch-up must be reported");

    assert!(matches!(
        error,
        MetaError::CatchUpStalled {
            local_version: 0,
            target_version: 1
        }
    ));
}

#[test]
fn startup_catch_up_drain_stops_when_session_is_cancelled() {
    let pulls = std::cell::Cell::new(0_usize);
    let changed = drain_startup_catch_up_pages(
        1,
        || 0,
        || false,
        || {
            pulls.set(pulls.get() + 1);
            Ok(true)
        },
    )
    .expect("cancelled startup catch-up should return cleanly");

    assert!(!changed);
    assert_eq!(pulls.get(), 0);
}

#[test]
fn startup_catch_up_drain_stops_after_partial_progress_when_cancelled() {
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);
    let continue_calls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || local_version.get(),
        || {
            let call = continue_calls.get();
            continue_calls.set(call + 1);
            call == 0
        },
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(1);
            Ok(true)
        },
    )
    .expect("cancellation after a committed page should return cleanly");

    assert!(changed);
    assert_eq!(local_version.get(), 1);
    assert_eq!(pulls.get(), 1);
    assert_eq!(continue_calls.get(), 2);
}

#[test]
fn startup_catch_up_drain_rejects_negative_target() {
    let error = drain_startup_catch_up_pages(-1, || 0, || true, || Ok(false))
        .expect_err("a startup catch-up target must not be negative");

    assert!(error.to_string().contains("target must not be negative"));
}

#[test]
fn startup_catch_up_drain_skips_pull_when_already_at_target() {
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        3,
        || 3,
        || panic!("continuation must not be checked at the target"),
        || {
            pulls.set(pulls.get() + 1);
            Ok(true)
        },
    )
    .expect("an already reached target should complete");

    assert!(!changed);
    assert_eq!(pulls.get(), 0);
}

#[test]
fn startup_catch_up_drain_skips_pull_when_local_cursor_exceeds_target() {
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || 5,
        || panic!("continuation must not be checked past the target"),
        || {
            pulls.set(pulls.get() + 1);
            Ok(true)
        },
    )
    .expect("a cursor past the target should complete");

    assert!(!changed);
    assert_eq!(pulls.get(), 0);
}

#[test]
fn startup_catch_up_drain_propagates_page_error() {
    let error = drain_startup_catch_up_pages(
        1,
        || 0,
        || true,
        || Err(MetaError::Storage("page failure".to_string())),
    )
    .expect_err("a page error must terminate startup catch-up");

    assert!(error.to_string().contains("page failure"));
}

#[test]
fn startup_catch_up_drain_reads_cursor_once_per_loop_iteration() {
    let local_version = std::cell::Cell::new(0_i64);
    let reads = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || {
            reads.set(reads.get() + 1);
            local_version.get()
        },
        || true,
        || {
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect("cursor should reach the target");

    assert!(changed);
    assert_eq!(reads.get(), 3);
}

#[test]
fn startup_catch_up_stall_resets_target_and_enters_degraded_lifecycle() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    engine
        .metadata_startup_target_version
        .store(9, std::sync::atomic::Ordering::Release);

    engine.reset_startup_catch_up_after_stall();

    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        -1
    );
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

#[test]
fn startup_catch_up_session_rejects_fatal_lifecycle_as_success() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::Fatal as u8,
        std::sync::atomic::Ordering::Release,
    );

    let error = engine
        .run_startup_catch_up_session(
            MetadataSyncSource::Manual,
            &std::sync::atomic::AtomicBool::new(false),
        )
        .expect_err("fatal lifecycle must not be reported as a successful sync");

    assert!(matches!(error, MetaError::LifecycleFatal));
}

#[test]
fn startup_catch_up_session_honors_shutdown_signal() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    let shutdown = std::sync::atomic::AtomicBool::new(true);

    let changed = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect("shutdown should stop startup catch-up cleanly");

    assert!(!changed);
}

#[test]
fn startup_catch_up_drain_honors_shutdown_after_a_page() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .metadata_startup_target_version
        .store(2, std::sync::atomic::Ordering::Release);
    let shutdown = std::sync::atomic::AtomicBool::new(false);
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || local_version.get(),
        || !shutdown.load(std::sync::atomic::Ordering::Acquire),
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(1);
            shutdown.store(true, std::sync::atomic::Ordering::Release);
            Ok(true)
        },
    )
    .expect("shutdown after a committed page should stop catch-up cleanly");

    assert!(changed);
    assert_eq!(pulls.get(), 1);
    engine
        .local_version
        .store(local_version.get(), std::sync::atomic::Ordering::Release);
    engine
        .try_complete_catchup_sync()
        .expect("incomplete shutdown catch-up should remain recoverable");
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

struct StartupCatchUpFaultFixture {
    _volume: PgTestVolume,
    _dir: tempfile::TempDir,
    db: std::path::PathBuf,
    target_version: i64,
    stale: Option<std::sync::Arc<PgMetaEngine>>,
}

type StartupCatchUpFaultObservations = std::sync::Arc<std::sync::Mutex<Vec<CatchUpFaultPoint>>>;

const STARTUP_CATCH_UP_FAULT_PAGES: &[&[u8]] = &[
    b"fault-page-1",
    b"fault-page-2",
    b"fault-page-3",
    b"fault-page-4",
];

fn startup_catch_up_fault_fixture() -> StartupCatchUpFaultFixture {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-startup-catch-up-faults");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("stale.redb");
    seed_local(&db, &volume.format_info());

    let stale = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 0);

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata client");
    for &name in STARTUP_CATCH_UP_FAULT_PAGES {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create catch-up event");
        publisher.flush_metadata().expect("publish catch-up event");
    }
    drop(publisher);
    drop(publisher_dir);

    let empty_checkpoint = postcard::to_stdvec(&crate::snapshot::ServingCheckpoint::default())
        .expect("encode empty replay-tail checkpoint");
    let empty_checkpoint_sha256 = Sha256::digest(&empty_checkpoint).to_vec();
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET checkpoint_version = 0, snapshot = $1, checkpoint_sha256 = $2 WHERE vol = $3",
                    &[&empty_checkpoint, &empty_checkpoint_sha256, &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("pin replay-tail checkpoint at version zero");

    StartupCatchUpFaultFixture {
        _volume: volume,
        _dir: dir,
        db,
        target_version: STARTUP_CATCH_UP_FAULT_PAGES.len() as i64,
        stale: Some(stale),
    }
}

fn startup_catch_up_fault_engine(
    fixture: &mut StartupCatchUpFaultFixture,
) -> std::sync::Arc<PgMetaEngine> {
    let engine = fixture.stale.take().unwrap_or_else(|| {
        PgMetaEngine::open_with_options(
            &fixture.db,
            PgOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .expect("open catch-up metadata client")
    });
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    assert_eq!(
        engine
            .frozen_sync_target_version()
            .expect("freeze catch-up target"),
        fixture.target_version
    );
    engine
}

fn startup_catch_up_fault_hook(
    should_fail: impl Fn(CatchUpFaultPoint) -> bool + Send + Sync + 'static,
) -> (StartupCatchUpFaultObservations, CatchUpFaultHook) {
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_hook = std::sync::Arc::clone(&observed);
    let fired_hook = std::sync::Arc::clone(&fired);
    let hook = std::sync::Arc::new(move |point: CatchUpFaultPoint| {
        observed_hook
            .lock()
            .expect("catch-up fault observation lock")
            .push(point);
        // Inject the fault once so recovery can continue after the first failure.
        if should_fail(point) && !fired_hook.swap(true, std::sync::atomic::Ordering::AcqRel) {
            Err(MetaError::Timeout("injected catch-up fault".to_string()))
        } else {
            Ok(())
        }
    });
    (observed, hook)
}

fn persisted_startup_catch_up_cursor(path: &std::path::Path) -> i64 {
    let local = RedbMetaEngine::open(path).expect("open local metadata for cursor check");
    PgMetaEngine::load_persisted_local_version(&local).expect("read persisted catch-up cursor")
}

fn startup_catch_up_fault_resume(fixture: &mut StartupCatchUpFaultFixture, expected_cursor: i64) {
    assert_eq!(
        persisted_startup_catch_up_cursor(&fixture.db),
        expected_cursor
    );
    let engine = startup_catch_up_fault_engine(fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|_| false);
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        engine
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("resumed catch-up session")
    );
    assert_eq!(
        engine.local_version.load(Ordering::Acquire),
        fixture.target_version
    );
    for &name in STARTUP_CATCH_UP_FAULT_PAGES {
        engine
            .lookup(ROOT_INO, name)
            .expect("resumed catch-up should expose every page");
    }
    let first_fetch = observed
        .lock()
        .expect("read resumed catch-up observations")
        .iter()
        .copied()
        .find(|point| matches!(point, CatchUpFaultPoint::BeforePageFetch { .. }));
    assert_eq!(
        first_fetch,
        Some(CatchUpFaultPoint::BeforePageFetch {
            cursor: expected_cursor
        }),
        "resumed catch-up must begin at the durable cursor"
    );
    drop(engine);
    assert_eq!(
        persisted_startup_catch_up_cursor(&fixture.db),
        fixture.target_version,
        "resumed catch-up must persist the final target version to redb"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_before_page_fetch_reopens_from_original_cursor() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(point, CatchUpFaultPoint::BeforePageFetch { cursor: 0 })
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("a pre-fetch fault must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(point, CatchUpFaultPoint::BeforePageFetch { cursor: 0 }))
    );
    drop(engine);
    assert_eq!(
        persisted_startup_catch_up_cursor(&fixture.db),
        0,
        "a pre-fetch fault must not advance the durable cursor"
    );

    startup_catch_up_fault_resume(&mut fixture, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_after_later_page_fetch_reopens_from_last_committed_page() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(
            point,
            CatchUpFaultPoint::AfterPageFetch {
                cursor: 1,
                first_batch_version: Some(2),
                last_batch_version: Some(2),
            }
        )
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("a later page fetch fault must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert_eq!(
        engine.local_version.load(Ordering::Acquire),
        1,
        "the first page should remain applied in memory"
    );
    engine
        .lookup(ROOT_INO, b"fault-page-1")
        .expect("the first page should remain visible");
    assert!(matches!(
        engine.lookup(ROOT_INO, b"fault-page-2"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(point, CatchUpFaultPoint::AfterPageFetch { cursor: 1, .. }))
    );
    drop(engine);
    assert_eq!(persisted_startup_catch_up_cursor(&fixture.db), 1);

    startup_catch_up_fault_resume(&mut fixture, 1);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_before_local_commit_reopens_without_partial_page() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(
            point,
            CatchUpFaultPoint::BeforeLocalCommit {
                last_batch_version: 2
            }
        )
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("a pre-commit fault must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert_eq!(engine.local_version.load(Ordering::Acquire), 1);
    engine
        .lookup(ROOT_INO, b"fault-page-1")
        .expect("the first page should remain visible");
    assert!(matches!(
        engine.lookup(ROOT_INO, b"fault-page-2"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(
                point,
                CatchUpFaultPoint::BeforeLocalCommit {
                    last_batch_version: 2
                }
            ))
    );
    drop(engine);
    assert_eq!(persisted_startup_catch_up_cursor(&fixture.db), 1);

    startup_catch_up_fault_resume(&mut fixture, 1);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_after_local_commit_reopens_from_committed_page() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(
            point,
            CatchUpFaultPoint::AfterLocalCommit {
                last_batch_version: 1
            }
        )
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("an after-commit interruption must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert_eq!(
        engine.local_version.load(Ordering::Acquire),
        0,
        "the interrupted engine must not publish its post-commit cursor"
    );
    engine
        .lookup(ROOT_INO, b"fault-page-1")
        .expect("the committed page should remain visible after interruption");
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(
                point,
                CatchUpFaultPoint::AfterLocalCommit {
                    last_batch_version: 1
                }
            ))
    );
    drop(engine);
    assert_eq!(persisted_startup_catch_up_cursor(&fixture.db), 1);

    startup_catch_up_fault_resume(&mut fixture, 1);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_single_oversized_batch_is_applied_atomically() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let _byte_limit = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "1");
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-startup-catch-up-oversized-batch");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());

    let events = vec![
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"oversized-page-1".to_vec(),
            entry: DirEntryValue {
                ino: 11,
                kind: FileType::Directory,
            },
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"oversized-page-2".to_vec(),
            entry: DirEntryValue {
                ino: 12,
                kind: FileType::Directory,
            },
        },
    ];
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |mut client| async move {
            insert_encoded_event_batch(&mut client, volume_id, 1, "remote-oversized", &events)
                .await?;
            client
                .execute(
                    "UPDATE metadata_state SET version = 1 WHERE vol = $1",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("insert oversized foreign batch");

    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open oversized-batch catch-up client");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    assert_eq!(
        engine
            .frozen_sync_target_version()
            .expect("freeze oversized-batch target"),
        1
    );
    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        engine
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("apply oversized foreign batch")
    );

    assert_eq!(engine.local_version.load(Ordering::Acquire), 1);
    for name in [b"oversized-page-1".as_slice(), b"oversized-page-2"] {
        engine
            .lookup(ROOT_INO, name)
            .expect("all rows in one oversized batch must be visible");
    }
    let diagnostics = engine.pg_sync_diagnostics();
    assert_eq!(diagnostics.last_page.map(|page| page.batch_count), Some(1));
    assert_eq!(diagnostics.last_page.map(|page| page.event_count), Some(2));
    assert!(diagnostics.last_page.is_some_and(|page| page.oversized));
}

struct EventLogLockGuard {
    release_tx: Option<tokio::sync::oneshot::Sender<()>>,
    lock_thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for EventLogLockGuard {
    fn drop(&mut self) {
        if let Some(release_tx) = self.release_tx.take() {
            let _ = release_tx.send(());
        }
        if let Some(lock_thread) = self.lock_thread.take() {
            let _ = lock_thread.join();
        }
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn stale_local_open_does_not_replay_remote_event_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "1000");
    let volume = PgTestVolume::new(&dsn, "pg-open-local-first");
    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata");
    publisher
        .create_dir(ROOT_INO, b"remote-entry", 0, 0, 0o755)
        .expect("create remote event");
    publisher
        .flush_metadata()
        .expect("publish first remote event");
    publisher
        .create_dir(ROOT_INO, b"remote-entry-2", 0, 0, 0o755)
        .expect("create second remote event");
    publisher
        .flush_metadata()
        .expect("publish second remote event");
    drop(publisher);

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let local = RedbMetaEngine::open(&db).expect("open seeded local metadata");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist stale replay cursor");
    drop(local);

    let (lock_ready_tx, lock_ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let lock_dsn = dsn.clone();
    let lock_thread = std::thread::spawn(move || {
        run_async(async move {
            let (mut client, connection) =
                tokio_postgres::connect(&lock_dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let transaction = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            transaction
                .batch_execute("LOCK TABLE event_log IN ACCESS EXCLUSIVE MODE")
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            lock_ready_tx
                .send(())
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            release_rx.await.map_err(|_| {
                MetaError::Storage("event-log lock release signal dropped".to_string())
            })?;
            transaction
                .rollback()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            connection_task.abort();
            let _ = connection_task.await;
            Ok(())
        })
        .expect("hold event-log lock");
    });
    let lock_guard = EventLogLockGuard {
        release_tx: Some(release_tx),
        lock_thread: Some(lock_thread),
    };
    lock_ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("wait for event-log lock");

    let started = Instant::now();
    let opened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("stale local metadata should open while event replay is blocked");
    let elapsed = started.elapsed();
    let lifecycle = opened.pg_sync_status().lifecycle;

    assert!(
        elapsed < Duration::from_millis(1_500),
        "open should not perform repeated event-tail replay, elapsed {elapsed:?}"
    );
    assert_eq!(
        lifecycle,
        PgSyncLifecycle::CatchingUp,
        "stale local metadata must remain explicitly non-live"
    );

    drop(lock_guard);
    opened
        .sync_metadata()
        .expect("catch up after event-log unlock");
    assert_eq!(opened.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(opened.pg_sync_status().allows_new_mutations());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_reports_rebootstrap_below_retained_event_log_floor() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-floor");
    let volume_id = volume.volume_id;
    run_async(with_pg_test_client(&dsn, move |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 12, checkpoint_version = 8, event_log_prune_floor = 8 WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("seed retained replay floor");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("stale.redb");
    seed_local(&db, &volume.format_info());
    let local = RedbMetaEngine::open(&db).expect("open seeded local metadata");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 5)
        .expect("persist cursor below retained floor");
    drop(local);

    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    let error = engine
        .sync_metadata_with_source(MetadataSyncSource::Manual)
        .expect_err("a cursor below retained history must require rebootstrap");

    assert!(matches!(
        error,
        MetaError::RebootstrapRequired {
            local_version: 5,
            retained_floor: 8,
            checkpoint_version: 8,
            target_version: 12,
        }
    ));
    let diagnostics = engine.pg_sync_diagnostics();
    assert!(diagnostics.rebootstrap_required);
    assert_eq!(
        diagnostics.last_sync_error,
        Some(PgSyncFailure::RebootstrapRequired {
            local_version: Some(5),
            retained_floor: Some(8),
            checkpoint_version: Some(8),
            target_version: Some(12),
        })
    );
    assert_eq!(diagnostics.lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn stale_client_rebootstrap_without_pending_overlay_reaches_live_and_replays_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-clean-client");

    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    for name in [b"checkpoint-clean-1".as_slice(), b"checkpoint-clean-2"] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create checkpoint event");
        publisher
            .flush_metadata()
            .expect("publish checkpoint event");
    }
    let checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    publisher
        .create_dir(ROOT_INO, b"replayed-after-clean-rebootstrap", 0, 0, 0o755)
        .expect("create replay tail event");
    publisher
        .flush_metadata()
        .expect("publish replay tail event");
    drop(publisher);

    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version <= 2",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "UPDATE metadata_state SET version = 3, checkpoint_version = 2, snapshot = $1, checkpoint_sha256 = $2, event_log_prune_floor = 2 WHERE vol = $3",
                    &[&checkpoint.snapshot, &checkpoint.sha256.as_slice(), &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("set retained replay floor");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let local = RedbMetaEngine::open(&stale_db).expect("open stale local metadata");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist stale client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist cursor below retained floor");
    drop(local);

    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open clean stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 1);
    assert!(matches!(
        stale.lookup(ROOT_INO, b"checkpoint-clean-2"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(matches!(
        stale.lookup(ROOT_INO, b"replayed-after-clean-rebootstrap"),
        Err(MetaError::EntryNotFound { .. })
    ));

    stale
        .sync_metadata()
        .expect("clean stale client should rebootstrap from the checkpoint");

    assert_eq!(stale.local_version.load(Ordering::Acquire), 3);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(stale.pg_sync_status().allows_new_mutations());
    for name in [
        b"checkpoint-clean-1".as_slice(),
        b"checkpoint-clean-2",
        b"replayed-after-clean-rebootstrap",
    ] {
        stale
            .lookup(ROOT_INO, name)
            .expect("checkpoint and replay tail entries must be visible");
    }
    stale
        .create_dir(ROOT_INO, b"post-rebootstrap", 0, 0, 0o755)
        .expect("create entry after clean rebootstrap");
    stale
        .flush_metadata()
        .expect("flush entry after clean rebootstrap");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 4);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_rebases_stale_client_from_replay_gap_preserving_pending_overlay() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-checkpoint");

    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    for name in [b"checkpoint-1".as_slice(), b"checkpoint-2"] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create checkpoint event");
        publisher
            .flush_metadata()
            .expect("publish checkpoint event");
    }
    let checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    publisher
        .create_dir(ROOT_INO, b"replayed-after-checkpoint", 0, 0, 0o755)
        .expect("create replay tail event");
    publisher
        .flush_metadata()
        .expect("publish replay tail event");
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version = 2",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "UPDATE metadata_state SET version = 3, checkpoint_version = 2, snapshot = $1, checkpoint_sha256 = $2, event_log_prune_floor = 0 WHERE vol = $3",
                    &[&checkpoint.snapshot, &checkpoint.sha256.as_slice(), &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("restore checkpoint descriptor at retained floor");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let local = RedbMetaEngine::open(&stale_db).expect("open stale local metadata");
    local
        .create_dir(ROOT_INO, b"pending-local", 0, 0, 0o755)
        .expect("create pending local overlay");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist stale client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist stale replay cursor");
    drop(local);

    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 1);
    assert_eq!(
        stale
            .remote_metadata_head_version()
            .expect("read remote head after stale client open"),
        3
    );
    stale
        .sync_metadata()
        .expect("manual sync should rebase a stale replay cursor");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 3);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        stale
            .metadata_invalidation_diagnostics()
            .fallback_poll_discoveries,
        0
    );
    publisher
        .create_dir(ROOT_INO, b"advanced-after-rebootstrap", 0, 0, 0o755)
        .expect("create event after rebootstrap detection");
    publisher
        .flush_metadata()
        .expect("publish event after rebootstrap detection");
    drop(publisher);

    stale
        .sync_metadata()
        .expect("manual sync should replay the post-rebase event");

    assert_eq!(stale.local_version.load(Ordering::Acquire), 4);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    for name in [
        b"checkpoint-1".as_slice(),
        b"checkpoint-2",
        b"replayed-after-checkpoint",
        b"advanced-after-rebootstrap",
        b"pending-local",
    ] {
        stale
            .lookup(ROOT_INO, name)
            .expect("checkpoint, replay, and pending entries must survive rebase");
    }

    stale
        .flush_metadata()
        .expect("flush preserved overlay after rebase");
    let (remote_version, remote_events) = read_pg_event_batch(&dsn, volume.volume_id, 5);
    assert_eq!(remote_version, 5);
    assert!(remote_events.iter().any(|event| matches!(
        event,
        MetaEvent::AddEntry { name, .. } if name.as_slice() == b"pending-local"
    )));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn stale_client_rebootstrap_failure_preserves_old_base_until_valid_checkpoint_is_available() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-atomicity");

    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    publisher
        .create_dir(ROOT_INO, b"old-complete-base", 0, 0, 0o755)
        .expect("create old base event");
    publisher.flush_metadata().expect("publish old base event");
    let valid_checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    publisher
        .create_dir(ROOT_INO, b"new-complete-base", 0, 0, 0o755)
        .expect("create new base event");
    publisher.flush_metadata().expect("publish new base event");
    drop(publisher);

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let local = RedbMetaEngine::open(&stale_db).expect("open stale local metadata");
    local
        .import_serving_checkpoint_bytes(&valid_checkpoint.snapshot)
        .expect("seed stale client with old complete base");
    local
        .create_file(ROOT_INO, b"unflushed-pending.bin", 0, 0, 0o644)
        .expect("create unflushed pending overlay");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist stale client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 0)
        .expect("persist stale replay cursor");
    drop(local);

    let invalid_checksum = [0_u8; 32];
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        let checkpoint = valid_checkpoint.snapshot.clone();
        move |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version < 2",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "UPDATE metadata_state SET version = 2, checkpoint_version = 1, snapshot = $1, checkpoint_sha256 = $2, event_log_prune_floor = 1 WHERE vol = $3",
                    &[&checkpoint, &invalid_checksum.as_slice(), &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("publish invalid checkpoint checksum");

    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("invalid checkpoint must not prevent local serving");
    assert!(stale.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(stale.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(matches!(
        stale.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    let error = stale
        .sync_metadata()
        .expect_err("checksum failure must stop before base replacement");
    assert!(matches!(
        error,
        MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::Checksum
        }
    ));
    assert!(stale.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(stale.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(matches!(
        stale.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert_eq!(stale.local_version.load(Ordering::Acquire), 0);

    let mut malformed_checkpoint: crate::snapshot::ServingCheckpoint =
        postcard::from_bytes(&valid_checkpoint.snapshot).expect("decode valid checkpoint");
    malformed_checkpoint
        .inodes
        .push((u64::MAX - 1, b"invalid-inode-encoding".to_vec()));
    let malformed_checkpoint =
        postcard::to_stdvec(&malformed_checkpoint).expect("encode malformed checkpoint");
    let malformed_checkpoint_sha256 = Sha256::digest(&malformed_checkpoint).to_vec();
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET snapshot = $1, checkpoint_sha256 = $2 WHERE vol = $3",
                    &[&malformed_checkpoint, &malformed_checkpoint_sha256, &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("publish checkpoint that fails during replacement");

    let error = stale
        .sync_metadata()
        .expect_err("malformed inode must abort checkpoint replacement");
    assert!(
        matches!(&error, MetaError::Serialization(_)),
        "malformed checkpoint must be reported as a serialization failure; got: {error:?}"
    );
    assert!(stale.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(stale.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(matches!(
        stale.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    drop(stale);

    let reopened = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("restart must retain the old complete base");
    assert!(reopened.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(reopened.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(
        reopened.list_inode_attrs().is_ok(),
        "local metadata database must remain structurally valid when rebootstrap aborts"
    );
    assert!(matches!(
        reopened.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert_eq!(
        reopened.local_version.load(Ordering::Acquire),
        0,
        "aborted rebootstrap attempts must not advance the local replay cursor"
    );

    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET snapshot = $1, checkpoint_sha256 = $2 WHERE vol = $3",
                    &[
                        &valid_checkpoint.snapshot,
                        &valid_checkpoint.sha256.as_slice(),
                        &volume_id,
                    ],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("restore valid checkpoint");

    reopened
        .sync_metadata()
        .expect("restart should rebootstrap after checkpoint recovery");
    assert_eq!(reopened.local_version.load(Ordering::Acquire), 2);
    assert_eq!(reopened.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    for name in [
        b"old-complete-base".as_slice(),
        b"new-complete-base",
        b"unflushed-pending.bin",
    ] {
        reopened
            .lookup(ROOT_INO, name)
            .expect("recovered checkpoint and replay tail must be complete");
    }
    reopened
        .flush_metadata()
        .expect("recovered client must flush with its preserved local overlay");
    let (remote_version, remote_events) = read_pg_event_batch(&dsn, volume.volume_id, 3);
    assert_eq!(remote_version, 2);
    assert!(
        remote_events.is_empty(),
        "an uncommitted regular-file overlay must remain local-only"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_reaches_live_at_frozen_target_while_remote_head_advances() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _page_limit = set_test_env("STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE", "1");
    let volume = PgTestVolume::new(&dsn, "pg-frozen-startup-target");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 0);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata client");

    for name in [b"backlog-1".as_slice(), b"backlog-2", b"backlog-3"] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create backlog event");
        publisher.flush_metadata().expect("publish backlog event");
    }

    stale.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    stale.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    let startup_target = stale
        .frozen_sync_target_version()
        .expect("capture frozen startup target");
    assert_eq!(startup_target, 3);

    let active_writer = std::sync::Arc::clone(&publisher);
    let observed_cursors = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_cursors_hook = std::sync::Arc::clone(&observed_cursors);
    stale.set_catch_up_fault_hook(std::sync::Arc::new(move |point| {
        if let CatchUpFaultPoint::BeforePageFetch { cursor } = point {
            observed_cursors_hook
                .lock()
                .expect("observe moving-head catch-up cursors")
                .push(cursor);
            let name = format!("moving-head-{cursor}");
            active_writer
                .create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .map_err(|error| {
                    MetaError::Storage(format!("moving-head publisher failed: {error}"))
                })?;
            active_writer.flush_metadata().map_err(|error| {
                MetaError::Storage(format!("moving-head publisher flush failed: {error}"))
            })?;
        }
        Ok(())
    }));

    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        stale
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("drain frozen startup target")
    );
    assert_eq!(stale.local_version.load(Ordering::Acquire), startup_target);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        observed_cursors
            .lock()
            .expect("read moving-head catch-up cursors")
            .as_slice(),
        &[0, 1, 2]
    );
    let remote_head = stale
        .remote_metadata_head_version()
        .expect("read remote head after moving-head catch-up");
    let cursor_count = observed_cursors
        .lock()
        .expect("count moving-head catch-up cursors")
        .len();
    assert_eq!(remote_head, startup_target + cursor_count as i64);

    stale.set_catch_up_fault_hook(std::sync::Arc::new(|_| Ok(())));
    let post_target_event_count = remote_head.saturating_sub(startup_target);
    for _ in 0..post_target_event_count {
        stale
            .sync_metadata()
            .expect("replicate moving-head runtime event");
    }
    assert_eq!(stale.local_version.load(Ordering::Acquire), remote_head);
    for cursor in 0..cursor_count {
        let name = format!("moving-head-{cursor}");
        stale
            .lookup(ROOT_INO, name.as_bytes())
            .expect("runtime replication should apply moving-head events");
    }
    for name in [b"backlog-1".as_slice(), b"backlog-2", b"backlog-3"] {
        stale
            .lookup(ROOT_INO, name)
            .expect("startup catch-up should apply backlog events");
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_retry_keeps_frozen_target_while_remote_head_advances() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let volume = PgTestVolume::new(&dsn, "pg-frozen-startup-retry-target");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(engine.local_version.load(Ordering::Acquire), 0);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open moving-head publisher");

    for name in [
        b"backlog-1".as_slice(),
        b"backlog-2",
        b"backlog-3",
        b"backlog-4",
    ] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create backlog event");
        publisher.flush_metadata().expect("publish backlog event");
    }

    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    let startup_target = engine
        .frozen_sync_target_version()
        .expect("capture frozen startup target");
    assert_eq!(startup_target, 4);

    let next_write = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1));
    let observed_cursors = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let injected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let active_writer = std::sync::Arc::clone(&publisher);
    let next_write_hook = std::sync::Arc::clone(&next_write);
    let observed_cursors_hook = std::sync::Arc::clone(&observed_cursors);
    let injected_hook = std::sync::Arc::clone(&injected);
    engine.set_catch_up_fault_hook(std::sync::Arc::new(move |point| {
        if let CatchUpFaultPoint::BeforePageFetch { cursor } = point {
            observed_cursors_hook
                .lock()
                .expect("observe retry catch-up cursors")
                .push(cursor);
            let write_number = next_write_hook.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let name = format!("retry-moving-head-{write_number}");
            active_writer
                .create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .map_err(|error| {
                    MetaError::Storage(format!("moving-head publisher failed: {error}"))
                })?;
            active_writer.flush_metadata().map_err(|error| {
                MetaError::Storage(format!("moving-head publisher flush failed: {error}"))
            })?;
            if cursor == 1 && !injected_hook.swap(true, std::sync::atomic::Ordering::AcqRel) {
                return Err(MetaError::Timeout(
                    "injected temporary catch-up failure".to_string(),
                ));
            }
        }
        Ok(())
    }));
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("temporary catch-up failure must interrupt the first session");
    match error {
        MetaError::Timeout(_) => {}
        unexpected => panic!("expected temporary catch-up timeout, got {unexpected:?}"),
    }
    assert_eq!(engine.local_version.load(Ordering::Acquire), 1);
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::CatchingUp
    );
    assert_eq!(
        engine.pg_sync_diagnostics().startup_target_version,
        Some(startup_target as u64)
    );
    assert!(
        engine
            .remote_metadata_head_version()
            .expect("read moving remote head after failure")
            > startup_target
    );

    assert!(
        engine
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("retry frozen-target catch-up")
    );
    assert_eq!(engine.local_version.load(Ordering::Acquire), startup_target);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(engine.pg_sync_diagnostics().startup_target_version, None);
    assert_eq!(
        observed_cursors
            .lock()
            .expect("read retry catch-up cursors")
            .as_slice(),
        &[0, 1, 1, 2, 3]
    );

    let remote_head = engine
        .remote_metadata_head_version()
        .expect("read remote head after retry");
    let published_count = (next_write.load(std::sync::atomic::Ordering::Acquire) - 1) as i64;
    assert_eq!(remote_head, startup_target + published_count);
    engine.set_catch_up_fault_hook(std::sync::Arc::new(|_| Ok(())));
    let post_target_event_count = remote_head.saturating_sub(startup_target);
    for _ in 0..post_target_event_count {
        engine
            .sync_metadata()
            .expect("runtime replication should consume post-target event");
    }
    assert_eq!(engine.local_version.load(Ordering::Acquire), remote_head);
    for write_number in 1..next_write.load(std::sync::atomic::Ordering::Acquire) {
        let name = format!("retry-moving-head-{write_number}");
        engine
            .lookup(ROOT_INO, name.as_bytes())
            .expect("runtime replication should apply every post-target event");
    }
    for name in [
        b"backlog-1".as_slice(),
        b"backlog-2",
        b"backlog-3",
        b"backlog-4",
    ] {
        engine
            .lookup(ROOT_INO, name)
            .expect("startup catch-up retry should apply backlog events");
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn fresh_client_empty_volume_reaches_live_at_zero_target() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-fresh-client-empty-volume");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("fresh.redb");
    seed_local(&db, &volume.format_info());

    let fresh = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )
    .expect("fresh empty volume should open with local serving state");

    assert_eq!(fresh.local_version.load(Ordering::Acquire), 0);
    assert_eq!(
        fresh.pg_sync_status().lifecycle,
        PgSyncLifecycle::CatchingUp
    );
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&fresh.local)
            .expect("read missing fresh-client slot"),
        None
    );

    fresh
        .sync_metadata()
        .expect("zero-version startup sync should complete the fresh client");

    assert_eq!(fresh.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(fresh.pg_sync_status().allows_new_mutations());
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read cleared fresh-client marker"),
        0
    );
    assert!(
        PgMetaEngine::load_persisted_client_slot(&fresh.local)
            .expect("read assigned fresh-client slot")
            .is_some()
    );
}

#[test]
fn prepare_for_close_does_not_block_forever_on_slow_background_thread() {
    let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
    let (_dir, engine) = temp_pg_engine_for_unit();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("pg-test-slow-close".into())
        .spawn(move || {
            started_tx.send(()).expect("signal slow worker start");
            release_rx.recv().expect("release slow worker");
        })
        .expect("spawn slow background thread");
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");
    started_rx.recv().expect("wait for slow worker start");

    let started = Instant::now();
    let first_error = engine
        .prepare_for_close()
        .expect_err("slow worker should make close report a timeout");

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "prepare_for_close should respect the bounded join timeout for slow threads"
    );
    let second_error = engine
        .prepare_for_close()
        .expect_err("repeated close should replay the first timeout");
    assert_eq!(first_error.to_string(), second_error.to_string());
    release_tx.send(()).expect("release slow background worker");
}

#[test]
fn prepare_for_close_joins_checkpoint_worker_before_releasing_redb() {
    let (dir, engine) = temp_pg_engine_for_unit();
    let local = Arc::clone(&engine.local);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("pg-test-slow-checkpoint-close".into())
        .spawn(move || {
            started_tx.send(()).expect("signal checkpoint worker start");
            release_rx.recv().expect("release checkpoint worker");
            drop(local);
        })
        .expect("spawn slow checkpoint worker");
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Checkpoint, handle)
        .expect("install slow checkpoint worker");
    started_rx.recv().expect("wait for checkpoint worker start");

    let engine = Arc::new(engine);
    let closing_engine = Arc::clone(&engine);
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let close_thread = std::thread::spawn(move || {
        let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
        result_tx
            .send(closing_engine.prepare_for_close())
            .expect("send checkpoint close result");
    });

    assert!(
        result_rx.recv_timeout(Duration::from_millis(150)).is_err(),
        "checkpoint close must not release its worker after the bounded deadline"
    );
    release_tx.send(()).expect("release checkpoint worker");
    result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("checkpoint close should finish after worker release")
        .expect("checkpoint worker should join successfully");
    close_thread.join().expect("join checkpoint close thread");
    drop(engine);

    let reopened = RedbMetaEngine::open(dir.path().join("meta.redb"));
    assert!(
        reopened.is_ok(),
        "redb should be reopenable after checkpoint worker join"
    );
}

#[test]
fn prepare_for_close_serializes_lazy_worker_registration() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let start_guard = engine
        .lock_worker_start_lock
        .lock()
        .expect("worker start lock");
    let state_guard = engine
        .background_workers
        .state
        .lock()
        .expect("background worker state lock");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let closing_engine = engine.clone();
    let close_thread = std::thread::spawn(move || {
        started_tx.send(()).expect("signal close start");
        result_tx
            .send(closing_engine.prepare_for_close())
            .expect("send close result");
    });
    started_rx.recv().expect("wait for close start");

    let handle = std::thread::spawn(|| {});
    drop(state_guard);

    if result_rx.recv_timeout(Duration::from_millis(250)).is_ok() {
        drop(start_guard);
        close_thread.join().expect("join close thread");
        panic!("close completed before the spawned worker was registered");
    }

    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("register worker before releasing the start lock");
    drop(start_guard);

    result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("close should finish after worker registration")
        .expect("close should join the registered worker");
    close_thread.join().expect("join close thread");
}

#[test]
fn shutdown_releases_worker_state_before_joining_workers() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        release_rx.recv().expect("release slow background worker");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");

    let closing_engine = engine.clone();
    let close_thread = std::thread::spawn(move || {
        closing_engine
            .background_workers
            .shutdown(Duration::from_secs(30))
    });

    let deadline = Instant::now() + Duration::from_secs(1);
    let observed_shutdown_start = loop {
        match engine.background_workers.state.try_lock() {
            Ok(state) if state.shutdown_complete => break true,
            Ok(_) | Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("background worker state lock poisoned")
            }
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };

    if !observed_shutdown_start {
        release_tx.send(()).expect("release slow background worker");
        let _ = close_thread.join().expect("join close thread");
        panic!("shutdown should publish its terminal state before joining workers");
    }

    let handle = std::thread::spawn(|| {});
    let install_result = engine
        .background_workers
        .install_handle(PgBackgroundWorker::MetadataNotify, handle);
    assert!(
        install_result.is_err(),
        "registration after shutdown must be rejected without waiting for join"
    );

    release_tx.send(()).expect("release slow background worker");
    close_thread
        .join()
        .expect("join close thread")
        .expect("shutdown should join the released worker");
}

#[test]
fn concurrent_shutdown_callers_wait_for_terminal_result() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        release_rx.recv().expect("release slow background worker");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");

    let (first_result_tx, first_result_rx) = std::sync::mpsc::channel();
    let first_engine = engine.clone();
    let first = std::thread::spawn(move || {
        first_result_tx
            .send(
                first_engine
                    .background_workers
                    .shutdown(Duration::from_secs(5)),
            )
            .expect("send first shutdown result");
    });

    let deadline = Instant::now() + Duration::from_secs(1);
    let observed_shutdown_start = loop {
        match engine.background_workers.state.try_lock() {
            Ok(state) if state.shutdown_complete => break true,
            Ok(_) | Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("background worker state lock poisoned")
            }
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };

    if !observed_shutdown_start {
        release_tx.send(()).expect("release slow background worker");
        first.join().expect("join first shutdown thread");
        panic!("first shutdown should publish its state before joining workers");
    }

    let (second_result_tx, second_result_rx) = std::sync::mpsc::channel();
    let second_engine = engine.clone();
    let second = std::thread::spawn(move || {
        second_result_tx
            .send(
                second_engine
                    .background_workers
                    .shutdown(Duration::from_secs(5)),
            )
            .expect("send second shutdown result");
    });

    let second_early_result = second_result_rx
        .recv_timeout(Duration::from_millis(100))
        .ok();
    let second_returned_early = second_early_result.is_some();
    release_tx.send(()).expect("release slow background worker");
    first_result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first shutdown should finish")
        .expect("first shutdown should succeed");
    if let Some(result) = second_early_result {
        result.expect("second shutdown should succeed");
    } else {
        second_result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second shutdown should finish after the first")
            .expect("second shutdown should replay the terminal result");
    }
    first.join().expect("join first shutdown thread");
    second.join().expect("join second shutdown thread");

    assert!(
        !second_returned_early,
        "a concurrent shutdown caller must wait for the terminal result"
    );
}

#[test]
fn metadata_notify_start_skips_start_lock_when_already_registered() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let handle = std::thread::spawn(|| {});
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::MetadataNotify, handle)
        .expect("register metadata notify worker");
    let engine = std::sync::Arc::new(engine);
    let start_guard = engine
        .lock_worker_start_lock
        .lock()
        .expect("worker start lock");
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let calling_engine = engine.clone();
    let caller = std::thread::spawn(move || {
        result_tx
            .send(calling_engine.ensure_metadata_notify_listener_started())
            .expect("send metadata notify start result");
    });

    let early_result = result_rx.recv_timeout(Duration::from_millis(250));
    let completed_without_waiting = early_result.is_ok();
    drop(start_guard);
    let result = match early_result {
        Ok(result) => result,
        Err(_) => result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("metadata notify start should finish after releasing the lock"),
    };
    caller.join().expect("join metadata notify start");

    assert!(
        completed_without_waiting,
        "an already registered metadata notify worker should not wait for the start lock"
    );
    result.expect("already registered metadata notify worker should be a no-op");
}

#[test]
fn postgres_startup_reachability_classification_keeps_timeout_typed() {
    assert!(MetaError::RemoteUnavailable("connection refused".to_string()).is_reachability_error());
    assert!(
        !MetaError::Storage("postgres connect failed: error connecting to server".to_string())
            .is_reachability_error()
    );
    let connection_timeout = PgMetaEngine::pg_timeout_error(
        PgTimeoutContext::Connection("validate postgres schema connect"),
        Duration::from_millis(10),
    );
    assert!(connection_timeout.is_reachability_error());
    assert!(matches!(connection_timeout, MetaError::Timeout(_)));

    let command_timeout = PgMetaEngine::pg_timeout_error(
        PgTimeoutContext::Command("validate postgres schema"),
        Duration::from_millis(10),
    );
    assert!(command_timeout.is_reachability_error());
    assert!(matches!(command_timeout, MetaError::Timeout(_)));
}

#[test]
fn prepare_for_close_is_idempotent_when_no_workers_are_running() {
    let (_dir, engine) = temp_pg_engine_for_unit();

    engine
        .prepare_for_close()
        .expect("first close should succeed without workers");
    engine
        .prepare_for_close()
        .expect("repeated close should remain successful");
}

#[test]
fn dropping_engine_signals_background_workers_without_waiting() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let shutdown = engine.background_workers.poll_shutdown();
    let (observed_tx, observed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        observed_tx.send(()).expect("signal worker stop");
        release_rx.recv().expect("release worker");
        finished_tx.send(()).expect("signal worker finish");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install worker");

    let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
    let drop_thread = std::thread::spawn(move || {
        drop(engine);
        dropped_tx.send(()).expect("signal engine drop");
    });

    let observed_stop = observed_rx.recv_timeout(Duration::from_secs(1)).is_ok();
    let dropped_without_waiting =
        observed_stop && dropped_rx.recv_timeout(Duration::from_secs(1)).is_ok();
    release_tx.send(()).expect("release background worker");
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("background worker should finish after release");
    drop_thread.join().expect("join engine drop thread");

    assert!(
        observed_stop,
        "background worker should observe drop cancellation"
    );
    assert!(
        dropped_without_waiting,
        "dropping an engine must not wait for a background worker"
    );
}

#[test]
fn open_fails_when_pending_local_event_log_is_corrupt() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "corrupt-local-event-log",
            "corrupt-local-event-log-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    local
        .replace_allocator_state(
            AllocatorStateV1::new(
                AllocatorRecord::new(1, 0).expect("inode namespace"),
                AllocatorRecord::new(2, 0).expect("payload namespace"),
            )
            .expect("allocator state"),
        )
        .expect("persist allocator state");

    let event_seq = local.get_counter("nextEventSeq").expect("event start");
    local
        .create_file(ROOT_INO, b"pending-local-file.bin", 0, 0, 0o644)
        .expect("create pending local file");
    local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, event_seq)
        .expect("persist unflushed event start");
    let txn = local
        .begin_write_txn()
        .expect("begin corruption transaction");
    {
        let event_log = redb::TableDefinition::<u64, &[u8]>::new("event_log");
        let mut events = txn.open_table(event_log).expect("open event log");
        events
            .insert(event_seq, b"invalid-event".as_slice())
            .expect("corrupt pending event");
    }
    txn.commit().expect("commit corrupted event");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    );

    let Err(error) = reopened else {
        panic!("startup must reject an unreadable pending local event");
    };
    assert!(
        matches!(&error, MetaError::Serialization(_)),
        "unsafe pending local state must be reported as a typed decode failure; got: {error:?}"
    );

    let local = RedbMetaEngine::open(&path).expect("reopen local metadata after rejection");
    local
        .lookup(ROOT_INO, b"pending-local-file.bin")
        .expect("unsafe local overlay must remain available for recovery");
    let snapshot = local
        .export_snapshot()
        .expect("export rejected local metadata for recovery");
    assert!(
        snapshot
            .event_log
            .iter()
            .any(|(seq, payload)| *seq == event_seq && payload.as_slice() == b"invalid-event"),
        "rejected pending state must retain the corrupt event-log row for recovery"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
            .expect("read pending event cursor"),
        event_seq,
        "rejected pending state must not advance its durable event cursor"
    );
    assert!(
        local
            .get_counter("nextEventSeq")
            .expect("read event-log end")
            > event_seq,
        "rejected pending state must retain the local event-log suffix"
    );
}

#[test]
fn startup_snapshot_preserves_local_overlay_when_local_overlay_exists() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create local draft");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("local overlay mutation"),
        "local draft namespace should register as a local overlay mutation"
    );
    assert_eq!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 0)
            .expect("startup snapshot decision"),
        Some(super::StartupSnapshotImportMode::PreserveLocalState),
        "startup snapshot must preserve a local overlay after crash/restart"
    );
}

#[test]
fn repair_startup_keeps_recovery_marker_when_remote_sync_fails() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "repair-startup-remote-failure",
            "repair-startup-remote-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set unavailable postgres config");
    local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 1)
        .expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 73)
        .expect("persist stale replay cursor");
    local
        .set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, 71)
        .expect("persist stale namespace replay cursor");
    local
        .set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)
        .expect("set repair recovery marker");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    );
    assert!(
        reopened.is_err(),
        "repair startup must fail rather than starting cleanup from stale local state"
    );

    let local = RedbMetaEngine::open(&db).expect("reopen local metadata");
    assert_eq!(
        local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .expect("read repair recovery marker"),
        1,
        "failed repair startup must leave the recovery marker durable for a later retry"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("read replay cursor"),
        0,
        "repair startup must reset stale replay cursor before attempting remote recovery"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER)
            .expect("read namespace replay cursor"),
        0,
        "repair startup must reset stale namespace replay cursor before attempting remote recovery"
    );
}

#[test]
fn fresh_client_startup_failure_is_degraded_and_marker_survives_reopen() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "fresh-client-startup-failure",
            "fresh-client-startup-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    drop(local);

    let fresh = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )
    .expect("fresh local view should survive unavailable PostgreSQL");
    let status = fresh.pg_sync_status();
    assert_eq!(status.lifecycle, PgSyncLifecycle::Degraded);
    assert!(!status.allows_new_mutations());
    assert!(
        fresh
            .remote_transport_unavailable
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        !fresh
            .remote_identity_validated
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1,
        "failed fresh startup must retain its durable write gate"
    );
    drop(fresh);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen failed fresh client locally");
    assert_eq!(
        reopened.pg_sync_status().lifecycle,
        PgSyncLifecycle::Degraded,
        "ordinary reopen must not mistake an unfinished fresh bootstrap for an established client"
    );
}

#[test]
fn regular_consistency_check_retries_after_startup_remote_transport_failure() {
    let (_dir, mut engine) = temp_pg_engine_for_unit();
    engine
        .remote_transport_unavailable
        .store(true, std::sync::atomic::Ordering::Release);
    engine.command_timeout = Duration::from_millis(1);
    engine.command_pool = Arc::new(
        PgConnectionPool::new(
            "postgres://127.0.0.1:1/studiofs".to_string(),
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
        )
        .expect("non-zero command pool size"),
    );

    let error = engine
        .check_consistency()
        .expect_err("regular consistency checks must retry remote identity probing");
    assert!(!matches!(
        error,
        MetaError::RemoteUnavailable(message)
            if message == "remote PostgreSQL transport is currently unavailable"
    ));
}

#[test]
fn remote_pull_requires_remote_identity_validation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_identity_validated
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .local_version
        .store(1, std::sync::atomic::Ordering::Release);

    let error = engine
        .pull_foreign_events_locked_to_target(None, 0)
        .expect_err("remote pulls must validate remote identity first");

    assert!(error.is_reachability_error());
    assert_eq!(
        engine
            .local_version
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "identity-pending remote pull must not advance the local cursor"
    );
}

#[test]
fn allocator_ready_client_stays_read_only_until_identity_is_validated() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_identity_validated
        .store(false, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::TargetReached as u8,
        std::sync::atomic::Ordering::Release,
    );

    engine.refresh_pg_sync_lifecycle_for_allocator();
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::TargetReached,
        "allocator readiness must not bypass the identity gate"
    );

    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.refresh_pg_sync_lifecycle_for_allocator();
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn successful_remote_operation_clears_transport_unavailable() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_transport_unavailable
        .store(true, std::sync::atomic::Ordering::Release);

    engine
        .record_remote_operation_result::<()>(Ok(()))
        .expect("successful remote operation");

    assert!(
        !engine
            .remote_transport_unavailable
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );

    engine
        .record_remote_operation_result::<()>(Err(MetaError::UuidMismatch {
            local: "local-volume".to_string(),
            remote: "remote-volume".to_string(),
        }))
        .expect_err("remote identity mismatch");
    assert!(
        engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );

    engine
        .record_remote_operation_result::<()>(Err(MetaError::RemoteUnavailable(
            "test-postgres-dsn".to_string(),
        )))
        .expect_err("unavailable remote operation");
    assert!(
        !engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[test]
fn replay_gap_downgrades_live_client_and_resets_stale_startup_target() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Live);
    engine
        .metadata_startup_target_version
        .store(60, std::sync::atomic::Ordering::Release);

    let error = MetaError::ReplayGap {
        local_version: 5,
        first_available_version: 51,
        target_version: 60,
    };
    let returned = engine.record_remote_operation_result::<bool>(Err(error));

    assert!(matches!(returned, Err(MetaError::ReplayGap { .. })));
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        -1
    );
}

#[test]
fn metadata_sync_replay_gap_records_degraded_lifecycle() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Live);
    let result = Err(MetaError::ReplayGap {
        local_version: 5,
        first_available_version: 6,
        target_version: 60,
    });

    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &result, false);

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert!(
        engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[test]
fn metadata_sync_result_does_not_repeat_recorded_remote_failure() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let result = Err(MetaError::RemoteUnavailable(
        "remote unavailable".to_string(),
    ));

    let _ = engine.record_remote_operation_result::<bool>(result.clone());
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &result, true);

    assert!(
        engine
            .remote_format_validated
            .load(std::sync::atomic::Ordering::Acquire),
        "aggregate recording must not invalidate a failure already recorded by the remote operation"
    );
    assert_eq!(engine.pg_sync_diagnostics().sync_errors, 1);
}

#[test]
fn established_zero_version_startup_failure_is_degraded() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "established-zero-version-startup-failure",
            "established-zero-version-startup-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    drop(local);

    let opened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("established local view should survive unavailable PostgreSQL");

    assert_eq!(opened.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!opened.pg_sync_status().allows_new_mutations());
}

#[test]
fn deferred_mount_startup_uses_trusted_local_state_without_remote_probe() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "deferred-mount-startup",
            "deferred-mount-startup-uuid",
            "test-postgres-dsn",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    local
        .replace_allocator_state(
            AllocatorStateV1::new(
                AllocatorRecord::new(1, 0).expect("inode namespace"),
                AllocatorRecord::new(2, 0).expect("payload namespace"),
            )
            .expect("allocator state"),
        )
        .expect("persist allocator state");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist local version");
    drop(local);

    let opened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..Default::default()
        },
    )
    .expect("trusted local metadata should open without PostgreSQL");

    assert_eq!(opened.command_pool.stats().checkouts, 0);
    assert!(!opened.remote_format_validated.load(Ordering::Acquire));
    assert_eq!(
        opened.metadata_remote_head_version.load(Ordering::Acquire),
        -1
    );
    assert!(opened.allocator_runtime_available_for_test());
    assert!(opened.next_inode().is_ok());
}

#[test]
fn fresh_client_startup_without_client_slot_is_degraded_and_marker_survives_reopen() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "fresh-client-startup-without-slot",
            "fresh-client-startup-without-slot-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    drop(local);

    let fresh = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )
    .expect("fresh local view should survive unavailable PostgreSQL without a client slot");
    assert_eq!(fresh.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!fresh.pg_sync_status().allows_new_mutations());
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1,
        "failed fresh startup must retain its durable write gate"
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&fresh.local).expect("read client slot"),
        None,
        "unavailable PostgreSQL must not fabricate a client slot"
    );
    drop(fresh);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen failed fresh client locally without a client slot");
    assert_eq!(
        reopened.pg_sync_status().lifecycle,
        PgSyncLifecycle::Degraded,
        "ordinary reopen must retain the unfinished fresh bootstrap state"
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&reopened.local).expect("read client slot"),
        None,
        "reopen must not reserve a slot while PostgreSQL remains unavailable"
    );
}

#[test]
fn degraded_fresh_client_recovers_after_sync_reaches_remote_head() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: 5,
        first_available_version: 6,
        target_version: 7,
    });
    engine
        .try_complete_catchup_sync()
        .expect("complete degraded fresh-client sync");

    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::Live,
        "allocator-ready recovery should restore write authority"
    );
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
    assert!(!engine.pg_sync_diagnostics().rebootstrap_required);
}

#[test]
fn catching_up_client_reaches_live_after_sync_reaches_remote_head() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete established-client catch-up");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.pg_sync_status().allows_new_mutations());
}

#[test]
fn catchup_completion_uses_frozen_startup_target() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(9, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete catch-up at the frozen startup target");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn catchup_completion_requires_a_new_target_after_recovery() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .try_complete_catchup_sync()
        .expect("complete the first recovery cycle");

    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::Degraded as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(8, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(10, std::sync::atomic::Ordering::Release);
    engine
        .try_complete_catchup_sync()
        .expect("inspect the second recovery cycle");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

#[test]
fn concurrent_catchup_completion_preserves_live_lifecycle() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    let engine = std::sync::Arc::new(engine);

    let first = std::sync::Arc::clone(&engine);
    let second = std::sync::Arc::clone(&engine);
    let first = std::thread::spawn(move || first.try_complete_catchup_sync());
    let second = std::thread::spawn(move || second.try_complete_catchup_sync());
    first
        .join()
        .expect("first catch-up worker panicked")
        .expect("first catch-up");
    second
        .join()
        .expect("second catch-up worker panicked")
        .expect("second catch-up");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn catching_up_fresh_client_completes_slot_and_allocator_bootstrap() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete healthy fresh-client catch-up");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
}

#[test]
fn failed_fresh_allocator_bootstrap_keeps_recovery_marker() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .try_complete_catchup_sync()
        .expect("transient allocator bootstrap failure stays recoverable");

    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1,
        "failed allocator bootstrap must retain the durable recovery marker"
    );
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        7,
        "allocator recovery must retain the frozen target"
    );
}

fn fresh_allocator_recovery_fixture() -> (tempfile::TempDir, PgMetaEngine) {
    let (dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .write_invalid_allocator_state_for_test()
        .expect("persist invalid allocator state");
    engine.clear_allocator_runtime_for_test();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    (dir, engine)
}

#[test]
fn allocator_pending_retries_missing_allocator_authority() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();

    engine
        .try_complete_catchup_sync()
        .expect("first allocator bootstrap attempt");
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );

    engine
        .local
        .replace_allocator_state(
            AllocatorStateV1::new(
                AllocatorRecord::new(11, 0).expect("inode namespace"),
                AllocatorRecord::new(12, 0).expect("payload namespace"),
            )
            .expect("allocator state"),
        )
        .expect("persist recovered allocator state");

    engine
        .try_complete_catchup_sync()
        .expect("retry allocator bootstrap");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
    assert!(engine.next_inode().is_ok());
}

#[test]
fn allocator_pending_recovers_without_startup_target() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 0)
        .expect("clear fresh-client marker for existing client");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::AllocatorPending as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .metadata_startup_target_version
        .store(-1, std::sync::atomic::Ordering::Release);
    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(13, 0).expect("inode namespace"),
        AllocatorRecord::new(14, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || Ok(recovered_state)));

    engine
        .try_complete_catchup_sync()
        .expect("allocator-pending client recovers without a startup target");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.next_inode().is_ok());
}

#[test]
fn transient_allocator_bootstrap_recovers_on_background_retry() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(21, 0).expect("inode namespace"),
        AllocatorRecord::new(22, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    let hook_attempts = std::sync::Arc::clone(&attempts);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        if hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel) == 0 {
            Err(MetaError::RemoteUnavailable(
                "transient allocator bootstrap failure".to_string(),
            ))
        } else {
            Ok(recovered_state)
        }
    }));

    engine
        .try_complete_catchup_sync()
        .expect("first allocator bootstrap attempt");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Acquire), 1);
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );

    let sync_result = Ok(false);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Acquire),
        1,
        "backoff must suppress an immediate duplicate mint attempt"
    );
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        7
    );

    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.retry_allocator_bootstrap_now_for_test();
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);

    assert_eq!(attempts.load(std::sync::atomic::Ordering::Acquire), 2);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
    assert!(engine.next_inode().is_ok());
}

#[test]
fn permanent_allocator_bootstrap_failure_is_not_retried() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_attempts = std::sync::Arc::clone(&attempts);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Err(MetaError::Storage(
            "postgres allocator namespace exhausted".to_string(),
        ))
    }));

    engine
        .try_complete_catchup_sync()
        .expect("permanent allocator failure remains read-only");
    engine.retry_allocator_bootstrap_now_for_test();
    let sync_result = Ok(false);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);

    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Acquire),
        1,
        "permanent allocator failures must not mint another namespace pair"
    );
    assert_eq!(
        engine.pg_sync_diagnostics().last_sync_error,
        Some(PgSyncFailure::Storage)
    );
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
}

#[test]
fn allocator_bootstrap_does_not_hold_retry_lock_during_mint() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let engine = std::sync::Arc::new(engine);
    let hook_engine = std::sync::Arc::downgrade(&engine);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        hook_engine
            .upgrade()
            .expect("allocator engine still alive")
            .retry_allocator_bootstrap_now_for_test();
        Err(MetaError::RemoteUnavailable(
            "transient allocator bootstrap failure".to_string(),
        ))
    }));

    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker_engine = std::sync::Arc::clone(&engine);
    let worker = std::thread::spawn(move || {
        result_tx
            .send(worker_engine.try_complete_catchup_sync())
            .expect("send allocator bootstrap result");
    });
    result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("allocator bootstrap must not deadlock on its retry lock")
        .expect("allocator bootstrap retry remains recoverable");
    worker.join().expect("allocator bootstrap worker panicked");
}

#[test]
fn allocator_bootstrap_panic_releases_in_flight_state_and_can_retry() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 0)
        .expect("clear fresh-client marker for allocator-only retry");
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_attempts = std::sync::Arc::clone(&attempts);
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(23, 0).expect("inode namespace"),
        AllocatorRecord::new(24, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        if hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel) == 0 {
            panic!("simulated allocator bootstrap panic");
        }
        Ok(recovered_state)
    }));

    let first_attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine
            .try_complete_catchup_sync()
            .expect("first attempt reaches allocator bootstrap");
    }));
    assert!(first_attempt.is_err());
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);

    engine
        .try_complete_catchup_sync()
        .expect("allocator bootstrap can retry after panic");

    assert_eq!(attempts.load(std::sync::atomic::Ordering::Acquire), 2);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn fresh_client_marker_clear_failure_keeps_engine_read_only() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(25, 0).expect("inode namespace"),
        AllocatorRecord::new(26, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || Ok(recovered_state)));
    engine.set_catch_up_fault_hook(std::sync::Arc::new(|point| {
        if point == CatchUpFaultPoint::BeforeFreshClientMarkerClear {
            return Err(MetaError::Storage("marker clear failed".to_string()));
        }
        Ok(())
    }));

    let error = engine
        .try_complete_catchup_sync()
        .expect_err("marker clear failure must be surfaced");

    assert!(matches!(error, MetaError::Storage(_)));
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read retained fresh-client marker"),
        1
    );
}

#[test]
fn poisoned_allocator_retry_state_preserves_backoff() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let engine = std::sync::Arc::new(engine);
    let poisoned_engine = std::sync::Arc::clone(&engine);
    std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_engine
                .allocator_bootstrap_retry_state
                .lock()
                .expect("allocator retry state lock");
            panic!("poison allocator retry state");
        }));
    })
    .join()
    .expect("poisoning worker panicked outside catch_unwind");

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_attempts = std::sync::Arc::clone(&attempts);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Err(MetaError::RemoteUnavailable(
            "transient allocator bootstrap failure".to_string(),
        ))
    }));

    engine
        .try_complete_catchup_sync()
        .expect("first allocator bootstrap attempt");
    let sync_result = Ok(false);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);

    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Acquire),
        1,
        "poisoned retry state must retain the backoff gate"
    );
}

#[test]
fn allocator_bootstrap_retry_delay_is_bounded() {
    let next = PgMetaEngine::next_allocator_bootstrap_retry_delay(Duration::from_secs(1));
    assert!(next >= Duration::from_secs(2));
    assert!(next <= Duration::from_millis(2_500));
    let capped = PgMetaEngine::next_allocator_bootstrap_retry_delay(Duration::from_secs(30));
    assert!(capped >= Duration::from_millis(22_500));
    assert!(capped < Duration::from_secs(30));
}

#[test]
fn poisoned_allocator_runtime_does_not_restore_write_authority() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let poisoned_engine = std::sync::Arc::clone(&engine);
    std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_engine
                .allocator_runtime
                .lock()
                .expect("allocator runtime lock");
            panic!("poison allocator runtime");
        }));
    })
    .join()
    .expect("poisoning worker panicked outside catch_unwind");

    assert!(engine.bootstrap_allocator_state().is_err());
    let status = engine.pg_sync_status();
    assert_eq!(status.lifecycle, PgSyncLifecycle::AllocatorPending);
    assert!(!status.allows_new_mutations());
    assert!(!status.authority.inode_id_headroom);
    assert!(!status.authority.object_id_headroom);
}

#[test]
fn failed_allocator_bootstrap_survives_reopen_and_recovers() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let (dir, engine) = fresh_allocator_recovery_fixture();
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(|| {
        Err(MetaError::RemoteUnavailable(
            "transient allocator bootstrap failure".to_string(),
        ))
    }));
    engine
        .try_complete_catchup_sync()
        .expect("failed allocator bootstrap remains recoverable");
    drop(engine);

    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen failed fresh client locally");
    assert_eq!(
        reopened
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker after reopen"),
        1
    );
    assert!(!reopened.pg_sync_status().allows_new_mutations());

    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(31, 0).expect("inode namespace"),
        AllocatorRecord::new(32, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    reopened.set_allocator_bootstrap_hook(std::sync::Arc::new(move || Ok(recovered_state)));
    reopened
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    reopened
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    reopened
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    reopened
        .try_complete_catchup_sync()
        .expect("recover allocator authority after reopen");

    assert_eq!(reopened.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        reopened
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read cleared fresh-client marker"),
        0
    );
    assert!(reopened.next_inode().is_ok());
}

#[test]
fn snapshot_reachability_bootstrap_skips_after_success() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .snapshot_reachability_bootstrapped
        .store(true, std::sync::atomic::Ordering::Release);

    engine
        .backfill_current_reference_ledger_if_needed()
        .expect("completed reachability bootstrap should not contact PostgreSQL again");
}

#[test]
fn degraded_fresh_client_rehydrates_allocator_runtime_before_restoring_write_authority() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.clear_allocator_runtime_for_test();
    engine
        .inode_id_headroom_available
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .object_id_headroom_available
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete degraded fresh-client sync");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.allocator_runtime_available_for_test());
}

#[test]
fn degraded_fresh_client_without_client_slot_stays_degraded_at_remote_head() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 0)
        .expect("clear client slot");
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    assert!(
        engine.try_complete_catchup_sync().is_err(),
        "fresh-client recovery must not promote without a reserved client slot"
    );
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&engine.local).expect("read client slot"),
        None
    );
}

#[test]
fn catching_up_fresh_client_slot_failure_becomes_degraded() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 0)
        .expect("clear client slot");
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    assert!(engine.try_complete_catchup_sync().is_err());
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

#[test]
fn allocator_pending_fresh_client_slot_failure_becomes_degraded() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 0)
        .expect("clear client slot");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::AllocatorPending as u8,
        std::sync::atomic::Ordering::Release,
    );

    assert!(engine.try_complete_catchup_sync().is_err());
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
}

#[test]
fn degraded_fresh_client_stays_degraded_until_remote_head_is_reached() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(8, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(8, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("inspect incomplete degraded fresh-client sync");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
}

#[test]
fn degraded_fresh_client_recovers_to_allocator_pending_without_write_authority() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .inode_id_headroom_available
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete degraded fresh-client sync");

    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
}

#[test]
fn startup_snapshot_preserves_local_overlay_when_only_acl_changed() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .local
        .create_file(ROOT_INO, b"acl-only.txt", 0, 0, 0o644)
        .expect("create base file");
    let snapshot = pg.local.export_snapshot().expect("export base snapshot");
    pg.local
        .import_snapshot(&snapshot)
        .expect("seed imported base");

    assert!(
        !PgMetaEngine::has_any_local_overlay_mutation(&pg.local)
            .expect("no local overlay mutation before ACL edit")
    );

    pg.local
        .set_acl_rule(file.ino, "alice", AccessLevel::ReadOnly)
        .expect("set local ACL overlay");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("ACL overlay mutation"),
        "ACL-only local changes should require preserve-local-state startup import"
    );
}

#[test]
fn persisted_pg_version_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );
}

#[test]
fn startup_snapshot_is_skipped_for_existing_local_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 42)
            .expect("startup snapshot decision")
            .is_none(),
        "an existing local graph must catch up from its own cursor without importing a checkpoint"
    );
}

#[test]
fn startup_snapshot_replaces_acknowledged_local_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.local
        .create_dir(ROOT_INO, b"acknowledged", 0, 0, 0o755)
        .expect("create local directory");
    let event_end = pg.local.get_counter("nextEventSeq").expect("event end");
    pg.persist_flushed_event_seq(event_end)
        .expect("acknowledge local events");

    assert_eq!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 0)
            .expect("startup snapshot decision"),
        Some(super::StartupSnapshotImportMode::ReplaceLocalState),
        "acknowledged local events must not hide the authoritative remote snapshot"
    );
}

#[test]
fn startup_snapshot_replay_starts_from_snapshot_epoch_not_pg_head() {
    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(3, 9),
        3,
        "metadata_state.version is the remote head; serving import must resume from checkpoint_version"
    );
}

#[test]
fn startup_snapshot_replay_caps_snapshot_epoch_to_pg_head() {
    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(12, 9),
        9,
        "a checkpoint cursor must not advance past the advertised PG head"
    );
}

#[test]
fn startup_snapshot_replay_uses_previous_epoch_when_snapshot_has_no_cursor() {
    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(0, 9),
        0,
        "a serving checkpoint always carries an explicit checkpoint version"
    );
}

#[test]
fn bootstrap_checkpoint_descriptor_validates_and_exposes_replay_floor() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();

    descriptor
        .validate()
        .expect("complete bootstrap descriptor should validate");
    assert_eq!(descriptor.checkpoint_version, 3);
    assert_eq!(
        descriptor.remote_head_version, 3,
        "the remote head is separate from the checkpoint replay floor"
    );
}

#[test]
fn bootstrap_checkpoint_descriptor_validate_and_decode_returns_checkpoint() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();

    let checkpoint = descriptor
        .validate_and_decode()
        .expect("complete bootstrap descriptor should decode");

    assert_eq!(checkpoint, crate::snapshot::ServingCheckpoint::default());
}

#[test]
fn bootstrap_checkpoint_descriptor_debug_redacts_format_info() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();
    let debug = format!("{descriptor:?}");

    assert!(debug.contains("format_info_bytes: \"<redacted>\""));
    assert!(!debug.contains("test-postgres-dsn"));
}

fn valid_bootstrap_checkpoint_descriptor() -> crate::BootstrapCheckpointDescriptor {
    let format_info = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "test-postgres-dsn",
        7,
    );
    let checkpoint_bytes = postcard::to_stdvec(&crate::snapshot::ServingCheckpoint::default())
        .expect("encode serving checkpoint");
    let checkpoint_sha256 = Sha256::digest(&checkpoint_bytes).to_vec();

    crate::BootstrapCheckpointDescriptor {
        volume_id: 7,
        volume_uuid: "bootstrap-descriptor-uuid".to_string(),
        format_info_bytes: serde_json::to_vec(&format_info).expect("encode format info"),
        metadata_format_version: CURRENT_FORMAT_VERSION,
        remote_head_version: 3,
        checkpoint_version: 3,
        checkpoint_bytes,
        checkpoint_sha256,
        publication_state: "published".to_string(),
        publication_generation: 1,
    }
}

fn assert_descriptor_failure(
    descriptor: crate::BootstrapCheckpointDescriptor,
    expected: CheckpointDescriptorFailure,
) {
    match descriptor.validate() {
        Err(MetaError::InvalidCheckpointDescriptor { reason }) => {
            assert_eq!(reason, expected);
        }
        other => panic!("expected InvalidCheckpointDescriptor({expected:?}), got {other:?}"),
    }
}

#[test]
fn bootstrap_checkpoint_descriptor_rejects_invalid_invariants() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();

    let mut invalid = descriptor.clone();
    invalid.volume_id = 0;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::VolumeIdentity);

    let mut invalid = descriptor.clone();
    invalid.volume_uuid = "  ".to_string();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::VolumeIdentity);

    let mut invalid = descriptor.clone();
    invalid.metadata_format_version = CURRENT_FORMAT_VERSION + 1;
    assert!(matches!(
        invalid.validate(),
        Err(MetaError::UnsupportedFormatVersion { found, expected })
            if found == CURRENT_FORMAT_VERSION + 1 && expected == CURRENT_FORMAT_VERSION
    ));

    let mut invalid = descriptor.clone();
    invalid.checkpoint_version = -1;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Version);

    let mut invalid = descriptor.clone();
    invalid.checkpoint_version = 4;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Version);

    let mut invalid = descriptor.clone();
    invalid.remote_head_version = -1;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Version);

    let mut invalid = descriptor.clone();
    invalid.publication_state = "pending".to_string();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::PublicationState);

    let mut invalid = descriptor.clone();
    invalid.publication_generation = 0;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Generation);

    let mut invalid = descriptor.clone();
    invalid.checkpoint_sha256 = vec![0; 31];
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Checksum);

    let mut invalid = descriptor.clone();
    invalid.checkpoint_sha256 = vec![0; 32];
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Checksum);

    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = b"{".to_vec();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mut missing_postgres = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "postgres://unused",
        7,
    );
    missing_postgres.metadata.postgres = None;
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&missing_postgres).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mismatched_uuid = format_info("bootstrap-descriptor", "other-uuid", "postgres://unused", 7);
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&mismatched_uuid).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mut local_format = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "postgres://unused",
        7,
    );
    local_format.metadata.backend = MetadataBackend::RedbLocal;
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&local_format).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mismatched_volume = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "postgres://unused",
        8,
    );
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&mismatched_volume).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mut invalid = descriptor;
    invalid.checkpoint_bytes = b"invalid".to_vec();
    invalid.checkpoint_sha256 = Sha256::digest(&invalid.checkpoint_bytes).to_vec();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::CheckpointBytes);
}

#[test]
fn metadata_head_query_contains_checkpoint_version() {
    let sql = super::sql::SELECT_METADATA_HEAD_AND_CHECKPOINT_VERSION;

    assert!(sql.contains("SELECT version, checkpoint_version"));
    assert!(sql.contains("FROM metadata_state"));
    assert!(sql.contains("WHERE vol=$1"));
}

#[test]
fn checkpoint_freshness_sql_uses_persisted_scalar_bookkeeping() {
    let freshness = super::sql::SELECT_CHECKPOINT_FRESHNESS;
    for fragment in [
        "version",
        "checkpoint_version",
        "total_event_payload_bytes",
        "checkpoint_total_event_bytes",
        "checkpoint_updated_at",
        "FROM metadata_state",
        "WHERE vol = $1",
    ] {
        assert!(
            freshness.contains(fragment),
            "freshness query is missing {fragment}"
        );
    }

    let bump = super::sql::BUMP_METADATA_VERSION;
    assert!(bump.contains("total_event_payload_bytes = total_event_payload_bytes + $3"));
    assert!(bump.contains("RETURNING"));
    assert!(bump.contains("checkpoint_total_event_bytes"));

    let checkpoint = super::sql::UPSERT_METADATA_CHECKPOINT;
    assert!(
        checkpoint.contains("checkpoint_total_event_bytes = EXCLUDED.checkpoint_total_event_bytes")
    );
    assert!(checkpoint.contains("checkpoint_updated_at = now()"));
    for fragment in [
        "RETURNING",
        "version",
        "checkpoint_version",
        "total_event_payload_bytes",
        "checkpoint_total_event_bytes",
        "checkpoint_age_ms",
        "format_info_present",
        "metadata_format_version",
    ] {
        assert!(
            checkpoint.contains(fragment),
            "checkpoint upsert is missing {fragment}"
        );
    }
    assert!(!checkpoint.contains("EXCLUDED.checkpoint_version >= metadata_state.version"));
}

#[test]
fn fenced_checkpoint_sql_skips_identical_initialized_versions() {
    let checkpoint = super::sql::UPSERT_FENCED_METADATA_CHECKPOINT;

    assert!(checkpoint.contains("EXCLUDED.checkpoint_version > metadata_state.checkpoint_version"));
    assert!(checkpoint.contains("COALESCE(octet_length(metadata_state.format_info), 0) = 0"));
    assert!(
        !checkpoint.contains("EXCLUDED.checkpoint_version >= metadata_state.checkpoint_version")
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_fenced_checkpoint_does_not_republish_identical_initialized_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-idempotent");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");
    let owner = "checkpoint-idempotent-owner";
    let lease = pg
        .try_claim_checkpoint_lease(owner, Duration::from_secs(60))
        .expect("claim checkpoint lease")
        .expect("checkpoint lease should be available");
    let format_version = CURRENT_FORMAT_VERSION as i16;
    let version = 0_i64;
    let checkpoint_bytes = Vec::<u8>::new();
    let checkpoint_sha256 = vec![0_u8; 32];
    let format_info = Vec::<u8>::new();
    let published = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_FENCED_METADATA_CHECKPOINT,
                &[
                    &volume.volume_id,
                    &format_version,
                    &version,
                    &version,
                    &version,
                    &checkpoint_bytes,
                    &checkpoint_sha256,
                    &format_info,
                    &owner,
                    &lease.fencing_token,
                ],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<bool, MetaError>(row.is_some())
    }))
    .expect("attempt identical checkpoint publication");
    assert!(!published);

    let generation = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("read checkpoint generation");
    assert_eq!(generation, 1);
}

#[test]
fn bootstrap_checkpoint_query_contains_all_descriptor_columns() {
    let sql = super::sql::SELECT_BOOTSTRAP_CHECKPOINT_DESCRIPTOR;

    for fragment in [
        "v.vol",
        "v.uuid",
        "ms.metadata_format_version",
        "ms.version",
        "ms.checkpoint_version",
        "ms.snapshot",
        "ms.checkpoint_sha256",
        "ms.format_info",
        "ms.checkpoint_publication_state",
        "ms.checkpoint_generation",
        "FROM volumes AS v",
        "INNER JOIN metadata_state AS ms ON ms.vol = v.vol",
        "WHERE v.vol = $1",
    ] {
        assert!(
            sql.contains(fragment),
            "bootstrap query is missing {fragment}"
        );
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_snapshot_backend_reads_a_validated_bootstrap_descriptor() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-bootstrap-descriptor");
    let backend = PgSnapshotBackend::open(&dsn, volume.volume_id).expect("open snapshot backend");
    let descriptor = backend
        .read_bootstrap_checkpoint()
        .expect("read bootstrap checkpoint descriptor");

    assert_eq!(descriptor.volume_id, volume.volume_id);
    assert_eq!(descriptor.volume_uuid, volume.uuid);
    assert_eq!(descriptor.metadata_format_version, CURRENT_FORMAT_VERSION);
    assert_eq!(descriptor.remote_head_version, 0);
    assert_eq!(descriptor.checkpoint_version, 0);
    assert_eq!(descriptor.publication_state, "published");
    assert_eq!(descriptor.publication_generation, 1);
    assert_eq!(
        descriptor.checkpoint_sha256,
        Sha256::digest(&descriptor.checkpoint_bytes).to_vec()
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn invalid_bootstrap_descriptor_does_not_discard_healthy_pool_connection() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-bootstrap-descriptor-pool");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET checkpoint_publication_state = 'pending' WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("mark checkpoint publication pending");

    let before = engine.command_pool_stats();
    let error = engine.snapshot_backend().read_bootstrap_checkpoint();

    assert!(matches!(
        error,
        Err(MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::PublicationState
        })
    ));
    let after = engine.command_pool_stats();
    assert_eq!(
        after.discarded - before.discarded,
        0,
        "logical descriptor rejection should return a healthy connection to the pool"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_snapshot_skips_stale_checkpoint_without_downloading_payload() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-bootstrap-stale-checkpoint");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");
    engine.local_version.store(1, Ordering::SeqCst);

    let snapshot = b"invalid serving checkpoint".to_vec();
    let checksum = Sha256::digest(&snapshot).to_vec();
    let volume_id = volume.volume_id;
    run_async(with_pg_test_client(&dsn, move |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = $1, checkpoint_version = $2, snapshot = $3, checkpoint_sha256 = $4 WHERE vol = $5",
                &[&2_i64, &0_i64, &snapshot, &checksum, &volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("seed stale checkpoint state");

    assert!(
        !engine
            .pull_snapshot_locked(StartupSnapshotImportMode::PreserveLocalState)
            .expect("stale checkpoint should be skipped")
    );
}

#[test]
fn remote_snapshot_does_not_export_local_pg_state() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
    local
        .write_counters(|counters| {
            counters.set(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, 150)?;
            counters.set(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, 1_000)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, 1_100)?;
            counters.set(LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, 1_200)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER, 2_000)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER, 2_200)?;
            Ok(())
        })
        .expect("seed local pg counters");

    let snapshot = local
        .export_remote_snapshot()
        .expect("export remote snapshot");

    for local_counter in [
        LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER,
        LOCAL_PG_CLIENT_SLOT_COUNTER,
        LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER,
        LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER,
    ] {
        assert!(
            snapshot
                .counters
                .iter()
                .all(|(name, _)| name != local_counter),
            "remote discovery snapshots must not export local-only PG counter {local_counter}"
        );
    }
}

#[test]
fn import_remote_snapshot_drops_legacy_local_pg_state() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 3).expect("persist reserved local slot");

    let mut legacy_snapshot = RedbSnapshot::default();
    legacy_snapshot.counters.extend([
        (LOCAL_PG_VERSION_COUNTER.to_string(), 9),
        (LOCAL_PG_NAMESPACE_VERSION_COUNTER.to_string(), 8),
        (LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER.to_string(), 150),
        (LOCAL_PG_CLIENT_SLOT_COUNTER.to_string(), 8),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER.to_string(),
            1_000,
        ),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER.to_string(),
            1_100,
        ),
        (
            LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER.to_string(),
            1_200,
        ),
        (
            LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER.to_string(),
            2_000,
        ),
        (LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER.to_string(), 2_100),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER.to_string(),
            2_200,
        ),
    ]);

    local
        .import_remote_snapshot(&legacy_snapshot)
        .expect("import legacy remote snapshot");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("load client slot"),
        None,
        "remote snapshot import must not reuse the checkpoint writer slot"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
            .expect("load flushed cursor"),
        0,
        "remote snapshot import must drop legacy local event-log cursor"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("load applied version"),
        9,
        "remote replay cursor must remain part of the imported data snapshot"
    );
}

#[test]
fn snapshot_reconcile_resets_stale_regular_file_serving_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-stale-serving.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 227, 1);
    let local_payload = PayloadRef::new_partial(91_001, 227);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local payload"),
        "local publish should make the file unprotected"
    );
    let event_end = pg.local.get_counter("nextEventSeq").expect("event end");
    pg.flushed_event_seq.store(event_end, Ordering::SeqCst);

    let mut remote_attr = pg.get_attr(file.ino).expect("load published attr");
    remote_attr.size = 223;
    remote_attr.committed_size = 223;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    remote_attr.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .expect("apply remote base attr");

    assert_eq!(
        pg.get_attr(file.ino).expect("load stale serving attr").size,
        227,
        "base-only import can leave a stale serving attr until reconciliation"
    );

    pg.reconcile_imported_snapshot_regular_files_with_active_open_inodes(None)
        .expect("reconcile imported snapshot");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "snapshot reconciliation must reset stale regular-file serving attrs from base"
    );
}

#[test]
fn snapshot_rebootstrap_holds_active_open_lock_through_import() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.note_active_open_inode(ROOT_INO);

    let (captured_tx, captured_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
    let hook_release_rx = std::sync::Arc::clone(&release_rx);
    pg.set_snapshot_import_after_active_open_capture_hook(std::sync::Arc::new(move || {
        captured_tx.send(()).expect("signal active-open capture");
        hook_release_rx
            .lock()
            .expect("snapshot import hook release lock")
            .recv()
            .expect("release snapshot import hook");
    }));

    let pg = std::sync::Arc::new(pg);
    let worker_pg = std::sync::Arc::clone(&pg);
    let worker = std::thread::spawn(move || {
        worker_pg.import_serving_checkpoint_locked(
            StartupSnapshotImportMode::PreserveLocalState,
            &crate::snapshot::ServingCheckpoint::default(),
            0,
        )
    });

    captured_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("snapshot import should capture active opens");
    let note_pg = std::sync::Arc::clone(&pg);
    let (noted_tx, noted_rx) = std::sync::mpsc::channel();
    let note_worker = std::thread::spawn(move || {
        note_pg.note_active_open_inode(ROOT_INO);
        noted_tx.send(()).expect("signal active-open registration");
    });
    let note_completed_during_import = noted_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_ok();

    release_tx.send(()).expect("release snapshot import");
    worker
        .join()
        .expect("join snapshot import worker")
        .expect("snapshot import");
    note_worker.join().expect("join active-open registration");

    assert!(
        !note_completed_during_import,
        "active-open registration must wait for snapshot replacement"
    );
}

#[test]
fn snapshot_rebootstrap_preserves_active_open_inode_binding() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-open.txt", 0, 0, 0o644)
        .expect("create file");
    let entry = pg
        .lookup(ROOT_INO, b"snapshot-open.txt")
        .expect("lookup file");
    pg.local
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: file.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"snapshot-open.txt".to_vec(),
                entry,
            },
        ])
        .expect("publish file metadata to base");
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");
    pg.note_active_open_inode(file.ino);
    assert!(
        !pg.local
            .local_overlay_attr_inodes()
            .expect("local attr overlays")
            .contains(&file.ino)
    );
    assert!(
        !pg.local
            .local_overlay_inodes()
            .expect("local overlays")
            .contains(&file.ino)
    );

    pg.import_serving_checkpoint_locked(
        StartupSnapshotImportMode::PreserveLocalState,
        &crate::snapshot::ServingCheckpoint::default(),
        0,
    )
    .expect("import serving checkpoint");

    assert!(
        pg.get_attr(file.ino).is_ok(),
        "snapshot rebootstrap must retain metadata for active open handles"
    );
}

#[test]
fn snapshot_rebootstrap_waits_for_pending_mutation_lock() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let pg = std::sync::Arc::new(pg);
    let pending_guard = pg.pending_state.lock().expect("pending state");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker_pg = std::sync::Arc::clone(&pg);

    let worker = std::thread::spawn(move || {
        started_tx
            .send(())
            .expect("signal checkpoint import worker");
        result_tx
            .send(worker_pg.import_serving_checkpoint_locked(
                StartupSnapshotImportMode::PreserveLocalState,
                &crate::snapshot::ServingCheckpoint::default(),
                0,
            ))
            .expect("send checkpoint import result");
    });

    started_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("checkpoint import worker should start");
    assert!(
        result_rx
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "checkpoint import must wait for the pending mutation lock"
    );

    drop(pending_guard);
    result_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("checkpoint import should finish after the lock is released")
        .expect("checkpoint import");
    worker.join().expect("join checkpoint import worker");
}

#[test]
fn persisted_pg_flushed_event_seq_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("initial flushed event seq"),
        0
    );
    pg.persist_flushed_event_seq(19)
        .expect("persist flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 19);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        19
    );
}

#[test]
fn local_position_leaf_persistence_is_monotonic() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");
    pg.persist_local_version(17)
        .expect("ignore older local version");
    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );

    pg.persist_namespace_version(23)
        .expect("persist namespace version");
    pg.persist_namespace_version(11)
        .expect("ignore older namespace version");
    assert_eq!(pg.namespace_version.load(Ordering::SeqCst), 23);
    assert_eq!(
        PgMetaEngine::load_persisted_namespace_version(&pg.local)
            .expect("reload namespace version"),
        23
    );

    pg.persist_flushed_event_seq(25)
        .expect("persist flushed event seq");
    pg.persist_flushed_event_seq(19)
        .expect("ignore older flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 25);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        25
    );
}

#[test]
fn replay_position_does_not_regress_flushed_event_seq() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_flushed_event_seq(25)
        .expect("persist newer flushed event seq");
    pg.persist_replay_position(&RemoteReplayApplyOutcome {
        changed: true,
        max_version: 7,
        max_namespace_version: 3,
        flushed_event_seq: Some(19),
    })
    .expect("persist replay position");

    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 25);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        25,
        "replay position persistence must not move the local flushed cursor backwards"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_flush_advances_checkpoint_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-test");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    create_published_small_file(&pg, ROOT_INO, b"a.txt", 44_010);
    pg.flush_metadata().expect("flush metadata");
    let ok = wait_until(Duration::from_secs(10), || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_opt(
                    "SELECT version FROM metadata_state WHERE vol=$1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok::<bool, MetaError>(row.map(|r| r.get::<_, i64>(0) >= 1).unwrap_or(false))
        }))
        .unwrap_or(false)
    });
    assert!(ok, "expected metadata_state version >= 1");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_freshness_request_tracks_publication_and_reset() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "1");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "9223372036854775807");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "3600");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-freshness");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    let batch_version = pg
        .push_event_log_batch_locked(&[MetaEvent::RemoveAttr { ino: 42 }])
        .expect("publish event batch");
    let stale = pg.checkpoint_freshness_diagnostics();
    assert!(stale.refresh_needed);
    assert_eq!(
        stale.refresh_reason,
        Some(MetadataCheckpointRefreshReason::VersionLag)
    );
    assert_eq!(stale.head_version, Some(batch_version as u64));
    assert!(stale.event_payload_bytes_since_checkpoint > 0);

    pg.local_version.store(batch_version, Ordering::SeqCst);
    pg.write_checkpoint_with_lease()
        .expect("write fresh checkpoint");
    let fresh = pg.checkpoint_freshness_diagnostics();
    assert!(!fresh.refresh_needed);
    assert_eq!(fresh.checkpoint_version, Some(batch_version as u64));
    assert_eq!(fresh.event_payload_bytes_since_checkpoint, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_explicit_flush_defers_checkpoint_while_another_lease_owner_is_active() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-explicit-checkpoint-lease");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");

    create_published_small_file(&pg, ROOT_INO, b"lease-held.txt", 44_011);
    let owner = "other-checkpoint-owner";
    let lease = pg
        .try_claim_checkpoint_lease(owner, Duration::from_secs(60))
        .expect("claim competing checkpoint lease")
        .expect("competing owner should acquire checkpoint lease");
    let before = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation, checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("read checkpoint before explicit flush");

    pg.flush_metadata()
        .expect("explicit flush should defer checkpoint publication");

    let after = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation, checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("read checkpoint after explicit flush");
    assert_eq!(
        after, before,
        "explicit flush must not bypass the active lease"
    );

    pg.release_checkpoint_lease(owner, lease)
        .expect("release competing checkpoint lease");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_lease_takeover_advances_fence_and_rejects_old_owner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-lease");
    let first_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let first_db = first_dir.path().join("meta.redb");
    seed_local(&first_db, &volume.format_info());
    let first = PgMetaEngine::open_with_options(
        &first_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open first pg engine");

    let first_owner = "checkpoint-owner-a";
    let second_owner = "checkpoint-owner-b";
    let first_lease = first
        .try_claim_checkpoint_lease(first_owner, Duration::from_secs(60))
        .expect("claim first checkpoint lease")
        .expect("first owner should acquire checkpoint lease");
    assert_eq!(first_lease.fencing_token, 1);
    assert!(
        first
            .try_claim_checkpoint_lease(second_owner, Duration::from_secs(60))
            .expect("check active checkpoint lease")
            .is_none(),
        "a live checkpoint lease must exclude a peer"
    );

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE bootstrap_checkpoint_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("expire first checkpoint lease");

    let second_lease = first
        .try_claim_checkpoint_lease(second_owner, Duration::from_secs(60))
        .expect("claim takeover checkpoint lease")
        .expect("expired checkpoint lease should be taken over");
    assert!(second_lease.fencing_token > first_lease.fencing_token);

    let format_version = CURRENT_FORMAT_VERSION as i16;
    let version = 0_i64;
    let checkpoint_bytes = Vec::<u8>::new();
    let checkpoint_sha256 = vec![0_u8; 32];
    let format_info = Vec::<u8>::new();
    let old_owner_publish = run_async(with_pg_test_client(&dsn, move |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_FENCED_METADATA_CHECKPOINT,
                &[
                    &volume.volume_id,
                    &format_version,
                    &version,
                    &version,
                    &version,
                    &checkpoint_bytes,
                    &checkpoint_sha256,
                    &format_info,
                    &first_owner,
                    &first_lease.fencing_token,
                ],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<bool, MetaError>(row.is_some())
    }))
    .expect("attempt old-owner checkpoint publication");
    assert!(
        !old_owner_publish,
        "a builder with an expired fencing token must not publish"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_publishes_captured_cut_while_local_writes_continue_and_replays_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-captured-cut");
    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10000, total_event_payload_bytes = 10000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote head for checkpoint refresh");
    publisher
        .local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 10_000)
        .expect("persist the captured local version");
    publisher.local_version.store(10_000, Ordering::Release);
    publisher.observe_checkpoint_freshness();

    let cut = publisher
        .capture_checkpoint_source_cut()
        .expect("capture checkpoint source cut")
        .expect("checkpoint source should be eligible");
    let owner = publisher.current_client_id().expect("checkpoint owner");
    let lease = publisher
        .try_claim_checkpoint_lease(&owner, Duration::from_secs(60))
        .expect("claim checkpoint lease")
        .expect("checkpoint lease should be available");

    let stop_writer = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop_writer);
    let writer_engine = Arc::clone(&publisher);
    let writer_start = Arc::new(std::sync::Barrier::new(2));
    let writer_start_gate = Arc::clone(&writer_start);
    let (first_write_tx, first_write_rx) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        let first_name = "checkpoint-cut-write-0";
        writer_engine
            .create_dir(ROOT_INO, first_name.as_bytes(), 0, 0, 0o755)
            .expect("write after checkpoint cut");
        first_write_tx
            .send(first_name.to_string())
            .expect("report first writer commit");
        let mut writes = 1;
        writer_start_gate.wait();
        for index in 1..128 {
            if writer_stop.load(Ordering::Acquire) {
                break;
            }
            let name = format!("checkpoint-cut-write-{index}");
            writer_engine
                .create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .expect("write during checkpoint publication");
            writes += 1;
        }
        writes
    });
    let first_write = first_write_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("writer should commit after the captured cut");
    writer_start.wait();

    let publish_result = publisher.build_and_publish_checkpoint(&owner, lease, cut);
    stop_writer.store(true, Ordering::Release);
    let writer_count = writer.join().expect("join checkpoint writer");
    publisher
        .release_checkpoint_lease(&owner, lease)
        .expect("release checkpoint lease");
    assert!(
        publish_result.expect("publish captured checkpoint cut"),
        "a local write after capture must not invalidate the checkpoint cut"
    );
    assert!(
        writer_count > 0,
        "writer workload must run during publication"
    );

    let checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    let checkpoint_state: crate::snapshot::ServingCheckpoint =
        postcard::from_bytes(&checkpoint.snapshot).expect("decode published checkpoint");
    assert!(
        checkpoint_state.dentries.iter().all(|(key, _)| {
            !key.windows(first_write.len())
                .any(|window| window == first_write.as_bytes())
        }),
        "the checkpoint bytes must remain at the captured cut"
    );

    let _recent_flush = {
        let _guard = publisher
            .replication_lock
            .lock()
            .expect("lock local replication for event-tail publication");
        publisher
            .ensure_remote_format_validated()
            .expect("validate remote format before event-tail publication");
        publisher
            .replay_local_upload_ledger()
            .expect("replay local upload ledger before event-tail publication");
        let recent_flush = if let Some(batch) = publisher.claim_pending_batch(true) {
            publisher
                .flush_pending_claimed_batch_with_restore(batch)
                .expect("publish the local event tail after the checkpoint")
        } else {
            None
        };
        publisher
            .pull_and_reconcile_foreign_events_locked(recent_flush.as_ref())
            .expect("reconcile after event-tail publication");
        recent_flush
    };
    let (remote_head, checkpoint_version) =
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT version, checkpoint_version FROM metadata_state WHERE vol = $1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
        }))
        .expect("read checkpoint and replay target");
    assert!(remote_head > checkpoint_version);
    assert_eq!(checkpoint_version, 10_000);

    let cold_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let cold_db = cold_dir.path().join("cold.redb");
    seed_local(&cold_db, &volume.format_info());
    let cold = PgMetaEngine::open_with_options(
        &cold_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open cold client");
    cold.sync_metadata()
        .expect("restore checkpoint and replay the post-cut event tail");
    assert_eq!(cold.local_version.load(Ordering::Acquire), remote_head);
    cold.lookup(ROOT_INO, first_write.as_bytes())
        .expect("cold client should restore the cut and replay the later write");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_background_checkpoint_coordinator_coalesces_two_clients_and_publishes_exact_cut() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-coordinator");
    let first_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let first_db = first_dir.path().join("meta.redb");
    seed_local(&first_db, &volume.format_info());
    let second_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let second_db = second_dir.path().join("meta.redb");
    seed_local(&second_db, &volume.format_info());

    let first = PgMetaEngine::open(&first_db).expect("open first background pg engine");
    let second = PgMetaEngine::open(&second_db).expect("open second background pg engine");

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10000, total_event_payload_bytes = 10_000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote head for checkpoint refresh");
    first.local_version.store(10_000, Ordering::Release);
    second.local_version.store(10_000, Ordering::Release);
    first.observe_checkpoint_freshness();
    second.observe_checkpoint_freshness();

    let published = wait_until(Duration::from_secs(15), || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_opt(
                    "SELECT checkpoint_generation FROM metadata_state WHERE vol = $1 AND checkpoint_version = 10000",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<bool, MetaError>(row.is_some_and(|row| row.get::<_, i64>(0) >= 2))
        }))
        .unwrap_or(false)
    });
    assert!(
        published,
        "background coordinator should publish a checkpoint"
    );

    first.prepare_for_close().expect("close first pg engine");
    second.prepare_for_close().expect("close second pg engine");

    let (generation, version, checkpoint_version, snapshot, checkpoint_sha256) =
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT checkpoint_generation, version, checkpoint_version, snapshot, checkpoint_sha256 FROM metadata_state WHERE vol = $1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(i64, i64, i64, Vec<u8>, Vec<u8>), MetaError>((
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
            ))
        }))
        .expect("read published checkpoint");
    assert_eq!(generation, 2, "coalesced clients must publish once");
    assert_eq!(version, 10_000);
    assert_eq!(checkpoint_version, 10_000);
    assert_eq!(checkpoint_sha256, Sha256::digest(&snapshot).to_vec());
    assert!(!first.checkpoint_freshness_diagnostics().refresh_needed);
    assert!(!second.checkpoint_freshness_diagnostics().refresh_needed);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_coordinator_rechecks_authoritative_freshness_after_coordination_miss() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-coordination-miss");
    let (_dir, engine) = open_pg_publisher(&volume);

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10000, total_event_payload_bytes = 10000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote head for checkpoint freshness");
    engine.local_version.store(10_000, Ordering::Release);
    engine.observe_checkpoint_freshness();
    assert!(
        engine.checkpoint_freshness_diagnostics().refresh_needed,
        "the initial remote head must require a checkpoint refresh"
    );

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET checkpoint_version = 10000, checkpoint_total_event_bytes = 10000, checkpoint_updated_at = now() WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("publish authoritative checkpoint state");

    assert!(
        engine.refresh_checkpoint_freshness_after_coordination_miss(),
        "a coordination miss must report fresh state after the peer publishes"
    );
    assert!(!engine.checkpoint_freshness_diagnostics().refresh_needed);
    engine.prepare_for_close().expect("close pg engine");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_coordinator_skips_unflushed_local_batch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-pending");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    pg.create_dir(ROOT_INO, b"pending-dir", 0, 0, 0o755)
        .expect("create unflushed local directory");
    assert!(
        pg.replication_backlog().pending_ops > 0,
        "local mutation should remain pending before the checkpoint attempt"
    );

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10_000, total_event_payload_bytes = 10_000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote metadata head");
    pg.local_version.store(10_000, Ordering::Release);
    pg.observe_checkpoint_freshness();

    assert!(
        !pg.run_checkpoint_refresh_once(Duration::from_secs(60))
            .expect("checkpoint refresh should defer pending local state"),
        "checkpoint refresh must not publish while local replication is pending"
    );
    {
        let mut pending_state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut pending_state);
    }
    assert!(
        !pg.run_checkpoint_refresh_once(Duration::from_secs(60))
            .expect("checkpoint refresh should defer an unflushed event log"),
        "checkpoint refresh must not publish while the local event log is unflushed"
    );
    let checkpoint_version = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("read checkpoint version");
    assert_eq!(checkpoint_version, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_promotion_preserves_post_capture_event_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-tail");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    let batch_version = pg
        .push_event_log_batch_locked(&[MetaEvent::RemoveAttr { ino: 43 }])
        .expect("publish event batch");
    assert!(batch_version > 0);

    let volume_id = volume.volume_id;
    let format_version = CURRENT_FORMAT_VERSION as i16;
    let version = 0_i64;
    let checkpoint_version = 0_i64;
    let checkpoint_total_event_bytes = 0_i64;
    let snapshot = Vec::<u8>::new();
    let checkpoint_sha256 = vec![0_u8; 32];
    let format_info = Vec::<u8>::new();
    let promoted = run_async(with_pg_test_client(&dsn, move |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_METADATA_CHECKPOINT,
                &[
                    &volume_id,
                    &format_version,
                    &version,
                    &checkpoint_version,
                    &checkpoint_total_event_bytes,
                    &snapshot,
                    &checkpoint_sha256,
                    &format_info,
                ],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<bool, MetaError>(row.is_some())
    }))
    .expect("promote checkpoint after event publication");
    assert!(
        promoted,
        "checkpoint promotion must not starve behind a newer head"
    );

    let state = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT version, checkpoint_version, total_event_payload_bytes, checkpoint_total_event_bytes FROM metadata_state WHERE vol=$1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(i64, i64, i64, i64), MetaError>((
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
        ))
    }))
    .expect("query checkpoint tail state");

    assert_eq!(state.0, batch_version);
    assert_eq!(state.1, 0);
    assert!(state.2 > state.3);
    assert_eq!(state.3, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_skip_records_remote_stale_observation() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "1");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "9223372036854775807");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "3600");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-stale-skip");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 1, total_event_payload_bytes = 10 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote metadata head");

    let refresh_result = pg
        .run_checkpoint_refresh_once(Duration::from_secs(60))
        .expect("skip checkpoint refresh while local state is stale");
    assert!(!refresh_result);
    let lease_exists = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM bootstrap_checkpoint_leases WHERE vol = $1)",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<bool, MetaError>(row.get(0))
    }))
    .expect("query stale checkpoint lease state");
    assert!(
        !lease_exists,
        "stale local state must not claim a checkpoint lease"
    );

    pg.write_checkpoint_with_lease()
        .expect("skip stale local checkpoint write");
    let diagnostics = pg.checkpoint_freshness_diagnostics();
    assert_eq!(diagnostics.head_version, Some(1));
    assert_eq!(diagnostics.checkpoint_version, Some(0));
    assert_eq!(diagnostics.event_payload_bytes_since_checkpoint, 10);
    assert!(diagnostics.refresh_needed);
    assert_eq!(
        diagnostics.refresh_reason,
        Some(MetadataCheckpointRefreshReason::VersionLag)
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_skip_when_remote_checkpoint_covers_local_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-current");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    pg.write_checkpoint_with_lease()
        .expect("skip already-current checkpoint write");

    let checkpoint_generation = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("query checkpoint generation");

    assert_eq!(checkpoint_generation, 1);
    let diagnostics = pg.checkpoint_freshness_diagnostics();
    assert_eq!(diagnostics.head_version, Some(0));
    assert_eq!(diagnostics.checkpoint_version, Some(0));
    assert!(!diagnostics.refresh_needed);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_does_not_move_remote_head_backwards() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-monotonic");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    pg.local_version.store(10, Ordering::SeqCst);
    pg.write_checkpoint_with_lease()
        .expect("write newer checkpoint");
    pg.local_version.store(5, Ordering::SeqCst);
    pg.write_checkpoint_with_lease()
        .expect("write stale checkpoint");

    let version = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT version FROM metadata_state WHERE vol=$1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("query checkpoint version");

    assert_eq!(
        version, 10,
        "stale checkpoint writes must not move the remote metadata head backwards"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_refuses_to_promote_legacy_format() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-legacy-format");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let legacy_format = (CURRENT_FORMAT_VERSION - 1) as i16;
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET metadata_format_version = $1 WHERE vol = $2",
                &[&legacy_format, &volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("restore legacy metadata format");

    let error = pg
        .write_checkpoint_with_lease()
        .expect_err("checkpoint upsert must reject a legacy metadata row");
    assert!(
        matches!(
            error,
            MetaError::UnsupportedFormatVersion {
                found,
                expected
            } if found == legacy_format as u16 && expected == CURRENT_FORMAT_VERSION
        ),
        "unexpected checkpoint format error: {error}"
    );

    let actual = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<i16, MetaError>(row.get(0))
    }))
    .expect("read legacy metadata format");
    assert_eq!(actual, legacy_format, "legacy format must not be promoted");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_event_publication_refuses_legacy_format() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-event-legacy-format");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let legacy_format = (CURRENT_FORMAT_VERSION - 1) as i16;
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET metadata_format_version = $1 WHERE vol = $2",
                &[&legacy_format, &volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("restore legacy metadata format");

    let error = pg
        .push_event_log_batch_locked(&[MetaEvent::SetAclRule {
            ino: ROOT_INO,
            principal: "alice".to_string(),
            access_level: AccessLevel::ReadOnly,
        }])
        .expect_err("event publication must reject a legacy metadata row");
    assert!(
        matches!(
            error,
            MetaError::UnsupportedFormatVersion {
                found,
                expected
            } if found == legacy_format as u16 && expected == CURRENT_FORMAT_VERSION
        ),
        "unexpected event publication format error: {error}"
    );

    let (version, event_count) = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT version, (SELECT count(*) FROM event_log WHERE vol = $1) FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("read legacy publication state");
    assert_eq!(
        version, 0,
        "legacy publication must not bump remote version"
    );
    assert_eq!(event_count, 0, "legacy publication must not insert events");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_repairs_missing_remote_format_info() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-format-info");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET format_info = decode('', 'hex') WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("clear remote format info");

    pg.write_checkpoint_with_lease()
        .expect("write checkpoint with format info");

    let actual = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT format_info FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<Vec<u8>, MetaError>(row.get(0))
    }))
    .expect("read repaired format info");

    assert_eq!(
        actual,
        serde_json::to_vec(&volume.format_info()).expect("serialize expected format info")
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_capacity_is_bounded_by_configured_pool_size() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _pool_env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-capacity");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 1,
            defer_remote_startup_probes: false,
        },
    )
    .expect("open pg engine with one command-pool slot");

    let before = engine.command_pool_stats();
    let pool = engine.command_pool.clone();
    let command_timeout = Duration::from_secs(5);
    let (first_result, second_result, observed_wait) = engine
        .run_async(async move {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();

            let first_pool = pool.clone();
            let first = tokio::spawn(async move {
                PgMetaEngine::with_postgres_client_timeout(
                    first_pool,
                    command_timeout,
                    "pool capacity first command",
                    move |_client| {
                        Box::pin(async move {
                            started_tx.send(()).expect("signal first command checkout");
                            release_rx.await.expect("release first command checkout");
                            Ok::<(), MetaError>(())
                        })
                    },
                )
                .await
            });

            started_rx
                .await
                .expect("first command should acquire a pool slot");

            let second_pool = pool.clone();
            let second = tokio::spawn(async move {
                PgMetaEngine::with_postgres_client_timeout(
                    second_pool,
                    command_timeout,
                    "pool capacity second command",
                    |_client| Box::pin(async { Ok::<(), MetaError>(()) }),
                )
                .await
            });

            let observed_wait = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if pool.stats().checkout_waits > before.checkout_waits {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();

            release_tx.send(()).expect("release first command checkout");
            let first_result = first.await.expect("join first command");
            let second_result = second.await.expect("join second command");
            Ok((first_result, second_result, observed_wait))
        })
        .expect("run pool capacity test commands");

    assert!(first_result.is_ok(), "first command should complete");
    assert!(second_result.is_ok(), "second command should complete");
    assert!(
        observed_wait,
        "second command should wait for the sole pool slot"
    );

    let after = engine.command_pool_stats();
    assert_eq!(
        after.max_size, 1,
        "configured command-pool size should be preserved"
    );
    assert_eq!(
        after.total, 1,
        "one-slot pool should retain one reusable connection"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_reuses_connection_for_repeated_remote_metadata_queries() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _pool_env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-reuse");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 2,
            defer_remote_startup_probes: false,
        },
    )
    .expect("open pg engine");

    let before = engine.command_pool_stats();
    assert_eq!(
        before.max_size, 2,
        "explicit command-pool size should be applied to the opened engine"
    );
    engine
        .remote_metadata_head_version()
        .expect("first metadata head query");
    engine
        .remote_metadata_head_version()
        .expect("second metadata head query");
    let after = engine.command_pool_stats();

    assert!(
        after.connection_creations - before.connection_creations <= 1,
        "repeated command-path queries should reuse one pooled PostgreSQL connection"
    );
    assert_eq!(
        after.checkouts - before.checkouts,
        2,
        "both metadata head queries should go through the command pool"
    );
    assert_eq!(
        after.discarded - before.discarded,
        0,
        "successful repeated queries should keep the pooled connection healthy"
    );
    assert_eq!(
        after.checkout_waits - before.checkout_waits,
        0,
        "serial repeated queries should not wait for a saturated command pool"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_discards_client_after_application_error() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-app-error");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    let before = engine.command_pool_stats();
    let command_pool = engine.command_pool.clone();
    let command_timeout = engine.command_timeout;
    let bad_query = engine.run_async(async move {
        PgMetaEngine::with_postgres_client_timeout(
            command_pool,
            command_timeout,
            "intentional application error",
            move |client| {
                Box::pin(async move {
                    client
                        .batch_execute("SELECT * FROM studiofs_missing_pool_test_table")
                        .await
                        .map_err(|e| {
                            PgMetaEngine::classify_pg_error("intentional application error", e)
                        })?;
                    Ok(())
                })
            },
        )
        .await
    });
    assert!(bad_query.is_err(), "bad query should fail");

    engine
        .remote_metadata_head_version()
        .expect("metadata head query after application error");
    let after = engine.command_pool_stats();

    assert_eq!(
        after.discarded - before.discarded,
        1,
        "application errors should discard the pooled client to avoid stale transaction state"
    );
    assert_eq!(
        after.connection_creations - before.connection_creations,
        1,
        "the next command should reconnect after the failed client is discarded"
    );
}
