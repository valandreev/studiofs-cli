use super::*;
use crate::engine::{CheckpointDescriptorFailure, MetaAcl, MetadataCheckpointRefreshReason};
use crate::redb_engine::RedbSnapshot;
use sha2::{Digest, Sha256};

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
        .run_startup_catch_up_session(&std::sync::atomic::AtomicBool::new(false))
        .expect_err("fatal lifecycle must not be reported as a successful sync");

    assert!(matches!(error, MetaError::LifecycleFatal));
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

    publisher
        .create_dir(ROOT_INO, b"post-target", 0, 0, 0o755)
        .expect("create post-target event");
    publisher
        .flush_metadata()
        .expect("publish post-target event");
    assert_eq!(
        stale
            .remote_metadata_head_version()
            .expect("read advanced remote head"),
        4
    );

    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        stale
            .run_startup_catch_up_session(&shutdown)
            .expect("drain frozen startup target")
    );
    assert_eq!(stale.local_version.load(Ordering::Acquire), startup_target);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);

    stale
        .sync_metadata()
        .expect("replicate post-target runtime event");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 4);
    stale
        .lookup(ROOT_INO, b"post-target")
        .expect("post-target event should become visible during runtime sync");
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
fn postgres_startup_fallback_keeps_timeout_typed() {
    assert!(PgMetaEngine::is_postgres_unavailable(
        &MetaError::RemoteUnavailable("connection refused".to_string())
    ));
    assert!(!PgMetaEngine::is_postgres_unavailable(&MetaError::Storage(
        "postgres connect failed: error connecting to server".to_string()
    )));
    let connection_timeout = PgMetaEngine::pg_timeout_error(
        PgTimeoutContext::Connection("validate postgres schema connect"),
        Duration::from_millis(10),
    );
    assert!(!PgMetaEngine::is_postgres_unavailable(&connection_timeout));
    assert!(matches!(connection_timeout, MetaError::Timeout(_)));

    let command_timeout = PgMetaEngine::pg_timeout_error(
        PgTimeoutContext::Command("validate postgres schema"),
        Duration::from_millis(10),
    );
    assert!(!PgMetaEngine::is_postgres_unavailable(&command_timeout));
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

    assert!(
        reopened.is_err(),
        "startup must not silently discard an unreadable pending local event"
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
        "checkpoint_event_bytes",
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
    assert!(bump.contains("checkpoint_event_bytes = checkpoint_event_bytes + $3"));

    let checkpoint = super::sql::UPSERT_METADATA_CHECKPOINT;
    assert!(checkpoint.contains("checkpoint_event_bytes = 0"));
    assert!(checkpoint.contains("checkpoint_updated_at = now()"));
    assert!(checkpoint.contains("EXCLUDED.checkpoint_version >= metadata_state.version"));
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

    pg.reconcile_imported_snapshot_regular_files()
        .expect("reconcile imported snapshot");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "snapshot reconciliation must reset stale regular-file serving attrs from base"
    );
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
    pg.write_checkpoint_locked()
        .expect("write fresh checkpoint");
    let fresh = pg.checkpoint_freshness_diagnostics();
    assert!(!fresh.refresh_needed);
    assert_eq!(fresh.checkpoint_version, Some(batch_version as u64));
    assert_eq!(fresh.event_payload_bytes_since_checkpoint, 0);
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
    pg.write_checkpoint_locked()
        .expect("write newer checkpoint");
    pg.local_version.store(5, Ordering::SeqCst);
    pg.write_checkpoint_locked()
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
        .write_checkpoint_locked()
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

    pg.write_checkpoint_locked()
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
