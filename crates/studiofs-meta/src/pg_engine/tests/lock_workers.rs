use super::*;

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_passive_lock_monitoring_starts_notify_listener_but_defers_renew_worker() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-lock-workers-lazy-start");
    let info = volume.format_info();
    seed_local(&db, &info);

    let engine = PgMetaEngine::open(&db).expect("open engine");
    assert!(
        !engine
            .background_workers
            .has_handle(PgBackgroundWorker::LockNotify),
        "open alone should not eagerly start the pg lock notify listener"
    );
    assert!(
        !engine
            .background_workers
            .has_handle(PgBackgroundWorker::LockRenew),
        "open should still defer the pg lock renew worker until a lock is held"
    );

    engine
        .ensure_known_file_lock_monitoring()
        .expect("start passive lock monitoring");
    assert!(
        engine
            .background_workers
            .has_handle(PgBackgroundWorker::LockNotify),
        "passive lock monitoring should start the pg lock notify listener"
    );
    assert!(
        !engine
            .background_workers
            .has_handle(PgBackgroundWorker::LockRenew),
        "passive lock monitoring should not eagerly start the pg lock renew worker"
    );

    let file = engine
        .create_file(ROOT_INO, b"lazy-lock-workers.txt", 0, 0, 0o644)
        .expect("create file");
    let acquired = engine
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, Duration::from_secs(2))
        .expect("acquire lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));
    assert!(
        engine
            .background_workers
            .has_handle(PgBackgroundWorker::LockNotify),
        "first lock usage should start the pg lock notify listener"
    );
    assert!(
        engine
            .background_workers
            .has_handle(PgBackgroundWorker::LockRenew),
        "first lock usage should start the pg lock renew worker"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_lock_notify_listener_releases_engine_after_drop() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-lock-notify-drop");
    seed_local(&db, &volume.format_info());

    let engine = PgMetaEngine::open(&db).expect("open engine");
    engine
        .ensure_known_file_lock_monitoring()
        .expect("start lock notify listener");
    assert!(wait_until(Duration::from_secs(10), || {
        let (started, finished) = engine
            .background_workers
            .handle_status(PgBackgroundWorker::LockNotify);
        started && !finished
    }));

    let weak = Arc::downgrade(&engine);
    drop(engine);

    assert!(
        wait_until(Duration::from_secs(3), || weak.upgrade().is_none()),
        "lock notify listener must not keep PgMetaEngine alive after the last external Arc drops"
    );
}
