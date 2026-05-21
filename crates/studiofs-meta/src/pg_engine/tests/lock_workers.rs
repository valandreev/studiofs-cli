use super::*;

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_passive_lock_monitoring_starts_notify_listener_but_defers_renew_worker() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-lock-workers-lazy-start");
    let info = volume.format_info();
    seed_local(&db, &info);

    let engine = PgMetaEngine::open(&db).expect("open engine");
    assert!(
        engine
            .lock_notify_handle
            .lock()
            .expect("lock notify handle")
            .is_none(),
        "open alone should not eagerly start the pg lock notify listener"
    );
    assert!(
        engine
            .lock_renew_handle
            .lock()
            .expect("lock renew handle")
            .is_none(),
        "open should still defer the pg lock renew worker until a lock is held"
    );

    engine
        .ensure_known_file_lock_monitoring()
        .expect("start passive lock monitoring");
    assert!(
        engine
            .lock_notify_handle
            .lock()
            .expect("lock notify handle after passive monitoring")
            .is_some(),
        "passive lock monitoring should start the pg lock notify listener"
    );
    assert!(
        engine
            .lock_renew_handle
            .lock()
            .expect("lock renew handle after passive monitoring")
            .is_none(),
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
            .lock_notify_handle
            .lock()
            .expect("lock notify handle after acquire")
            .is_some(),
        "first lock usage should start the pg lock notify listener"
    );
    assert!(
        engine
            .lock_renew_handle
            .lock()
            .expect("lock renew handle after acquire")
            .is_some(),
        "first lock usage should start the pg lock renew worker"
    );
}
