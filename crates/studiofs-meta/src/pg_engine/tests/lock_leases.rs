use super::*;

#[test]
fn open_with_persisted_allocator_state_does_not_require_file_lock_schema() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-sqlstate",
            "offline-reopen-sqlstate-uuid",
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
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open must not require file-lock schema before a lock operation");

    let lock_error = reopened
        .current_file_lock_lease(ROOT_INO)
        .expect_err("file-lock operations must initialize their own schema");
    assert!(
        lock_error.to_string().contains("postgres"),
        "lock schema failure should be reported at first lock use, got: {lock_error}"
    );
}

#[test]
fn file_lock_schema_failure_uses_backoff_before_retrying() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let first = pg
        .current_file_lock_lease(ROOT_INO)
        .expect_err("first lock operation should report schema ensure failure");
    assert!(
        first.to_string().contains("postgres"),
        "first failure should include the schema error, got: {first}"
    );

    let second = pg
        .current_file_lock_lease(ROOT_INO)
        .expect_err("second lock operation should use cached schema failure");
    let second = second.to_string();
    assert!(
        second.contains("recently failed"),
        "second failure should use the backoff fast path, got: {second}"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_lease_blocks_peer_until_release() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-blocks-peer");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"locked.txt", 50_201);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"locked.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(2);
    let acquired = a
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire exclusive lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

    let blocked = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b conflicting acquire result");
    assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));

    let released = a
        .release_file_lock_lease(file.ino)
        .expect("a release lease");
    assert!(matches!(
        released,
        FileLockLeaseReleaseOutcome::Released(None)
    ));

    let acquired_after_release = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b acquire after release");
    assert!(matches!(
        acquired_after_release,
        FileLockLeaseAcquireOutcome::Acquired(_)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_first_acquire_serializes_concurrent_racers() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-first-acquire-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"first-acquire-race.txt", 50_202);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"first-acquire-race.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(2);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);

    let acquire_a = std::thread::spawn(move || {
        barrier_a.wait();
        a2.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
    });
    let acquire_b = std::thread::spawn(move || {
        barrier_b.wait();
        b2.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
    });

    let outcome_a = acquire_a
        .join()
        .expect("join acquire a")
        .expect("acquire a");
    let outcome_b = acquire_b
        .join()
        .expect("join acquire b")
        .expect("acquire b");
    let acquired_count = [outcome_a, outcome_b]
        .into_iter()
        .filter(|outcome| matches!(outcome, FileLockLeaseAcquireOutcome::Acquired(_)))
        .count();
    assert_eq!(
        acquired_count, 1,
        "exactly one concurrent first-acquire attempt should win the lease row"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_open_recreates_missing_file_lock_lease_table() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-open-recreates-file-lock-lease-table");
    let info = volume.format_info();
    seed_local(&db, &info);
    drop_file_lock_lease_table(&dsn);

    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open pg engine after dropping file_lock_leases");
    let file = engine
        .create_file(ROOT_INO, b"recreated-lock-table.txt", 0, 0, 0o644)
        .expect("create file");

    let acquired = engine
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, Duration::from_secs(2))
        .expect("acquire lease after open-time schema ensure");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_lease_expires_for_peer() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-expiry");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"expiring.txt", 50_203);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"expiring.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_millis(150);
    let acquired = a
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

    std::thread::sleep(Duration::from_millis(350));

    let peer_acquired = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b acquire after expiry");
    assert!(matches!(
        peer_acquired,
        FileLockLeaseAcquireOutcome::Acquired(_)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_lease_renew_keeps_peer_blocked() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-renew");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"renew.txt", 50_204);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"renew.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(3);
    let acquired = a
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

    std::thread::sleep(Duration::from_millis(500));

    let renewed = a
        .renew_file_lock_lease(file.ino, ttl)
        .expect("a renew lease");
    assert!(
        matches!(renewed, FileLockLeaseRenewOutcome::Renewed(_)),
        "expected renewed lease, got {renewed:?}"
    );

    std::thread::sleep(Duration::from_secs(1));

    let blocked = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b conflicting acquire after renew");
    assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));

    std::thread::sleep(Duration::from_millis(3500));

    let acquired_after_renewed_expiry = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b acquire after renewed expiry");
    assert!(matches!(
        acquired_after_renewed_expiry,
        FileLockLeaseAcquireOutcome::Acquired(_)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_known_state_refreshes_from_notify() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-notify-refresh");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"notify.txt", 50_205);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"notify.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(2);
    a.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire lease");

    let blocked = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b conflicting acquire result");
    assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));
    assert!(
        b.known_file_lock_lease(file.ino).is_some(),
        "blocked peer should cache the known conflicting lease"
    );

    a.release_file_lock_lease(file.ino)
        .expect("a release lease");

    assert!(
        wait_until(Duration::from_secs(10), || b
            .known_file_lock_lease(file.ino)
            .is_none()),
        "peer known lock cache should clear after NOTIFY-driven refresh"
    );
}
