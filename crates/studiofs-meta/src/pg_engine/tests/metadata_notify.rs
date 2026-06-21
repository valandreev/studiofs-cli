use super::*;

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_sync_metadata_between_clients() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-sync");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create dir on a");
    a.flush_metadata().expect("flush a");
    assert!(
        wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
        "shared dir should become visible on b"
    );
    let entry = b.lookup(ROOT_INO, b"shared").expect("lookup on b");
    assert!(entry.ino > 0);
    assert_eq!(entry.kind, FileType::Directory);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_metadata_notify_listener_wakes_remote_sync_without_poll() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-metadata-notify-sync");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let channel = PgMetaEngine::metadata_notify_channel(volume.volume_id);
    let readiness_received = wait_until(Duration::from_secs(10), || {
        if b.metadata_invalidation_diagnostics().notifications_received > 0 {
            true
        } else {
            send_pg_notify(&dsn, &channel, "ready");
            false
        }
    });
    let readiness_diagnostics = b.metadata_invalidation_diagnostics();
    let (readiness_listener_started, readiness_listener_finished) = b
        .metadata_notify_handle
        .lock()
        .expect("metadata notify handle")
        .as_ref()
        .map(|handle| (true, handle.is_finished()))
        .unwrap_or((false, false));
    assert!(
        readiness_received,
        "metadata notify listener should receive a readiness notification before the metadata assertion; diagnostics={readiness_diagnostics:?}; listener_started={readiness_listener_started}; listener_finished={readiness_listener_finished}"
    );

    a.create_dir(ROOT_INO, b"notify-sync", 0, 0, 0o755)
        .expect("create dir on a");
    a.flush_metadata().expect("flush a");

    let visible = wait_until(Duration::from_secs(10), || {
        b.lookup(ROOT_INO, b"notify-sync").is_ok()
    });
    let diagnostics = b.metadata_invalidation_diagnostics();
    let (listener_started, listener_finished) = b
        .metadata_notify_handle
        .lock()
        .expect("metadata notify handle")
        .as_ref()
        .map(|handle| (true, handle.is_finished()))
        .unwrap_or((false, false));
    assert!(
        visible,
        "metadata NOTIFY should wake the remote peer without waiting for the long fallback poll; diagnostics={diagnostics:?}; listener_started={listener_started}; listener_finished={listener_finished}"
    );
    assert!(
        diagnostics.notifications_received > 0,
        "peer should count the metadata notification that woke sync"
    );
    assert!(
        diagnostics.last_successful_sync_unix_ms.is_some(),
        "peer should record the successful NOTIFY-driven metadata sync"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_metadata_notify_listener_start_syncs_missed_change() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-metadata-notify-missed");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b without listener");

    a.create_dir(ROOT_INO, b"missed-notify", 0, 0, 0o755)
        .expect("create dir on a");
    a.flush_metadata().expect("flush a");
    assert!(
        b.lookup(ROOT_INO, b"missed-notify").is_err(),
        "peer with listener disabled should not see the change before a sync"
    );

    b.ensure_metadata_notify_listener_started()
        .expect("start metadata notify listener");

    assert!(
        wait_until(Duration::from_secs(10), || b
            .lookup(ROOT_INO, b"missed-notify")
            .is_ok()),
        "listener startup should force an immediate sync for missed notifications"
    );
    assert!(
        wait_until(Duration::from_secs(10), || b
            .metadata_invalidation_diagnostics()
            .last_successful_sync_unix_ms
            .is_some()),
        "forced reconnect/start sync should update metadata sync diagnostics; diagnostics={:?}",
        b.metadata_invalidation_diagnostics()
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_metadata_notify_listener_releases_engine_after_drop() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-metadata-notify-drop");
    let info = volume.format_info();
    seed_local(&db, &info);

    let engine = PgMetaEngine::open(&db).expect("open engine");
    let channel = PgMetaEngine::metadata_notify_channel(volume.volume_id);
    assert!(
        wait_until(Duration::from_secs(10), || {
            if engine
                .metadata_invalidation_diagnostics()
                .notifications_received
                > 0
            {
                true
            } else {
                send_pg_notify(&dsn, &channel, "ready");
                false
            }
        }),
        "metadata notify listener should receive a readiness notification"
    );

    let weak = Arc::downgrade(&engine);
    drop(engine);

    assert!(
        wait_until(Duration::from_secs(3), || weak.upgrade().is_none()),
        "metadata notify listener must not keep PgMetaEngine alive after the last external Arc drops"
    );
}

#[test]
fn metadata_notify_channel_is_distinct_from_lock_notify_channel() {
    assert_eq!(
        PgMetaEngine::metadata_notify_channel(7),
        "studiofs_meta_vol_7"
    );
    assert_ne!(
        PgMetaEngine::metadata_notify_channel(7),
        PgMetaEngine::lock_notify_channel(7),
        "metadata invalidation and lock lease wakeups must stay on separate PG channels"
    );
}
