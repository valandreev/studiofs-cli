use super::*;
use crate::pg_engine::client_sessions::SessionOwner;
use crate::pg_engine::{PublicationSession, insert_initial_volume_protocol, read_client_sessions};

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_fence_expired_generations_on_live_postgres() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-fencing");
    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        let vol = volume.volume_id;
        insert_initial_volume_protocol(&client, vol, "sfs1008-test").await?;

        let owner = SessionOwner::new();
        let original_lease = owner.heartbeat(&client, vol, None).await?;
        let initial = read_client_sessions(&client, vol).await?.pop().unwrap();
        assert!(initial.active);
        assert_eq!(initial.policy_epoch, 1);
        assert_eq!(initial.build, crate::version::CLIENT_RELEASE);
        assert!(!initial.capabilities.write.is_empty());

        // Simulate a missed lease deadline using only the server clock.
        client.execute(
            "UPDATE client_sessions SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1",
            &[&vol],
        ).await.expect("expire lease");
        assert!(!read_client_sessions(&client, vol).await?[0].active);
        let tx = client.transaction().await.expect("begin stale publication");
        assert!(original_lease.stamp(&tx, vol).await.is_err());
        tx.rollback().await.expect("rollback stale publication");

        let renewed_lease = owner.heartbeat(&client, vol, Some(initial.fencing_token)).await?;
        let renewed = read_client_sessions(&client, vol).await?.pop().unwrap();
        assert_eq!(renewed.session_id, initial.session_id);
        assert_eq!(renewed.fencing_token, initial.fencing_token + 1);
        assert!(owner.heartbeat(&client, vol, Some(initial.fencing_token)).await.is_err());
        original_lease.release(&client, vol).await?;
        assert!(read_client_sessions(&client, vol).await?[0].active);

        let tx = client.transaction().await.expect("begin publication");
        renewed_lease.stamp(&tx, vol).await?;
        let stamp = tx.query_one(
            "SELECT current_setting('studiofs.session_id'), current_setting('studiofs.policy_epoch'), current_setting('studiofs.session_fencing_token')",
            &[],
        ).await.expect("read stamp");
        assert_eq!(stamp.get::<_, String>(0), renewed.session_id.to_string());
        assert_eq!(stamp.get::<_, String>(1), "1");
        assert_eq!(stamp.get::<_, String>(2), renewed.fencing_token.to_string());
        tx.commit().await.expect("commit publication");
        let row = client.query_one("SELECT current_setting('studiofs.session_id', true)", &[])
            .await.expect("read pooled connection context");
        assert_ne!(row.get::<_, Option<String>>(0).as_deref(), Some(renewed.session_id.to_string().as_str()));

        let tx = client.transaction().await.expect("begin maintenance publication");
        let session = PublicationSession::begin(&tx, vol).await?;
        session.finish(&tx, vol).await?;
        tx.commit().await.expect("commit maintenance publication");
        let sessions = read_client_sessions(&client, vol).await?;
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions.iter().filter(|s| s.active).count(), 1);
        Ok(())
    }).expect("verify session fencing");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_register_and_close_janitor() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-janitor");
    let mut janitor = crate::pg_engine::PgJanitorSession::new(schema.dsn.clone(), volume.volume_id);
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("acquire janitor lock")
    );

    let sessions = || {
        run_async(async {
            let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            let _connection = tokio::spawn(connection);
            read_client_sessions(&client, volume.volume_id).await
        })
        .expect("read janitor sessions")
    };
    let registered = sessions();
    assert_eq!(registered.len(), 1);
    assert!(registered[0].active);
    assert!(!registered[0].capabilities.maintain.is_empty());
    janitor
        .release_global_janitor_lock()
        .expect("release janitor lock");
    assert!(!sessions()[0].active);
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_register_renew_and_close_pg_engines() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-mounts");
    let dir = studiofs_test_support::workspace_target_tempdir("sfs1008-mounts-");
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");
    seed_local(&first_path, &volume.format_info());
    seed_local(&second_path, &volume.format_info());

    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            PgMetaEngine::open_with_options(
                first_path,
                PgOpenOptions {
                    defer_remote_startup_probes: true,
                    ..PgOpenOptions::default()
                },
            )
            .expect("open first engine before remote probes")
        });
        let second = PgMetaEngine::open(second_path).expect("open second engine");
        (first.join().expect("join first mount"), second)
    });

    let sessions = || {
        run_async(async {
            let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            let _connection = tokio::spawn(connection);
            read_client_sessions(&client, volume.volume_id).await
        })
        .expect("read sessions")
    };

    assert!(wait_until(Duration::from_secs(10), || {
        let current = sessions();
        current.len() == 2
            && current.iter().all(|s| s.active)
            && first.current_pg_sync_lifecycle() == PgSyncLifecycle::Live
    }));
    run_async(async {
        let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await.expect("connect");
        let _connection = tokio::spawn(connection);
        client.batch_execute(
            "CREATE FUNCTION assert_publication_session() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NOT EXISTS (
                     SELECT 1 FROM client_sessions WHERE vol = NEW.vol
                     AND session_id = current_setting('studiofs.session_id', true)
                     AND policy_epoch::TEXT = current_setting('studiofs.policy_epoch', true)
                     AND fencing_token::TEXT = current_setting('studiofs.session_fencing_token', true)
                     AND lease_expires_at > clock_timestamp()
                 ) THEN RAISE EXCEPTION 'publication session missing'; END IF;
                 RETURN NEW;
             END $$;
             CREATE TRIGGER assert_publication_session BEFORE INSERT ON event_log
             FOR EACH ROW EXECUTE FUNCTION assert_publication_session();"
        ).await.expect("install publication assertion");
        Ok(())
    }).expect("verify publication transaction stamp");
    use crate::trash::MetaVolumeSettings;
    assert!(
        first
            .compare_and_set_trash_days(0, 7)
            .expect("publish settings with session")
            .is_some()
    );
    first
        .create_dir(ROOT_INO, b"session-test", 0, 0, 0o700)
        .expect("create directory locally");
    first
        .flush_metadata()
        .expect("publish event log with session");

    let before = sessions();
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|s| s.active));
    assert!(wait_until(Duration::from_secs(25), || {
        let current = sessions();
        current.len() == before.len()
            && current.iter().zip(&before).all(|(now, old)| {
                now.session_id == old.session_id
                    && now.active
                    && now.heartbeat_at > old.heartbeat_at
                    && now.fencing_token == old.fencing_token
            })
    }));
    first.prepare_for_close().expect("close first engine");
    assert_eq!(sessions().iter().filter(|s| s.active).count(), 1);
    second.prepare_for_close().expect("close second engine");
    assert!(sessions().iter().all(|s| !s.active));
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_failed_heartbeat_preserves_local_reads_and_authority() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-outage");
    let dir = studiofs_test_support::workspace_target_tempdir("sfs1008-outage-");
    let path = dir.path().join("local");
    seed_local(&path, &volume.format_info());

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        client
            .batch_execute("ALTER TABLE client_sessions RENAME TO unavailable_client_sessions")
            .await
            .expect("disable session registry in isolated schema");
        Ok(())
    })
    .expect("inject heartbeat failure");

    let engine = PgMetaEngine::open_with_options(
        path,
        PgOpenOptions {
            background_poll: false,
            ..PgOpenOptions::default()
        },
    )
    .expect("startup with failed registration");
    let before = engine.offline_write_authority.load(Ordering::Acquire);
    assert_ne!(before, OFFLINE_AUTHORITY_GRANTED);
    assert!(engine.refresh_client_session().is_err());
    assert_eq!(
        engine.get_attr(ROOT_INO).expect("serve local root").ino,
        ROOT_INO
    );
    assert_eq!(
        engine.offline_write_authority.load(Ordering::Acquire),
        before
    );
    engine
        .prepare_for_close()
        .expect("close without an active session");
}
