use super::*;
use crate::engine::UpgradeRequiredReason;
use crate::pg_engine::client_sessions::{
    HeartbeatError, INACTIVE_SESSION_RETENTION_SECONDS, REFRESH_IO_HOOK, SessionOwner, SessionRole,
    prune_expired_client_sessions,
};
use crate::pg_engine::{
    PublicationSession, insert_initial_volume_protocol, read_client_sessions,
    upgrade_shared_format5,
};
use crate::volume_protocol::{FeatureState, VolumeProtocol};

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
        let (original_lease, policy_error) = owner
            .heartbeat(&client, vol, None, SessionRole::Mount)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(policy_error.is_none());
        let initial = read_client_sessions(&client, vol).await?.pop().unwrap();
        assert!(initial.active);
        assert_eq!(initial.policy_epoch, 1);
        assert_eq!(initial.build, crate::version::CLIENT_RELEASE);
        assert!(!initial.capabilities.write.is_empty());

        // Delayed initial REGISTER (None) must never overwrite an existing session.
        assert!(matches!(
            owner.heartbeat(&client, vol, None, SessionRole::Mount).await,
            Err(HeartbeatError::Fenced)
        ));

        // Simulate a missed lease deadline using only the server clock.
        client
            .execute(
                "UPDATE client_sessions SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1",
                &[&vol],
            )
            .await
            .expect("expire lease");
        assert!(!read_client_sessions(&client, vol).await?[0].active);
        let tx = client.transaction().await.expect("begin stale publication");
        assert!(original_lease.stamp(&tx, vol).await.is_err());
        tx.rollback().await.expect("rollback stale publication");

        let (renewed_lease, _) = owner
            .heartbeat(
                &client,
                vol,
                Some(initial.fencing_token),
                SessionRole::Mount,
            )
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        let renewed = read_client_sessions(&client, vol).await?.pop().unwrap();
        assert_eq!(renewed.session_id, initial.session_id);
        assert_eq!(renewed.fencing_token, initial.fencing_token + 1);
        assert!(matches!(
            owner
                .heartbeat(&client, vol, Some(initial.fencing_token), SessionRole::Mount)
                .await,
            Err(HeartbeatError::Fenced)
        ));
        // A stale lease release cannot expire a newer generation.
        original_lease.release(&client, vol).await?;
        assert!(read_client_sessions(&client, vol).await?[0].active);

        let tx = client.transaction().await.expect("begin publication");
        renewed_lease.stamp(&tx, vol).await?;
        let stamp = tx
            .query_one(
                "SELECT current_setting('studiofs.session_id'), current_setting('studiofs.policy_epoch'), current_setting('studiofs.session_fencing_token')",
                &[],
            )
            .await
            .expect("read stamp");
        assert_eq!(stamp.get::<_, String>(0), renewed.session_id.to_string());
        assert_eq!(stamp.get::<_, String>(1), "1");
        assert_eq!(stamp.get::<_, String>(2), renewed.fencing_token.to_string());
        tx.commit().await.expect("commit publication");
        let row = client
            .query_one("SELECT current_setting('studiofs.session_id', true)", &[])
            .await
            .expect("read pooled connection context");
        assert_ne!(
            row.get::<_, Option<String>>(0).as_deref(),
            Some(renewed.session_id.to_string().as_str())
        );

        // Short-lived PublicationSession is visible inside the transaction and
        // removed on finish so CLI commands do not accumulate dead rows.
        let tx = client
            .transaction()
            .await
            .expect("begin maintenance publication");
        let session = PublicationSession::begin(&tx, vol).await?;
        let in_tx_count: i64 = tx
            .query_one(
                "SELECT count(*) FROM client_sessions WHERE vol = $1 AND lease_expires_at > clock_timestamp()",
                &[&vol],
            )
            .await
            .expect("count active in tx")
            .get(0);
        assert_eq!(in_tx_count, 2);
        session.finish(&tx, vol).await?;
        tx.commit()
            .await
            .expect("commit maintenance publication");
        let sessions = read_client_sessions(&client, vol).await?;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions.iter().filter(|s| s.active).count(), 1);

        // Clean owner close prevents resurrection by delayed renewals or initial inserts.
        owner.close(&client, vol).await?;
        assert!(!read_client_sessions(&client, vol).await?[0].active);
        assert!(matches!(
            owner
                .heartbeat(&client, vol, Some(renewed.fencing_token), SessionRole::Mount)
                .await,
            Err(HeartbeatError::Fenced)
        ));
        assert!(matches!(
            owner.heartbeat(&client, vol, None, SessionRole::Mount).await,
            Err(HeartbeatError::Fenced)
        ));

        // Closing an owner before a delayed initial REGISTER arrives leaves a
        // tombstone that blocks resurrection.
        let tombstoned = SessionOwner::new();
        tombstoned.close(&client, vol).await?;
        assert!(matches!(
            tombstoned
                .heartbeat(&client, vol, None, SessionRole::Mount)
                .await,
            Err(HeartbeatError::Fenced)
        ));

        // Bounded retention prunes only sessions expired past the retention window.
        client
            .execute(
                "UPDATE client_sessions SET lease_expires_at = clock_timestamp() - interval '2 days'
                 WHERE vol = $1 AND session_id = $2",
                &[&vol, &tombstoned.id()],
            )
            .await
            .expect("backdate expired session");
        let pruned =
            prune_expired_client_sessions(&client, vol, INACTIVE_SESSION_RETENTION_SECONDS)
                .await?;
        assert_eq!(pruned, 1);
        let remaining = read_client_sessions(&client, vol).await?;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].session_id, renewed.session_id);
        Ok(())
    })
    .expect("verify session fencing");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_validate_policy_epoch_and_role_capabilities() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-policy");
    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        let vol = volume.volume_id;
        insert_initial_volume_protocol(&client, vol, "sfs1008-policy").await?;

        let mount = SessionOwner::new();
        let janitor = SessionOwner::new();
        let (mount_lease, mount_err) = mount
            .heartbeat(&client, vol, None, SessionRole::Mount)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(mount_err.is_none());
        assert_eq!(mount_lease.policy_epoch(), 1);
        let (janitor_lease, janitor_err) = janitor
            .heartbeat(&client, vol, None, SessionRole::Maintenance)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(janitor_err.is_none());
        assert_eq!(janitor_lease.policy_epoch(), 1);

        // Advance policy to epoch 2 with a compatible optional feature: both roles validate it.
        let mut compatible = VolumeProtocol::baseline();
        compatible.requirements.features.insert(
            "table.successor_stats.v1".to_owned(),
            FeatureState::Compatible,
        );
        let compatible_json = compatible.encode_requirements()?;
        client
            .execute(
                "UPDATE volume_protocol SET policy_epoch = 2, requirements = $2::TEXT::JSONB WHERE vol = $1",
                &[&vol, &compatible_json],
            )
            .await
            .expect("advance compatible policy");

        // Stamping before revalidating epoch 2 is rejected as stale.
        let tx = client.transaction().await.expect("begin stale epoch tx");
        assert!(mount_lease.stamp(&tx, vol).await.is_err());
        tx.rollback().await.expect("rollback stale epoch tx");

        let (mount_lease, mount_err) = mount
            .heartbeat(&client, vol, Some(mount_lease.token()), SessionRole::Mount)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(mount_err.is_none());
        assert_eq!(mount_lease.policy_epoch(), 2);
        let tx = client.transaction().await.expect("begin epoch 2 tx");
        mount_lease.stamp(&tx, vol).await?;
        let stamped_epoch: String = tx
            .query_one("SELECT current_setting('studiofs.policy_epoch')", &[])
            .await
            .expect("read stamped epoch")
            .get(0);
        assert_eq!(stamped_epoch, "2");
        tx.commit().await.expect("commit epoch 2 tx");

        // Advance policy to epoch 3 with an unsupported maintenance capability:
        // Mount still validates epoch 3, Maintenance does NOT.
        let mut maintain_only = compatible.clone();
        maintain_only
            .requirements
            .maintain
            .insert("maintenance.payload_gc.v3".to_owned());
        maintain_only.requirements.features.insert(
            "maintenance.payload_gc.v3".to_owned(),
            FeatureState::Required,
        );
        let maintain_json = maintain_only.encode_requirements()?;
        client
            .execute(
                "UPDATE volume_protocol SET policy_epoch = 3, requirements = $2::TEXT::JSONB WHERE vol = $1",
                &[&vol, &maintain_json],
            )
            .await
            .expect("advance maintain policy");

        let (mount_lease, mount_err) = mount
            .heartbeat(&client, vol, Some(mount_lease.token()), SessionRole::Mount)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(mount_err.is_none());
        assert_eq!(mount_lease.policy_epoch(), 3);

        let (janitor_lease, janitor_err) = janitor
            .heartbeat(
                &client,
                vol,
                Some(janitor_lease.token()),
                SessionRole::Maintenance,
            )
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(matches!(janitor_err, Some(MetaError::UpgradeRequired { .. })));
        // Janitor must NOT advance its stamped policy_epoch to 3.
        assert_eq!(janitor_lease.policy_epoch(), 1);

        // Advance policy to epoch 4 with an unsupported write capability:
        // Mount must NOT advance its policy_epoch and must refuse publication.
        let mut unsupported_write = maintain_only;
        unsupported_write
            .requirements
            .write
            .insert("metadata.future_write.v1".to_owned());
        unsupported_write.requirements.features.insert(
            "metadata.future_write.v1".to_owned(),
            FeatureState::Required,
        );
        let write_json = unsupported_write.encode_requirements()?;
        client
            .execute(
                "UPDATE volume_protocol SET policy_epoch = 4, requirements = $2::TEXT::JSONB WHERE vol = $1",
                &[&vol, &write_json],
            )
            .await
            .expect("advance unsupported write policy");

        let (mount_lease, mount_err) = mount
            .heartbeat(&client, vol, Some(mount_lease.token()), SessionRole::Mount)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        assert!(matches!(
            mount_err,
            Some(MetaError::UpgradeRequired {
                reason: UpgradeRequiredReason::VolumeProtocolUnknown { .. }
            })
        ));
        assert_eq!(mount_lease.policy_epoch(), 3);

        let tx = client.transaction().await.expect("begin refused pub tx");
        assert!(mount_lease.stamp(&tx, vol).await.is_err());
        assert!(matches!(
            PublicationSession::begin(&tx, vol).await,
            Err(MetaError::UpgradeRequired { .. })
        ));
        tx.rollback().await.expect("rollback refused pub tx");
        Ok(())
    })
    .expect("verify policy epoch validation");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_register_and_close_janitor() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-janitor");
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

    let mut janitor = crate::pg_engine::PgJanitorSession::new(schema.dsn.clone(), volume.volume_id);
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("acquire janitor lock")
    );
    let registered = sessions();
    assert_eq!(registered.len(), 1);
    assert!(registered[0].active);
    assert!(!registered[0].capabilities.maintain.is_empty());
    janitor
        .release_global_janitor_lock()
        .expect("release janitor lock");
    assert!(!sessions()[0].active);

    // Dropping PgJanitorSession without explicit release also closes its session.
    {
        let mut dropped_janitor =
            crate::pg_engine::PgJanitorSession::new(schema.dsn.clone(), volume.volume_id);
        assert!(
            dropped_janitor
                .try_acquire_global_janitor_lock()
                .expect("acquire second janitor lock")
        );
        assert_eq!(sessions().iter().filter(|s| s.active).count(), 1);
    }
    assert!(sessions().iter().all(|s| !s.active));

    // Missing client_sessions schema gives actionable upgrade remediation.
    execute_test_sql(
        &schema.dsn,
        "ALTER TABLE client_sessions RENAME TO hidden_client_sessions".to_owned(),
    )
    .expect("hide client_sessions");
    let mut missing_schema_janitor =
        crate::pg_engine::PgJanitorSession::new(schema.dsn.clone(), volume.volume_id);
    let err = missing_schema_janitor
        .try_acquire_global_janitor_lock()
        .expect_err("missing client_sessions must fail janitor acquire");
    assert!(
        err.to_string().contains("studiofs upgrade"),
        "expected upgrade remediation, got: {err}"
    );
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
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        client
            .batch_execute(
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
                 FOR EACH ROW EXECUTE FUNCTION assert_publication_session();",
            )
            .await
            .expect("install publication assertion");
        Ok(())
    })
    .expect("verify publication transaction stamp");
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

    // Forced server-side lease expiry recovers immediately on the next publication
    // without waiting for the 15s heartbeat interval.
    execute_test_sql(
        &schema.dsn,
        format!(
            "UPDATE client_sessions SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = {}",
            volume.volume_id
        ),
    )
    .expect("expire mount sessions");
    first
        .create_dir(ROOT_INO, b"session-recovered", 0, 0, 0o700)
        .expect("create directory after expiry");
    first
        .flush_metadata()
        .expect("recover expired lease immediately on publication");
    let after_recovery = sessions();
    assert!(
        after_recovery
            .iter()
            .any(|s| s.active && s.fencing_token >= 2),
        "expected recovered session with incremented fencing_token: {after_recovery:?}"
    );

    let before = sessions();
    assert!(wait_until(Duration::from_secs(25), || {
        let current = sessions();
        current.len() == before.len()
            && current.iter().all(|s| s.active)
            && current.iter().zip(&before).all(|(now, old)| {
                now.session_id == old.session_id && now.heartbeat_at > old.heartbeat_at
            })
    }));
    first.prepare_for_close().expect("close first engine");
    assert_eq!(sessions().iter().filter(|s| s.active).count(), 1);
    second.prepare_for_close().expect("close second engine");
    assert!(sessions().iter().all(|s| !s.active));
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_failed_heartbeat_preserves_local_reads_and_recovers() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-outage");
    let dir = studiofs_test_support::workspace_target_tempdir("sfs1008-outage-");
    let path = dir.path().join("local");
    seed_local(&path, &volume.format_info());

    execute_test_sql(
        &schema.dsn,
        "ALTER TABLE client_sessions RENAME TO unavailable_client_sessions".to_owned(),
    )
    .expect("disable session registry in isolated schema");

    // Startup schema validation refuses missing client_sessions with upgrade guidance.
    let schema_err = PgMetaEngine::validate_postgres_schema(
        &schema.dsn,
        volume.volume_id,
        Duration::from_secs(5),
        Duration::from_secs(5),
    )
    .err()
    .expect("startup schema validation must refuse missing client_sessions");
    assert!(
        schema_err.to_string().contains("client_sessions")
            && schema_err.to_string().contains("studiofs upgrade"),
        "unexpected schema error: {schema_err}"
    );

    // Deferred startup still serves local reads and never grants offline write authority.
    let engine = PgMetaEngine::open_with_options(
        path,
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..PgOpenOptions::default()
        },
    )
    .expect("deferred startup with failed registration");
    let before = engine.offline_write_authority.load(Ordering::Acquire);
    assert_ne!(before, OFFLINE_AUTHORITY_GRANTED);
    let err = engine
        .refresh_client_session()
        .err()
        .expect("missing table fails refresh");
    assert!(err.to_string().contains("studiofs upgrade"), "{err}");
    assert_eq!(
        engine.get_attr(ROOT_INO).expect("serve local root").ino,
        ROOT_INO
    );
    assert_eq!(
        engine.offline_write_authority.load(Ordering::Acquire),
        before
    );

    // Once the table is restored, publication recovers after the short backoff
    // (well under the 15s heartbeat interval).
    execute_test_sql(
        &schema.dsn,
        "ALTER TABLE unavailable_client_sessions RENAME TO client_sessions".to_owned(),
    )
    .expect("restore session registry");
    // Poll until the failed-refresh backoff lapses instead of assuming its length.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let recovered = loop {
        match engine.refresh_client_session() {
            Ok(lease) => break lease,
            Err(error) if std::time::Instant::now() >= deadline => {
                panic!("session did not recover within the deadline: {error}")
            }
            Err(_) => std::thread::sleep(Duration::from_millis(25)),
        }
    };
    assert_eq!(recovered.token(), 1);
    engine.prepare_for_close().expect("close recovered engine");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_stamp_shared_format5_upgrade_without_policy() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-fmt5-upgrade");
    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        let vol = volume.volume_id;
        let mut fmt5 = volume.format_info();
        fmt5.format_version = 5;
        let fmt5_bytes = serde_json::to_vec(&fmt5).expect("encode format 5");
        drop_volume_protocol(&client, vol).await?;
        client
            .execute(
                "UPDATE metadata_state SET metadata_format_version = 5, format_info = $2 WHERE vol = $1",
                &[&vol, &fmt5_bytes],
            )
            .await
            .expect("seed format 5");
        client
            .batch_execute(
                "CREATE FUNCTION assert_format_upgrade_session() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                     IF current_setting('studiofs.session_id', true) IS NULL
                        OR current_setting('studiofs.policy_epoch', true) <> '0'
                     THEN RAISE EXCEPTION 'format upgrade publication session missing'; END IF;
                     RETURN NEW;
                 END $$;
                 CREATE TRIGGER assert_format_upgrade_session BEFORE UPDATE ON metadata_state
                 FOR EACH ROW EXECUTE FUNCTION assert_format_upgrade_session();",
            )
            .await
            .expect("install format upgrade trigger");

        upgrade_shared_format5(&mut client, vol, &fmt5.uuid).await?;
        let version: i16 = client
            .query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                &[&vol],
            )
            .await
            .expect("read upgraded format")
            .get(0);
        assert_eq!(version, 6);
        // Short-lived PublicationSession row was deleted on finish.
        assert!(read_client_sessions(&client, vol).await?.is_empty());
        Ok(())
    })
    .expect("verify format 5 upgrade publication session");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn published_format_adoption_retries_and_fences_legacy_publication() {
    use super::super::{read_volume_protocol, upgrade_shared_format6};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume5 = PgTestVolume::new(&schema.dsn, "sfs1132-format5");
    let volume6 = PgTestVolume::new(&schema.dsn, "sfs1132-format6");
    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        for (volume, initial) in [(&volume5, 5_i16), (&volume6, 6)] {
            let vol = volume.volume_id;
            let mut header = volume.format_info();
            header.format_version = u16::try_from(initial).unwrap();
            let bytes = serde_json::to_vec(&header).unwrap();
            drop_volume_protocol(&client, vol).await?;
            client.execute(
                "UPDATE metadata_state SET metadata_format_version=$2, format_info=$3 WHERE vol=$1",
                &[&vol, &initial, &bytes],
            ).await.unwrap();
            let state = |row: tokio_postgres::Row| -> Vec<u8> { row.get(0) };
            let checkpoint_sql = "SELECT snapshot FROM metadata_state WHERE vol=$1";
            let checkpoint = state(client.query_one(checkpoint_sql, &[&vol]).await.unwrap());
            if initial == 5 {
                upgrade_shared_format5(&mut client, vol, &header.uuid).await?;
            }
            client.batch_execute(
                "CREATE FUNCTION interrupt_format_adoption() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                     IF NEW.metadata_format_version = 7
                        AND EXISTS (SELECT 1 FROM volume_protocol WHERE vol=NEW.vol)
                     THEN RAISE EXCEPTION 'interrupted after installing policy'; END IF;
                     RETURN NEW;
                 END $$;
                 CREATE TRIGGER interrupt_format_adoption BEFORE UPDATE ON metadata_state
                 FOR EACH ROW EXECUTE FUNCTION interrupt_format_adoption();",
            ).await.unwrap();
            assert!(upgrade_shared_format6(&mut client, vol, &header.uuid).await.is_err());
            let row = client.query_one(
                "SELECT metadata_format_version, EXISTS(SELECT 1 FROM volume_protocol WHERE vol=$1)
                 FROM metadata_state WHERE vol=$1",
                &[&vol],
            ).await.unwrap();
            assert_eq!(row.get::<_, i16>(0), 6);
            assert!(!row.get::<_, bool>(1));
            client.batch_execute(
                "DROP TRIGGER interrupt_format_adoption ON metadata_state;
                 DROP FUNCTION interrupt_format_adoption();",
            ).await.unwrap();
            if initial == 5 {
                upgrade_shared_format5(&mut client, vol, &header.uuid).await?;
            }
            for _ in 0..2 {
                let upgraded = upgrade_shared_format6(&mut client, vol, &header.uuid).await?;
                assert_eq!(upgraded.format_version, 7);
            }
            assert_eq!(read_volume_protocol(&client, vol).await?, crate::volume_protocol::VolumeProtocol::baseline());
            let older_update = client.execute(
                "UPDATE metadata_state SET version=version+1 WHERE vol=$1 AND metadata_format_version=6",
                &[&vol],
            ).await.unwrap();
            assert_eq!(older_update, 0);
            let row = client.query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol=$1", &[&vol],
            ).await.unwrap();
            let old_reader = crate::version::VersionIdentity {
                name: "0.2.4 volume format",
                current: 6,
                min_supported: 6,
            };
            assert!(!old_reader.supports_i16(row.get(0)));
            assert_eq!(state(client.query_one(checkpoint_sql, &[&vol]).await.unwrap()), checkpoint);
            // A 0.2.x `init` re-applies the baseline DDL; it must not lift the fences.
            client.batch_execute(crate::pg_engine::schema::studiofs_pg_schema_sql()).await.unwrap();
            // A 0.2.x `load` replaces the row with its own format; refused and rolled back.
            let tx = client.transaction().await.unwrap();
            let downgrade = tx.batch_execute(&format!(
                "CREATE TEMP TABLE sfs1132_row ON COMMIT DROP AS SELECT * FROM metadata_state WHERE vol={vol};
                 DELETE FROM metadata_state WHERE vol={vol};
                 UPDATE sfs1132_row SET metadata_format_version=6;
                 INSERT INTO metadata_state SELECT * FROM sfs1132_row;",
            )).await.unwrap_err();
            assert_eq!(
                downgrade.as_db_error().unwrap().message(),
                "volume format 7 cannot be replaced by an older format"
            );
            drop(tx);
            let row = client.query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol=$1", &[&vol],
            ).await.unwrap();
            assert_eq!(row.get::<_, i16>(0), 7);
            let claim = "INSERT INTO snapshot_payload_upload_objects
                (vol, object_id, state, reconcile_owner)
                VALUES ($1, 1132, 'delete_pending', 'sfs1132')";
            // A 0.2.4 Janitor declares only the legacy capability.
            client.query_one("SELECT set_config('studiofs.deletion_format', '6', false), set_config('studiofs.deletion_volume_format', '', false)", &[])
                .await.unwrap();
            let refused = client.execute(claim, &[&vol]).await.unwrap_err();
            assert_eq!(
                refused.as_db_error().unwrap().message(),
                "deletion claim requires current volume format capability"
            );
            client.query_one("SELECT set_config('studiofs.deletion_format', '6', false), set_config('studiofs.deletion_volume_format', '7', false)", &[])
                .await.unwrap();
            assert_eq!(client.execute(claim, &[&vol]).await.unwrap(), 1);
        }
        Ok(())
    }).expect("retry shared adoption");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_concurrent_refreshes_converge_on_one_live_owner() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-refresh-race");
    let dir = studiofs_test_support::workspace_target_tempdir("sfs1008-refresh-race-");
    let path = dir.path().join("mount");
    seed_local(&path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        path,
        PgOpenOptions {
            background_poll: false,
            ..PgOpenOptions::default()
        },
    )
    .expect("open engine");
    let stale = engine.refresh_client_session().expect("register session");
    execute_test_sql(
        &schema.dsn,
        format!(
            "UPDATE client_sessions SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = {}",
            volume.volume_id
        ),
    )
    .expect("expire mount session");

    // Hold the first refresh before its I/O until a second refresh also reaches
    // I/O. With single-flight refresh the second never arrives and the bounded
    // wait lapses; the assertions below hold for every interleaving.
    // Clears the global hook even when a refresh thread panics.
    struct ResetRefreshHook;
    impl Drop for ResetRefreshHook {
        fn drop(&mut self) {
            *REFRESH_IO_HOOK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }
    let arrivals = Arc::new((Mutex::new(0_usize), std::sync::Condvar::new()));
    let hook_arrivals = Arc::clone(&arrivals);
    let reset_hook = ResetRefreshHook;
    *REFRESH_IO_HOOK.lock().expect("hook") = Some(Arc::new(move || {
        let (count, cvar) = &*hook_arrivals;
        let mut count = count.lock().expect("arrivals");
        *count += 1;
        cvar.notify_all();
        let _ = cvar
            .wait_timeout_while(count, Duration::from_secs(2), |count| *count < 2)
            .expect("arrivals");
    }));
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| engine.force_refresh_publication_session(&stale));
        let second = scope.spawn(|| engine.force_refresh_publication_session(&stale));
        (
            first.join().expect("join first refresh"),
            second.join().expect("join second refresh"),
        )
    });
    drop(reset_hook);
    let first = first.expect("first refresh");
    let second = second.expect("second refresh");
    assert!(
        first.owner() == second.owner() && first.token() == second.token(),
        "concurrent refreshes must return the same live lease"
    );
    let sessions = run_async(async {
        let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        read_client_sessions(&client, volume.volume_id).await
    })
    .expect("read sessions");
    let active: Vec<_> = sessions.iter().filter(|s| s.active).collect();
    assert_eq!(active.len(), 1, "exactly one live session: {sessions:?}");
    assert_eq!(active[0].session_id.to_string(), first.owner().id());
    assert_eq!(active[0].fencing_token, first.token());
    engine.prepare_for_close().expect("close engine");
    let after_close = run_async(async {
        let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        read_client_sessions(&client, volume.volume_id).await
    })
    .expect("read sessions after close");
    assert!(after_close.iter().all(|s| !s.active), "{after_close:?}");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn client_session_leases_prune_skips_row_renewed_concurrently() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1008-prune-race");
    run_async(async {
        let connect = || async {
            let (client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(connection);
            client
        };
        let mut renewer = connect().await;
        let pruner = connect().await;
        let observer = connect().await;
        let vol = volume.volume_id;
        let owner = SessionOwner::new();
        owner
            .heartbeat(&renewer, vol, None, SessionRole::Mount)
            .await
            .map_err(HeartbeatError::into_meta_error)?;
        renewer
            .execute(
                "UPDATE client_sessions SET lease_expires_at = clock_timestamp() - interval '2 days'
                 WHERE vol = $1 AND session_id = $2",
                &[&vol, &owner.id()],
            )
            .await
            .expect("backdate session past retention");

        // An uncommitted renewal holds the row lock while prune evaluates its CTE.
        let renewal = renewer.transaction().await.expect("begin renewal");
        renewal
            .execute(
                "UPDATE client_sessions SET lease_expires_at = clock_timestamp() + interval '60 seconds'
                 WHERE vol = $1 AND session_id = $2",
                &[&vol, &owner.id()],
            )
            .await
            .expect("renew session");
        let pruner_pid: i32 = pruner
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("pruner pid")
            .get(0);
        let prune = tokio::spawn(async move {
            prune_expired_client_sessions(&pruner, vol, INACTIVE_SESSION_RETENTION_SECONDS).await
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let waiting: bool = observer
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                     WHERE pid = $1 AND wait_event_type = 'Lock')",
                    &[&pruner_pid],
                )
                .await
                .expect("observe pruner")
                .get(0);
            if waiting {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "prune never blocked on the renewed row"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        renewal.commit().await.expect("commit renewal");
        let pruned = prune.await.expect("join prune")?;
        assert_eq!(pruned, 0, "a concurrently renewed session must survive prune");
        let remaining: i64 = observer
            .query_one(
                "SELECT count(*) FROM client_sessions WHERE vol = $1 AND session_id = $2",
                &[&vol, &owner.id()],
            )
            .await
            .expect("count sessions")
            .get(0);
        assert_eq!(remaining, 1);
        Ok(())
    })
    .expect("prune race");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn event_publication_fence_requires_current_stamp_per_volume() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1014-stamp");
    let legacy5 = PgTestVolume::new(&schema.dsn, "sfs1014-legacy5");
    let legacy6 = PgTestVolume::new(&schema.dsn, "sfs1014-legacy6");
    let format7 = PgTestVolume::new(&schema.dsn, "sfs1014-format7");
    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let _connection = tokio::spawn(connection);
        let vol = volume.volume_id;
        let insert = "INSERT INTO event_log (vol, batch_version, event_type, event_payload)
                      VALUES ($1, 1, 'test', ''::bytea)";

        let unstamped = client.transaction().await.unwrap();
        assert!(
            unstamped
                .query_opt(sql::BUMP_METADATA_VERSION, &[&vol, &7_i16, &0_i64, &1_i16])
                .await
                .unwrap()
                .is_none()
        );
        unstamped
            .execute(
                "UPDATE metadata_state SET version = 1 WHERE vol = $1",
                &[&vol],
            )
            .await
            .unwrap();
        let error = unstamped.execute(insert, &[&vol]).await.unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().message(),
            "event publication requires a current session and policy epoch"
        );
        unstamped.rollback().await.unwrap();
        let head: i64 = client
            .query_one("SELECT version FROM metadata_state WHERE vol = $1", &[&vol])
            .await
            .unwrap()
            .get(0);
        assert_eq!(head, 0);

        let tx = client.transaction().await.unwrap();
        let session = PublicationSession::begin(&tx, vol).await?;
        assert!(
            tx.query_opt(sql::BUMP_METADATA_VERSION, &[&vol, &7_i16, &0_i64, &1_i16])
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(tx.execute(insert, &[&vol]).await.unwrap(), 1);
        session.finish(&tx, vol).await?;
        tx.commit().await.unwrap();
        assert!(
            client.execute(insert, &[&vol]).await.is_err(),
            "stamp leaked across transactions"
        );

        let tx = client.transaction().await.unwrap();
        let _session = PublicationSession::begin(&tx, vol).await?;
        tx.execute(
            "UPDATE client_sessions SET fencing_token = fencing_token + 1 WHERE vol = $1",
            &[&vol],
        )
        .await
        .unwrap();
        let error = tx.execute(insert, &[&vol]).await.unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().message(),
            "event publication requires a current session and policy epoch"
        );
        tx.rollback().await.unwrap();

        // An expired lease is refused even while the policy epoch is unchanged.
        let tx = client.transaction().await.unwrap();
        let _session = PublicationSession::begin(&tx, vol).await?;
        tx.execute(
            "UPDATE client_sessions SET lease_expires_at = clock_timestamp() WHERE vol = $1",
            &[&vol],
        )
        .await
        .unwrap();
        let error = tx.execute(insert, &[&vol]).await.unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().message(),
            "event publication requires a current session and policy epoch"
        );
        tx.rollback().await.unwrap();

        // Format 7 alone enables the fence, without any volume policy.
        drop_volume_protocol(&client, format7.volume_id).await?;
        let error = client
            .execute(insert, &[&format7.volume_id])
            .await
            .unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().message(),
            "event publication requires a current session and policy epoch"
        );

        for (legacy, format) in [(legacy5.volume_id, 5_i16), (legacy6.volume_id, 6)] {
            drop_volume_protocol(&client, legacy).await?;
            client
                .execute(
                    "UPDATE metadata_state SET metadata_format_version = $2 WHERE vol = $1",
                    &[&legacy, &format],
                )
                .await
                .unwrap();
            assert_eq!(client.execute(insert, &[&legacy]).await.unwrap(), 1);
        }
        Ok(())
    })
    .expect("publication stamp scenario");
}

#[test]
#[ignore = "requires a disposable PostgreSQL instance"]
fn event_publication_fence_rechecks_epoch_after_waiting_for_activation() {
    use crate::compat::ClientCapabilities;
    use crate::pg_engine::{read_volume_protocol, update_volume_protocol};
    use crate::volume_protocol::ProtocolRejection;

    const FENCED: &str = "event publication requires a current session and policy epoch";
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1014-race");
    run_async(async {
        let (mut writer, writer_conn) = tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
            .await
            .expect("connect writer");
        let (mut activator, activator_conn) =
            tokio_postgres::connect(&schema.dsn, tokio_postgres::NoTls)
                .await
                .expect("connect activator");
        let _writer_conn = tokio::spawn(writer_conn);
        let _activator_conn = tokio::spawn(activator_conn);
        let vol = volume.volume_id;
        let feature = "metadata.test.v1";
        let mut actor = ClientCapabilities::current();
        actor.read.insert(feature);
        let expand = activator.transaction().await.unwrap();
        let mut next = read_volume_protocol(&expand, vol).await?;
        next.requirements
            .features
            .insert(feature.into(), FeatureState::Available);
        update_volume_protocol(&expand, vol, 1, &next, &actor, "expand").await?;
        expand.commit().await.unwrap();

        let writer_pid: i32 = writer
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let publication = writer.transaction().await.unwrap();
        let _session = PublicationSession::begin(&publication, vol).await?;

        let activation = activator.transaction().await.unwrap();
        let mut next = read_volume_protocol(&activation, vol).await?;
        next.requirements
            .features
            .insert(feature.into(), FeatureState::Compatible);
        let next = update_volume_protocol(&activation, vol, 2, &next, &actor, "activate").await?;
        assert_eq!(next.epoch, 3);

        // The unchanged write requirement and metadata format still admit the
        // old UPDATE. Force its statement snapshot to precede activation COMMIT.
        let args: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&vol, &7_i16, &10_i64, &1_i16];
        let wait_then_activate = async {
            loop {
                let waiting: bool = activation
                    .query_one(
                        "SELECT pg_backend_pid() = ANY(pg_blocking_pids($1))",
                        &[&writer_pid],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if waiting {
                    break;
                }
                tokio::task::yield_now().await;
            }
            activation.commit().await.unwrap();
        };
        let (bumped, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(
                publication.query_opt(sql::BUMP_METADATA_VERSION, args),
                wait_then_activate
            )
        })
        .await
        .expect("publisher did not wait for activation");
        assert!(
            bumped.unwrap().is_none(),
            "stale publisher advanced the head"
        );
        let refused =
            crate::pg_engine::volume_protocol::refused_publication(&publication, vol).await;
        assert!(matches!(
            refused,
            MetaError::VolumeProtocolRejected(ProtocolRejection::EpochConflict {
                expected: 2,
                found: 3
            })
        ));
        let error = publication
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload)
                 VALUES ($1, 1, 'test', ''::bytea), ($1, 1, 'test', ''::bytea)",
                &[&vol],
            )
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().message(), FENCED);
        publication.rollback().await.unwrap();
        let row = writer
            .query_one(
                "SELECT version, total_event_payload_bytes,
                        (SELECT count(*) FROM event_log WHERE vol = $1)
                 FROM metadata_state WHERE vol = $1",
                &[&vol],
            )
            .await
            .unwrap();
        let durable: (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
        assert_eq!(durable, (0, 0, 0));
        Ok(())
    })
    .expect("publication activation race");
}
