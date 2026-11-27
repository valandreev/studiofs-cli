//! SFS-1007: per-volume protocol policy and write requirement on a live database.

use super::*;
use crate::compat::ClientCapabilities;
use crate::engine::UpgradeRequiredReason;
use crate::pg_engine::migrations::migrate_registered_schema;
use crate::pg_engine::{
    insert_initial_volume_protocol, read_volume_protocol, set_shared_trash_days,
    update_volume_protocol,
};
use crate::volume_protocol::{FeatureState, ProtocolRejection, VolumeProtocol};

async fn scalar(client: &tokio_postgres::Client, sql: &str, volume: i64) -> i64 {
    client
        .query_one(sql, &[&volume])
        .await
        .map_err(pg_err)
        .expect(sql)
        .get(0)
}

async fn update(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected: u64,
    proposed: &VolumeProtocol,
) -> MetaResult<VolumeProtocol> {
    let tx = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
        .start()
        .await
        .map_err(|e| MetaError::Storage(pg_err(e)))?;
    let result = update_volume_protocol(
        &tx,
        volume,
        expected,
        proposed,
        &ClientCapabilities::current(),
        "sfs1007-test",
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| MetaError::Storage(pg_err(e)))?;
    Ok(result)
}

const HISTORY: &str = "SELECT count(*) FROM volume_protocol_history WHERE vol = $1";

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn volume_protocol_policy_and_write_requirement_on_live_postgres() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    // The fixture volumes lose their policy below, like volumes provisioned
    // before the policy existed.
    let existing = PgTestVolume::new(schema.dsn(), "sfs1007-existing");
    let legacy = PgTestVolume::new(schema.dsn(), "sfs1007-legacy");
    let dsn = schema.dsn().to_owned();
    let (existing, legacy) = (existing.volume_id, legacy.volume_id);
    run_async(async move {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        let connection = tokio::spawn(connection);
        let client = &mut client;

        migrate_registered_schema(client, true).await?;
        for volume in [existing, legacy] {
            drop_volume_protocol(client, volume).await?;
        }
        for volume in [existing, legacy] {
            assert!(matches!(
                read_volume_protocol(&*client, volume).await,
                Err(MetaError::VolumeProtocolRejected(
                    ProtocolRejection::Missing
                ))
            ));
            assert_eq!(scalar(client, HISTORY, volume).await, 0);
        }
        let fresh_name = "sfs1007-fresh";
        let fresh_uuid = Uuid::new_v4().to_string();
        let tx = client.transaction().await.map_err(|e| MetaError::Storage(pg_err(e)))?;
        let fresh = tx
            .query_one(
                "INSERT INTO volumes (name, uuid, block_size) VALUES ($1, $2, $3) RETURNING vol",
                &[&fresh_name, &fresh_uuid, &(DEFAULT_BLOCK_SIZE as i32)],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?
            .get(0);
        for _ in 0..2 {
            insert_initial_volume_protocol(&tx, fresh, "studiofs init").await?;
        }
        let fresh_format = serde_json::to_vec(&format_info(fresh_name, &fresh_uuid, &dsn, fresh))
            .map_err(|e| MetaError::Serialization(e.to_string()))?;
        tx
            .execute(
                "INSERT INTO metadata_state (
                    vol, metadata_format_version, version, checkpoint_version, snapshot,
                    checkpoint_sha256, format_info, checkpoint_publication_state, checkpoint_generation
                )
                SELECT $1, metadata_format_version, version, checkpoint_version, snapshot,
                    checkpoint_sha256, $3, checkpoint_publication_state, checkpoint_generation
                FROM metadata_state WHERE vol = $2",
                &[&fresh, &existing, &fresh_format],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        tx.commit().await.map_err(|e| MetaError::Storage(pg_err(e)))?;
        // The initializer is also atomic when used with an autocommit client.
        client
            .execute(
                "INSERT INTO volume_protocol_history
                    (vol, policy_epoch, write_requirement, requirements, changed_by)
                 SELECT $1, policy_epoch, write_requirement, requirements, changed_by
                 FROM volume_protocol_history WHERE vol = $2 AND policy_epoch = 1",
                &[&existing, &fresh],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert!(
            insert_initial_volume_protocol(&*client, existing, "selected test fixture")
                .await
                .is_err()
        );
        assert!(matches!(
            read_volume_protocol(&*client, existing).await,
            Err(MetaError::VolumeProtocolRejected(ProtocolRejection::Missing))
        ));
        client
            .execute(
                "DELETE FROM volume_protocol_history WHERE vol = $1 AND policy_epoch = 1",
                &[&existing],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        for _ in 0..2 {
            insert_initial_volume_protocol(&*client, existing, "selected test fixture").await?;
        }
        migrate_registered_schema(client, true).await?;
        assert!(matches!(
            read_volume_protocol(&*client, legacy).await,
            Err(MetaError::VolumeProtocolRejected(ProtocolRejection::Missing))
        ));
        assert_eq!(scalar(client, HISTORY, legacy).await, 0);
        let baseline = VolumeProtocol::baseline();
        for volume in [existing, fresh] {
            assert_eq!(read_volume_protocol(&*client, volume).await?, baseline);
            assert_eq!(scalar(client, HISTORY, volume).await, 1);
        }

        // An unexpected duplicate history epoch must abort the whole update.
        client
            .execute(
                "INSERT INTO volume_protocol_history
                    (vol, policy_epoch, write_requirement, requirements, changed_by)
                SELECT vol, 2, write_requirement, requirements, changed_by
                FROM volume_protocol_history WHERE vol = $1 AND policy_epoch = 1",
                &[&fresh],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        let mut rejected = baseline.clone();
        rejected
            .requirements
            .features
            .insert("metadata.conflicting_history.v1".to_owned(), FeatureState::Available);
        assert!(update(client, fresh, 1, &rejected).await.is_err());
        assert_eq!(read_volume_protocol(&*client, fresh).await?, baseline);
        client
            .execute(
                "DELETE FROM volume_protocol_history WHERE vol = $1 AND policy_epoch = 2",
                &[&fresh],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;

        // A change bumps the epoch and appends history; a lost-response retry
        // returns the committed policy without a second bump.
        let mut expanded = baseline.clone();
        expanded
            .requirements
            .features
            .insert("metadata.successor.v1".to_owned(), FeatureState::Available);
        expanded.requirements.checkpoint.format = 3;
        for _ in 0..2 {
            let stored = update(client, fresh, 1, &expanded).await?;
            assert_eq!(
                (stored.epoch, &stored.requirements),
                (2, &expanded.requirements)
            );
        }
        assert_eq!(scalar(client, HISTORY, fresh).await, 2);
        let stored = read_volume_protocol(&*client, fresh).await?;
        let history = client
            .query_one(
                "SELECT policy_epoch, write_requirement, requirements::TEXT
                 FROM volume_protocol_history WHERE vol = $1 AND policy_epoch = 2",
                &[&fresh],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert_eq!(
            VolumeProtocol::decode(history.get(0), history.get(1), history.get(2))?,
            stored
        );
        assert_eq!(stored.requirements.checkpoint, expanded.requirements.checkpoint);
        assert!(matches!(
            update(client, fresh, 1, &baseline).await,
            Err(MetaError::VolumeProtocolRejected(
                ProtocolRejection::EpochConflict {
                    expected: 1,
                    found: 2
                }
            ))
        ));

        // Invalid transitions and unknown capabilities change nothing.
        let mut retired = expanded.clone();
        retired.requirements.features.insert(
            crate::compat::CAP_EVENT_CODEC_V1.to_owned(),
            FeatureState::Retired,
        );
        retired
            .requirements
            .read
            .remove(crate::compat::CAP_EVENT_CODEC_V1);
        retired
            .requirements
            .write
            .remove(crate::compat::CAP_EVENT_CODEC_V1);
        let mut unknown = expanded.clone();
        unknown
            .requirements
            .features
            .insert("metadata.successor.v1".to_owned(), FeatureState::Compatible);
        for proposed in [&retired, &unknown] {
            assert!(matches!(
                update(client, fresh, 2, proposed).await,
                Err(MetaError::VolumeProtocolRejected(_))
            ));
        }
        assert_eq!(read_volume_protocol(&*client, fresh).await?.epoch, 2);
        assert_eq!(scalar(client, HISTORY, fresh).await, 2);

        // A successor's Activate raised the write requirement: publication is
        // refused with a typed reason in its version-bump UPDATE, while the
        // `metadata_format_version` read gate is untouched.
        let version = "SELECT version FROM metadata_state WHERE vol = $1";
        let before = scalar(client, version, existing).await;
        client
            .execute(
                "UPDATE metadata_state SET write_requirement = 2 WHERE vol = $1",
                &[&existing],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        let refused = set_shared_trash_days(client, existing, None, 3, "sfs1007-test", 1).await;
        assert!(
            matches!(
                refused,
                Err(MetaError::UpgradeRequired {
                    reason: UpgradeRequiredReason::WriteRequirementNewer {
                        found: 2,
                        supported: 1
                    }
                })
            ),
            "{refused:?}"
        );
        assert_eq!(scalar(client, version, existing).await, before);
        let format: i16 = client
            .query_one(sql::SELECT_METADATA_FORMAT_VERSION, &[&existing])
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?
            .get(0);
        crate::check_pg_volume_format(Some(format))?;
        client
            .execute(
                "UPDATE metadata_state SET write_requirement = 1 WHERE vol = $1",
                &[&existing],
            )
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert!(
            set_shared_trash_days(client, existing, None, 3, "sfs1007-test", 1)
                .await?
                .is_some()
        );
        assert_eq!(scalar(client, version, existing).await, before + 1);

        connection.abort();
        Ok(())
    })
    .expect("volume protocol scenario");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn volume_protocol_waiting_update_rechecks_policy_epoch_on_live_postgres() {
    use std::time::Duration;

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(schema.dsn(), "sfs1007-race");
    run_async(async {
        let pg_error = |error| MetaError::Storage(pg_err(error));
        let (mut winner_client, winner_connection) =
            tokio_postgres::connect(schema.dsn(), tokio_postgres::NoTls)
                .await
                .map_err(pg_error)?;
        let (mut stale_client, stale_connection) =
            tokio_postgres::connect(schema.dsn(), tokio_postgres::NoTls)
                .await
                .map_err(pg_error)?;
        let winner_connection = tokio::spawn(winner_connection);
        let stale_connection = tokio::spawn(stale_connection);
        let vol = volume.volume_id;
        insert_initial_volume_protocol(&winner_client, vol, "selected test fixture").await?;
        let stale_pid: i32 = stale_client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(pg_error)?
            .get(0);

        let initial = read_volume_protocol(&winner_client, vol).await?;
        let mut winner = initial.clone();
        winner
            .requirements
            .features
            .insert("metadata.winner.v1".to_owned(), FeatureState::Available);
        let mut stale = initial;
        stale
            .requirements
            .features
            .insert("metadata.stale.v1".to_owned(), FeatureState::Available);
        let tx = winner_client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(pg_error)?;
        tx.query_one(
            "SELECT vol FROM metadata_state WHERE vol = $1 FOR UPDATE",
            &[&vol],
        )
        .await
        .map_err(pg_error)?;

        let (stale_result, committed) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(update(&mut stale_client, vol, 1, &stale), async move {
                loop {
                    let waiting: bool = tx
                        .query_one(
                            "SELECT pg_backend_pid() = ANY(pg_blocking_pids($1))",
                            &[&stale_pid],
                        )
                        .await
                        .map_err(pg_error)?
                        .get(0);
                    if waiting {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let committed = update_volume_protocol(
                    &tx,
                    vol,
                    1,
                    &winner,
                    &ClientCapabilities::current(),
                    "winner",
                )
                .await?;
                tx.commit().await.map_err(pg_error)?;
                Ok::<_, MetaError>(committed)
            })
        })
        .await
        .expect("stale policy updater did not finish after waiting on the winner's volume lock");
        let committed = committed?;
        assert_eq!(committed.epoch, 2);
        assert!(
            matches!(
                stale_result,
                Err(MetaError::VolumeProtocolRejected(
                    ProtocolRejection::EpochConflict {
                        expected: 1,
                        found: 2
                    }
                ))
            ),
            "{stale_result:?}"
        );
        assert_eq!(read_volume_protocol(&winner_client, vol).await?, committed);
        assert_eq!(scalar(&winner_client, HISTORY, vol).await, 2);
        let history = winner_client
            .query_one(
                "SELECT policy_epoch, write_requirement, requirements::TEXT, changed_by
                 FROM volume_protocol_history WHERE vol = $1 AND policy_epoch = 2",
                &[&vol],
            )
            .await
            .map_err(pg_error)?;
        assert_eq!(history.get::<_, &str>(3), "winner");
        assert_eq!(
            VolumeProtocol::decode(history.get(0), history.get(1), history.get(2))?,
            committed
        );
        winner_connection.abort();
        stale_connection.abort();
        Ok(())
    })
    .expect("concurrent volume protocol update");
}

/// Runs one statement on the isolated schema; returns its first column, if any.
fn sql_i64(dsn: &str, sql: &str, volume: i64, text: Option<String>) -> Option<i64> {
    let (dsn, statement) = (dsn.to_owned(), sql.to_owned());
    run_async(async move {
        let sql = statement;
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        let connection = tokio::spawn(connection);
        let rows = match &text {
            Some(text) => client.query(&sql, &[&volume, text]).await,
            None => client.query(&sql, &[&volume]).await,
        }
        .map_err(|e| MetaError::Storage(pg_err(e)))?;
        connection.abort();
        Ok(rows.first().map(|row| row.get::<_, i64>(0)))
    })
    .expect(sql)
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn mount_policy_change_freezes_local_writer() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(schema.dsn(), "sfs1015-policy");
    let vol = volume.volume_id;
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    seed_local(&path, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open writer");
    assert!(pg.pg_sync_status().allows_new_mutations());
    let draft = pg.create_dir(ROOT_INO, b"draft", 0, 0, 0o755).unwrap();
    let pending = pg.replication_backlog().pending_ops;
    assert!(pending > 0);

    let set_policy = |epoch: i64, write_requirement: u16| {
        run_async(async {
            let (mut client, connection) =
                tokio_postgres::connect(schema.dsn(), tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(pg_err(e)))?;
            let connection = tokio::spawn(connection);
            let tx = client.transaction().await.unwrap();
            tx.execute(
                "UPDATE volume_protocol SET policy_epoch = $2 WHERE vol = $1",
                &[&vol, &epoch],
            )
            .await
            .unwrap();
            tx.execute(
                "UPDATE metadata_state SET write_requirement = $2 WHERE vol = $1",
                &[&vol, &(write_requirement as i16)],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            connection.abort();
            Ok(())
        })
        .unwrap();
        assert_eq!(
            sql_i64(
                schema.dsn(),
                "SELECT publication_epoch FROM metadata_state WHERE vol = $1",
                vol,
                None
            ),
            Some(epoch)
        );
    };
    let baseline = crate::version::SHARED_WRITE_REQUIREMENT.current;
    set_policy(2, baseline + 1);
    pg.sync_metadata()
        .expect("read-compatible policy still syncs");
    assert!(!pg.pg_sync_status().allows_new_mutations());
    assert_eq!(
        pg.pg_sync_status().lifecycle,
        PgSyncLifecycle::ReadOnlyCompatible
    );
    assert_eq!(
        pg.pg_sync_diagnostics().last_sync_error,
        Some(PgSyncFailure::UpgradeRequired)
    );
    assert_eq!(pg.lookup(ROOT_INO, b"draft").unwrap().ino, draft.ino);
    assert!(matches!(
        pg.flush_metadata(),
        Err(MetaError::UpgradeRequired {
            reason: UpgradeRequiredReason::WriteRequirementNewer { .. }
        })
    ));
    assert_eq!(pg.replication_backlog().pending_ops, pending);
    let events = "SELECT count(*) FROM event_log WHERE vol = $1";
    assert_eq!(sql_i64(schema.dsn(), events, vol, None), Some(0));

    set_policy(3, baseline);
    pg.sync_metadata().expect("revalidate restored policy");
    assert!(pg.pg_sync_status().allows_new_mutations());
    pg.flush_metadata().expect("publish retained event journal");
    assert_eq!(pg.replication_backlog().pending_ops, 0);
    assert!(sql_i64(schema.dsn(), events, vol, None).unwrap() > 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pending_events_stay_unpublished_while_the_policy_cannot_represent_them() {
    use crate::pg_engine::set_shared_trash_days;

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(schema.dsn(), "sfs1010-pending");
    let vol = volume.volume_id;
    run_async(async {
        let (client, connection) = tokio_postgres::connect(schema.dsn(), tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        let connection = tokio::spawn(connection);
        insert_initial_volume_protocol(&client, vol, "sfs1010-test").await?;
        connection.abort();
        Ok(())
    })
    .unwrap();
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    seed_local(&path, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open writer");

    // Authored under the baseline policy, then the policy stops requiring the
    // v1 event codec for readers before the journal is published.
    let draft = pg.create_dir(ROOT_INO, b"draft", 0, 0, 0o755).unwrap();
    let mut narrowed = VolumeProtocol::baseline();
    let sets = &mut narrowed.requirements;
    sets.read.remove(crate::compat::CAP_EVENT_CODEC_V1);
    sets.write.remove(crate::compat::CAP_EVENT_CODEC_V1);
    sets.features.insert(
        crate::compat::CAP_EVENT_CODEC_V1.to_owned(),
        FeatureState::Compatible,
    );
    let set_policy = |epoch: i64, policy: &VolumeProtocol| {
        sql_i64(
            schema.dsn(),
            &format!(
                "UPDATE volume_protocol SET policy_epoch = {epoch}, requirements = $2::TEXT::JSONB \
                 WHERE vol = $1 RETURNING policy_epoch"
            ),
            vol,
            Some(policy.encode_requirements().unwrap()),
        )
    };
    assert_eq!(set_policy(2, &narrowed), Some(2));
    let events = "SELECT count(*) FROM event_log WHERE vol = $1";
    let version = "SELECT version FROM metadata_state WHERE vol = $1";
    let version_before = sql_i64(schema.dsn(), version, vol, None);
    let cursor = pg.flushed_event_seq.load(Ordering::SeqCst);
    let pending = pg.replication_backlog().pending_ops;
    assert!(pending > 0);

    let batch = pg.claim_pending_batch(true).expect("pending batch");
    let refused = pg.flush_pending_claimed_batch_with_restore(batch).err();
    assert!(
        matches!(
            refused,
            Some(MetaError::VolumeProtocolRejected(
                ProtocolRejection::EventNotRepresentable { .. }
            ))
        ),
        "{refused:?}"
    );
    assert_eq!(sql_i64(schema.dsn(), events, vol, None), Some(0));
    assert_eq!(sql_i64(schema.dsn(), version, vol, None), version_before);
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), cursor);
    assert_eq!(pg.replication_backlog().pending_ops, pending);
    assert_eq!(pg.lookup(ROOT_INO, b"draft").unwrap().ino, draft.ino);
    // The refusal stays visible: a successful poll does not clear it, and it
    // does not invalidate the validated remote identity.
    let blocked = Some(crate::engine::PgSyncFailure::PublicationBlocked);
    assert_eq!(pg.pg_sync_progress_snapshot().last_sync_error, blocked);
    assert!(pg.remote_identity_validated.load(Ordering::Acquire));
    pg.sync_metadata_with_source(crate::pg_engine::MetadataSyncSource::FallbackPoll)
        .expect("poll while publication is blocked");
    assert_eq!(pg.pg_sync_progress_snapshot().last_sync_error, blocked);

    // Online settings changes negotiate against the same policy.
    let settings = run_async({
        let dsn = schema.dsn().to_owned();
        async move {
            let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| MetaError::Storage(pg_err(e)))?;
            let connection = tokio::spawn(connection);
            let result = set_shared_trash_days(&mut client, vol, None, 3, "sfs1010-test", 1).await;
            connection.abort();
            Ok(result)
        }
    })
    .unwrap();
    assert!(
        matches!(
            settings,
            Err(MetaError::VolumeProtocolRejected(
                ProtocolRejection::EventNotRepresentable {
                    kind: "set_volume_settings",
                    ..
                }
            ))
        ),
        "{settings:?}"
    );
    assert_eq!(sql_i64(schema.dsn(), events, vol, None), Some(0));

    // Once the policy can carry the encoding again, the retained journal publishes.
    assert_eq!(set_policy(3, &VolumeProtocol::baseline()), Some(3));
    let batch = pg.claim_pending_batch(true).expect("retained batch");
    pg.flush_pending_claimed_batch_with_restore(batch)
        .expect("publish retained journal");
    assert_eq!(pg.replication_backlog().pending_ops, 0);
    assert_eq!(pg.pg_sync_progress_snapshot().last_sync_error, None);
    assert!(sql_i64(schema.dsn(), events, vol, None).unwrap() > 0);
    pg.prepare_for_close().expect("close writer");
}
