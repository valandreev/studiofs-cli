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
        .transaction()
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
    // `setup_pg_volume` inserts volume rows without a policy, like a volume
    // provisioned before the policy existed.
    let existing = PgTestVolume::new(&schema.dsn, "sfs1007-existing");
    let fresh = PgTestVolume::new(&schema.dsn, "sfs1007-fresh");
    let dsn = schema.dsn.clone();
    let (existing, fresh) = (existing.volume_id, fresh.volume_id);
    run_async(async move {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        let connection = tokio::spawn(connection);
        let client = &mut client;

        // The baseline backfilled both volumes when the schema was applied.
        // Remove both rows: the baseline re-application restores the existing
        // volume's, and `init`'s helper writes the fresh one's.
        client
            .batch_execute("DELETE FROM volume_protocol_history; DELETE FROM volume_protocol")
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert!(matches!(
            read_volume_protocol(&*client, fresh).await,
            Err(MetaError::VolumeProtocolRejected(
                ProtocolRejection::Missing
            ))
        ));
        migrate_registered_schema(client, true).await?;
        for _ in 0..2 {
            insert_initial_volume_protocol(&*client, fresh, "studiofs init").await?;
        }
        let baseline = VolumeProtocol::baseline();
        for volume in [existing, fresh] {
            assert_eq!(read_volume_protocol(&*client, volume).await?, baseline);
            assert_eq!(scalar(client, HISTORY, volume).await, 1);
        }

        // A change bumps the epoch and appends history; a lost-response retry
        // returns the committed policy without a second bump.
        let mut expanded = baseline.clone();
        expanded
            .requirements
            .features
            .insert("metadata.successor.v1".to_owned(), FeatureState::Available);
        for _ in 0..2 {
            let stored = update(client, fresh, 1, &expanded).await?;
            assert_eq!(
                (stored.epoch, &stored.requirements),
                (2, &expanded.requirements)
            );
        }
        assert_eq!(scalar(client, HISTORY, fresh).await, 2);
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
        crate::ensure_supported_pg_sync_format_version(Some(format))?;
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
