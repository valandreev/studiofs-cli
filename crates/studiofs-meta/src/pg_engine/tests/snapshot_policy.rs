use super::*;

use crate::snapshot::{
    SnapshotArtifactDescriptor, SnapshotCreationLease, SnapshotCreationState, SnapshotDailyPolicy,
    SnapshotLifecycle, SnapshotOrigin, SnapshotRetention, SnapshotSha256,
};
use crate::snapshot_ports::{
    SnapshotJobClaim, SnapshotJobStore, SnapshotPayloadProof, SnapshotPolicyStore, SnapshotWorkerId,
};
use chrono::{NaiveTime, Utc};

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn policy_set_load_cancel_uses_generation_cas() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-policy");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let info = volume.format_info();
    seed_local(&db_path, &info);
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open postgres engine");
    let policy = SnapshotDailyPolicy::new(
        NaiveTime::from_hms_opt(2, 30, 0).expect("valid policy time"),
        "UTC",
        Duration::from_secs(3600),
    )
    .expect("valid policy");

    run_async(async {
        let first = SnapshotPolicyStore::set_policy(&*engine, &policy, None)
            .await
            .expect("set policy");
        assert_eq!(first.generation, 1);
        assert_eq!(
            SnapshotPolicyStore::load_policy(&*engine)
                .await
                .expect("load policy")
                .expect("active policy"),
            first
        );

        let stale = SnapshotPolicyStore::cancel_policy(&*engine, 99)
            .await
            .expect_err("stale generation must be rejected");
        assert!(stale.to_string().contains("stale_policy_generation"));

        SnapshotPolicyStore::cancel_policy(&*engine, first.generation)
            .await
            .expect("cancel policy");
        assert!(
            SnapshotPolicyStore::load_policy(&*engine)
                .await
                .expect("load canceled policy")
                .is_none()
        );

        let second = SnapshotPolicyStore::set_policy(&*engine, &policy, Some(1))
            .await
            .expect("re-enable policy");
        assert_eq!(second.generation, 2);
        Ok(())
    })
    .expect("policy control plane");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn snapshot_job_claim_renew_release_is_fenced() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-job");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let info = volume.format_info();
    seed_local(&db_path, &info);
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open postgres engine");
    let created = engine
        .create_snapshot_v2(
            SnapshotOrigin::Manual,
            SnapshotRetention::manual(None).expect("manual infinite retention"),
        )
        .expect("create queued snapshot");
    let worker_a = SnapshotWorkerId::new("worker-a").expect("worker a");
    let worker_b = SnapshotWorkerId::new("worker-b").expect("worker b");

    run_async(async {
        let claim = SnapshotJobStore::claim_next(&*engine, &worker_a, Duration::from_secs(60))
            .await
            .expect("claim queued job")
            .expect("one queued job");
        assert_eq!(claim.job.snapshot_id, created.snapshot_id);
        assert_eq!(claim.job.state, SnapshotCreationState::Running);

        assert!(
            SnapshotJobStore::claim_next(&*engine, &worker_b, Duration::from_secs(60))
                .await
                .expect("second claim")
                .is_none()
        );
        let stale = SnapshotJobStore::renew_lease(
            &*engine,
            &SnapshotJobClaim {
                job: claim.job.clone(),
                lease: SnapshotCreationLease {
                    owner: worker_b.as_str().to_string(),
                    expires_at: claim.lease.expires_at,
                    fencing_token: claim.lease.fencing_token,
                },
            },
            Duration::from_secs(60),
        )
        .await
        .expect_err("wrong owner must be fenced");
        assert!(stale.to_string().contains("stale_lease"));

        SnapshotJobStore::release_lease(&*engine, &claim)
            .await
            .expect("release claim");
        let takeover = SnapshotJobStore::claim_next(&*engine, &worker_b, Duration::from_secs(60))
            .await
            .expect("take over released job")
            .expect("released job should be claimable");
        assert!(takeover.lease.fencing_token > claim.lease.fencing_token);

        let snapshot_id = takeover.job.snapshot_id.to_string();
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to expire lease");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "UPDATE snapshot_creation_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1 AND snapshot_id = $2",
                &[&volume.volume_id, &snapshot_id],
            )
            .await
            .expect("expire test lease");
        connection_task.abort();

        let stale_takeover = SnapshotJobStore::claim_next(&*engine, &worker_a, Duration::from_secs(60))
            .await
            .expect("claim stale job")
            .expect("expired lease should be claimable");
        assert!(stale_takeover.lease.fencing_token > takeover.lease.fencing_token);
        Ok(())
    })
    .expect("snapshot job control plane");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn snapshot_publish_ready_stores_all_payload_proofs() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-publish");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let info = volume.format_info();
    seed_local(&db_path, &info);
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open postgres engine");
    let created = engine
        .create_snapshot_v2(
            SnapshotOrigin::Manual,
            SnapshotRetention::manual(None).expect("manual infinite retention"),
        )
        .expect("create queued snapshot");

    run_async(async {
        let worker = SnapshotWorkerId::new("publish-worker").expect("worker");
        let claim = SnapshotJobStore::claim_next(&*engine, &worker, Duration::from_secs(60))
            .await
            .expect("claim snapshot job")
            .expect("queued snapshot job");
        let descriptor = SnapshotArtifactDescriptor::new(
            format!("snapshots/{}/artifact", created.snapshot_id),
            1,
            1,
            SnapshotSha256::from_bytes([7; 32]),
            64,
            created.cut,
            Utc::now(),
        )
        .expect("artifact descriptor");
        let proofs: Vec<_> = (0..64)
            .map(|index| SnapshotPayloadProof {
                object_id: 10_000 + index,
                offset: index * 64,
                length: 64,
                sha256: SnapshotSha256::from_bytes([index as u8; 32]),
            })
            .collect();

        let published = SnapshotJobStore::publish_ready(&*engine, &claim, &descriptor, &proofs)
            .await
            .expect("publish snapshot");
        assert_eq!(published.lifecycle, SnapshotLifecycle::Ready);

        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to count payload proofs");
        let connection_task = tokio::spawn(connection);
        let proof_count: i64 = client
            .query_one(
                "SELECT count(*) FROM snapshot_artifact_payload_proofs WHERE vol = $1 AND snapshot_id = $2",
                &[&volume.volume_id, &created.snapshot_id.to_string()],
            )
            .await
            .expect("count payload proofs")
            .get(0);
        connection_task.abort();
        assert_eq!(proof_count, 64);
        Ok(())
    })
    .expect("publish snapshot proofs");
}
