//! Process termination, snapshot capture and resumption on the shared disposable stack.

use super::*;

async fn wait_for_claim_barrier(client: &tokio_postgres::Client, pid: u32) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let waiting: bool = client.query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'SELECT 1 FROM metadata_state%FOR SHARE' AND pid IN (SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted))",
                &[],
            ).await?.get(0);
            if waiting {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.with_context(|| format!("CLI {pid} never reached the claim publication fence"))?
}

async fn wait_for_cli(child: &mut OwnedCli) -> anyhow::Result<std::process::ExitStatus> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.0.try_wait()? {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("CLI did not exit after termination request")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; creates isolated PostgreSQL and RustFS containers"]
async fn live_cli_disposable_kill_restart_and_signal_drain() -> anyhow::Result<()> {
    let stack = DisposableStack::start().await?;
    // Windows hard termination is covered here; console Ctrl+C needs a console process group.
    let modes: &[bool] = if cfg!(unix) { &[false, true] } else { &[false] };
    for &graceful in modes {
        let id = object_id(u64::from(graceful))?;
        let prefix = format!("sfs978-process/{id:016x}/objects/");
        seed_live_object(&stack, &prefix, id).await?;
        let client_root = stack.root.join(format!("process-{graceful}"));
        let (volume_id, _, _) = create_live_postgres_janitor_fixture(
            &stack.postgres_url,
            &stack.s3(&prefix),
            &client_root.join("meta/janitor-live"),
            id,
        )
        .await?;
        let (mut blocker, blocker_task) = connection(&stack).await?;
        let tx = blocker.transaction().await?;
        tx.query_one(
            "SELECT 1 FROM metadata_state WHERE vol = $1 FOR UPDATE",
            &[&volume_id],
        )
        .await?;
        let (observer, observer_task) = connection(&stack).await?;
        let args = cli_args(false, 10);
        let stdout_path = stack
            .artifact_root
            .join(format!("process-{graceful}.stdout"));
        let stderr_path = stack
            .artifact_root
            .join(format!("process-{graceful}.stderr"));
        let mut command = stack.command(&args, &client_root)?;
        command.stdout(std::fs::File::create(&stdout_path)?);
        command.stderr(std::fs::File::create(&stderr_path)?);
        let mut child = OwnedCli(command.spawn()?);
        wait_for_claim_barrier(&observer, child.0.id()).await?;
        let competing = stack.cli(&args, &client_root)?;
        assert_eq!(
            competing.status.code(),
            Some(3),
            "global lock must exclude a second CLI"
        );
        let keys = object_keys(&prefix, id);
        for key in &keys {
            assert_eq!(
                stack.writer()?.read(key).await?.to_vec(),
                b"janitor fixture"
            );
        }

        if graceful {
            #[cfg(unix)]
            {
                let result = Command::new("/bin/kill")
                    .args(["-INT", &child.0.id().to_string()])
                    .status()?;
                anyhow::ensure!(result.success(), "could not deliver SIGINT to owned CLI");
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if std::fs::read_to_string(&stderr_path)?.contains("draining admitted work")
                        {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("CLI did not acknowledge SIGINT")??;
                assert!(
                    child.0.try_wait()?.is_none(),
                    "admitted claim must drain before exit"
                );
            }
        } else {
            child.0.kill()?;
            let status = wait_for_cli(&mut child).await?;
            assert!(!status.success());
            let candidate: bool = observer.query_one(
                "SELECT acknowledged_at IS NULL FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2",
                &[&volume_id, &i64::try_from(id)?],
            ).await?.get(0);
            assert!(candidate, "kill before claim must retain unfinished work");
            for key in &keys {
                assert_eq!(
                    stack.writer()?.read(key).await?.to_vec(),
                    b"janitor fixture"
                );
            }
        }
        tx.rollback().await?;
        if graceful {
            let status = wait_for_cli(&mut child).await?;
            assert_eq!(
                status.code(),
                Some(2),
                "cancelled CLI must report partial coverage"
            );
            let events: Vec<serde_json::Value> = std::fs::read_to_string(&stdout_path)?
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?;
            assert_eq!(summary(&events)["outcome"], "partial");
        }
        // A new process must reacquire the session lock and finish only the remaining work.
        let retry = stack.cli(&args, &client_root)?;
        anyhow::ensure!(retry.status.success(), "restart failed: {}", retry.status);
        for key in &keys {
            assert!(!stack.writer()?.exists(key).await?);
        }
        let finished: bool = observer.query_one(
            "SELECT acknowledged_at IS NOT NULL FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(id)?],
        ).await?.get(0);
        assert!(finished, "restart did not acknowledge confirmed deletion");
        let owners: i64 = observer.query_one(
            "SELECT count(*) FROM snapshot_payload_upload_objects WHERE vol = $1 AND reconcile_owner IS NOT NULL",
            &[&volume_id],
        ).await?.get(0);
        assert_eq!(owners, 0, "process left a deletion claim owned");
        observer_task.abort();
        blocker_task.abort();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; creates isolated PostgreSQL and RustFS containers"]
async fn live_cli_disposable_concurrent_snapshot_cuts_wait_for_janitor() -> anyhow::Result<()> {
    use sha2::{Digest, Sha256};
    use studiofs_meta::snapshot::{SnapshotOrigin, SnapshotRetention};

    let stack = DisposableStack::start().await?;
    let orphan = object_id(0)?;
    let pinned = orphan + 1;
    let prefix = format!("sfs978-snapshot/{orphan:016x}/objects/");
    for id in [orphan, pinned] {
        seed_live_object(&stack, &prefix, id).await?;
    }
    let client_root = stack.root.join("snapshot-cuts");
    let (volume_id, db_path, _) = create_live_postgres_janitor_fixture(
        &stack.postgres_url,
        &stack.s3(&prefix),
        &client_root.join("meta/janitor-live"),
        orphan,
    )
    .await?;
    let local = RedbMetaEngine::open_read_only(&db_path)?;
    let checkpoint = local.export_serving_checkpoint_bytes()?;
    drop(local);
    let (mut client, task) = connection(&stack).await?;
    client
        .execute(
            "UPDATE metadata_state SET snapshot = $2, checkpoint_sha256 = $3 WHERE vol = $1",
            &[
                &volume_id,
                &checkpoint,
                &Sha256::digest(&checkpoint).to_vec(),
            ],
        )
        .await?;
    client
        .execute(
            "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, 1, $2)",
            &[&volume_id, &i64::try_from(pinned)?],
        )
        .await?;
    client.execute(
        "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, $2, 0)",
        &[&volume_id, &i64::try_from(pinned)?],
    ).await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT 1 FROM metadata_state WHERE vol = $1 FOR UPDATE",
        &[&volume_id],
    )
    .await?;
    let args = cli_args(false, 10);
    let mut command = stack.command(&args, &client_root)?;
    command.stdout(std::fs::File::create(
        stack.artifact_root.join("snapshot-cli.stdout"),
    )?);
    command.stderr(std::fs::File::create(
        stack.artifact_root.join("snapshot-cli.stderr"),
    )?);
    let mut child = OwnedCli(command.spawn()?);
    let (observer, observer_task) = connection(&stack).await?;
    wait_for_claim_barrier(&observer, child.0.id()).await?;
    let creators: Vec<_> = (0..2)
        .map(|_| {
            let dsn = stack.postgres_url.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let backend = studiofs_meta::PgSnapshotBackend::open(&dsn, volume_id)?;
                Ok(backend
                    .create_snapshot_v2(SnapshotOrigin::Manual, SnapshotRetention::manual(None)?)?)
            })
        })
        .collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiters: i64 = observer
                .query_one(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                    &[],
                )
                .await?
                .get(0);
            if waiters == 2 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("both snapshot captures must wait on the CLI janitor lock")??;
    child.0.kill()?;
    wait_for_cli(&mut child).await?;
    tx.rollback().await?;
    for creator in creators {
        tokio::time::timeout(Duration::from_secs(30), creator).await???;
    }
    // Remove today's reference; only the two newly captured snapshots now pin the bytes.
    observer
        .execute(
            "DELETE FROM snapshot_current_references WHERE vol = $1",
            &[&volume_id],
        )
        .await?;
    let result = stack.cli(&args, &client_root)?;
    anyhow::ensure!(
        result.status.success(),
        "janitor retry after snapshot cuts failed"
    );
    let writer = stack.writer()?;
    for id in [orphan, pinned] {
        for key in object_keys(&prefix, id) {
            if id == orphan {
                assert!(!writer.exists(&key).await?);
            } else {
                assert_eq!(writer.read(&key).await?.to_vec(), b"janitor fixture");
            }
        }
    }
    let snapshots: i64 = observer
        .query_one(
            "SELECT count(*) FROM snapshot_catalog WHERE vol = $1 AND lifecycle = 'creating'",
            &[&volume_id],
        )
        .await?
        .get(0);
    assert_eq!(snapshots, 2);
    observer_task.abort();
    task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; creates isolated PostgreSQL and RustFS containers"]
async fn live_cli_disposable_scan_budget_resumes_checkpoint_across_processes() -> anyhow::Result<()>
{
    let stack = DisposableStack::start().await?;
    let base = object_id(0)?;
    let prefix = format!("sfs978-resume/{base:016x}/objects/");
    let client_root = stack.root.join("resume");
    let (volume_id, _, _) = create_live_postgres_janitor_fixture(
        &stack.postgres_url,
        &stack.s3(&prefix),
        &client_root.join("meta/janitor-live"),
        base,
    )
    .await?;
    let (client, task) = connection(&stack).await?;
    // Exercise discovery and its persisted pending object, rather than the candidate fast path.
    client
        .execute(
            "DELETE FROM snapshot_payload_gc_candidates WHERE vol = $1",
            &[&volume_id],
        )
        .await?;
    for id in base..base + 3 {
        seed_live_object(&stack, &prefix, id).await?;
    }
    let mut args = cli_args(false, 10);
    args.retain(|arg| arg != "--full-scan");
    args.extend(["--max-scan-entries".to_string(), "1".to_string()]);
    let mut complete = false;
    for run in 0..12 {
        let result = stack.cli(&args, &client_root)?;
        assert!(
            matches!(result.status.code(), Some(0 | 2)),
            "{}",
            result.status
        );
        let events = parse_events(&result)?;
        let cycle = events
            .iter()
            .find(|event| event["event"] == "cycle")
            .expect("cycle");
        assert!(cycle["listed_entries"].as_u64().expect("listed count") <= 1);
        if run == 0 {
            let saved: bool = client.query_one(
                "SELECT after_key IS NOT NULL AND pending_object_id IS NOT NULL FROM snapshot_payload_sweep_checkpoints WHERE vol = $1",
                &[&volume_id],
            ).await?.get(0);
            assert!(
                saved,
                "first bounded process did not preserve pending scan work"
            );
            for id in base..base + 3 {
                assert_eq!(
                    stack
                        .writer()?
                        .read(&studiofs_data::object::format_object_key(&prefix, id))
                        .await?
                        .to_vec(),
                    b"janitor fixture"
                );
            }
        }
        if summary(&events)["outcome"] == "complete" {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "bounded invocations never completed the persisted sweep"
    );
    for id in base..base + 3 {
        for key in object_keys(&prefix, id) {
            assert!(!stack.writer()?.exists(&key).await?);
        }
    }
    let acknowledged: i64 = client.query_one(
        "SELECT count(*) FROM snapshot_payload_gc_candidates WHERE vol = $1 AND acknowledged_at IS NOT NULL",
        &[&volume_id],
    ).await?.get(0);
    assert_eq!(acknowledged, 3);
    task.abort();
    Ok(())
}
