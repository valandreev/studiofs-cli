use super::*;

fn pg_counter_value(dsn: &str, volume_id: i64, name: &str) -> Option<i64> {
    let dsn = dsn.to_owned();
    let name = name.to_owned();
    run_async(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| MetaError::Storage(format!("connect postgres: {error}")))?;
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_opt(
                "SELECT value FROM counters WHERE vol = $1 AND name = $2",
                &[&volume_id, &name],
            )
            .await
            .map_err(|error| MetaError::Storage(format!("query postgres counter: {error}")))?;
        task.abort();
        Ok(row.map(|row| row.get(0)))
    })
    .expect("read PostgreSQL counter")
}

#[test]
fn persisted_pg_client_slot_roundtrips_in_local_counter() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let local = RedbMetaEngine::open(dir.path().join("meta.redb")).expect("open local redb");
    local.init_root(0, 0).expect("init root");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("initial slot"),
        None
    );
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist slot");
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("reloaded slot"),
        Some(7)
    );
}

#[test]
fn allocator_uses_separate_inode_and_payload_namespaces() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let inode = pg.next_inode().expect("allocate inode");
    let payload = pg.next_object_ids(1).expect("allocate payload")[0];

    assert_ne!(
        inode, payload,
        "equal local counters must use distinct namespaces"
    );
}

#[test]
fn sfs623_recovery_trusted_state_allocates_offline_after_reopen() {
    let (dir, pg) = temp_pg_engine_for_unit();
    let first = pg.next_inode().expect("allocate first inode");
    let reserved_through = pg
        .local
        .read_allocator_state()
        .expect("read allocator state")
        .expect("allocator state")
        .inode
        .reserved_through();
    drop(pg);

    let local = RedbMetaEngine::open(dir.path().join("meta.redb")).expect("reopen local redb");
    let state = local
        .read_allocator_state()
        .expect("read restarted allocator state")
        .expect("allocator state");
    let expected = state
        .inode
        .id_at(reserved_through)
        .expect("first ID after reserved tail");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen PgMetaEngine with PostgreSQL unavailable");
    assert!(
        !reopened.remote_format_validated.load(Ordering::Acquire),
        "offline startup must leave remote format validation pending"
    );
    let next = reopened
        .next_inode()
        .expect("allocate after offline restart");

    assert!(
        next > first,
        "restart must not reuse the unconsumed durable reservation"
    );
    assert_eq!(
        next, expected,
        "offline reopen must resume at the durable reserved high-water mark"
    );
}

#[test]
fn allocator_refill_does_not_hold_runtime_lock_during_local_persistence() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let pg = Arc::new(pg);
    pg.next_object_ids(1).expect("prewarm payload batch");

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let hook_release_rx = Arc::clone(&release_rx);
    pg.local
        .set_allocator_state_write_hook_for_test(Arc::new(move || {
            entered_tx.send(()).expect("signal allocator persistence");
            hook_release_rx
                .lock()
                .expect("release receiver lock")
                .recv()
                .expect("release allocator persistence");
        }));

    let refill_pg = Arc::clone(&pg);
    let refill = std::thread::spawn(move || refill_pg.next_inode());
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("allocator refill should reach local persistence");

    let fast_pg = Arc::clone(&pg);
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let fast = std::thread::spawn(move || {
        result_tx
            .send(fast_pg.next_object_id())
            .expect("send payload allocation result");
    });
    let payload_result_during_refill = result_rx.recv_timeout(Duration::from_millis(100)).ok();
    let payload_completed_during_refill = payload_result_during_refill.is_some();

    release_tx.send(()).expect("release allocator persistence");
    fast.join()
        .expect("payload allocation thread should not panic");
    let payload_result = payload_result_during_refill
        .or_else(|| result_rx.recv().ok())
        .expect("payload allocation should report a result")
        .expect("payload allocation should succeed");
    refill
        .join()
        .expect("allocator refill thread should not panic")
        .expect("allocator refill should succeed");
    assert!(
        payload_completed_during_refill,
        "a payload allocation with a warm batch must not wait for inode persistence; got {payload_result:?}"
    );
}

#[test]
fn sfs623_recovery_exhausted_persisted_record_is_read_only() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let state = AllocatorStateV1::new(
        AllocatorRecord::new(1, COUNTER_LIMIT).expect("exhausted inode namespace"),
        AllocatorRecord::new(2, 0).expect("available payload namespace"),
    )
    .expect("allocator state");
    pg.local
        .replace_allocator_state(state)
        .expect("persist exhausted allocator state");

    pg.bootstrap_allocator_state();

    let authority = MetaReplication::metadata_authority_status(&pg);
    assert!(!authority.inode_id_headroom);
    assert!(authority.object_id_headroom);
}

#[test]
fn sfs623_recovery_final_inode_refill_marks_authority_read_only() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let state = AllocatorStateV1::new(
        AllocatorRecord::new(1, COUNTER_LIMIT - 1).expect("final inode namespace"),
        AllocatorRecord::new(2, 0).expect("available payload namespace"),
    )
    .expect("allocator state");
    pg.local
        .replace_allocator_state(state)
        .expect("persist final inode allocator state");
    pg.bootstrap_allocator_state();

    pg.next_inode().expect("allocate final inode");

    let authority = MetaReplication::metadata_authority_status(&pg);
    assert!(!authority.inode_id_headroom);
    assert!(authority.object_id_headroom);
}

#[test]
fn sfs623_recovery_final_payload_refill_marks_authority_read_only() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let state = AllocatorStateV1::new(
        AllocatorRecord::new(1, 0).expect("available inode namespace"),
        AllocatorRecord::new(2, COUNTER_LIMIT - 1).expect("final payload namespace"),
    )
    .expect("allocator state");
    pg.local
        .replace_allocator_state(state)
        .expect("persist final payload allocator state");
    pg.bootstrap_allocator_state();

    pg.next_object_ids(1).expect("allocate final payload ID");

    let authority = MetaReplication::metadata_authority_status(&pg);
    assert!(authority.inode_id_headroom);
    assert!(!authority.object_id_headroom);
}

#[test]
fn sfs623_recovery_overflow_keeps_authority_read_only_until_reopen() {
    let (dir, pg) = temp_pg_engine_for_unit();
    let remaining = 5_000;
    let state = AllocatorStateV1::new(
        AllocatorRecord::new(1, COUNTER_LIMIT - remaining).expect("inode namespace"),
        AllocatorRecord::new(2, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    pg.local
        .replace_allocator_state(state)
        .expect("persist partial allocator state");
    pg.bootstrap_allocator_state();

    assert!(
        pg.next_inode_ids(8_192).is_err(),
        "a request larger than the remaining range must fail"
    );
    assert!(!MetaReplication::metadata_authority_status(&pg).inode_id_headroom);

    drop(pg);
    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen engine with the surviving allocator tail");

    let authority = MetaReplication::metadata_authority_status(&*reopened);
    assert!(
        authority.inode_id_headroom,
        "only engine open may restore authority from the surviving allocator tail"
    );
}

#[test]
fn sfs623_recovery_untrustworthy_state_is_read_only_offline() {
    let (dir, pg) = temp_pg_engine_for_unit();
    pg.local
        .write_invalid_allocator_state_for_test()
        .expect("persist invalid allocator state");
    drop(pg);

    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open known metadata view with PostgreSQL unavailable");

    let authority = MetaReplication::metadata_authority_status(&*reopened);
    assert!(!authority.inode_id_headroom);
    assert!(!authority.object_id_headroom);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn sfs623_pg_allocator_two_fresh_roots_receive_four_unique_namespaces() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-allocator-namespaces");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    let a_state = a
        .local
        .read_allocator_state()
        .expect("read a state")
        .expect("a state");
    let b_state = b
        .local
        .read_allocator_state()
        .expect("read b state")
        .expect("b state");
    let mut namespaces = vec![
        a_state.inode.namespace(),
        a_state.payload.namespace(),
        b_state.inode.namespace(),
        b_state.payload.namespace(),
    ];
    namespaces.sort_unstable();
    namespaces.dedup();

    assert_eq!(namespaces.len(), 4, "two fresh roots need four namespaces");
    let a_slot = PgMetaEngine::load_persisted_client_slot(&a.local)
        .expect("a slot")
        .expect("persisted a slot");
    let b_slot = PgMetaEngine::load_persisted_client_slot(&b.local)
        .expect("b slot")
        .expect("persisted b slot");
    assert_ne!(
        a_slot, b_slot,
        "client slots remain independent replication identities"
    );

    assert_eq!(
        a.current_client_id().expect("a client id"),
        format!("client-slot:{a_slot}"),
        "self-batch identity must be derived from the persisted client slot"
    );
    a.push_event_log_batch_locked(&[MetaEvent::AddEntry {
        parent: ROOT_INO,
        name: b"self-batch.txt".to_vec(),
        entry: DirEntryValue {
            ino: ROOT_INO + 100,
            kind: FileType::RegularFile,
        },
    }])
    .expect("write self-authored batch");
    let rows = {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| MetaError::Storage(format!("connect postgres: {error}")))?;
            let task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let rows = client
                .query(
                    "SELECT batch_version, seq_id, event_type, event_payload, client_id FROM event_log WHERE vol = $1 ORDER BY batch_version, seq_id",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(format!("query self batch: {error}")))?;
            task.abort();
            Ok(rows)
        })
        .expect("read self-authored batch")
    };
    let a_client_id = a.current_client_id().expect("a client id");
    let (_, _, _, is_self_batch) =
        RemoteApplyCoordinator::decode_foreign_batch(&rows, 0, &a_client_id)
            .expect("decode self-authored batch");
    assert!(
        is_self_batch,
        "a batch carrying the persisted client identity must be classified as self-authored"
    );

    a.prepare_for_close().expect("close allocator test engine");
    drop(a);
    let reopened = PgMetaEngine::open(&db_a).expect("reopen a");
    assert_eq!(
        reopened.current_client_id().expect("reopened client id"),
        format!("client-slot:{a_slot}"),
        "reopen must reuse the persisted client slot for self-batch detection"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn sfs623_pg_allocator_retired_inode_counter_is_removed_during_bootstrap() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-allocator-retired-counter");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    seed_local(&path, &volume.format_info());
    let volume_id = volume.volume_id;
    let namespace_before = pg_counter_value(&dsn, volume_id, "next_allocator_namespace");
    let seed_dsn = dsn.clone();
    run_async(async move {
        let (client, connection) = tokio_postgres::connect(&seed_dsn, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute(
                "INSERT INTO counters (vol, name, value) VALUES ($1, 'next_inode', 4)",
                &[&volume_id],
            )
            .await
            .expect("seed retired counter");
        task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("seed retired PostgreSQL counter");

    let engine = PgMetaEngine::open(&path).expect("open local root with retired counter");

    assert_eq!(
        pg_counter_value(&dsn, volume_id, "next_inode"),
        None,
        "bootstrap must remove the retired counter"
    );
    assert!(
        pg_counter_value(&dsn, volume_id, "next_allocator_namespace") > namespace_before,
        "retired counter must not prevent namespace minting"
    );
    let authority = MetaReplication::metadata_authority_status(&*engine);
    assert!(authority.inode_id_headroom);
    assert!(authority.object_id_headroom);

    let state = engine
        .local
        .read_allocator_state()
        .expect("read allocator state")
        .expect("retired counter must not prevent allocator state persistence");
    assert!(
        state.inode.namespace() != state.payload.namespace(),
        "allocator records must retain separate namespaces"
    );
    engine
        .next_inode()
        .expect("allocate inode with retired counter");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn sfs623_pg_allocator_untrustworthy_state_mints_fresh_pair_online() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-allocator-untrustworthy-recovery");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    seed_local(&path, &volume.format_info());

    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local
        .write_invalid_allocator_state_for_test()
        .expect("persist invalid allocator state");
    drop(local);

    let engine = PgMetaEngine::open(&path).expect("recover allocator state with PostgreSQL");
    let state = engine
        .local
        .read_allocator_state()
        .expect("read recovered allocator state")
        .expect("recovered allocator state");
    let authority = MetaReplication::metadata_authority_status(&*engine);

    assert_ne!(state.inode.namespace(), state.payload.namespace());
    assert!(authority.inode_id_headroom);
    assert!(authority.object_id_headroom);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn sfs623_pg_allocator_invalid_namespace_does_not_commit() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-allocator-invalid-namespace");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    seed_local(&path, &volume.format_info());
    let volume_id = volume.volume_id;
    let dsn_for_seed = dsn.clone();
    run_async(async move {
        let (client, connection) = tokio_postgres::connect(&dsn_for_seed, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute(
                "INSERT INTO counters (vol, name, value) VALUES ($1, 'next_allocator_namespace', $2)",
                &[&volume_id, &i64::from(crate::allocator_state::MAX_NAMESPACE)],
            )
            .await
            .expect("seed exhausted namespace counter");
        task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("seed exhausted PostgreSQL namespace counter");

    let before = pg_counter_value(&dsn, volume_id, "next_allocator_namespace")
        .expect("counter should exist before open");
    let engine = PgMetaEngine::open(&path).expect("open should remain read-only");
    let after = pg_counter_value(&dsn, volume_id, "next_allocator_namespace")
        .expect("counter should remain present after open");

    assert_eq!(
        before, after,
        "invalid namespace mint must roll back the registry"
    );
    assert!(
        engine
            .local
            .read_allocator_state()
            .expect("read allocator state")
            .is_none()
    );
    let authority = MetaReplication::metadata_authority_status(&*engine);
    assert!(!authority.inode_id_headroom);
    assert!(!authority.object_id_headroom);
}
