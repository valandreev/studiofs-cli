use super::*;

fn retained_test_source_fences(
    engine: &PgMetaEngine,
    receipt: Ino,
) -> Vec<crate::trash::RetainedSourceFence> {
    let mut pending = vec![engine.lookup(receipt, b"data").unwrap().ino];
    let mut seen = HashSet::new();
    let mut fences = Vec::new();
    while let Some(ino) = pending.pop() {
        if !seen.insert(ino) {
            continue;
        }
        let attr = engine.get_attr(ino).unwrap();
        fences.push(crate::trash::RetainedSourceFence {
            ino,
            ctime: attr.ctime,
            size: attr.size,
            content_epoch: attr.content_epoch,
        });
        if attr.kind == FileType::Directory {
            pending.extend(
                engine
                    .readdir(ino)
                    .unwrap()
                    .into_iter()
                    .filter(|e| e.name != b"." && e.name != b"..")
                    .map(|e| e.ino),
            );
        }
    }
    fences
}

#[test]
#[ignore = "requires PostgreSQL; durable retained publication retry"]
fn test_pg_retained_publication_response_loss_retry() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-retry");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-retry-");
    let path = dir.path().join("local");
    seed_local(&path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 4,
            defer_remote_startup_probes: false,
        },
    )
    .unwrap();
    engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let start = engine
        .local
        .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
        .unwrap();
    engine.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let store = engine
        .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
        .unwrap();
    let receipt = engine.next_inode().unwrap();
    engine
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    engine.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
    engine
        .restore_retained_entry(
            receipt,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&engine, receipt),
        )
        .unwrap();
    let events = engine.local.read_events_range(start, u64::MAX).unwrap();
    let version = engine.push_event_log_batch_locked(&events).unwrap();
    assert_eq!(
        engine.push_event_log_batch_locked(&events).unwrap(),
        version
    );
    drop(engine);
    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 4,
            defer_remote_startup_probes: false,
        },
    )
    .unwrap();
    assert_eq!(
        reopened.push_event_log_batch_locked(&events).unwrap(),
        version
    );
    let before_remove = reopened
        .local
        .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
        .unwrap();
    reopened.rmdir(ROOT_INO, b"restored").unwrap();
    let removal = reopened
        .local
        .read_events_range(before_remove, u64::MAX)
        .unwrap();
    let removed_version = reopened.push_event_log_batch_locked(&removal).unwrap();
    assert!(removed_version > version);
    assert_eq!(
        reopened.push_event_log_batch_locked(&events).unwrap(),
        version
    );
    assert_eq!(
        reopened.remote_metadata_head_version().unwrap(),
        removed_version,
        "retry must not append old namespace events after later deletion"
    );
    let mut changed = events.clone();
    for event in &mut changed {
        if let crate::MetaEvent::SetXattr { name, value, .. } = event
            && name == crate::trash::RETAINED_RECEIPT_XATTR
        {
            let mut record: crate::trash::RetainedEntry = serde_json::from_slice(value).unwrap();
            record.original_name = b"different-identity".to_vec();
            *value = serde_json::to_vec(&record).unwrap();
        }
    }
    assert!(reopened.push_event_log_batch_locked(&changed).is_err());
    assert_eq!(
        reopened.remote_metadata_head_version().unwrap(),
        removed_version
    );
}

#[test]
#[ignore = "requires PostgreSQL; shared receipt proof cannot be erased"]
fn test_pg_retained_authority_rejects_receipt_removal() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RETAINED_RECEIPT_XATTR};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-removal");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-removal-");
    let path = dir.path().join("local");
    seed_local(&path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 4,
            defer_remote_startup_probes: false,
        },
    )
    .unwrap();
    engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let start = engine
        .local
        .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
        .unwrap();
    engine.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let store = engine
        .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
        .unwrap();
    let receipt = engine.next_inode().unwrap();
    engine
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    engine.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
    engine
        .restore_retained_entry(
            receipt,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&engine, receipt),
        )
        .unwrap();
    let events = engine.local.read_events_range(start, u64::MAX).unwrap();
    engine.push_event_log_batch_locked(&events).unwrap();
    let volume_id = volume.volume_id;
    let shared_record = || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT record FROM trash_entry_authority WHERE vol=$1 AND receipt=$2",
                    &[&volume_id, &(receipt as i64)],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok(row.get::<_, Vec<u8>>(0))
        }))
        .unwrap()
    };
    let record = shared_record();

    // The local guard refuses removing the emptied receipt directory, so the
    // shared rejection below is defense in depth against forged batches.
    let receipt_name = receipt.to_string();
    assert!(matches!(
        engine.rmdir(store.ino, receipt_name.as_bytes()),
        Err(MetaError::AccessDenied { .. })
    ));
    let before_empty = engine
        .local
        .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
        .unwrap();
    let empty = engine.create_dir(store.ino, b"empty", 0, 0, 0o700).unwrap();
    let created = engine
        .local
        .read_events_range(before_empty, u64::MAX)
        .unwrap();
    let version = engine.push_event_log_batch_locked(&created).unwrap();
    let rejected = |batch: &[crate::MetaEvent]| {
        let error = engine.push_event_log_batch_locked(batch).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("receipt cannot be removed from shared metadata"),
            "{batch:?}: {error}"
        );
        assert_eq!(engine.remote_metadata_head_version().unwrap(), version);
    };
    rejected(&[crate::MetaEvent::RemoveXattr {
        ino: receipt,
        name: RETAINED_RECEIPT_XATTR.to_vec(),
    }]);
    rejected(&[crate::MetaEvent::RemoveAttr { ino: receipt }]);
    // A directory rename over the empty receipt displaces it.
    rejected(&[crate::MetaEvent::RenameEntry {
        parent: store.ino,
        name: b"empty".to_vec(),
        new_parent: store.ino,
        new_name: receipt_name.as_bytes().to_vec(),
        entry: crate::DirEntryValue {
            ino: empty.ino,
            kind: crate::FileType::Directory,
        },
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: Some(receipt),
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    }]);
    assert_eq!(shared_record(), record);

    // Retain → restore → rmdir in one batch: the row admitted earlier in the
    // same transaction must also reject the removal, and nothing commits.
    let start = engine
        .local
        .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
        .unwrap();
    engine.create_dir(ROOT_INO, b"second", 0, 0, 0o700).unwrap();
    let second = engine.next_inode().unwrap();
    engine
        .retain_namespace_entry(ROOT_INO, b"second", store.ino, second)
        .unwrap();
    engine.create_dir(second, b"stage", 0, 0, 0o700).unwrap();
    engine
        .restore_retained_entry(
            second,
            ROOT_INO,
            b"second-restored",
            b"stage",
            &retained_test_source_fences(&engine, second),
        )
        .unwrap();
    let mut batch = engine.local.read_events_range(start, u64::MAX).unwrap();
    batch.push(crate::MetaEvent::RemoveAttr { ino: second });
    rejected(&batch);
    let admitted = run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .query_opt(
                "SELECT 1 FROM trash_entry_authority WHERE vol=$1 AND receipt=$2",
                &[&volume_id, &(second as i64)],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))
    }))
    .unwrap();
    assert!(
        admitted.is_none(),
        "rejected batch must roll back admission"
    );
}

fn retained_test_engine(path: &Path) -> Arc<PgMetaEngine> {
    PgMetaEngine::open_with_options(
        path,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 4,
            defer_remote_startup_probes: false,
        },
    )
    .unwrap()
}

fn sync_all_pages(engine: &PgMetaEngine) {
    let target = engine.remote_metadata_head_version().unwrap();
    for _ in 0..256 {
        if engine.local_version.load(Ordering::SeqCst) >= target {
            break;
        }
        engine.sync_metadata().unwrap();
    }
    assert!(
        engine.local_version.load(Ordering::SeqCst) >= target,
        "failed to catch up to {target}, stopped at {}",
        engine.local_version.load(Ordering::SeqCst)
    );
}

fn publish_and_pull(engine: &PgMetaEngine) {
    engine.request_eager_flush();
    engine.flush_due_pending().unwrap();
    sync_all_pages(engine);
}

#[test]
#[ignore = "requires PostgreSQL; acknowledged purge settles through the janitor queue"]
fn test_pg_retained_purge_settlement_reclaims_through_gc_queue() {
    use crate::snapshot_ports::{UploadedFragmentLedger, UploadedObjectDeletionClaim};
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RetainedPurgeReason};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-settle");
    let volume_id = volume.volume_id;
    let dir = studiofs_test_support::workspace_target_tempdir("retained-settle-");
    let path = dir.path().join("local");
    seed_local(&path, &volume.format_info());
    let engine = retained_test_engine(&path);
    engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    const PURGED: u64 = 61_001;
    const LINKED: u64 = 61_002;
    const OPENED: u64 = 61_003;
    let purged = create_published_small_file(&engine, ROOT_INO, b"purged", PURGED);
    let linked = create_published_small_file(&engine, ROOT_INO, b"linked", LINKED);
    engine.link(linked.ino, ROOT_INO, b"live-link").unwrap();
    let opened = create_published_small_file(&engine, ROOT_INO, b"opened", OPENED);
    let store = engine
        .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
        .unwrap();
    publish_and_pull(&engine);
    let sql = |statement: &'static str| {
        run_async(with_pg_test_client(&dsn, move |client| async move {
            client
                .query(statement, &[&volume_id])
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))
        }))
        .unwrap()
    };
    // Confirmed uploads, and a ready snapshot cut that still reaches them.
    sql(
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) SELECT $1, id, 'published' FROM unnest(ARRAY[61001, 61002, 61003]::BIGINT[]) AS id ON CONFLICT (vol, object_id) DO UPDATE SET state = 'published'",
    );
    sql(
        "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) SELECT $1, 'retained-pin', 'manual', version, now(), 'ready' FROM metadata_state WHERE vol = $1",
    );

    let mut receipts = Vec::new();
    for name in [&b"purged"[..], b"linked", b"opened"] {
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, name, store.ino, receipt)
            .unwrap();
        engine
            .request_retained_purge(receipt, RetainedPurgeReason::User)
            .unwrap();
        receipts.push(receipt);
    }
    let [purged_receipt, linked_receipt, opened_receipt] = receipts[..] else {
        unreachable!()
    };
    let now = 1_u64 << 40;
    // A local purge request alone settles nothing.
    assert_eq!(
        engine.run_retained_lifecycle_once(now).unwrap(),
        Default::default()
    );
    for (receipt, ino) in [
        (purged_receipt, purged.ino),
        (linked_receipt, linked.ino),
        (opened_receipt, opened.ino),
    ] {
        assert_eq!(engine.lookup(receipt, b"data").unwrap().ino, ino);
    }

    publish_and_pull(&engine);
    engine.note_open_handle(opened.ino, true);
    let sweep = engine.run_retained_lifecycle_once(now).unwrap();
    assert_eq!((sweep.settled, sweep.deferred), (2, 1));
    assert!(engine.lookup(purged_receipt, b"data").is_err());
    assert!(engine.get_attr(purged.ino).is_err());
    assert_eq!(
        engine.lookup(ROOT_INO, b"live-link").unwrap().ino,
        linked.ino
    );
    assert_eq!(engine.get_attr(linked.ino).unwrap().nlink, 1);
    // The open handle keeps its inode, link and content graph.
    assert_eq!(
        engine.lookup(opened_receipt, b"data").unwrap().ino,
        opened.ino
    );
    assert_eq!(engine.get_attr(opened.ino).unwrap().size, 64);

    // Crash after the local unlink but before publication, with a purge that
    // is acknowledged but not yet unlinked (the handle disappears with the process).
    drop(engine);
    let engine = retained_test_engine(&path);
    let sweep = engine.run_retained_lifecycle_once(now).unwrap();
    assert_eq!((sweep.settled, sweep.deferred), (1, 0));
    publish_and_pull(&engine);
    assert_eq!(
        engine.run_retained_lifecycle_once(now).unwrap(),
        Default::default()
    );
    let head = engine.remote_metadata_head_version().unwrap();
    publish_and_pull(&engine);
    assert_eq!(engine.remote_metadata_head_version().unwrap(), head);
    // `sync_all_pages` skips `sync_metadata` at the head, so its Live
    // bootstrap never ran; the janitor guard requires the reachability ledger.
    engine
        .backfill_current_reference_ledger_if_needed()
        .unwrap();
    assert!(
        engine
            .snapshot_reachability_bootstrapped
            .load(Ordering::Acquire),
        "the current-reference ledger must be bootstrapped before janitor checks"
    );

    // Each data link was removed in shared metadata exactly once.
    let removed = sql("SELECT event_type, event_payload FROM event_log WHERE vol = $1 AND event_type = 'remove_entry'")
        .into_iter()
        .filter_map(|row| {
            match crate::events::decode_event_with_type(row.get(0), row.get(1)).unwrap() {
                crate::MetaEvent::RemoveEntry { parent, name } if name == b"data" => Some(parent),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    let mut expected = vec![purged_receipt, linked_receipt, opened_receipt];
    let mut removed = removed;
    removed.sort_unstable();
    expected.sort_unstable();
    assert_eq!(removed, expected);

    // Receipt tombstones stay: shared rows and local receipt directories.
    let rows = sql("SELECT receipt FROM trash_entry_authority WHERE vol = $1");
    assert_eq!(rows.len(), 3);
    for receipt in &expected {
        assert!(engine.retained_entry(*receipt).unwrap().is_some());
        let name = receipt.to_string();
        assert_eq!(
            engine.lookup(store.ino, name.as_bytes()).unwrap().ino,
            *receipt
        );
    }

    // Unreferenced objects reach the ordinary janitor queue; the live hard
    // link keeps its object out of it.
    let mut queued = sql("SELECT object_id FROM snapshot_payload_gc_candidates WHERE vol = $1")
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    queued.sort_unstable();
    assert_eq!(queued, vec![PURGED as i64, OPENED as i64]);
    // The ready snapshot cut still pins the purged payload against deletion.
    assert!(engine.get_global_snapshot_pins().unwrap().contains(&PURGED));
    assert_eq!(
        engine
            .claim_uploaded_object_deletion(PURGED, "retained-settle-test")
            .unwrap(),
        UploadedObjectDeletionClaim::Protected
    );
    sql("UPDATE snapshot_catalog SET lifecycle = 'deleting' WHERE vol = $1");
    assert!(matches!(
        engine
            .claim_uploaded_object_deletion(PURGED, "retained-settle-test")
            .unwrap(),
        UploadedObjectDeletionClaim::Claimed { .. }
    ));
    assert_eq!(
        engine
            .claim_uploaded_object_deletion(LINKED, "retained-settle-test")
            .unwrap(),
        UploadedObjectDeletionClaim::Protected,
        "a live hard link keeps its object referenced"
    );
}

#[test]
#[ignore = "requires PostgreSQL; expiry and offline restore share one terminal authority"]
fn test_pg_retained_expiry_conflicts_with_offline_restore_like_purge() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedEntryState, RetainedPurgeReason,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    for expiry_first in [true, false] {
        let volume = PgTestVolume::new(&dsn, "retained-expiry");
        let volume_id = volume.volume_id;
        let dir = studiofs_test_support::workspace_target_tempdir("retained-expiry-");
        seed_local(&dir.path().join("first"), &volume.format_info());
        seed_local(&dir.path().join("second"), &volume.format_info());
        let first = retained_test_engine(&dir.path().join("first"));
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let child = first
            .create_dir(payload.ino, b"accepted-child", 0, 0, 0o700)
            .unwrap();
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        let retained = first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        let second = retained_test_engine(&dir.path().join("second"));
        let deadline = Instant::now() + Duration::from_secs(10);
        while second.retained_entry(receipt).unwrap().is_none() {
            assert!(Instant::now() < deadline, "peer never observed the receipt");
            publish_and_pull(&first);
            second.sync_metadata().unwrap();
        }

        // Controlled clock: whole days from the initial deletion, inclusive.
        let expires_at = retained.deleted_at_unix_secs + 7 * 86_400;
        assert_eq!(
            second.run_retained_lifecycle_once(expires_at - 1).unwrap(),
            Default::default()
        );
        let staged = first.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        let staged_child = first
            .create_dir(staged.ino, b"accepted-child", 0, 0, 0o700)
            .unwrap();
        assert_ne!(staged.ino, payload.ino);
        assert_ne!(staged_child.ino, child.ino);
        first
            .restore_retained_entry(
                receipt,
                ROOT_INO,
                b"restored-offline",
                b"stage",
                &retained_test_source_fences(&first, receipt),
            )
            .unwrap();
        assert_eq!(
            second
                .run_retained_lifecycle_once(expires_at)
                .unwrap()
                .expired,
            1
        );
        assert_eq!(
            second.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::Expired {
                    settings_revision: 1
                }
            }
        );
        // Not yet acknowledged: nothing settles.
        let sweep = second.run_retained_lifecycle_once(expires_at).unwrap();
        assert_eq!((sweep.expired, sweep.settled), (0, 0));
        assert_eq!(second.lookup(receipt, b"data").unwrap().ino, payload.ino);

        let (winner, loser) = if expiry_first {
            (&second, &first)
        } else {
            (&first, &second)
        };
        winner.request_eager_flush();
        winner.flush_due_pending().unwrap();
        loser.request_eager_flush();
        // A losing restore has a namespace remainder; a losing expiry is the whole batch.
        assert_eq!(loser.flush_due_pending().is_err(), expiry_first);
        first.sync_metadata().unwrap();
        second.sync_metadata().unwrap();
        let shared = run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT record FROM trash_entry_authority WHERE vol=$1 AND receipt=$2",
                    &[&volume_id, &(receipt as i64)],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok(row.get::<_, Vec<u8>>(0))
        }))
        .unwrap();
        let shared = RedbMetaEngine::decode_retained_entry(receipt, &shared).unwrap();
        assert_eq!(
            matches!(
                shared.state,
                RetainedEntryState::PurgeRequested {
                    reason: RetainedPurgeReason::Expired {
                        settings_revision: 1
                    }
                }
            ),
            expiry_first
        );

        let sweep = second.run_retained_lifecycle_once(expires_at).unwrap();
        assert_eq!(
            (sweep.expired, sweep.settled),
            (0, usize::from(expiry_first))
        );
        // The restoring client never settles: restore already unlinked its data tree.
        assert_eq!(
            first.run_retained_lifecycle_once(expires_at).unwrap(),
            Default::default()
        );
        assert_eq!(
            first.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino
        );
        assert_eq!(
            first.lookup(payload.ino, b"accepted-child").unwrap().ino,
            child.ino
        );
        if expiry_first {
            assert!(second.lookup(receipt, b"data").is_err());
            publish_and_pull(&second);
            first.sync_metadata().unwrap();
            first.request_eager_flush();
            first.flush_due_pending().unwrap();
            second.sync_metadata().unwrap();
            assert_eq!(
                first.replication_backlog().pending_ops,
                0,
                "losing restore must leave no unpublished ops after flush and sync"
            );
            let first_restored = first.lookup(ROOT_INO, b"restored-offline").unwrap();
            let second_restored = second.lookup(ROOT_INO, b"restored-offline").unwrap();
            assert_eq!(first_restored.ino, payload.ino);
            assert_eq!(second_restored.ino, payload.ino);
            assert_eq!(
                first
                    .lookup(first_restored.ino, b"accepted-child")
                    .unwrap()
                    .ino,
                child.ino
            );
            assert_eq!(
                second
                    .lookup(second_restored.ino, b"accepted-child")
                    .unwrap()
                    .ino,
                child.ino
            );
            assert!(second.get_attr(payload.ino).is_ok());
            assert!(first.get_attr(payload.ino).is_ok());
        } else {
            assert_eq!(
                second.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
                payload.ino
            );
            assert_eq!(
                second.lookup(payload.ino, b"accepted-child").unwrap().ino,
                child.ino
            );
            assert!(second.get_attr(payload.ino).is_ok());
        }
    }
}

#[test]
#[ignore = "requires PostgreSQL; identical peer expiry must publish the pulling client's own batch"]
fn test_pg_retained_identical_peer_expiry_publishes_own_batch() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedEntryState, RetainedPurgeReason,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    for pull_before_flush in [true, false] {
        let volume = PgTestVolume::new(&dsn, "retained-peer-expiry");
        let volume_id = volume.volume_id;
        let dir = studiofs_test_support::workspace_target_tempdir("retained-peer-expiry-");
        seed_local(&dir.path().join("first"), &volume.format_info());
        seed_local(&dir.path().join("second"), &volume.format_info());
        let first = retained_test_engine(&dir.path().join("first"));
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        let retained = first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        let second = retained_test_engine(&dir.path().join("second"));
        let deadline = Instant::now() + Duration::from_secs(10);
        while second.retained_entry(receipt).unwrap().is_none() {
            assert!(Instant::now() < deadline, "peer never observed the receipt");
            publish_and_pull(&first);
            second.sync_metadata().unwrap();
        }

        // Both clients queue byte-identical expiry proposals at the same instant.
        let expires_at = retained.deleted_at_unix_secs + 7 * 86_400;
        assert_eq!(
            first
                .run_retained_lifecycle_once(expires_at)
                .unwrap()
                .expired,
            1
        );
        assert_eq!(
            second
                .run_retained_lifecycle_once(expires_at)
                .unwrap()
                .expired,
            1
        );
        first.request_eager_flush();
        first.flush_due_pending().unwrap();
        if pull_before_flush {
            second.sync_metadata().unwrap();
        }
        second.request_eager_flush();
        second.flush_due_pending().unwrap();
        assert_eq!(
            second.replication_backlog().pending_ops,
            0,
            "identical peer expiry must drain its journal (pull_before_flush={pull_before_flush})"
        );
        let second_client = second.current_client_id().unwrap();
        let own_batches = run_async(with_pg_test_client(&dsn, move |client| async move {
            client
                .query_one(
                    "SELECT count(*) FROM event_log WHERE vol=$1 AND client_id=$2",
                    &[&volume_id, &second_client],
                )
                .await
                .map(|row| row.get::<_, i64>(0))
                .map_err(|error| MetaError::Storage(error.to_string()))
        }))
        .unwrap();
        assert!(
            own_batches > 0,
            "the peer's own expiry must reach event_log (pull_before_flush={pull_before_flush})"
        );
        let local = second.retained_entry(receipt).unwrap().unwrap();
        assert_eq!(
            local.state,
            RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::Expired {
                    settings_revision: 1
                }
            }
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; expiry judged under a superseded shared period is retired, not published"]
fn test_pg_retained_stale_expiry_after_period_raise_is_retired() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedEntryState, RetainedPurgeReason,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    for sync_before_flush in [false, true] {
        let volume = PgTestVolume::new(&dsn, "retained-stale-expiry");
        let dir = studiofs_test_support::workspace_target_tempdir("retained-stale-expiry-");
        let second_path = dir.path().join("second");
        seed_local(&dir.path().join("first"), &volume.format_info());
        seed_local(&second_path, &volume.format_info());
        let first = retained_test_engine(&dir.path().join("first"));
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        let retained = first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        let second = retained_test_engine(&second_path);
        let deadline = Instant::now() + Duration::from_secs(10);
        while second.retained_entry(receipt).unwrap().is_none() {
            assert!(Instant::now() < deadline, "peer never observed the receipt");
            publish_and_pull(&first);
            second.sync_metadata().unwrap();
        }

        // B judges the entry expired under the 7-day period but has not flushed.
        let expires_at = retained.deleted_at_unix_secs + 7 * 86_400;
        assert_eq!(
            second
                .run_retained_lifecycle_once(expires_at)
                .unwrap()
                .expired,
            1
        );
        // A raises the shared period before B's stale proposal reaches commit.
        let revision = first.volume_settings().unwrap().revision;
        first
            .compare_and_set_trash_days(revision, 30)
            .unwrap()
            .unwrap();
        if sync_before_flush {
            second.sync_metadata().unwrap();
        }
        second.request_eager_flush();
        // The commit rejects the stale proposal. A fully retired batch may return
        // Ok through the cursor-recovery path, so the assertions below check state.
        let _ = second.flush_due_pending();
        assert_eq!(
            second.replication_backlog().pending_ops,
            0,
            "stale proposal must be retired (sync_before_flush={sync_before_flush})"
        );
        assert_eq!(
            second.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Retained
        );
        assert_eq!(second.lookup(receipt, b"data").unwrap().ino, payload.ino);

        // The retired proposal stays retired across restart.
        drop(second);
        let second = reopen_pg_engine_with_retry(&second_path);
        assert_eq!(second.replication_backlog().pending_ops, 0);
        assert_eq!(
            second.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Retained
        );
        first.sync_metadata().unwrap();
        assert_eq!(
            first.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Retained
        );

        // Not due under the raised period yet; due after 30 days, and that works.
        second.sync_metadata().unwrap();
        assert_eq!(
            second.run_retained_lifecycle_once(expires_at).unwrap(),
            Default::default()
        );
        let due = retained.deleted_at_unix_secs + 30 * 86_400;
        assert_eq!(second.run_retained_lifecycle_once(due).unwrap().expired, 1);
        second.request_eager_flush();
        second.flush_due_pending().unwrap();
        assert_eq!(second.replication_backlog().pending_ops, 0);
        first.sync_metadata().unwrap();
        assert_eq!(
            first.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::Expired {
                    settings_revision: 2
                }
            }
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; a stale expiry abandoned in the revert window must not publish after a period ABA"]
fn test_pg_retained_abandoned_expiry_never_publishes_after_restore_and_period_aba() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RetainedEntryState};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    // (restart, flush_before_restore). Restart replays the pending proposal, so a
    // local restore is refused until a flush heals it; without restart, the
    // restore runs inside the window between revert and retirement.
    for (restart, flush_before_restore) in [(false, false), (false, true), (true, true)] {
        let volume = PgTestVolume::new(&dsn, "retained-abandoned-expiry");
        let dir = studiofs_test_support::workspace_target_tempdir("retained-abandoned-expiry-");
        let second_path = dir.path().join("second");
        seed_local(&dir.path().join("first"), &volume.format_info());
        seed_local(&second_path, &volume.format_info());
        let first = retained_test_engine(&dir.path().join("first"));
        let judged = first
            .compare_and_set_trash_days(0, 7)
            .unwrap()
            .unwrap()
            .revision;
        first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        let retained = first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        let second = retained_test_engine(&second_path);
        let deadline = Instant::now() + Duration::from_secs(10);
        while second.retained_entry(receipt).unwrap().is_none() {
            assert!(Instant::now() < deadline, "peer never observed the receipt");
            publish_and_pull(&first);
            second.sync_metadata().unwrap();
        }

        // Expiry judged at revision `judged` is queued.
        let expires_at = retained.deleted_at_unix_secs + 7 * 86_400;
        assert_eq!(
            second
                .run_retained_lifecycle_once(expires_at)
                .unwrap()
                .expired,
            1
        );
        // Window: the overlay revert became durable, the retirement did not.
        second
            .local
            .discard_stale_retained_expiry(receipt, judged)
            .unwrap();
        let second = if restart {
            drop(second);
            let reopened = reopen_pg_engine_with_retry(&second_path);
            // Replay re-applies the still-pending proposal over the reverted overlay.
            assert!(matches!(
                reopened.retained_entry(receipt).unwrap().unwrap().state,
                RetainedEntryState::PurgeRequested { .. }
            ));
            reopened
        } else {
            second
        };

        // ABA: 7 -> 30 -> 7. The period matches the judged one again; the revision does not.
        let raised = first
            .compare_and_set_trash_days(judged, 30)
            .unwrap()
            .unwrap();
        let revision = first
            .compare_and_set_trash_days(raised.revision, 7)
            .unwrap()
            .unwrap()
            .revision;
        assert_eq!(revision, 3);

        if flush_before_restore {
            // Split: the stale proposal is rejected, reverted and retired first.
            second.request_eager_flush();
            let _ = second.flush_due_pending();
        }
        second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        second
            .restore_retained_entry(
                receipt,
                ROOT_INO,
                b"restored",
                b"stage",
                &retained_test_source_fences(&second, receipt),
            )
            .unwrap();
        // Combined: the stale proposal shares a batch with the restore and must not win.
        for _ in 0..2 {
            second.request_eager_flush();
            let _ = second.flush_due_pending();
        }
        assert_eq!(
            second.replication_backlog().pending_ops,
            0,
            "stale expiry must not keep the journal pending (restart={restart}, flush_before_restore={flush_before_restore})"
        );
        first.sync_metadata().unwrap();
        let shared = first.retained_entry(receipt).unwrap().unwrap();
        assert!(
            matches!(shared.state, RetainedEntryState::Restored { .. }),
            "the accepted restore must win over the abandoned expiry (restart={restart}, flush_before_restore={flush_before_restore}): {:?}",
            shared.state
        );
        assert_eq!(
            first.lookup(ROOT_INO, b"restored").unwrap().ino,
            shared.inode
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; load-seeded authority must equal published authority"]
fn test_pg_retained_seeded_authority_matches_publication() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RETAINED_RECEIPT_XATTR, RetainedEntry,
        RetainedEntryState, RetainedPurgeReason, VolumeSettings,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-seed");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-seed-");
    let path = dir.path().join("local");
    seed_local(&path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 4,
            defer_remote_startup_probes: false,
        },
    )
    .unwrap();
    engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let next_seq = || {
        engine
            .local
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap()
    };
    let start = next_seq();
    engine.create_dir(ROOT_INO, b"kept", 0, 0, 0o700).unwrap();
    engine.create_dir(ROOT_INO, b"done", 0, 0, 0o700).unwrap();
    let store = engine
        .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
        .unwrap();
    let kept = engine.next_inode().unwrap();
    engine
        .retain_namespace_entry(ROOT_INO, b"kept", store.ino, kept)
        .unwrap();
    let done = engine.next_inode().unwrap();
    let admitted = engine
        .retain_namespace_entry(ROOT_INO, b"done", store.ino, done)
        .unwrap();
    engine.create_dir(done, b"stage", 0, 0, 0o700).unwrap();
    engine
        .restore_retained_entry(
            done,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&engine, done),
        )
        .unwrap();
    let events = engine.local.read_events_range(start, u64::MAX).unwrap();
    let head = engine.push_event_log_batch_locked(&events).unwrap();

    let volume_id = volume.volume_id;
    let projection = || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let rows = client
                .query(
                    "SELECT receipt, record FROM trash_entry_authority WHERE vol=$1 ORDER BY receipt",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok(rows
                .iter()
                .map(|row| (row.get::<_, i64>(0), row.get::<_, Vec<u8>>(1)))
                .collect::<Vec<_>>())
        }))
        .unwrap()
    };
    let published = projection();
    assert_eq!(published.len(), 2);
    let log = run_async(with_pg_test_client(&dsn, |client| async move {
        let rows = client
            .query(
                "SELECT batch_version, event_type, event_payload FROM event_log WHERE vol=$1 ORDER BY batch_version, seq_id",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1), row.get::<_, Vec<u8>>(2)))
            .collect::<Vec<_>>())
    }))
    .unwrap();
    let reseed = |checkpoint: Vec<u8>, cut: i64, log: Vec<(i64, String, Vec<u8>)>| {
        run_async(with_pg_test_client(&dsn, |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.execute(
                "DELETE FROM trash_entry_authority WHERE vol=$1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.execute(
                "UPDATE metadata_state SET volume_settings=NULL WHERE vol=$1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
            let events = log
                .iter()
                .map(|(version, kind, payload)| (*version, kind.as_str(), payload.as_slice()))
                .collect::<Vec<_>>();
            super::retained::seed_retained_authority(&tx, volume_id, &checkpoint, cut, &events)
                .await?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))
        }))
    };
    let shared_settings = || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let bytes: Option<Vec<u8>> = client
                .query_one(
                    "SELECT volume_settings FROM metadata_state WHERE vol=$1",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?
                .get(0);
            Ok(bytes.map(|bytes| serde_json::from_slice::<VolumeSettings>(&bytes).unwrap()))
        }))
        .unwrap()
    };
    let settings = engine.volume_settings().unwrap();
    assert_eq!(settings.trash.days(), 7);
    assert_eq!(shared_settings(), Some(settings));
    // Checkpoint at head (thin-checkpoint load) and pure log replay agree,
    // for the receipt projection and for the trash-days CAS row.
    let checkpoint = engine.local.export_serving_checkpoint_bytes().unwrap();
    reseed(checkpoint, head, Vec::new()).unwrap();
    assert_eq!(projection(), published);
    assert_eq!(shared_settings(), Some(settings));
    let empty_checkpoint = vec![0; 6];
    reseed(empty_checkpoint.clone(), 0, log.clone()).unwrap();
    assert_eq!(projection(), published);
    assert_eq!(shared_settings(), Some(settings));
    // A terminal record without its admission is rejected and rolls back.
    let terminal_only = log
        .iter()
        .filter(|(_, _, payload)| {
            matches!(
                crate::events::decode_event(payload),
                Ok(crate::MetaEvent::SetXattr { ino, name, value, .. })
                    if ino == done && name == RETAINED_RECEIPT_XATTR
                        && !value.windows(8).any(|w| w == b"Retained")
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(terminal_only.len(), 1);
    assert!(reseed(empty_checkpoint, 0, terminal_only).is_err());
    assert_eq!(projection(), published);

    // The seeded projection fences publication exactly like the published one.
    let purge = RetainedEntry {
        state: RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        },
        revision: 1,
        ..admitted.clone()
    };
    let receipt_event = |entry: &RetainedEntry| crate::MetaEvent::SetXattr {
        ino: done,
        name: RETAINED_RECEIPT_XATTR.to_vec(),
        value: serde_json::to_vec(entry).unwrap(),
        flags: 0,
    };
    for conflicting in [receipt_event(&purge), receipt_event(&admitted)] {
        assert!(
            engine
                .push_event_log_batch_locked(std::slice::from_ref(&conflicting))
                .is_err(),
            "{conflicting:?} must not publish over the seeded terminal record"
        );
        assert_eq!(engine.remote_metadata_head_version().unwrap(), head);
    }
    let before_restore = next_seq();
    engine.create_dir(kept, b"stage", 0, 0, 0o700).unwrap();
    engine
        .restore_retained_entry(
            kept,
            ROOT_INO,
            b"kept-back",
            b"stage",
            &retained_test_source_fences(&engine, kept),
        )
        .unwrap();
    let restore = engine
        .local
        .read_events_range(before_restore, u64::MAX)
        .unwrap();
    assert!(engine.push_event_log_batch_locked(&restore).unwrap() > head);
}

#[test]
#[ignore = "requires PostgreSQL; real retained transition publication in both orders"]
fn test_pg_retained_restore_purge_conflict_preserves_local_loser() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedEntryState, RetainedPurgeReason,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    for purge_first in [true, false] {
        let volume = PgTestVolume::new(&dsn, "retained-conflict");
        let dir = studiofs_test_support::workspace_target_tempdir("retained-pg-");
        let first_path = dir.path().join("first");
        let second_path = dir.path().join("second");
        seed_local(&first_path, &volume.format_info());
        seed_local(&second_path, &volume.format_info());
        let first = PgMetaEngine::open_with_options(
            &first_path,
            PgOpenOptions {
                background_poll: false,
                fresh_client: false,
                pg_pool_size: 4,
                defer_remote_startup_probes: false,
            },
        )
        .unwrap();
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let child = first
            .create_dir(payload.ino, b"accepted-child", 0, 0, 0o700)
            .unwrap();
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        first.request_eager_flush();
        first.flush_due_pending().unwrap();
        first.sync_metadata().unwrap();
        let second = PgMetaEngine::open_with_options(
            &second_path,
            PgOpenOptions {
                background_poll: false,
                fresh_client: false,
                pg_pool_size: 4,
                defer_remote_startup_probes: false,
            },
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            first.request_eager_flush();
            first.flush_due_pending().unwrap();
            first.sync_metadata().unwrap();
            second.sync_metadata().unwrap();
            if second.retained_entry(receipt).unwrap().is_some() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "peer never observed the retained receipt"
            );
            std::thread::yield_now();
        }
        assert!(matches!(
            second.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Retained
        ));
        let first_seq = first
            .local
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        let second_seq = second
            .local
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        let staged = first.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        let staged_child = first
            .create_dir(staged.ino, b"accepted-child", 0, 0, 0o700)
            .unwrap();
        assert_ne!(staged.ino, payload.ino);
        assert_ne!(staged_child.ino, child.ino);
        first
            .restore_retained_entry(
                receipt,
                ROOT_INO,
                b"restored-offline",
                b"stage",
                &[
                    first.get_attr(payload.ino).unwrap(),
                    first.get_attr(child.ino).unwrap(),
                ]
                .map(|attr| crate::trash::RetainedSourceFence {
                    ino: attr.ino,
                    ctime: attr.ctime,
                    size: attr.size,
                    content_epoch: attr.content_epoch,
                }),
            )
            .unwrap();
        second
            .request_retained_purge(receipt, RetainedPurgeReason::User)
            .unwrap();
        let accepted_restore = first.retained_entry(receipt).unwrap().unwrap();
        let accepted_purge = second.retained_entry(receipt).unwrap().unwrap();
        let restore_events = first.local.read_events_range(first_seq, u64::MAX).unwrap();
        let purge_events = second
            .local
            .read_events_range(second_seq, u64::MAX)
            .unwrap();
        let winner_version = if purge_first {
            let version = second.push_event_log_batch_locked(&purge_events).unwrap();
            assert!(first.push_event_log_batch_locked(&restore_events).is_err());
            version
        } else {
            let version = first.push_event_log_batch_locked(&restore_events).unwrap();
            assert!(second.push_event_log_batch_locked(&purge_events).is_err());
            version
        };
        let volume_id = volume.volume_id;
        let (record, version) = run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client.query_one("SELECT a.record,ms.version FROM trash_entry_authority a JOIN metadata_state ms USING(vol) WHERE a.vol=$1 AND a.receipt=$2", &[&volume_id, &(receipt as i64)]).await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok((row.get::<_, Vec<u8>>(0), row.get::<_, i64>(1)))
        })).unwrap();
        assert_eq!(
            version, winner_version,
            "rejected transition must roll back the version bump and event inserts"
        );
        let record = RedbMetaEngine::decode_retained_entry(receipt, &record).unwrap();
        assert_eq!(
            matches!(record.state, RetainedEntryState::PurgeRequested { .. }),
            purge_first
        );
        assert_eq!(
            first.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino
        );
        assert_eq!(
            first.lookup(payload.ino, b"accepted-child").unwrap().ino,
            child.ino
        );
        assert_eq!(
            first.local.read_events_range(first_seq, u64::MAX).unwrap(),
            restore_events
        );
        // A local or published purge request still owns its physical data link.
        assert_eq!(second.lookup(receipt, b"data").unwrap().ino, payload.ino);
        if purge_first {
            second.sync_metadata().unwrap();
            assert_eq!(
                second
                    .mutate(|local| local.settle_retained_purge(receipt, |_| false))
                    .unwrap(),
                crate::redb_engine::RetainedSettlement::Settled
            );
            second.request_eager_flush();
            second.flush_due_pending().unwrap();
            assert!(second.get_attr(payload.ino).is_err());
            assert!(second.get_attr(child.ino).is_err());
        }
        let purge_events = second
            .local
            .read_events_range(second_seq, u64::MAX)
            .unwrap();
        // Pull the winner over the accepted local transition before reopening.
        // A rejected publication must not let foreign replay erase local recovery.
        first.sync_metadata().unwrap();
        second.sync_metadata().unwrap();
        assert_eq!(
            first.retained_entry(receipt).unwrap(),
            Some(accepted_restore.clone())
        );
        assert_eq!(
            second.retained_entry(receipt).unwrap(),
            Some(accepted_purge.clone())
        );
        assert_eq!(
            first.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino
        );
        assert_eq!(
            first.local.read_events_range(first_seq, u64::MAX).unwrap(),
            restore_events
        );
        // Restart with the accepted reconstruction still unpublished, after
        // actual foreign RemoveAttr/RemoveEntry settlement has been applied.
        drop(first);
        let first = PgMetaEngine::open_with_options(
            &first_path,
            PgOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            first.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino
        );
        assert_eq!(
            first.lookup(payload.ino, b"accepted-child").unwrap().ino,
            child.ino
        );
        // Only the losing terminal receipt record is dropped from publication;
        // the rest of the accepted local journal must not stay blocked behind it.
        let loser = if purge_first { &first } else { &second };
        loser.request_eager_flush();
        // A losing restore has a namespace remainder, so its first attempt reports
        // the conflict; a losing purge is the whole batch and retires cleanly.
        assert_eq!(
            loser.flush_due_pending().is_err(),
            purge_first,
            "only a loser with a publishable remainder reports the conflict"
        );
        loser.request_eager_flush();
        loser.flush_due_pending().unwrap();
        second.sync_metadata().unwrap();
        assert_eq!(
            second.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino,
            "the accepted restore namespace change is republished in both orders"
        );
        let shared = run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT record FROM trash_entry_authority WHERE vol=$1 AND receipt=$2",
                    &[&volume_id, &(receipt as i64)],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok(row.get::<_, Vec<u8>>(0))
        }))
        .unwrap();
        assert_eq!(
            RedbMetaEngine::decode_retained_entry(receipt, &shared).unwrap(),
            record,
            "shared receipt authority is never overwritten by the loser"
        );
        // A losing terminal record is final: after reopen nothing republishes
        // the accepted journal, and the durable cursor passes the loser.
        let shared_log_position = || {
            run_async(with_pg_test_client(&dsn, |client| async move {
                let row = client
                    .query_one(
                        "SELECT (SELECT version FROM metadata_state WHERE vol=$1), (SELECT count(*) FROM event_log WHERE vol=$1)",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
                Ok((row.get::<_, i64>(0), row.get::<_, i64>(1)))
            }))
            .unwrap()
        };
        let position_before_reopen = shared_log_position();
        drop(first);
        drop(second);
        let reopened = PgMetaEngine::open_with_options(
            &first_path,
            PgOpenOptions {
                background_poll: false,
                fresh_client: false,
                pg_pool_size: 4,
                defer_remote_startup_probes: false,
            },
        )
        .unwrap();
        assert_eq!(
            reopened.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino
        );
        assert_eq!(
            reopened.lookup(payload.ino, b"accepted-child").unwrap().ino,
            child.ino
        );
        assert_eq!(
            reopened.retained_entry(receipt).unwrap(),
            Some(accepted_restore)
        );
        assert_eq!(
            reopened
                .local
                .read_events_range(first_seq, u64::MAX)
                .unwrap(),
            restore_events
        );
        let reopened_purge = PgMetaEngine::open_with_options(
            &second_path,
            PgOpenOptions {
                background_poll: false,
                fresh_client: false,
                pg_pool_size: 4,
                defer_remote_startup_probes: false,
            },
        )
        .unwrap();
        assert_eq!(
            reopened_purge.retained_entry(receipt).unwrap(),
            Some(accepted_purge)
        );
        // In both orders the accepted restore's namespace move is published, so
        // the receipt keeps no data link and nothing is lost to the purge.
        assert_eq!(
            reopened_purge
                .lookup(ROOT_INO, b"restored-offline")
                .unwrap()
                .ino,
            payload.ino
        );
        assert!(reopened_purge.lookup(receipt, b"data").is_err());
        assert_eq!(
            reopened_purge
                .local
                .read_events_range(second_seq, u64::MAX)
                .unwrap(),
            purge_events
        );
        for engine in [&reopened, &reopened_purge] {
            for _ in 0..2 {
                engine.request_eager_flush();
                engine.flush_due_pending().unwrap();
                engine.sync_metadata().unwrap();
            }
        }
        assert_eq!(
            shared_log_position(),
            position_before_reopen,
            "a retired loser must not republish its already-published journal"
        );
        for engine in [&reopened, &reopened_purge] {
            let next_seq = engine
                .local
                .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                .unwrap();
            assert_eq!(engine.flushed_event_seq.load(Ordering::SeqCst), next_seq);
            engine.prune_local_event_log_once().unwrap();
            assert_eq!(engine.local.first_event_seq().unwrap(), None);
        }

        // A client bootstrapped from the LOSER's serving checkpoint (whose
        // overlay still holds its retired record) must hold the winner in base.
        let (head, _) = position_before_reopen;
        let loser = if purge_first {
            &reopened
        } else {
            &reopened_purge
        };
        let checkpoint = loser.local.export_serving_checkpoint_bytes().unwrap();
        let cold_path = dir.path().join("cold");
        let cold_seed = RedbMetaEngine::open(&cold_path).unwrap();
        cold_seed
            .initialize_discovered_postgres_checkpoint(&volume.format_info(), &checkpoint, head)
            .unwrap();
        drop(cold_seed);
        let cold = PgMetaEngine::open_with_options(
            &cold_path,
            PgOpenOptions {
                background_poll: false,
                fresh_client: false,
                pg_pool_size: 4,
                defer_remote_startup_probes: false,
            },
        )
        .unwrap();
        assert_eq!(
            cold.retained_entry(receipt).unwrap(),
            Some(record.clone()),
            "a checkpoint-bootstrapped client must hold the shared winner, never the loser"
        );
        assert_eq!(
            cold.lookup(ROOT_INO, b"restored-offline").unwrap().ino,
            payload.ino
        );
        // Discovery leaves the fresh-client gate set; the background poll would
        // complete it (client slot + allocator) once the target is reached.
        cold.sync_metadata().unwrap();
        cold.try_complete_catchup_sync().unwrap();
        assert!(cold.pg_sync_status().allows_new_mutations());
        // Its own later publication self-acks normally: exactly one new batch,
        // no receipt record republished, and the winner stays everywhere.
        let cold_seq = cold
            .local
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        cold.create_dir(ROOT_INO, b"after-bootstrap", 0, 0, 0o700)
            .unwrap();
        for _ in 0..2 {
            cold.request_eager_flush();
            cold.flush_due_pending().unwrap();
            cold.sync_metadata().unwrap();
        }
        assert_eq!(
            cold.flushed_event_seq.load(Ordering::SeqCst),
            cold.local
                .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                .unwrap()
        );
        assert!(cold_seq < cold.flushed_event_seq.load(Ordering::SeqCst));
        let receipt_rows_after = run_async(with_pg_test_client(&dsn, |client| async move {
            let rows = client
                .query(
                    "SELECT batch_version, event_type, event_payload FROM event_log WHERE vol=$1 AND batch_version>$2",
                    &[&volume_id, &head],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok(rows
                .iter()
                .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1), row.get::<_, Vec<u8>>(2)))
                .collect::<Vec<_>>())
        }))
        .unwrap();
        assert!(
            receipt_rows_after
                .iter()
                .all(|(version, _, _)| *version == head + 1),
            "only the cold client's own batch is published after the cut"
        );
        assert!(
            receipt_rows_after
                .iter()
                .all(|(_, kind, payload)| !matches!(
                    crate::events::decode_event_with_type(kind, payload),
                    Ok(crate::MetaEvent::SetXattr { name, .. })
                        if name == crate::trash::RETAINED_RECEIPT_XATTR
                )),
            "no receipt record is republished after bootstrap"
        );
        assert_eq!(shared_log_position().0, head + 1);
        assert_eq!(cold.retained_entry(receipt).unwrap(), Some(record.clone()));
        let shared = run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT record FROM trash_entry_authority WHERE vol=$1 AND receipt=$2",
                    &[&volume_id, &(receipt as i64)],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok(row.get::<_, Vec<u8>>(0))
        }))
        .unwrap();
        assert_eq!(
            RedbMetaEngine::decode_retained_entry(receipt, &shared).unwrap(),
            record
        );
        // The loser replays the cold batch and still never republishes.
        loser.sync_metadata().unwrap();
        loser.request_eager_flush();
        loser.flush_due_pending().unwrap();
        assert_eq!(shared_log_position().0, head + 1);
        assert!(loser.lookup(ROOT_INO, b"after-bootstrap").is_ok());
        // Acknowledged restore has no permanent immunity from later peer deletion.
        reopened_purge
            .rmdir(payload.ino, b"accepted-child")
            .unwrap();
        reopened_purge.rmdir(ROOT_INO, b"restored-offline").unwrap();
        reopened_purge.request_eager_flush();
        reopened_purge.flush_due_pending().unwrap();
        reopened.sync_metadata().unwrap();
        assert!(reopened.lookup(ROOT_INO, b"restored-offline").is_err());
        assert!(reopened.get_attr(payload.ino).is_err());
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_format6_upgrade_fences_old_publication_and_deletion_claims() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "format6-upgrade");
    let mut legacy = volume.format_info();
    legacy.format_version = 5;
    let bytes = serde_json::to_vec(&legacy).unwrap();
    let volume_id = volume.volume_id;
    let volume_uuid = volume.uuid.clone();
    let lock_dsn = dsn.clone();
    run_async(with_pg_test_client(&dsn, |mut client| async move {
        client.execute("UPDATE metadata_state SET metadata_format_version=5, format_info=$2 WHERE vol=$1", &[&volume_id, &bytes]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let (locker, connection) = crate::connect_studiofs_postgres(&lock_dsn).await?;
        let connection_task = tokio::spawn(async move { let _ = connection.await; });
        let janitor_key = super::super::janitor::pg_janitor_lock_key(volume_id);
        locker.query_one("SELECT pg_advisory_lock($1)", &[&janitor_key]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        assert!(super::super::format_upgrade::upgrade_shared_format5(&mut client, volume_id, &volume_uuid).await.is_err());
        locker.query_one("SELECT pg_advisory_unlock($1)", &[&janitor_key]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        drop(locker);
        connection_task.abort();
        let _ = connection_task.await;
        // A claim made by old code before the transition must block it, even
        // when its lease expired; an S3 request may still be in flight.
        client.execute("INSERT INTO snapshot_payload_upload_objects(vol,object_id,state,reconcile_owner,reconcile_lease_expires_at,reconcile_fence) VALUES($1,7001,'delete_pending','old-owner',now()-interval '1 hour',1)", &[&volume_id]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        assert!(super::super::format_upgrade::upgrade_shared_format5(&mut client, volume_id, &volume_uuid).await.is_err());
        let row = client.query_one("SELECT metadata_format_version FROM metadata_state WHERE vol=$1", &[&volume_id]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        assert_eq!(row.get::<_, i16>(0), 5);
        client.execute("UPDATE snapshot_payload_upload_objects SET reconcile_owner=NULL WHERE vol=$1", &[&volume_id]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        super::super::format_upgrade::upgrade_shared_format5(&mut client, volume_id, &volume_uuid).await?;
        super::super::format_upgrade::upgrade_shared_format5(&mut client, volume_id, &volume_uuid).await?;
        assert!(client.query_opt(sql::BUMP_METADATA_VERSION, &[&volume_id, &5_i16, &0_i64, &1_i16]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?.is_none());
        assert!(client.execute("UPDATE snapshot_payload_upload_objects SET reconcile_owner='old-owner',reconcile_fence=2 WHERE vol=$1 AND object_id=7001", &[&volume_id]).await.is_err());
        let tx = client.transaction().await.map_err(|error| MetaError::Storage(error.to_string()))?;
        tx.query_one(super::super::janitor::CLAIM_PUBLICATION_FENCE_SQL, &[&volume_id]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        assert_eq!(tx.execute("UPDATE snapshot_payload_upload_objects SET reconcile_owner='new-owner',reconcile_fence=2 WHERE vol=$1 AND object_id=7001", &[&volume_id]).await
            .map_err(|error| MetaError::Storage(error.to_string()))?, 1);
        tx.rollback().await.map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(())
    })).unwrap();
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_volume_settings_authoritative_cas_converges_two_clients() {
    use crate::trash::MetaVolumeSettings;
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required; no silent skip");
    let dir = studiofs_test_support::workspace_target_tempdir("pg-trash-settings-");
    let volume = PgTestVolume::new(&dsn, "pg-trash-settings");
    let first_path = dir.path().join("first.redb");
    let second_path = dir.path().join("second.redb");
    seed_local(&first_path, &volume.format_info());
    seed_local(&second_path, &volume.format_info());
    let first = PgMetaEngine::open(&first_path).unwrap();
    let second = PgMetaEngine::open(&second_path).unwrap();
    // Client two reaches PG first despite both callers observing revision zero.
    let winner = second.compare_and_set_trash_days(0, 30).unwrap().unwrap();
    assert!(first.compare_and_set_trash_days(0, 7).unwrap().is_none());
    let disabled = first
        .compare_and_set_trash_days(winner.revision, 0)
        .unwrap()
        .unwrap();
    assert_eq!(disabled.trash.existing_entry_days(), 30);
    let (payloads, revisions) = run_async(with_pg_test_client(&dsn, |client| async move {
        let rows = client.query(
            "SELECT event_payload, batch_version FROM event_log WHERE vol=$1 AND event_type='set_volume_settings' ORDER BY batch_version",
            &[&volume.volume_id],
        ).await.map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok((rows.iter().map(|row| row.get::<_, Vec<u8>>(0)).collect::<Vec<_>>(),
            rows.iter().map(|row| row.get::<_, i64>(1)).collect::<Vec<_>>()))
    })).unwrap();
    assert_eq!(payloads.len(), 2);
    assert!(revisions[0] < revisions[1]);
    let events = payloads
        .iter()
        .map(|bytes| crate::events::decode_event(bytes).unwrap())
        .collect::<Vec<_>>();
    second.sync_metadata().unwrap();
    first.sync_metadata().unwrap();
    assert_eq!(second.volume_settings().unwrap(), disabled);
    assert_eq!(first.volume_settings().unwrap(), disabled);
    first.write_checkpoint_with_lease().unwrap();
    let third_path = dir.path().join("third.redb");
    seed_local(&third_path, &volume.format_info());
    let third = PgMetaEngine::open(&third_path).unwrap();
    third.sync_metadata().unwrap();
    assert_eq!(third.volume_settings().unwrap(), disabled);
    let limited = first
        .compare_and_set_logical_quota_bytes(disabled.revision, Some(1024))
        .unwrap()
        .unwrap();
    assert!(
        second
            .compare_and_set_logical_quota_bytes(disabled.revision, None)
            .unwrap()
            .is_none()
    );
    second.sync_metadata().unwrap();
    assert_eq!(second.volume_settings().unwrap(), limited);
    let retained = second
        .compare_and_set_trash_days(limited.revision, 7)
        .unwrap()
        .unwrap();
    assert_eq!(retained.logical_quota_bytes, Some(1024));
    third.sync_metadata().unwrap();
    assert_eq!(third.volume_settings().unwrap(), retained);
    assert!(
        first.push_event_log_batch_locked(&events).is_err(),
        "local journal cannot bypass global CAS"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_compression_policy_cas_checkpoint_and_replication() {
    use crate::trash::{CompressionPolicy, MetaVolumeSettings};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required; no silent skip");
    let dir = studiofs_test_support::workspace_target_tempdir("pg-compression-settings-");
    let volume = PgTestVolume::new(&dsn, "pg-compression-settings");
    let first_path = dir.path().join("first.redb");
    let second_path = dir.path().join("second.redb");
    seed_local(&first_path, &volume.format_info());
    seed_local(&second_path, &volume.format_info());
    let first = PgMetaEngine::open(&first_path).unwrap();
    let second = PgMetaEngine::open(&second_path).unwrap();

    // New unseeded volume reads as compression = off.
    assert_eq!(
        first.volume_settings().unwrap().compression,
        CompressionPolicy::off()
    );

    // Client 1 sets zstd:9 at revision 0 -> revision 1; client 2's stale CAS is rejected.
    let zstd9 = CompressionPolicy::zstd(9).unwrap();
    let updated = first
        .compare_and_set_compression(0, zstd9)
        .unwrap()
        .unwrap();
    assert_eq!(updated.revision, 1);
    assert_eq!(updated.compression, zstd9);
    assert!(
        second
            .compare_and_set_compression(0, CompressionPolicy::zstd_default())
            .unwrap()
            .is_none()
    );

    // Replication delivers the tag-21 event to client 2's local redb view.
    second.sync_metadata().unwrap();
    assert_eq!(second.volume_settings().unwrap(), updated);

    // v4 checkpoint preserves the compression policy for a freshly bootstrapped client.
    first.write_checkpoint_with_lease().unwrap();
    let third_path = dir.path().join("third.redb");
    seed_local(&third_path, &volume.format_info());
    let third = PgMetaEngine::open(&third_path).unwrap();
    third.sync_metadata().unwrap();
    assert_eq!(third.volume_settings().unwrap(), updated);

    // Switching back to off replicates to all peers.
    let disabled = second
        .compare_and_set_compression(updated.revision, CompressionPolicy::off())
        .unwrap()
        .unwrap();
    assert_eq!(disabled.revision, 2);
    assert_eq!(disabled.compression, CompressionPolicy::off());
    first.sync_metadata().unwrap();
    third.sync_metadata().unwrap();
    assert_eq!(first.volume_settings().unwrap(), disabled);
    assert_eq!(third.volume_settings().unwrap(), disabled);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_umeta_updates_requested_fields_atomically_and_refreshes_existing_cache() {
    use crate::engine::MetaEngine;
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required; no silent skip");
    let dir = studiofs_test_support::workspace_target_tempdir("pg-umeta-");
    let volume = PgTestVolume::new(&dsn, "pg-umeta");
    let original = volume.format_info();
    let volume_id = volume.volume_id;
    let expected_uuid = original.uuid.clone();
    let first_expected_uuid = expected_uuid.clone();
    let local_path = dir.path().join("existing.redb");
    seed_local(&local_path, &original);
    let updated = run_async(with_pg_test_client(&dsn, |mut client| async move {
        crate::pg_engine::update_shared_s3_settings(
            &mut client,
            volume_id,
            &first_expected_uuid,
            crate::pg_engine::S3VolumeSettingsUpdate {
                endpoint: Some("https://rotated.example.test"),
                bucket: Some("new-bucket"),
                access_key: Some("rotated-access"),
                secret_key: Some("rotated-secret"),
                region: Some("eu-central-1"),
                trash_days: Some(11),
            },
            "test-umeta",
            1,
        )
        .await
    }))
    .expect("atomic shared S3/trash update");
    assert_eq!(updated.endpoint, "https://rotated.example.test");
    assert_eq!(updated.bucket, "new-bucket");
    assert_eq!(updated.access_key, "rotated-access");
    assert_eq!(updated.secret_key, "rotated-secret");
    assert_eq!(updated.region, "eu-central-1");
    assert_eq!(updated.format_version, original.format_version);
    assert_eq!(updated.name, original.name);
    assert_eq!(updated.data_prefix, original.data_prefix);
    assert_eq!(updated.uuid, original.uuid);
    assert_eq!(updated.encryption, original.encryption);
    assert_eq!(updated.block_size, original.block_size);
    assert_eq!(updated.metadata.backend, original.metadata.backend);
    assert!(updated.metadata.remote.is_none());
    let original_pg = original
        .metadata
        .postgres
        .as_ref()
        .expect("original PostgreSQL format");
    assert!(
        updated.metadata.postgres.as_ref().is_some_and(|pg| {
            pg.volume_id == original_pg.volume_id && pg.url == original_pg.url
        })
    );

    let mut expected_only_access_key = serde_json::to_value(&updated).expect("encode format");
    expected_only_access_key["access_key"] = serde_json::Value::String("only-access".into());
    let access_key_only = run_async(with_pg_test_client(&dsn, |mut client| async move {
        crate::pg_engine::update_shared_s3_settings(
            &mut client,
            volume_id,
            &expected_uuid,
            crate::pg_engine::S3VolumeSettingsUpdate {
                access_key: Some("only-access"),
                ..Default::default()
            },
            "test-umeta-fields",
            2,
        )
        .await
    }))
    .expect("update only the requested access key");
    assert!(
        serde_json::to_value(access_key_only).expect("encode changed format")
            == expected_only_access_key,
        "an access-key-only update must preserve every other format field"
    );

    let local = PgMetaEngine::open_with_options(
        &local_path,
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..Default::default()
        },
    )
    .expect("reopen existing cache");
    let observed_credentials = std::sync::Arc::new(std::sync::Mutex::new(None));
    let observed = observed_credentials.clone();
    local.register_shared_s3_credentials_observer(std::sync::Arc::new(move |access, secret| {
        *observed.lock().unwrap() = Some((access.to_owned(), secret.to_owned()));
    }));
    assert_eq!(
        *observed_credentials.lock().unwrap(),
        Some((original.access_key.clone(), original.secret_key.clone())),
        "observer registration seeds the existing local credential pair"
    );
    assert_eq!(
        local
            .get_format_info()
            .expect("read cached format")
            .access_key,
        original.access_key,
        "reopening local metadata must not synchronously probe PostgreSQL for S3 settings"
    );
    let shared_format = crate::pg_engine::read_shared_volume_format_info(&dsn, volume_id)
        .expect("read shared format through the janitor/snapshot helper")
        .expect("shared format is published");
    assert_eq!(shared_format.uuid, original.uuid);
    assert_eq!(shared_format.access_key, "only-access");
    local
        .refresh_shared_s3_format_info()
        .expect("background poll refreshes shared S3 settings");
    assert_eq!(
        *observed_credentials.lock().unwrap(),
        Some(("only-access".into(), "rotated-secret".into())),
        "successful local format refresh publishes the changed credential pair"
    );
    let refreshed = local
        .get_format_info()
        .expect("read refreshed local format");
    assert_eq!(refreshed.access_key, "only-access");
    assert_eq!(refreshed.secret_key, "rotated-secret");
    assert_eq!(refreshed.endpoint, "https://rotated.example.test");

    // Publishing the checkpoint from this existing cache must keep shared config.
    local
        .write_checkpoint_with_lease()
        .expect("publish checkpoint");
    let stale_format_bytes = serde_json::to_vec(&original).expect("encode stale format");
    let stale_snapshot = Vec::<u8>::new();
    let stale_checksum = vec![0_u8; 32];
    let current_format_version = CURRENT_FORMAT_VERSION as i16;
    run_async(with_pg_test_client(&dsn, |client| async move {
        let published = client
            .query_opt(
                super::sql::UPSERT_METADATA_CHECKPOINT,
                &[
                    &volume_id,
                    &current_format_version,
                    &0_i64,
                    &0_i64,
                    &0_i64,
                    &stale_snapshot,
                    &stale_checksum,
                    &stale_format_bytes,
                ],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        assert!(
            published.is_some(),
            "stale fixture checkpoint should be accepted"
        );
        Ok(())
    }))
    .expect("publish deliberately stale checkpoint metadata");
    let remote = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT format_info, volume_settings FROM metadata_state WHERE vol=$1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let info: crate::FormatInfo = crate::decode_format_info(&row.get::<_, Vec<u8>>(0))?;
        let settings_bytes: Vec<u8> = row.get(1);
        let settings: crate::trash::VolumeSettings = serde_json::from_slice(&settings_bytes)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        Ok((info, settings))
    }))
    .expect("read shared state after checkpoint");
    assert_eq!(remote.0.secret_key, "rotated-secret");
    assert_eq!(remote.0.access_key, "only-access");
    assert_eq!(remote.1.trash.days(), 11);
    assert_eq!(remote.1.revision, 1);
}
use crate::engine::{
    CheckpointDescriptorFailure, MetaAcl, MetaError, MetadataCheckpointRefreshReason,
    PgSyncFailure, PgSyncPageDiagnostics,
};
use crate::redb_engine::RedbSnapshot;
use sha2::{Digest, Sha256};

#[test]
fn checkpoint_coordinator_coalesces_requests_until_consumed() {
    let state = super::checkpoint_coordinator::CheckpointCoordinatorState::new();

    assert!(state.request());
    assert!(!state.request());
    assert!(state.take_request());
    assert!(!state.take_request());
    assert!(state.request());
}

#[test]
fn checkpoint_lease_sql_is_fenced_and_short_lived() {
    let claim = super::sql::CLAIM_BOOTSTRAP_CHECKPOINT_LEASE;
    assert!(claim.contains("lease_expires_at"));
    assert!(claim.contains("fencing_token"));
    assert!(claim.contains("clock_timestamp()"));
    assert!(claim.contains("RETURNING"));

    let publish = super::sql::UPSERT_FENCED_METADATA_CHECKPOINT;
    assert!(publish.contains("fencing_token"));
    assert!(publish.contains("lease_expires_at > clock_timestamp()"));
    assert!(publish.contains("RETURNING"));
}

#[test]
fn checkpoint_coordination_miss_fails_closed_when_freshness_observation_fails() {
    let (_dir, engine) = temp_pg_engine_for_unit();

    assert!(
        !engine.refresh_checkpoint_freshness_after_coordination_miss(),
        "a failed freshness observation must not report a fresh checkpoint"
    );
}

async fn with_pg_test_client<T, F, Fut>(dsn: &str, f: F) -> Result<T, MetaError>
where
    F: FnOnce(tokio_postgres::Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, MetaError>>,
{
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let result = f(client).await;
    conn_task.abort();
    let _ = conn_task.await;
    result
}

fn open_pg_publisher(volume: &PgTestVolume) -> (tempfile::TempDir, Arc<PgMetaEngine>) {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("publisher.redb");
    seed_local(&db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata client");
    (dir, publisher)
}

#[derive(Debug, Clone)]
struct PgCheckpointData {
    snapshot: Vec<u8>,
    sha256: [u8; 32],
}

fn read_pg_checkpoint(dsn: &str, volume_id: i64) -> PgCheckpointData {
    run_async(with_pg_test_client(dsn, move |client| async move {
        let row = client
            .query_one(
                "SELECT snapshot, checkpoint_sha256 FROM metadata_state WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let snapshot: Vec<u8> = row
            .try_get("snapshot")
            .map_err(|error| MetaError::Storage(format!("invalid snapshot column: {error}")))?;
        let sha256: Vec<u8> = row.try_get("checkpoint_sha256").map_err(|error| {
            MetaError::Storage(format!("invalid checkpoint checksum column: {error}"))
        })?;
        let sha256: [u8; 32] = sha256.try_into().map_err(|bytes: Vec<u8>| {
            MetaError::Storage(format!(
                "invalid checkpoint checksum length: expected 32, got {}",
                bytes.len()
            ))
        })?;
        Ok::<PgCheckpointData, MetaError>(PgCheckpointData { snapshot, sha256 })
    }))
    .expect("read checkpoint")
}

fn read_pg_event_batch(dsn: &str, volume_id: i64, batch_version: i64) -> (i64, Vec<MetaEvent>) {
    run_async(with_pg_test_client(dsn, move |client| async move {
        let state = client
            .query_one(
                "SELECT version FROM metadata_state WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let rows = client
            .query(
                "SELECT event_type, event_payload FROM event_log WHERE vol = $1 AND batch_version = $2 ORDER BY seq_id",
                &[&volume_id, &batch_version],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let events = rows
            .into_iter()
            .map(|row| crate::events::decode_event_with_type(row.get(0), row.get(1)))
            .collect::<MetaResult<Vec<_>>>()?;
        Ok::<(i64, Vec<MetaEvent>), MetaError>((state.get(0), events))
    }))
    .expect("read event batch from postgres")
}

#[test]
fn pg_sync_lifecycle_raw_values_keep_fatal_explicit_and_unknown_fail_closed() {
    let cases = [
        (0, PgSyncLifecycle::LocalReady),
        (1, PgSyncLifecycle::CatchingUp),
        (2, PgSyncLifecycle::TargetReached),
        (3, PgSyncLifecycle::AllocatorPending),
        (4, PgSyncLifecycle::Live),
        (5, PgSyncLifecycle::Degraded),
        (6, PgSyncLifecycle::Fatal),
    ];

    for (raw, expected) in cases {
        assert_eq!(PgMetaEngine::pg_sync_lifecycle_from_raw(raw), expected);
    }
    assert_eq!(
        PgMetaEngine::pg_sync_lifecycle_from_raw(u8::MAX),
        PgSyncLifecycle::Fatal
    );
}

#[test]
fn pg_sync_diagnostics_reports_frozen_target_and_remaining_distance() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local_version
        .store(4, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(12, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(10, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );

    let diagnostics = engine.pg_sync_diagnostics();

    assert_eq!(diagnostics.lifecycle, PgSyncLifecycle::CatchingUp);
    assert_eq!(diagnostics.local_applied_version, Some(4));
    assert_eq!(diagnostics.startup_target_version, Some(10));
    assert_eq!(diagnostics.remote_head_version, Some(12));
    assert_eq!(diagnostics.remaining_version_distance, Some(6));
}

#[test]
fn rebootstrap_required_is_distinct_and_records_checkpoint_coordinates() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Live);
    let error = MetaError::RebootstrapRequired {
        local_version: 5,
        retained_floor: 8,
        checkpoint_version: 8,
        target_version: 12,
    };

    let returned = engine.record_remote_operation_result::<bool>(Err(error.clone()));

    assert!(matches!(
        returned,
        Err(MetaError::RebootstrapRequired {
            local_version: 5,
            retained_floor: 8,
            checkpoint_version: 8,
            target_version: 12,
        })
    ));
    assert_eq!(
        PgSyncFailure::from_error(&error),
        PgSyncFailure::RebootstrapRequired {
            local_version: Some(5),
            retained_floor: Some(8),
            checkpoint_version: Some(8),
            target_version: Some(12),
        }
    );
    let diagnostics = engine.pg_sync_diagnostics();
    assert!(diagnostics.rebootstrap_required);
    assert_eq!(
        diagnostics.last_sync_error,
        Some(PgSyncFailure::RebootstrapRequired {
            local_version: Some(5),
            retained_floor: Some(8),
            checkpoint_version: Some(8),
            target_version: Some(12),
        })
    );
    assert_eq!(
        diagnostics
            .last_sync_error
            .expect("rebootstrap diagnostic")
            .to_string(),
        "checkpoint rebootstrap required (local cursor 5, retained floor 8, checkpoint 8, target 12)"
    );
    assert!(diagnostics.source_connected);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
}

#[test]
fn pg_sync_diagnostics_reports_distance_from_uninitialized_cursor() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local_version
        .store(-1, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(100, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );

    let diagnostics = engine.pg_sync_diagnostics();

    assert_eq!(diagnostics.local_applied_version, None);
    assert_eq!(diagnostics.startup_target_version, Some(100));
    assert_eq!(diagnostics.remaining_version_distance, Some(101));
}

#[test]
fn pg_sync_diagnostics_prefers_newer_checkpoint_head_observation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .metadata_remote_head_version
        .store(10, std::sync::atomic::Ordering::Release);
    engine.record_checkpoint_freshness_observation(
        super::super::metadata_io::CheckpointFreshnessObservation {
            head_version: 14,
            checkpoint_version: 10,
            total_event_payload_bytes: 0,
            format_info_present: true,
            event_payload_bytes: 0,
            checkpoint_age: Duration::ZERO,
        },
    );

    assert_eq!(engine.pg_sync_diagnostics().remote_head_version, Some(14));
}

#[test]
fn pg_sync_diagnostics_tracks_redacted_error_and_page_completion() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let failure = MetaError::RemoteUnavailable("test-postgres-dsn".into());
    engine.record_pg_sync_failure(&failure);

    let failed = engine.pg_sync_diagnostics();
    assert_eq!(
        failed.last_sync_error,
        Some(PgSyncFailure::RemoteUnavailable)
    );
    assert!(
        !serde_json::to_string(&failed)
            .unwrap()
            .contains("test-postgres-dsn")
    );

    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: 4,
        first_available_version: 8,
        target_version: 12,
    });
    let gap = engine.pg_sync_diagnostics();
    assert!(gap.rebootstrap_required);
    assert!(matches!(
        gap.last_sync_error,
        Some(PgSyncFailure::ReplayGap { .. })
    ));

    let first_page = PgSyncPageDiagnostics {
        first_batch_version: Some(1),
        last_batch_version: Some(4),
        batch_count: 2,
        event_count: 3,
        response_bytes: 64,
        has_more: true,
        oversized: false,
        ..Default::default()
    };
    let next_page = PgSyncPageDiagnostics {
        first_batch_version: Some(5),
        last_batch_version: Some(8),
        has_more: false,
        ..first_page
    };

    assert!(engine.record_pg_sync_page_selected(first_page));
    engine.record_pg_sync_failure(&MetaError::RemoteUnavailable("test-postgres-dsn".into()));
    assert_eq!(engine.pg_sync_diagnostics().current_page, None);

    assert!(!engine.record_pg_sync_page_selected(first_page));
    assert!(!engine.record_pg_sync_page_selected(first_page));
    assert!(engine.record_pg_sync_page_selected(next_page));
    assert_eq!(engine.pg_sync_diagnostics().current_page, Some(next_page));

    engine.record_pg_sync_page_applied(next_page);
    assert_eq!(engine.pg_sync_diagnostics().current_page, None);
    assert_eq!(engine.pg_sync_diagnostics().last_page, Some(next_page));

    engine.record_pg_sync_success();
    assert_eq!(engine.pg_sync_diagnostics().last_sync_error, None);
}

#[test]
fn worker_shutdown_during_format_validation_preserves_remote_state() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_transport_unavailable
        .store(true, Ordering::Release);
    engine.background_workers.signal_stop();
    let result = {
        let _scope = super::super::runtime::WorkerRemoteCancellationScope::enter();
        engine.ensure_remote_format_validated()
    };

    assert!(
        matches!(result, Err(MetaError::WorkerShutdownCancelled)),
        "{result:?}"
    );
    assert!(engine.remote_identity_validated.load(Ordering::Acquire));
    assert!(!engine.metadata_source_connected.load(Ordering::Acquire));
    assert!(engine.remote_transport_unavailable.load(Ordering::Acquire));
}

#[test]
fn checkpoint_worker_shutdown_cancellation_preserves_remote_validation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .metadata_source_connected
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .remote_transport_unavailable
        .store(true, std::sync::atomic::Ordering::Release);
    engine.background_workers.signal_stop();

    let result = {
        let _scope = super::super::runtime::WorkerRemoteCancellationScope::enter();
        engine.run_async(std::future::pending::<MetaResult<()>>())
    };
    let _ = engine.record_remote_operation_result(result);

    let diagnostics = engine.pg_sync_diagnostics();
    assert_eq!(diagnostics.last_sync_error, Some(PgSyncFailure::Cancelled));
    assert!(
        engine
            .remote_format_validated
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        engine
            .remote_identity_validated
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(!diagnostics.source_connected);
    assert!(
        engine
            .remote_transport_unavailable
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[test]
fn pg_sync_success_clears_rebootstrap_requirement() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: -1,
        first_available_version: 1,
        target_version: 3,
    });
    assert!(engine.pg_sync_diagnostics().rebootstrap_required);

    engine.record_pg_sync_success();

    assert!(!engine.pg_sync_diagnostics().rebootstrap_required);
}

#[test]
fn idle_degraded_sync_success_keeps_rebootstrap_requirement() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: 1,
        first_available_version: 2,
        target_version: 3,
    });

    engine.record_pg_sync_success();

    assert!(engine.pg_sync_diagnostics().rebootstrap_required);
}

#[test]
fn pg_sync_failure_classifies_nested_and_oversized_errors() {
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::EventTooLarge {
            estimated: 10,
            limit: 5,
        }),
        PgSyncFailure::Storage
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::RepairRequired("repair".into())),
        PgSyncFailure::Storage
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::Serialization("decode".into())),
        PgSyncFailure::Storage
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::RegularFileOverlayBatchOperation {
            index: 0,
            source: Box::new(MetaError::RemoteUnavailable("test-postgres-dsn".into(),)),
        }),
        PgSyncFailure::RemoteUnavailable
    );
}

#[test]
fn pg_sync_failure_preserves_uninitialized_versions() {
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::ReplayGap {
            local_version: -1,
            first_available_version: 1,
            target_version: 3,
        }),
        PgSyncFailure::ReplayGap {
            local_version: None,
            first_available_version: Some(1),
            target_version: Some(3),
        }
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::CatchUpStalled {
            local_version: -1,
            target_version: 3,
        }),
        PgSyncFailure::CatchUpStalled {
            local_version: None,
            target_version: Some(3),
        }
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::CatchUpBudgetExceeded {
            local_version: -1,
            target_version: 3,
            pages: 0,
        }),
        PgSyncFailure::CatchUpBudgetExceeded {
            local_version: None,
            target_version: Some(3),
            pages: 0,
        }
    );
    assert_eq!(
        PgSyncFailure::from_error(&MetaError::RebootstrapRequired {
            local_version: -1,
            retained_floor: -1,
            checkpoint_version: -1,
            target_version: -1,
        }),
        PgSyncFailure::RebootstrapRequired {
            local_version: None,
            retained_floor: None,
            checkpoint_version: None,
            target_version: None,
        }
    );
}

#[test]
fn startup_catch_up_drain_replays_pages_until_frozen_target() {
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        3,
        || local_version.get(),
        || true,
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect("startup catch-up should drain each bounded page");

    assert!(changed);
    assert_eq!(local_version.get(), 3);
    assert_eq!(pulls.get(), 3);
}

#[test]
fn startup_rebootstrap_requires_snapshot_import_before_refreshing_target() {
    let rebootstrap_error = MetaError::RebootstrapRequired {
        local_version: 5,
        retained_floor: 8,
        checkpoint_version: 8,
        target_version: 12,
    };
    let target_calls = std::cell::Cell::new(0_usize);

    let error = rebootstrap_startup_target(
        rebootstrap_error.clone(),
        || Ok(false),
        || {
            target_calls.set(target_calls.get() + 1);
            Ok(15)
        },
    )
    .expect_err("a missing snapshot must keep recovery failed");

    assert!(matches!(error, MetaError::RebootstrapRequired { .. }));
    assert_eq!(target_calls.get(), 0);

    let target = rebootstrap_startup_target(
        rebootstrap_error,
        || Ok(true),
        || {
            target_calls.set(target_calls.get() + 1);
            Ok(15)
        },
    )
    .expect("an imported snapshot should allow a fresh target");

    assert_eq!(target, 15);
    assert_eq!(target_calls.get(), 1);
}

#[test]
fn startup_rebootstrap_propagates_snapshot_import_error_without_refreshing_target() {
    let rebootstrap_error = MetaError::RebootstrapRequired {
        local_version: 5,
        retained_floor: 8,
        checkpoint_version: 8,
        target_version: 12,
    };
    let target_calls = std::cell::Cell::new(0_usize);

    let error = rebootstrap_startup_target(
        rebootstrap_error,
        || Err(MetaError::Storage("io failure".to_string())),
        || {
            target_calls.set(target_calls.get() + 1);
            Ok(15)
        },
    )
    .expect_err("snapshot import errors must be returned unchanged");

    assert!(matches!(
        error,
        MetaError::Storage(message) if message == "io failure"
    ));
    assert_eq!(target_calls.get(), 0);
}

#[test]
fn startup_catch_up_drain_yields_at_page_budget_and_resumes_from_cursor() {
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);

    let error = drain_startup_catch_up_pages(
        17,
        || local_version.get(),
        || true,
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect_err("a startup session must yield before monopolizing replication");

    assert!(matches!(
        error,
        MetaError::CatchUpBudgetExceeded {
            local_version: 16,
            target_version: 17,
            pages: 16
        }
    ));
    assert_eq!(local_version.get(), 16);
    assert_eq!(pulls.get(), 16);

    let resumed_pulls = std::cell::Cell::new(0_usize);
    let changed = drain_startup_catch_up_pages(
        17,
        || local_version.get(),
        || true,
        || {
            resumed_pulls.set(resumed_pulls.get() + 1);
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect("the next session must resume from the durable cursor");

    assert!(changed);
    assert_eq!(local_version.get(), 17);
    assert_eq!(resumed_pulls.get(), 1);
}

#[test]
fn startup_catch_up_drain_rejects_page_without_cursor_progress() {
    let error = drain_startup_catch_up_pages(1, || 0, || true, || Ok(false))
        .expect_err("a stalled startup catch-up must be reported");

    assert!(matches!(
        error,
        MetaError::CatchUpStalled {
            local_version: 0,
            target_version: 1
        }
    ));
}

#[test]
fn startup_catch_up_drain_stops_when_session_is_cancelled() {
    let pulls = std::cell::Cell::new(0_usize);
    let changed = drain_startup_catch_up_pages(
        1,
        || 0,
        || false,
        || {
            pulls.set(pulls.get() + 1);
            Ok(true)
        },
    )
    .expect("cancelled startup catch-up should return cleanly");

    assert!(!changed);
    assert_eq!(pulls.get(), 0);
}

#[test]
fn startup_catch_up_drain_stops_after_partial_progress_when_cancelled() {
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);
    let continue_calls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || local_version.get(),
        || {
            let call = continue_calls.get();
            continue_calls.set(call + 1);
            call == 0
        },
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(1);
            Ok(true)
        },
    )
    .expect("cancellation after a committed page should return cleanly");

    assert!(changed);
    assert_eq!(local_version.get(), 1);
    assert_eq!(pulls.get(), 1);
    assert_eq!(continue_calls.get(), 2);
}

#[test]
fn startup_catch_up_drain_rejects_negative_target() {
    let error = drain_startup_catch_up_pages(-1, || 0, || true, || Ok(false))
        .expect_err("a startup catch-up target must not be negative");

    assert!(error.to_string().contains("target must not be negative"));
}

#[test]
fn startup_catch_up_drain_skips_pull_when_already_at_target() {
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        3,
        || 3,
        || panic!("continuation must not be checked at the target"),
        || {
            pulls.set(pulls.get() + 1);
            Ok(true)
        },
    )
    .expect("an already reached target should complete");

    assert!(!changed);
    assert_eq!(pulls.get(), 0);
}

#[test]
fn startup_catch_up_drain_skips_pull_when_local_cursor_exceeds_target() {
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || 5,
        || panic!("continuation must not be checked past the target"),
        || {
            pulls.set(pulls.get() + 1);
            Ok(true)
        },
    )
    .expect("a cursor past the target should complete");

    assert!(!changed);
    assert_eq!(pulls.get(), 0);
}

#[test]
fn startup_catch_up_drain_propagates_page_error() {
    let error = drain_startup_catch_up_pages(
        1,
        || 0,
        || true,
        || Err(MetaError::Storage("page failure".to_string())),
    )
    .expect_err("a page error must terminate startup catch-up");

    assert!(error.to_string().contains("page failure"));
}

#[test]
fn startup_catch_up_drain_reads_cursor_once_per_loop_iteration() {
    let local_version = std::cell::Cell::new(0_i64);
    let reads = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || {
            reads.set(reads.get() + 1);
            local_version.get()
        },
        || true,
        || {
            local_version.set(local_version.get() + 1);
            Ok(true)
        },
    )
    .expect("cursor should reach the target");

    assert!(changed);
    assert_eq!(reads.get(), 3);
}

#[test]
fn startup_catch_up_stall_resets_target_and_enters_degraded_lifecycle() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    engine
        .metadata_startup_target_version
        .store(9, std::sync::atomic::Ordering::Release);

    engine.reset_startup_catch_up_after_stall();

    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        -1
    );
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

#[test]
fn startup_catch_up_session_rejects_fatal_lifecycle_as_success() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::Fatal as u8,
        std::sync::atomic::Ordering::Release,
    );

    let error = engine
        .run_startup_catch_up_session(
            MetadataSyncSource::Manual,
            &std::sync::atomic::AtomicBool::new(false),
        )
        .expect_err("fatal lifecycle must not be reported as a successful sync");

    assert!(matches!(error, MetaError::LifecycleFatal));
}

#[test]
fn startup_catch_up_session_honors_shutdown_signal() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    let shutdown = std::sync::atomic::AtomicBool::new(true);

    let changed = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect("shutdown should stop startup catch-up cleanly");

    assert!(!changed);
}

#[test]
fn startup_catch_up_skips_page_fetch_after_worker_shutdown() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .metadata_startup_target_version
        .store(1, std::sync::atomic::Ordering::Release);
    engine
        .background_workers
        .poll_shutdown_ref()
        .store(true, std::sync::atomic::Ordering::Release);
    engine.set_catch_up_fault_hook(std::sync::Arc::new(|_| {
        Err(MetaError::Timeout("unexpected page fetch".to_string()))
    }));
    let caller_shutdown = std::sync::atomic::AtomicBool::new(false);

    let changed = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &caller_shutdown)
        .expect("worker shutdown should stop catch-up before fetching another page");

    assert!(!changed);
}

#[test]
fn replay_cancellation_includes_worker_and_caller_shutdown() {
    use super::super::metadata_sync::replay_cancelled;

    let shutdown = std::sync::atomic::AtomicBool::new(true);
    let running = std::sync::atomic::AtomicBool::new(false);

    assert!(!replay_cancelled(&running, None));
    assert!(!replay_cancelled(&running, Some(&running)));
    assert!(replay_cancelled(&shutdown, None));
    assert!(replay_cancelled(&running, Some(&shutdown)));
    assert!(replay_cancelled(&shutdown, Some(&running)));
    assert!(replay_cancelled(&shutdown, Some(&shutdown)));
}

#[test]
fn startup_catch_up_drain_honors_shutdown_after_a_page() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .metadata_startup_target_version
        .store(2, std::sync::atomic::Ordering::Release);
    let shutdown = std::sync::atomic::AtomicBool::new(false);
    let local_version = std::cell::Cell::new(0_i64);
    let pulls = std::cell::Cell::new(0_usize);

    let changed = drain_startup_catch_up_pages(
        2,
        || local_version.get(),
        || !shutdown.load(std::sync::atomic::Ordering::Acquire),
        || {
            pulls.set(pulls.get() + 1);
            local_version.set(1);
            shutdown.store(true, std::sync::atomic::Ordering::Release);
            Ok(true)
        },
    )
    .expect("shutdown after a committed page should stop catch-up cleanly");

    assert!(changed);
    assert_eq!(pulls.get(), 1);
    engine
        .local_version
        .store(local_version.get(), std::sync::atomic::Ordering::Release);
    engine
        .try_complete_catchup_sync()
        .expect("incomplete shutdown catch-up should remain recoverable");
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

struct StartupCatchUpFaultFixture {
    _volume: PgTestVolume,
    _dir: tempfile::TempDir,
    db: std::path::PathBuf,
    target_version: i64,
    stale: Option<std::sync::Arc<PgMetaEngine>>,
}

type StartupCatchUpFaultObservations = std::sync::Arc<std::sync::Mutex<Vec<CatchUpFaultPoint>>>;

const STARTUP_CATCH_UP_FAULT_PAGES: &[&[u8]] = &[
    b"fault-page-1",
    b"fault-page-2",
    b"fault-page-3",
    b"fault-page-4",
];

fn startup_catch_up_fault_fixture() -> StartupCatchUpFaultFixture {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-startup-catch-up-faults");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("stale.redb");
    seed_local(&db, &volume.format_info());

    let stale = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 0);

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata client");
    for &name in STARTUP_CATCH_UP_FAULT_PAGES {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create catch-up event");
        publisher.flush_metadata().expect("publish catch-up event");
    }
    drop(publisher);
    drop(publisher_dir);

    let empty_checkpoint = postcard::to_stdvec(&crate::snapshot::ServingCheckpoint::default())
        .expect("encode empty replay-tail checkpoint");
    let empty_checkpoint_sha256 = Sha256::digest(&empty_checkpoint).to_vec();
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET checkpoint_version = 0, snapshot = $1, checkpoint_sha256 = $2 WHERE vol = $3",
                    &[&empty_checkpoint, &empty_checkpoint_sha256, &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("pin replay-tail checkpoint at version zero");

    StartupCatchUpFaultFixture {
        _volume: volume,
        _dir: dir,
        db,
        target_version: STARTUP_CATCH_UP_FAULT_PAGES.len() as i64,
        stale: Some(stale),
    }
}

fn startup_catch_up_fault_engine(
    fixture: &mut StartupCatchUpFaultFixture,
) -> std::sync::Arc<PgMetaEngine> {
    let engine = fixture.stale.take().unwrap_or_else(|| {
        PgMetaEngine::open_with_options(
            &fixture.db,
            PgOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .expect("open catch-up metadata client")
    });
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    assert_eq!(
        engine
            .frozen_sync_target_version()
            .expect("freeze catch-up target"),
        fixture.target_version
    );
    engine
}

fn startup_catch_up_fault_hook(
    should_fail: impl Fn(CatchUpFaultPoint) -> bool + Send + Sync + 'static,
) -> (StartupCatchUpFaultObservations, CatchUpFaultHook) {
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_hook = std::sync::Arc::clone(&observed);
    let fired_hook = std::sync::Arc::clone(&fired);
    let hook = std::sync::Arc::new(move |point: CatchUpFaultPoint| {
        observed_hook
            .lock()
            .expect("catch-up fault observation lock")
            .push(point);
        // Inject the fault once so recovery can continue after the first failure.
        if should_fail(point) && !fired_hook.swap(true, std::sync::atomic::Ordering::AcqRel) {
            Err(MetaError::Timeout("injected catch-up fault".to_string()))
        } else {
            Ok(())
        }
    });
    (observed, hook)
}

fn persisted_startup_catch_up_cursor(path: &std::path::Path) -> i64 {
    let local = RedbMetaEngine::open(path).expect("open local metadata for cursor check");
    PgMetaEngine::load_persisted_local_version(&local).expect("read persisted catch-up cursor")
}

fn startup_catch_up_fault_resume(fixture: &mut StartupCatchUpFaultFixture, expected_cursor: i64) {
    assert_eq!(
        persisted_startup_catch_up_cursor(&fixture.db),
        expected_cursor
    );
    let engine = startup_catch_up_fault_engine(fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|_| false);
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        engine
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("resumed catch-up session")
    );
    assert_eq!(
        engine.local_version.load(Ordering::Acquire),
        fixture.target_version
    );
    for &name in STARTUP_CATCH_UP_FAULT_PAGES {
        engine
            .lookup(ROOT_INO, name)
            .expect("resumed catch-up should expose every page");
    }
    let first_fetch = observed
        .lock()
        .expect("read resumed catch-up observations")
        .iter()
        .copied()
        .find(|point| matches!(point, CatchUpFaultPoint::BeforePageFetch { .. }));
    assert_eq!(
        first_fetch,
        Some(CatchUpFaultPoint::BeforePageFetch {
            cursor: expected_cursor
        }),
        "resumed catch-up must begin at the durable cursor"
    );
    drop(engine);
    assert_eq!(
        persisted_startup_catch_up_cursor(&fixture.db),
        fixture.target_version,
        "resumed catch-up must persist the final target version to redb"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_before_page_fetch_reopens_from_original_cursor() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(point, CatchUpFaultPoint::BeforePageFetch { cursor: 0 })
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("a pre-fetch fault must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(point, CatchUpFaultPoint::BeforePageFetch { cursor: 0 }))
    );
    drop(engine);
    assert_eq!(
        persisted_startup_catch_up_cursor(&fixture.db),
        0,
        "a pre-fetch fault must not advance the durable cursor"
    );

    startup_catch_up_fault_resume(&mut fixture, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_after_later_page_fetch_reopens_from_last_committed_page() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(
            point,
            CatchUpFaultPoint::AfterPageFetch {
                cursor: 1,
                first_batch_version: Some(2),
                last_batch_version: Some(2),
            }
        )
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("a later page fetch fault must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert_eq!(
        engine.local_version.load(Ordering::Acquire),
        1,
        "the first page should remain applied in memory"
    );
    engine
        .lookup(ROOT_INO, b"fault-page-1")
        .expect("the first page should remain visible");
    assert!(matches!(
        engine.lookup(ROOT_INO, b"fault-page-2"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(point, CatchUpFaultPoint::AfterPageFetch { cursor: 1, .. }))
    );
    drop(engine);
    assert_eq!(persisted_startup_catch_up_cursor(&fixture.db), 1);

    startup_catch_up_fault_resume(&mut fixture, 1);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_before_local_commit_reopens_without_partial_page() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(
            point,
            CatchUpFaultPoint::BeforeLocalCommit {
                last_batch_version: 2
            }
        )
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("a pre-commit fault must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert_eq!(engine.local_version.load(Ordering::Acquire), 1);
    engine
        .lookup(ROOT_INO, b"fault-page-1")
        .expect("the first page should remain visible");
    assert!(matches!(
        engine.lookup(ROOT_INO, b"fault-page-2"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(
                point,
                CatchUpFaultPoint::BeforeLocalCommit {
                    last_batch_version: 2
                }
            ))
    );
    drop(engine);
    assert_eq!(persisted_startup_catch_up_cursor(&fixture.db), 1);

    startup_catch_up_fault_resume(&mut fixture, 1);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_fault_after_local_commit_reopens_from_committed_page() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let mut fixture = startup_catch_up_fault_fixture();
    let engine = startup_catch_up_fault_engine(&mut fixture);
    let (observed, hook) = startup_catch_up_fault_hook(|point| {
        matches!(
            point,
            CatchUpFaultPoint::AfterLocalCommit {
                last_batch_version: 1
            }
        )
    });
    engine.set_catch_up_fault_hook(hook);
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("an after-commit interruption must stop the session");
    assert!(matches!(error, MetaError::Timeout(_)));
    assert_eq!(
        engine.local_version.load(Ordering::Acquire),
        0,
        "the interrupted engine must not publish its post-commit cursor"
    );
    engine
        .lookup(ROOT_INO, b"fault-page-1")
        .expect("the committed page should remain visible after interruption");
    assert!(
        observed
            .lock()
            .expect("read catch-up fault observations")
            .iter()
            .any(|point| matches!(
                point,
                CatchUpFaultPoint::AfterLocalCommit {
                    last_batch_version: 1
                }
            ))
    );
    drop(engine);
    assert_eq!(persisted_startup_catch_up_cursor(&fixture.db), 1);

    startup_catch_up_fault_resume(&mut fixture, 1);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_single_oversized_batch_is_applied_atomically() {
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let _byte_limit = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "1");
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-startup-catch-up-oversized-batch");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());

    let events = vec![
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"oversized-page-1".to_vec(),
            entry: DirEntryValue {
                ino: 11,
                kind: FileType::Directory,
            },
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"oversized-page-2".to_vec(),
            entry: DirEntryValue {
                ino: 12,
                kind: FileType::Directory,
            },
        },
    ];
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |mut client| async move {
            insert_encoded_event_batch(&mut client, volume_id, 1, "remote-oversized", &events)
                .await?;
            client
                .execute(
                    "UPDATE metadata_state SET version = 1 WHERE vol = $1",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("insert oversized foreign batch");

    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open oversized-batch catch-up client");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    assert_eq!(
        engine
            .frozen_sync_target_version()
            .expect("freeze oversized-batch target"),
        1
    );
    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        engine
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("apply oversized foreign batch")
    );

    assert_eq!(engine.local_version.load(Ordering::Acquire), 1);
    for name in [b"oversized-page-1".as_slice(), b"oversized-page-2"] {
        engine
            .lookup(ROOT_INO, name)
            .expect("all rows in one oversized batch must be visible");
    }
    let diagnostics = engine.pg_sync_diagnostics();
    assert_eq!(diagnostics.last_page.map(|page| page.batch_count), Some(1));
    assert_eq!(diagnostics.last_page.map(|page| page.event_count), Some(2));
    assert!(diagnostics.last_page.is_some_and(|page| page.oversized));
}

struct EventLogLockGuard {
    release_tx: Option<tokio::sync::oneshot::Sender<()>>,
    lock_thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for EventLogLockGuard {
    fn drop(&mut self) {
        if let Some(release_tx) = self.release_tx.take() {
            let _ = release_tx.send(());
        }
        if let Some(lock_thread) = self.lock_thread.take() {
            let _ = lock_thread.join();
        }
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn stale_local_open_does_not_replay_remote_event_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "1000");
    let volume = PgTestVolume::new(&dsn, "pg-open-local-first");
    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata");
    publisher
        .create_dir(ROOT_INO, b"remote-entry", 0, 0, 0o755)
        .expect("create remote event");
    publisher
        .flush_metadata()
        .expect("publish first remote event");
    publisher
        .create_dir(ROOT_INO, b"remote-entry-2", 0, 0, 0o755)
        .expect("create second remote event");
    publisher
        .flush_metadata()
        .expect("publish second remote event");
    drop(publisher);

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let local = RedbMetaEngine::open(&db).expect("open seeded local metadata");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist stale replay cursor");
    drop(local);

    let (lock_ready_tx, lock_ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let lock_dsn = dsn.clone();
    let lock_thread = std::thread::spawn(move || {
        run_async(async move {
            let (mut client, connection) =
                tokio_postgres::connect(&lock_dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let transaction = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            transaction
                .batch_execute("LOCK TABLE event_log IN ACCESS EXCLUSIVE MODE")
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            lock_ready_tx
                .send(())
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            release_rx.await.map_err(|_| {
                MetaError::Storage("event-log lock release signal dropped".to_string())
            })?;
            transaction
                .rollback()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            connection_task.abort();
            let _ = connection_task.await;
            Ok(())
        })
        .expect("hold event-log lock");
    });
    let lock_guard = EventLogLockGuard {
        release_tx: Some(release_tx),
        lock_thread: Some(lock_thread),
    };
    lock_ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("wait for event-log lock");

    let started = Instant::now();
    let opened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("stale local metadata should open while event replay is blocked");
    let elapsed = started.elapsed();
    let lifecycle = opened.pg_sync_status().lifecycle;

    assert!(
        elapsed < Duration::from_millis(1_500),
        "open should not perform repeated event-tail replay, elapsed {elapsed:?}"
    );
    assert_eq!(
        lifecycle,
        PgSyncLifecycle::CatchingUp,
        "stale local metadata must remain explicitly non-live"
    );

    drop(lock_guard);
    opened
        .sync_metadata()
        .expect("catch up after event-log unlock");
    assert_eq!(opened.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(opened.pg_sync_status().allows_new_mutations());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_reports_rebootstrap_below_retained_event_log_floor() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-floor");
    let volume_id = volume.volume_id;
    run_async(with_pg_test_client(&dsn, move |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 12, checkpoint_version = 8, event_log_prune_floor = 8 WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("seed retained replay floor");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("stale.redb");
    seed_local(&db, &volume.format_info());
    let local = RedbMetaEngine::open(&db).expect("open seeded local metadata");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 5)
        .expect("persist cursor below retained floor");
    drop(local);

    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    let error = engine
        .sync_metadata_with_source(MetadataSyncSource::Manual)
        .expect_err("a cursor below retained history must require rebootstrap");

    assert!(matches!(
        error,
        MetaError::RebootstrapRequired {
            local_version: 5,
            retained_floor: 8,
            checkpoint_version: 8,
            target_version: 12,
        }
    ));
    let diagnostics = engine.pg_sync_diagnostics();
    assert!(diagnostics.rebootstrap_required);
    assert_eq!(
        diagnostics.last_sync_error,
        Some(PgSyncFailure::RebootstrapRequired {
            local_version: Some(5),
            retained_floor: Some(8),
            checkpoint_version: Some(8),
            target_version: Some(12),
        })
    );
    assert_eq!(diagnostics.lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn stale_client_rebootstrap_without_pending_overlay_reaches_live_and_replays_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-clean-client");

    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    for name in [b"checkpoint-clean-1".as_slice(), b"checkpoint-clean-2"] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create checkpoint event");
        publisher
            .flush_metadata()
            .expect("publish checkpoint event");
    }
    let checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    publisher
        .create_dir(ROOT_INO, b"replayed-after-clean-rebootstrap", 0, 0, 0o755)
        .expect("create replay tail event");
    publisher
        .flush_metadata()
        .expect("publish replay tail event");
    drop(publisher);

    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version <= 2",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "UPDATE metadata_state SET version = 3, checkpoint_version = 2, snapshot = $1, checkpoint_sha256 = $2, event_log_prune_floor = 2 WHERE vol = $3",
                    &[&checkpoint.snapshot, &checkpoint.sha256.as_slice(), &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("set retained replay floor");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let local = RedbMetaEngine::open(&stale_db).expect("open stale local metadata");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist stale client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist cursor below retained floor");
    drop(local);

    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open clean stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 1);
    assert!(matches!(
        stale.lookup(ROOT_INO, b"checkpoint-clean-2"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(matches!(
        stale.lookup(ROOT_INO, b"replayed-after-clean-rebootstrap"),
        Err(MetaError::EntryNotFound { .. })
    ));

    stale
        .sync_metadata()
        .expect("clean stale client should rebootstrap from the checkpoint");

    assert_eq!(stale.local_version.load(Ordering::Acquire), 3);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(stale.pg_sync_status().allows_new_mutations());
    for name in [
        b"checkpoint-clean-1".as_slice(),
        b"checkpoint-clean-2",
        b"replayed-after-clean-rebootstrap",
    ] {
        stale
            .lookup(ROOT_INO, name)
            .expect("checkpoint and replay tail entries must be visible");
    }
    stale
        .create_dir(ROOT_INO, b"post-rebootstrap", 0, 0, 0o755)
        .expect("create entry after clean rebootstrap");
    stale
        .flush_metadata()
        .expect("flush entry after clean rebootstrap");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 4);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_rebases_stale_client_from_replay_gap_preserving_pending_overlay() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-checkpoint");

    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    for name in [b"checkpoint-1".as_slice(), b"checkpoint-2"] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create checkpoint event");
        publisher
            .flush_metadata()
            .expect("publish checkpoint event");
    }
    let checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    publisher
        .create_dir(ROOT_INO, b"replayed-after-checkpoint", 0, 0, 0o755)
        .expect("create replay tail event");
    publisher
        .flush_metadata()
        .expect("publish replay tail event");
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version = 2",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "UPDATE metadata_state SET version = 3, checkpoint_version = 2, snapshot = $1, checkpoint_sha256 = $2, event_log_prune_floor = 0 WHERE vol = $3",
                    &[&checkpoint.snapshot, &checkpoint.sha256.as_slice(), &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("restore checkpoint descriptor at retained floor");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let local = RedbMetaEngine::open(&stale_db).expect("open stale local metadata");
    local
        .create_dir(ROOT_INO, b"pending-local", 0, 0, 0o755)
        .expect("create pending local overlay");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist stale client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist stale replay cursor");
    drop(local);

    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 1);
    assert_eq!(
        stale
            .remote_metadata_head_version()
            .expect("read remote head after stale client open"),
        3
    );
    stale
        .sync_metadata()
        .expect("manual sync should rebase a stale replay cursor");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 3);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        stale
            .metadata_invalidation_diagnostics()
            .fallback_poll_discoveries,
        0
    );
    publisher
        .create_dir(ROOT_INO, b"advanced-after-rebootstrap", 0, 0, 0o755)
        .expect("create event after rebootstrap detection");
    publisher
        .flush_metadata()
        .expect("publish event after rebootstrap detection");
    drop(publisher);

    stale
        .sync_metadata()
        .expect("manual sync should replay the post-rebase event");

    assert_eq!(stale.local_version.load(Ordering::Acquire), 4);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    for name in [
        b"checkpoint-1".as_slice(),
        b"checkpoint-2",
        b"replayed-after-checkpoint",
        b"advanced-after-rebootstrap",
        b"pending-local",
    ] {
        stale
            .lookup(ROOT_INO, name)
            .expect("checkpoint, replay, and pending entries must survive rebase");
    }

    stale
        .flush_metadata()
        .expect("flush preserved overlay after rebase");
    let (remote_version, remote_events) = read_pg_event_batch(&dsn, volume.volume_id, 5);
    assert_eq!(remote_version, 5);
    assert!(remote_events.iter().any(|event| matches!(
        event,
        MetaEvent::AddEntry { name, .. } if name.as_slice() == b"pending-local"
    )));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn stale_client_rebootstrap_failure_preserves_old_base_until_valid_checkpoint_is_available() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-rebootstrap-atomicity");

    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    publisher
        .create_dir(ROOT_INO, b"old-complete-base", 0, 0, 0o755)
        .expect("create old base event");
    publisher.flush_metadata().expect("publish old base event");
    let valid_checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    publisher
        .create_dir(ROOT_INO, b"new-complete-base", 0, 0, 0o755)
        .expect("create new base event");
    publisher.flush_metadata().expect("publish new base event");
    drop(publisher);

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let local = RedbMetaEngine::open(&stale_db).expect("open stale local metadata");
    local
        .import_serving_checkpoint_bytes(&valid_checkpoint.snapshot)
        .expect("seed stale client with old complete base");
    local
        .create_file(ROOT_INO, b"unflushed-pending.bin", 0, 0, 0o644)
        .expect("create unflushed pending overlay");
    PgMetaEngine::persist_client_slot(&local, 0).expect("persist stale client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 0)
        .expect("persist stale replay cursor");
    drop(local);

    let invalid_checksum = [0_u8; 32];
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        let checkpoint = valid_checkpoint.snapshot.clone();
        move |mut client| async move {
            let tx = client
                .transaction()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version < 2",
                    &[&volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx
                .execute(
                    "UPDATE metadata_state SET version = 2, checkpoint_version = 1, snapshot = $1, checkpoint_sha256 = $2, event_log_prune_floor = 1 WHERE vol = $3",
                    &[&checkpoint, &invalid_checksum.as_slice(), &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            tx.commit()
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("publish invalid checkpoint checksum");

    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("invalid checkpoint must not prevent local serving");
    assert!(stale.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(stale.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(matches!(
        stale.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    let error = stale
        .sync_metadata()
        .expect_err("checksum failure must stop before base replacement");
    assert!(matches!(
        error,
        MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::Checksum
        }
    ));
    assert!(stale.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(stale.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(matches!(
        stale.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert_eq!(stale.local_version.load(Ordering::Acquire), 0);

    let mut malformed_checkpoint: crate::snapshot::ServingCheckpoint =
        postcard::from_bytes(&valid_checkpoint.snapshot).expect("decode valid checkpoint");
    malformed_checkpoint
        .inodes
        .push((u64::MAX - 1, b"invalid-inode-encoding".to_vec()));
    let malformed_checkpoint =
        postcard::to_stdvec(&malformed_checkpoint).expect("encode malformed checkpoint");
    let malformed_checkpoint_sha256 = Sha256::digest(&malformed_checkpoint).to_vec();
    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET snapshot = $1, checkpoint_sha256 = $2 WHERE vol = $3",
                    &[&malformed_checkpoint, &malformed_checkpoint_sha256, &volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("publish checkpoint that fails during replacement");

    let error = stale
        .sync_metadata()
        .expect_err("malformed inode must abort checkpoint replacement");
    assert!(
        matches!(&error, MetaError::Serialization(_)),
        "malformed checkpoint must be reported as a serialization failure; got: {error:?}"
    );
    assert!(stale.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(stale.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(matches!(
        stale.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    drop(stale);

    let reopened = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("restart must retain the old complete base");
    assert!(reopened.lookup(ROOT_INO, b"old-complete-base").is_ok());
    assert!(reopened.lookup(ROOT_INO, b"unflushed-pending.bin").is_ok());
    assert!(
        reopened.list_inode_attrs().is_ok(),
        "local metadata database must remain structurally valid when rebootstrap aborts"
    );
    assert!(matches!(
        reopened.lookup(ROOT_INO, b"new-complete-base"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert_eq!(
        reopened.local_version.load(Ordering::Acquire),
        0,
        "aborted rebootstrap attempts must not advance the local replay cursor"
    );

    run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET snapshot = $1, checkpoint_sha256 = $2 WHERE vol = $3",
                    &[
                        &valid_checkpoint.snapshot,
                        &valid_checkpoint.sha256.as_slice(),
                        &volume_id,
                    ],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }
    }))
    .expect("restore valid checkpoint");

    reopened
        .sync_metadata()
        .expect("restart should rebootstrap after checkpoint recovery");
    assert_eq!(reopened.local_version.load(Ordering::Acquire), 2);
    assert_eq!(reopened.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    for name in [
        b"old-complete-base".as_slice(),
        b"new-complete-base",
        b"unflushed-pending.bin",
    ] {
        reopened
            .lookup(ROOT_INO, name)
            .expect("recovered checkpoint and replay tail must be complete");
    }
    reopened
        .flush_metadata()
        .expect("recovered client must flush with its preserved local overlay");
    let (remote_version, remote_events) = read_pg_event_batch(&dsn, volume.volume_id, 3);
    assert_eq!(remote_version, 2);
    assert!(
        remote_events.is_empty(),
        "an uncommitted regular-file overlay must remain local-only"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_reaches_live_at_frozen_target_while_remote_head_advances() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _page_limit = set_test_env("STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE", "1");
    let volume = PgTestVolume::new(&dsn, "pg-frozen-startup-target");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let stale = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(stale.local_version.load(Ordering::Acquire), 0);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher metadata client");

    for name in [b"backlog-1".as_slice(), b"backlog-2", b"backlog-3"] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create backlog event");
        publisher.flush_metadata().expect("publish backlog event");
    }

    stale.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    stale.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    let startup_target = stale
        .frozen_sync_target_version()
        .expect("capture frozen startup target");
    assert_eq!(startup_target, 3);

    let active_writer = std::sync::Arc::clone(&publisher);
    let observed_cursors = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_cursors_hook = std::sync::Arc::clone(&observed_cursors);
    stale.set_catch_up_fault_hook(std::sync::Arc::new(move |point| {
        if let CatchUpFaultPoint::BeforePageFetch { cursor } = point {
            observed_cursors_hook
                .lock()
                .expect("observe moving-head catch-up cursors")
                .push(cursor);
            let name = format!("moving-head-{cursor}");
            active_writer
                .create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .map_err(|error| {
                    MetaError::Storage(format!("moving-head publisher failed: {error}"))
                })?;
            active_writer.flush_metadata().map_err(|error| {
                MetaError::Storage(format!("moving-head publisher flush failed: {error}"))
            })?;
        }
        Ok(())
    }));

    let shutdown = std::sync::atomic::AtomicBool::new(false);
    assert!(
        stale
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("drain frozen startup target")
    );
    assert_eq!(stale.local_version.load(Ordering::Acquire), startup_target);
    assert_eq!(stale.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        observed_cursors
            .lock()
            .expect("read moving-head catch-up cursors")
            .as_slice(),
        &[0, 1, 2]
    );
    let remote_head = stale
        .remote_metadata_head_version()
        .expect("read remote head after moving-head catch-up");
    let cursor_count = observed_cursors
        .lock()
        .expect("count moving-head catch-up cursors")
        .len();
    assert_eq!(remote_head, startup_target + cursor_count as i64);

    stale.set_catch_up_fault_hook(std::sync::Arc::new(|_| Ok(())));
    let post_target_event_count = remote_head.saturating_sub(startup_target);
    for _ in 0..post_target_event_count {
        stale
            .sync_metadata()
            .expect("replicate moving-head runtime event");
    }
    assert_eq!(stale.local_version.load(Ordering::Acquire), remote_head);
    for cursor in 0..cursor_count {
        let name = format!("moving-head-{cursor}");
        stale
            .lookup(ROOT_INO, name.as_bytes())
            .expect("runtime replication should apply moving-head events");
    }
    for name in [b"backlog-1".as_slice(), b"backlog-2", b"backlog-3"] {
        stale
            .lookup(ROOT_INO, name)
            .expect("startup catch-up should apply backlog events");
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_catch_up_retry_keeps_frozen_target_while_remote_head_advances() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _page_limit = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1");
    let volume = PgTestVolume::new(&dsn, "pg-frozen-startup-retry-target");

    let stale_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let stale_db = stale_dir.path().join("stale.redb");
    seed_local(&stale_db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &stale_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open stale metadata client");
    assert_eq!(engine.local_version.load(Ordering::Acquire), 0);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open moving-head publisher");

    for name in [
        b"backlog-1".as_slice(),
        b"backlog-2",
        b"backlog-3",
        b"backlog-4",
    ] {
        publisher
            .create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create backlog event");
        publisher.flush_metadata().expect("publish backlog event");
    }

    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
    let startup_target = engine
        .frozen_sync_target_version()
        .expect("capture frozen startup target");
    assert_eq!(startup_target, 4);

    let next_write = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1));
    let observed_cursors = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let injected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let active_writer = std::sync::Arc::clone(&publisher);
    let next_write_hook = std::sync::Arc::clone(&next_write);
    let observed_cursors_hook = std::sync::Arc::clone(&observed_cursors);
    let injected_hook = std::sync::Arc::clone(&injected);
    engine.set_catch_up_fault_hook(std::sync::Arc::new(move |point| {
        if let CatchUpFaultPoint::BeforePageFetch { cursor } = point {
            observed_cursors_hook
                .lock()
                .expect("observe retry catch-up cursors")
                .push(cursor);
            let write_number = next_write_hook.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let name = format!("retry-moving-head-{write_number}");
            active_writer
                .create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .map_err(|error| {
                    MetaError::Storage(format!("moving-head publisher failed: {error}"))
                })?;
            active_writer.flush_metadata().map_err(|error| {
                MetaError::Storage(format!("moving-head publisher flush failed: {error}"))
            })?;
            if cursor == 1 && !injected_hook.swap(true, std::sync::atomic::Ordering::AcqRel) {
                return Err(MetaError::Timeout(
                    "injected temporary catch-up failure".to_string(),
                ));
            }
        }
        Ok(())
    }));
    let shutdown = std::sync::atomic::AtomicBool::new(false);

    let error = engine
        .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
        .expect_err("temporary catch-up failure must interrupt the first session");
    match error {
        MetaError::Timeout(_) => {}
        unexpected => panic!("expected temporary catch-up timeout, got {unexpected:?}"),
    }
    assert_eq!(engine.local_version.load(Ordering::Acquire), 1);
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::CatchingUp
    );
    assert_eq!(
        engine.pg_sync_diagnostics().startup_target_version,
        Some(startup_target as u64)
    );
    assert!(
        engine
            .remote_metadata_head_version()
            .expect("read moving remote head after failure")
            > startup_target
    );

    assert!(
        engine
            .run_startup_catch_up_session(MetadataSyncSource::Manual, &shutdown)
            .expect("retry frozen-target catch-up")
    );
    assert_eq!(engine.local_version.load(Ordering::Acquire), startup_target);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(engine.pg_sync_diagnostics().startup_target_version, None);
    assert_eq!(
        observed_cursors
            .lock()
            .expect("read retry catch-up cursors")
            .as_slice(),
        &[0, 1, 1, 2, 3]
    );

    let remote_head = engine
        .remote_metadata_head_version()
        .expect("read remote head after retry");
    let published_count = (next_write.load(std::sync::atomic::Ordering::Acquire) - 1) as i64;
    assert_eq!(remote_head, startup_target + published_count);
    engine.set_catch_up_fault_hook(std::sync::Arc::new(|_| Ok(())));
    let post_target_event_count = remote_head.saturating_sub(startup_target);
    for _ in 0..post_target_event_count {
        engine
            .sync_metadata()
            .expect("runtime replication should consume post-target event");
    }
    assert_eq!(engine.local_version.load(Ordering::Acquire), remote_head);
    for write_number in 1..next_write.load(std::sync::atomic::Ordering::Acquire) {
        let name = format!("retry-moving-head-{write_number}");
        engine
            .lookup(ROOT_INO, name.as_bytes())
            .expect("runtime replication should apply every post-target event");
    }
    for name in [
        b"backlog-1".as_slice(),
        b"backlog-2",
        b"backlog-3",
        b"backlog-4",
    ] {
        engine
            .lookup(ROOT_INO, name)
            .expect("startup catch-up retry should apply backlog events");
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn fresh_client_empty_volume_reaches_live_at_zero_target() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-fresh-client-empty-volume");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("fresh.redb");
    seed_local(&db, &volume.format_info());

    let fresh = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )
    .expect("fresh empty volume should open with local serving state");

    assert_eq!(fresh.local_version.load(Ordering::Acquire), 0);
    assert_eq!(
        fresh.pg_sync_status().lifecycle,
        PgSyncLifecycle::CatchingUp
    );
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&fresh.local)
            .expect("read missing fresh-client slot"),
        None
    );

    fresh
        .sync_metadata()
        .expect("zero-version startup sync should complete the fresh client");

    assert_eq!(fresh.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(fresh.pg_sync_status().allows_new_mutations());
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read cleared fresh-client marker"),
        0
    );
    assert!(
        PgMetaEngine::load_persisted_client_slot(&fresh.local)
            .expect("read assigned fresh-client slot")
            .is_some()
    );
}

#[test]
fn prepare_for_close_cancels_poll_metadata_sync() {
    for lifecycle in [PgSyncLifecycle::Live, PgSyncLifecycle::CatchingUp] {
        let (_dir, engine) = temp_pg_engine_for_unit();
        engine
            .remote_format_validated
            .store(true, Ordering::Release);
        engine
            .metadata_startup_target_version
            .store(1, Ordering::Release);
        engine
            .pg_sync_lifecycle
            .store(lifecycle as u8, Ordering::Release);
        let engine = Arc::new(engine);
        let weak = Arc::downgrade(&engine);
        let release = tokio_util::sync::CancellationToken::new();
        let release_step = release.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        engine.set_catch_up_fault_hook(Arc::new(move |point| {
            if !matches!(point, CatchUpFaultPoint::BeforePageFetch { .. }) {
                return Ok(());
            }
            let started_tx = started_tx.clone();
            let release_step = release_step.clone();
            let result = weak
                .upgrade()
                .expect("poll owns engine")
                .run_async(async move {
                    started_tx.send(()).expect("signal in-flight remote step");
                    release_step.cancelled().await;
                    Ok(())
                });
            result_tx.send(result.clone()).expect("send remote result");
            result
        }));
        let handle = PgMetaEngine::start_background_poll(
            Arc::downgrade(&engine),
            Duration::from_secs(3600),
            engine.background_workers.poll_shutdown(),
            engine.background_workers.poll_wakeup(),
        )
        .expect("start poll worker");
        engine
            .background_workers
            .install_handle(PgBackgroundWorker::Poll, handle)
            .expect("install poll worker");
        engine.background_workers.signal_poll_wakeup();
        let started = started_rx.recv_timeout(Duration::from_secs(2));
        let closed = engine
            .background_workers
            .shutdown(Duration::from_millis(200));
        // Unblock the old implementation before asserting, so red runs retain no worker.
        release.cancel();
        engine
            .background_workers
            .shutdown(Duration::from_secs(2))
            .expect("join released worker");
        started.expect("poll reaches the remote step");
        let result = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("remote result");
        assert!(
            closed.is_ok(),
            "{lifecycle:?} poll must cancel before close deadline: {closed:?}"
        );
        assert!(
            matches!(result, Err(MetaError::WorkerShutdownCancelled)),
            "{result:?}"
        );
        assert_eq!(engine.metadata_invalidation_diagnostics().sync_errors, 0);
        assert_eq!(
            engine.pg_sync_diagnostics().last_sync_error,
            Some(PgSyncFailure::Cancelled)
        );
        assert!(engine.remote_format_validated.load(Ordering::Acquire));
    }
}

#[test]
fn prepare_for_close_retains_timed_out_worker_for_successful_retry() {
    let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
    let (_dir, engine) = temp_pg_engine_for_unit();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("pg-test-slow-close".into())
        .spawn(move || {
            started_tx.send(()).expect("signal slow worker start");
            release_rx.recv().expect("release slow worker");
            finished_tx.send(()).expect("signal slow worker finish");
        })
        .expect("spawn slow background thread");
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");
    started_rx.recv().expect("wait for slow worker start");

    let started = Instant::now();
    let first_error = engine
        .prepare_for_close()
        .expect_err("slow worker should make close report a timeout");
    let retained_after_timeout = engine
        .background_workers
        .handle_status(PgBackgroundWorker::Poll)
        .0;
    let elapsed = started.elapsed();

    release_tx.send(()).expect("release slow background worker");
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("slow background worker should exit after release");
    let worker_exit_deadline = Instant::now() + Duration::from_secs(1);
    while !engine
        .background_workers
        .handle_status(PgBackgroundWorker::Poll)
        .1
        && Instant::now() < worker_exit_deadline
    {
        std::thread::yield_now();
    }
    let retry_result = engine.prepare_for_close();

    assert!(
        elapsed < Duration::from_secs(1),
        "prepare_for_close should respect the bounded join timeout for slow threads"
    );
    assert!(matches!(first_error, MetaError::Timeout(_)));
    assert!(
        retained_after_timeout,
        "a timed-out worker handle must remain owned for a later close attempt"
    );
    retry_result.expect("close retry should join the exited worker");
    assert!(
        !engine
            .background_workers
            .handle_status(PgBackgroundWorker::Poll)
            .0,
        "successful close should clear the joined worker handle"
    );
}

#[test]
fn prepare_for_close_joins_checkpoint_worker_before_releasing_redb() {
    let (dir, engine) = temp_pg_engine_for_unit();
    let local = Arc::clone(&engine.local);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("pg-test-slow-checkpoint-close".into())
        .spawn(move || {
            started_tx.send(()).expect("signal checkpoint worker start");
            release_rx.recv().expect("release checkpoint worker");
            drop(local);
            finished_tx
                .send(())
                .expect("signal checkpoint worker finish");
        })
        .expect("spawn slow checkpoint worker");
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Checkpoint, handle)
        .expect("install slow checkpoint worker");
    started_rx.recv().expect("wait for checkpoint worker start");

    let engine = Arc::new(engine);
    let closing_engine = Arc::clone(&engine);
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let close_thread = std::thread::spawn(move || {
        let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
        result_tx
            .send(closing_engine.prepare_for_close())
            .expect("send checkpoint close result");
    });

    let first_result = result_rx.recv_timeout(Duration::from_millis(150));
    let returned_within_deadline = first_result.is_ok();
    let retained_after_timeout = first_result
        .as_ref()
        .ok()
        .and_then(|result| result.as_ref().err())
        .is_some()
        && engine
            .background_workers
            .handle_status(PgBackgroundWorker::Checkpoint)
            .0;
    release_tx.send(()).expect("release checkpoint worker");
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("checkpoint worker should exit after release");
    let worker_exit_deadline = Instant::now() + Duration::from_secs(1);
    while !engine
        .background_workers
        .handle_status(PgBackgroundWorker::Checkpoint)
        .1
        && Instant::now() < worker_exit_deadline
    {
        std::thread::yield_now();
    }
    let first_result = match first_result {
        Ok(result) => result,
        Err(_) => result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("checkpoint close should finish after worker release"),
    };
    if first_result.is_err() {
        engine
            .prepare_for_close()
            .expect("checkpoint close retry should join the exited worker");
    }
    close_thread.join().expect("join checkpoint close thread");
    drop(engine);

    let reopened = RedbMetaEngine::open(dir.path().join("meta.redb"));
    assert!(
        returned_within_deadline,
        "checkpoint close should report timeout within its bounded deadline"
    );
    assert!(
        retained_after_timeout,
        "timed-out checkpoint worker must remain owned until a retry joins it"
    );
    assert!(
        reopened.is_ok(),
        "redb should be reopenable after checkpoint worker join"
    );
}

#[test]
fn prepare_for_close_serializes_lazy_worker_registration() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let start_guard = engine
        .lock_worker_start_lock
        .lock()
        .expect("worker start lock");
    let state_guard = engine
        .background_workers
        .state
        .lock()
        .expect("background worker state lock");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let closing_engine = engine.clone();
    let close_thread = std::thread::spawn(move || {
        started_tx.send(()).expect("signal close start");
        result_tx
            .send(closing_engine.prepare_for_close())
            .expect("send close result");
    });
    started_rx.recv().expect("wait for close start");

    let handle = std::thread::spawn(|| {});
    drop(state_guard);

    if result_rx.recv_timeout(Duration::from_millis(250)).is_ok() {
        drop(start_guard);
        close_thread.join().expect("join close thread");
        panic!("close completed before the spawned worker was registered");
    }

    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("register worker before releasing the start lock");
    drop(start_guard);

    result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("close should finish after worker registration")
        .expect("close should join the registered worker");
    close_thread.join().expect("join close thread");
}

#[test]
fn prepare_for_close_obeys_deadline_while_waiting_for_worker_start_lock() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = Arc::new(engine);
    let start_guard = engine
        .lock_worker_start_lock
        .lock()
        .expect("worker start lock");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let closing_engine = Arc::clone(&engine);
    let close_thread = std::thread::spawn(move || {
        let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
        started_tx.send(()).expect("signal close start");
        result_tx
            .send(closing_engine.prepare_for_close())
            .expect("send close result");
    });
    started_rx.recv().expect("wait for close start");

    let result_before_unlock = result_rx.recv_timeout(Duration::from_millis(250));
    let completed_before_unlock = result_before_unlock.is_ok();
    drop(start_guard);
    let result = match result_before_unlock {
        Ok(result) => result,
        Err(_) => result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("close should finish after releasing the start lock"),
    };
    close_thread.join().expect("join close thread");

    assert!(
        completed_before_unlock,
        "prepare_for_close should honor its deadline while waiting for worker startup"
    );
    assert!(
        matches!(result, Err(MetaError::Timeout(_))),
        "start-lock contention should return the bounded close timeout"
    );
    assert!(
        engine
            .background_workers
            .poll_shutdown_ref()
            .load(Ordering::Acquire),
        "timed-out close must still request worker shutdown"
    );
    assert!(
        engine
            .background_workers
            .metadata_notify_cancel()
            .is_cancelled()
    );
    assert!(
        engine
            .background_workers
            .lock_notify_cancel()
            .is_cancelled()
    );
}

#[test]
fn close_timeout_retains_worker_registered_after_stop() {
    let (dir, engine) = temp_pg_engine_for_unit();
    let engine = Arc::new(engine);
    let start_guard = engine
        .lock_worker_start_lock
        .lock()
        .expect("worker start lock");
    let local = Arc::clone(&engine.local);
    let (worker_started_tx, worker_started_rx) = std::sync::mpsc::channel();
    let (worker_release_tx, worker_release_rx) = std::sync::mpsc::channel();
    let (worker_finished_tx, worker_finished_rx) = std::sync::mpsc::channel();
    let incoming_handle = std::thread::spawn(move || {
        let _ = worker_started_tx.send(());
        let _ = worker_release_rx.recv();
        drop(local);
        let _ = worker_finished_tx.send(());
    });
    worker_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("incoming worker should start");

    let (close_result_tx, close_result_rx) = std::sync::mpsc::channel();
    let closing_engine = Arc::clone(&engine);
    let close_thread = std::thread::spawn(move || {
        let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
        let _ = close_result_tx.send(closing_engine.prepare_for_close());
    });
    let close_result_while_locked = close_result_rx.recv_timeout(Duration::from_secs(1));
    let close_completed_while_locked = close_result_while_locked.is_ok();
    let mut incoming_handle = Some(incoming_handle);
    let mut install_result = None;
    let mut manager_owns_incoming_handle = false;
    let mut start_guard = Some(start_guard);
    if close_completed_while_locked {
        install_result = Some(engine.background_workers.install_handle(
            PgBackgroundWorker::MetadataNotify,
            incoming_handle.take().unwrap(),
        ));
        manager_owns_incoming_handle = engine
            .background_workers
            .handle_status(PgBackgroundWorker::MetadataNotify)
            .0;
    }

    let first_close_result = match close_result_while_locked {
        Ok(result) => result,
        Err(_) => {
            drop(start_guard.take());
            close_result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("close should finish after releasing the start lock")
        }
    };
    close_thread.join().expect("join timed out close thread");

    drop(start_guard.take());
    drop(incoming_handle.take());
    let stop_requested = engine
        .background_workers
        .poll_shutdown_ref()
        .load(Ordering::Acquire);
    let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
    let retry_result = engine.prepare_for_close();
    let retry_timed_out = matches!(retry_result, Err(MetaError::Timeout(_)));

    let _ = worker_release_tx.send(());
    worker_finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("incoming worker should release its redb owner");
    let final_close_result = engine.prepare_for_close();
    drop(start_guard);
    drop(engine);
    let reopened = RedbMetaEngine::open(dir.path().join("meta.redb"));

    assert!(
        close_completed_while_locked,
        "close should time out under the held start lock"
    );
    assert!(matches!(first_close_result, Err(MetaError::Timeout(_))));
    assert!(
        stop_requested,
        "timed-out close should request worker shutdown"
    );
    assert!(
        matches!(install_result, Some(Err(MetaError::Storage(_)))),
        "registration after stop should return the closing error"
    );
    assert!(
        manager_owns_incoming_handle,
        "the worker manager should retain the rejected handle until close can join it"
    );
    assert!(
        retry_timed_out,
        "close retry should retain the live worker handle"
    );
    final_close_result.expect("final close should join the released incoming worker");
    assert!(
        reopened.is_ok(),
        "redb should reopen after the incoming worker exits"
    );
}

#[test]
fn shutdown_reaps_worker_from_failed_partial_startup() {
    let (dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let local = Arc::clone(&engine.local);
    let shutdown = engine.background_workers.poll_shutdown();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        release_rx.recv().expect("release slow background worker");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");

    let closing_engine = engine.clone();
    let close_thread = std::thread::spawn(move || {
        closing_engine
            .background_workers
            .shutdown(Duration::from_secs(30))
    });

    let deadline = Instant::now() + Duration::from_secs(1);
    let observed_shutdown_start = loop {
        match engine.background_workers.state.try_lock() {
            Ok(state) if state.shutdown_in_progress => break true,
            Ok(_) | Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("background worker state lock poisoned")
            }
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };

    if !observed_shutdown_start {
        release_tx.send(()).expect("release slow background worker");
        let _ = close_thread.join().expect("join close thread");
        panic!("shutdown should publish its terminal state before joining workers");
    }

    let (late_started_tx, late_started_rx) = std::sync::mpsc::channel();
    let (late_finished_tx, late_finished_rx) = std::sync::mpsc::channel();
    let late_local = Arc::clone(&local);
    let late_handle = std::thread::spawn(move || {
        late_started_tx.send(()).expect("signal late worker start");
        while !shutdown.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        drop(late_local);
        late_finished_tx
            .send(())
            .expect("signal late worker finish");
    });
    late_started_rx.recv().expect("wait for late worker start");
    let install_result = engine
        .background_workers
        .install_handle(PgBackgroundWorker::MetadataNotify, late_handle);
    assert!(
        install_result.is_err(),
        "registration after shutdown must be rejected without waiting for join"
    );

    release_tx.send(()).expect("release slow background worker");
    close_thread
        .join()
        .expect("join close thread")
        .expect("shutdown should join the released worker");
    late_finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("rejected startup worker should exit after shutdown");
    drop(engine);
    drop(local);
    let reopened = RedbMetaEngine::open(dir.path().join("meta.redb"));
    assert!(
        reopened.is_ok(),
        "redb should reopen after the rejected startup worker releases its owner"
    );
}

#[test]
fn successful_shutdown_joins_every_registered_worker_type() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let workers = [
        PgBackgroundWorker::Poll,
        PgBackgroundWorker::Checkpoint,
        PgBackgroundWorker::MetadataNotify,
        PgBackgroundWorker::LockNotify,
        PgBackgroundWorker::LockRenew,
    ];
    for worker in workers {
        engine
            .background_workers
            .install_handle(worker, std::thread::spawn(|| {}))
            .expect("install worker");
    }

    engine
        .background_workers
        .shutdown(Duration::from_secs(1))
        .expect("shutdown should join every worker");

    for worker in workers {
        assert_eq!(
            engine.background_workers.handle_status(worker),
            (false, false),
            "successful close should clear every worker handle"
        );
    }
}

#[test]
fn shutdown_timeout_preserves_unfinished_handle_and_worker_panic_error() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let panic_handle = std::thread::spawn(|| panic!("intentional worker panic"));
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, panic_handle)
        .expect("install panicking worker");

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let blocked_handle = std::thread::spawn(move || {
        started_tx.send(()).expect("signal blocked worker start");
        release_rx.recv().expect("release blocked worker");
        finished_tx.send(()).expect("signal blocked worker finish");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Checkpoint, blocked_handle)
        .expect("install blocked worker");
    started_rx.recv().expect("wait for blocked worker start");

    let panic_exit_deadline = Instant::now() + Duration::from_secs(1);
    while !engine
        .background_workers
        .handle_status(PgBackgroundWorker::Poll)
        .1
        && Instant::now() < panic_exit_deadline
    {
        std::thread::yield_now();
    }
    let panicked_worker_finished = engine
        .background_workers
        .handle_status(PgBackgroundWorker::Poll)
        .1;

    let first_result = engine
        .background_workers
        .shutdown(Duration::from_millis(50));
    let blocked_handle_retained = engine
        .background_workers
        .handle_status(PgBackgroundWorker::Checkpoint)
        .0;
    release_tx.send(()).expect("release blocked worker");
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocked worker should finish after release");
    let worker_exit_deadline = Instant::now() + Duration::from_secs(1);
    while !engine
        .background_workers
        .handle_status(PgBackgroundWorker::Checkpoint)
        .1
        && Instant::now() < worker_exit_deadline
    {
        std::thread::yield_now();
    }
    let retry_result = engine.background_workers.shutdown(Duration::from_secs(1));
    let repeated_result = engine.background_workers.shutdown(Duration::from_secs(1));

    let retry_error = retry_result.expect_err("retry should report the worker panic");
    let repeated_error = repeated_result.expect_err("terminal close should retain the panic");
    assert!(
        panicked_worker_finished,
        "panic worker should finish before close"
    );
    assert!(
        matches!(first_result, Err(MetaError::Timeout(_))),
        "the blocked worker timeout should take precedence over the completed panic"
    );
    assert!(
        blocked_handle_retained,
        "timed-out blocked worker handle must remain owned for retry"
    );
    assert!(
        matches!(&retry_error, MetaError::Storage(_)),
        "retry should preserve the joined worker panic error"
    );
    assert_eq!(retry_error.to_string(), repeated_error.to_string());
}

#[test]
fn concurrent_shutdown_callers_wait_for_terminal_result() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        release_rx.recv().expect("release slow background worker");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");

    let (first_result_tx, first_result_rx) = std::sync::mpsc::channel();
    let first_engine = engine.clone();
    let first = std::thread::spawn(move || {
        first_result_tx
            .send(
                first_engine
                    .background_workers
                    .shutdown(Duration::from_secs(5)),
            )
            .expect("send first shutdown result");
    });

    let deadline = Instant::now() + Duration::from_secs(1);
    let observed_shutdown_start = loop {
        match engine.background_workers.state.try_lock() {
            Ok(state) if state.shutdown_in_progress => break true,
            Ok(_) | Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("background worker state lock poisoned")
            }
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };

    if !observed_shutdown_start {
        release_tx.send(()).expect("release slow background worker");
        first.join().expect("join first shutdown thread");
        panic!("first shutdown should publish its state before joining workers");
    }

    let (second_result_tx, second_result_rx) = std::sync::mpsc::channel();
    let second_engine = engine.clone();
    let second = std::thread::spawn(move || {
        second_result_tx
            .send(
                second_engine
                    .background_workers
                    .shutdown(Duration::from_secs(5)),
            )
            .expect("send second shutdown result");
    });

    let second_early_result = second_result_rx
        .recv_timeout(Duration::from_millis(100))
        .ok();
    let second_returned_early = second_early_result.is_some();
    release_tx.send(()).expect("release slow background worker");
    first_result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first shutdown should finish")
        .expect("first shutdown should succeed");
    if let Some(result) = second_early_result {
        result.expect("second shutdown should succeed");
    } else {
        second_result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second shutdown should finish after the first")
            .expect("second shutdown should replay the terminal result");
    }
    first.join().expect("join first shutdown thread");
    second.join().expect("join second shutdown thread");

    assert!(
        !second_returned_early,
        "a concurrent shutdown caller must wait for the terminal result"
    );
}

#[test]
fn concurrent_shutdown_caller_obeys_its_own_deadline() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = Arc::new(engine);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        release_rx.recv().expect("release slow background worker");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install slow background worker");

    let (first_result_tx, first_result_rx) = std::sync::mpsc::channel();
    let first_engine = Arc::clone(&engine);
    let first = std::thread::spawn(move || {
        first_result_tx
            .send(
                first_engine
                    .background_workers
                    .shutdown(Duration::from_secs(30)),
            )
            .expect("send first shutdown result");
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    let observed_shutdown_start = loop {
        match engine.background_workers.state.try_lock() {
            Ok(state) if state.shutdown_in_progress => break true,
            Ok(_) | Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("background worker state lock poisoned")
            }
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };

    if !observed_shutdown_start {
        release_tx.send(()).expect("release slow background worker");
        first.join().expect("join first shutdown thread");
        panic!("first shutdown should publish its state before joining workers");
    }

    let (second_result_tx, second_result_rx) = std::sync::mpsc::channel();
    let second_engine = Arc::clone(&engine);
    let second = std::thread::spawn(move || {
        let started = Instant::now();
        let result = second_engine
            .background_workers
            .shutdown(Duration::from_millis(50));
        second_result_tx
            .send((started.elapsed(), result))
            .expect("send second shutdown result");
    });
    let second_result_before_release = second_result_rx.recv_timeout(Duration::from_millis(500));
    let completed_before_release = second_result_before_release.is_ok();

    release_tx.send(()).expect("release slow background worker");
    first_result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first shutdown should finish")
        .expect("first shutdown should succeed");
    let (second_elapsed, second_result) = match second_result_before_release {
        Ok(result) => result,
        Err(_) => second_result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second shutdown should finish after the first"),
    };
    first.join().expect("join first shutdown thread");
    second.join().expect("join second shutdown thread");

    assert!(
        completed_before_release,
        "short-deadline caller should return while first shutdown remains blocked"
    );
    assert!(
        second_elapsed < Duration::from_millis(500),
        "second shutdown should respect its own deadline"
    );
    assert!(
        matches!(second_result, Err(MetaError::Timeout(_))),
        "second shutdown should report its own timeout"
    );
}

#[test]
fn metadata_notify_start_skips_start_lock_when_already_registered() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let handle = std::thread::spawn(|| {});
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::MetadataNotify, handle)
        .expect("register metadata notify worker");
    let engine = std::sync::Arc::new(engine);
    let start_guard = engine
        .lock_worker_start_lock
        .lock()
        .expect("worker start lock");
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let calling_engine = engine.clone();
    let caller = std::thread::spawn(move || {
        result_tx
            .send(calling_engine.ensure_metadata_notify_listener_started())
            .expect("send metadata notify start result");
    });

    let early_result = result_rx.recv_timeout(Duration::from_millis(250));
    let completed_without_waiting = early_result.is_ok();
    drop(start_guard);
    let result = match early_result {
        Ok(result) => result,
        Err(_) => result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("metadata notify start should finish after releasing the lock"),
    };
    caller.join().expect("join metadata notify start");

    assert!(
        completed_without_waiting,
        "an already registered metadata notify worker should not wait for the start lock"
    );
    result.expect("already registered metadata notify worker should be a no-op");
}

#[test]
fn postgres_startup_reachability_classification_keeps_timeout_typed() {
    assert!(MetaError::RemoteUnavailable("connection refused".to_string()).is_reachability_error());
    assert!(
        !MetaError::Storage("postgres connect failed: error connecting to server".to_string())
            .is_reachability_error()
    );
    let connection_timeout = PgMetaEngine::pg_timeout_error(
        PgTimeoutContext::Connection("validate postgres schema connect"),
        Duration::from_millis(10),
    );
    assert!(connection_timeout.is_reachability_error());
    assert!(matches!(connection_timeout, MetaError::Timeout(_)));

    let command_timeout = PgMetaEngine::pg_timeout_error(
        PgTimeoutContext::Command("validate postgres schema"),
        Duration::from_millis(10),
    );
    assert!(command_timeout.is_reachability_error());
    assert!(matches!(command_timeout, MetaError::Timeout(_)));
}

#[test]
fn prepare_for_close_is_idempotent_when_no_workers_are_running() {
    let (_dir, engine) = temp_pg_engine_for_unit();

    engine
        .prepare_for_close()
        .expect("first close should succeed without workers");
    engine
        .prepare_for_close()
        .expect("repeated close should remain successful");
}

#[test]
fn dropping_engine_signals_background_workers_without_waiting() {
    let (dir, engine) = temp_pg_engine_for_unit();
    let local = Arc::clone(&engine.local);
    let shutdown = engine.background_workers.poll_shutdown();
    let (observed_tx, observed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        observed_tx.send(()).expect("signal worker stop");
        release_rx.recv().expect("release worker");
        drop(local);
        finished_tx.send(()).expect("signal worker finish");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install worker");

    let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
    let drop_thread = std::thread::spawn(move || {
        drop(engine);
        dropped_tx.send(()).expect("signal engine drop");
    });

    let observed_stop = observed_rx.recv_timeout(Duration::from_secs(1)).is_ok();
    let dropped_without_waiting =
        observed_stop && dropped_rx.recv_timeout(Duration::from_secs(1)).is_ok();
    release_tx.send(()).expect("release background worker");
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("background worker should finish after release");
    drop_thread.join().expect("join engine drop thread");

    assert!(
        observed_stop,
        "background worker should observe drop cancellation"
    );
    assert!(
        dropped_without_waiting,
        "dropping an engine must not wait for a background worker"
    );
    let reopened = RedbMetaEngine::open(dir.path().join("meta.redb"));
    assert!(
        reopened.is_ok(),
        "redb should reopen after the signaled worker releases its owner"
    );
}

#[test]
fn dropping_final_engine_arc_on_worker_thread_does_not_self_join() {
    let (dir, engine) = temp_pg_engine_for_unit();
    let engine = Arc::new(engine);
    let weak = Arc::downgrade(&engine);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker_engine = Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        started_tx.send(()).expect("signal worker start");
        release_rx.recv().expect("release worker");
        drop(worker_engine);
        finished_tx
            .send(())
            .expect("signal worker after releasing final engine Arc");
    });
    engine
        .background_workers
        .install_handle(PgBackgroundWorker::Poll, handle)
        .expect("install worker holding final engine Arc");
    started_rx.recv().expect("wait for worker start");

    drop(engine);
    release_tx.send(()).expect("release worker");
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker should drop its final engine Arc without self-joining");
    let engine_drop_deadline = Instant::now() + Duration::from_secs(1);
    while weak.upgrade().is_some() && Instant::now() < engine_drop_deadline {
        std::thread::yield_now();
    }
    assert!(
        weak.upgrade().is_none(),
        "final engine Arc should be released after the worker exits"
    );

    let reopened = RedbMetaEngine::open(dir.path().join("meta.redb"));
    assert!(
        reopened.is_ok(),
        "redb should reopen after the worker releases the final engine Arc"
    );
}

#[test]
fn open_fails_when_pending_local_event_log_is_corrupt() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "corrupt-local-event-log",
            "corrupt-local-event-log-uuid",
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

    let event_seq = local.get_counter(NEXT_EVENT_SEQ).expect("event start");
    local
        .create_file(ROOT_INO, b"pending-local-file.bin", 0, 0, 0o644)
        .expect("create pending local file");
    local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ, event_seq)
        .expect("persist unflushed event start");
    let txn = local
        .begin_write_txn()
        .expect("begin corruption transaction");
    {
        let event_log = redb::TableDefinition::<u64, &[u8]>::new("event_log");
        let mut events = txn.open_table(event_log).expect("open event log");
        events
            .insert(event_seq, b"invalid-event".as_slice())
            .expect("corrupt pending event");
    }
    txn.commit().expect("commit corrupted event");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    );

    let Err(error) = reopened else {
        panic!("startup must reject an unreadable pending local event");
    };
    assert!(
        matches!(&error, MetaError::Serialization(_)),
        "unsafe pending local state must be reported as a typed decode failure; got: {error:?}"
    );

    let local = RedbMetaEngine::open(&path).expect("reopen local metadata after rejection");
    local
        .lookup(ROOT_INO, b"pending-local-file.bin")
        .expect("unsafe local overlay must remain available for recovery");
    let snapshot = local
        .export_snapshot()
        .expect("export rejected local metadata for recovery");
    assert!(
        snapshot
            .event_log
            .iter()
            .any(|(seq, payload)| *seq == event_seq && payload.as_slice() == b"invalid-event"),
        "rejected pending state must retain the corrupt event-log row for recovery"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ)
            .expect("read pending event cursor"),
        event_seq,
        "rejected pending state must not advance its durable event cursor"
    );
    assert!(
        local
            .get_counter(NEXT_EVENT_SEQ)
            .expect("read event-log end")
            > event_seq,
        "rejected pending state must retain the local event-log suffix"
    );
}

#[test]
fn startup_snapshot_preserves_local_overlay_when_local_overlay_exists() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create local draft");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("local overlay mutation"),
        "local draft namespace should register as a local overlay mutation"
    );
    assert_eq!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 0)
            .expect("startup snapshot decision"),
        Some(super::StartupSnapshotImportMode::PreserveLocalState),
        "startup snapshot must preserve a local overlay after crash/restart"
    );
}

#[test]
fn repair_startup_keeps_recovery_marker_when_remote_sync_fails() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "repair-startup-remote-failure",
            "repair-startup-remote-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set unavailable postgres config");
    local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 1)
        .expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 73)
        .expect("persist stale replay cursor");
    local
        .set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, 71)
        .expect("persist stale namespace replay cursor");
    local
        .set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)
        .expect("set repair recovery marker");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    );
    assert!(
        reopened.is_err(),
        "repair startup must fail rather than starting cleanup from stale local state"
    );

    let local = RedbMetaEngine::open(&db).expect("reopen local metadata");
    assert_eq!(
        local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .expect("read repair recovery marker"),
        1,
        "failed repair startup must leave the recovery marker durable for a later retry"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("read replay cursor"),
        0,
        "repair startup must reset stale replay cursor before attempting remote recovery"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER)
            .expect("read namespace replay cursor"),
        0,
        "repair startup must reset stale namespace replay cursor before attempting remote recovery"
    );
}

#[test]
fn fresh_client_startup_failure_is_degraded_and_marker_survives_reopen() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "fresh-client-startup-failure",
            "fresh-client-startup-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    drop(local);

    let fresh = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )
    .expect("fresh local view should survive unavailable PostgreSQL");
    let status = fresh.pg_sync_status();
    assert_eq!(status.lifecycle, PgSyncLifecycle::Degraded);
    assert!(!status.allows_new_mutations());
    assert!(
        fresh
            .remote_transport_unavailable
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        !fresh
            .remote_identity_validated
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1,
        "failed fresh startup must retain its durable write gate"
    );
    drop(fresh);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen failed fresh client locally");
    assert_eq!(
        reopened.pg_sync_status().lifecycle,
        PgSyncLifecycle::Degraded,
        "ordinary reopen must not mistake an unfinished fresh bootstrap for an established client"
    );
}

#[test]
fn regular_consistency_check_retries_after_startup_remote_transport_failure() {
    let (_dir, mut engine) = temp_pg_engine_for_unit();
    engine
        .remote_transport_unavailable
        .store(true, std::sync::atomic::Ordering::Release);
    engine.command_timeout = Duration::from_millis(1);
    engine.command_pool = Arc::new(
        PgConnectionPool::new(
            "postgres://127.0.0.1:1/studiofs".to_string(),
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
        )
        .expect("non-zero command pool size"),
    );

    let error = engine
        .check_consistency()
        .expect_err("regular consistency checks must retry remote identity probing");
    assert!(!matches!(
        error,
        MetaError::RemoteUnavailable(message)
            if message == "remote PostgreSQL transport is currently unavailable"
    ));
}

#[test]
fn remote_pull_requires_remote_identity_validation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_identity_validated
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .local_version
        .store(1, std::sync::atomic::Ordering::Release);

    let error = engine
        .pull_foreign_events_locked_to_target(None, 0, None)
        .expect_err("remote pulls must validate remote identity first");

    assert!(error.is_reachability_error());
    assert_eq!(
        engine
            .local_version
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "identity-pending remote pull must not advance the local cursor"
    );
}

#[test]
fn allocator_ready_client_stays_read_only_until_identity_is_validated() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_identity_validated
        .store(false, std::sync::atomic::Ordering::Release);
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::TargetReached as u8,
        std::sync::atomic::Ordering::Release,
    );

    engine.refresh_pg_sync_lifecycle_for_allocator();
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::TargetReached,
        "allocator readiness must not bypass the identity gate"
    );

    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.refresh_pg_sync_lifecycle_for_allocator();
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn successful_remote_operation_clears_transport_unavailable() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_transport_unavailable
        .store(true, std::sync::atomic::Ordering::Release);

    engine
        .record_remote_operation_result::<()>(Ok(()))
        .expect("successful remote operation");

    assert!(
        !engine
            .remote_transport_unavailable
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );

    engine
        .record_remote_operation_result::<()>(Err(MetaError::UuidMismatch {
            local: "local-volume".to_string(),
            remote: "remote-volume".to_string(),
        }))
        .expect_err("remote identity mismatch");
    assert!(
        engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );

    engine
        .record_remote_operation_result::<()>(Err(MetaError::RemoteUnavailable(
            "test-postgres-dsn".to_string(),
        )))
        .expect_err("unavailable remote operation");
    assert!(
        !engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[test]
fn replay_gap_downgrades_live_client_and_resets_stale_startup_target() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Live);
    engine
        .metadata_startup_target_version
        .store(60, std::sync::atomic::Ordering::Release);

    let error = MetaError::ReplayGap {
        local_version: 5,
        first_available_version: 51,
        target_version: 60,
    };
    let returned = engine.record_remote_operation_result::<bool>(Err(error));

    assert!(matches!(returned, Err(MetaError::ReplayGap { .. })));
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        -1
    );
}

#[test]
fn metadata_sync_replay_gap_records_degraded_lifecycle() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Live);
    let result = Err(MetaError::ReplayGap {
        local_version: 5,
        first_available_version: 6,
        target_version: 60,
    });

    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &result, false);

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert!(
        engine
            .metadata_source_connected
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[test]
fn metadata_sync_result_does_not_repeat_recorded_remote_failure() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let result = Err(MetaError::RemoteUnavailable(
        "remote unavailable".to_string(),
    ));

    let _ = engine.record_remote_operation_result::<bool>(result.clone());
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &result, true);

    assert!(
        engine
            .remote_format_validated
            .load(std::sync::atomic::Ordering::Acquire),
        "aggregate recording must not invalidate a failure already recorded by the remote operation"
    );
    assert_eq!(engine.pg_sync_diagnostics().sync_errors, 1);
}

#[test]
fn metadata_sync_result_ignores_expected_catch_up_cancellation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let result = Err(MetaError::CatchUpCancelled);

    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &result, true);

    assert_eq!(engine.pg_sync_diagnostics().sync_errors, 0);
}

#[test]
fn local_catch_up_aborts_preserve_remote_validation() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);

    let assert_validation_preserved = || {
        assert!(
            engine
                .remote_format_validated
                .load(std::sync::atomic::Ordering::Acquire),
            "local catch-up abort must preserve remote format validation"
        );
        assert!(
            engine
                .remote_identity_validated
                .load(std::sync::atomic::Ordering::Acquire),
            "local catch-up abort must preserve remote identity validation"
        );
    };

    for result in [
        Err(MetaError::CatchUpCancelled),
        Err(MetaError::CatchUpDeadlineExceeded),
    ] {
        engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &result, false);
        assert_validation_preserved();
    }

    for error in [
        MetaError::CatchUpCancelled,
        MetaError::CatchUpDeadlineExceeded,
    ] {
        assert!(
            engine
                .record_remote_operation_result::<()>(Err(error))
                .is_err()
        );
        assert_validation_preserved();
    }
}

#[test]
fn established_zero_version_startup_failure_is_degraded() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "established-zero-version-startup-failure",
            "established-zero-version-startup-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    drop(local);

    let opened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("established local view should survive unavailable PostgreSQL");

    assert_eq!(opened.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!opened.pg_sync_status().allows_new_mutations());
}

#[test]
fn deferred_mount_startup_uses_trusted_local_state_without_remote_probe() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "deferred-mount-startup",
            "deferred-mount-startup-uuid",
            "test-postgres-dsn",
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
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 1)
        .expect("persist local version");
    drop(local);

    let opened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..Default::default()
        },
    )
    .expect("trusted local metadata should open without PostgreSQL");

    assert_eq!(opened.command_pool.stats().checkouts, 0);
    assert!(!opened.remote_format_validated.load(Ordering::Acquire));
    assert_eq!(
        opened.metadata_remote_head_version.load(Ordering::Acquire),
        -1
    );
    assert!(opened.allocator_runtime_available_for_test());
    assert!(opened.next_inode().is_ok());
}

#[test]
fn fresh_client_startup_without_client_slot_is_degraded_and_marker_survives_reopen() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "fresh-client-startup-without-slot",
            "fresh-client-startup-without-slot-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    drop(local);

    let fresh = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )
    .expect("fresh local view should survive unavailable PostgreSQL without a client slot");
    assert_eq!(fresh.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!fresh.pg_sync_status().allows_new_mutations());
    assert_eq!(
        fresh
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1,
        "failed fresh startup must retain its durable write gate"
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&fresh.local).expect("read client slot"),
        None,
        "unavailable PostgreSQL must not fabricate a client slot"
    );
    drop(fresh);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen failed fresh client locally without a client slot");
    assert_eq!(
        reopened.pg_sync_status().lifecycle,
        PgSyncLifecycle::Degraded,
        "ordinary reopen must retain the unfinished fresh bootstrap state"
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&reopened.local).expect("read client slot"),
        None,
        "reopen must not reserve a slot while PostgreSQL remains unavailable"
    );
}

#[test]
fn degraded_fresh_client_recovers_after_sync_reaches_remote_head() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine.record_pg_sync_failure(&MetaError::ReplayGap {
        local_version: 5,
        first_available_version: 6,
        target_version: 7,
    });
    engine
        .try_complete_catchup_sync()
        .expect("complete degraded fresh-client sync");

    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::Live,
        "allocator-ready recovery should restore write authority"
    );
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
    assert!(!engine.pg_sync_diagnostics().rebootstrap_required);
}

#[test]
fn catching_up_client_reaches_live_after_sync_reaches_remote_head() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete established-client catch-up");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.pg_sync_status().allows_new_mutations());
}

#[test]
fn catchup_completion_uses_frozen_startup_target() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(9, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete catch-up at the frozen startup target");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn catchup_completion_requires_a_new_target_after_recovery() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .try_complete_catchup_sync()
        .expect("complete the first recovery cycle");

    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::Degraded as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(8, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(10, std::sync::atomic::Ordering::Release);
    engine
        .try_complete_catchup_sync()
        .expect("inspect the second recovery cycle");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

#[test]
fn concurrent_catchup_completion_preserves_live_lifecycle() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    let engine = std::sync::Arc::new(engine);

    let first = std::sync::Arc::clone(&engine);
    let second = std::sync::Arc::clone(&engine);
    let first = std::thread::spawn(move || first.try_complete_catchup_sync());
    let second = std::thread::spawn(move || second.try_complete_catchup_sync());
    first
        .join()
        .expect("first catch-up worker panicked")
        .expect("first catch-up");
    second
        .join()
        .expect("second catch-up worker panicked")
        .expect("second catch-up");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn catching_up_fresh_client_completes_slot_and_allocator_bootstrap() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete healthy fresh-client catch-up");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
}

#[test]
fn failed_fresh_allocator_bootstrap_keeps_recovery_marker() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .try_complete_catchup_sync()
        .expect("transient allocator bootstrap failure stays recoverable");

    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1,
        "failed allocator bootstrap must retain the durable recovery marker"
    );
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        7,
        "allocator recovery must retain the frozen target"
    );
}

fn fresh_allocator_recovery_fixture() -> (tempfile::TempDir, PgMetaEngine) {
    let (dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .write_invalid_allocator_state_for_test()
        .expect("persist invalid allocator state");
    engine.clear_allocator_runtime_for_test();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    (dir, engine)
}

#[test]
fn allocator_pending_retries_missing_allocator_authority() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();

    engine
        .try_complete_catchup_sync()
        .expect("first allocator bootstrap attempt");
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );

    engine
        .local
        .replace_allocator_state(
            AllocatorStateV1::new(
                AllocatorRecord::new(11, 0).expect("inode namespace"),
                AllocatorRecord::new(12, 0).expect("payload namespace"),
            )
            .expect("allocator state"),
        )
        .expect("persist recovered allocator state");

    engine
        .try_complete_catchup_sync()
        .expect("retry allocator bootstrap");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
    assert!(engine.next_inode().is_ok());
}

#[test]
fn allocator_pending_recovers_without_startup_target() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 0)
        .expect("clear fresh-client marker for existing client");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::AllocatorPending as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .metadata_startup_target_version
        .store(-1, std::sync::atomic::Ordering::Release);
    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(13, 0).expect("inode namespace"),
        AllocatorRecord::new(14, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || Ok(recovered_state)));

    engine
        .try_complete_catchup_sync()
        .expect("allocator-pending client recovers without a startup target");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.next_inode().is_ok());
}

#[test]
fn transient_allocator_bootstrap_recovers_on_background_retry() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(21, 0).expect("inode namespace"),
        AllocatorRecord::new(22, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    let hook_attempts = std::sync::Arc::clone(&attempts);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        if hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel) == 0 {
            Err(MetaError::RemoteUnavailable(
                "transient allocator bootstrap failure".to_string(),
            ))
        } else {
            Ok(recovered_state)
        }
    }));

    engine
        .try_complete_catchup_sync()
        .expect("first allocator bootstrap attempt");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Acquire), 1);
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );

    let sync_result = Ok(false);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Acquire),
        1,
        "backoff must suppress an immediate duplicate mint attempt"
    );
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        7
    );

    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.retry_allocator_bootstrap_now_for_test();
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);

    assert_eq!(attempts.load(std::sync::atomic::Ordering::Acquire), 2);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
    assert!(engine.next_inode().is_ok());
}

#[test]
fn permanent_allocator_bootstrap_failure_is_not_retried() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_attempts = std::sync::Arc::clone(&attempts);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Err(MetaError::Storage(
            "postgres allocator namespace exhausted".to_string(),
        ))
    }));

    engine
        .try_complete_catchup_sync()
        .expect("permanent allocator failure remains read-only");
    assert_eq!(
        engine.pg_sync_diagnostics().last_sync_error,
        Some(PgSyncFailure::Storage)
    );
    engine
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine
        .remote_format_validated
        .store(true, std::sync::atomic::Ordering::Release);
    engine.retry_allocator_bootstrap_now_for_test();
    let sync_result = Ok(false);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);

    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Acquire),
        1,
        "permanent allocator failures must not mint another namespace pair"
    );
    assert_eq!(
        engine.pg_sync_diagnostics().last_sync_error,
        Some(PgSyncFailure::Storage)
    );
    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
}

#[test]
fn allocator_bootstrap_does_not_hold_retry_lock_during_mint() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let engine = std::sync::Arc::new(engine);
    let hook_engine = std::sync::Arc::downgrade(&engine);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        hook_engine
            .upgrade()
            .expect("allocator engine still alive")
            .retry_allocator_bootstrap_now_for_test();
        Err(MetaError::RemoteUnavailable(
            "transient allocator bootstrap failure".to_string(),
        ))
    }));

    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker_engine = std::sync::Arc::clone(&engine);
    let worker = std::thread::spawn(move || {
        result_tx
            .send(worker_engine.try_complete_catchup_sync())
            .expect("send allocator bootstrap result");
    });
    result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("allocator bootstrap must not deadlock on its retry lock")
        .expect("allocator bootstrap retry remains recoverable");
    worker.join().expect("allocator bootstrap worker panicked");
}

#[test]
fn resolved_local_graph_repair_marker_allows_catchup_completion() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)
        .expect("set local graph recovery marker");
    engine
        .metadata_startup_target_version
        .store(1, std::sync::atomic::Ordering::Release);
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);

    engine
        .try_complete_catchup_sync()
        .expect("repair cannot complete before reaching the target");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .unwrap(),
        1
    );
    engine
        .local_version
        .store(1, std::sync::atomic::Ordering::Release);
    engine
        .try_complete_catchup_sync()
        .expect("complete resolved repair");
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .unwrap(),
        0
    );
    assert_eq!(
        engine
            .metadata_startup_target_version
            .load(std::sync::atomic::Ordering::Acquire),
        -1,
        "a later recovery must capture a new remote head"
    );
}

#[test]
fn allocator_bootstrap_panic_releases_in_flight_state_and_can_retry() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 0)
        .expect("clear fresh-client marker for allocator-only retry");
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_attempts = std::sync::Arc::clone(&attempts);
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(23, 0).expect("inode namespace"),
        AllocatorRecord::new(24, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        if hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel) == 0 {
            panic!("simulated allocator bootstrap panic");
        }
        Ok(recovered_state)
    }));

    let first_attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine
            .try_complete_catchup_sync()
            .expect("first attempt reaches allocator bootstrap");
    }));
    assert!(first_attempt.is_err());
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);

    engine
        .try_complete_catchup_sync()
        .expect("allocator bootstrap can retry after panic");

    assert_eq!(attempts.load(std::sync::atomic::Ordering::Acquire), 2);
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
}

#[test]
fn fresh_client_marker_clear_failure_keeps_engine_read_only() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(25, 0).expect("inode namespace"),
        AllocatorRecord::new(26, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || Ok(recovered_state)));
    engine.set_catch_up_fault_hook(std::sync::Arc::new(|point| {
        if point == CatchUpFaultPoint::BeforeFreshClientMarkerClear {
            return Err(MetaError::Storage("marker clear failed".to_string()));
        }
        Ok(())
    }));

    let error = engine
        .try_complete_catchup_sync()
        .expect_err("marker clear failure must be surfaced");

    assert!(matches!(error, MetaError::Storage(_)));
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read retained fresh-client marker"),
        1
    );
}

#[test]
fn poisoned_allocator_retry_state_preserves_backoff() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    let engine = std::sync::Arc::new(engine);
    let poisoned_engine = std::sync::Arc::clone(&engine);
    std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_engine
                .allocator_bootstrap_retry_state
                .lock()
                .expect("allocator retry state lock");
            panic!("poison allocator retry state");
        }));
    })
    .join()
    .expect("poisoning worker panicked outside catch_unwind");

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_attempts = std::sync::Arc::clone(&attempts);
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(move || {
        hook_attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Err(MetaError::RemoteUnavailable(
            "transient allocator bootstrap failure".to_string(),
        ))
    }));

    engine
        .try_complete_catchup_sync()
        .expect("first allocator bootstrap attempt");
    let sync_result = Ok(false);
    engine.record_metadata_sync_result(MetadataSyncSource::FallbackPoll, &sync_result, false);

    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Acquire),
        1,
        "poisoned retry state must retain the backoff gate"
    );
}

#[test]
fn allocator_bootstrap_retry_delay_is_bounded() {
    let next = PgMetaEngine::next_allocator_bootstrap_retry_delay(Duration::from_secs(1));
    assert!(next >= Duration::from_secs(2));
    assert!(next <= Duration::from_millis(2_500));
    let capped = PgMetaEngine::next_allocator_bootstrap_retry_delay(Duration::from_secs(30));
    assert!(capped >= Duration::from_millis(22_500));
    assert!(capped < Duration::from_secs(30));
}

#[test]
fn poisoned_allocator_runtime_does_not_restore_write_authority() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    let engine = std::sync::Arc::new(engine);
    let poisoned_engine = std::sync::Arc::clone(&engine);
    std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned_engine
                .allocator_runtime
                .lock()
                .expect("allocator runtime lock");
            panic!("poison allocator runtime");
        }));
    })
    .join()
    .expect("poisoning worker panicked outside catch_unwind");

    assert!(engine.bootstrap_allocator_state().is_err());
    let status = engine.pg_sync_status();
    assert_eq!(status.lifecycle, PgSyncLifecycle::AllocatorPending);
    assert!(!status.allows_new_mutations());
    assert!(!status.authority.inode_id_headroom);
    assert!(!status.authority.object_id_headroom);
}

#[test]
fn failed_allocator_bootstrap_survives_reopen_and_recovers() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let (dir, engine) = fresh_allocator_recovery_fixture();
    engine.set_allocator_bootstrap_hook(std::sync::Arc::new(|| {
        Err(MetaError::RemoteUnavailable(
            "transient allocator bootstrap failure".to_string(),
        ))
    }));
    engine
        .try_complete_catchup_sync()
        .expect("failed allocator bootstrap remains recoverable");
    drop(engine);

    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen failed fresh client locally");
    assert_eq!(
        reopened
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker after reopen"),
        1
    );
    assert!(!reopened.pg_sync_status().allows_new_mutations());

    let recovered_state = AllocatorStateV1::new(
        AllocatorRecord::new(31, 0).expect("inode namespace"),
        AllocatorRecord::new(32, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    reopened.set_allocator_bootstrap_hook(std::sync::Arc::new(move || Ok(recovered_state)));
    reopened
        .remote_identity_validated
        .store(true, std::sync::atomic::Ordering::Release);
    reopened
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    reopened
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);
    reopened
        .try_complete_catchup_sync()
        .expect("recover allocator authority after reopen");

    assert_eq!(reopened.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert_eq!(
        reopened
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read cleared fresh-client marker"),
        0
    );
    assert!(reopened.next_inode().is_ok());
}

#[test]
fn snapshot_reachability_bootstrap_skips_after_success() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .snapshot_reachability_bootstrapped
        .store(true, std::sync::atomic::Ordering::Release);

    engine
        .backfill_current_reference_ledger_if_needed()
        .expect("completed reachability bootstrap should not contact PostgreSQL again");
}

#[test]
fn degraded_fresh_client_rehydrates_allocator_runtime_before_restoring_write_authority() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine.clear_allocator_runtime_for_test();
    engine
        .inode_id_headroom_available
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .object_id_headroom_available
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete degraded fresh-client sync");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(engine.allocator_runtime_available_for_test());
}

#[test]
fn degraded_fresh_client_without_client_slot_stays_degraded_at_remote_head() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 0)
        .expect("clear client slot");
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    assert!(
        engine.try_complete_catchup_sync().is_err(),
        "fresh-client recovery must not promote without a reserved client slot"
    );
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&engine.local).expect("read client slot"),
        None
    );
}

#[test]
fn catching_up_fresh_client_slot_failure_becomes_degraded() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 0)
        .expect("clear client slot");
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::CatchingUp as u8,
        std::sync::atomic::Ordering::Release,
    );
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    assert!(engine.try_complete_catchup_sync().is_err());
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
}

#[test]
fn allocator_pending_fresh_client_slot_failure_becomes_degraded() {
    let (_dir, engine) = fresh_allocator_recovery_fixture();
    engine
        .local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 0)
        .expect("clear client slot");
    engine.pg_sync_lifecycle.store(
        PgSyncLifecycle::AllocatorPending as u8,
        std::sync::atomic::Ordering::Release,
    );

    assert!(engine.try_complete_catchup_sync().is_err());
    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
}

#[test]
fn degraded_fresh_client_stays_degraded_until_remote_head_is_reached() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(8, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(8, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("inspect incomplete degraded fresh-client sync");

    assert_eq!(engine.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        1
    );
}

#[test]
fn degraded_fresh_client_recovers_to_allocator_pending_without_write_authority() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    engine
        .local
        .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)
        .expect("set fresh-client marker");
    engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    engine
        .inode_id_headroom_available
        .store(false, std::sync::atomic::Ordering::Release);
    engine
        .local_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_remote_head_version
        .store(7, std::sync::atomic::Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(7, std::sync::atomic::Ordering::Release);

    engine
        .try_complete_catchup_sync()
        .expect("complete degraded fresh-client sync");

    assert_eq!(
        engine.pg_sync_status().lifecycle,
        PgSyncLifecycle::AllocatorPending
    );
    assert!(!engine.pg_sync_status().allows_new_mutations());
    assert_eq!(
        engine
            .local
            .get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
            .expect("read fresh-client marker"),
        0
    );
}

#[test]
fn startup_snapshot_preserves_local_overlay_when_only_acl_changed() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .local
        .create_file(ROOT_INO, b"acl-only.txt", 0, 0, 0o644)
        .expect("create base file");
    let snapshot = pg.local.export_snapshot().expect("export base snapshot");
    pg.local
        .import_snapshot(&snapshot)
        .expect("seed imported base");

    assert!(
        !PgMetaEngine::has_any_local_overlay_mutation(&pg.local)
            .expect("no local overlay mutation before ACL edit")
    );

    pg.local
        .set_acl_rule(file.ino, "alice", AccessLevel::ReadOnly)
        .expect("set local ACL overlay");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("ACL overlay mutation"),
        "ACL-only local changes should require preserve-local-state startup import"
    );
}

#[test]
fn persisted_pg_version_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );
}

#[test]
fn startup_snapshot_is_skipped_for_existing_local_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 42)
            .expect("startup snapshot decision")
            .is_none(),
        "an existing local graph must catch up from its own cursor without importing a checkpoint"
    );
}

#[test]
fn startup_snapshot_replaces_acknowledged_local_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.local
        .create_dir(ROOT_INO, b"acknowledged", 0, 0, 0o755)
        .expect("create local directory");
    let event_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("event end");
    pg.persist_flushed_event_seq(event_end)
        .expect("acknowledge local events");

    assert_eq!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 0)
            .expect("startup snapshot decision"),
        Some(super::StartupSnapshotImportMode::ReplaceLocalState),
        "acknowledged local events must not hide the authoritative remote snapshot"
    );
}

#[test]
fn replacing_local_state_resets_the_in_memory_prune_cursor() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let checkpoint = pg
        .local
        .begin_serving_checkpoint_snapshot()
        .expect("capture replacement checkpoint")
        .export_serving_checkpoint()
        .expect("export replacement checkpoint");
    for name in [b"old-a".as_slice(), b"old-b", b"old-c"] {
        pg.create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create acknowledged local directory");
    }
    let old_cursor = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(old_cursor)
        .expect("acknowledge old local events");

    pg.import_serving_checkpoint_locked(
        StartupSnapshotImportMode::ReplaceLocalState,
        &checkpoint,
        0,
    )
    .expect("replace local serving state");
    pg.create_dir(ROOT_INO, b"new-unflushed", 0, 0, 0o755)
        .expect("create unflushed event after replacement");

    pg.prune_local_event_log_once()
        .expect("prune acknowledged events after replacement");

    assert_eq!(
        pg.local.first_event_seq().unwrap(),
        Some(1),
        "the live cursor must reset with the replaced event log so new events are not pruned"
    );
}

#[test]
fn replacing_local_state_succeeds_when_event_progress_lock_is_poisoned() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let checkpoint = pg
        .local
        .begin_serving_checkpoint_snapshot()
        .expect("capture replacement checkpoint")
        .export_serving_checkpoint()
        .expect("export replacement checkpoint");
    pg.flushed_event_seq.store(99, Ordering::SeqCst);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _progress = pg.local_event_progress.lock().unwrap();
        panic!("poison the local event progress lock");
    }));

    pg.import_serving_checkpoint_locked(
        StartupSnapshotImportMode::ReplaceLocalState,
        &checkpoint,
        0,
    )
    .expect("post-import cursor reset must not fail");

    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 0);
}

#[test]
fn startup_snapshot_replay_starts_from_snapshot_epoch_not_pg_head() {
    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(3, 9),
        3,
        "metadata_state.version is the remote head; serving import must resume from checkpoint_version"
    );
}

#[test]
fn startup_snapshot_replay_caps_snapshot_epoch_to_pg_head() {
    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(12, 9),
        9,
        "a checkpoint cursor must not advance past the advertised PG head"
    );
}

#[test]
fn startup_snapshot_replay_uses_previous_epoch_when_snapshot_has_no_cursor() {
    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(0, 9),
        0,
        "a serving checkpoint always carries an explicit checkpoint version"
    );
}

#[test]
fn bootstrap_checkpoint_descriptor_validates_and_exposes_replay_floor() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();

    descriptor
        .validate()
        .expect("complete bootstrap descriptor should validate");
    assert_eq!(descriptor.checkpoint_version, 3);
    assert_eq!(
        descriptor.remote_head_version, 3,
        "the remote head is separate from the checkpoint replay floor"
    );
}

#[test]
fn bootstrap_checkpoint_descriptor_validate_and_decode_returns_checkpoint() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();

    let checkpoint = descriptor
        .validate_and_decode()
        .expect("complete bootstrap descriptor should decode");

    assert_eq!(checkpoint, crate::snapshot::ServingCheckpoint::default());
}

#[test]
fn bootstrap_checkpoint_descriptor_debug_redacts_format_info() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();
    let debug = format!("{descriptor:?}");

    assert!(debug.contains("format_info_bytes: \"<redacted>\""));
    assert!(!debug.contains("test-postgres-dsn"));
}

fn valid_bootstrap_checkpoint_descriptor() -> crate::BootstrapCheckpointDescriptor {
    let format_info = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "test-postgres-dsn",
        7,
    );
    let checkpoint_bytes = postcard::to_stdvec(&crate::snapshot::ServingCheckpoint::default())
        .expect("encode serving checkpoint");
    let checkpoint_sha256 = Sha256::digest(&checkpoint_bytes).to_vec();

    crate::BootstrapCheckpointDescriptor {
        volume_id: 7,
        volume_uuid: "bootstrap-descriptor-uuid".to_string(),
        format_info_bytes: serde_json::to_vec(&format_info).expect("encode format info"),
        metadata_format_version: CURRENT_FORMAT_VERSION,
        remote_head_version: 3,
        checkpoint_version: 3,
        checkpoint_encoding: None,
        checkpoint_bytes,
        checkpoint_sha256,
        publication_state: "published".to_string(),
        publication_generation: 1,
    }
}

fn assert_descriptor_failure(
    descriptor: crate::BootstrapCheckpointDescriptor,
    expected: CheckpointDescriptorFailure,
) {
    match descriptor.validate() {
        Err(MetaError::InvalidCheckpointDescriptor { reason }) => {
            assert_eq!(reason, expected);
        }
        other => panic!("expected InvalidCheckpointDescriptor({expected:?}), got {other:?}"),
    }
}

#[test]
fn bootstrap_checkpoint_descriptor_rejects_invalid_invariants() {
    let descriptor = valid_bootstrap_checkpoint_descriptor();

    let mut invalid = descriptor.clone();
    invalid.volume_id = 0;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::VolumeIdentity);

    let mut invalid = descriptor.clone();
    invalid.volume_uuid = "  ".to_string();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::VolumeIdentity);

    let mut invalid = descriptor.clone();
    invalid.metadata_format_version = CURRENT_FORMAT_VERSION + 1;
    assert!(matches!(
        invalid.validate(),
        Err(MetaError::UnsupportedFormatVersion { found, expected })
            if found == CURRENT_FORMAT_VERSION + 1 && expected == CURRENT_FORMAT_VERSION
    ));

    let mut invalid = descriptor.clone();
    invalid.checkpoint_version = -1;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Version);

    let mut invalid = descriptor.clone();
    invalid.checkpoint_version = 4;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Version);

    let mut invalid = descriptor.clone();
    invalid.remote_head_version = -1;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Version);

    let mut invalid = descriptor.clone();
    invalid.publication_state = "pending".to_string();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::PublicationState);

    let mut invalid = descriptor.clone();
    invalid.publication_generation = 0;
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Generation);

    let mut invalid = descriptor.clone();
    invalid.checkpoint_sha256 = vec![0; 31];
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Checksum);

    let mut invalid = descriptor.clone();
    invalid.checkpoint_sha256 = vec![0; 32];
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::Checksum);

    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = b"{".to_vec();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mut missing_postgres = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "postgres://unused",
        7,
    );
    missing_postgres.metadata.postgres = None;
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&missing_postgres).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mismatched_uuid = format_info("bootstrap-descriptor", "other-uuid", "postgres://unused", 7);
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&mismatched_uuid).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mut local_format = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "postgres://unused",
        7,
    );
    local_format.metadata.backend = MetadataBackend::RedbLocal;
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&local_format).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mismatched_volume = format_info(
        "bootstrap-descriptor",
        "bootstrap-descriptor-uuid",
        "postgres://unused",
        8,
    );
    let mut invalid = descriptor.clone();
    invalid.format_info_bytes = serde_json::to_vec(&mismatched_volume).expect("encode format info");
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::FormatInfo);

    let mut invalid = descriptor;
    invalid.checkpoint_bytes = b"invalid".to_vec();
    invalid.checkpoint_sha256 = Sha256::digest(&invalid.checkpoint_bytes).to_vec();
    assert_descriptor_failure(invalid, CheckpointDescriptorFailure::CheckpointBytes);
}

#[test]
fn metadata_head_query_contains_checkpoint_version() {
    let sql = super::sql::SELECT_METADATA_HEAD_AND_CHECKPOINT_VERSION;

    assert!(sql.contains("SELECT version, checkpoint_version"));
    assert!(sql.contains("FROM metadata_state"));
    assert!(sql.contains("WHERE vol=$1"));
}

#[test]
fn checkpoint_freshness_sql_uses_persisted_scalar_bookkeeping() {
    let freshness = super::sql::SELECT_CHECKPOINT_FRESHNESS;
    for fragment in [
        "version",
        "checkpoint_version",
        "total_event_payload_bytes",
        "checkpoint_total_event_bytes",
        "checkpoint_updated_at",
        "FROM metadata_state",
        "WHERE vol = $1",
    ] {
        assert!(
            freshness.contains(fragment),
            "freshness query is missing {fragment}"
        );
    }

    let bump = super::sql::BUMP_METADATA_VERSION;
    assert!(bump.contains("total_event_payload_bytes = total_event_payload_bytes + $3"));
    assert!(bump.contains("AND write_requirement <= $4"));
    assert!(bump.contains("RETURNING"));
    assert!(bump.contains("checkpoint_total_event_bytes"));

    let checkpoint = super::sql::UPSERT_METADATA_CHECKPOINT;
    assert!(checkpoint.contains("THEN EXCLUDED.format_info"));
    assert!(checkpoint.contains("ELSE metadata_state.format_info"));
    assert!(
        checkpoint.contains("checkpoint_total_event_bytes = EXCLUDED.checkpoint_total_event_bytes")
    );
    assert!(checkpoint.contains("checkpoint_updated_at = now()"));
    for fragment in [
        "RETURNING",
        "version",
        "checkpoint_version",
        "total_event_payload_bytes",
        "checkpoint_total_event_bytes",
        "checkpoint_age_ms",
        "format_info_present",
        "metadata_format_version",
    ] {
        assert!(
            checkpoint.contains(fragment),
            "checkpoint upsert is missing {fragment}"
        );
    }
    assert!(!checkpoint.contains("EXCLUDED.checkpoint_version >= metadata_state.version"));
}

#[test]
fn fenced_checkpoint_sql_skips_identical_initialized_versions() {
    let checkpoint = super::sql::UPSERT_FENCED_METADATA_CHECKPOINT;

    assert!(checkpoint.contains("EXCLUDED.checkpoint_version > metadata_state.checkpoint_version"));
    assert!(checkpoint.contains("COALESCE(octet_length(metadata_state.format_info), 0) = 0"));
    assert!(
        !checkpoint.contains("EXCLUDED.checkpoint_version >= metadata_state.checkpoint_version")
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_fenced_checkpoint_does_not_republish_identical_initialized_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-idempotent");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");
    let owner = "checkpoint-idempotent-owner";
    let lease = pg
        .try_claim_checkpoint_lease(owner, Duration::from_secs(60))
        .expect("claim checkpoint lease")
        .expect("checkpoint lease should be available");
    let format_version = CURRENT_FORMAT_VERSION as i16;
    let version = 0_i64;
    let checkpoint_bytes = Vec::<u8>::new();
    let checkpoint_sha256 = vec![0_u8; 32];
    let format_info = Vec::<u8>::new();
    let published = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_FENCED_METADATA_CHECKPOINT,
                &[
                    &volume.volume_id,
                    &format_version,
                    &version,
                    &version,
                    &version,
                    &checkpoint_bytes,
                    &checkpoint_sha256,
                    &format_info,
                    &owner,
                    &lease.fencing_token,
                ],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<bool, MetaError>(row.is_some())
    }))
    .expect("attempt identical checkpoint publication");
    assert!(!published);

    let generation = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("read checkpoint generation");
    assert_eq!(generation, 1);
}

#[test]
fn bootstrap_checkpoint_query_contains_all_descriptor_columns() {
    let sql = super::sql::SELECT_BOOTSTRAP_CHECKPOINT_DESCRIPTOR;

    for fragment in [
        "v.vol",
        "v.uuid",
        "ms.metadata_format_version",
        "ms.version",
        "ms.checkpoint_version",
        "ms.snapshot",
        "ms.checkpoint_sha256",
        "ms.format_info",
        "ms.checkpoint_publication_state",
        "ms.checkpoint_generation",
        "FROM volumes AS v",
        "INNER JOIN metadata_state AS ms ON ms.vol = v.vol",
        "WHERE v.vol = $1",
    ] {
        assert!(
            sql.contains(fragment),
            "bootstrap query is missing {fragment}"
        );
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_snapshot_backend_reads_a_validated_bootstrap_descriptor() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-bootstrap-descriptor");
    let backend = PgSnapshotBackend::open(&dsn, volume.volume_id).expect("open snapshot backend");
    let descriptor = backend
        .read_bootstrap_checkpoint()
        .expect("read bootstrap checkpoint descriptor");

    assert_eq!(descriptor.volume_id, volume.volume_id);
    assert_eq!(descriptor.volume_uuid, volume.uuid);
    assert_eq!(descriptor.metadata_format_version, CURRENT_FORMAT_VERSION);
    assert_eq!(descriptor.remote_head_version, 0);
    assert_eq!(descriptor.checkpoint_version, 0);
    assert_eq!(descriptor.publication_state, "published");
    assert_eq!(descriptor.publication_generation, 1);
    assert_eq!(
        descriptor.checkpoint_sha256,
        Sha256::digest(&descriptor.checkpoint_bytes).to_vec()
    );
}

#[test]
#[cfg(feature = "test-successor")]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_encodings_keep_baseline_and_successor_replayable() {
    use crate::snapshot::ServingCheckpoint;

    let base_dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let schema = IsolatedPgSchema::new(&base_dsn);
    let dsn = &schema.dsn;
    let volume = PgTestVolume::new(dsn, "pg-dual-checkpoint");
    let volume_id = volume.volume_id;
    let info = volume.format_info();
    let dir = studiofs_test_support::workspace_target_tempdir("pg-dual-checkpoint-");
    let writer_path = dir.path().join("writer.redb");
    seed_local(&writer_path, &info);
    let writer = retained_test_engine(&writer_path);
    let backend = writer.snapshot_backend();

    writer.create_dir(ROOT_INO, b"first", 0, 0, 0o700).unwrap();
    writer.flush_metadata().unwrap();
    let successor = backend.read_bootstrap_checkpoint().unwrap();
    let first_cut = successor.checkpoint_version;
    assert!(first_cut > 0);
    assert_eq!(
        successor.checkpoint_encoding,
        Some(crate::CheckpointEncoding {
            format: 5,
            schema: 1,
        })
    );

    let baseline_bytes = run_async(with_pg_test_client(dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_version, snapshot, checkpoint_sha256 FROM metadata_state WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .unwrap();
        let baseline_bytes: Vec<u8> = row.get(1);
        assert_eq!(row.get::<_, i64>(0), first_cut);
        assert_eq!(row.get::<_, Vec<u8>>(2), Sha256::digest(&baseline_bytes).as_slice());
        let empty_i16: Vec<i16> = Vec::new();
        let empty_i64: Vec<i64> = Vec::new();
        let old_client_row = client
            .query_one(
                super::sql::SELECT_COMPATIBLE_CHECKPOINT_DESCRIPTOR,
                &[
                    &volume_id,
                    &1_i16,
                    &4_i16,
                    &1_i16,
                    &1_i16,
                    &empty_i16,
                    &empty_i16,
                    &empty_i64,
                ],
            )
            .await
            .unwrap();
        assert_eq!(old_client_row.get::<_, Option<i16>>("checkpoint_format"), None);
        assert_eq!(old_client_row.get::<_, Vec<u8>>("snapshot"), baseline_bytes);
        Ok(baseline_bytes)
    }))
    .unwrap();
    assert_ne!(baseline_bytes, successor.checkpoint_bytes);
    assert!(ServingCheckpoint::decode(&successor.checkpoint_bytes).is_err());
    assert_eq!(
        ServingCheckpoint::decode(&baseline_bytes).unwrap(),
        successor.validate_and_decode().unwrap()
    );

    run_async(with_pg_test_client(dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET format_info = decode('', 'hex') WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .unwrap();
        Ok(())
    }))
    .unwrap();
    writer.write_checkpoint_with_lease().unwrap();
    let repaired = backend.read_bootstrap_checkpoint().unwrap();
    assert_eq!(repaired.checkpoint_bytes, successor.checkpoint_bytes);
    assert_eq!(
        repaired.publication_generation,
        successor.publication_generation
    );

    let corrupt_checksum = vec![0_u8; 32];
    let baseline_sha = Sha256::digest(&baseline_bytes).to_vec();
    let baseline_copy = baseline_bytes.clone();
    let corrupt_copy = corrupt_checksum.clone();
    run_async(with_pg_test_client(dsn, move |client| async move {
        client
            .execute(
                "UPDATE serving_checkpoint_encodings SET checkpoint_sha256 = $2 WHERE vol = $1",
                &[&volume_id, &corrupt_copy],
            )
            .await
            .unwrap();
        // Seed a valid sibling alternate at the same cut while the baseline is
        // pending so selection proves it tries remaining alternates before failing.
        client
            .execute(
                "INSERT INTO serving_checkpoint_encodings (vol, checkpoint_version, checkpoint_format, checkpoint_schema, publication_generation, snapshot, checkpoint_sha256) VALUES ($1, $2, 4, 1, 1, $3, $4)",
                &[&volume_id, &first_cut, &baseline_copy, &baseline_sha],
            )
            .await
            .unwrap();
        client
            .execute(
                "UPDATE metadata_state SET checkpoint_publication_state = 'pending' WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .unwrap();
        Ok(())
    }))
    .unwrap();
    let sibling = backend.read_bootstrap_checkpoint().unwrap();
    assert_eq!(
        sibling.checkpoint_encoding,
        Some(crate::CheckpointEncoding {
            format: 4,
            schema: 1,
        })
    );
    assert_eq!(sibling.checkpoint_bytes, baseline_bytes);

    let corrupt_copy = corrupt_checksum.clone();
    run_async(with_pg_test_client(dsn, move |client| async move {
        client
            .execute(
                "UPDATE serving_checkpoint_encodings SET checkpoint_sha256 = $2 WHERE vol = $1 AND checkpoint_format = 4",
                &[&volume_id, &corrupt_copy],
            )
            .await
            .unwrap();
        Ok(())
    }))
    .unwrap();
    assert!(matches!(
        backend.read_bootstrap_checkpoint(),
        Err(MetaError::NeedCompatibleCheckpoint { replay_floor: 0 })
    ));

    run_async(with_pg_test_client(dsn, move |client| async move {
        client
            .execute(
                "DELETE FROM serving_checkpoint_encodings WHERE vol = $1 AND checkpoint_format = 4",
                &[&volume_id],
            )
            .await
            .unwrap();
        client
            .execute(
                "UPDATE metadata_state SET checkpoint_publication_state = 'published' WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .unwrap();
        Ok(())
    }))
    .unwrap();
    let fallback = backend.read_bootstrap_checkpoint().unwrap();
    assert_eq!(fallback.checkpoint_encoding, None);
    assert_eq!(fallback.checkpoint_bytes, baseline_bytes);
    run_async(with_pg_test_client(dsn, |client| async move {
        client
            .execute(
                "UPDATE serving_checkpoint_encodings SET checkpoint_sha256 = $2 WHERE vol = $1",
                &[&volume_id, &successor.checkpoint_sha256],
            )
            .await
            .unwrap();
        Ok(())
    }))
    .unwrap();

    let returning_path = dir.path().join("returning.redb");
    let returning = RedbMetaEngine::open(&returning_path).unwrap();
    returning
        .initialize_discovered_postgres_checkpoint(&info, &baseline_bytes, first_cut)
        .unwrap();
    drop(returning);
    let returning = retained_test_engine(&returning_path);

    writer.create_dir(ROOT_INO, b"tail", 0, 0, 0o700).unwrap();
    publish_and_pull(&writer);
    let head = writer.remote_metadata_head_version().unwrap();
    assert!(head > first_cut);
    assert_eq!(
        writer
            .run_event_log_prune_once()
            .unwrap()
            .unwrap()
            .durable_floor,
        first_cut
    );

    let fresh_path = dir.path().join("fresh.redb");
    let fresh_cut = backend.read_bootstrap_checkpoint().unwrap();
    assert_eq!(fresh_cut.remote_head_version, head);
    assert_eq!(fresh_cut.checkpoint_version, first_cut);
    let fresh = RedbMetaEngine::open(&fresh_path).unwrap();
    fresh
        .initialize_discovered_postgres_checkpoint(
            &info,
            &fresh_cut.checkpoint_bytes,
            fresh_cut.checkpoint_version,
        )
        .unwrap();
    drop(fresh);
    let fresh = retained_test_engine(&fresh_path);
    sync_all_pages(&fresh);
    assert_eq!(fresh.local_version.load(Ordering::SeqCst), head);
    assert!(fresh.lookup(ROOT_INO, b"first").is_ok());
    assert!(fresh.lookup(ROOT_INO, b"tail").is_ok());

    // Simulate a baseline coordinator using the unchanged fenced writer SQL.
    // It advances the same legacy slot an actual 0.2.5 binary publishes.
    let format_info = serde_json::to_vec(&info).unwrap();
    let payload = writer.local.export_serving_checkpoint_bytes().unwrap();
    let checksum = Sha256::digest(&payload).to_vec();
    let owner = "baseline-checkpoint-writer";
    let lease = writer
        .try_claim_checkpoint_lease(owner, Duration::from_secs(60))
        .unwrap()
        .unwrap();
    run_async(with_pg_test_client(dsn, |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_FENCED_METADATA_CHECKPOINT,
                &[
                    &volume_id,
                    &(CURRENT_FORMAT_VERSION as i16),
                    &head,
                    &head,
                    &0_i64,
                    &payload,
                    &checksum,
                    &format_info,
                    &owner,
                    &lease.fencing_token,
                ],
            )
            .await
            .unwrap();
        assert!(row.is_some());
        Ok(())
    }))
    .unwrap();
    writer.release_checkpoint_lease(owner, lease).unwrap();
    assert_eq!(returning.local_version.load(Ordering::SeqCst), first_cut);
    assert_eq!(
        writer
            .run_event_log_prune_once()
            .unwrap()
            .unwrap()
            .durable_floor,
        head
    );
    let baseline = backend.read_bootstrap_checkpoint().unwrap();
    assert_eq!(baseline.checkpoint_encoding, None);
    assert_eq!(baseline.checkpoint_version, head);

    // This client selected the old checkpoint before pruning won the race
    // against its first replay page. Startup must reselect a retained cut.
    let raced_path = dir.path().join("raced.redb");
    let raced = RedbMetaEngine::open(&raced_path).unwrap();
    raced
        .initialize_discovered_postgres_checkpoint(
            &info,
            &fresh_cut.checkpoint_bytes,
            fresh_cut.checkpoint_version,
        )
        .unwrap();
    drop(raced);
    writer
        .create_dir(ROOT_INO, b"after-prune", 0, 0, 0o700)
        .unwrap();
    publish_and_pull(&writer);
    let raced = retained_test_engine(&raced_path);
    sync_all_pages(&raced);
    sync_all_pages(&returning);
    assert_eq!(
        returning.local_version.load(Ordering::SeqCst),
        writer.remote_metadata_head_version().unwrap()
    );
    for name in [b"first".as_slice(), b"tail", b"after-prune"] {
        assert!(raced.lookup(ROOT_INO, name).is_ok());
        assert!(returning.lookup(ROOT_INO, name).is_ok());
    }

    writer.flush_metadata().unwrap();
    let promoted = backend.read_bootstrap_checkpoint().unwrap();
    let promoted_cut = promoted.checkpoint_version;
    assert!(promoted_cut > head);
    assert_eq!(
        promoted.checkpoint_encoding,
        Some(crate::CheckpointEncoding {
            format: 5,
            schema: 1,
        })
    );
    run_async(with_pg_test_client(dsn, |client| async move {
        let encodings = client
            .query(
                "SELECT checkpoint_version FROM serving_checkpoint_encodings WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .unwrap();
        assert_eq!(encodings.len(), 1);
        assert_eq!(encodings[0].get::<_, i64>(0), promoted_cut);
        Ok(())
    }))
    .unwrap();

    // A missing compatible cut is diagnosed at selection and on returning replay.
    writer
        .create_dir(ROOT_INO, b"uncheckpointed", 0, 0, 0o700)
        .unwrap();
    publish_and_pull(&writer);
    run_async(with_pg_test_client(dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET event_log_prune_floor = version WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .unwrap();
        Ok(())
    }))
    .unwrap();
    assert!(matches!(
        backend.read_bootstrap_checkpoint(),
        Err(MetaError::NeedCompatibleCheckpoint { replay_floor }) if replay_floor > promoted_cut
    ));
    let _guard = returning.replication_lock.lock().unwrap();
    assert!(matches!(
        returning.pull_foreign_events_locked(None),
        Err(MetaError::NeedCompatibleCheckpoint { replay_floor }) if replay_floor > promoted_cut
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn invalid_bootstrap_descriptor_does_not_discard_healthy_pool_connection() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-bootstrap-descriptor-pool");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET checkpoint_publication_state = 'pending' WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("mark checkpoint publication pending");

    let before = engine.command_pool_stats();
    let error = engine.snapshot_backend().read_bootstrap_checkpoint();

    assert!(matches!(
        error,
        Err(MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::PublicationState
        })
    ));
    let after = engine.command_pool_stats();
    assert_eq!(
        after.discarded - before.discarded,
        0,
        "logical descriptor rejection should return a healthy connection to the pool"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_snapshot_skips_stale_checkpoint_without_downloading_payload() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-bootstrap-stale-checkpoint");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");
    engine.local_version.store(1, Ordering::SeqCst);

    let snapshot = b"invalid serving checkpoint".to_vec();
    let checksum = Sha256::digest(&snapshot).to_vec();
    let volume_id = volume.volume_id;
    run_async(with_pg_test_client(&dsn, move |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = $1, checkpoint_version = $2, snapshot = $3, checkpoint_sha256 = $4 WHERE vol = $5",
                &[&2_i64, &0_i64, &snapshot, &checksum, &volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("seed stale checkpoint state");

    assert!(
        !engine
            .pull_snapshot_locked(StartupSnapshotImportMode::PreserveLocalState)
            .expect("stale checkpoint should be skipped")
    );
}

#[test]
fn remote_snapshot_does_not_export_local_pg_state() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
    local
        .write_counters(|counters| {
            counters.set(LOCAL_PG_FLUSHED_EVENT_SEQ, 150)?;
            counters.set(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, 1_000)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, 1_100)?;
            counters.set(LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, 1_200)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER, 2_000)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER, 2_200)?;
            Ok(())
        })
        .expect("seed local pg counters");

    let snapshot = local
        .export_remote_snapshot()
        .expect("export remote snapshot");

    for local_counter in [
        LOCAL_PG_FLUSHED_EVENT_SEQ,
        LOCAL_PG_CLIENT_SLOT_COUNTER,
        LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER,
        LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER,
    ] {
        assert!(
            snapshot
                .counters
                .iter()
                .all(|(name, _)| name != local_counter),
            "remote discovery snapshots must not export local-only PG counter {local_counter}"
        );
    }
}

#[test]
fn import_remote_snapshot_drops_legacy_local_pg_state() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 3).expect("persist reserved local slot");
    local
        .set_counter(
            crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ,
            99,
        )
        .expect("persist local prune floor");

    let mut legacy_snapshot = RedbSnapshot::default();
    legacy_snapshot.counters.extend([
        (LOCAL_PG_VERSION_COUNTER.to_string(), 9),
        (LOCAL_PG_NAMESPACE_VERSION_COUNTER.to_string(), 8),
        (LOCAL_PG_FLUSHED_EVENT_SEQ.to_string(), 150),
        (LOCAL_PG_CLIENT_SLOT_COUNTER.to_string(), 8),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER.to_string(),
            1_000,
        ),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER.to_string(),
            1_100,
        ),
        (
            LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER.to_string(),
            1_200,
        ),
        (
            LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER.to_string(),
            2_000,
        ),
        (LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER.to_string(), 2_100),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER.to_string(),
            2_200,
        ),
    ]);

    local
        .import_remote_snapshot(&legacy_snapshot)
        .expect("import legacy remote snapshot");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("load client slot"),
        None,
        "remote snapshot import must not reuse the checkpoint writer slot"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ)
            .expect("load flushed cursor"),
        0,
        "remote snapshot import must drop legacy local event-log cursor"
    );
    assert_eq!(
        local
            .get_counter(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)
            .expect("load prune floor after remote snapshot import"),
        0,
        "replacing the local event log must reset its old prune floor"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("load applied version"),
        9,
        "remote replay cursor must remain part of the imported data snapshot"
    );
}

#[test]
fn snapshot_reconcile_resets_stale_regular_file_serving_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-stale-serving.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 227, 1);
    let local_payload = PayloadRef::new_partial(91_001, 227);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local payload"),
        "local publish should make the file unprotected"
    );
    let event_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("event end");
    pg.flushed_event_seq.store(event_end, Ordering::SeqCst);

    let mut remote_attr = pg.get_attr(file.ino).expect("load published attr");
    remote_attr.size = 223;
    remote_attr.committed_size = 223;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    remote_attr.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .expect("apply remote base attr");

    assert_eq!(
        pg.get_attr(file.ino).expect("load stale serving attr").size,
        227,
        "base-only import can leave a stale serving attr until reconciliation"
    );

    pg.reconcile_imported_snapshot_regular_files_with_active_open_inodes(None)
        .expect("reconcile imported snapshot");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "snapshot reconciliation must reset stale regular-file serving attrs from base"
    );
}

#[test]
fn snapshot_rebootstrap_holds_active_open_lock_through_import() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.note_active_open_inode(ROOT_INO);

    let (captured_tx, captured_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
    let hook_release_rx = std::sync::Arc::clone(&release_rx);
    pg.set_snapshot_import_after_active_open_capture_hook(std::sync::Arc::new(move || {
        captured_tx.send(()).expect("signal active-open capture");
        hook_release_rx
            .lock()
            .expect("snapshot import hook release lock")
            .recv()
            .expect("release snapshot import hook");
    }));

    let pg = std::sync::Arc::new(pg);
    let worker_pg = std::sync::Arc::clone(&pg);
    let worker = std::thread::spawn(move || {
        worker_pg.import_serving_checkpoint_locked(
            StartupSnapshotImportMode::PreserveLocalState,
            &crate::snapshot::ServingCheckpoint::default(),
            0,
        )
    });

    captured_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("snapshot import should capture active opens");
    let note_pg = std::sync::Arc::clone(&pg);
    let (noted_tx, noted_rx) = std::sync::mpsc::channel();
    let note_worker = std::thread::spawn(move || {
        note_pg.note_active_open_inode(ROOT_INO);
        noted_tx.send(()).expect("signal active-open registration");
    });
    let note_completed_during_import = noted_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_ok();

    release_tx.send(()).expect("release snapshot import");
    worker
        .join()
        .expect("join snapshot import worker")
        .expect("snapshot import");
    note_worker.join().expect("join active-open registration");

    assert!(
        !note_completed_during_import,
        "active-open registration must wait for snapshot replacement"
    );
}

#[test]
fn snapshot_rebootstrap_preserves_active_open_inode_binding() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-open.txt", 0, 0, 0o644)
        .expect("create file");
    let entry = pg
        .lookup(ROOT_INO, b"snapshot-open.txt")
        .expect("lookup file");
    pg.local
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: file.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"snapshot-open.txt".to_vec(),
                entry,
            },
        ])
        .expect("publish file metadata to base");
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");
    pg.note_active_open_inode(file.ino);
    assert!(
        !pg.local
            .local_overlay_attr_inodes()
            .expect("local attr overlays")
            .contains(&file.ino)
    );
    assert!(
        !pg.local
            .local_overlay_inodes()
            .expect("local overlays")
            .contains(&file.ino)
    );

    pg.import_serving_checkpoint_locked(
        StartupSnapshotImportMode::PreserveLocalState,
        &crate::snapshot::ServingCheckpoint::default(),
        0,
    )
    .expect("import serving checkpoint");

    assert!(
        pg.get_attr(file.ino).is_ok(),
        "snapshot rebootstrap must retain metadata for active open handles"
    );
}

#[test]
fn snapshot_rebootstrap_waits_for_pending_mutation_lock() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let pg = std::sync::Arc::new(pg);
    let pending_guard = pg.pending_state.lock().expect("pending state");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker_pg = std::sync::Arc::clone(&pg);

    let worker = std::thread::spawn(move || {
        started_tx
            .send(())
            .expect("signal checkpoint import worker");
        result_tx
            .send(worker_pg.import_serving_checkpoint_locked(
                StartupSnapshotImportMode::PreserveLocalState,
                &crate::snapshot::ServingCheckpoint::default(),
                0,
            ))
            .expect("send checkpoint import result");
    });

    started_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("checkpoint import worker should start");
    assert!(
        result_rx
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "checkpoint import must wait for the pending mutation lock"
    );

    drop(pending_guard);
    result_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("checkpoint import should finish after the lock is released")
        .expect("checkpoint import");
    worker.join().expect("join checkpoint import worker");
}

#[test]
fn persisted_pg_flushed_event_seq_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("initial flushed event seq"),
        0
    );
    pg.persist_flushed_event_seq(19)
        .expect("persist flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 19);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        19
    );
    pg.local
        .set_counter(
            crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ,
            24,
        )
        .expect("persist local prune floor");
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload cursor after pruned history"),
        25,
        "restart must not move the replay cursor behind durably pruned history"
    );
}

#[test]
fn local_position_leaf_persistence_is_monotonic() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");
    pg.persist_local_version(17)
        .expect("ignore older local version");
    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );

    pg.persist_namespace_version(23)
        .expect("persist namespace version");
    pg.persist_namespace_version(11)
        .expect("ignore older namespace version");
    assert_eq!(pg.namespace_version.load(Ordering::SeqCst), 23);
    assert_eq!(
        PgMetaEngine::load_persisted_namespace_version(&pg.local)
            .expect("reload namespace version"),
        23
    );

    pg.persist_flushed_event_seq(25)
        .expect("persist flushed event seq");
    pg.persist_flushed_event_seq(19)
        .expect("ignore older flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 25);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        25
    );
}

#[test]
fn replay_position_does_not_regress_flushed_event_seq() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_flushed_event_seq(25)
        .expect("persist newer flushed event seq");
    pg.persist_replay_position(&RemoteReplayApplyOutcome {
        changed: true,
        max_version: 7,
        max_namespace_version: 3,
        flushed_event_seq: Some(19),
        serving_reconciled: true,
        scratch_reconciliation: Default::default(),
    })
    .expect("persist replay position");

    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 25);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        25,
        "replay position persistence must not move the local flushed cursor backwards"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_flush_advances_checkpoint_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-test");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    create_published_small_file(&pg, ROOT_INO, b"a.txt", 44_010);
    pg.flush_metadata().expect("flush metadata");
    let ok = wait_until(Duration::from_secs(10), || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_opt(
                    "SELECT version FROM metadata_state WHERE vol=$1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok::<bool, MetaError>(row.map(|r| r.get::<_, i64>(0) >= 1).unwrap_or(false))
        }))
        .unwrap_or(false)
    });
    assert!(ok, "expected metadata_state version >= 1");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_freshness_request_tracks_publication_and_reset() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "1");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "9223372036854775807");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "3600");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-freshness");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    let batch_version = pg
        .push_event_log_batch_locked(&[MetaEvent::RemoveAttr { ino: 42 }])
        .expect("publish event batch");
    let stale = pg.checkpoint_freshness_diagnostics();
    assert!(stale.refresh_needed);
    assert_eq!(
        stale.refresh_reason,
        Some(MetadataCheckpointRefreshReason::VersionLag)
    );
    assert_eq!(stale.head_version, Some(batch_version as u64));
    assert!(stale.event_payload_bytes_since_checkpoint > 0);

    pg.local_version.store(batch_version, Ordering::SeqCst);
    pg.write_checkpoint_with_lease()
        .expect("write fresh checkpoint");
    let fresh = pg.checkpoint_freshness_diagnostics();
    assert!(!fresh.refresh_needed);
    assert_eq!(fresh.checkpoint_version, Some(batch_version as u64));
    assert_eq!(fresh.event_payload_bytes_since_checkpoint, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_explicit_flush_defers_checkpoint_while_another_lease_owner_is_active() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-explicit-checkpoint-lease");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");

    create_published_small_file(&pg, ROOT_INO, b"lease-held.txt", 44_011);
    let owner = "other-checkpoint-owner";
    let lease = pg
        .try_claim_checkpoint_lease(owner, Duration::from_secs(60))
        .expect("claim competing checkpoint lease")
        .expect("competing owner should acquire checkpoint lease");
    let before = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation, checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("read checkpoint before explicit flush");

    pg.flush_metadata()
        .expect("explicit flush should defer checkpoint publication");

    let after = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation, checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("read checkpoint after explicit flush");
    assert_eq!(
        after, before,
        "explicit flush must not bypass the active lease"
    );

    pg.release_checkpoint_lease(owner, lease)
        .expect("release competing checkpoint lease");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_lease_takeover_advances_fence_and_rejects_old_owner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-lease");
    let first_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let first_db = first_dir.path().join("meta.redb");
    seed_local(&first_db, &volume.format_info());
    let first = PgMetaEngine::open_with_options(
        &first_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open first pg engine");

    let first_owner = "checkpoint-owner-a";
    let second_owner = "checkpoint-owner-b";
    let first_lease = first
        .try_claim_checkpoint_lease(first_owner, Duration::from_secs(60))
        .expect("claim first checkpoint lease")
        .expect("first owner should acquire checkpoint lease");
    assert_eq!(first_lease.fencing_token, 1);
    assert!(
        first
            .try_claim_checkpoint_lease(second_owner, Duration::from_secs(60))
            .expect("check active checkpoint lease")
            .is_none(),
        "a live checkpoint lease must exclude a peer"
    );

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE bootstrap_checkpoint_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("expire first checkpoint lease");

    let second_lease = first
        .try_claim_checkpoint_lease(second_owner, Duration::from_secs(60))
        .expect("claim takeover checkpoint lease")
        .expect("expired checkpoint lease should be taken over");
    assert!(second_lease.fencing_token > first_lease.fencing_token);

    let format_version = CURRENT_FORMAT_VERSION as i16;
    let version = 0_i64;
    let checkpoint_bytes = Vec::<u8>::new();
    let checkpoint_sha256 = vec![0_u8; 32];
    let format_info = Vec::<u8>::new();
    let old_owner_publish = run_async(with_pg_test_client(&dsn, move |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_FENCED_METADATA_CHECKPOINT,
                &[
                    &volume.volume_id,
                    &format_version,
                    &version,
                    &version,
                    &version,
                    &checkpoint_bytes,
                    &checkpoint_sha256,
                    &format_info,
                    &first_owner,
                    &first_lease.fencing_token,
                ],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<bool, MetaError>(row.is_some())
    }))
    .expect("attempt old-owner checkpoint publication");
    assert!(
        !old_owner_publish,
        "a builder with an expired fencing token must not publish"
    );
}

#[test]
fn checkpoint_source_event_seq_uses_captured_redb_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _replication_lock = pg.replication_lock.lock().expect("replication lock");
    let snapshot = pg
        .local
        .begin_serving_checkpoint_snapshot()
        .expect("capture local read transaction");
    let captured_event_seq = snapshot
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read captured event sequence");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::Acquire),
        captured_event_seq,
        "the initial event sequence should be fully flushed before capture"
    );
    assert!(
        captured_event_seq > 0,
        "the captured event sequence should start from a non-zero flushed baseline"
    );

    <crate::redb_engine::RedbMetaEngine as crate::engine::MetaFsOps>::create_dir(
        pg.local.as_ref(),
        ROOT_INO,
        b"local-write-after-cut",
        0,
        0,
        0o755,
    )
    .expect("write after cut without updating pg pending state");
    let live_event_seq =
        <crate::redb_engine::RedbMetaEngine as crate::engine::MetaInodes>::get_counter(
            pg.local.as_ref(),
            NEXT_EVENT_SEQ,
        )
        .expect("read live event sequence");
    assert!(live_event_seq > captured_event_seq);
    assert_eq!(
        pg.checkpoint_source_event_seq(&snapshot)
            .expect("check captured source position"),
        Some(captured_event_seq)
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_rejects_malformed_dentry_without_replacing_published_descriptor() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "1");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "9223372036854775807");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "3600");

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-malformed-dentry");
    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    let read_descriptor = || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT checkpoint_version, row_to_json(descriptor)::text FROM (SELECT metadata_format_version, version, checkpoint_version, snapshot, checkpoint_sha256, format_info, checkpoint_publication_state, checkpoint_generation, total_event_payload_bytes, checkpoint_total_event_bytes, checkpoint_updated_at::text FROM metadata_state WHERE vol = $1) AS descriptor",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<_, MetaError>((row.get::<_, i64>(0), row.get::<_, String>(1)))
        }))
        .expect("read checkpoint descriptor and covered cut")
    };

    let advance_remote_head = |version: i64| {
        run_async(with_pg_test_client(&dsn, |client| async move {
            client
                .execute(
                    "UPDATE metadata_state SET version = $2, total_event_payload_bytes = $2 WHERE vol = $1",
                    &[&volume.volume_id, &version],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(), MetaError>(())
        }))
    };

    advance_remote_head(10_000).expect("advance remote head for initial checkpoint");
    publisher
        .local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 10_000)
        .expect("persist initial checkpoint version");
    publisher.local_version.store(10_000, Ordering::Release);
    publisher.observe_checkpoint_freshness();

    let owner = publisher.current_client_id().expect("checkpoint owner");
    let initial_lease = publisher
        .try_claim_checkpoint_lease(&owner, Duration::from_secs(60))
        .expect("claim initial checkpoint lease")
        .expect("initial checkpoint lease should be available");
    let initial_cut = publisher
        .capture_checkpoint_source_cut()
        .expect("capture initial checkpoint source cut")
        .expect("initial checkpoint source should be eligible");
    assert!(
        publisher
            .build_and_publish_checkpoint(&owner, initial_lease, initial_cut)
            .expect("publish initial checkpoint")
    );
    publisher
        .release_checkpoint_lease(&owner, initial_lease)
        .expect("release initial checkpoint lease");
    advance_remote_head(20_000).expect("advance remote head beyond published checkpoint");
    let prior_descriptor_and_cut = read_descriptor();
    assert_eq!(prior_descriptor_and_cut.0, 10_000);
    publisher
        .local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 20_000)
        .expect("persist malformed checkpoint version");
    publisher.local_version.store(20_000, Ordering::Release);
    {
        let txn = publisher
            .local
            .begin_write_txn()
            .expect("begin raw dentry write");
        let dentry_table: redb::TableDefinition<&[u8], &[u8]> =
            redb::TableDefinition::new("dentries");
        txn.open_table(dentry_table)
            .expect("open dentry table")
            .insert(
                crate::redb_engine::layout::encode_dentry_key(
                    ROOT_INO,
                    b"malformed-checkpoint-entry",
                )
                .as_slice(),
                b"".as_slice(),
            )
            .expect("insert malformed dentry value");
        txn.commit().expect("commit malformed dentry value");
    }
    publisher.observe_checkpoint_freshness();

    let failing_lease = publisher
        .try_claim_checkpoint_lease(&owner, Duration::from_secs(60))
        .expect("claim failing checkpoint lease")
        .expect("failing checkpoint lease should be available");
    let failing_cut = publisher
        .capture_checkpoint_source_cut()
        .expect("capture malformed checkpoint source cut")
        .expect("malformed checkpoint source should be eligible");
    let error = publisher
        .build_and_publish_checkpoint(&owner, failing_lease, failing_cut)
        .expect_err("malformed dentry must fail checkpoint export");
    assert!(matches!(error, MetaError::Serialization(_)), "{error}");
    publisher
        .release_checkpoint_lease(&owner, failing_lease)
        .expect("release failing checkpoint lease");

    assert!(
        read_descriptor() == prior_descriptor_and_cut,
        "checkpoint descriptor and covered-cut metadata must remain unchanged after failed export"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_publishes_captured_cut_while_local_writes_continue_and_replays_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-captured-cut");
    let (_publisher_dir, publisher) = open_pg_publisher(&volume);
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10000, total_event_payload_bytes = 10000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote head for checkpoint refresh");
    publisher
        .local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 10_000)
        .expect("persist the captured local version");
    publisher.local_version.store(10_000, Ordering::Release);
    publisher.observe_checkpoint_freshness();

    let cut = publisher
        .capture_checkpoint_source_cut()
        .expect("capture checkpoint source cut")
        .expect("checkpoint source should be eligible");
    let owner = publisher.current_client_id().expect("checkpoint owner");
    let lease = publisher
        .try_claim_checkpoint_lease(&owner, Duration::from_secs(60))
        .expect("claim checkpoint lease")
        .expect("checkpoint lease should be available");

    let stop_writer = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop_writer);
    let writer_engine = Arc::clone(&publisher);
    let writer_start = Arc::new(std::sync::Barrier::new(2));
    let writer_start_gate = Arc::clone(&writer_start);
    let (first_write_tx, first_write_rx) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        let first_name = "checkpoint-cut-write-0";
        writer_engine
            .create_dir(ROOT_INO, first_name.as_bytes(), 0, 0, 0o755)
            .expect("write after checkpoint cut");
        first_write_tx
            .send(first_name.to_string())
            .expect("report first writer commit");
        let mut writes = 1;
        writer_start_gate.wait();
        for index in 1..128 {
            if writer_stop.load(Ordering::Acquire) {
                break;
            }
            let name = format!("checkpoint-cut-write-{index}");
            writer_engine
                .create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .expect("write during checkpoint publication");
            writes += 1;
        }
        writes
    });
    let first_write = first_write_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("writer should commit after the captured cut");
    writer_start.wait();

    let replication_guard = publisher
        .replication_lock
        .lock()
        .expect("hold replication lock during checkpoint publication");
    let build_publisher = Arc::clone(&publisher);
    let build_owner = owner.clone();
    let (publish_tx, publish_rx) = std::sync::mpsc::sync_channel(1);
    let build_thread = std::thread::spawn(move || {
        let result = build_publisher.build_and_publish_checkpoint(&build_owner, lease, cut);
        publish_tx
            .send(result)
            .expect("report checkpoint publication result");
    });
    let result_while_replication_locked = publish_rx.recv_timeout(Duration::from_secs(15));
    drop(replication_guard);
    let (publish_result, published_without_replication_lock) = match result_while_replication_locked
    {
        Ok(result) => (result, true),
        Err(_) => (
            publish_rx
                .recv_timeout(Duration::from_secs(15))
                .expect("checkpoint publication should finish after releasing replication lock"),
            false,
        ),
    };
    build_thread.join().expect("join checkpoint publication");
    stop_writer.store(true, Ordering::Release);
    let writer_count = writer.join().expect("join checkpoint writer");
    publisher
        .release_checkpoint_lease(&owner, lease)
        .expect("release checkpoint lease");
    assert!(
        published_without_replication_lock,
        "checkpoint publication must not require replication_lock"
    );
    assert!(
        publish_result.expect("publish captured checkpoint cut"),
        "a local write after capture must not invalidate the checkpoint cut"
    );
    assert!(
        writer_count > 0,
        "writer workload must run during publication"
    );

    let checkpoint = read_pg_checkpoint(&dsn, volume.volume_id);
    let checkpoint_state: crate::snapshot::ServingCheckpoint =
        postcard::from_bytes(&checkpoint.snapshot).expect("decode published checkpoint");
    let concurrent_write_prefix = b"checkpoint-cut-write";
    assert!(
        checkpoint_state.dentries.iter().all(|(key, _)| {
            !key.windows(concurrent_write_prefix.len())
                .any(|window| window == concurrent_write_prefix)
        }),
        "the checkpoint bytes must remain at the captured cut"
    );

    let _recent_flush = {
        let _guard = publisher
            .replication_lock
            .lock()
            .expect("lock local replication for event-tail publication");
        publisher
            .ensure_remote_format_validated()
            .expect("validate remote format before event-tail publication");
        publisher
            .replay_local_upload_ledger()
            .expect("replay local upload ledger before event-tail publication");
        let recent_flush = if let Some(batch) = publisher.claim_pending_batch(true) {
            publisher
                .flush_pending_claimed_batch_with_restore(batch)
                .expect("publish the local event tail after the checkpoint")
        } else {
            None
        };
        publisher
            .pull_and_reconcile_foreign_events_locked(recent_flush.as_ref())
            .expect("reconcile after event-tail publication");
        recent_flush
    };
    let (remote_head, checkpoint_version) =
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT version, checkpoint_version FROM metadata_state WHERE vol = $1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
        }))
        .expect("read checkpoint and replay target");
    assert!(remote_head > checkpoint_version);
    assert_eq!(checkpoint_version, 10_000);

    let cold_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let cold_db = cold_dir.path().join("cold.redb");
    seed_local(&cold_db, &volume.format_info());
    let cold = PgMetaEngine::open_with_options(
        &cold_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open cold client");
    cold.sync_metadata()
        .expect("restore checkpoint and replay the post-cut event tail");
    assert_eq!(cold.local_version.load(Ordering::Acquire), remote_head);
    cold.lookup(ROOT_INO, first_write.as_bytes())
        .expect("cold client should restore the cut and replay the later write");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_background_checkpoint_coordinator_coalesces_two_clients_and_publishes_exact_cut() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-coordinator");
    let first_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let first_db = first_dir.path().join("meta.redb");
    seed_local(&first_db, &volume.format_info());
    let second_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let second_db = second_dir.path().join("meta.redb");
    seed_local(&second_db, &volume.format_info());

    let first = PgMetaEngine::open(&first_db).expect("open first background pg engine");
    let second = PgMetaEngine::open(&second_db).expect("open second background pg engine");

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10000, total_event_payload_bytes = 10_000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote head for checkpoint refresh");
    first.local_version.store(10_000, Ordering::Release);
    second.local_version.store(10_000, Ordering::Release);
    first.observe_checkpoint_freshness();
    second.observe_checkpoint_freshness();

    let published = wait_until(Duration::from_secs(15), || {
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_opt(
                    "SELECT checkpoint_generation FROM metadata_state WHERE vol = $1 AND checkpoint_version = 10000",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<bool, MetaError>(row.is_some_and(|row| row.get::<_, i64>(0) >= 2))
        }))
        .unwrap_or(false)
    });
    assert!(
        published,
        "background coordinator should publish a checkpoint"
    );

    let retry_window = PgMetaEngine::default_checkpoint_retry_interval().max(Duration::from_secs(
        super::super::checkpoint_coordinator::MAX_CHECKPOINT_RETRY_SECS,
    ));
    // Allow a full attempt: freshness read, lease claim, publish, and lease release.
    let checkout_timeout = first.connect_timeout.max(second.connect_timeout);
    let command_timeout = first.command_timeout.max(second.command_timeout);
    let attempt_budget = checkout_timeout
        .saturating_add(command_timeout)
        .saturating_mul(4);
    assert!(
        wait_until(
            retry_window
                .saturating_add(attempt_budget)
                .saturating_add(Duration::from_secs(5)),
            || {
                !first.checkpoint_freshness_diagnostics().refresh_needed
                    && !second.checkpoint_freshness_diagnostics().refresh_needed
            },
        ),
        "both checkpoint clients should observe the published checkpoint"
    );

    first.prepare_for_close().expect("close first pg engine");
    second.prepare_for_close().expect("close second pg engine");

    let (generation, version, checkpoint_version, snapshot, checkpoint_sha256) =
        run_async(with_pg_test_client(&dsn, |client| async move {
            let row = client
                .query_one(
                    "SELECT checkpoint_generation, version, checkpoint_version, snapshot, checkpoint_sha256 FROM metadata_state WHERE vol = $1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            Ok::<(i64, i64, i64, Vec<u8>, Vec<u8>), MetaError>((
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
            ))
        }))
        .expect("read published checkpoint");
    assert_eq!(generation, 2, "coalesced clients must publish once");
    assert_eq!(version, 10_000);
    assert_eq!(checkpoint_version, 10_000);
    assert_eq!(checkpoint_sha256, Sha256::digest(&snapshot).to_vec());
    assert!(!first.checkpoint_freshness_diagnostics().refresh_needed);
    assert!(!second.checkpoint_freshness_diagnostics().refresh_needed);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_coordinator_rechecks_authoritative_freshness_after_coordination_miss() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-coordination-miss");
    let (_dir, engine) = open_pg_publisher(&volume);

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10000, total_event_payload_bytes = 10000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote head for checkpoint freshness");
    engine.local_version.store(10_000, Ordering::Release);
    engine.observe_checkpoint_freshness();
    assert!(
        engine.checkpoint_freshness_diagnostics().refresh_needed,
        "the initial remote head must require a checkpoint refresh"
    );

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET checkpoint_version = 10000, checkpoint_total_event_bytes = 10000, checkpoint_updated_at = now() WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("publish authoritative checkpoint state");

    assert!(
        engine.refresh_checkpoint_freshness_after_coordination_miss(),
        "a coordination miss must report fresh state after the peer publishes"
    );
    assert!(!engine.checkpoint_freshness_diagnostics().refresh_needed);
    engine.prepare_for_close().expect("close pg engine");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_coordinator_skips_unflushed_local_batch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-pending");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    pg.create_dir(ROOT_INO, b"pending-dir", 0, 0, 0o755)
        .expect("create unflushed local directory");
    assert!(
        pg.replication_backlog().pending_ops > 0,
        "local mutation should remain pending before the checkpoint attempt"
    );

    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 10_000, total_event_payload_bytes = 10_000 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote metadata head");
    pg.local_version.store(10_000, Ordering::Release);
    pg.observe_checkpoint_freshness();

    assert!(
        !pg.run_checkpoint_refresh_once(Duration::from_secs(60))
            .expect("checkpoint refresh should defer pending local state"),
        "checkpoint refresh must not publish while local replication is pending"
    );
    {
        let mut pending_state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut pending_state);
    }
    assert!(
        !pg.run_checkpoint_refresh_once(Duration::from_secs(60))
            .expect("checkpoint refresh should defer an unflushed event log"),
        "checkpoint refresh must not publish while the local event log is unflushed"
    );
    let checkpoint_version = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("read checkpoint version");
    assert_eq!(checkpoint_version, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_promotion_preserves_post_capture_event_tail() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-tail");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    let batch_version = pg
        .push_event_log_batch_locked(&[MetaEvent::RemoveAttr { ino: 43 }])
        .expect("publish event batch");
    assert!(batch_version > 0);

    let volume_id = volume.volume_id;
    let format_version = CURRENT_FORMAT_VERSION as i16;
    let version = 0_i64;
    let checkpoint_version = 0_i64;
    let checkpoint_total_event_bytes = 0_i64;
    let snapshot = Vec::<u8>::new();
    let checkpoint_sha256 = vec![0_u8; 32];
    let format_info = Vec::<u8>::new();
    let promoted = run_async(with_pg_test_client(&dsn, move |client| async move {
        let row = client
            .query_opt(
                super::sql::UPSERT_METADATA_CHECKPOINT,
                &[
                    &volume_id,
                    &format_version,
                    &version,
                    &checkpoint_version,
                    &checkpoint_total_event_bytes,
                    &snapshot,
                    &checkpoint_sha256,
                    &format_info,
                ],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<bool, MetaError>(row.is_some())
    }))
    .expect("promote checkpoint after event publication");
    assert!(
        promoted,
        "checkpoint promotion must not starve behind a newer head"
    );

    let state = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT version, checkpoint_version, total_event_payload_bytes, checkpoint_total_event_bytes FROM metadata_state WHERE vol=$1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(i64, i64, i64, i64), MetaError>((
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
        ))
    }))
    .expect("query checkpoint tail state");

    assert_eq!(state.0, batch_version);
    assert_eq!(state.1, 0);
    assert!(state.2 > state.3);
    assert_eq!(state.3, 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_skips_cut_when_freshness_head_lags_local_cursor() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "1");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "9223372036854775807");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "3600");

    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-lagging-freshness");
    let (_dir, pg) = open_pg_publisher(&volume);
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 1, total_event_payload_bytes = 10 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("set freshness head below the local cursor");
    pg.local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 2)
        .expect("persist local cursor ahead of freshness row");
    pg.local_version.store(2, Ordering::Release);
    pg.observe_checkpoint_freshness();

    assert!(
        !pg.run_checkpoint_refresh_once(Duration::from_secs(60))
            .expect("defer a source cut without matching freshness coordinates"),
        "the coordinator must not publish with a byte baseline from an older head"
    );
    let lease_exists = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM bootstrap_checkpoint_leases WHERE vol = $1)",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<bool, MetaError>(row.get(0))
    }))
    .expect("query lease after freshness mismatch");
    assert!(
        !lease_exists,
        "mismatched freshness coordinates must not claim a lease"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_skip_records_remote_stale_observation() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "1");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "9223372036854775807");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "3600");

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-stale-skip");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = 1, total_event_payload_bytes = 10 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote metadata head");

    let refresh_result = pg
        .run_checkpoint_refresh_once(Duration::from_secs(60))
        .expect("skip checkpoint refresh while local state is stale");
    assert!(!refresh_result);
    let lease_exists = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM bootstrap_checkpoint_leases WHERE vol = $1)",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<bool, MetaError>(row.get(0))
    }))
    .expect("query stale checkpoint lease state");
    assert!(
        !lease_exists,
        "stale local state must not claim a checkpoint lease"
    );

    pg.write_checkpoint_with_lease()
        .expect("skip stale local checkpoint write");
    let diagnostics = pg.checkpoint_freshness_diagnostics();
    assert_eq!(diagnostics.head_version, Some(1));
    assert_eq!(diagnostics.checkpoint_version, Some(0));
    assert_eq!(diagnostics.event_payload_bytes_since_checkpoint, 10);
    assert!(diagnostics.refresh_needed);
    assert_eq!(
        diagnostics.refresh_reason,
        Some(MetadataCheckpointRefreshReason::VersionLag)
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_checkpoint_skip_when_remote_checkpoint_covers_local_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-current");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    pg.write_checkpoint_with_lease()
        .expect("skip already-current checkpoint write");

    let checkpoint_generation = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT checkpoint_generation FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<i64, MetaError>(row.get(0))
    }))
    .expect("query checkpoint generation");

    assert_eq!(checkpoint_generation, 1);
    let diagnostics = pg.checkpoint_freshness_diagnostics();
    assert_eq!(diagnostics.head_version, Some(0));
    assert_eq!(diagnostics.checkpoint_version, Some(0));
    assert!(!diagnostics.refresh_needed);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_does_not_move_remote_head_backwards() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-monotonic");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg");
    let newer_version = 10;
    // A checkpoint cut is eligible only when its local version matches the remote head.
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET version = $1 WHERE vol = $2",
                &[&newer_version, &volume.volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("advance remote metadata head");

    pg.local_version.store(newer_version, Ordering::SeqCst);
    pg.write_checkpoint_with_lease()
        .expect("write newer checkpoint");
    pg.local_version.store(5, Ordering::SeqCst);
    pg.write_checkpoint_with_lease()
        .expect("write stale checkpoint");

    let (version, checkpoint_version) = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT version, checkpoint_version FROM metadata_state WHERE vol=$1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("query checkpoint state");

    assert_eq!(
        (version, checkpoint_version),
        (newer_version, newer_version),
        "the first checkpoint should publish at the remote head and the stale write should be skipped"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_refuses_to_promote_legacy_format() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-legacy-format");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let legacy_format = (CURRENT_FORMAT_VERSION - 1) as i16;
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET metadata_format_version = $1 WHERE vol = $2",
                &[&legacy_format, &volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("restore legacy metadata format");

    let error = pg
        .write_checkpoint_with_lease()
        .expect_err("checkpoint upsert must reject a legacy metadata row");
    assert!(
        matches!(
            error,
            MetaError::NeedsMigration { found } if found == legacy_format as u16
        ),
        "unexpected checkpoint format error: {error}"
    );

    let actual = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<i16, MetaError>(row.get(0))
    }))
    .expect("read legacy metadata format");
    assert_eq!(actual, legacy_format, "legacy format must not be promoted");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_event_publication_refuses_legacy_format() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-event-legacy-format");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let legacy_format = (CURRENT_FORMAT_VERSION - 1) as i16;
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET metadata_format_version = $1 WHERE vol = $2",
                &[&legacy_format, &volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("restore legacy metadata format");

    let error = pg
        .push_event_log_batch_locked(&[MetaEvent::SetAclRule {
            ino: ROOT_INO,
            principal: "alice".to_string(),
            access_level: AccessLevel::ReadOnly,
        }])
        .expect_err("event publication must reject a legacy metadata row");
    assert!(
        matches!(
            error,
            MetaError::NeedsMigration { found } if found == legacy_format as u16
        ),
        "unexpected event publication format error: {error}"
    );

    let (version, event_count) = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT version, (SELECT count(*) FROM event_log WHERE vol = $1) FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(i64, i64), MetaError>((row.get(0), row.get(1)))
    }))
    .expect("read legacy publication state");
    assert_eq!(
        version, 0,
        "legacy publication must not bump remote version"
    );
    assert_eq!(event_count, 0, "legacy publication must not insert events");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_repairs_missing_remote_format_info() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-format-info");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    run_async(with_pg_test_client(&dsn, |client| async move {
        client
            .execute(
                "UPDATE metadata_state SET format_info = decode('', 'hex') WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<(), MetaError>(())
    }))
    .expect("clear remote format info");

    pg.write_checkpoint_with_lease()
        .expect("write checkpoint with format info");

    let actual = run_async(with_pg_test_client(&dsn, |client| async move {
        let row = client
            .query_one(
                "SELECT format_info FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok::<Vec<u8>, MetaError>(row.get(0))
    }))
    .expect("read repaired format info");

    assert_eq!(
        actual,
        serde_json::to_vec(&volume.format_info()).expect("serialize expected format info")
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_capacity_is_bounded_by_configured_pool_size() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _pool_env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-capacity");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 1,
            defer_remote_startup_probes: false,
        },
    )
    .expect("open pg engine with one command-pool slot");

    let before = engine.command_pool_stats();
    let pool = engine.command_pool.clone();
    let command_timeout = Duration::from_secs(5);
    let (first_result, second_result, observed_wait) = engine
        .run_async(async move {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();

            let first_pool = pool.clone();
            let first = tokio::spawn(async move {
                PgMetaEngine::with_postgres_client_timeout(
                    first_pool,
                    command_timeout,
                    "pool capacity first command",
                    move |_client| {
                        Box::pin(async move {
                            started_tx.send(()).expect("signal first command checkout");
                            release_rx.await.expect("release first command checkout");
                            Ok::<(), MetaError>(())
                        })
                    },
                )
                .await
            });

            started_rx
                .await
                .expect("first command should acquire a pool slot");

            let second_pool = pool.clone();
            let second = tokio::spawn(async move {
                PgMetaEngine::with_postgres_client_timeout(
                    second_pool,
                    command_timeout,
                    "pool capacity second command",
                    |_client| Box::pin(async { Ok::<(), MetaError>(()) }),
                )
                .await
            });

            let observed_wait = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if pool.stats().checkout_waits > before.checkout_waits {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();

            release_tx.send(()).expect("release first command checkout");
            let first_result = first.await.expect("join first command");
            let second_result = second.await.expect("join second command");
            Ok((first_result, second_result, observed_wait))
        })
        .expect("run pool capacity test commands");

    assert!(first_result.is_ok(), "first command should complete");
    assert!(second_result.is_ok(), "second command should complete");
    assert!(
        observed_wait,
        "second command should wait for the sole pool slot"
    );

    let after = engine.command_pool_stats();
    assert_eq!(
        after.max_size, 1,
        "configured command-pool size should be preserved"
    );
    assert_eq!(
        after.total, 1,
        "one-slot pool should retain one reusable connection"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_reuses_connection_for_repeated_remote_metadata_queries() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _pool_env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-reuse");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            fresh_client: false,
            pg_pool_size: 2,
            defer_remote_startup_probes: false,
        },
    )
    .expect("open pg engine");

    let before = engine.command_pool_stats();
    assert_eq!(
        before.max_size, 2,
        "explicit command-pool size should be applied to the opened engine"
    );
    engine
        .remote_metadata_head_version()
        .expect("first metadata head query");
    engine
        .remote_metadata_head_version()
        .expect("second metadata head query");
    let after = engine.command_pool_stats();

    assert!(
        after.connection_creations - before.connection_creations <= 1,
        "repeated command-path queries should reuse one pooled PostgreSQL connection"
    );
    assert_eq!(
        after.checkouts - before.checkouts,
        2,
        "both metadata head queries should go through the command pool"
    );
    assert_eq!(
        after.discarded - before.discarded,
        0,
        "successful repeated queries should keep the pooled connection healthy"
    );
    assert_eq!(
        after.checkout_waits - before.checkout_waits,
        0,
        "serial repeated queries should not wait for a saturated command pool"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_discards_client_after_application_error() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-app-error");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    let before = engine.command_pool_stats();
    let command_pool = engine.command_pool.clone();
    let command_timeout = engine.command_timeout;
    let bad_query = engine.run_async(async move {
        PgMetaEngine::with_postgres_client_timeout(
            command_pool,
            command_timeout,
            "intentional application error",
            move |client| {
                Box::pin(async move {
                    client
                        .batch_execute("SELECT * FROM studiofs_missing_pool_test_table")
                        .await
                        .map_err(|e| {
                            PgMetaEngine::classify_pg_error("intentional application error", e)
                        })?;
                    Ok(())
                })
            },
        )
        .await
    });
    assert!(bad_query.is_err(), "bad query should fail");

    engine
        .remote_metadata_head_version()
        .expect("metadata head query after application error");
    let after = engine.command_pool_stats();

    assert_eq!(
        after.discarded - before.discarded,
        1,
        "application errors should discard the pooled client to avoid stale transaction state"
    );
    assert_eq!(
        after.connection_creations - before.connection_creations,
        1,
        "the next command should reconnect after the failed client is discarded"
    );
}

#[test]
#[ignore = "requires PostgreSQL; restore must not republish ancestor authority"]
fn retained_restore_does_not_republish_ancestor_authority() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "restore-ancestor-authority");
    let dir = studiofs_test_support::workspace_target_tempdir("restore-ancestor-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());
    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let ancestor = a.create_dir(ROOT_INO, b"ancestor", 0, 0, 0o755).unwrap();
    let dest = a.create_dir(ancestor.ino, b"dest", 0, 0, 0o755).unwrap();
    let source = a.create_dir(dest.ino, b"source", 0, 0, 0o755).unwrap();
    a.set_acl_rule(ancestor.ino, "alice", AccessLevel::ReadWrite)
        .unwrap();
    a.set_acl_rule(source.ino, "alice", AccessLevel::ReadOnly)
        .unwrap();
    let private = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(dest.ino, b"source", private.ino, receipt)
        .unwrap();
    publish_and_pull(&a);
    let b = retained_test_engine(&b_path);
    b.sync_metadata().unwrap();
    a.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
    a.restore_retained_entry(
        receipt,
        dest.ino,
        b"restored",
        b"stage",
        &retained_test_source_fences(&a, receipt),
    )
    .unwrap();
    b.set_acl_rule(ancestor.ino, "alice", AccessLevel::ReadOnly)
        .unwrap();
    let mut changed = b.get_attr(ancestor.ino).unwrap();
    changed.mode = 0o500;
    b.set_attr(&changed).unwrap();
    b.rename(ROOT_INO, b"ancestor", ROOT_INO, b"moved").unwrap();
    publish_and_pull(&b);
    a.sync_metadata().unwrap();
    assert_eq!(
        a.get_acl_rule(ancestor.ino, "alice").unwrap(),
        Some(AccessLevel::ReadOnly)
    );
    assert_eq!(a.get_attr(ancestor.ino).unwrap().mode, 0o500);
    assert!(a.lookup(ROOT_INO, b"ancestor").is_err());
    publish_and_pull(&a);
    b.sync_metadata().unwrap();
    assert!(b.lookup(ROOT_INO, b"ancestor").is_err());
    assert_eq!(b.lookup(ROOT_INO, b"moved").unwrap().ino, ancestor.ino);
    assert_eq!(
        b.get_acl_rule(ancestor.ino, "alice").unwrap(),
        Some(AccessLevel::ReadOnly)
    );
    assert_eq!(b.get_attr(ancestor.ino).unwrap().mode, 0o500);
    assert_eq!(b.lookup(dest.ino, b"restored").unwrap().ino, source.ino);
    assert_eq!(
        b.get_acl_rule(source.ino, "alice").unwrap(),
        Some(AccessLevel::ReadOnly)
    );
}

#[test]
#[ignore = "requires PostgreSQL; rename and restore must not clobber concurrent parent attrs or ACLs"]
fn rename_and_restore_do_not_republish_stale_parent_attrs_or_acl() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "rename-restore-parent-acl");
    let dir = studiofs_test_support::workspace_target_tempdir("rename-parent-acl-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());
    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let src_dir = a.create_dir(ROOT_INO, b"src_dir", 0, 0, 0o755).unwrap();
    let moved_dir = a
        .create_dir(src_dir.ino, b"moved_dir", 0, 0, 0o755)
        .unwrap();
    let dest = a.create_dir(ROOT_INO, b"dest", 0, 0, 0o755).unwrap();
    let source = a.create_dir(dest.ino, b"source", 0, 0, 0o755).unwrap();
    a.set_acl_rule(src_dir.ino, "alice", AccessLevel::ReadWrite)
        .unwrap();
    a.set_acl_rule(dest.ino, "alice", AccessLevel::ReadWrite)
        .unwrap();
    let private = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(dest.ino, b"source", private.ino, receipt)
        .unwrap();
    publish_and_pull(&a);
    let b = retained_test_engine(&b_path);
    b.sync_metadata().unwrap();

    // Peer A performs an ordinary cross-parent directory rename AND a retained directory restore locally.
    a.rename(src_dir.ino, b"moved_dir", dest.ino, b"moved_dir")
        .unwrap();
    a.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
    a.restore_retained_entry(
        receipt,
        dest.ino,
        b"restored",
        b"stage",
        &retained_test_source_fences(&a, receipt),
    )
    .unwrap();

    // Concurrently Peer B updates ACL rules and attrs (mode/uid/gid/win_attrs) on both parents and publishes.
    for parent_ino in [src_dir.ino, dest.ino] {
        b.set_acl_rule(parent_ino, "alice", AccessLevel::ReadOnly)
            .unwrap();
        let mut attr = b.get_attr(parent_ino).unwrap();
        attr.mode = 0o500;
        attr.uid = 2001;
        attr.gid = 2002;
        attr.win_attrs = 4;
        b.set_attr(&attr).unwrap();
    }
    publish_and_pull(&b);

    // Peer A pulls B's authority changes while holding its pending rename + restore.
    a.sync_metadata().unwrap();
    for (label, ino, expected_nlink) in [
        ("src_dir (after A sync)", src_dir.ino, 2),
        ("dest (after A sync)", dest.ino, 4),
    ] {
        assert_eq!(
            a.get_acl_rule(ino, "alice").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{label} must preserve concurrent ACL rule"
        );
        let attr = a.get_attr(ino).unwrap();
        assert_eq!(attr.mode, 0o500, "{label} mode");
        assert_eq!(attr.uid, 2001, "{label} uid");
        assert_eq!(attr.gid, 2002, "{label} gid");
        assert_eq!(attr.win_attrs, 4, "{label} win_attrs");
        assert_eq!(attr.nlink, expected_nlink, "{label} nlink");
    }

    // Peer A publishes its rename + restore and both peers converge.
    publish_and_pull(&a);
    b.sync_metadata().unwrap();
    for (peer_name, engine) in [("a", &a), ("b", &b)] {
        for (parent_name, ino, expected_nlink) in
            [("src_dir", src_dir.ino, 2), ("dest", dest.ino, 4)]
        {
            assert_eq!(
                engine.get_acl_rule(ino, "alice").unwrap(),
                Some(AccessLevel::ReadOnly),
                "{peer_name}/{parent_name} must preserve ACL rule after publish"
            );
            let attr = engine.get_attr(ino).unwrap();
            assert_eq!(attr.mode, 0o500, "{peer_name}/{parent_name} mode");
            assert_eq!(attr.uid, 2001, "{peer_name}/{parent_name} uid");
            assert_eq!(attr.gid, 2002, "{peer_name}/{parent_name} gid");
            assert_eq!(attr.win_attrs, 4, "{peer_name}/{parent_name} win_attrs");
            assert_eq!(
                attr.nlink, expected_nlink,
                "{peer_name}/{parent_name} nlink"
            );
        }
        assert_eq!(
            engine.lookup(dest.ino, b"moved_dir").unwrap().ino,
            moved_dir.ino
        );
        assert_eq!(
            engine.lookup(dest.ino, b"restored").unwrap().ino,
            source.ino
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; competing restores must not create directory aliases"]
fn retained_competing_restores_keep_only_winner_destination_after_restart() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-two-restores");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-two-restores-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());
    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let source = a.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let child = a.create_dir(source.ino, b"child", 0, 0, 0o700).unwrap();
    let file = a.create_file(source.ino, b"file", 0, 0, 0o600).unwrap();
    let mut ready = file.clone();
    ready.content_epoch = 1;
    a.set_attr(&ready).unwrap();
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );
    a.link(file.ino, ROOT_INO, b"outside").unwrap();
    let winner_parent = a
        .create_dir(ROOT_INO, b"winner-parent", 0, 0, 0o700)
        .unwrap();
    let loser_parent = a
        .create_dir(ROOT_INO, b"loser-parent", 0, 0, 0o700)
        .unwrap();
    let store = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&a);
    let b = retained_test_engine(&b_path);
    b.sync_metadata().unwrap();
    for (engine, parent, name) in [
        (&a, winner_parent.ino, b"winner".as_slice()),
        (&b, loser_parent.ino, b"loser".as_slice()),
    ] {
        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        engine.create_dir(stage.ino, b"child", 0, 0, 0o700).unwrap();
        engine.create_file(stage.ino, b"file", 0, 0, 0o600).unwrap();
        let restored = engine
            .restore_retained_entry(
                receipt,
                parent,
                name,
                b"stage",
                &retained_test_source_fences(engine, receipt),
            )
            .unwrap();
        for (ino, epoch) in restored.upload_epochs {
            assert!(
                engine
                    .publish_regular_file_overlay_to_base_if_matches(ino, epoch)
                    .unwrap()
            );
        }
    }
    // A real hardlink accepted after the restore transaction must remain publishable.
    b.link(file.ino, ROOT_INO, b"later-link").unwrap();
    assert_eq!(b.get_attr(file.ino).unwrap().nlink, 3, "accepted link");
    publish_and_pull(&a);
    b.request_eager_flush();
    assert!(b.flush_due_pending().is_err());
    b.sync_metadata().unwrap();
    assert_eq!(
        b.get_attr(file.ino).unwrap().nlink,
        3,
        "after winner foreign apply"
    );
    drop(b);
    let b = reopen_pg_engine_with_retry(&b_path);
    assert_eq!(b.get_attr(file.ino).unwrap().nlink, 3, "after reopen");
    publish_and_pull(&b);
    assert_eq!(
        b.get_attr(file.ino).unwrap().nlink,
        3,
        "after self publication"
    );
    assert_eq!(
        b.replication_backlog().pending_ops,
        0,
        "loser remainder completely published"
    );
    a.sync_metadata().unwrap();
    for engine in [&a, &b] {
        assert!(
            engine.lookup(loser_parent.ino, b"loser").is_err(),
            "losing restore left a directory alias"
        );
        assert_eq!(
            engine.lookup(winner_parent.ino, b"winner").unwrap().ino,
            source.ino
        );
        assert_eq!(engine.lookup(source.ino, b"child").unwrap().ino, child.ino);
        assert_eq!(
            engine.get_attr(source.ino).unwrap().parent,
            winner_parent.ino
        );
        assert_eq!(engine.lookup(ROOT_INO, b"outside").unwrap().ino, file.ino);
        assert_eq!(
            engine.lookup(ROOT_INO, b"later-link").unwrap().ino,
            file.ino
        );
        assert_eq!(
            engine.get_attr(file.ino).unwrap().nlink,
            3,
            "converged peer hardlink count"
        );
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 2);
    }
    assert_eq!(b.replication_backlog().pending_ops, 0);
}

fn stage_exact_small_payload(engine: &PgMetaEngine, ino: Ino, object_id: u64, size: u32) {
    use crate::schema::{ChunkId, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord};
    set_file_state(engine, ino, u64::from(size), 1);
    let mut slice = SliceRecord::new(
        SliceId(0),
        SliceOrder(0),
        SliceRange::new(0, size).expect("slice range"),
        PayloadRef::new_partial(object_id, size),
    );
    slice.payload_state = PayloadRefState::LocalOnly;
    engine
        .flush_regular_file_overlay_slices(ino, &[(ChunkId(0), vec![slice])])
        .expect("stage exact payload slice");
}

#[test]
#[ignore = "requires PostgreSQL; split restore batches must self-ack across pages and restart"]
fn retained_restore_split_batches_self_ack_and_restart_convergence() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};
    use std::sync::atomic::AtomicUsize;

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let _byte_limit = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "640");
    let volume = PgTestVolume::new(&dsn, "retained-split-restart");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-split-restart-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());

    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let dest = a.create_dir(ROOT_INO, b"dest", 0, 0, 0o755).unwrap();
    let source = a.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
    let child = a.create_dir(source.ino, b"child", 0, 0, 0o755).unwrap();
    let file = create_published_small_file(&a, source.ino, b"file", 91_001);
    a.link(file.ino, ROOT_INO, b"outside").unwrap();
    let store = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&a);

    let b = retained_test_engine(&b_path);
    sync_all_pages(&b);
    let version_before_restore = a.local_version.load(Ordering::SeqCst);

    let stage = a.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
    let stage_child = a.create_dir(stage.ino, b"child", 0, 0, 0o755).unwrap();
    let stage_file = a.create_file(stage.ino, b"file", 0, 0, 0o644).unwrap();
    stage_exact_small_payload(&a, stage_file.ino, 92_001, 64);
    let _ = stage_child;
    let restored = a
        .restore_retained_entry(
            receipt,
            dest.ino,
            b"restored",
            b"stage",
            &retained_test_source_fences(&a, receipt),
        )
        .unwrap();
    assert_eq!(restored.upload_epochs.len(), 1);
    let (&restored_file_ino, &restored_epoch) = restored.upload_epochs.iter().next().unwrap();
    assert_eq!(restored_file_ino, file.ino);
    mark_overlay_objects_uploaded(&a, file.ino, restored_epoch, &[92_001]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, restored_epoch)
            .unwrap()
    );
    a.link(file.ino, ROOT_INO, b"later-link").unwrap();
    let after_dir = a.create_dir(dest.ino, b"after-dir", 0, 0, 0o755).unwrap();
    a.set_acl_rule(dest.ino, "alice", AccessLevel::ReadWrite)
        .unwrap();
    a.set_acl_rule(source.ino, "bob", AccessLevel::ReadOnly)
        .unwrap();

    // Inject a crash on A right after the first replay page commits during flush catch-up.
    let committed_pages = Arc::new(AtomicUsize::new(0));
    a.set_catch_up_fault_hook(Arc::new({
        let committed_pages = Arc::clone(&committed_pages);
        move |point| match point {
            CatchUpFaultPoint::AfterLocalCommit { .. } => {
                committed_pages.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            CatchUpFaultPoint::BeforePageFetch { .. }
                if committed_pages.load(Ordering::SeqCst) >= 1 =>
            {
                Err(MetaError::Storage("injected mid-split crash".to_string()))
            }
            _ => Ok(()),
        }
    }));

    a.request_eager_flush();
    a.flush_due_pending()
        .expect("push split batches and commit page 1");
    assert_eq!(committed_pages.load(Ordering::SeqCst), 1);
    let err = a
        .sync_metadata()
        .expect_err("mid-split catch-up should fail at injected page 2 fetch");
    assert!(err.to_string().contains("injected mid-split crash"));

    let batch_count_after_push = run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            let row = client
                .query_one(
                    "SELECT COUNT(DISTINCT batch_version)::BIGINT FROM event_log WHERE vol = $1 AND batch_version > $2",
                    &[&volume_id, &version_before_restore],
                )
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok::<i64, MetaError>(row.get(0))
        }
    }))
    .unwrap();
    assert!(
        batch_count_after_push >= 3,
        "small byte limit must split restore publication into multiple PG batches, got {batch_count_after_push}"
    );

    // Simulate process restart between page 1 and remaining pages of self-batches.
    drop(a);
    let a = retained_test_engine(&a_path);
    sync_all_pages(&a);
    let a_head = a.local_version.load(Ordering::SeqCst);
    assert_eq!(
        a.replication_backlog().pending_ops,
        0,
        "restarted peer must finish self-ack across split batches without re-flushing"
    );
    assert_eq!(
        a.flushed_event_seq.load(Ordering::SeqCst),
        a.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
        "durable flushed_event_seq must advance to next_event_seq after split recovery"
    );
    a.request_eager_flush();
    a.flush_due_pending().unwrap();
    assert_eq!(a.local_version.load(Ordering::SeqCst), a_head);

    sync_all_pages(&b);
    assert_eq!(b.local_version.load(Ordering::SeqCst), a_head);
    assert_eq!(b.replication_backlog().pending_ops, 0);

    // Reopen A a second time to verify no dormant un-acknowledged range triggers re-publication.
    drop(a);
    let a = retained_test_engine(&a_path);
    assert_eq!(a.replication_backlog().pending_ops, 0);
    a.request_eager_flush();
    a.flush_due_pending().unwrap();
    assert_eq!(a.local_version.load(Ordering::SeqCst), a_head);

    for (name, engine) in [("a", &a), ("b", &b)] {
        assert_eq!(
            engine.lookup(dest.ino, b"restored").unwrap().ino,
            source.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(source.ino, b"child").unwrap().ino,
            child.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(source.ino, b"file").unwrap().ino,
            file.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(dest.ino, b"after-dir").unwrap().ino,
            after_dir.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(ROOT_INO, b"later-link").unwrap().ino,
            file.ino,
            "{name}"
        );
        assert_eq!(engine.get_attr(file.ino).unwrap().nlink, 3, "{name} nlink");
        assert_eq!(
            lookup_object_ids(engine, file.ino, 64).unwrap(),
            vec![92_001],
            "{name} payload"
        );
        assert_eq!(
            engine.get_acl_rule(dest.ino, "alice").unwrap(),
            Some(AccessLevel::ReadWrite),
            "{name} dest acl"
        );
        assert_eq!(
            engine.get_acl_rule(source.ino, "bob").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name} source acl"
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; losing restore remainder split across multiple batches must converge"]
fn retained_competing_restore_losing_remainder_split_batches_converge() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let _byte_limit = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "640");
    let volume = PgTestVolume::new(&dsn, "retained-loser-split");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-loser-split-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());

    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let winner_parent = a
        .create_dir(ROOT_INO, b"winner-parent", 0, 0, 0o755)
        .unwrap();
    let loser_parent = a
        .create_dir(ROOT_INO, b"loser-parent", 0, 0, 0o755)
        .unwrap();
    let source = a.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
    let child = a.create_dir(source.ino, b"child", 0, 0, 0o755).unwrap();
    let file = create_published_small_file(&a, source.ino, b"file", 91_001);
    a.link(file.ino, ROOT_INO, b"outside").unwrap();
    let store = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&a);

    let b = retained_test_engine(&b_path);
    sync_all_pages(&b);

    for (engine, parent, name, fresh_obj) in [
        (&a, winner_parent.ino, b"winner".as_slice(), 92_001_u64),
        (&b, loser_parent.ino, b"loser".as_slice(), 93_001_u64),
    ] {
        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
        engine.create_dir(stage.ino, b"child", 0, 0, 0o755).unwrap();
        let stage_file = engine.create_file(stage.ino, b"file", 0, 0, 0o644).unwrap();
        stage_exact_small_payload(engine, stage_file.ino, fresh_obj, 64);
        let restored = engine
            .restore_retained_entry(
                receipt,
                parent,
                name,
                b"stage",
                &retained_test_source_fences(engine, receipt),
            )
            .unwrap();
        for (ino, epoch) in restored.upload_epochs {
            mark_overlay_objects_uploaded(engine, ino, epoch, &[fresh_obj]);
            assert!(
                engine
                    .publish_regular_file_overlay_to_base_if_matches(ino, epoch)
                    .unwrap()
            );
        }
    }

    // Add post-restore mutations on loser B before conflict resolution.
    b.link(file.ino, ROOT_INO, b"later-link").unwrap();
    let after_dir = b
        .create_dir(loser_parent.ino, b"after-dir", 0, 0, 0o755)
        .unwrap();
    b.set_acl_rule(loser_parent.ino, "alice", AccessLevel::ReadOnly)
        .unwrap();
    b.set_acl_rule(source.ino, "bob", AccessLevel::ReadWrite)
        .unwrap();

    publish_and_pull(&a);
    let version_after_winner = a.local_version.load(Ordering::SeqCst);

    // B loses the shared receipt, reconciles locally, pulls winner, and flushes remainder across split batches.
    b.request_eager_flush();
    assert!(b.flush_due_pending().is_err());
    sync_all_pages(&b);
    publish_and_pull(&b);

    let loser_batch_count = run_async(with_pg_test_client(&dsn, {
        let volume_id = volume.volume_id;
        move |client| async move {
            let row = client
                .query_one(
                    "SELECT COUNT(DISTINCT batch_version)::BIGINT FROM event_log WHERE vol = $1 AND batch_version > $2",
                    &[&volume_id, &version_after_winner],
                )
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok::<i64, MetaError>(row.get(0))
        }
    }))
    .unwrap();
    assert!(
        loser_batch_count >= 3,
        "losing restore remainder must split across multiple PG batches, got {loser_batch_count}"
    );

    sync_all_pages(&a);
    drop(b);
    let b = retained_test_engine(&b_path);
    assert_eq!(b.replication_backlog().pending_ops, 0);
    assert_eq!(
        b.flushed_event_seq.load(Ordering::SeqCst),
        b.local.get_counter(NEXT_EVENT_SEQ).unwrap()
    );

    for (name, engine) in [("a", &a), ("b", &b)] {
        assert!(
            engine.lookup(loser_parent.ino, b"loser").is_err(),
            "{name} loser alias gone"
        );
        assert_eq!(
            engine.lookup(winner_parent.ino, b"winner").unwrap().ino,
            source.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(source.ino, b"child").unwrap().ino,
            child.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(loser_parent.ino, b"after-dir").unwrap().ino,
            after_dir.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(ROOT_INO, b"later-link").unwrap().ino,
            file.ino,
            "{name}"
        );
        assert_eq!(
            engine.get_attr(source.ino).unwrap().parent,
            winner_parent.ino,
            "{name}"
        );
        assert_eq!(
            engine.get_attr(loser_parent.ino).unwrap().nlink,
            3,
            "{name} loser_parent nlink"
        );
        assert_eq!(
            engine.get_attr(file.ino).unwrap().nlink,
            3,
            "{name} file nlink"
        );
        assert_eq!(
            engine.get_acl_rule(loser_parent.ino, "alice").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name} loser_parent acl"
        );
        assert_eq!(
            engine.get_acl_rule(source.ino, "bob").unwrap(),
            Some(AccessLevel::ReadWrite),
            "{name} source acl"
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; crash between losing reconcile and range completion retries and converges"]
fn retained_competing_restore_crash_between_reconcile_and_range_completion_retries_and_converges() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-reconcile-crash");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-reconcile-crash-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());

    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let winner_parent = a
        .create_dir(ROOT_INO, b"winner-parent", 0, 0, 0o755)
        .unwrap();
    let loser_parent = a
        .create_dir(ROOT_INO, b"loser-parent", 0, 0, 0o755)
        .unwrap();
    let source = a.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
    let file = create_published_small_file(&a, source.ino, b"file", 91_001);
    a.link(file.ino, ROOT_INO, b"outside").unwrap();
    let store = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&a);

    let b = retained_test_engine(&b_path);
    sync_all_pages(&b);

    for (engine, parent, name, fresh_obj) in [
        (&a, winner_parent.ino, b"winner".as_slice(), 92_001_u64),
        (&b, loser_parent.ino, b"loser".as_slice(), 93_001_u64),
    ] {
        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
        let stage_file = engine.create_file(stage.ino, b"file", 0, 0, 0o644).unwrap();
        stage_exact_small_payload(engine, stage_file.ino, fresh_obj, 64);
        let restored = engine
            .restore_retained_entry(
                receipt,
                parent,
                name,
                b"stage",
                &retained_test_source_fences(engine, receipt),
            )
            .unwrap();
        for (ino, epoch) in restored.upload_epochs {
            mark_overlay_objects_uploaded(engine, ino, epoch, &[fresh_obj]);
            assert!(
                engine
                    .publish_regular_file_overlay_to_base_if_matches(ino, epoch)
                    .unwrap()
            );
        }
    }

    b.link(file.ino, ROOT_INO, b"later-link").unwrap();
    let after_dir = b
        .create_dir(loser_parent.ino, b"after-dir", 0, 0, 0o755)
        .unwrap();
    b.set_acl_rule(loser_parent.ino, "alice", AccessLevel::ReadOnly)
        .unwrap();

    // A wins the shared receipt and publishes.
    publish_and_pull(&a);

    // Simulate B running `losing_retained_terminal_records` (which commits `reconcile_losing_retained_restore`
    // in redb) and crashing immediately BEFORE `complete_local_event_ranges` runs.
    let flushed = b.flushed_event_seq.load(Ordering::SeqCst);
    let next = b.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let pending = b.local.read_events_range(flushed, next).unwrap();
    let (losing, _) = b
        .losing_retained_terminal_records(
            &pending,
            &[LocalEventRange {
                start: flushed.max(1),
                end: next,
            }],
        )
        .unwrap();
    assert_eq!(losing.len(), 1);
    drop(b);

    // Reopen B, verify idempotence of a second classification pass and restart convergence.
    let b = retained_test_engine(&b_path);
    assert_eq!(b.get_attr(loser_parent.ino).unwrap().nlink, 3);
    let reopened_flushed = b.flushed_event_seq.load(Ordering::SeqCst);
    let reopened_next = b.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let pending_after_reopen = b
        .local
        .read_events_range(reopened_flushed, reopened_next)
        .unwrap();
    let (losing_retry, _) = b
        .losing_retained_terminal_records(
            &pending_after_reopen,
            &[LocalEventRange {
                start: reopened_flushed.max(1),
                end: reopened_next,
            }],
        )
        .unwrap();
    assert!(
        losing_retry.is_empty(),
        "already-reconciled losing terminal record must be idempotent across restarts"
    );
    assert_eq!(b.get_attr(loser_parent.ino).unwrap().nlink, 3);

    sync_all_pages(&b);
    publish_and_pull(&b);
    sync_all_pages(&a);

    drop(a);
    drop(b);
    let a = retained_test_engine(&a_path);
    let b = retained_test_engine(&b_path);
    for (name, engine) in [("a", &a), ("b", &b)] {
        assert_eq!(engine.replication_backlog().pending_ops, 0, "{name}");
        assert_eq!(
            engine.flushed_event_seq.load(Ordering::SeqCst),
            engine.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
            "{name} cursor"
        );
        assert!(engine.lookup(loser_parent.ino, b"loser").is_err(), "{name}");
        assert_eq!(
            engine.lookup(winner_parent.ino, b"winner").unwrap().ino,
            source.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(loser_parent.ino, b"after-dir").unwrap().ino,
            after_dir.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(ROOT_INO, b"later-link").unwrap().ino,
            file.ino,
            "{name}"
        );
        assert_eq!(
            engine.get_attr(loser_parent.ino).unwrap().nlink,
            3,
            "{name}"
        );
        assert_eq!(engine.get_attr(file.ino).unwrap().nlink, 3, "{name}");
        assert_eq!(
            lookup_object_ids(engine, file.ino, 64).unwrap(),
            vec![92_001],
            "{name}"
        );
        assert_eq!(
            engine.get_acl_rule(loser_parent.ino, "alice").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name}"
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; competing nonempty restores preserve winner bytes and dest/ancestor ACLs & attrs"]
fn retained_competing_restores_nonempty_payload_with_concurrent_dest_and_ancestor_acl_attr_mutations()
 {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-nonempty-acl");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-nonempty-acl-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());

    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let dest_a_anc = a.create_dir(ROOT_INO, b"dest-a-anc", 0, 0, 0o750).unwrap();
    a.set_acl_rule(dest_a_anc.ino, "alice", AccessLevel::ReadWrite)
        .unwrap();
    let dest_a = a
        .create_dir(dest_a_anc.ino, b"dest-a", 0, 0, 0o755)
        .unwrap();
    let dest_b_anc = a.create_dir(ROOT_INO, b"dest-b-anc", 0, 0, 0o750).unwrap();
    a.set_acl_rule(dest_b_anc.ino, "bob", AccessLevel::ReadOnly)
        .unwrap();
    let dest_b = a
        .create_dir(dest_b_anc.ino, b"dest-b", 0, 0, 0o755)
        .unwrap();

    let source = a.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
    let child = a.create_dir(source.ino, b"child", 0, 0, 0o755).unwrap();
    let file_a = create_published_small_file(&a, source.ino, b"file-a", 91_001);
    let file_b = create_published_small_file(&a, child.ino, b"file-b", 91_002);
    a.link(file_a.ino, ROOT_INO, b"outside").unwrap();
    let store = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&a);

    let b = retained_test_engine(&b_path);
    sync_all_pages(&b);

    // Concurrent destination and ancestor ACL/attr mutations on A alongside A's restore.
    let mut anc_a_attr = a.get_attr(dest_a_anc.ino).unwrap();
    anc_a_attr.mode = 0o755;
    a.set_attr(&anc_a_attr).unwrap();
    a.set_acl_rule(dest_a_anc.ino, "carol", AccessLevel::ReadWrite)
        .unwrap();
    let mut dest_a_attr = a.get_attr(dest_a.ino).unwrap();
    dest_a_attr.mode = 0o700;
    a.set_attr(&dest_a_attr).unwrap();
    a.set_acl_rule(dest_a.ino, "dave", AccessLevel::ReadOnly)
        .unwrap();

    let a_stage = a.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
    let a_stage_child = a.create_dir(a_stage.ino, b"child", 0, 0, 0o755).unwrap();
    let a_stage_fa = a.create_file(a_stage.ino, b"file-a", 0, 0, 0o644).unwrap();
    let a_stage_fb = a
        .create_file(a_stage_child.ino, b"file-b", 0, 0, 0o644)
        .unwrap();
    stage_exact_small_payload(&a, a_stage_fa.ino, 92_001, 64);
    stage_exact_small_payload(&a, a_stage_fb.ino, 92_002, 64);
    let a_restored = a
        .restore_retained_entry(
            receipt,
            dest_a.ino,
            b"restored-a",
            b"stage",
            &retained_test_source_fences(&a, receipt),
        )
        .unwrap();
    assert_eq!(a_restored.upload_epochs.len(), 2);
    for (ino, obj_id) in [(file_a.ino, 92_001_u64), (file_b.ino, 92_002_u64)] {
        let epoch = a_restored.upload_epochs[&ino];
        mark_overlay_objects_uploaded(&a, ino, epoch, &[obj_id]);
        assert!(
            a.publish_regular_file_overlay_to_base_if_matches(ino, epoch)
                .unwrap()
        );
    }
    a.set_acl_rule(source.ino, "eve", AccessLevel::ReadWrite)
        .unwrap();

    // Concurrent destination and ancestor ACL/attr mutations on B alongside B's competing restore.
    let mut anc_b_attr = b.get_attr(dest_b_anc.ino).unwrap();
    anc_b_attr.mode = 0o711;
    b.set_attr(&anc_b_attr).unwrap();
    b.set_acl_rule(dest_b_anc.ino, "frank", AccessLevel::ReadWrite)
        .unwrap();
    let mut dest_b_attr = b.get_attr(dest_b.ino).unwrap();
    dest_b_attr.mode = 0o751;
    b.set_attr(&dest_b_attr).unwrap();
    b.set_acl_rule(dest_b.ino, "grace", AccessLevel::ReadOnly)
        .unwrap();

    let b_stage = b.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
    let b_stage_child = b.create_dir(b_stage.ino, b"child", 0, 0, 0o755).unwrap();
    let b_stage_fa = b.create_file(b_stage.ino, b"file-a", 0, 0, 0o644).unwrap();
    let b_stage_fb = b
        .create_file(b_stage_child.ino, b"file-b", 0, 0, 0o644)
        .unwrap();
    stage_exact_small_payload(&b, b_stage_fa.ino, 93_001, 64);
    stage_exact_small_payload(&b, b_stage_fb.ino, 93_002, 64);
    let b_restored = b
        .restore_retained_entry(
            receipt,
            dest_b.ino,
            b"restored-b",
            b"stage",
            &retained_test_source_fences(&b, receipt),
        )
        .unwrap();
    assert_eq!(b_restored.upload_epochs.len(), 2);
    for (ino, obj_id) in [(file_a.ino, 93_001_u64), (file_b.ino, 93_002_u64)] {
        let epoch = b_restored.upload_epochs[&ino];
        mark_overlay_objects_uploaded(&b, ino, epoch, &[obj_id]);
        assert!(
            b.publish_regular_file_overlay_to_base_if_matches(ino, epoch)
                .unwrap()
        );
    }
    let b_after = b.create_dir(dest_b.ino, b"b-after", 0, 0, 0o755).unwrap();
    b.link(file_a.ino, ROOT_INO, b"b-link").unwrap();
    b.set_acl_rule(source.ino, "heidi", AccessLevel::ReadOnly)
        .unwrap();

    // A publishes first and wins the receipt; B reconciles, syncs, publishes remainder, and both restart.
    publish_and_pull(&a);
    b.request_eager_flush();
    assert!(b.flush_due_pending().is_err());
    sync_all_pages(&b);
    publish_and_pull(&b);
    sync_all_pages(&a);

    drop(a);
    drop(b);
    let a = retained_test_engine(&a_path);
    let b = retained_test_engine(&b_path);
    for (name, engine) in [("a", &a), ("b", &b)] {
        assert_eq!(engine.replication_backlog().pending_ops, 0, "{name}");
        assert_eq!(
            engine.flushed_event_seq.load(Ordering::SeqCst),
            engine.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
            "{name} cursor"
        );
        assert!(engine.lookup(dest_b.ino, b"restored-b").is_err(), "{name}");
        assert_eq!(
            engine.lookup(dest_a.ino, b"restored-a").unwrap().ino,
            source.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(source.ino, b"child").unwrap().ino,
            child.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(source.ino, b"file-a").unwrap().ino,
            file_a.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(child.ino, b"file-b").unwrap().ino,
            file_b.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(dest_b.ino, b"b-after").unwrap().ino,
            b_after.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(ROOT_INO, b"b-link").unwrap().ino,
            file_a.ino,
            "{name}"
        );
        assert_eq!(
            engine.get_attr(file_a.ino).unwrap().nlink,
            3,
            "{name} file_a nlink"
        );
        assert_eq!(
            engine.get_attr(dest_a.ino).unwrap().nlink,
            3,
            "{name} dest_a nlink"
        );
        assert_eq!(
            engine.get_attr(dest_b.ino).unwrap().nlink,
            3,
            "{name} dest_b nlink"
        );

        // Nonempty winner bytes survive on both files.
        assert_eq!(
            lookup_object_ids(engine, file_a.ino, 64).unwrap(),
            vec![92_001],
            "{name} file_a payload"
        );
        assert_eq!(
            lookup_object_ids(engine, file_b.ino, 64).unwrap(),
            vec![92_002],
            "{name} file_b payload"
        );

        // Destination and ancestor modes + ACLs remain intact on both peers.
        assert_eq!(
            engine.get_attr(dest_a_anc.ino).unwrap().mode,
            0o755,
            "{name} dest_a_anc mode"
        );
        assert_eq!(
            engine.get_acl_rule(dest_a_anc.ino, "alice").unwrap(),
            Some(AccessLevel::ReadWrite),
            "{name}"
        );
        assert_eq!(
            engine.get_acl_rule(dest_a_anc.ino, "carol").unwrap(),
            Some(AccessLevel::ReadWrite),
            "{name}"
        );
        assert_eq!(
            engine.get_attr(dest_a.ino).unwrap().mode,
            0o700,
            "{name} dest_a mode"
        );
        assert_eq!(
            engine.get_acl_rule(dest_a.ino, "dave").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name}"
        );

        assert_eq!(
            engine.get_attr(dest_b_anc.ino).unwrap().mode,
            0o711,
            "{name} dest_b_anc mode"
        );
        assert_eq!(
            engine.get_acl_rule(dest_b_anc.ino, "bob").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name}"
        );
        assert_eq!(
            engine.get_acl_rule(dest_b_anc.ino, "frank").unwrap(),
            Some(AccessLevel::ReadWrite),
            "{name}"
        );
        assert_eq!(
            engine.get_attr(dest_b.ino).unwrap().mode,
            0o751,
            "{name} dest_b mode"
        );
        assert_eq!(
            engine.get_acl_rule(dest_b.ino, "grace").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name}"
        );

        assert_eq!(
            engine.get_acl_rule(source.ino, "eve").unwrap(),
            Some(AccessLevel::ReadWrite),
            "{name}"
        );
        assert_eq!(
            engine.get_acl_rule(source.ino, "heidi").unwrap(),
            Some(AccessLevel::ReadOnly),
            "{name}"
        );
    }
}

#[derive(Clone, Copy)]
enum RetainedWinnerEpochSource {
    EventTail,
    Checkpoint,
}

fn retained_losing_restore_post_restore_write_survives_winner_apply(
    publish_later_write: bool,
    suffix: &str,
    write_on_winner: bool,
    size_only: bool,
    winner_epoch_source: Option<RetainedWinnerEpochSource>,
) {
    use crate::engine::MetaEngine;
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let _page_limit = winner_epoch_source
        .is_some()
        .then(|| set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "1"));
    let volume = PgTestVolume::new(&dsn, suffix);
    let dir = studiofs_test_support::workspace_target_tempdir("retained-losing-write-");
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");
    seed_local(&a_path, &volume.format_info());
    seed_local(&b_path, &volume.format_info());

    let a = retained_test_engine(&a_path);
    a.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let winner_parent = a
        .create_dir(ROOT_INO, b"winner-parent", 0, 0, 0o755)
        .unwrap();
    let loser_parent = a
        .create_dir(ROOT_INO, b"loser-parent", 0, 0, 0o755)
        .unwrap();
    let source = a.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
    let graph_size = u64::from(crate::schema::DEFAULT_CHUNK_SIZE) + 64;
    let stage_graph = |engine: &PgMetaEngine, ino, object| {
        set_file_state(engine, ino, graph_size, 1);
        let chunks = [(0, crate::schema::DEFAULT_CHUNK_SIZE), (1, 64)].map(|(chunk, size)| {
            (
                crate::schema::ChunkId(chunk),
                vec![crate::schema::SliceRecord::new(
                    crate::schema::SliceId(0),
                    crate::schema::SliceOrder(0),
                    crate::schema::SliceRange::new(0, size).unwrap(),
                    PayloadRef::new_partial(object + chunk, size),
                )],
            )
        });
        engine
            .flush_regular_file_overlay_slices(ino, &chunks)
            .unwrap();
    };
    let file = a.create_file(source.ino, b"file", 0, 0, 0o644).unwrap();
    stage_graph(&a, file.ino, 91_001);
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[91_001, 91_002]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );
    a.link(file.ino, ROOT_INO, b"outside").unwrap();
    let store = a.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = a.next_inode().unwrap();
    a.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&a);

    let b = retained_test_engine(&b_path);
    sync_all_pages(&b);
    let mut restore_epochs = Vec::new();
    for (engine, parent, name, object_id, is_winner) in [
        (
            &a,
            winner_parent.ino,
            b"winner".as_slice(),
            92_001_u64,
            true,
        ),
        (&b, loser_parent.ino, b"loser".as_slice(), 93_001_u64, false),
    ] {
        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
        let stage_file = engine.create_file(stage.ino, b"file", 0, 0, 0o644).unwrap();
        stage_graph(engine, stage_file.ino, object_id);
        if write_on_winner && !is_winner {
            // Equivalent overlapping slices make the pristine restore a real compaction candidate.
            let overlap = crate::schema::SliceRecord::new(
                crate::schema::SliceId(0),
                crate::schema::SliceOrder(0),
                crate::schema::SliceRange::new(0, 16).unwrap(),
                PayloadRef::new_partial(object_id, 16),
            );
            engine
                .flush_regular_file_overlay_slices(
                    stage_file.ino,
                    &[(crate::schema::ChunkId(0), vec![overlap])],
                )
                .unwrap();
        }
        let restored = engine
            .restore_retained_entry(
                receipt,
                parent,
                name,
                b"stage",
                &retained_test_source_fences(engine, receipt),
            )
            .unwrap();
        assert_eq!(restored.upload_epochs.len(), 1);
        let (&ino, &epoch) = restored.upload_epochs.iter().next().unwrap();
        assert_eq!(ino, file.ino);
        if write_on_winner && !is_winner {
            assert!(engine.compact_fragmented_regular_file_chunks(32).unwrap() > 0);
        }
        if is_winner || write_on_winner {
            mark_overlay_objects_uploaded(engine, ino, epoch, &[object_id, object_id + 1]);
            assert!(
                engine
                    .publish_regular_file_overlay_to_base_if_matches(ino, epoch)
                    .unwrap()
            );
        }
        if !is_winner {
            restore_epochs.push(epoch);
        }
    }

    if winner_epoch_source.is_some() {
        let mut attr = a.get_attr(file.ino).unwrap();
        attr.content_epoch += 1;
        attr.upload_state = UploadState::Updating;
        let patch = crate::schema::SliceRecord::new(
            crate::schema::SliceId(0),
            crate::schema::SliceOrder(0),
            crate::schema::SliceRange::new(24, 40).unwrap(),
            PayloadRef::new_partial(95_001, 16),
        );
        a.flush_regular_file_overlay_slices_with_attr(
            file.ino,
            &[(crate::schema::ChunkId(0), vec![patch])],
            Some(&attr),
        )
        .unwrap();
        mark_overlay_objects_uploaded(&a, file.ino, attr.content_epoch, &[95_001]);
        assert!(
            a.publish_regular_file_overlay_to_base_if_matches(file.ino, attr.content_epoch)
                .unwrap()
        );
    }
    let later_object = 94_001;
    let writer = if write_on_winner { &a } else { &b };
    let mut written_attr = writer.get_attr(file.ino).unwrap();
    if write_on_winner {
        written_attr.content_epoch += 1;
        written_attr.upload_state = UploadState::Updating;
    }
    let patch = crate::schema::SliceRecord::new(
        crate::schema::SliceId(0),
        crate::schema::SliceOrder(0),
        crate::schema::SliceRange::new(24, 40).unwrap(),
        PayloadRef::new_partial(later_object, 16),
    );
    if size_only {
        written_attr.size += 1;
        writer.set_attr(&written_attr).unwrap();
    } else {
        writer
            .flush_regular_file_overlay_slices_with_attr(
                file.ino,
                &[(crate::schema::ChunkId(0), vec![patch])],
                Some(&written_attr),
            )
            .unwrap();
    }
    if publish_later_write {
        mark_overlay_objects_uploaded(
            writer,
            file.ino,
            written_attr.content_epoch,
            &[later_object],
        );
    }
    if !write_on_winner {
        assert_eq!(written_attr.content_epoch, restore_epochs[0]);
        assert_eq!(written_attr.committed_content_epoch, 0);
        if publish_later_write {
            mark_overlay_objects_uploaded(&b, file.ino, restore_epochs[0], &[93_001, 93_002]);
        }
    }
    if publish_later_write {
        assert!(
            writer
                .publish_regular_file_overlay_to_base_if_matches(
                    file.ino,
                    written_attr.content_epoch
                )
                .unwrap()
        );
    }

    // A wins the receipt; B's accepted write is later than B's restore fork.
    publish_and_pull(&a);
    if matches!(
        winner_epoch_source,
        Some(RetainedWinnerEpochSource::Checkpoint)
    ) {
        a.flush_metadata().unwrap();
    }
    if let Some(source) = winner_epoch_source {
        let descriptor = a.snapshot_backend().read_bootstrap_checkpoint().unwrap();
        match source {
            RetainedWinnerEpochSource::EventTail => {
                assert!(descriptor.checkpoint_version < descriptor.remote_head_version);
            }
            RetainedWinnerEpochSource::Checkpoint => {
                assert_eq!(
                    descriptor.checkpoint_version,
                    descriptor.remote_head_version
                );
                let checkpoint = descriptor.validate_and_decode().unwrap();
                let (_, bytes) = checkpoint
                    .inodes
                    .iter()
                    .find(|(ino, _)| *ino == file.ino)
                    .unwrap();
                let attr: InodeAttr = postcard::from_bytes(bytes).unwrap();
                assert_eq!(attr.content_epoch, restore_epochs[0] + 1);
            }
        }
    }
    let flushed = b.flushed_event_seq.load(Ordering::SeqCst);
    let next = b.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let pending = b.local.read_events_range(flushed, next).unwrap();
    let (losing, _) = b
        .losing_retained_terminal_records(
            &pending,
            &[LocalEventRange {
                start: flushed.max(1),
                end: next,
            }],
        )
        .unwrap();
    assert_eq!(losing.len(), 1, "actual losing restore classification");
    drop(b);
    let b = retained_test_engine(&b_path);
    sync_all_pages(&b);
    let unchanged_object = if write_on_winner { 92_001 } else { 93_001 };
    let expected_objects = if size_only {
        vec![unchanged_object, unchanged_object + 1]
    } else {
        vec![
            unchanged_object,
            later_object,
            unchanged_object,
            unchanged_object + 1,
        ]
    };
    assert_eq!(
        lookup_object_ids(&b, file.ino, graph_size).unwrap(),
        expected_objects,
        "loser's local serving view after winner apply"
    );
    let references = b.local.get_local_referenced_object_ids().unwrap();
    assert!(
        expected_objects
            .iter()
            .all(|object| references.contains(object))
    );
    if size_only {
        assert_eq!(b.get_attr(file.ino).unwrap().size, graph_size + 1);
        assert_eq!(b.lookup(ROOT_INO, b"outside").unwrap().ino, file.ino);
        assert_eq!(
            b.lookup(winner_parent.ino, b"winner").unwrap().ino,
            source.ino
        );
        assert!(b.lookup(loser_parent.ino, b"loser").is_err());
        // Bare metadata set_attr has no VFS sparse-hole journal; this case ends at local replay.
        return;
    }
    if !write_on_winner {
        let pending = b.get_attr(file.ino).unwrap();
        assert!(pending.content_epoch > restore_epochs[0]);
        if winner_epoch_source.is_some() {
            assert!(
                pending.content_epoch > a.get_attr(file.ino).unwrap().content_epoch,
                "accepted B content must have an epoch above the already-published winner"
            );
        }
        assert_ne!(pending.upload_state, UploadState::Ready);
        assert_eq!(pending.committed_content_epoch, 0);
        assert_eq!(pending.committed_size, 0);
        let lifecycle = b.local.get_inode_lifecycle(file.ino).unwrap().unwrap();
        assert_eq!(lifecycle.local_content_epoch, pending.content_epoch);
        assert!(lifecycle.local_view_differs_from_published());
        if !publish_later_write {
            assert!(
                !b.publish_regular_file_overlay_to_base_if_matches(file.ino, pending.content_epoch)
                    .unwrap()
            );
            let mut object_ids = expected_objects.clone();
            object_ids.sort_unstable();
            object_ids.dedup();
            mark_overlay_objects_uploaded(&b, file.ino, pending.content_epoch, &object_ids);
        }
        assert!(
            b.publish_regular_file_overlay_to_base_if_matches(file.ino, pending.content_epoch)
                .unwrap()
        );
    }
    publish_and_pull(&b);
    sync_all_pages(&a);
    drop(a);
    drop(b);
    let a = retained_test_engine(&a_path);
    let b = retained_test_engine(&b_path);

    for (name, engine) in [("a", &a), ("b", &b)] {
        assert_eq!(
            engine.lookup(ROOT_INO, b"outside").unwrap().ino,
            file.ino,
            "{name}"
        );
        assert_eq!(
            engine.lookup(winner_parent.ino, b"winner").unwrap().ino,
            source.ino,
            "{name} winner namespace"
        );
        assert!(
            engine.lookup(loser_parent.ino, b"loser").is_err(),
            "{name} loser namespace"
        );
        assert_eq!(
            engine.get_attr(file.ino).unwrap().nlink,
            2,
            "{name} hardlink identity"
        );
        assert_eq!(
            lookup_object_ids(engine, file.ino, graph_size).unwrap(),
            expected_objects,
            "{name} accepted post-restore bytes"
        );
        let attr = engine.get_attr(file.ino).unwrap();
        assert_eq!(attr.upload_state, UploadState::Ready, "{name} upload state");
        assert_eq!(
            attr.size,
            graph_size + u64::from(size_only),
            "{name} accepted size"
        );
        assert_eq!(attr.committed_size, attr.size, "{name} committed size");
        assert_eq!(
            attr.committed_content_epoch, attr.content_epoch,
            "{name} committed epoch"
        );
    }
    assert_eq!(b.replication_backlog().pending_ops, 0);
}

#[test]
#[ignore = "requires PostgreSQL; accepted overlay write after losing restore must survive winner apply"]
fn retained_losing_restore_unpublished_post_restore_write_survives_winner_apply() {
    retained_losing_restore_post_restore_write_survives_winner_apply(
        false,
        "retained-losing-unpublished-write",
        false,
        false,
        None,
    );
}

#[test]
#[ignore = "requires PostgreSQL; accepted published write after losing restore must survive winner apply"]
fn retained_losing_restore_published_post_restore_write_survives_winner_apply() {
    retained_losing_restore_post_restore_write_survives_winner_apply(
        true,
        "retained-losing-published-write",
        false,
        false,
        None,
    );
}

#[test]
#[ignore = "requires PostgreSQL; a pristine losing restore must not supersede winner writes"]
fn retained_losing_restore_pristine_fork_preserves_later_winner_write() {
    retained_losing_restore_post_restore_write_survives_winner_apply(
        true,
        "retained-losing-pristine-control",
        true,
        false,
        None,
    );
}

#[test]
#[ignore = "requires PostgreSQL; a size-only accepted extension is not a pristine restore"]
fn retained_losing_restore_size_only_extension_survives_local_winner_replay() {
    retained_losing_restore_post_restore_write_survives_winner_apply(
        false,
        "retained-losing-size-only-extension",
        false,
        true,
        None,
    );
}

#[test]
#[ignore = "requires PostgreSQL; accepted loser content must advance past the winner's later epoch"]
fn retained_losing_restore_published_write_advances_past_later_winner_epoch() {
    retained_losing_restore_post_restore_write_survives_winner_apply(
        true,
        "retained-losing-winner-epoch",
        false,
        false,
        Some(RetainedWinnerEpochSource::EventTail),
    );
}

#[test]
#[ignore = "requires PostgreSQL; epoch rebasing must include the validated shared checkpoint"]
fn retained_losing_restore_published_write_advances_past_checkpointed_winner_epoch() {
    retained_losing_restore_post_restore_write_survives_winner_apply(
        true,
        "retained-losing-checkpoint-winner-epoch",
        false,
        false,
        Some(RetainedWinnerEpochSource::Checkpoint),
    );
}

#[test]
#[ignore = "requires PostgreSQL; durable child purge versus peer restore in both orders"]
fn test_pg_retained_child_purge_versus_peer_restore_never_deletes_live_child() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome, RetainedEntryState,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    for restore_first in [true, false] {
        let volume = PgTestVolume::new(&dsn, "retained-child-purge");
        let dir = studiofs_test_support::workspace_target_tempdir("retained-child-purge-");
        seed_local(&dir.path().join("first"), &volume.format_info());
        seed_local(&dir.path().join("second"), &volume.format_info());
        let first = retained_test_engine(&dir.path().join("first"));
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let kept = first.create_dir(payload.ino, b"kept", 0, 0, 0o700).unwrap();
        let doomed = first
            .create_dir(payload.ino, b"doomed", 0, 0, 0o700)
            .unwrap();
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        let second = retained_test_engine(&dir.path().join("second"));
        let deadline = Instant::now() + Duration::from_secs(10);
        while second.retained_entry(receipt).unwrap().is_none() {
            assert!(Instant::now() < deadline, "peer never observed the receipt");
            publish_and_pull(&first);
            second.sync_metadata().unwrap();
        }
        let restore_on_second = |names: &[&[u8]]| {
            let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
            for name in names {
                second.create_dir(staged.ino, name, 0, 0, 0o700).unwrap();
            }
            second
                .restore_retained_entry(
                    receipt,
                    ROOT_INO,
                    b"restored",
                    b"stage",
                    &retained_test_source_fences(&second, receipt),
                )
                .unwrap();
        };

        if restore_first {
            // Peer restore wins; the offline child purge must lose without
            // deleting the now-live child anywhere.
            restore_on_second(&[b"kept", b"doomed"]);
            publish_and_pull(&second);
            assert!(matches!(
                first
                    .purge_retained_child(receipt, payload.ino, b"doomed", doomed.ino, true)
                    .unwrap(),
                RetainedChildPurgeOutcome::Removed(_)
            ));
            assert!(first.lookup(payload.ino, b"doomed").is_err());
            first.request_eager_flush();
            let _ = first.flush_due_pending();
            publish_and_pull(&first);
            second.sync_metadata().unwrap();
            for engine in [&first, &second] {
                assert!(matches!(
                    engine.retained_entry(receipt).unwrap().unwrap().state,
                    RetainedEntryState::Restored { .. }
                ));
                assert_eq!(
                    engine.lookup(ROOT_INO, b"restored").unwrap().ino,
                    payload.ino
                );
                assert_eq!(
                    engine.lookup(payload.ino, b"doomed").unwrap().ino,
                    doomed.ino,
                    "a losing child purge must never delete a restored live child"
                );
                assert_eq!(engine.lookup(payload.ino, b"kept").unwrap().ino, kept.ino);
            }
            assert!(
                first.flushed_event_seq.load(Ordering::SeqCst)
                    >= first
                        .local
                        .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                        .unwrap(),
                "the losing child purge must retire instead of blocking the journal"
            );
        } else {
            // The child purge wins while the receipt is still retained; the peer
            // sees it gone and restores only the remaining tree.
            assert!(matches!(
                first
                    .purge_retained_child(receipt, payload.ino, b"doomed", doomed.ino, true)
                    .unwrap(),
                RetainedChildPurgeOutcome::Removed(_)
            ));
            publish_and_pull(&first);
            let deadline = Instant::now() + Duration::from_secs(10);
            while second.lookup(payload.ino, b"doomed").is_ok() {
                assert!(
                    Instant::now() < deadline,
                    "peer never observed the child purge"
                );
                sync_all_pages(&second);
            }
            assert!(second.get_attr(doomed.ino).is_err());
            restore_on_second(&[b"kept"]);
            publish_and_pull(&second);
            sync_all_pages(&first);
            for engine in [&first, &second] {
                assert!(matches!(
                    engine.retained_entry(receipt).unwrap().unwrap().state,
                    RetainedEntryState::Restored { .. }
                ));
                assert_eq!(
                    engine.lookup(ROOT_INO, b"restored").unwrap().ino,
                    payload.ino
                );
                assert_eq!(engine.lookup(payload.ino, b"kept").unwrap().ino, kept.ino);
                assert!(engine.lookup(payload.ino, b"doomed").is_err());
            }
        }
    }
}

#[test]
#[ignore = "requires PostgreSQL; a published-but-unacknowledged child purge must survive a later losing child purge"]
fn test_pg_retained_losing_child_purge_keeps_published_unacked_span() {
    retained_losing_child_purge_keeps_published_unacked_span(false);
}

#[test]
#[ignore = "requires PostgreSQL; after a restart, foreign apply self-acks the published purge and then retires the loser"]
fn test_pg_retained_losing_child_purge_restart_foreign_apply_keeps_published_span() {
    retained_losing_child_purge_keeps_published_unacked_span(true);
}

/// With `restart_before_send`, the process restarts after child purge 2 but
/// before it is sent, so foreign apply (not the flush classifier) handles it.
fn retained_losing_child_purge_keeps_published_unacked_span(restart_before_send: bool) {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome, RetainedEntryState,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-child-purge-unacked");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-child-purge-unacked-");
    let first_path = dir.path().join("first");
    seed_local(&first_path, &volume.format_info());
    seed_local(&dir.path().join("second"), &volume.format_info());
    let first = retained_test_engine(&first_path);
    first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let kept = first.create_dir(payload.ino, b"kept", 0, 0, 0o700).unwrap();
    let published = first
        .create_dir(payload.ino, b"published", 0, 0, 0o700)
        .unwrap();
    let losing = first
        .create_dir(payload.ino, b"losing", 0, 0, 0o700)
        .unwrap();
    let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&first);
    let second = retained_test_engine(&dir.path().join("second"));
    sync_all_pages(&second);
    assert!(second.retained_entry(receipt).unwrap().is_some());

    // Child purge 1 reaches PG, but its self batch is not replayed yet.
    assert!(matches!(
        first
            .purge_retained_child(receipt, payload.ino, b"published", published.ino, true)
            .unwrap(),
        RetainedChildPurgeOutcome::Removed(_)
    ));
    let batch = first.claim_pending_batch(true).expect("claim child purge");
    first
        .flush_pending_claimed_batch_with_restore(batch)
        .unwrap();
    let next_seq = || {
        first
            .local
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap()
    };
    assert!(
        first.flushed_event_seq.load(Ordering::SeqCst) < next_seq(),
        "fixture must leave the published child purge unacknowledged"
    );

    // The peer observes it and restores the remaining tree.
    sync_all_pages(&second);
    assert!(second.lookup(payload.ino, b"published").is_err());
    let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
    for name in [&b"kept"[..], b"losing"] {
        second.create_dir(staged.ino, name, 0, 0, 0o700).unwrap();
    }
    second
        .restore_retained_entry(
            receipt,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&second, receipt),
        )
        .unwrap();
    publish_and_pull(&second);

    // Child purge 2 loses at its fence; only its own span may retire.
    assert!(matches!(
        first
            .purge_retained_child(receipt, payload.ino, b"losing", losing.ino, true)
            .unwrap(),
        RetainedChildPurgeOutcome::Removed(_)
    ));
    let first = if restart_before_send {
        // Restart with an empty in-memory published ledger, then pull: PG order
        // replays the self batch (acknowledging span 1) before the peer restore,
        // whose foreign apply must retire only span 2.
        drop(first);
        let first = retained_test_engine(&first_path);
        sync_all_pages(&first);
        first
    } else {
        // Push before any replay, so the flush classifier (not foreign apply) handles
        // the loss, then crash before the recovery pull: the in-memory published
        // ledger is gone and only the journal can prove the first span's ownership.
        let batch = first.claim_pending_batch(true).expect("claim losing purge");
        assert!(
            first.flush_pending_claimed_batch(batch).is_err(),
            "the fence must conflict with the peer's restore"
        );
        first
    };
    // The already-committed span stays byte-identical so its self batch still
    // matches; only the losing span retires.
    let journal = first.local.read_events_range(1, u64::MAX).unwrap();
    let span_children: Vec<Ino> = (0..journal.len())
        .filter_map(|index| RedbMetaEngine::child_purge_event_span(&journal, index))
        .map(|span| span.child_ino)
        .collect();
    assert_eq!(
        span_children,
        vec![published.ino],
        "reconcile must retire only the losing span, never the published one"
    );
    drop(first);
    let first = retained_test_engine(&first_path);
    for _ in 0..4 {
        publish_and_pull(&first);
    }
    second.sync_metadata().unwrap();

    let check = |first: &PgMetaEngine| {
        assert!(
            first.flushed_event_seq.load(Ordering::SeqCst)
                >= first
                    .local
                    .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                    .unwrap(),
            "the published child purge must self-acknowledge and the loser retire"
        );
        for engine in [first, &*second] {
            assert!(matches!(
                engine.retained_entry(receipt).unwrap().unwrap().state,
                RetainedEntryState::Restored { .. }
            ));
            assert_eq!(
                engine.lookup(ROOT_INO, b"restored").unwrap().ino,
                payload.ino
            );
            assert_eq!(engine.lookup(payload.ino, b"kept").unwrap().ino, kept.ino);
            assert_eq!(
                engine.lookup(payload.ino, b"losing").unwrap().ino,
                losing.ino,
                "a losing child purge must never delete a restored live child"
            );
            assert!(engine.lookup(payload.ino, b"published").is_err());
        }
    };
    check(&first);
    drop(first);
    let first = retained_test_engine(&first_path);
    sync_all_pages(&first);
    check(&first);
}

/// How the source handle opened before Move to Trash is finalized around the
/// child purge classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrphanFinalizeOrder {
    /// Last close before the losing purge is classified.
    CloseBefore,
    /// Last close after loser reconcile.
    CloseAfter,
    /// Process restart (deferred-delete replay) before classification.
    RestartBefore,
    /// No peer restore: the purge wins and restart replay finalizes it.
    WinnerRestart,
    /// No peer restore: the purge wins and the running lifecycle sweep
    /// finalizes it once acknowledged and closed, without a restart.
    WinnerSweep,
}

fn pg_event_touches_ino(dsn: &str, volume_id: i64, ino: Ino) -> bool {
    run_async(with_pg_test_client(dsn, move |client| async move {
        client
            .query(
                "SELECT event_type, event_payload FROM event_log WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .map_err(|error| MetaError::Storage(error.to_string()))
    }))
    .unwrap()
    .into_iter()
    .any(|row| {
        matches!(
            crate::events::decode_event_with_type(row.get(0), row.get(1)).unwrap(),
            crate::MetaEvent::RemoveAttr { ino: removed }
                | crate::MetaEvent::RemoveRegularFileGraph { ino: removed }
                if removed == ino
        )
    })
}

#[test]
#[ignore = "requires PostgreSQL; an orphan close must never finalize a child that a losing purge revived"]
fn test_pg_retained_open_child_purge_orphan_finalize_respects_live_binding() {
    use crate::engine::{MetaRecovery, OrphanInodeFinalization};
    use crate::snapshot_ports::{UploadedFragmentLedger, UploadedObjectDeletionClaim};
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome, RetainedEntryState,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    for (order, object_id) in [
        (OrphanFinalizeOrder::CloseBefore, 62_001_u64),
        (OrphanFinalizeOrder::CloseAfter, 62_002),
        (OrphanFinalizeOrder::RestartBefore, 62_003),
        (OrphanFinalizeOrder::WinnerRestart, 62_004),
        (OrphanFinalizeOrder::WinnerSweep, 62_005),
    ] {
        let volume = PgTestVolume::new(&dsn, "retained-orphan-close");
        let volume_id = volume.volume_id;
        let dir = studiofs_test_support::workspace_target_tempdir("retained-orphan-close-");
        let first_path = dir.path().join("first");
        seed_local(&first_path, &volume.format_info());
        seed_local(&dir.path().join("second"), &volume.format_info());
        let first = retained_test_engine(&first_path);
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let child = create_published_small_file(&first, payload.ino, b"open", object_id);
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        // The peer's restore adopts its eager stage copy, so these are the live bytes.
        let restored_object = object_id + 100;
        run_async(with_pg_test_client(&dsn, move |client| async move {
            client
                .execute(
                    "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, $2, 'published') ON CONFLICT (vol, object_id) DO UPDATE SET state = 'published'",
                    &[&volume_id, &(restored_object as i64)],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))
        }))
        .unwrap();
        let second = retained_test_engine(&dir.path().join("second"));
        sync_all_pages(&second);
        assert!(second.retained_entry(receipt).unwrap().is_some());
        if !matches!(
            order,
            OrphanFinalizeOrder::WinnerRestart | OrphanFinalizeOrder::WinnerSweep
        ) {
            let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
            let copy = second
                .create_file(staged.ino, b"open", 0, 0, 0o644)
                .unwrap();
            set_file_state(&second, copy.ino, 64, 1);
            // A complete eager copy covers exactly the file size.
            let copy_slices: Vec<_> = fixture_payload_slices(
                0,
                Some(&PayloadRef::new_partial(restored_object, 64)),
                PayloadRefState::LocalOnly,
            )
            .unwrap()
            .into_iter()
            .map(|(chunk, slices)| {
                let data: Vec<_> = slices.into_iter().filter(|s| s.payload.is_some()).collect();
                (chunk, data)
            })
            .collect();
            second
                .flush_regular_file_overlay_slices(copy.ino, &copy_slices)
                .unwrap();
            second
                .restore_retained_entry(
                    receipt,
                    ROOT_INO,
                    b"restored",
                    b"stage",
                    &retained_test_source_fences(&second, receipt),
                )
                .unwrap();
            let epoch = second.get_attr(child.ino).unwrap().content_epoch;
            mark_overlay_objects_uploaded(&second, child.ino, epoch, &[restored_object]);
            assert!(
                second
                    .publish_regular_file_overlay_to_base_if_matches(child.ino, epoch)
                    .unwrap()
            );
            publish_and_pull(&second);
        }

        // The source handle opened before Move to Trash keeps the purged inode orphaned.
        assert!(matches!(
            first
                .purge_retained_child(receipt, payload.ino, b"open", child.ino, false)
                .unwrap(),
            RetainedChildPurgeOutcome::Removed(_)
        ));
        assert_eq!(first.get_attr(child.ino).unwrap().nlink, 0);
        let first = match order {
            OrphanFinalizeOrder::CloseBefore
            | OrphanFinalizeOrder::WinnerRestart
            | OrphanFinalizeOrder::WinnerSweep => {
                assert_eq!(
                    first.finalize_orphan_inode(child.ino, 1).unwrap(),
                    OrphanInodeFinalization::Deferred,
                    "{order:?}: an unacknowledged retained child purge must not finalize"
                );
                first
            }
            OrphanFinalizeOrder::RestartBefore => {
                drop(first);
                let first = retained_test_engine(&first_path);
                first.recover_undo_logs().unwrap();
                assert_eq!(first.get_attr(child.ino).unwrap().nlink, 0);
                first
            }
            OrphanFinalizeOrder::CloseAfter => first,
        };
        first.request_eager_flush();
        let _ = first.flush_due_pending();
        for _ in 0..3 {
            publish_and_pull(&first);
        }
        match order {
            OrphanFinalizeOrder::CloseAfter => assert_eq!(
                first.finalize_orphan_inode(child.ino, 1).unwrap(),
                OrphanInodeFinalization::Live,
                "a reconciled losing purge leaves a live binding"
            ),
            OrphanFinalizeOrder::WinnerSweep => {
                let now = 1_u64 << 40;
                // A still-open source handle keeps the orphan for its own last close.
                first.note_open_handle(child.ino, true);
                assert_eq!(
                    first
                        .run_retained_lifecycle_once(now)
                        .unwrap()
                        .orphans_finalized,
                    0
                );
                assert_eq!(first.get_attr(child.ino).unwrap().nlink, 0);
                first.note_open_handle(child.ino, false);
                assert_eq!(
                    first
                        .run_retained_lifecycle_once(now)
                        .unwrap()
                        .orphans_finalized,
                    1,
                    "the running sweep finalizes an acknowledged winner without a restart"
                );
                assert!(first.get_attr(child.ino).is_err());
                for _ in 0..2 {
                    publish_and_pull(&first);
                }
                assert!(pg_event_touches_ino(&dsn, volume_id, child.ino));
                assert!(
                    first
                        .get_pending_deletions()
                        .unwrap()
                        .iter()
                        .any(|(id, _)| *id == object_id)
                );
                assert_eq!(
                    first
                        .run_retained_lifecycle_once(now)
                        .unwrap()
                        .orphans_finalized,
                    0
                );
                continue;
            }
            OrphanFinalizeOrder::WinnerRestart => {
                drop(first);
                let reopened = retained_test_engine(&first_path);
                reopened.recover_undo_logs().unwrap();
                assert!(
                    reopened.get_attr(child.ino).is_err(),
                    "an acknowledged winning purge finalizes on restart replay"
                );
                for _ in 0..2 {
                    publish_and_pull(&reopened);
                }
                assert!(
                    pg_event_touches_ino(&dsn, volume_id, child.ino),
                    "the winner's orphan finalization reaches shared metadata"
                );
                assert!(
                    reopened
                        .get_pending_deletions()
                        .unwrap()
                        .iter()
                        .any(|(id, _)| *id == object_id)
                );
                continue;
            }
            _ => {}
        }
        for _ in 0..2 {
            publish_and_pull(&first);
        }
        sync_all_pages(&second);

        let check = |first: &PgMetaEngine| {
            assert!(
                first.flushed_event_seq.load(Ordering::SeqCst)
                    >= first.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
                "{order:?}: the losing purge must retire and the journal drain"
            );
            for engine in [first, &*second] {
                assert!(matches!(
                    engine.retained_entry(receipt).unwrap().unwrap().state,
                    RetainedEntryState::Restored { .. }
                ));
                assert_eq!(
                    engine.lookup(payload.ino, b"open").unwrap().ino,
                    child.ino,
                    "{order:?}: the restored child must stay bound"
                );
                let attr = engine.get_attr(child.ino).unwrap();
                assert_eq!((attr.nlink, attr.size), (1, 64), "{order:?}");
                assert_eq!(
                    lookup_first_object_id(engine, child.ino).unwrap(),
                    Some(restored_object),
                    "{order:?}: the restored child keeps its bytes"
                );
            }
            assert!(
                !first
                    .get_pending_deletions()
                    .unwrap()
                    .iter()
                    .any(|(id, _)| *id == restored_object),
                "{order:?}: a live object must not be queued for deletion"
            );
        };
        check(&first);
        assert!(
            !pg_event_touches_ino(&dsn, volume_id, child.ino),
            "{order:?}: no orphan finalization may reach shared metadata"
        );
        assert_eq!(
            first
                .claim_uploaded_object_deletion(restored_object, "retained-orphan-close")
                .unwrap(),
            UploadedObjectDeletionClaim::Protected,
            "{order:?}: the restored child's current reference protects its object"
        );
        drop(first);
        let first = retained_test_engine(&first_path);
        first.recover_undo_logs().unwrap();
        sync_all_pages(&first);
        check(&first);
    }
}

#[test]
#[ignore = "requires PostgreSQL; a local restore must not hide an unacknowledged child purge from the orphan guard"]
fn test_pg_retained_child_purge_then_local_restore_defers_orphan_finalize() {
    use crate::engine::{MetaRecovery, OrphanInodeFinalization};
    use crate::snapshot_ports::{UploadedFragmentLedger, UploadedObjectDeletionClaim};
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    // `true`: a source handle stays open and its last close finalizes; `false`:
    // no handle, so the running lifecycle sweep finalizes.
    for (open_handle, object_id) in [(true, 64_001_u64), (false, 64_002)] {
        let volume = PgTestVolume::new(&dsn, "retained-purge-local-restore");
        let volume_id = volume.volume_id;
        let dir = studiofs_test_support::workspace_target_tempdir("retained-purge-local-restore-");
        let first_path = dir.path().join("first");
        seed_local(&first_path, &volume.format_info());
        seed_local(&dir.path().join("second"), &volume.format_info());
        let first = retained_test_engine(&first_path);
        first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let child = create_published_small_file(&first, payload.ino, b"open", object_id);
        let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = first.next_inode().unwrap();
        first
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        publish_and_pull(&first);
        let restored_object = object_id + 100;
        run_async(with_pg_test_client(&dsn, move |client| async move {
            client
                .execute(
                    "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, $2, 'published') ON CONFLICT (vol, object_id) DO UPDATE SET state = 'published'",
                    &[&volume_id, &(restored_object as i64)],
                )
                .await
                .map_err(|error| MetaError::Storage(error.to_string()))
        }))
        .unwrap();
        // B restores R first and publishes: its restore wins.
        let second = retained_test_engine(&dir.path().join("second"));
        sync_all_pages(&second);
        let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        let copy = second
            .create_file(staged.ino, b"open", 0, 0, 0o644)
            .unwrap();
        set_file_state(&second, copy.ino, 64, 1);
        let copy_slices: Vec<_> = fixture_payload_slices(
            0,
            Some(&PayloadRef::new_partial(restored_object, 64)),
            PayloadRefState::LocalOnly,
        )
        .unwrap()
        .into_iter()
        .map(|(chunk, slices)| {
            let data: Vec<_> = slices.into_iter().filter(|s| s.payload.is_some()).collect();
            (chunk, data)
        })
        .collect();
        second
            .flush_regular_file_overlay_slices(copy.ino, &copy_slices)
            .unwrap();
        second
            .restore_retained_entry(
                receipt,
                ROOT_INO,
                b"restored",
                b"stage",
                &retained_test_source_fences(&second, receipt),
            )
            .unwrap();
        let epoch = second.get_attr(child.ino).unwrap().content_epoch;
        mark_overlay_objects_uploaded(&second, child.ino, epoch, &[restored_object]);
        assert!(
            second
                .publish_regular_file_overlay_to_base_if_matches(child.ino, epoch)
                .unwrap()
        );
        publish_and_pull(&second);

        // A (offline from B's restore) purges the child, then restores R locally:
        // the purged child's old parent leaves the receipt subtree.
        assert!(matches!(
            first
                .purge_retained_child(receipt, payload.ino, b"open", child.ino, false)
                .unwrap(),
            RetainedChildPurgeOutcome::Removed(_)
        ));
        first.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        first
            .restore_retained_entry(
                receipt,
                ROOT_INO,
                b"mine",
                b"stage",
                &retained_test_source_fences(&first, receipt),
            )
            .unwrap();
        assert_ne!(
            first.get_attr(payload.ino).unwrap().parent,
            receipt,
            "the local restore moved the purge parent out of the receipt"
        );
        if open_handle {
            assert_eq!(
                first.finalize_orphan_inode(child.ino, 1).unwrap(),
                OrphanInodeFinalization::Deferred,
                "last close: an unacknowledged child purge must defer after a local restore"
            );
        } else {
            assert_eq!(
                first
                    .run_retained_lifecycle_once(1_u64 << 40)
                    .unwrap()
                    .orphans_finalized,
                0,
                "sweep: an unacknowledged child purge must defer after a local restore"
            );
        }
        assert!(first.local.get_attr(child.ino).is_ok());

        first.request_eager_flush();
        let _ = first.flush_due_pending();
        for _ in 0..3 {
            publish_and_pull(&first);
        }
        if open_handle {
            let _ = first.finalize_orphan_inode(child.ino, 1).unwrap();
        } else {
            let _ = first.run_retained_lifecycle_once(1_u64 << 40).unwrap();
        }
        for _ in 0..2 {
            publish_and_pull(&first);
        }
        sync_all_pages(&second);
        assert!(
            !pg_event_touches_ino(&dsn, volume_id, child.ino),
            "open_handle={open_handle}: no orphan finalization may reach shared metadata"
        );
        assert_eq!(
            second.lookup(payload.ino, b"open").unwrap().ino,
            child.ino,
            "open_handle={open_handle}: B's restored child stays bound"
        );
        let attr = second.get_attr(child.ino).unwrap();
        assert_eq!((attr.nlink, attr.size), (1, 64));
        assert_eq!(
            lookup_first_object_id(&second, child.ino).unwrap(),
            Some(restored_object)
        );
        assert_eq!(
            first
                .claim_uploaded_object_deletion(restored_object, "retained-purge-local-restore")
                .unwrap(),
            UploadedObjectDeletionClaim::Protected,
            "open_handle={open_handle}: B's current reference protects its bytes"
        );
    }
}

#[test]
#[ignore = "requires PostgreSQL; one failing deferred orphan must not stop the sweep for the others"]
fn test_pg_retained_lifecycle_sweep_continues_past_failing_orphan() {
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-sweep-poison");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-sweep-poison-");
    let path = dir.path().join("first");
    seed_local(&path, &volume.format_info());
    let first = retained_test_engine(&path);
    first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let poison = create_published_small_file(&first, payload.ino, b"poison", 65_001);
    let healthy = create_published_small_file(&first, payload.ino, b"healthy", 65_002);
    let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&first);
    for (name, ino) in [(&b"poison"[..], poison.ino), (&b"healthy"[..], healthy.ino)] {
        assert!(matches!(
            first
                .purge_retained_child(receipt, payload.ino, name, ino, false)
                .unwrap(),
            RetainedChildPurgeOutcome::Removed(_)
        ));
    }
    first.request_eager_flush();
    let _ = first.flush_due_pending();
    for _ in 0..3 {
        publish_and_pull(&first);
    }
    first.local.corrupt_inode_row_for_test(poison.ino);
    let sweep = first.run_retained_lifecycle_once(1_u64 << 40).unwrap();
    assert_eq!(
        (sweep.orphans_finalized, sweep.orphan_finalize_errors),
        (1, 1),
        "the healthy acknowledged orphan finalizes despite the failing one"
    );
    assert!(first.get_attr(healthy.ino).is_err());
    let remaining: Vec<_> = crate::engine::MetaRecovery::list_deferred_deletes(&*first.local)
        .unwrap()
        .into_iter()
        .map(|record| record.ino)
        .collect();
    assert_eq!(
        remaining,
        vec![poison.ino],
        "the failing record is kept, nothing deleted"
    );
}

#[test]
#[ignore = "requires PostgreSQL; an accepted SetAttr before a losing child purge must stay visible"]
fn test_pg_retained_losing_child_purge_keeps_pre_purge_set_attr_visible() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome, RetainedEntryState,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-child-purge-setattr");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-child-purge-setattr-");
    let first_path = dir.path().join("first");
    seed_local(&first_path, &volume.format_info());
    seed_local(&dir.path().join("second"), &volume.format_info());
    let first = retained_test_engine(&first_path);
    first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let doomed = first
        .create_dir(payload.ino, b"doomed", 0, 0, 0o700)
        .unwrap();
    let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&first);
    let second = retained_test_engine(&dir.path().join("second"));
    sync_all_pages(&second);
    assert!(second.retained_entry(receipt).unwrap().is_some());
    let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
    second
        .create_dir(staged.ino, b"doomed", 0, 0, 0o700)
        .unwrap();
    second
        .restore_retained_entry(
            receipt,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&second, receipt),
        )
        .unwrap();
    publish_and_pull(&second);

    // An accepted, unpublished chmod precedes the losing child purge.
    let mut attr = first.get_attr(doomed.ino).unwrap();
    attr.mode = (attr.mode & !0o7777) | 0o750;
    first.set_attr(&attr).unwrap();
    assert!(matches!(
        first
            .purge_retained_child(receipt, payload.ino, b"doomed", doomed.ino, true)
            .unwrap(),
        RetainedChildPurgeOutcome::Removed(_)
    ));
    first.request_eager_flush();
    let _ = first.flush_due_pending();
    // The loser retired; the earlier chmod is the child's local overlay again.
    let local = first.local.get_attr(doomed.ino).unwrap();
    assert_eq!(
        local.mode & 0o7777,
        0o750,
        "the pre-purge SetAttr must not be lost"
    );
    assert_eq!(
        first.local.lookup(payload.ino, b"doomed").unwrap().ino,
        doomed.ino,
        "a losing child purge must not hide the restored child locally"
    );
    for _ in 0..3 {
        publish_and_pull(&first);
    }
    sync_all_pages(&second);
    assert!(
        first.flushed_event_seq.load(Ordering::SeqCst)
            >= first.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
        "the journal must drain"
    );
    for engine in [&first, &second] {
        assert!(matches!(
            engine.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Restored { .. }
        ));
        assert_eq!(
            engine.lookup(payload.ino, b"doomed").unwrap().ino,
            doomed.ino
        );
        assert_eq!(engine.get_attr(doomed.ino).unwrap().mode & 0o7777, 0o750);
    }
}

#[test]
#[ignore = "requires PostgreSQL; a losing closed-file purge keeps its local graph and accepted overlay bytes"]
fn test_pg_retained_losing_file_purge_keeps_local_graph_and_overlay() {
    use crate::engine::{MetaRecovery, OrphanInodeFinalization};
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome, RetainedEntryState,
    };
    const BASE: u64 = 63_001;
    const PREWRITE: u64 = 63_002;
    const RESTORED: u64 = 63_003;
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-file-purge-overlay");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-file-purge-overlay-");
    let first_path = dir.path().join("first");
    seed_local(&first_path, &volume.format_info());
    seed_local(&dir.path().join("second"), &volume.format_info());
    let first = retained_test_engine(&first_path);
    first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let child = create_published_small_file(&first, payload.ino, b"file", BASE);
    let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&first);

    let second = retained_test_engine(&dir.path().join("second"));
    sync_all_pages(&second);
    let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
    let copy = second
        .create_file(staged.ino, b"file", 0, 0, 0o644)
        .unwrap();
    set_file_state(&second, copy.ino, 64, 1);
    let exact = |object_id: u64| -> Vec<(ChunkId, Vec<SliceRecord>)> {
        fixture_payload_slices(
            0,
            Some(&PayloadRef::new_partial(object_id, 64)),
            PayloadRefState::LocalOnly,
        )
        .unwrap()
        .into_iter()
        .map(|(chunk, slices)| {
            let data: Vec<_> = slices.into_iter().filter(|s| s.payload.is_some()).collect();
            (chunk, data)
        })
        .collect()
    };
    second
        .flush_regular_file_overlay_slices(copy.ino, &exact(RESTORED))
        .unwrap();
    second
        .restore_retained_entry(
            receipt,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&second, receipt),
        )
        .unwrap();
    let epoch = second.get_attr(child.ino).unwrap().content_epoch;
    mark_overlay_objects_uploaded(&second, child.ino, epoch, &[RESTORED]);
    assert!(
        second
            .publish_regular_file_overlay_to_base_if_matches(child.ino, epoch)
            .unwrap()
    );
    publish_and_pull(&second);

    // An accepted, uploaded but unpublished overwrite precedes the closed-file purge.
    set_file_state(&first, child.ino, 64, 2);
    first
        .flush_regular_file_overlay_slices(child.ino, &exact(PREWRITE))
        .unwrap();
    mark_overlay_objects_uploaded(&first, child.ino, 2, &[PREWRITE]);
    assert!(matches!(
        first
            .purge_retained_child(receipt, payload.ino, b"file", child.ino, false)
            .unwrap(),
        RetainedChildPurgeOutcome::Removed(_)
    ));
    // The VFS no-handle path finalizes through the same guard.
    assert_eq!(
        first.finalize_orphan_inode(child.ino, 1).unwrap(),
        OrphanInodeFinalization::Deferred,
        "a closed-file purge must keep its graph until shared authority settles it"
    );
    first.request_eager_flush();
    let _ = first.flush_due_pending();
    // Before any pull: the loser is reconciled and the accepted bytes are local again.
    assert_eq!(
        first.local.lookup(payload.ino, b"file").unwrap().ino,
        child.ino,
        "a losing file purge must not hide the restored child"
    );
    assert_eq!(
        lookup_first_object_id(&first, child.ino).unwrap(),
        Some(PREWRITE),
        "the accepted overwrite must stay in the local graph"
    );
    let pending = |engine: &PgMetaEngine| {
        engine
            .get_pending_deletions()
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>()
    };
    assert!(
        !pending(&first).contains(&PREWRITE),
        "GC must not be offered the kept overlay object"
    );
    // The uploader then publishes the accepted overlay, as it would after any write.
    let epoch = first.get_attr(child.ino).unwrap().content_epoch;
    assert!(
        first
            .publish_regular_file_overlay_to_base_if_matches(child.ino, epoch)
            .unwrap(),
        "the kept overlay must still be publishable"
    );
    for _ in 0..3 {
        publish_and_pull(&first);
    }
    sync_all_pages(&second);
    assert!(
        first.flushed_event_seq.load(Ordering::SeqCst)
            >= first.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
        "the journal must drain"
    );
    let first_object = lookup_first_object_id(&first, child.ino).unwrap();
    for engine in [&first, &second] {
        assert!(matches!(
            engine.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Restored { .. }
        ));
        assert_eq!(engine.lookup(payload.ino, b"file").unwrap().ino, child.ino);
        assert_eq!(engine.get_attr(child.ino).unwrap().nlink, 1);
        assert_eq!(
            lookup_first_object_id(engine, child.ino).unwrap(),
            first_object,
            "both clients converge on the same bytes"
        );
    }
    assert!(
        !pending(&first).contains(&first_object.unwrap()),
        "the converged object must not be queued for deletion"
    );
    assert!(!pg_event_touches_ino(&dsn, volume.volume_id, child.ino));
    assert_eq!(first_object, Some(PREWRITE), "the accepted overwrite wins");
}

#[test]
#[ignore = "requires PostgreSQL; a losing purge restores names bound by an earlier local rename or add"]
fn test_pg_retained_losing_child_purge_restores_pre_purge_rename_and_add() {
    use crate::trash::{
        MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome, RetainedEntryState,
    };
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-child-purge-rename");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-child-purge-rename-");
    let first_path = dir.path().join("first");
    seed_local(&first_path, &volume.format_info());
    seed_local(&dir.path().join("second"), &volume.format_info());
    let first = retained_test_engine(&first_path);
    first.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let payload = first.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let moved = first.create_dir(payload.ino, b"a", 0, 0, 0o700).unwrap();
    let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();
    publish_and_pull(&first);
    let second = retained_test_engine(&dir.path().join("second"));
    sync_all_pages(&second);
    let staged = second.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
    second.create_dir(staged.ino, b"a", 0, 0, 0o700).unwrap();
    second
        .restore_retained_entry(
            receipt,
            ROOT_INO,
            b"restored",
            b"stage",
            &retained_test_source_fences(&second, receipt),
        )
        .unwrap();
    publish_and_pull(&second);

    // Accepted local namespace changes precede the losing purges.
    first.rename(payload.ino, b"a", payload.ino, b"b").unwrap();
    let added = first.create_dir(payload.ino, b"new", 0, 0, 0o700).unwrap();
    for (name, ino) in [(&b"b"[..], moved.ino), (b"new", added.ino)] {
        assert!(matches!(
            first
                .purge_retained_child(receipt, payload.ino, name, ino, true)
                .unwrap(),
            RetainedChildPurgeOutcome::Removed(_)
        ));
    }
    first.request_eager_flush();
    let _ = first.flush_due_pending();
    let local = &first.local;
    assert_eq!(
        local.lookup(payload.ino, b"b").unwrap().ino,
        moved.ino,
        "the renamed child must be restored at its new name"
    );
    assert!(
        local.lookup(payload.ino, b"a").is_err(),
        "the old name stays renamed away"
    );
    assert_eq!(
        local.lookup(payload.ino, b"new").unwrap().ino,
        added.ino,
        "the locally added child must be restored"
    );
    for _ in 0..3 {
        publish_and_pull(&first);
    }
    sync_all_pages(&second);
    assert!(
        first.flushed_event_seq.load(Ordering::SeqCst)
            >= first.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
        "the journal must drain"
    );
    for engine in [&first, &second] {
        assert!(matches!(
            engine.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Restored { .. }
        ));
        assert_eq!(engine.lookup(payload.ino, b"b").unwrap().ino, moved.ino);
        assert!(engine.lookup(payload.ino, b"a").is_err());
        assert_eq!(engine.lookup(payload.ino, b"new").unwrap().ino, added.ino);
    }
}

#[test]
fn retained_child_purge_seq_survives_repair_checkpoint_import_and_defers_orphan_finalize() {
    use crate::engine::{MetaRecovery, OrphanInodeFinalization};
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome};

    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.local.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let payload = pg.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
    let child = create_published_small_file(&pg, payload.ino, b"open", 66_001);
    let store = pg.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let receipt = pg.next_inode().unwrap();
    pg.retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
        .unwrap();

    // Simulate everything through the retention having been published and checkpointed.
    let checkpoint = pg
        .local
        .begin_serving_checkpoint_snapshot()
        .unwrap()
        .export_serving_checkpoint()
        .unwrap();
    let flushed = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(flushed).unwrap();

    // Unacknowledged child purge keeps a deferred delete and raises LOCAL_RETAINED_CHILD_PURGE_SEQ.
    assert!(matches!(
        pg.purge_retained_child(receipt, payload.ino, b"open", child.ino, false)
            .unwrap(),
        RetainedChildPurgeOutcome::Removed(_)
    ));

    // Repair-style snapshot import keeps the local event journal and deferred deletes,
    // and replays unacked events back into serving state.
    pg.import_serving_checkpoint_locked(
        StartupSnapshotImportMode::ReplaceLocalStatePreservingEventLog,
        &checkpoint,
        0,
    )
    .unwrap();
    pg.restore_pending_state_from_local_log().unwrap();

    // Guarded finalization with UnackedChildPurges must defer rather than dropping child payload.
    assert_eq!(
        pg.finalize_orphan_inode(child.ino, 1).unwrap(),
        OrphanInodeFinalization::Deferred,
        "repair checkpoint import + replay must preserve LOCAL_RETAINED_CHILD_PURGE_SEQ so guarded finalize defers"
    );
    assert_ne!(
        pg.local
            .get_counter(crate::schema::counters::LOCAL_RETAINED_CHILD_PURGE_SEQ)
            .unwrap(),
        0,
        "LOCAL_RETAINED_CHILD_PURGE_SEQ must survive ReplaceLocalStatePreservingEventLog"
    );
    assert_eq!(
        lookup_first_object_id(&pg, child.ino).unwrap(),
        Some(66_001),
        "child payload must stay intact while its purge span is unacknowledged"
    );
    assert!(
        MetaRecovery::list_deferred_deletes(&*pg.local)
            .unwrap()
            .iter()
            .any(|record| record.ino == child.ino),
        "deferred delete record must remain for the next sweep"
    );
    let remote_snapshot = pg.local.export_remote_snapshot().unwrap();
    assert!(
        !remote_snapshot
            .counters
            .iter()
            .any(|(name, _)| name == crate::schema::counters::LOCAL_RETAINED_CHILD_PURGE_SEQ),
        "client-local purge sequence counter must not be exported in remote checkpoints"
    );
}

#[test]
#[ignore = "requires PostgreSQL; >batch failing/Deferred low inodes must not starve healthy high inodes"]
fn test_pg_retained_lifecycle_sweep_advances_past_deferred_and_failing_low_inodes() {
    use crate::engine::MetaRecovery;
    use crate::trash::{MetaRetainedStore, MetaVolumeSettings, RetainedChildPurgeOutcome};

    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
    let volume = PgTestVolume::new(&dsn, "retained-sweep-fairness");
    let dir = studiofs_test_support::workspace_target_tempdir("retained-sweep-fairness-");
    let path = dir.path().join("first");
    seed_local(&path, &volume.format_info());
    let first = retained_test_engine(&path);
    first.compare_and_set_trash_days(0, 7).unwrap().unwrap();

    // Allocate 1,020 low inodes first so all synthetic poison and unacked deferred
    // inodes stay strictly below `healthy.ino`.
    let poison_inos = first.next_inode_ids(1_020).unwrap();
    let unacked_dir = first
        .create_dir(ROOT_INO, b"unacked-source", 0, 0, 0o700)
        .unwrap();
    let mut unacked_files = Vec::new();
    for idx in 0..10 {
        let name = format!("unacked-{idx}");
        let file = create_published_small_file(
            &first,
            unacked_dir.ino,
            name.as_bytes(),
            65_100 + idx as u64,
        );
        unacked_files.push((name, file));
    }
    let healthy_dir = first
        .create_dir(ROOT_INO, b"healthy-source", 0, 0, 0o700)
        .unwrap();
    let healthy = create_published_small_file(&first, healthy_dir.ino, b"healthy", 65_200);
    assert!(poison_inos.iter().all(|&ino| ino < healthy.ino));
    assert!(unacked_files.iter().all(|(_, f)| f.ino < healthy.ino));

    let store = first.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
    let r_unacked = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"unacked-source", store.ino, r_unacked)
        .unwrap();
    let r_healthy = first.next_inode().unwrap();
    first
        .retain_namespace_entry(ROOT_INO, b"healthy-source", store.ino, r_healthy)
        .unwrap();
    publish_and_pull(&first);

    // Purge `healthy` and acknowledge its purge in PG so it is ready to finalize.
    assert!(matches!(
        first
            .purge_retained_child(r_healthy, healthy_dir.ino, b"healthy", healthy.ino, false)
            .unwrap(),
        RetainedChildPurgeOutcome::Removed(_)
    ));
    first.request_eager_flush();
    let _ = first.flush_due_pending();
    for _ in 0..3 {
        publish_and_pull(&first);
    }

    // Purge the 10 low `unacked` files locally without publishing so their guard stays Deferred.
    for (name, file) in &unacked_files {
        assert!(matches!(
            first
                .purge_retained_child(r_unacked, unacked_dir.ino, name.as_bytes(), file.ino, false)
                .unwrap(),
            RetainedChildPurgeOutcome::Removed(_)
        ));
    }
    first
        .local
        .insert_failing_deferred_deletes_for_test(&poison_inos);

    // Total deferred records below `healthy` = 1,020 failing + 10 Deferred = 1,030 > batch (1,024).
    let sweep1 = first.run_retained_lifecycle_once(1_u64 << 40).unwrap();
    assert_eq!(sweep1.orphans_finalized, 0);
    assert_eq!(sweep1.orphan_finalize_errors, 1_020);
    assert!(
        first.get_attr(healthy.ino).is_ok(),
        "healthy inode sits past the first batch"
    );

    // Pass 2 must resume past pass 1's cursor, finalize `healthy`, keep bounded work,
    // and never prematurely drop the unacked Deferred purges.
    let sweep2 = first.run_retained_lifecycle_once(1_u64 << 40).unwrap();
    assert_eq!(
        sweep2.orphans_finalized, 1,
        "second sweep must advance past >batch failing/Deferred low inodes and finalize healthy high inode"
    );
    assert_eq!(
        sweep2.orphan_finalize_errors, 1_017,
        "second sweep must cap total visited candidates to ORPHAN_FINALIZE_BATCH (6 unacked + 1 healthy + 1017 poison = 1024)"
    );
    assert!(
        first.get_attr(healthy.ino).is_err(),
        "acknowledged healthy orphan must be finalized"
    );
    let remaining: Vec<_> = MetaRecovery::list_deferred_deletes(&*first.local)
        .unwrap()
        .into_iter()
        .map(|record| record.ino)
        .collect();
    assert!(!remaining.contains(&healthy.ino));
    assert_eq!(
        remaining.len(),
        1_030,
        "failing and unacknowledged Deferred records stay protected"
    );
    for (_, file) in &unacked_files {
        let attr = first
            .local
            .get_attr(file.ino)
            .expect("unacked deferred orphan must keep its inode until finalized");
        assert_eq!(attr.nlink, 0);
        assert!(
            lookup_first_object_id(&first, file.ino).unwrap().is_some(),
            "unacked deferred orphan {} must keep its payload",
            file.ino
        );
    }
}

#[test]
fn deferred_orphan_sweep_batch_wraps_and_respects_concurrent_open_handle_recheck() {
    use crate::engine::MetaRecovery;

    let (_dir, pg) = temp_pg_engine_for_unit();
    // 5 failing low inodes + 1 healthy high inode (5 is > batch_limit=3).
    pg.local
        .insert_failing_deferred_deletes_for_test(&[100, 101, 102, 103, 104]);
    let dir = pg.create_dir(ROOT_INO, b"src", 0, 0, 0o755).unwrap();
    let open_high = create_published_small_file(&pg, dir.ino, b"open_high", 66_100);
    let healthy_high = create_published_small_file(&pg, dir.ino, b"healthy_high", 66_101);
    pg.unlink_open_handle(dir.ino, b"open_high").unwrap();
    pg.unlink_open_handle(dir.ino, b"healthy_high").unwrap();
    // Mark `open_high` as open so the sweep must neither batch it nor finalize it.
    pg.note_open_handle(open_high.ino, true);
    // Simulate PG having already acknowledged local deletes so healthy_high is eligible.
    let seq = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(seq).unwrap();

    // Pass 1 (batch_limit = 3): visits [100, 101, 102] only (bounded work = 3).
    let mut sweep1 = crate::pg_engine::retained::RetainedLifecycleSweep::default();
    let visited1 = pg
        .finalize_deferred_orphans_batch(1_000, 3, &mut sweep1)
        .unwrap();
    assert_eq!(visited1, 3);
    assert_eq!(
        (sweep1.orphans_finalized, sweep1.orphan_finalize_errors),
        (0, 3)
    );
    assert!(pg.local.get_attr(healthy_high.ino).is_ok());

    // Pass 2 (batch_limit = 3): resumes after 102 -> visits [103, 104, healthy_high] (skipping open_high).
    let mut sweep2 = crate::pg_engine::retained::RetainedLifecycleSweep::default();
    let visited2 = pg
        .finalize_deferred_orphans_batch(1_000, 3, &mut sweep2)
        .unwrap();
    assert_eq!(visited2, 3);
    assert_eq!(
        (sweep2.orphans_finalized, sweep2.orphan_finalize_errors),
        (1, 2)
    );
    assert!(pg.local.get_attr(healthy_high.ino).is_err());

    // Add one more low healthy inode and test two concurrent sweeps sharing the cursor.
    let wrap_healthy = create_published_small_file(&pg, dir.ino, b"wrap_healthy", 66_102);
    pg.unlink_open_handle(dir.ino, b"wrap_healthy").unwrap();
    let seq = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(seq).unwrap();

    let (t1, t2) = std::thread::scope(|scope| {
        let h1 = scope.spawn(|| {
            let mut sweep = crate::pg_engine::retained::RetainedLifecycleSweep::default();
            let visited = pg
                .finalize_deferred_orphans_batch(1_000, 3, &mut sweep)
                .unwrap();
            (visited, sweep)
        });
        let h2 = scope.spawn(|| {
            let mut sweep = crate::pg_engine::retained::RetainedLifecycleSweep::default();
            let visited = pg
                .finalize_deferred_orphans_batch(1_000, 3, &mut sweep)
                .unwrap();
            (visited, sweep)
        });
        (h1.join().unwrap(), h2.join().unwrap())
    });
    assert!(t1.0 <= 3 && t2.0 <= 3);
    assert_eq!(
        t1.1.orphans_finalized + t2.1.orphans_finalized,
        1,
        "exactly one concurrent sweep finalizes wrap_healthy"
    );
    assert!(pg.local.get_attr(wrap_healthy.ino).is_err());

    let remaining: Vec<_> = MetaRecovery::list_deferred_deletes(&*pg.local)
        .unwrap()
        .into_iter()
        .map(|record| record.ino)
        .collect();
    assert!(remaining.contains(&open_high.ino));
    pg.note_open_handle(open_high.ino, false);
}
