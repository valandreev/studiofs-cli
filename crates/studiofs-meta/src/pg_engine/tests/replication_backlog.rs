use super::*;
use crate::redb_engine::layout::{encode_dentry_key, encode_xattr_key};
use crate::{LocalOutboundAckSnapshot, ReadOnlyMetaEngine};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

fn uploaded_pending_file(pg: &PgMetaEngine, name: &[u8], object_id: u64) -> InodeAttr {
    let file = pg
        .create_file(ROOT_INO, name, 0, 0, 0o644)
        .expect("create pending file");
    let payload = PayloadRef::new_partial(object_id, 152);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage pending payload");
    set_file_state(pg, file.ino, 152, 1);
    let attr = pg.get_attr(file.ino).expect("pending attr");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(
            file.ino,
            attr.content_epoch,
            payload.object_id,
        )
        .expect("mark pending payload uploaded")
    );
    attr
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn drained_out_of_order_file_publications_reopen_without_metadata_backlog() {
    let dsn = pg_dsn().expect("PostgreSQL regression requires STUDIOFS_TEST_PG_DSN");
    let _bytes = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "1024");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-drained-publications-reopen");
    seed_local(&path, &volume.format_info());
    let options = PgOpenOptions {
        background_poll: false,
        ..Default::default()
    };
    let pg = PgMetaEngine::open_with_options(&path, options).expect("open writer");
    let files = (0..32)
        .map(|index| {
            let name = format!("import-{index:03}.bin").into_bytes();
            let attr = uploaded_pending_file(&pg, &name, 90_000 + index);
            (name, attr)
        })
        .collect::<Vec<_>>();
    let mut local_tail = None;
    for (index, (_, attr)) in files.iter().rev().enumerate() {
        assert!(
            pg.publish_regular_file_overlay_to_base_if_matches(attr.ino, attr.content_epoch)
                .expect("publish uploaded file")
        );
        if index + 1 == files.len() {
            let tail = pg
                .create_file(ROOT_INO, b"local-tail.bin", 0, 0, 0o644)
                .unwrap();
            set_file_state(&pg, tail.ino, 152, 1);
            pg.stage_payload_slice(tail.ino, 0, &PayloadRef::new_partial(91_001, 152))
                .unwrap();
            local_tail = Some(tail);
        }
        if index % 4 == 3 {
            pg.flush_due_pending().expect("drain publication batch");
        }
    }
    pg.flush_due_pending().expect("finish metadata publication");
    assert_eq!(pg.replication_backlog().total_ops(), 0, "writer drained");
    let target = pg
        .remote_metadata_head_version()
        .expect("publication target");
    for _ in 0..128 {
        if pg.local_version.load(Ordering::SeqCst) >= target {
            break;
        }
        pg.sync_metadata()
            .expect("apply remaining publication pages");
    }
    assert_eq!(
        pg.local_version.load(Ordering::SeqCst),
        target,
        "writer caught up"
    );
    pg.prepare_for_close().expect("close writer workers");
    pg.flush_local_metadata().expect("persist local metadata");
    drop(pg);

    let reopened = PgMetaEngine::open_with_options(&path, options).expect("reopen writer");
    assert_eq!(
        reopened.replication_backlog().total_ops(),
        0,
        "a fully drained close must not republish previously uploaded files"
    );
    for (name, attr) in files {
        assert_eq!(reopened.lookup(ROOT_INO, &name).unwrap().ino, attr.ino);
        let restored = reopened.get_attr(attr.ino).unwrap();
        assert_eq!(restored.size, 152);
        assert_eq!(restored.committed_size, 152);
        assert_eq!(restored.upload_state, UploadState::Ready);
    }
    let tail = local_tail.unwrap();
    assert_eq!(
        reopened.lookup(ROOT_INO, b"local-tail.bin").unwrap().ino,
        tail.ino
    );
    assert_eq!(reopened.get_attr(tail.ino).unwrap().size, 152);
    assert!(
        reopened
            .local
            .export_snapshot()
            .unwrap()
            .regular_file_overlay
            .iter()
            .any(|(ino, _)| *ino == tail.ino)
    );
    reopened.prepare_for_close().expect("close reopened writer");
}

#[test]
fn split_publication_prefix_ack_waits_for_last_page_and_survives_rollback() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let start = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ, start)
        .unwrap();
    let attr = uploaded_pending_file(&pg, b"split-publish.bin", 91_000);
    pg.publish_regular_file_overlay_to_base_if_matches(attr.ino, attr.content_epoch)
        .unwrap();
    let end = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let projected = RemoteApplyCoordinator::project_local_batch(
        &pg.local.read_events_range(start, end).unwrap(),
    );
    let split = projected.events.len() / 2;
    let prefix = vec![LocalEventRange { start, end }];
    pg.mark_local_event_ranges_published_with_prefix(&prefix, Some((2, prefix.clone())))
        .unwrap();
    let apply = |version, events| {
        pg.apply_replay_page_atomically(
            vec![DecodedReplayBatch {
                batch_version: version,
                events,
                is_self_batch: true,
            }],
            None,
            RemoteReplayApplyContext {
                last_seq: pg.local_version.load(Ordering::SeqCst),
                current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq: pg.flushed_event_seq.load(Ordering::SeqCst),
                next_event_seq: end,
                initial_overlay: pg.local_overlay_conflicts().unwrap(),
            },
        )
    };
    apply(1, projected.events[..split].to_vec()).unwrap();
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local).unwrap(),
        start
    );
    pg.set_catch_up_fault_hook(Arc::new(|point| {
        if matches!(
            point,
            CatchUpFaultPoint::BeforeLocalCommit {
                last_batch_version: 2
            }
        ) {
            Err(MetaError::Storage(
                "injected publication page rollback".into(),
            ))
        } else {
            Ok(())
        }
    }));
    assert!(apply(2, projected.events[split..].to_vec()).is_err());
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local).unwrap(),
        start
    );
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).unwrap(),
        1
    );
    assert_eq!(
        pg.unprocessed_local_event_progress()
            .unwrap()
            .published_prefixes
            .len(),
        1
    );

    pg.set_catch_up_fault_hook(Arc::new(|_| Ok(())));
    apply(2, projected.events[split..].to_vec()).unwrap();
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local).unwrap(),
        end
    );
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).unwrap(),
        2
    );
    assert!(
        pg.unprocessed_local_event_progress()
            .unwrap()
            .published_prefixes
            .is_empty()
    );
}

#[test]
fn local_graph_repair_marker_clears_only_after_conflicting_payload_is_published() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-conflicting-name.bin";
    let file = uploaded_pending_file(&pg, name, 77_028);
    let replacement = pg
        .create_file(ROOT_INO, b"replacement.bin", 0, 0, 0o644)
        .unwrap();
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: name.to_vec(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: name.to_vec(),
                entry: crate::types::DirEntryValue {
                    ino: replacement.ino,
                    kind: replacement.kind,
                },
            },
        ])
        .unwrap();
    assert!(
        !pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .unwrap()
    );
    pg.local
        .set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)
        .unwrap();
    pg.metadata_startup_target_version
        .store(0, Ordering::Release);
    pg.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    pg.try_complete_catchup_sync().unwrap();
    assert_eq!(pg.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    assert!(!pg.pg_sync_status().allows_new_mutations());
    assert_eq!(
        pg.local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .unwrap(),
        1
    );
    assert_eq!(
        pg.local.lookup(ROOT_INO, name).unwrap().ino,
        replacement.ino
    );
    assert_eq!(
        pg.local.pending_uploaded_regular_file_recoveries().unwrap()[0]
            .attr
            .ino,
        file.ino
    );
    assert!(
        pg.local
            .get_referenced_object_ids()
            .unwrap()
            .contains(&77_028)
    );
    assert_eq!(
        pg.metadata_startup_target_version.load(Ordering::Acquire),
        -1
    );
    assert!(
        pg.local
            .publish_regular_file_overlay_to_base_if_matches(file.ino, file.content_epoch)
            .unwrap()
    );
    assert!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .unwrap()
            .is_empty()
    );
    pg.metadata_startup_target_version
        .store(1, Ordering::Release);
    pg.local_version.store(1, Ordering::Release);
    pg.try_complete_catchup_sync().unwrap();
    assert_eq!(pg.pg_sync_status().lifecycle, PgSyncLifecycle::Live);
    assert!(pg.pg_sync_status().allows_new_mutations());
    assert_eq!(
        pg.local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .unwrap(),
        0
    );
    assert_eq!(
        pg.local.lookup(ROOT_INO, name).unwrap().ino,
        replacement.ino
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, file.size).unwrap(),
        vec![77_028]
    );
}

fn reject_oversized_test_event(pg: &mut PgMetaEngine, name: &[u8]) -> u64 {
    pg.remote_format_validated.store(true, Ordering::Release);
    pg.sync_max_response_bytes_per_page = 1;
    let cursor = pg.flushed_event_seq.load(Ordering::SeqCst);
    pg.set_xattr(ROOT_INO, b"user.sfs726.oversized", name, 0)
        .expect("append one oversized event");
    let rejected = pg
        .claim_pending_batch(true)
        .expect("oversized event should be claimable");
    let error = match pg.flush_pending_claimed_batch_with_restore(rejected) {
        Ok(_) => panic!("oversized event must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, MetaError::EventTooLarge { .. }));
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), cursor);
    cursor
}

#[test]
fn flush_due_pending_skips_remote_validation_when_no_batch_is_due() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.flush_due_pending()
        .expect("idle flush must not contact unavailable PostgreSQL");
    assert!(
        !pg.remote_format_validated.load(Ordering::Acquire),
        "idle flush must leave the pending format validation state unchanged"
    );
}

#[test]
fn local_event_pruning_keeps_rejected_and_unprocessed_suffix_without_postgres() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"prune-acknowledged", 0, 0, 0o755)
        .expect("create acknowledged directory");
    let cursor = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read acknowledged end");
    pg.persist_flushed_event_seq(cursor)
        .expect("persist acknowledged local cursor");

    pg.create_file(ROOT_INO, b"prune-rejected", 0, 0, 0o644)
        .expect("create rejected suffix");
    let completed_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read completed suffix end");
    assert!(completed_end > cursor + 1);
    pg.reject_local_event_ranges(&[LocalEventRange {
        start: cursor,
        end: cursor + 1,
    }])
    .expect("mark the first suffix event rejected");
    pg.complete_local_event_ranges(&[LocalEventRange {
        start: cursor + 1,
        end: completed_end,
    }])
    .expect("complete later suffix events");
    pg.create_dir(ROOT_INO, b"prune-unprocessed", 0, 0, 0o755)
        .expect("create unprocessed suffix");
    let next_event_seq = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read next event sequence");

    let pruned = pg
        .prune_local_event_log_once()
        .expect("prune acknowledged local event prefix");

    assert!(pruned.rows > 0);
    assert_eq!(pg.local.first_event_seq().unwrap(), Some(cursor));
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), cursor);
    assert_eq!(
        pg.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
        next_event_seq,
        "pruning must not renumber local event sequences"
    );
    assert!(
        !pg.remote_format_validated.load(Ordering::Acquire),
        "local pruning must not require PostgreSQL validation"
    );
}

#[test]
fn pruned_acknowledged_prefix_reopens_with_unacknowledged_journal_suffix() {
    let (dir, pg) = temp_pg_engine_for_unit();
    for name in [b"acknowledged-a".as_slice(), b"acknowledged-b"] {
        pg.create_dir(ROOT_INO, name, 0, 0, 0o755)
            .expect("create acknowledged directory");
    }
    let suffix_start = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(suffix_start)
        .expect("persist acknowledged prefix cursor");
    pg.create_dir(ROOT_INO, b"unacknowledged", 0, 0, 0o755)
        .expect("create unacknowledged suffix");
    let suffix_end = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let expected_suffix = pg
        .local
        .read_events_range(suffix_start, suffix_end)
        .expect("read unacknowledged suffix before prune");
    assert!(!expected_suffix.is_empty());

    let pruned = pg
        .prune_local_event_log_once()
        .expect("prune acknowledged prefix");
    assert!(pruned.rows > 0);
    assert_eq!(pg.local.first_event_seq().unwrap(), Some(suffix_start));
    drop(pg);

    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..Default::default()
        },
    )
    .expect("reopen local metadata without PostgreSQL");
    let cursor = reopened.flushed_event_seq.load(Ordering::SeqCst);
    assert_eq!(cursor, suffix_start);
    let restored_suffix = reopened
        .local
        .read_events_range(cursor, suffix_end)
        .expect("read retained suffix after reopen");
    assert_eq!(restored_suffix, expected_suffix);

    {
        let mut state = reopened.pending_state.lock().unwrap();
        *state = PendingReplicationState::default();
    }
    reopened
        .restore_pending_state_from_local_log()
        .expect("restore pending suffix after restart");
    let state = reopened.pending_state.lock().unwrap();
    assert!(state.force_flush);
    assert_eq!(state.event_start, Some(suffix_start));
    assert_eq!(state.event_end, suffix_end);
}

#[test]
fn local_event_pruning_does_not_wait_for_pending_replication_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let pg = Arc::new(pg);
    let pending_guard = pg.pending_state.lock().unwrap();
    let worker_pg = Arc::clone(&pg);
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        sender.send(worker_pg.prune_local_event_log_once()).unwrap();
    });

    let completed_without_pending = receiver.recv_timeout(Duration::from_secs(2));
    let completed_on_time = completed_without_pending.is_ok();
    drop(pending_guard);
    let result = match completed_without_pending {
        Ok(result) => result,
        Err(_) => receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("prune worker must finish after pending state is released"),
    };
    worker.join().unwrap();

    assert!(
        completed_on_time,
        "pruning only needs the durable cursor and must not hold pending replication state"
    );
    result.expect("local event pruning should complete");
}

#[test]
fn background_local_event_pruning_drains_full_batches_up_to_its_budget() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let next_event_seq = 5_001;
    let event = encode_event(&MetaEvent::RemovePendingDeletion { object_id: 1 }).unwrap();
    let txn = pg.local.begin_write_txn().unwrap();
    {
        let mut events = txn
            .open_table(redb::TableDefinition::<u64, &[u8]>::new("event_log"))
            .unwrap();
        events.retain(|_, _| false).unwrap();
        for seq in 1..next_event_seq {
            events.insert(seq, event.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    pg.local
        .set_counter(NEXT_EVENT_SEQ, next_event_seq)
        .unwrap();
    pg.persist_flushed_event_seq(next_event_seq)
        .expect("persist acknowledged event cursor");

    pg.prune_local_event_log_for_poll(&AtomicBool::new(false))
        .expect("background prune should drain its bounded batch budget");

    let diagnostics = pg
        .local
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(
        diagnostics.local_event_log_pruned_rows_total,
        (LOCAL_EVENT_LOG_PRUNE_BATCH_ROWS * LOCAL_EVENT_LOG_PRUNE_MAX_BATCHES_PER_POLL) as u64
    );
    assert_eq!(pg.local.first_event_seq().unwrap(), Some(4_097));
    assert_eq!(
        pg.local.get_counter(NEXT_EVENT_SEQ).unwrap(),
        next_event_seq
    );
}

#[test]
fn background_local_event_pruning_skips_work_after_shutdown() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"shutdown-pending", 0, 0, 0o755)
        .expect("create pruneable event");
    let cursor = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(cursor)
        .expect("acknowledge pruneable event");

    pg.prune_local_event_log_for_poll(&AtomicBool::new(true))
        .expect("shutdown should end background pruning");

    assert_eq!(pg.local.first_event_seq().unwrap(), Some(1));
    assert_eq!(
        pg.local
            .local_storage_diagnostics()
            .unwrap()
            .unwrap()
            .local_event_log_pruned_rows_total,
        0
    );
}

#[test]
fn background_local_event_pruning_stops_after_short_batch() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"short-prune-batch", 0, 0, 0o755)
        .expect("create an acknowledged local event");
    let cursor = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let eligible_rows = usize::try_from(cursor - 1).unwrap();
    assert!(eligible_rows < LOCAL_EVENT_LOG_PRUNE_BATCH_ROWS);
    pg.persist_flushed_event_seq(cursor)
        .expect("acknowledge local events");

    pg.prune_local_event_log_for_poll(&AtomicBool::new(false))
        .expect("prune the short batch");

    let diagnostics = pg.local.local_storage_diagnostics().unwrap().unwrap();
    assert_eq!(
        diagnostics.last_local_event_log_prune_rows,
        Some(u64::try_from(eligible_rows).unwrap()),
        "a second empty pass would replace the short batch's last-attempt result"
    );
    assert!(pg.local.first_event_seq().unwrap().is_none());
}

#[test]
fn pending_replay_lock_blocks_local_mutation_before_commit() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let pg = Arc::new(pg);
    let worker_pg = Arc::clone(&pg);
    let started = Arc::new(Barrier::new(2));
    let worker_started = Arc::clone(&started);
    let pending_guard = pg.pending_state.lock().expect("pending_state");

    let worker = thread::spawn(move || {
        worker_started.wait();
        worker_pg.create_file(ROOT_INO, b"blocked-during-replay", 0, 0, 0o644)
    });
    started.wait();
    thread::sleep(Duration::from_millis(100));
    assert!(
        pg.local.lookup(ROOT_INO, b"blocked-during-replay").is_err(),
        "local mutation must not commit while pending serving-state replay owns the lock"
    );

    drop(pending_guard);
    worker.join().unwrap().unwrap();
}

#[test]
fn pg_pending_deletion_batch_applies_locally_in_one_transaction() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let markers = [(60, 600), (70, 700)];
    let before = pg.local.metadata_write_observability();
    let start_seq = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();

    pg.add_pending_deletions(&markers)
        .expect("apply pending deletion batch through PgMeta");

    let after = pg.local.metadata_write_observability();
    assert_eq!(
        after.begin_write.calls - before.begin_write.calls,
        2,
        "PgMeta should use one marker batch transaction plus one local-event acknowledgement"
    );
    assert_eq!(after.commit.calls - before.commit.calls, 2);
    assert_eq!(pg.get_pending_deletions().unwrap(), markers.to_vec());

    let end_seq = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let events = pg
        .local
        .read_events_range(start_seq, end_seq)
        .expect("read PgMeta pending deletion events");
    assert!(matches!(
        events.as_slice(),
        [
            MetaEvent::AddPendingDeletion {
                object_id: 60,
                timestamp: 600
            },
            MetaEvent::AddPendingDeletion {
                object_id: 70,
                timestamp: 700
            }
        ]
    ));
}

#[test]
fn pg_empty_pending_deletion_batch_does_not_acquire_pending_state_lock() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let pg = Arc::new(pg);
    let pending_guard = pg.pending_state.lock().expect("pending_state");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker_pg = Arc::clone(&pg);

    let worker = thread::spawn(move || {
        started_tx.send(()).expect("signal empty batch worker");
        result_tx
            .send(worker_pg.add_pending_deletions(&[]))
            .expect("send empty batch result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("empty batch worker should start");
    result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("empty batch should not wait for pending state")
        .expect("empty pending deletion batch");

    drop(pending_guard);
    worker.join().expect("join empty batch worker");
}

#[test]
fn restore_pending_state_acknowledges_initial_root_snapshot_event() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let root_event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read root event end");
    pg.local
        .create_dir(ROOT_INO, b"after-root", 0, 0, 0o755)
        .expect("create local directory after root setup");
    let event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read local event end");
    pg.local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ, 0)
        .expect("reset the startup cursor");
    pg.flushed_event_seq.store(0, Ordering::SeqCst);

    pg.restore_pending_state_from_local_log()
        .expect("restore startup pending state");

    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        root_event_end,
        "the root event is already represented by the initial serving checkpoint"
    );
    assert_eq!(
        pg.local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ)
            .expect("read persisted startup cursor"),
        root_event_end
    );
    let pending_events = pg
        .local
        .read_events_range(root_event_end, event_end)
        .expect("read local events after root setup");
    let projected = RemoteApplyCoordinator::project_local_batch(&pending_events);
    let state = pg.pending_state.lock().expect("pending_state");
    assert_eq!(state.ops, projected.visible_ops);
    assert_eq!(state.event_start, Some(root_event_end));
    assert_eq!(state.event_end, event_end);
}

#[test]
fn unpublished_file_deletion_suffix_is_not_remote_visible() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"visible-prefix", 0, 0, 0o755)
        .expect("create visible pending prefix");
    let create_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read create start");
    pg.create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create unpublished file");
    let create_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read create end");
    pg.create_file(ROOT_INO, b"draft-2.txt", 0, 0, 0o644)
        .expect("create second unpublished file");
    let create_events = pg
        .local
        .read_events_range(create_start, create_end)
        .expect("read unpublished create events");
    let add_entry_only = [create_events
        .last()
        .cloned()
        .expect("unpublished create must append an AddEntry")];
    let add_entry_projected = RemoteApplyCoordinator::project_local_batch(&add_entry_only);
    assert!(
        pg.projected_suffix_is_local_only(&add_entry_only, &add_entry_projected.events)
            .expect("classify unpublished AddEntry suffix"),
        "an AddEntry suffix for a new empty regular file must stay local-only"
    );
    let deletion_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read deletion start");

    pg.unlink(ROOT_INO, b"draft.txt")
        .expect("unlink unpublished file");
    pg.unlink(ROOT_INO, b"draft-2.txt")
        .expect("unlink second unpublished file");

    let deletion_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read deletion end");
    let deletion_events = pg
        .local
        .read_events_range(deletion_start, deletion_end)
        .expect("read unpublished deletion events");
    let projected = RemoteApplyCoordinator::project_local_batch(&deletion_events);

    assert!(
        pg.projected_suffix_is_local_only(&deletion_events, &projected.events)
            .expect("classify unpublished deletion suffix"),
        "deleting a regular file with no published base must not re-project the pending prefix: deletion_events={deletion_events:?} projected={:?}",
        projected.events,
    );
}

#[test]
fn removing_unpublished_regular_attr_does_not_create_remote_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"draft-to-remove.txt", 0, 0, 0o644)
        .expect("create unpublished file");

    pg.remove_attr(file.ino)
        .expect("remove unpublished file attributes");

    let state = pg.pending_state.lock().expect("pending_state");
    assert_eq!(
        state.ops, 0,
        "removing a never-published regular file must not create remote-visible work"
    );
}

#[test]
fn pending_state_survives_flushed_cursor_persist_failure() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"persist-failure-draft.txt", 0, 0, 0o644)
        .expect("create unpublished file");
    let before_seq = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read before seq");
    pg.local
        .remove_attr(file.ino)
        .expect("remove unpublished file attributes");
    let after_seq = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read after seq");

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = pg.local_position_lock.lock().expect("local position lock");
        panic!("poison local position lock for persistence failure test");
    }));

    assert!(
        pg.mark_pending_after_mutation(before_seq, after_seq, PendingMutationHint::Unknown)
            .is_err(),
        "the poisoned cursor lock must make the acknowledgement fail"
    );
    let state = pg.pending_state.lock().expect("pending state");
    assert!(
        state.event_start.is_some(),
        "pending range must remain available after cursor persistence fails"
    );
}

#[test]
fn restore_pending_state_survives_flushed_cursor_persist_failure() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = create_published_small_file(&pg, ROOT_INO, b"restore-persist-failure.bin", 77_007);
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("acknowledge published setup");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
        pg.local
            .remove_regular_file_graph(file.ino, None)
            .expect("remove published file graph");
        state.event_start = Some(setup_end);
    }

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = pg.local_position_lock.lock().expect("local position lock");
        panic!("poison local position lock for restore persistence failure test");
    }));

    assert!(
        pg.restore_pending_state_from_local_log().is_err(),
        "the poisoned cursor lock must make restore fail"
    );
    let state = pg.pending_state.lock().expect("pending state");
    assert_eq!(
        state.event_start,
        Some(setup_end),
        "restore must not clear pending state before cursor persistence succeeds"
    );
}

#[test]
fn removing_regular_file_graph_does_not_create_remote_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"draft-graph-remove.txt", 0, 0, 0o644)
        .expect("create unpublished file");

    pg.remove_regular_file_graph(file.ino, None)
        .expect("remove unpublished file graph");

    let state = pg.pending_state.lock().expect("pending_state");
    assert_eq!(
        state.ops, 0,
        "regular-file graph cleanup is local-only and must not create remote-visible work"
    );
}

#[test]
fn removing_published_regular_file_graph_does_not_remain_local_only() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = create_published_small_file(&pg, ROOT_INO, b"published-graph-remove.txt", 77_006);
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("acknowledge published setup");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    pg.remove_regular_file_graph(file.ino, None)
        .expect("remove published file graph");

    let graph_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("graph end");
    let state = pg.pending_state.lock().expect("pending_state");
    assert_eq!(
        state.ops, 0,
        "graph cleanup itself is not a remote-visible operation"
    );
    assert!(
        state.event_start.is_none(),
        "published graph cleanup must not be retained as a local-only pending range"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        graph_end,
        "published graph cleanup must advance the local projection cursor"
    );
}

#[test]
fn restore_pending_state_from_local_log_rebuilds_unpublished_batch() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let start = pg.local.get_counter(NEXT_EVENT_SEQ).expect("baseline seq");

    pg.local
        .create_dir(ROOT_INO, b"draft", 0, 0, 0o755)
        .expect("create draft dir");
    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create draft file");

    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        *state = PendingReplicationState::default();
    }

    pg.restore_pending_state_from_local_log()
        .expect("restore pending state");

    let end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("end seq");
    let pending_events = pg
        .local
        .read_events_range(start, end)
        .expect("read pending events");
    let projected = RemoteApplyCoordinator::project_local_batch(&pending_events);
    let backlog = pg.replication_backlog();
    assert_eq!(backlog.pending_ops, projected.visible_ops);
    let state = pg.pending_state.lock().expect("pending_state");
    assert!(state.force_flush);
    assert_eq!(state.event_start, Some(start));
    assert_eq!(state.event_end, end);
}

#[test]
fn restore_pending_state_does_not_replay_stale_setup_over_published_file() {
    let (_local_dir, pg) = temp_pg_engine_for_unit();
    let local_file = pg
        .create_file(ROOT_INO, b"recovered-published.txt", 0, 0, 0o644)
        .expect("create local file");
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let remote_file =
        create_published_small_file(&remote, ROOT_INO, b"recovered-published.txt", 77_005);
    assert_eq!(local_file.ino, remote_file.ino);

    pg.local
        .import_remote_snapshot_preserving_local_event_log(
            &remote
                .local
                .export_remote_snapshot()
                .expect("export remote snapshot"),
        )
        .expect("import remote snapshot while keeping local journal");
    assert_eq!(
        pg.get_attr(local_file.ino).expect("published attr").size,
        64
    );

    pg.restore_pending_state_from_local_log()
        .expect("restore pending local journal");

    assert_eq!(
        pg.get_attr(local_file.ino)
            .expect("published attr after restore")
            .size,
        64,
        "a crash-replayed setup event must not hide a newer published base"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        pg.local.get_counter(NEXT_EVENT_SEQ).expect("event end"),
        "superseded setup events must not remain a PostgreSQL backlog"
    );
}

#[test]
fn restore_pending_state_keeps_unpublished_regular_file_replay_suffix() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let draft = pg
        .create_file(ROOT_INO, b"draft-with-provenance.xattr", 0, 0, 0o644)
        .expect("create unpublished file");
    pg.set_xattr(draft.ino, b"user.test", b"local-only", 0)
        .expect("set draft xattr");
    let mut uploading = pg.get_attr(draft.ino).expect("draft attr");
    uploading.size = 48;
    uploading.upload_state = UploadState::Uploading;
    pg.set_attr(&uploading).expect("set uploading draft attr");
    pg.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(77_004, 48))
        .expect("stage draft payload");
    let end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("end seq");
    let events = pg
        .local
        .read_events_range(start, end)
        .expect("read pending events");
    let xattr_index = events
        .iter()
        .position(|event| matches!(event, MetaEvent::SetXattr { ino, .. } if *ino == draft.ino))
        .expect("draft xattr event");
    let suffix_start = start.saturating_add(xattr_index as u64 + 1);
    pg.persist_flushed_event_seq(suffix_start)
        .expect("ack visible xattr prefix");

    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        *state = PendingReplicationState::default();
    }

    pg.restore_pending_state_from_local_log()
        .expect("restore pending state");

    let pending_events = pg
        .local
        .read_events_range(suffix_start, end)
        .expect("read pending events");
    let projected = RemoteApplyCoordinator::project_local_batch(&pending_events);
    assert!(projected.needs_local_replay);
    assert_eq!(projected.visible_ops, 0);

    let state = pg.pending_state.lock().expect("pending_state");
    assert_eq!(state.ops, 0);
    assert!(!state.force_flush);
    assert_eq!(state.event_start, Some(suffix_start));
    assert_eq!(state.event_end, end);
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), suffix_start);
}

#[test]
fn test_local_overlay_conflicts_reserve_pending_xattr_and_symlink() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .local
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .expect("create file");
    let sym = pg
        .local
        .create_symlink(ROOT_INO, b"link.txt", b"target-a", 0, 0)
        .expect("create symlink");

    let baseline = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read baseline seq");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        *state = PendingReplicationState::default();
    }

    pg.set_xattr(file.ino, b"user.color", b"red", 0)
        .expect("set local xattr");
    pg.set_symlink_target(sym.ino, b"target-b")
        .expect("set local symlink");

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        !overlay.block_inodes.contains(&file.ino),
        "xattr reservation should not masquerade as block overlay"
    );
    assert!(overlay.symlink_inodes.contains(&sym.ino));
    assert!(overlay.xattr_inodes.contains(&file.ino));
    assert!(
        overlay
            .xattr_keys
            .contains(&encode_xattr_key(file.ino, b"user.color"))
    );
    assert!(
        !overlay
            .xattr_keys
            .contains(&encode_xattr_key(file.ino, b"user.other"))
    );
    assert!(RemoteApplyCoordinator::aux_event_conflicts_with_overlay(
        &MetaEvent::SetXattr {
            ino: file.ino,
            name: b"user.color".to_vec(),
            value: b"blue".to_vec(),
            flags: 0,
        },
        &overlay,
        &HashSet::new(),
        &HashSet::new(),
    ));
    assert!(RemoteApplyCoordinator::aux_event_conflicts_with_overlay(
        &MetaEvent::SetSymlinkTarget {
            ino: sym.ino,
            target: b"target-c".to_vec(),
        },
        &overlay,
        &HashSet::new(),
        &HashSet::new(),
    ));

    let state = pg.pending_state.lock().expect("pending_state");
    let start = state.event_start.expect("pending start set");
    let end = state.event_end;
    assert_eq!(start, baseline);
    assert!(end > start);
}

#[test]
fn replication_backlog_reports_pending_and_inflight_ops() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        state.ops = 7;
    }
    pg.inflight_replication_ops.store(3, Ordering::SeqCst);

    let backlog = pg.replication_backlog();
    assert_eq!(backlog.pending_ops, 7);
    assert_eq!(backlog.inflight_ops, 3);
    assert_eq!(backlog.total_ops(), 10);
}

#[test]
fn empty_local_redb_uses_first_event_sequence_as_acknowledged_base() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let local = RedbMetaEngine::open(dir.path().join("meta.redb")).expect("open empty redb");

    let (local_event_end, durable_cursor) = local
        .local_outbound_ack_positions()
        .expect("read local event positions from one redb snapshot");
    assert_eq!(local_event_end, 1);
    assert_eq!(durable_cursor, 1);
    assert!(local.local_outbound_ack_snapshot().is_none());
    assert!(
        LocalOutboundAckSnapshot {
            local_event_end,
            durable_cursor,
            pending_ops: 0,
            inflight_ops: 0,
        }
        .is_acknowledged()
    );
}

#[test]
fn read_only_wrapper_preserves_pg_outbound_ack_snapshot() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let expected = pg.local_outbound_ack_snapshot().unwrap();
    let read_only = ReadOnlyMetaEngine::new(Arc::new(pg));

    assert_eq!(read_only.local_outbound_ack_snapshot(), Some(expected));
}

#[test]
fn outbound_ack_snapshot_detects_unacknowledged_journal_without_queued_ops() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"snapshot-unacked", 0, 0, 0o755)
        .expect("create local directory");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
    }
    pg.inflight_replication_ops.store(0, Ordering::SeqCst);

    let snapshot = pg
        .local_outbound_ack_snapshot()
        .expect("PostgreSQL-backed engine reports local ack state");
    assert_eq!(snapshot.pending_ops, 0);
    assert_eq!(snapshot.inflight_ops, 0);
    assert!(!snapshot.is_acknowledged());
    assert!(snapshot.durable_cursor < snapshot.local_event_end);
}

#[test]
fn local_only_journal_events_can_advance_the_ack_snapshot() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-local-only", 0, 0, 0o644)
        .expect("create unpublished file");
    pg.unlink(ROOT_INO, b"snapshot-local-only")
        .expect("unlink unpublished file");
    pg.restore_pending_state_from_local_log()
        .expect("retire local-only journal events");

    let snapshot = pg.local_outbound_ack_snapshot().unwrap();
    assert_eq!(snapshot.pending_ops, 0);
    assert!(snapshot.is_acknowledged());
    assert!(matches!(
        pg.local.get_attr(file.ino),
        Err(MetaError::InodeNotFound(_))
    ));
}

#[test]
fn peer_remote_head_progress_does_not_ack_local_journal() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"snapshot-peer-progress", 0, 0, 0o755)
        .expect("create local directory");
    let local_end = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.local_version
        .store(i64::try_from(local_end + 10).unwrap(), Ordering::SeqCst);

    let snapshot = pg.local_outbound_ack_snapshot().unwrap();
    assert!(!snapshot.is_acknowledged());
    assert_eq!(snapshot.local_event_end, local_end);
}

#[test]
fn outbound_ack_snapshot_tracks_durable_ack_across_reopen() {
    let (dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"snapshot-acknowledged", 0, 0, 0o755)
        .expect("create local directory");
    let event_end = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.persist_flushed_event_seq(event_end)
        .expect("persist acknowledged exclusive event end");
    let before_close = pg.local_outbound_ack_snapshot().unwrap();
    assert!(before_close.is_acknowledged());
    drop(pg);

    let reopened = PgMetaEngine::open_with_options(
        dir.path().join("meta.redb"),
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..Default::default()
        },
    )
    .expect("reopen local metadata");
    let after_reopen = reopened.local_outbound_ack_snapshot().unwrap();
    assert!(after_reopen.is_acknowledged());
    assert_eq!(after_reopen.local_event_end, event_end);
    assert_eq!(after_reopen.durable_cursor, event_end);
}

#[test]
fn upload_progress_only_mutation_does_not_create_pg_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"upload-progress-only.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let payload = PayloadRef::new_partial(77_001, 4096);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");

    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }
    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );
    let after_upload_progress = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("after upload progress");

    let backlog = pg.replication_backlog();
    assert_eq!(
        backlog.total_ops(),
        0,
        "local-only upload progress must not appear as remote metadata backlog"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        after_upload_progress,
        "the local-only event cursor should be acknowledged so reopen does not rebuild backlog"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "local-only upload progress must not wake the metadata poll worker"
    );
}

#[test]
fn redb_repair_restores_acknowledged_uploaded_overlay_before_startup_cleanup() {
    let (dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"repair-uploaded-overlay.bin", 0, 0, 0o644)
        .expect("create file");
    let create_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("create end");
    pg.persist_flushed_event_seq(create_end)
        .expect("acknowledge remote-visible create");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let payload = PayloadRef::new_partial(77_010, 152);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local-only payload");
    set_file_state(&pg, file.ino, 152, 1);
    let accepted = pg.get_attr(file.ino).expect("accepted attr");
    assert_eq!(accepted.size, 152);
    assert_eq!(accepted.committed_size, 0);
    assert_eq!(accepted.content_epoch, 1);
    assert_eq!(accepted.committed_content_epoch, 0);
    assert_eq!(accepted.upload_state, UploadState::Uploading);

    let accepted_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("accepted end");
    pg.persist_flushed_event_seq(accepted_end)
        .expect("acknowledge accepted metadata");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
    }
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(
            file.ino,
            accepted.content_epoch,
            payload.object_id,
        )
        .expect("mark payload uploaded"),
        "the accepted object should remain in the local overlay"
    );
    let acknowledged_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("uploaded-marker end");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        acknowledged_end,
        "the local-only uploaded marker is deliberately acknowledged without a PG backlog"
    );

    let prune = pg
        .prune_local_event_log_once()
        .expect("prune acknowledged local event history before repair");
    assert!(prune.rows > 0);
    assert_eq!(
        pg.local.first_event_seq().expect("first retained event"),
        None,
        "the acknowledged local journal prefix should be absent before redb repair"
    );

    let repair_snapshot = pg.local.export_snapshot().expect("export repair snapshot");
    pg.local
        .import_remote_snapshot(&repair_snapshot)
        .expect("simulate repair with no retained local event rows");
    assert!(
        pg.local
            .first_event_seq()
            .expect("first retained event")
            .is_none(),
        "the repair snapshot must contain no old local event rows"
    );

    let mut remote_projection = accepted.clone();
    remote_projection.size = 0;
    remote_projection.committed_size = 0;
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::RemoveAttr { ino: file.ino },
            MetaEvent::SetAttr {
                attr: remote_projection.clone(),
            },
        ])
        .expect("simulate repair plus remote startup projection");
    assert_eq!(
        pg.get_attr(file.ino).expect("projected startup attr").size,
        0,
        "the repair projection has the unfinished inode but no local extent"
    );
    assert!(
        pg.local
            .get_referenced_object_ids()
            .expect("references after repair projection")
            .contains(&payload.object_id),
        "the recovery row must keep the uploaded object reachable until repair restores its graph"
    );

    let path = dir.path().join("meta.redb");
    drop(pg);
    let pg = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
            defer_remote_startup_probes: true,
            ..Default::default()
        },
    )
    .expect("reopen repaired local metadata without PostgreSQL");

    let flushed_event_seq_before_restore = pg.flushed_event_seq.load(Ordering::SeqCst);
    let next_event_seq_before_restore = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("next event sequence before restore");
    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore acknowledged uploaded overlay")
    );

    let restored = pg.get_attr(file.ino).expect("restored attr");
    assert_eq!(restored.size, accepted.size);
    assert_eq!(restored.content_epoch, accepted.content_epoch);
    assert_eq!(
        restored.committed_content_epoch,
        accepted.committed_content_epoch
    );
    assert_eq!(restored.upload_state, accepted.upload_state);
    let lifecycle = pg
        .local
        .get_inode_lifecycle(file.ino)
        .expect("restored lifecycle")
        .expect("repaired inode has a lifecycle row");
    assert_eq!(
        lifecycle.state,
        crate::schema::InodeLifecycleState::PublishReady
    );
    assert_eq!(lifecycle.local_content_epoch, restored.content_epoch);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, restored.size).expect("restored object ids"),
        vec![payload.object_id],
        "repair must restore the uploaded extent before VFS startup cleanup"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        flushed_event_seq_before_restore,
        "repair replay must not reopen already-flushed metadata for PG publication"
    );
    assert_eq!(
        pg.local
            .get_counter(NEXT_EVENT_SEQ)
            .expect("next event sequence after restore"),
        next_event_seq_before_restore,
        "repair restore must not append to the local event journal"
    );
    assert_eq!(
        pg.replication_backlog().total_ops(),
        0,
        "repair replay is local-only and must not create a PG backlog"
    );
}

#[test]
fn redb_repair_does_not_restore_uploaded_overlay_over_conflicting_same_epoch_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"repair-newer-ready-state.bin", 0, 0, 0o644)
        .expect("create file");
    let create_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("create end");
    pg.persist_flushed_event_seq(create_end)
        .expect("acknowledge remote-visible create");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let payload = PayloadRef::new_partial(77_011, 152);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local-only payload");
    set_file_state(&pg, file.ino, 152, 1);
    let accepted = pg.get_attr(file.ino).expect("accepted attr");
    let accepted_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("accepted end");
    pg.persist_flushed_event_seq(accepted_end)
        .expect("acknowledge accepted metadata");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
    }
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(
            file.ino,
            accepted.content_epoch,
            payload.object_id,
        )
        .expect("mark payload uploaded")
    );
    let mut newer_remote = accepted.clone();
    newer_remote.size = 0;
    newer_remote.committed_size = 0;
    newer_remote.content_epoch = accepted.content_epoch;
    newer_remote.committed_content_epoch = accepted.content_epoch;
    newer_remote.upload_state = UploadState::Ready;
    let mut remote_snapshot = pg
        .local
        .export_snapshot()
        .expect("export published snapshot");
    remote_snapshot.event_log.clear();
    for (ino, bytes) in &mut remote_snapshot.inodes {
        if *ino == file.ino {
            *bytes = postcard::to_stdvec(&newer_remote).expect("encode published attr");
        }
    }
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("simulate published remote snapshot");
    assert!(
        pg.local
            .first_event_seq()
            .expect("first retained event")
            .is_none()
    );

    let flushed_event_seq_before_restore = pg.flushed_event_seq.load(Ordering::SeqCst);
    assert!(
        !pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("attempt repair replay"),
        "an ambiguous retained row must keep startup repair pending"
    );

    let current = pg.get_attr(file.ino).expect("current attr");
    assert_eq!(
        current.content_epoch, newer_remote.content_epoch,
        "repair replay must not overwrite a conflicting same-epoch remote state"
    );
    assert_eq!(current.size, newer_remote.size);
    assert_eq!(current.committed_size, newer_remote.committed_size);
    assert_eq!(current.upload_state, newer_remote.upload_state);
    assert!(
        pg.local
            .get_referenced_object_ids()
            .expect("current references")
            .contains(&77_011),
        "the remote epoch counter does not prove that this payload was published"
    );
    assert_eq!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery records after conflicting projection")
            .len(),
        1,
        "the recovery row pins a payload absent from the published base graph"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        flushed_event_seq_before_restore,
        "guarded replay must leave the flushed cursor untouched"
    );
}

#[test]
fn redb_repair_retires_uploaded_recovery_after_inode_kind_change() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = uploaded_pending_file(&pg, b"repair-kind-change.bin", 77_016);
    let mut replacement = file.clone();
    replacement.kind = crate::types::FileType::Directory;
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::SetRegularFileOverlaySlices {
                ino: file.ino,
                chunk_slices: Vec::new(),
            },
            MetaEvent::SetAttr { attr: replacement },
        ])
        .expect("project changed inode kind");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("retire recovery for non-regular inode")
    );

    assert_eq!(
        pg.get_attr(file.ino).expect("projected inode").kind,
        crate::types::FileType::Directory
    );
    assert!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery records after kind change")
            .is_empty()
    );
}

#[test]
fn redb_repair_retires_uploaded_recovery_after_creation_time_change() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = uploaded_pending_file(&pg, b"repair-crtime-change.bin", 77_017);
    let mut replacement = file.clone();
    replacement.crtime = replacement.crtime.saturating_add(1);
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::SetRegularFileOverlaySlices {
                ino: file.ino,
                chunk_slices: Vec::new(),
            },
            MetaEvent::SetAttr {
                attr: replacement.clone(),
            },
        ])
        .expect("project changed inode creation time");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("retire recovery for changed inode identity")
    );

    assert_eq!(
        pg.get_attr(file.ino).expect("projected inode").crtime,
        replacement.crtime
    );
    assert!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery records after identity change")
            .is_empty()
    );
}

#[test]
fn redb_repair_does_not_partially_commit_when_lifecycle_rebuild_fails() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = uploaded_pending_file(&pg, b"repair-lifecycle-failure.bin", 77_018);
    let mut remote_projection = file.clone();
    remote_projection.size = 0;
    remote_projection.committed_size = 0;
    let mut remote_snapshot = pg
        .local
        .export_snapshot()
        .expect("export remote projection");
    remote_snapshot.event_log.clear();
    for (ino, bytes) in &mut remote_snapshot.inodes {
        if *ino == file.ino {
            *bytes = postcard::to_stdvec(&remote_projection).expect("encode remote attr");
        }
    }
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("project remote inode without its overlay");
    let txn = pg
        .local
        .begin_write_txn()
        .expect("open lifecycle transaction");
    txn.open_table(redb::TableDefinition::<u64, &[u8]>::new("inode_lifecycle"))
        .expect("open lifecycle table")
        .insert(file.ino, b"\xff".as_slice())
        .expect("inject malformed lifecycle row");
    txn.commit().expect("commit malformed lifecycle fixture");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .is_err()
    );

    assert_eq!(
        pg.get_attr(file.ino)
            .expect("inode after failed repair")
            .size,
        0
    );
    assert!(
        lookup_object_ids(&pg, file.ino, file.size)
            .expect("serving references after failed repair")
            .is_empty()
    );
    assert_eq!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery row after failed repair")
            .len(),
        1
    );
}

#[test]
fn redb_repair_does_not_restore_superseded_uploaded_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let superseded = uploaded_pending_file(&pg, b"repair-superseded.bin", 77_014);

    let mut remote_snapshot = pg.local.export_snapshot().expect("export remote snapshot");
    remote_snapshot.event_log.clear();
    for (ino, bytes) in &mut remote_snapshot.inodes {
        if *ino == superseded.ino {
            let mut attr: InodeAttr = postcard::from_bytes(bytes).expect("decode superseded attr");
            attr.content_epoch = attr.content_epoch.saturating_add(1);
            attr.size = 0;
            attr.upload_state = UploadState::Uploading;
            *bytes = postcard::to_stdvec(&attr).expect("encode superseded attr");
        }
    }
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != superseded.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("simulate superseding remote projection");
    assert!(
        pg.local
            .first_event_seq()
            .expect("first retained event")
            .is_none()
    );

    let next_event_seq = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("event sequence");
    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("attempt repair restore")
    );

    let current = pg.get_attr(superseded.ino).expect("superseding attr");
    assert_eq!(current.content_epoch, superseded.content_epoch + 1);
    assert_eq!(current.size, 0);
    assert_eq!(current.upload_state, UploadState::Uploading);
    assert!(
        pg.local
            .get_referenced_object_ids()
            .expect("projected object references")
            .is_empty()
    );
    assert!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery records after superseded/deleted projection")
            .is_empty(),
        "a superseded inode retires its obsolete recovery record"
    );
    assert_eq!(
        pg.local
            .get_counter(NEXT_EVENT_SEQ)
            .expect("event sequence after restore"),
        next_event_seq,
        "repair restore must not append events"
    );
}

#[test]
fn redb_repair_does_not_restore_deleted_uploaded_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-deleted.bin";
    let created = pg
        .create_file(ROOT_INO, name, 0, 0, 0o644)
        .expect("create file");
    pg.local
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: created.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: name.to_vec(),
                entry: crate::types::DirEntryValue {
                    ino: created.ino,
                    kind: created.kind,
                },
            },
        ])
        .expect("publish inode identity before pending content");
    let payload = PayloadRef::new_partial(77_015, 152);
    pg.stage_payload_slice(created.ino, 0, &payload)
        .expect("stage pending payload");
    set_file_state(&pg, created.ino, 152, 1);
    let deleted = pg.get_attr(created.ino).expect("pending attr");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(
            deleted.ino,
            deleted.content_epoch,
            payload.object_id,
        )
        .expect("mark pending payload uploaded")
    );

    let mut remote_snapshot = pg.local.export_snapshot().expect("export remote snapshot");
    remote_snapshot.event_log.clear();
    remote_snapshot
        .inodes
        .retain(|(ino, _)| *ino != deleted.ino);
    remote_snapshot
        .dentries
        .retain(|(key, _)| key != &encode_dentry_key(ROOT_INO, name));
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != deleted.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("simulate deleted remote projection");
    assert!(
        pg.local
            .first_event_seq()
            .expect("first retained event")
            .is_none()
    );

    let next_event_seq = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("event sequence");
    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("attempt repair restore")
    );

    assert!(matches!(
        pg.get_attr(deleted.ino),
        Err(MetaError::InodeNotFound(_))
    ));
    assert!(
        pg.local
            .get_referenced_object_ids()
            .expect("projected object references")
            .is_empty()
    );
    assert!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery records after deletion")
            .is_empty()
    );
    assert_eq!(
        pg.local
            .get_counter(NEXT_EVENT_SEQ)
            .expect("event sequence after restore"),
        next_event_seq,
        "repair restore must not append events"
    );
}

#[test]
fn garbage_collection_retires_superseded_uploaded_recovery_rows() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = uploaded_pending_file(&pg, b"recovery-gc-superseded.bin", 77_021);
    let mut superseding = file.clone();
    superseding.content_epoch = superseding.content_epoch.saturating_add(1);
    superseding.committed_content_epoch = superseding.content_epoch;
    superseding.size = 0;
    superseding.committed_size = 0;
    superseding.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::SetRegularFileOverlaySlices {
                ino: file.ino,
                chunk_slices: Vec::new(),
            },
            MetaEvent::SetAttr { attr: superseding },
        ])
        .expect("apply superseding serving state");

    assert!(
        pg.local
            .get_referenced_object_ids()
            .expect("read recovery references")
            .contains(&77_021),
        "reachability reads must not retire rows"
    );
    pg.local
        .reconcile_pending_regular_file_recoveries()
        .expect("retire superseded recovery row explicitly");
    assert!(
        pg.local
            .get_referenced_object_ids()
            .expect("references after reconciliation")
            .is_empty()
    );
    assert!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("reconciled recovery rows")
            .is_empty()
    );
}

#[test]
fn redb_repair_restores_unpublished_uploaded_file_missing_from_checkpoint() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-unpublished-missing.bin";
    let file = uploaded_pending_file(&pg, name, 77_019);

    let mut remote_snapshot = pg.local.export_snapshot().expect("export remote snapshot");
    remote_snapshot.event_log.clear();
    remote_snapshot.inodes.retain(|(ino, _)| *ino != file.ino);
    remote_snapshot
        .dentries
        .retain(|(key, _)| key != &encode_dentry_key(ROOT_INO, name));
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("simulate checkpoint omitting unpublished inode");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore unpublished uploaded file")
    );

    assert_eq!(pg.get_attr(file.ino).expect("restored inode"), file);
    assert_eq!(
        pg.local
            .lookup(ROOT_INO, name)
            .expect("restored namespace entry")
            .ino,
        file.ino
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, file.size).expect("restored uploaded payloads"),
        vec![77_019]
    );
    assert_eq!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery records")
            .len(),
        1,
        "the recovery row remains available until publication retires it"
    );
}

#[test]
fn redb_repair_restores_nameless_pending_inode() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-nameless.bin";
    let file = uploaded_pending_file(&pg, name, 77_023);
    pg.local
        .apply_replica_serving_state(&[MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: name.to_vec(),
        }])
        .expect("remove the serving name");
    pg.local
        .backfill_missing_uploaded_regular_file_recoveries()
        .expect("capture empty namespace");
    assert_eq!(
        pg.local
            .pending_uploaded_regular_file_recoveries()
            .expect("recovery row")[0]
            .dentries,
        Some(Vec::new())
    );

    let mut checkpoint = pg.local.export_snapshot().expect("export checkpoint");
    checkpoint.inodes.retain(|(ino, _)| *ino != file.ino);
    checkpoint
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&checkpoint)
        .expect("import checkpoint without unnamed inode");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore nameless pending inode")
    );
    assert_eq!(
        pg.get_attr(file.ino).expect("restored inode").size,
        file.size
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, file.size).expect("restored payload"),
        vec![77_023]
    );
}

#[test]
fn redb_repair_restores_pending_file_namespace_after_hardlink_metadata_drift() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-hardlink-source.bin";
    let alias = b"repair-hardlink-alias.bin";
    let before_link = uploaded_pending_file(&pg, name, 77_020);
    let after_link = pg
        .link(before_link.ino, ROOT_INO, alias)
        .expect("create hardlink");

    let mut remote_snapshot = pg.local.export_snapshot().expect("export remote snapshot");
    remote_snapshot.event_log.clear();
    for (ino, bytes) in &mut remote_snapshot.inodes {
        if *ino == before_link.ino {
            *bytes = postcard::to_stdvec(&before_link).expect("encode pre-link attr");
        }
    }
    remote_snapshot
        .dentries
        .retain(|(key, _)| key != &encode_dentry_key(ROOT_INO, alias));
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != before_link.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("simulate checkpoint missing a local hardlink");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore linked pending file")
    );

    assert_eq!(
        pg.get_attr(before_link.ino).expect("restored attr"),
        after_link
    );
    assert_eq!(
        pg.local
            .lookup(ROOT_INO, alias)
            .expect("restored hardlink")
            .ino,
        before_link.ino
    );
}

#[test]
fn redb_repair_does_not_resurrect_old_name_after_pending_file_rename() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let old_name = b"repair-rename-old.bin";
    let new_name = b"repair-rename-new.bin";
    let file = uploaded_pending_file(&pg, old_name, 77_026);
    pg.rename(ROOT_INO, old_name, ROOT_INO, new_name)
        .expect("rename pending file");

    let mut checkpoint = pg.local.export_snapshot().expect("export checkpoint");
    checkpoint
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&checkpoint)
        .expect("lose pending overlay during repair");
    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore renamed pending file")
    );
    assert!(pg.lookup(ROOT_INO, old_name).is_err());
    assert_eq!(
        pg.lookup(ROOT_INO, new_name)
            .expect("new name remains live")
            .ino,
        file.ino
    );
}

#[test]
fn redb_repair_preserves_checkpoint_metadata_and_link_count() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-peer-link.bin";
    let alias = b"repair-peer-link-alias.bin";
    let file = uploaded_pending_file(&pg, name, 77_022);

    let mut remote_snapshot = pg.local.export_snapshot().expect("export checkpoint");
    remote_snapshot.event_log.clear();
    for (ino, bytes) in &mut remote_snapshot.inodes {
        if *ino == file.ino {
            let mut attr: InodeAttr = postcard::from_bytes(bytes).expect("decode inode");
            attr.nlink = 2;
            attr.mode = 0o600;
            attr.uid = 42;
            attr.gid = 43;
            *bytes = postcard::to_stdvec(&attr).expect("encode peer metadata");
        }
    }
    let entry = remote_snapshot
        .dentries
        .iter()
        .find(|(key, _)| *key == encode_dentry_key(ROOT_INO, name))
        .expect("original checkpoint dentry")
        .1
        .clone();
    remote_snapshot
        .dentries
        .push((encode_dentry_key(ROOT_INO, alias), entry));
    remote_snapshot
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&remote_snapshot)
        .expect("import peer metadata without local overlay");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore pending payload")
    );
    let restored = pg.get_attr(file.ino).expect("restored inode");
    assert_eq!(
        (restored.nlink, restored.mode, restored.uid, restored.gid),
        (2, 0o600, 42, 43)
    );
    pg.unlink(ROOT_INO, name).expect("unlink one hardlink");
    assert_eq!(
        pg.lookup(ROOT_INO, alias).expect("surviving hardlink").ino,
        file.ino
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, file.size).expect("surviving payload"),
        vec![77_022]
    );
}

#[test]
fn redb_repair_preserves_local_permission_change_after_upload() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let name = b"repair-local-chmod.bin";
    let file = pg
        .create_file(ROOT_INO, name, 0, 0, 0o644)
        .expect("create file");
    pg.local
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: file.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: name.to_vec(),
                entry: crate::types::DirEntryValue {
                    ino: file.ino,
                    kind: file.kind,
                },
            },
        ])
        .expect("publish original metadata");
    let payload = PayloadRef::new_partial(77_027, 152);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local payload");
    set_file_state(&pg, file.ino, 152, 1);
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("upload payload")
    );
    let mut checkpoint = pg.local.export_snapshot().expect("export old permissions");
    checkpoint
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    let mut chmod = pg.get_attr(file.ino).expect("pending inode");
    chmod.mode = 0o600;
    pg.set_attr(&chmod).expect("accept local chmod");
    pg.local
        .backfill_missing_uploaded_regular_file_recoveries()
        .expect("capture local metadata before import");
    pg.local
        .import_remote_snapshot(&checkpoint)
        .expect("import older permissions without pending overlay");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore local pending state")
    );
    assert_eq!(pg.get_attr(file.ino).expect("restored inode").mode, 0o600);
}

#[test]
fn redb_repair_preserves_nonuploaded_sibling_in_same_chunk() {
    use crate::schema::{ChunkId, SliceId, SliceOrder, SliceRange, SliceRecord};

    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"repair-partial-chunk.bin", 0, 0, 0o644)
        .expect("create file");
    let first = PayloadRef::new_partial(77_024, 4096);
    let second = PayloadRef::new_partial(77_025, 4096);
    pg.local
        .flush_regular_file_overlay_slices(
            file.ino,
            &[(
                ChunkId(0),
                vec![
                    SliceRecord::new(
                        SliceId(0),
                        SliceOrder(0),
                        SliceRange::new(0, 4096).unwrap(),
                        first,
                    ),
                    SliceRecord::new(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange::new(4096, 8192).unwrap(),
                        second,
                    ),
                ],
            )],
        )
        .expect("stage two slices in one chunk");
    set_file_state(&pg, file.ino, 8192, 1);
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, first.object_id)
            .expect("upload first slice")
    );
    let mut checkpoint = pg.local.export_snapshot().expect("export checkpoint");
    checkpoint
        .regular_file_overlay
        .retain(|(ino, _)| *ino != file.ino);
    pg.local
        .import_remote_snapshot(&checkpoint)
        .expect("lose local overlay during repair");

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore mixed chunk")
    );
    let restored = pg.local.export_snapshot().expect("export restored graph");
    let (_, bytes) = restored
        .regular_file_overlay
        .iter()
        .find(|(ino, _)| *ino == file.ino)
        .expect("restored chunk");
    let state: crate::schema::RegularFileOverlayState =
        postcard::from_bytes(bytes).expect("decode restored chunk");
    let slices = &state.chunks[&ChunkId(0)].slices;
    assert_eq!(slices[&SliceId(0)].payload_state, PayloadRefState::Uploaded);
    assert_eq!(
        slices[&SliceId(1)].payload_state,
        PayloadRefState::LocalOnly
    );
}

#[test]
fn redb_repair_merges_uploaded_payloads_across_stale_overlay_events() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"repair-stale-overlay-events.bin", 0, 0, 0o644)
        .expect("create file");
    let first = PayloadRef::new_partial(77_012, DEFAULT_BLOCK_SIZE);
    let second = PayloadRef::new_partial(77_013, DEFAULT_BLOCK_SIZE);

    pg.stage_payload_slice(file.ino, 0, &first)
        .expect("stage first payload");
    pg.stage_payload_slice(file.ino, 1, &second)
        .expect("stage second payload");
    set_file_state(&pg, file.ino, u64::from(DEFAULT_BLOCK_SIZE) * 2, 1);
    let accepted = pg.get_attr(file.ino).expect("accepted attr");
    pg.mark_regular_file_overlay_object_uploaded_if_matches(
        file.ino,
        accepted.content_epoch,
        first.object_id,
    )
    .expect("mark first payload uploaded");

    let mut stale_snapshot = pg.local.export_snapshot().expect("export snapshot");
    for (_, bytes) in &mut stale_snapshot.regular_file_overlay {
        let mut state: crate::schema::RegularFileOverlayState =
            postcard::from_bytes(bytes).expect("decode overlay snapshot");
        for chunk in state.chunks.values_mut() {
            for slice in chunk.slices.values_mut() {
                if slice
                    .payload
                    .is_some_and(|payload| payload.object_id == first.object_id)
                {
                    slice.payload_state = PayloadRefState::LocalOnly;
                }
            }
        }
        *bytes = postcard::to_stdvec(&state).expect("encode stale overlay snapshot");
    }
    stale_snapshot.event_log.clear();
    pg.local
        .import_remote_snapshot(&stale_snapshot)
        .expect("replace serving state without retaining the old event log");

    pg.mark_regular_file_overlay_object_uploaded_if_matches(
        file.ino,
        accepted.content_epoch,
        second.object_id,
    )
    .expect("mark second payload uploaded");

    let mut repair_snapshot = pg.local.export_snapshot().expect("export repair snapshot");
    repair_snapshot.event_log.clear();
    pg.local
        .import_remote_snapshot(&repair_snapshot)
        .expect("remove the remaining local event rows");
    assert!(
        pg.local
            .first_event_seq()
            .expect("first retained event")
            .is_none()
    );

    assert!(
        pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
            .expect("restore acknowledged uploaded overlay")
    );

    let restored = pg.local.export_snapshot().expect("export restored state");
    let (_, bytes) = restored
        .regular_file_overlay
        .iter()
        .find(|(ino, _)| *ino == file.ino)
        .expect("restored overlay row");
    let state: crate::schema::RegularFileOverlayState =
        postcard::from_bytes(bytes).expect("decode restored overlay");
    let states: std::collections::HashMap<_, _> = state
        .chunks
        .values()
        .flat_map(|chunk| chunk.slices.values())
        .filter_map(|slice| {
            slice
                .payload
                .map(|payload| (payload.object_id, slice.payload_state))
        })
        .collect();
    assert_eq!(
        states.get(&first.object_id),
        Some(&PayloadRefState::Uploaded)
    );
    assert_eq!(
        states.get(&second.object_id),
        Some(&PayloadRefState::Uploaded)
    );
}

#[test]
fn upload_progress_only_mutation_does_not_advance_cursor_past_inflight_visible_batch() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.create_dir(ROOT_INO, b"inflight-visible-dir", 0, 0, 0o755)
        .expect("create visible directory");
    let file = pg
        .create_file(ROOT_INO, b"inflight-visible-create.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let payload = PayloadRef::new_partial(77_003, 4096);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");
    let batch = pg
        .claim_pending_batch(true)
        .expect("visible directory and local overlay setup should be claimable");
    let claimed_start = batch.event_start;
    assert!(
        pg.replication_backlog().inflight_ops > 0,
        "claimed visible batch should remain inflight until published or restored"
    );

    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );

    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        claimed_start,
        "local-only upload progress must not skip an inflight visible batch"
    );
}

#[test]
fn published_unlink_creates_remote_visible_pg_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    create_published_small_file(&pg, ROOT_INO, b"published-delete.bin", 77_002);
    let published_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("published end");
    pg.persist_flushed_event_seq(published_end)
        .expect("ack published setup");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    pg.unlink(ROOT_INO, b"published-delete.bin")
        .expect("unlink published file");
    pg.request_eager_flush();

    let backlog = pg.replication_backlog();
    assert!(
        backlog.total_ops() > 0,
        "published unlink must create remote-visible metadata backlog"
    );
    let batch = pg
        .claim_pending_batch(false)
        .expect("published unlink should be claimable after eager flush request");
    let events = pg
        .local
        .read_events_range(batch.event_start, batch.event_end)
        .expect("read unlink events");
    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    assert!(
        projected
            .iter()
            .any(|event| matches!(event, MetaEvent::RemoveEntry { .. })),
        "published unlink must project a remote RemoveEntry"
    );
    assert!(
        projected
            .iter()
            .any(|event| matches!(event, MetaEvent::RemoveAttr { .. })),
        "published unlink must project a remote RemoveAttr"
    );
}

#[test]
fn regular_namespace_mutations_batch_without_pg_poll_wakeup() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    create_published_small_file(&pg, ROOT_INO, b"batched-delete.bin", 77_004);
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("acknowledge remote-visible setup");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    *wakeup_lock.lock().expect("poll wakeup lock") = false;

    pg.unlink(ROOT_INO, b"batched-delete.bin")
        .expect("unlink published file");

    assert!(
        pg.replication_backlog().pending_ops > 0,
        "namespace mutation must remain a remote-visible metadata backlog"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "ordinary namespace mutations should wait for group-commit timing or an explicit barrier"
    );
    assert!(
        pg.claim_pending_batch(false).is_none(),
        "ordinary namespace mutations must not force a PG batch before the group-commit deadline"
    );
}

#[test]
fn request_eager_flush_wakes_pg_poll_worker() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.request_pending_flush();
    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "regular upload-pipeline flush requests should not wake the metadata poll worker"
    );

    pg.request_eager_flush();

    assert!(
        *wakeup_lock.lock().expect("poll wakeup lock"),
        "namespace eager flush requests should wake the metadata poll worker"
    );
}

#[test]
fn published_base_graph_wakes_pg_poll_worker() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"publish-wakeup.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let payload = PayloadRef::new_partial(77_003, 4096);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );

    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base graph"),
        "base graph publish must succeed"
    );

    assert!(
        *wakeup_lock.lock().expect("poll wakeup lock"),
        "publishing a remote-visible base graph should wake the metadata poll worker"
    );
}

#[test]
fn ready_file_attr_change_wakes_pg_poll_worker() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"ready-attr-wakeup.bin", 77_030);
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    let mut attr = pg.get_attr(file.ino).expect("get attr");
    attr.mtime = attr.mtime.saturating_add(1);
    attr.touch_ctime();
    pg.set_attr(&attr).expect("set ready attr");

    assert!(
        *wakeup_lock.lock().expect("poll wakeup lock"),
        "settled file metadata changes should wake the metadata poll worker"
    );
}

#[test]
fn xattr_change_batches_without_pg_poll_wakeup() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"xattr-batch-wakeup.bin", 0, 0, 0o644)
        .expect("create file");
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    pg.set_xattr(file.ino, b"user.color", b"blue", 0)
        .expect("set xattr");

    assert_eq!(
        pg.replication_backlog().pending_ops,
        1,
        "xattr changes remain remote-visible metadata"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "xattr overlay changes should batch instead of waking PG before an explicit flush"
    );
}

#[test]
fn uploading_file_attr_change_stays_local_without_pg_poll_wakeup() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"uploading-attr-batch.bin", 0, 0, 0o644)
        .expect("create file");
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.background_workers.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    let mut attr = pg.get_attr(file.ino).expect("get attr");
    attr.size = 4096;
    attr.committed_size = 0;
    attr.upload_state = UploadState::Uploading;
    attr.touch_ctime();
    pg.set_attr(&attr).expect("set uploading attr");

    assert_eq!(
        pg.replication_backlog().pending_ops,
        0,
        "unpublished uploading attrs must stay out of PostgreSQL"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "upload-progress attrs should batch instead of waking PG on the hot write path"
    );
}

#[test]
fn claim_and_restore_pending_batch_updates_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        state.ops = 4;
        state.force_flush = true;
        state.event_start = Some(10);
        state.event_end = 14;
        state.started_at = Some(Instant::now());
        state.last_mutation_at = Some(Instant::now());
    }

    let batch = pg.claim_pending_batch(true).expect("claimed batch");
    let claimed = pg.replication_backlog();
    assert_eq!(claimed.pending_ops, 0);
    assert_eq!(claimed.inflight_ops, 4);

    pg.restore_pending_batch(batch);
    let restored = pg.replication_backlog();
    assert_eq!(restored.pending_ops, 4);
    assert_eq!(restored.inflight_ops, 0);
}

#[test]
fn permanent_event_size_error_does_not_restore_claimed_pending_batch() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    pg.remote_format_validated.store(true, Ordering::Release);
    pg.sync_max_response_bytes_per_page = 1;

    pg.set_xattr(ROOT_INO, b"user.sfs726.permanent-size", &[b'x'; 128], 0)
        .expect("create oversized pending event");
    let claimed = pg
        .claim_pending_batch(true)
        .expect("oversized event should be claimable");

    let error = match pg.flush_pending_claimed_batch_with_restore(claimed) {
        Ok(_) => panic!("oversized event must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, MetaError::EventTooLarge { .. }));

    let backlog = pg.replication_backlog();
    assert_eq!(
        backlog.pending_ops, 0,
        "permanent error must not be retried"
    );
    assert_eq!(
        backlog.inflight_ops, 0,
        "claim must be completed once surfaced"
    );

    pg.sync_max_response_bytes_per_page = 8 * 1024 * 1024;
    pg.create_dir(ROOT_INO, b"event-after-permanent-error", 0, 0, 0o755)
        .expect("create event after permanent error");
    let later = pg
        .claim_pending_batch(true)
        .expect("later events must not be head-of-line blocked");
    assert!(
        later.event_start >= claimed.event_end,
        "later claim must start after the surfaced event range"
    );
}

#[test]
fn serving_dentry_overlay_check_uses_exact_merged_keys() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let dir = pg
        .create_dir(ROOT_INO, b"big", 0, 0, 0o755)
        .expect("create directory");
    for index in 0..32 {
        pg.create_file(dir.ino, format!("sibling-{index}").as_bytes(), 0, 0, 0o644)
            .expect("create sibling");
    }
    // A rename source name is an exact reconcile key that no longer binds anything.
    let mut keys = HashSet::new();
    keys.insert(encode_dentry_key(dir.ino, b"renamed-away"));
    crate::redb_engine::take_case_fallback_merged_scans();

    RemoteApplyCoordinator::new(&pg.local)
        .reconcile_serving_dentries_from_base_with_overlay(&keys, &OverlayConflictSets::default())
        .expect("reconcile serving dentries");

    assert_eq!(crate::redb_engine::take_case_fallback_merged_scans(), 0);
}

#[test]
fn flush_path_pending_restore_recomputes_counters_without_serving_replay() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    pg.remote_format_validated.store(true, Ordering::Release);
    pg.sync_max_response_bytes_per_page = 1;

    pg.create_dir(ROOT_INO, b"still-pending", 0, 0, 0o755)
        .expect("create independent pending event");
    pg.set_xattr(ROOT_INO, b"user.sfs890.oversized", &[b'x'; 128], 0)
        .expect("create oversized pending event");
    let claimed = pg
        .claim_pending_batch(true)
        .expect("oversized event should be claimable");
    crate::redb_engine::take_pending_serving_replays();

    // Nothing rewrote serving state since the mutations: the flush only
    // rejects and re-selects local journal ranges.
    assert!(matches!(
        pg.flush_pending_claimed_batch_with_restore(claimed),
        Err(MetaError::EventTooLarge { .. })
    ));
    assert_eq!(crate::redb_engine::take_pending_serving_replays(), 0);
    assert!(
        pg.replication_backlog().pending_ops > 0,
        "the independent directory create must stay pending"
    );
    assert!(pg.lookup(ROOT_INO, b"still-pending").is_ok());

    // Replaying after a serving rewrite still runs.
    pg.restore_pending_state_from_local_log()
        .expect("restore pending local log");
    assert!(crate::redb_engine::take_pending_serving_replays() > 0);
}

#[test]
fn oversized_event_rejects_only_its_sequence_and_restores_independent_events() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    pg.remote_format_validated.store(true, Ordering::Release);
    pg.sync_max_response_bytes_per_page = 1024;

    let event_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read first local event sequence");
    pg.local
        .set_xattr(ROOT_INO, b"user.small.before", b"ok", 0)
        .expect("append valid event before published range");
    let published_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read published range start");
    pg.local
        .set_xattr(ROOT_INO, b"user.published", b"ok", 0)
        .expect("append event to mark already published");
    let published_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read published range end");
    let oversized_start = published_end;
    pg.local
        .set_xattr(ROOT_INO, b"user.sfs726.oversized", &[b'x'; 8 * 1024], 0)
        .expect("append one oversized event");
    let oversized_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read oversized event end");
    pg.local
        .set_xattr(ROOT_INO, b"user.small.after", b"ok", 0)
        .expect("append valid event after oversized event");
    let event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read final local event sequence");
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: published_start,
        end: published_end,
    }])
    .expect("mark intervening event as published");

    {
        let mut state = pg.pending_state.lock().expect("pending state");
        state.ops = 3;
        state.force_flush = true;
        state.event_start = Some(event_start);
        state.event_end = event_end;
    }
    let claimed = pg
        .claim_pending_batch(true)
        .expect("claim surrounding independent events");
    let error = match pg.flush_pending_claimed_batch_with_restore(claimed) {
        Ok(_) => panic!("one oversized event must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, MetaError::EventTooLarge { .. }));

    let progress = pg
        .unprocessed_local_event_progress()
        .expect("read local event progress");
    assert_eq!(
        progress.rejected,
        vec![LocalEventRange {
            start: oversized_start,
            end: oversized_end,
        }],
        "only the oversized event should be rejected"
    );
    assert_eq!(
        progress.published,
        vec![LocalEventRange {
            start: published_start,
            end: published_end,
        }],
        "an earlier publication must remain acknowledged"
    );
    {
        let pending = pg.pending_state.lock().expect("restored pending state");
        assert_eq!(pending.event_start, Some(event_start));
        assert_eq!(pending.event_end, event_end);
        assert_eq!(pending.ops, 2, "both independent events should be restored");
        assert!(
            pending.force_flush,
            "restored independent work should be schedulable"
        );
    }
    assert_eq!(
        pg.replication_backlog().inflight_ops,
        0,
        "the oversized claim must be finished"
    );
}

#[test]
fn oversized_rejection_marker_failure_still_finishes_claim() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    pg.remote_format_validated.store(true, Ordering::Release);
    pg.sync_max_response_bytes_per_page = 1;
    pg.create_dir(
        ROOT_INO,
        b"oversized-with-poisoned-marker-lock",
        0,
        0,
        0o755,
    )
    .expect("create oversized event");
    let claimed = pg
        .claim_pending_batch(true)
        .expect("oversized event should be claimable");

    let pg = std::sync::Arc::new(pg);
    let poison_pg = std::sync::Arc::clone(&pg);
    let _ = std::thread::spawn(move || {
        let _guard = poison_pg
            .local_position_lock
            .lock()
            .expect("acquire local position lock before poisoning it");
        panic!("poison local position lock for rejection marker test");
    })
    .join();

    assert!(
        pg.flush_pending_claimed_batch_with_restore(claimed)
            .is_err()
    );
    assert_eq!(
        pg.replication_backlog().inflight_ops,
        0,
        "marker persistence failure must not leak the claimed operations"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local PostgreSQL"]
fn oversized_event_gap_survives_local_delete_and_retries_after_restart() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-oversized-local-event-gap");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());

    let low_limit = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "1024");
    let pg = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open metadata engine with a small event cap");
    let cursor_before_rejected_event = pg.flushed_event_seq.load(Ordering::SeqCst);
    let oversized_value = vec![b'x'; 8192];
    pg.set_xattr(ROOT_INO, b"user.sfs726", &oversized_value, 0)
        .expect("write oversized local event");
    let error = pg
        .flush_metadata()
        .expect_err("oversized local event must not be published with the small cap");
    assert!(matches!(error, MetaError::EventTooLarge { .. }));
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event
    );

    pg.create_file(ROOT_INO, b"ephemeral-after-gap", 0, 0, 0o644)
        .expect("create local-only file after the gap");
    pg.unlink(ROOT_INO, b"ephemeral-after-gap")
        .expect("delete local-only file after the gap");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event,
        "local-only deletion must not skip the oversized event"
    );

    pg.create_dir(ROOT_INO, b"small-independent-event", 0, 0, 0o755)
        .expect("create independent later event");
    pg.flush_metadata()
        .expect("publish the independent event while the oversized range remains parked");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event,
        "self-acknowledging later work must not move the cursor past the gap"
    );
    pg.prepare_for_close()
        .expect("close first local metadata engine");
    drop(pg);
    drop(low_limit);

    let _higher_limit = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "8388608");
    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("reopen with a larger event cap");
    assert_eq!(
        reopened.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event,
        "restart must reconstruct from the durable gap"
    );
    reopened
        .flush_metadata()
        .expect("publish the restored oversized event under the larger cap");
    let event_end = reopened
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read restored local event end");
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&reopened.local)
            .expect("load durable cursor after restored publication"),
        event_end
    );
    reopened
        .prepare_for_close()
        .expect("close reopened local metadata engine");
    drop(reopened);

    let verify_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let verify_db = verify_dir.path().join("verify.redb");
    seed_local(&verify_db, &volume.format_info());
    let verifier = PgMetaEngine::open_with_options(
        &verify_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open a fresh client to verify PostgreSQL publication");
    verifier
        .sync_metadata()
        .expect("replay published event on a fresh client");
    assert_eq!(
        verifier
            .get_xattr(ROOT_INO, b"user.sfs726")
            .expect("read published root xattr"),
        Some(oversized_value)
    );
    verifier
        .prepare_for_close()
        .expect("close verifying metadata engine");
}

#[test]
fn local_only_deletion_does_not_advance_cursor_past_rejected_event() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    let cursor_before_rejected_event =
        reject_oversized_test_event(&mut pg, b"oversized-before-local-delete");

    pg.create_file(ROOT_INO, b"ephemeral-local-file", 0, 0, 0o644)
        .expect("create local-only file");
    pg.unlink(ROOT_INO, b"ephemeral-local-file")
        .expect("delete local-only file");

    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event,
        "retiring later local-only events must not advance past the rejected event"
    );
}

#[test]
fn processed_local_ranges_stop_masking_foreign_dentries_across_rejected_gap() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    let cursor_before_gap = reject_oversized_test_event(&mut pg, b"overlay-gap");

    let completed_name = b"cancelled-after-gap";
    let completed_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read completed range start");
    pg.local
        .create_file(ROOT_INO, completed_name, 0, 0, 0o644)
        .expect("append completed local create");
    pg.local
        .unlink(ROOT_INO, completed_name)
        .expect("append completed local delete");
    let completed_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read completed range end");
    pg.complete_local_event_ranges(&[LocalEventRange {
        start: completed_start,
        end: completed_end,
    }])
    .expect("record the cancelled local range as completed");

    let published_name = b"published-after-gap";
    let published_start = completed_end;
    pg.local
        .create_dir(ROOT_INO, published_name, 0, 0, 0o755)
        .expect("append published local range");
    let event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read published range end");
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: published_start,
        end: event_end,
    }])
    .expect("mark local range as published");

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_gap,
        "the rejected event remains the durable cursor barrier"
    );
    assert!(
        overlay
            .xattr_keys
            .contains(&encode_xattr_key(ROOT_INO, b"user.sfs726.oversized")),
        "the rejected event should retain local authority"
    );
    assert!(
        !overlay
            .dentry_keys
            .contains(&encode_dentry_key(ROOT_INO, completed_name)),
        "a completed create/delete must not reserve its old dentry"
    );
    assert!(
        !overlay
            .dentry_keys
            .contains(&encode_dentry_key(ROOT_INO, published_name)),
        "a published range must not reserve its dentry while the cursor is behind the gap"
    );

    let remote_ino = ROOT_INO + 8_000;
    let remote_attr = InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755);
    apply_foreign_batch(
        &pg,
        &[
            MetaEvent::SetAttr { attr: remote_attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: completed_name.to_vec(),
                entry: DirEntryValue {
                    ino: remote_ino,
                    kind: FileType::Directory,
                },
            },
        ],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply foreign create for the completed name");
    assert_eq!(
        pg.lookup(ROOT_INO, completed_name)
            .expect("foreign create should become visible")
            .ino,
        remote_ino
    );
}

#[test]
fn completed_overlay_conflicts_are_removed_during_self_ack_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let event_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read self event start");
    pg.local
        .set_xattr(ROOT_INO, b"user.sfs726.self", b"published", 0)
        .expect("append self event");
    let self_event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read self event end");
    let self_events = pg
        .local
        .read_events_range(event_start, self_event_end)
        .expect("read self events");
    let self_batch = RemoteApplyCoordinator::project_local_batch(&self_events).events;
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: event_start,
        end: self_event_end,
    }])
    .expect("mark self range as published");

    let completed_name = b"completed-before-foreign-create";
    pg.local
        .create_file(ROOT_INO, completed_name, 0, 0, 0o644)
        .expect("append local create to be completed");
    pg.local
        .unlink(ROOT_INO, completed_name)
        .expect("append local delete to be completed");
    let next_event_seq = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read completed local suffix end");
    pg.complete_local_event_ranges(&[LocalEventRange {
        start: self_event_end,
        end: next_event_seq,
    }])
    .expect("record local create/delete as completed");

    let remote_ino = ROOT_INO + 8_001;
    let remote_attr = InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755);
    let outcome = pg
        .apply_replay_page_atomically(
            vec![
                DecodedReplayBatch {
                    batch_version: 1,
                    events: self_batch,
                    is_self_batch: true,
                },
                DecodedReplayBatch {
                    batch_version: 2,
                    events: vec![
                        MetaEvent::SetAttr { attr: remote_attr },
                        MetaEvent::AddEntry {
                            parent: ROOT_INO,
                            name: completed_name.to_vec(),
                            entry: DirEntryValue {
                                ino: remote_ino,
                                kind: FileType::Directory,
                            },
                        },
                    ],
                    is_self_batch: false,
                },
            ],
            None,
            RemoteReplayApplyContext {
                last_seq: 0,
                current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq: event_start,
                next_event_seq,
                initial_overlay: pg.local_overlay_conflicts().expect("local overlays"),
            },
        )
        .expect("apply self and foreign batches");

    assert_eq!(outcome.flushed_event_seq, Some(next_event_seq));
    assert_eq!(
        pg.lookup(ROOT_INO, completed_name)
            .expect("foreign create should be visible after completed local range")
            .ino,
        remote_ino
    );
}

#[test]
fn replay_ack_and_completed_suffix_persist_in_one_redb_transaction() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let event_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read replay event start");
    pg.local
        .set_xattr(ROOT_INO, b"user.sfs726.visible", b"published", 0)
        .expect("append visible event to be self-acknowledged");
    let acknowledged_cursor = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read self-ack event end");
    let visible_events = pg
        .local
        .read_events_range(event_start, acknowledged_cursor)
        .expect("read self-ack event");
    let self_batch = RemoteApplyCoordinator::project_local_batch(&visible_events).events;
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: event_start,
        end: acknowledged_cursor,
    }])
    .expect("mark self-ack range published");

    let completed_start = acknowledged_cursor;
    pg.local
        .add_pending_deletion(700, 700)
        .expect("append local-only completed event");
    let completed_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read completed event end");
    pg.complete_local_event_ranges(&[LocalEventRange {
        start: completed_start,
        end: completed_end,
    }])
    .expect("record local-only completion behind self-ack");

    let before = pg.local.metadata_write_observability();
    let outcome = pg
        .apply_replay_page_atomically(
            vec![DecodedReplayBatch {
                batch_version: 1,
                events: self_batch,
                is_self_batch: true,
            }],
            None,
            RemoteReplayApplyContext {
                last_seq: 0,
                current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq: event_start,
                next_event_seq: completed_end,
                initial_overlay: pg.local_overlay_conflicts().expect("local overlays"),
            },
        )
        .expect("apply self-ack and completed suffix");
    let after = pg.local.metadata_write_observability();

    assert_eq!(outcome.flushed_event_seq, Some(completed_end));
    assert_eq!(
        after.begin_write.calls - before.begin_write.calls,
        1,
        "self-ack and completed suffix should share the replay page transaction"
    );
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local).expect("read committed cursor"),
        completed_end
    );
}

#[test]
fn multiple_rejected_ranges_are_skipped_without_requeue() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    let first_gap = reject_oversized_test_event(&mut pg, b"first-oversized-range");
    let first_gap_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read first rejected range end");

    let published_start = first_gap_end;
    pg.local
        .create_dir(ROOT_INO, b"published-between-gaps", 0, 0, 0o755)
        .expect("append published event between rejected ranges");
    let published_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read intervening published range end");
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: published_start,
        end: published_end,
    }])
    .expect("mark intervening range published");

    reject_oversized_test_event(&mut pg, b"second-oversized-range");
    let second_gap_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read second rejected range end");
    let later_published_start = second_gap_end;
    pg.local
        .create_dir(ROOT_INO, b"published-after-gaps", 0, 0, 0o755)
        .expect("append published event after rejected ranges");
    let event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read later published range end");
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: later_published_start,
        end: event_end,
    }])
    .expect("mark later range published");

    let progress = pg
        .unprocessed_local_event_progress()
        .expect("read local event progress");
    assert_eq!(progress.rejected.len(), 2);
    assert_eq!(progress.rejected[0].start, first_gap);
    assert_eq!(progress.rejected[1].start, published_end);
    assert_eq!(
        progress.rejected.first().map(|range| range.start),
        Some(first_gap)
    );

    pg.restore_pending_state_from_local_log()
        .expect("restore after both rejected and published ranges");
    let pending = pg.pending_state.lock().expect("pending state");
    assert_eq!(pending.event_start, None);
    assert_eq!(pending.ops, 0);
}

#[test]
fn restoration_does_not_requeue_rejected_prefix_ahead_of_later_work() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    let cursor_before_rejected_event = reject_oversized_test_event(&mut pg, b"oversized-prefix");

    let later_event_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("next event seq");
    pg.create_dir(ROOT_INO, b"later-small-event", 0, 0, 0o755)
        .expect("create independent later event");
    let later_event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("next event seq");

    pg.restore_pending_state_from_local_log()
        .expect("restore the remaining local event ranges");

    let pending_state = pg.pending_state.lock().expect("pending state");
    assert_eq!(pending_state.event_start, Some(later_event_start));
    assert_eq!(pending_state.event_end, later_event_end);
    assert!(pending_state.ops > 0, "later range should remain pending");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event,
        "restoring later work must leave the durable cursor at the rejected prefix"
    );
}

#[test]
fn published_later_range_stays_pending_at_rejected_cursor_until_self_ack() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    let cursor_before_rejected_event =
        reject_oversized_test_event(&mut pg, b"oversized-before-publish");

    let later_event_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("next event seq");
    pg.create_dir(ROOT_INO, b"later-published-event", 0, 0, 0o755)
        .expect("create independent later event");
    let later_event_end = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("next event seq");
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: later_event_start,
        end: later_event_end,
    }])
    .expect("record the later range as published");

    pg.restore_pending_state_from_local_log()
        .expect("restore remaining local event ranges");

    let pending_state = pg.pending_state.lock().expect("pending state");
    assert_eq!(pending_state.event_start, None);
    assert_eq!(pending_state.ops, 0);
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        cursor_before_rejected_event,
        "publishing a later range must not advance the cursor across the gap"
    );
}

#[test]
fn pending_restore_does_not_resurrect_published_source_after_foreign_rename() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let start = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let attr = pg
        .create_dir(ROOT_INO, b"rename-source", 0, 0, 0o755)
        .unwrap();
    let end = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let entry = DirEntryValue {
        ino: attr.ino,
        kind: FileType::Directory,
    };
    pg.local
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"rename-source".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    pg.mark_local_event_ranges_published(&[LocalEventRange { start, end }])
        .unwrap();
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), start);

    let rename = MetaEvent::RenameEntry {
        parent: ROOT_INO,
        name: b"rename-source".to_vec(),
        new_parent: ROOT_INO,
        new_name: b"renamed-final".to_vec(),
        entry,
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: None,
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    };
    pg.local
        .apply_replica_events_to_base(std::slice::from_ref(&rename))
        .unwrap();
    pg.local
        .apply_replica_serving_state(std::slice::from_ref(&rename))
        .unwrap();
    assert!(pg.lookup(ROOT_INO, b"rename-source").is_err());

    pg.restore_pending_state_from_local_log().unwrap();
    assert!(pg.lookup(ROOT_INO, b"rename-source").is_err());
    assert!(pg.lookup(ROOT_INO, b"renamed-final").is_ok());
}

#[test]
fn self_ack_with_contextually_filtered_prefix_preserves_local_overlay() {
    let (_dir, mut pg) = temp_pg_engine_for_unit();
    pg.remote_format_validated.store(true, Ordering::Release);
    pg.sync_max_response_bytes_per_page = 1;
    let local_ino = ROOT_INO + 7_000;
    let mut base_attr = InodeAttr::new_file(local_ino, ROOT_INO, 0, 0, 0o644);
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: base_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"published-base-file".to_vec(),
                entry: DirEntryValue {
                    ino: local_ino,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .expect("seed published regular-file base without local journal events");

    let cursor_before_gap = pg.flushed_event_seq.load(Ordering::SeqCst);
    pg.local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ, cursor_before_gap)
        .expect("persist the initial local event cursor");
    let link_name = vec![b'l'; 240];
    let gap_start = pg.local.get_counter(NEXT_EVENT_SEQ).expect("event start");
    pg.local
        .add_entry(
            ROOT_INO,
            &link_name,
            &DirEntryValue {
                ino: local_ino,
                kind: FileType::RegularFile,
            },
        )
        .expect("append local hardlink event");
    let gap_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("event end");
    pg.mark_pending_after_mutation(gap_start, gap_end, PendingMutationHint::Unknown)
        .expect("queue hardlink event");
    let rejected = pg
        .claim_pending_batch(true)
        .expect("oversized hardlink should be claimable");
    let error = match pg.flush_pending_claimed_batch_with_restore(rejected) {
        Ok(_) => panic!("oversized hardlink event must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, MetaError::EventTooLarge { .. }));

    base_attr.size = 64;
    base_attr.content_epoch = 1;
    base_attr.upload_state = UploadState::Uploading;
    pg.set_attr(&base_attr)
        .expect("append later unready file attr that filters the rejected dentry");
    pg.sync_max_response_bytes_per_page = 8 * 1024 * 1024;
    let self_event_start = pg
        .local
        .get_counter(NEXT_EVENT_SEQ)
        .expect("self event start");
    pg.create_dir(ROOT_INO, b"later-independent-directory", 0, 0, 0o755)
        .expect("create later independent directory");
    let event_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("event end");

    let self_events = pg
        .local
        .read_events_range(self_event_start, event_end)
        .expect("read later self events");
    let projected_self = RemoteApplyCoordinator::project_local_batch(&self_events);
    let all_unflushed = pg
        .local
        .read_events_range(cursor_before_gap, event_end)
        .expect("read full local prefix");
    assert_eq!(
        RemoteApplyCoordinator::project_local_batch(&all_unflushed).events,
        projected_self.events,
        "the later self-batch can match a prefix whose rejected regular-file dentry is hidden by newer local state"
    );
    pg.mark_local_event_ranges_published(&[LocalEventRange {
        start: self_event_start,
        end: event_end,
    }])
    .expect("record later self-batch as published");

    let remote_ino = ROOT_INO + 7_001;
    let remote_attr = InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755);
    let outcome = pg
        .apply_replay_page_atomically(
            vec![
                DecodedReplayBatch {
                    batch_version: 1,
                    events: projected_self.events,
                    is_self_batch: true,
                },
                DecodedReplayBatch {
                    batch_version: 2,
                    events: vec![
                        MetaEvent::SetAttr { attr: remote_attr },
                        MetaEvent::AddEntry {
                            parent: ROOT_INO,
                            name: link_name.clone(),
                            entry: DirEntryValue {
                                ino: remote_ino,
                                kind: FileType::Directory,
                            },
                        },
                    ],
                    is_self_batch: false,
                },
            ],
            None,
            RemoteReplayApplyContext {
                last_seq: 0,
                current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq: cursor_before_gap,
                next_event_seq: event_end,
                initial_overlay: pg.local_overlay_conflicts().expect("local overlays"),
            },
        )
        .expect("apply later self and foreign batches");

    assert_eq!(
        pg.local
            .lookup(ROOT_INO, &link_name)
            .expect("local hardlink must remain visible")
            .ino,
        local_ino,
        "a self-ack across a rejected gap must not trim overlay conflicts before a later foreign batch"
    );
    assert_eq!(outcome.flushed_event_seq, None);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local).expect("read durable cursor"),
        cursor_before_gap
    );
}

#[test]
fn restore_pending_state_from_local_log_replays_unflushed_serving_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"lww-winner.txt", 0, 0, 0o644)
        .expect("create file");
    let setup_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    let setup_events = pg
        .local
        .read_events_range(setup_start, setup_end)
        .expect("read setup events");
    pg.local
        .apply_replica_events_to_base(&setup_events)
        .expect("publish setup to local base");
    pg.persist_flushed_event_seq(setup_end)
        .expect("mark setup flushed");

    let mut remote_empty = pg.get_attr(file.ino).expect("remote empty attr");
    remote_empty.size = 0;
    remote_empty.committed_size = 0;
    remote_empty.content_epoch = 2;
    remote_empty.committed_content_epoch = 2;
    remote_empty.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_empty.clone(),
        }])
        .expect("apply empty remote base attr");
    pg.local
        .apply_replica_serving_state(&[MetaEvent::SetAttr {
            attr: remote_empty.clone(),
        }])
        .expect("apply empty remote serving attr");

    let winner_payload = PayloadRef::new_partial(150_003, 271);
    pg.stage_payload_slice(file.ino, 0, &winner_payload)
        .expect("stage local winner payload");
    let mut local_winner = remote_empty.clone();
    local_winner.size = 271;
    local_winner.content_epoch = 3;
    local_winner.committed_content_epoch = 2;
    local_winner.upload_state = UploadState::Updating;
    pg.set_attr(&local_winner).expect("set local winner attr");
    assert_eq!(pg.get_attr(file.ino).expect("winner visible").size, 271);

    let mut reconcile = HashSet::new();
    reconcile.insert(file.ino);
    pg.local
        .reconcile_regular_file_serving_from_base(&reconcile)
        .expect("simulate startup remote reconcile over serving state");
    assert_eq!(
        pg.get_attr(file.ino).expect("remote attr visible").size,
        0,
        "remote catch-up can leave serving state stale before pending replay"
    );

    pg.restore_pending_state_from_local_log()
        .expect("restore pending local log");
    let restored = pg.get_attr(file.ino).expect("restored winner attr");
    assert_eq!(restored.size, local_winner.size);
    assert_eq!(restored.content_epoch, local_winner.content_epoch);
    let spans = pg
        .lookup_regular_file_slices(file.ino, 0, local_winner.size)
        .expect("restored winner slices");
    assert!(
        spans
            .iter()
            .any(|span| span.payload.as_ref().is_some_and(|payload| {
                payload.object_id == winner_payload.object_id
                    && payload.length == winner_payload.length
            })),
        "pending log replay must restore the unflushed local payload overlay"
    );
}

#[test]
fn restore_pending_state_keeps_newer_published_append_over_stale_updating_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = create_published_small_file(&pg, ROOT_INO, b"published-append-replay.txt", 77_031);
    let setup_end = pg.local.get_counter(NEXT_EVENT_SEQ).expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("acknowledge published setup");

    let mut stale_updating = pg.local.get_attr(file.ino).expect("published attr");
    stale_updating.size = 232;
    stale_updating.committed_size = 232;
    stale_updating.content_epoch = 2;
    stale_updating.committed_content_epoch = 1;
    stale_updating.upload_state = UploadState::Updating;
    pg.local
        .set_attr(&stale_updating)
        .expect("append stale updating attr to local journal");

    let head = PayloadRef::new_partial(77_032, 232);
    let tail = PayloadRef::new_partial(77_033, 206);
    let mut ready = stale_updating.clone();
    ready.size = 438;
    ready.committed_size = 438;
    ready.committed_content_epoch = 2;
    ready.upload_state = UploadState::Ready;
    let published = [
        MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: vec![(
                ChunkId(0),
                vec![
                    SliceRecord {
                        payload_state: PayloadRefState::Published,
                        ..SliceRecord::new(
                            SliceId(1),
                            SliceOrder(1),
                            SliceRange::new(0, 232).expect("head range"),
                            head,
                        )
                    },
                    SliceRecord {
                        payload_state: PayloadRefState::Published,
                        ..SliceRecord::new(
                            SliceId(2),
                            SliceOrder(2),
                            SliceRange::new(232, 438).expect("tail range"),
                            tail,
                        )
                    },
                ],
            )],
        },
        MetaEvent::SetAttr {
            attr: ready.clone(),
        },
    ];
    pg.local
        .apply_replica_events_to_base(&published)
        .expect("apply remote append to base");
    pg.local
        .apply_replica_serving_state(&published)
        .expect("apply remote append to serving state");

    pg.restore_pending_state_from_local_log()
        .expect("restore pending local log");

    assert_eq!(pg.local.get_attr(file.ino).expect("final attr"), ready);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, ready.size).expect("final payload IDs"),
        vec![head.object_id, tail.object_id],
        "replaying the stale local attr must keep both published append payloads"
    );
}

#[test]
fn pg_event_insert_chunking_covers_all_events() {
    let events = vec![MetaEvent::RemoveAttr { ino: 1 }; 1_025];
    let chunk_sizes: Vec<usize> = events
        .chunks(PG_EVENT_INSERT_CHUNK_SIZE)
        .map(|chunk| chunk.len())
        .collect();

    assert_eq!(chunk_sizes, vec![512, 512, 1]);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn hidden_flush_tail_is_republished_in_next_batch_and_acknowledged_after_remount() {
    let dsn = pg_dsn().expect("PostgreSQL regression requires STUDIOFS_TEST_PG_DSN");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-hidden-flush-tail-ack");
    seed_local(&path, &volume.format_info());
    let options = PgOpenOptions {
        background_poll: false,
        ..Default::default()
    };
    let pg = PgMetaEngine::open_with_options(&path, options).expect("open writer");
    let published = uploaded_pending_file(&pg, b"published.bin", 95_001);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(published.ino, published.content_epoch)
            .unwrap()
    );
    let base_inodes = redb::TableDefinition::<u64, &[u8]>::new("base_inodes");
    let root_base_before_create = pg
        .local
        .begin_read_txn()
        .unwrap()
        .open_table(base_inodes)
        .unwrap()
        .get(ROOT_INO)
        .unwrap()
        .map(|row| row.value().to_vec());
    // The still-uploading create is the hidden tail of the first flush.
    let tail = pg
        .create_file(ROOT_INO, b"hidden-tail.bin", 0, 0, 0o644)
        .unwrap();
    pg.request_pending_flush();
    pg.flush_due_pending().expect("flush published prefix");
    pg.sync_metadata().expect("acknowledge first batch");
    let cursor_after_first_batch = pg.flushed_event_seq.load(Ordering::SeqCst);
    let head_after_first_batch = pg.remote_metadata_head_version().unwrap();

    let payload = PayloadRef::new_partial(95_002, 152);
    pg.stage_payload_slice(tail.ino, 0, &payload).unwrap();
    set_file_state(&pg, tail.ino, 152, 1);
    let tail = pg.get_attr(tail.ino).unwrap();
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(
            tail.ino,
            tail.content_epoch,
            payload.object_id
        )
        .unwrap()
    );
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(tail.ino, tail.content_epoch)
            .unwrap()
    );
    pg.request_pending_flush();
    pg.flush_due_pending()
        .expect("flush hidden tail publication");
    pg.sync_metadata().expect("acknowledge second batch");
    let next_event_seq = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let committed_snapshot = pg
        .local_outbound_ack_snapshot()
        .expect("read local outbound acknowledgment");
    assert!(committed_snapshot.is_acknowledged());
    assert_eq!(committed_snapshot.local_event_end, next_event_seq);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local).unwrap(),
        next_event_seq,
        "every published local event must be acknowledged"
    );
    let head = pg.remote_metadata_head_version().unwrap();
    assert!(
        head > head_after_first_batch,
        "the hidden tail must be published by the next batch"
    );
    pg.prepare_for_close().expect("close writer workers");
    pg.flush_local_metadata().expect("persist local metadata");
    drop(pg);

    // A crash before the second self-ack leaves only the first batch's
    // cursor durable, and redb repair replays the remote tail from zero.
    // Such a crash also leaves local overlay authority over the remote base,
    // which makes the reopen preserve the journal. Self-acks already promoted
    // the tail create's parent update, so restore the root base from before it.
    {
        let local = RedbMetaEngine::open(&path).unwrap();
        let txn = local.begin_write_txn().unwrap();
        {
            let mut table = txn.open_table(base_inodes).unwrap();
            match &root_base_before_create {
                Some(row) => table.insert(ROOT_INO, row.as_slice()).map(drop),
                None => table.remove(ROOT_INO).map(drop),
            }
            .unwrap();
        }
        txn.commit().unwrap();
        local
            .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ, cursor_after_first_batch)
            .unwrap();
        local.set_counter(LOCAL_PG_VERSION_COUNTER, 0).unwrap();
        local
            .set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, 0)
            .unwrap();
    }
    let reopened = PgMetaEngine::open_with_options(&path, options).expect("reopen writer");
    let reopened_snapshot = reopened
        .local_outbound_ack_snapshot()
        .expect("read reopened outbound acknowledgment");
    assert!(!reopened_snapshot.is_acknowledged());
    assert_eq!(reopened_snapshot.local_event_end, next_event_seq);
    for _ in 0..16 {
        if reopened.local_version.load(Ordering::SeqCst) >= head {
            break;
        }
        reopened.sync_metadata().expect("replay self batches");
    }
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&reopened.local).unwrap(),
        next_event_seq,
        "replayed self batches must acknowledge the whole published journal"
    );
    assert!(
        reopened
            .local_outbound_ack_snapshot()
            .expect("read replayed outbound acknowledgment")
            .is_acknowledged()
    );
    reopened.request_pending_flush();
    reopened.flush_due_pending().expect("flush after remount");
    assert_eq!(
        reopened.remote_metadata_head_version().unwrap(),
        head,
        "already-published events must not be republished"
    );
    reopened.prepare_for_close().expect("close reopened writer");
}

#[test]
fn hidden_pending_window_grows_without_reprojecting_unpublished_overlay_history() {
    use crate::engine::MetaEngine;
    use crate::pg_engine::remote_apply::LOCAL_PROJECTION_WORK;
    use crate::schema::{
        ChunkId, PayloadRef, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
    };

    let (_dir, pg) = temp_pg_engine_for_unit();
    let mut attr = pg
        .create_file(ROOT_INO, b"hidden-window.bin", 0, 0, 0o644)
        .expect("create unpublished file");
    attr.content_epoch = 1;
    let window_start = pg
        .pending_state
        .lock()
        .expect("pending state")
        .event_start
        .expect("create keeps a local-only window");

    let mut work_per_round = Vec::new();
    for round in 0..32u32 {
        let start = round * 4096;
        let object_id = 97_000 + u64::from(round);
        attr.size = u64::from(start + 4096);
        let slice = SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(start, start + 4096).expect("slice range"),
            payload: Some(PayloadRef::new_partial(object_id, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        };
        LOCAL_PROJECTION_WORK.with(|work| work.set(0));
        pg.flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(ChunkId(0), vec![slice])],
            Some(&attr),
        )
        .expect("flush random-write overlay");
        assert!(
            pg.mark_regular_file_overlay_object_uploaded_if_matches(attr.ino, 1, object_id)
                .expect("mark uploaded")
        );
        work_per_round.push(LOCAL_PROJECTION_WORK.with(std::cell::Cell::get));
    }

    let after_seq = pg.local.get_counter(NEXT_EVENT_SEQ).expect("after seq");
    {
        let state = pg.pending_state.lock().expect("pending state");
        assert_eq!(
            state.ops, 0,
            "an unpublished file has no remote-visible work"
        );
        assert_eq!(state.event_start, Some(window_start));
        assert_eq!(
            state.event_end, after_seq,
            "the window must keep every event"
        );
    }
    let first = work_per_round[1];
    let last = *work_per_round.last().expect("rounds ran");
    assert!(
        last <= first,
        "each mutation must cost the same projection work however long the hidden window \
         grows: {work_per_round:?}"
    );

    pg.create_dir(ROOT_INO, b"visible-after-hidden-window", 0, 0, 0o755)
        .expect("create visible directory");
    assert!(
        pg.pending_state.lock().expect("pending state").ops > 0,
        "a remote-visible mutation after the hidden window must still be counted"
    );
}

#[test]
fn own_rename_self_batch_leaves_serving_view_unreconciled() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let dir = pg
        .create_dir(ROOT_INO, b"own-rename-dir", 0, 0, 0o755)
        .expect("create directory");
    pg.create_dir(dir.ino, b"child", 0, 0, 0o755)
        .expect("create child");
    let start = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    pg.local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ, start)
        .unwrap();
    pg.rename(dir.ino, b"child", dir.ino, b"child-moved")
        .expect("rename child");
    let end = pg.local.get_counter(NEXT_EVENT_SEQ).unwrap();
    let projected = RemoteApplyCoordinator::project_local_batch(
        &pg.local.read_events_range(start, end).unwrap(),
    );
    assert!(
        projected
            .events
            .iter()
            .any(|event| matches!(event, MetaEvent::RenameEntry { .. }))
    );
    let prefix = vec![LocalEventRange { start, end }];
    pg.mark_local_event_ranges_published_with_prefix(&prefix, None)
        .unwrap();

    let outcome = pg
        .apply_replay_page_atomically(
            vec![DecodedReplayBatch {
                batch_version: pg.local_version.load(Ordering::SeqCst) + 1,
                events: projected.events,
                is_self_batch: true,
            }],
            None,
            RemoteReplayApplyContext {
                last_seq: pg.local_version.load(Ordering::SeqCst),
                current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq: pg.flushed_event_seq.load(Ordering::SeqCst),
                next_event_seq: end,
                initial_overlay: pg.local_overlay_conflicts().unwrap(),
            },
        )
        .expect("apply own rename batch");

    assert!(
        !outcome.serving_reconciled,
        "mirroring this client's own rename must not count as a serving correction"
    );
    assert!(pg.local.lookup(dir.ino, b"child-moved").is_ok());
    assert!(pg.local.lookup(dir.ino, b"child").is_err());
}

#[test]
fn unchanged_self_page_restore_skips_serving_replay_unless_a_cleared_inode_is_pending() {
    use crate::redb_engine::take_pending_serving_replays;

    let (_dir, pg) = temp_pg_engine_for_unit();
    let mut pending_inodes = Vec::new();
    for index in 0..16u32 {
        let attr = pg
            .create_dir(ROOT_INO, format!("backlog-{index}").as_bytes(), 0, 0, 0o755)
            .expect("create backlog directory");
        pending_inodes.push(attr.ino);
    }
    let expected_ops = pg.pending_state.lock().expect("pending state").ops;
    assert!(expected_ops > 0);

    let (position_guard, mut state) = pg
        .lock_local_position_and_pending_state()
        .expect("replay locks");
    take_pending_serving_replays();
    pg.restore_pending_state_after_self_page_with_state(&mut state, &HashSet::from([u64::MAX]))
        .expect("restore after an unchanged self page");
    assert_eq!(
        take_pending_serving_replays(),
        0,
        "pending events already served must not be replayed"
    );
    assert_eq!(
        state.ops, expected_ops,
        "pending accounting is still restored"
    );

    pg.restore_pending_state_after_self_page_with_state(
        &mut state,
        &HashSet::from([pending_inodes[3]]),
    )
    .expect("restore after a page that cleared a pending inode");
    assert_eq!(
        take_pending_serving_replays(),
        1,
        "a pending event on an inode whose graph the page cleared must be replayed"
    );
    drop(state);
    drop(position_guard);
    assert!(pg.local.lookup(ROOT_INO, b"backlog-15").is_ok());
}

#[test]
fn visible_pending_window_extends_over_events_of_an_unmarked_concurrent_mutation() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    for index in 0..8u32 {
        pg.create_dir(ROOT_INO, format!("dir-{index}").as_bytes(), 0, 0, 0o755)
            .expect("create visible directory");
    }
    pg.rename(ROOT_INO, b"dir-0", ROOT_INO, b"moved-0")
        .expect("rename seeds the visible window cache");
    // A concurrent mutation commits but has not marked its range yet.
    pg.local
        .rename(ROOT_INO, b"dir-1", ROOT_INO, b"moved-1")
        .expect("unmarked local rename");
    pg.rename(ROOT_INO, b"dir-2", ROOT_INO, b"moved-2")
        .expect("rename after the unmarked mutation");

    let (event_start, event_end, ops) = {
        let state = pg.pending_state.lock().expect("pending state");
        (
            state.event_start.expect("visible window"),
            state.event_end,
            state.ops,
        )
    };
    let after_seq = pg.local.get_counter(NEXT_EVENT_SEQ).expect("after seq");
    assert_eq!(
        event_end, after_seq,
        "the window must cover the unmarked events"
    );
    let events = pg
        .local
        .read_events_range(event_start, after_seq)
        .expect("read window");
    assert_eq!(
        ops,
        RemoteApplyCoordinator::project_local_batch(&events).visible_ops,
        "the extended count must match a full projection of the window"
    );
}

#[test]
fn visible_pending_window_grows_without_reprojecting_published_namespace_history() {
    use crate::pg_engine::remote_apply::LOCAL_PROJECTION_WORK;

    let (_dir, pg) = temp_pg_engine_for_unit();
    for index in 0..64u32 {
        pg.create_dir(ROOT_INO, format!("dir-{index}").as_bytes(), 0, 0, 0o755)
            .expect("create visible directory");
    }
    let window_start = pg
        .pending_state
        .lock()
        .expect("pending state")
        .event_start
        .expect("directories keep a visible window");

    let mut work_per_rename = Vec::new();
    for index in 0..64u32 {
        LOCAL_PROJECTION_WORK.with(|work| work.set(0));
        pg.rename(
            ROOT_INO,
            format!("dir-{index}").as_bytes(),
            ROOT_INO,
            format!("moved-{index}").as_bytes(),
        )
        .expect("rename visible directory");
        work_per_rename.push(LOCAL_PROJECTION_WORK.with(std::cell::Cell::get));
    }

    let after_seq = pg.local.get_counter(NEXT_EVENT_SEQ).expect("after seq");
    let events = pg
        .local
        .read_events_range(window_start, after_seq)
        .expect("read window");
    let full = RemoteApplyCoordinator::project_local_batch(&events);
    {
        let state = pg.pending_state.lock().expect("pending state");
        assert_eq!(state.event_start, Some(window_start));
        assert_eq!(state.event_end, after_seq);
        assert_eq!(
            state.ops, full.visible_ops,
            "the incremental count must match a full projection of the window"
        );
    }
    let first = work_per_rename[1];
    let last = *work_per_rename.last().expect("renames ran");
    assert!(
        last <= first,
        "each mutation must cost the same projection work however long the visible window \
         grows: {work_per_rename:?}"
    );
}

#[test]
fn poisoned_hidden_window_cache_falls_back_without_failing_the_mutation() {
    use crate::engine::MetaEngine;
    use crate::schema::{
        ChunkId, PayloadRef, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
    };

    let (_dir, pg) = temp_pg_engine_for_unit();
    let mut attr = pg
        .create_file(ROOT_INO, b"poisoned-window.bin", 0, 0, 0o644)
        .expect("create unpublished file");
    attr.content_epoch = 1;
    let flush = |round: u32, attr: &mut InodeAttr| {
        let start = round * 4096;
        attr.size = u64::from(start + 4096);
        let slice = SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(start, start + 4096).expect("slice range"),
            payload: Some(PayloadRef::new_partial(98_000 + u64::from(round), 4096)),
            payload_state: PayloadRefState::LocalOnly,
        };
        pg.flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(ChunkId(0), vec![slice])],
            Some(attr),
        )
    };
    flush(0, &mut attr).expect("first flush caches the hidden window");
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = pg.pending_hidden_window.lock().expect("window cache");
        panic!("poison the hidden-window cache");
    }));

    flush(1, &mut attr).expect("a poisoned optimization cache must not fail the mutation");
    flush(2, &mut attr).expect("the rebuilt cache keeps serving later mutations");

    let after_seq = pg.local.get_counter(NEXT_EVENT_SEQ).expect("after seq");
    let state = pg.pending_state.lock().expect("pending state");
    assert_eq!(state.ops, 0);
    assert_eq!(
        state.event_end, after_seq,
        "the window must keep every event"
    );
    drop(state);
    assert!(
        !pg.pending_hidden_window.is_poisoned(),
        "the fallback must clear the poison"
    );
}

/// SFS-929 with real PostgreSQL: the bootstrap checkpoint predates a
/// published parent directory, and an uploaded child is unpublished at the
/// crash. Repair startup must judge the child against head, not the checkpoint.
#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_redb_repair_restores_unpublished_child_of_dir_published_after_checkpoint() {
    let dsn = pg_dsn().expect("PostgreSQL regression requires STUDIOFS_TEST_PG_DSN");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-sfs929-repair-child-after-checkpoint");
    seed_local(&path, &volume.format_info());
    let options = PgOpenOptions {
        background_poll: false,
        ..Default::default()
    };
    let pg = PgMetaEngine::open_with_options(&path, options).expect("open writer");
    pg.create_file(ROOT_INO, b"before-checkpoint", 0, 0, 0o644)
        .expect("seed entry");
    pg.pending_state.lock().unwrap().force_flush = true;
    pg.flush_due_pending().expect("publish seed entry");
    pg.write_checkpoint_with_lease()
        .expect("publish checkpoint before parent");
    let parent = pg
        .create_dir(ROOT_INO, b"after-checkpoint", 0, 0, 0o755)
        .expect("create parent");
    pg.pending_state.lock().unwrap().force_flush = true;
    pg.flush_due_pending()
        .expect("publish parent after checkpoint");
    assert_eq!(pg.replication_backlog().total_ops(), 0, "parent published");

    let name = b"unpublished-child.bin";
    let file = pg
        .create_file(parent.ino, name, 0, 0, 0o644)
        .expect("create child");
    let payload = PayloadRef::new_partial(92_929, 152);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage child payload");
    set_file_state(&pg, file.ino, 152, 1);
    let accepted = pg.get_attr(file.ino).expect("child attr");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(
            file.ino,
            accepted.content_epoch,
            payload.object_id,
        )
        .expect("mark child uploaded")
    );
    // The crash happens before the child create reaches PostgreSQL; model the
    // lost local journal tail the SFS-929 evidence showed.
    let next = pg.local.get_counter(NEXT_EVENT_SEQ).expect("next event");
    pg.persist_flushed_event_seq(next)
        .expect("acknowledge local tail");
    pg.prune_local_event_log_once().expect("prune local tail");
    drop(pg);

    studiofs_test_support::mark_redb_repair_required(&path).expect("mark repair");
    {
        let redb = RedbMetaEngine::open(&path).expect("redb auto-repair");
        redb.set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)
            .expect("set repair marker");
    }
    let pg = PgMetaEngine::open_with_options(&path, options).expect("repair reopen");

    assert_eq!(
        pg.lookup(parent.ino, name).expect("child restored").ino,
        file.ino
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, accepted.size).expect("restored payload"),
        vec![payload.object_id]
    );
    assert_eq!(
        pg.local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .expect("repair marker"),
        0
    );
    assert_ne!(pg.pg_sync_status().lifecycle, PgSyncLifecycle::Degraded);
    pg.prepare_for_close().expect("close repaired writer");
}
