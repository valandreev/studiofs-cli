use super::*;
use crate::redb_engine::layout::encode_xattr_key;
use std::sync::Barrier;
use std::thread;

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
    let start_seq = pg.local.get_counter("nextEventSeq").unwrap();

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

    let end_seq = pg.local.get_counter("nextEventSeq").unwrap();
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
        .get_counter("nextEventSeq")
        .expect("read root event end");
    pg.local
        .create_dir(ROOT_INO, b"after-root", 0, 0, 0o755)
        .expect("create local directory after root setup");
    let event_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read local event end");
    pg.local
        .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, 0)
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
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
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
        .get_counter("nextEventSeq")
        .expect("read create start");
    pg.create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create unpublished file");
    let create_end = pg
        .local
        .get_counter("nextEventSeq")
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
        .get_counter("nextEventSeq")
        .expect("read deletion start");

    pg.unlink(ROOT_INO, b"draft.txt")
        .expect("unlink unpublished file");
    pg.unlink(ROOT_INO, b"draft-2.txt")
        .expect("unlink second unpublished file");

    let deletion_end = pg
        .local
        .get_counter("nextEventSeq")
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
        .get_counter("nextEventSeq")
        .expect("read before seq");
    pg.local
        .remove_attr(file.ino)
        .expect("remove unpublished file attributes");
    let after_seq = pg
        .local
        .get_counter("nextEventSeq")
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
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("acknowledge published setup");
    {
        let mut state = pg.pending_state.lock().expect("pending state");
        PgMetaEngine::clear_pending_state(&mut state);
        pg.local
            .remove_regular_file_graph(file.ino)
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

    pg.remove_regular_file_graph(file.ino)
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
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("acknowledge published setup");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    pg.remove_regular_file_graph(file.ino)
        .expect("remove published file graph");

    let graph_end = pg.local.get_counter("nextEventSeq").expect("graph end");
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
    let start = pg.local.get_counter("nextEventSeq").expect("baseline seq");

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

    let end = pg.local.get_counter("nextEventSeq").expect("end seq");
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
        pg.local.get_counter("nextEventSeq").expect("event end"),
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
    let end = pg.local.get_counter("nextEventSeq").expect("end seq");
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
        .get_counter("nextEventSeq")
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
fn upload_progress_only_mutation_does_not_create_pg_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"upload-progress-only.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let payload = PayloadRef::new_partial(77_001, 4096);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");

    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
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
        .get_counter("nextEventSeq")
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
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"repair-uploaded-overlay.bin", 0, 0, 0o644)
        .expect("create file");
    let create_end = pg.local.get_counter("nextEventSeq").expect("create end");
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

    let accepted_end = pg.local.get_counter("nextEventSeq").expect("accepted end");
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
        .get_counter("nextEventSeq")
        .expect("uploaded-marker end");
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        acknowledged_end,
        "the local-only uploaded marker is deliberately acknowledged without a PG backlog"
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
            .is_empty(),
        "without repair replay startup cleanup would classify the uploaded object as stale"
    );

    pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
        .expect("restore acknowledged uploaded overlay");

    let restored = pg.get_attr(file.ino).expect("restored attr");
    assert_eq!(restored.size, accepted.size);
    assert_eq!(restored.content_epoch, accepted.content_epoch);
    assert_eq!(
        restored.committed_content_epoch,
        accepted.committed_content_epoch
    );
    assert_eq!(restored.upload_state, accepted.upload_state);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, restored.size).expect("restored object ids"),
        vec![payload.object_id],
        "repair must restore the uploaded extent before VFS startup cleanup"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        acknowledged_end,
        "repair replay must not reopen already-flushed metadata for PG publication"
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
    let create_end = pg.local.get_counter("nextEventSeq").expect("create end");
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
    let accepted_end = pg.local.get_counter("nextEventSeq").expect("accepted end");
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
    let acknowledged_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("uploaded-marker end");

    let mut newer_remote = accepted.clone();
    newer_remote.size = 0;
    newer_remote.committed_size = 0;
    newer_remote.content_epoch = accepted.content_epoch;
    newer_remote.committed_content_epoch = accepted.content_epoch;
    newer_remote.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_serving_state(&[
            MetaEvent::RemoveAttr { ino: file.ino },
            MetaEvent::SetAttr {
                attr: newer_remote.clone(),
            },
        ])
        .expect("simulate newer remote state after repair");

    pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
        .expect("attempt repair replay");

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
            .is_empty(),
        "the stale uploaded extent must stay unreachable under the newer remote state"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        acknowledged_end,
        "guarded replay must leave the flushed cursor untouched"
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
    pg.local
        .import_remote_snapshot_preserving_local_event_log(&stale_snapshot)
        .expect("replace serving state while retaining event log");

    pg.mark_regular_file_overlay_object_uploaded_if_matches(
        file.ino,
        accepted.content_epoch,
        second.object_id,
    )
    .expect("mark second payload uploaded");

    pg.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()
        .expect("restore acknowledged uploaded overlay");

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
    let published_end = pg.local.get_counter("nextEventSeq").expect("published end");
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
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
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
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
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
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
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
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
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

    pg.create_dir(ROOT_INO, b"oversized-pending-event", 0, 0, 0o755)
        .expect("create pending event");
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
fn restore_pending_state_from_local_log_replays_unflushed_serving_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"lww-winner.txt", 0, 0, 0o644)
        .expect("create file");
    let setup_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
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
fn pg_event_insert_chunking_covers_all_events() {
    let events = vec![MetaEvent::RemoveAttr { ino: 1 }; 1_025];
    let chunk_sizes: Vec<usize> = events
        .chunks(PG_EVENT_INSERT_CHUNK_SIZE)
        .map(|chunk| chunk.len())
        .collect();

    assert_eq!(chunk_sizes, vec![512, 512, 1]);
}
