use super::*;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

#[test]
fn replay_page_apply_and_local_ack_share_the_canonical_lock_order() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_file(ROOT_INO, b"cancelled-local-file", 0, 0, 0o644)
        .expect("create local file");
    pg.unlink(ROOT_INO, b"cancelled-local-file")
        .expect("unlink unpublished local file");

    let event_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read local event end");
    let expected_state = *pg.pending_state.lock().expect("pending state");
    let context = RemoteReplayApplyContext {
        last_seq: 0,
        current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
        flushed_event_seq: pg.flushed_event_seq.load(Ordering::SeqCst),
        next_event_seq: event_end,
        initial_overlay: pg.local_overlay_conflicts().expect("local overlays"),
    };
    let remote_ino = ROOT_INO + 900;
    let batch = DecodedReplayBatch {
        batch_version: 1,
        events: vec![
            MetaEvent::SetAttr {
                attr: InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"remote-during-local-ack".to_vec(),
                entry: DirEntryValue {
                    ino: remote_ino,
                    kind: FileType::Directory,
                },
            },
        ],
        is_self_batch: false,
    };

    let (hook_tx, hook_rx) = mpsc::channel();
    let hook_barrier = Arc::new(Barrier::new(2));
    let worker_hook_barrier = Arc::clone(&hook_barrier);
    pg.set_catch_up_fault_hook(Arc::new(move |point| {
        if matches!(point, CatchUpFaultPoint::BeforeLocalCommit { .. }) {
            hook_tx.send(()).expect("signal replay commit boundary");
            worker_hook_barrier.wait();
        }
        Ok(())
    }));

    let pg = Arc::new(pg);
    let pending_guard = pg.pending_state.lock().expect("hold pending state");
    let replay_start = Arc::new(Barrier::new(2));
    let worker_replay_start = Arc::clone(&replay_start);
    let replay_pg = Arc::clone(&pg);
    let (replay_tx, replay_rx) = mpsc::channel();
    let replay_worker = thread::spawn(move || {
        worker_replay_start.wait();
        let result = replay_pg.apply_replay_page_atomically(vec![batch], None, context);
        replay_tx.send(result).expect("send replay result");
    });
    replay_start.wait();

    // With the canonical order, replay owns local_position_lock and waits for
    // this test's pending_state guard before it reaches the local commit hook.
    let replay_reached_commit_while_pending_was_held =
        hook_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    let ack_start = Arc::new(Barrier::new(2));
    let worker_ack_start = Arc::clone(&ack_start);
    let ack_pg = Arc::clone(&pg);
    let (ack_tx, ack_rx) = mpsc::channel();
    let ack_worker = thread::spawn(move || {
        worker_ack_start.wait();
        let result = ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_end);
        ack_tx.send(result).expect("send acknowledgement result");
    });
    ack_start.wait();

    if replay_reached_commit_while_pending_was_held {
        // Release the pre-fix implementation safely so the test can report
        // the ordering assertion instead of leaking a blocked worker.
        hook_barrier.wait();
    }
    drop(pending_guard);

    if !replay_reached_commit_while_pending_was_held {
        hook_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("replay reaches its commit boundary after pending state is released");
        hook_barrier.wait();
    }

    let replay_outcome = replay_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("replay page completes")
        .expect("apply replay page");
    ack_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("local acknowledgement completes")
        .expect("acknowledge pending state");
    replay_worker.join().expect("join replay worker");
    ack_worker.join().expect("join acknowledgement worker");

    assert!(
        !replay_reached_commit_while_pending_was_held,
        "replay must acquire local_position_lock before pending_state"
    );
    assert!(replay_outcome.changed, "replay page must apply its batch");
    assert!(
        pg.local
            .lookup(ROOT_INO, b"remote-during-local-ack")
            .is_ok()
    );
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), event_end);
    assert!(*pg.pending_state.lock().expect("pending state") == PendingReplicationState::default());
}

#[test]
fn checkpoint_import_and_local_ack_share_the_canonical_lock_order() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_file(ROOT_INO, b"checkpoint-local-file", 0, 0, 0o644)
        .expect("create local file");
    pg.unlink(ROOT_INO, b"checkpoint-local-file")
        .expect("unlink unpublished local file");
    let event_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read local event end");
    let expected_state = *pg.pending_state.lock().expect("pending state");

    let (hook_tx, hook_rx) = mpsc::channel();
    let hook_barrier = Arc::new(Barrier::new(2));
    let worker_hook_barrier = Arc::clone(&hook_barrier);
    pg.set_snapshot_import_after_active_open_capture_hook(Arc::new(move || {
        hook_tx.send(()).expect("signal checkpoint import boundary");
        worker_hook_barrier.wait();
    }));

    let pg = Arc::new(pg);
    let position_guard = pg
        .local_position_lock
        .lock()
        .expect("hold local position lock");
    let import_start = Arc::new(Barrier::new(2));
    let worker_import_start = Arc::clone(&import_start);
    let import_pg = Arc::clone(&pg);
    let (import_tx, import_rx) = mpsc::channel();
    let import_worker = thread::spawn(move || {
        worker_import_start.wait();
        let result = import_pg.import_serving_checkpoint_locked(
            StartupSnapshotImportMode::PreserveLocalState,
            &crate::snapshot::ServingCheckpoint::default(),
            0,
        );
        import_tx
            .send(result)
            .expect("send checkpoint import result");
    });
    import_start.wait();

    // The import hook runs after pending_state is acquired. It must remain
    // unreachable while this test owns local_position_lock.
    let import_reached_hook_before_position_release =
        hook_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    if import_reached_hook_before_position_release {
        hook_barrier.wait();
        drop(position_guard);
        import_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("pre-fix checkpoint import completes")
            .expect("import serving checkpoint");
        import_worker.join().expect("join checkpoint import worker");
        assert!(
            !import_reached_hook_before_position_release,
            "checkpoint import must acquire local_position_lock before pending_state"
        );
        return;
    }

    drop(position_guard);
    hook_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint import reaches its guarded boundary");

    let import_holds_position_at_hook = matches!(
        pg.local_position_lock.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    );
    if !import_holds_position_at_hook {
        // A delayed pre-fix import reaches this hook while holding only
        // pending_state. Let it finish before starting an acknowledgment, so
        // the regression reports an assertion instead of recreating the ABBA.
        hook_barrier.wait();
        import_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("pre-fix checkpoint import completes")
            .expect("import serving checkpoint");
        import_worker.join().expect("join checkpoint import worker");
        assert!(
            import_holds_position_at_hook,
            "checkpoint import must retain local_position_lock through its transition"
        );
        return;
    }

    let ack_start = Arc::new(Barrier::new(2));
    let worker_ack_start = Arc::clone(&ack_start);
    let ack_pg = Arc::clone(&pg);
    let (ack_tx, ack_rx) = mpsc::channel();
    let ack_worker = thread::spawn(move || {
        worker_ack_start.wait();
        let result = ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_end);
        ack_tx.send(result).expect("send acknowledgement result");
    });
    ack_start.wait();
    hook_barrier.wait();

    import_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint import completes")
        .expect("import serving checkpoint");
    ack_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("local acknowledgement completes")
        .expect("acknowledge pending state");
    import_worker.join().expect("join checkpoint import worker");
    ack_worker.join().expect("join acknowledgement worker");
    assert!(*pg.pending_state.lock().expect("pending state") == PendingReplicationState::default());
}
