use super::*;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

fn wait_for_local_position_lock(pg: &PgMetaEngine, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        match pg.local_position_lock.try_lock() {
            Ok(guard) => drop(guard),
            Err(std::sync::TryLockError::WouldBlock) => return true,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("local position lock must not be poisoned")
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::yield_now();
    }
}

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
    let (replay_ready_tx, replay_ready_rx) = mpsc::channel();
    let (replay_tx, replay_rx) = mpsc::channel();
    let replay_worker = thread::spawn(move || {
        worker_replay_start.wait();
        replay_ready_tx
            .send(())
            .expect("signal replay worker ready");
        let result = replay_pg.apply_replay_page_atomically(vec![batch], None, context);
        replay_tx.send(result).expect("send replay result");
    });
    replay_start.wait();
    replay_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("replay reaches the lock acquisition boundary");

    let replay_holds_position_before_pending_release =
        wait_for_local_position_lock(&pg, Duration::from_secs(2));
    let ack = if replay_holds_position_before_pending_release {
        let ack_start = Arc::new(Barrier::new(2));
        let worker_ack_start = Arc::clone(&ack_start);
        let ack_pg = Arc::clone(&pg);
        let (ack_ready_tx, ack_ready_rx) = mpsc::channel();
        let (ack_tx, ack_rx) = mpsc::channel();
        let ack_worker = thread::spawn(move || {
            worker_ack_start.wait();
            ack_ready_tx.send(()).expect("signal acknowledgement ready");
            let result = ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_end);
            ack_tx.send(result).expect("send acknowledgement result");
        });
        ack_start.wait();
        ack_ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("acknowledgement reaches the lock acquisition boundary");
        Some((ack_rx, ack_worker))
    } else {
        None
    };
    drop(pending_guard);

    hook_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("replay reaches its commit boundary after pending state is released");
    let replay_holds_position_at_commit = wait_for_local_position_lock(&pg, Duration::ZERO);
    hook_barrier.wait();

    let replay_outcome = replay_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("replay page completes")
        .expect("apply replay page");
    replay_worker.join().expect("join replay worker");
    if let Some((ack_rx, ack_worker)) = ack {
        ack_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("local acknowledgement completes")
            .expect("acknowledge pending state");
        ack_worker.join().expect("join acknowledgement worker");
    }

    assert!(
        replay_holds_position_before_pending_release,
        "replay must acquire local_position_lock before pending_state"
    );
    assert!(
        replay_holds_position_at_commit,
        "replay must retain local_position_lock through commit"
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
    let pending_guard = pg.pending_state.lock().expect("hold pending state");
    let import_start = Arc::new(Barrier::new(2));
    let worker_import_start = Arc::clone(&import_start);
    let import_pg = Arc::clone(&pg);
    let (import_ready_tx, import_ready_rx) = mpsc::channel();
    let (import_tx, import_rx) = mpsc::channel();
    let import_worker = thread::spawn(move || {
        worker_import_start.wait();
        import_ready_tx
            .send(())
            .expect("signal checkpoint import ready");
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
    import_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint import reaches the lock acquisition boundary");

    let import_holds_position_before_pending_release =
        wait_for_local_position_lock(&pg, Duration::from_secs(2));
    if !import_holds_position_before_pending_release {
        drop(pending_guard);
        hook_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("checkpoint import reaches its guarded boundary");
        hook_barrier.wait();
        import_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("pre-fix checkpoint import completes")
            .expect("import serving checkpoint");
        import_worker.join().expect("join checkpoint import worker");
        assert!(
            import_holds_position_before_pending_release,
            "checkpoint import must acquire local_position_lock before pending_state"
        );
        return;
    }

    let ack_start = Arc::new(Barrier::new(2));
    let worker_ack_start = Arc::clone(&ack_start);
    let ack_pg = Arc::clone(&pg);
    let (ack_ready_tx, ack_ready_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();
    let ack_worker = thread::spawn(move || {
        worker_ack_start.wait();
        ack_ready_tx.send(()).expect("signal acknowledgement ready");
        let result = ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_end);
        ack_tx.send(result).expect("send acknowledgement result");
    });
    ack_start.wait();
    ack_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("acknowledgement reaches the lock acquisition boundary");
    drop(pending_guard);

    hook_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint import reaches its guarded boundary");
    let import_holds_position_at_hook = wait_for_local_position_lock(&pg, Duration::ZERO);
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
    assert!(
        import_holds_position_before_pending_release,
        "checkpoint import must acquire local_position_lock before pending_state"
    );
    assert!(
        import_holds_position_at_hook,
        "checkpoint import must retain local_position_lock through its transition"
    );
}

#[test]
fn restore_pending_state_and_local_ack_share_the_canonical_lock_order() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_file(ROOT_INO, b"restore-local-file", 0, 0, 0o644)
        .expect("create local file");
    let expected_state = *pg.pending_state.lock().expect("pending state");
    assert!(
        expected_state.event_start.is_some(),
        "fixture must contain a pending event range"
    );
    let event_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read local event end");

    let pg = Arc::new(pg);
    let pending_guard = pg.pending_state.lock().expect("hold pending state");
    let restore_start = Arc::new(Barrier::new(2));
    let worker_restore_start = Arc::clone(&restore_start);
    let restore_pg = Arc::clone(&pg);
    let (restore_ready_tx, restore_ready_rx) = mpsc::channel();
    let (restore_tx, restore_rx) = mpsc::channel();
    let restore_worker = thread::spawn(move || {
        worker_restore_start.wait();
        restore_ready_tx
            .send(())
            .expect("signal restore worker ready");
        restore_tx
            .send(restore_pg.restore_pending_state_from_local_log())
            .expect("send restore result");
    });
    restore_start.wait();
    restore_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("restore reaches the lock acquisition boundary");

    let restore_holds_position_before_pending_release =
        wait_for_local_position_lock(&pg, Duration::from_secs(2));
    let ack = if restore_holds_position_before_pending_release {
        let ack_start = Arc::new(Barrier::new(2));
        let worker_ack_start = Arc::clone(&ack_start);
        let ack_pg = Arc::clone(&pg);
        let (ack_ready_tx, ack_ready_rx) = mpsc::channel();
        let (ack_tx, ack_rx) = mpsc::channel();
        let ack_worker = thread::spawn(move || {
            worker_ack_start.wait();
            ack_ready_tx.send(()).expect("signal acknowledgement ready");
            let result = ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_end);
            ack_tx.send(result).expect("send acknowledgement result");
        });
        ack_start.wait();
        ack_ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("acknowledgement reaches the lock acquisition boundary");
        Some((ack_rx, ack_worker))
    } else {
        None
    };
    drop(pending_guard);

    restore_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("pending state restoration completes")
        .expect("restore pending state");
    restore_worker.join().expect("join restore worker");
    if let Some((ack_rx, ack_worker)) = ack {
        ack_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("local acknowledgement completes")
            .expect("acknowledge pending state");
        ack_worker.join().expect("join acknowledgement worker");
    }
    assert!(
        restore_holds_position_before_pending_release,
        "pending restoration must acquire local_position_lock before pending_state"
    );
}
