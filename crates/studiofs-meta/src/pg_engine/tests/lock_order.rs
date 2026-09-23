use super::*;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

#[derive(Debug, Eq, PartialEq)]
enum LockOrderHookEvent {
    LocalPositionAcquired,
    ReplayBeforeCommit { position_lock_held: bool },
    PendingStateRestored,
}

#[test]
fn replay_page_apply_and_local_ack_share_the_canonical_lock_order() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.create_dir(ROOT_INO, b"pending-local-dir", 0, 0, 0o755)
        .expect("create pending local directory");

    let event_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read local event end");
    let expected_state = *pg.pending_state.lock().expect("pending state");
    let event_start = expected_state.event_start.expect("pending event start");
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

    let pg = Arc::new(pg);
    let hook_pg = Arc::downgrade(&pg);
    let (hook_tx, hook_rx) = mpsc::channel();
    pg.set_catch_up_fault_hook(Arc::new(move |point| {
        let event = match point {
            CatchUpFaultPoint::AfterLocalPositionLockAcquired => {
                Some(LockOrderHookEvent::LocalPositionAcquired)
            }
            CatchUpFaultPoint::BeforeLocalCommit { .. } => {
                let position_lock_held = hook_pg.upgrade().is_some_and(|pg| {
                    matches!(
                        pg.local_position_lock.try_lock(),
                        Err(std::sync::TryLockError::WouldBlock)
                    )
                });
                Some(LockOrderHookEvent::ReplayBeforeCommit { position_lock_held })
            }
            _ => None,
        };
        if let Some(event) = event {
            hook_tx.send(event).expect("send replay lock event");
        }
        Ok(())
    }));

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

    let replay_lock_event = hook_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("replay reports acquiring local_position_lock");
    if replay_lock_event != LockOrderHookEvent::LocalPositionAcquired {
        drop(pending_guard);
        replay_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("replay page completes")
            .expect("apply replay page");
        replay_worker.join().expect("join replay worker");
        assert_eq!(
            replay_lock_event,
            LockOrderHookEvent::LocalPositionAcquired,
            "replay must acquire local_position_lock before pending_state"
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
        let contending = matches!(
            ack_pg.local_position_lock.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        ack_ready_tx
            .send(contending)
            .expect("signal acknowledgement lock state");
        let result =
            ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_start, event_end);
        ack_tx.send(result).expect("send acknowledgement result");
    });
    ack_start.wait();
    let ack_waited_for_replay = ack_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("acknowledgement reaches the lock acquisition boundary");
    drop(pending_guard);

    let replay_holds_position_at_commit = matches!(
        hook_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("replay reaches its commit boundary"),
        LockOrderHookEvent::ReplayBeforeCommit {
            position_lock_held: true
        }
    );

    let replay_outcome = replay_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("replay page completes")
        .expect("apply replay page");
    replay_worker.join().expect("join replay worker");
    ack_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("local acknowledgement completes")
        .expect("acknowledge pending state");
    ack_worker.join().expect("join acknowledgement worker");

    assert!(
        ack_waited_for_replay,
        "local acknowledgement must contend while replay holds local_position_lock"
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
    pg.create_dir(ROOT_INO, b"checkpoint-pending-dir", 0, 0, 0o755)
        .expect("create pending local directory");
    let event_end = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read local event end");
    let expected_state = *pg.pending_state.lock().expect("pending state");
    let event_start = expected_state.event_start.expect("pending event start");

    let pg = Arc::new(pg);
    let (position_tx, position_rx) = mpsc::channel();
    pg.set_catch_up_fault_hook(Arc::new(move |point| {
        if point == CatchUpFaultPoint::AfterLocalPositionLockAcquired {
            position_tx
                .send(LockOrderHookEvent::LocalPositionAcquired)
                .expect("signal checkpoint lock order");
        }
        Ok(())
    }));

    let (hook_tx, hook_rx) = mpsc::channel();
    let hook_barrier = Arc::new(Barrier::new(2));
    let worker_hook_barrier = Arc::clone(&hook_barrier);
    let snapshot_hook_pg = Arc::downgrade(&pg);
    pg.set_snapshot_import_after_active_open_capture_hook(Arc::new(move || {
        let position_lock_held = snapshot_hook_pg.upgrade().is_some_and(|pg| {
            matches!(
                pg.local_position_lock.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            )
        });
        hook_tx
            .send(position_lock_held)
            .expect("signal checkpoint import boundary");
        worker_hook_barrier.wait();
    }));

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

    let import_position_acquired_before_pending = matches!(
        position_rx.recv_timeout(Duration::from_secs(2)),
        Ok(LockOrderHookEvent::LocalPositionAcquired)
    );
    if !import_position_acquired_before_pending {
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
            import_position_acquired_before_pending,
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
        let contending = matches!(
            ack_pg.local_position_lock.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        ack_ready_tx
            .send(contending)
            .expect("signal acknowledgement lock state");
        let result =
            ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_start, event_end);
        ack_tx.send(result).expect("send acknowledgement result");
    });
    ack_start.wait();
    let ack_waited_for_import = ack_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("acknowledgement reaches the lock acquisition boundary");
    drop(pending_guard);

    let import_holds_position_at_hook = hook_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint import reaches its guarded boundary");
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
        import_position_acquired_before_pending,
        "checkpoint import must acquire local_position_lock before pending_state"
    );
    assert!(
        ack_waited_for_import,
        "local acknowledgement must contend while checkpoint import holds local_position_lock"
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
    let event_start = expected_state.event_start.expect("pending event start");

    let pg = Arc::new(pg);
    let restore_barrier = Arc::new(Barrier::new(2));
    let worker_restore_barrier = Arc::clone(&restore_barrier);
    let (restore_hook_tx, restore_hook_rx) = mpsc::channel();
    pg.set_catch_up_fault_hook(Arc::new(move |point| {
        match point {
            CatchUpFaultPoint::AfterLocalPositionLockAcquired => restore_hook_tx
                .send(LockOrderHookEvent::LocalPositionAcquired)
                .expect("signal restore position acquisition"),
            CatchUpFaultPoint::AfterPendingStateRestore => {
                restore_hook_tx
                    .send(LockOrderHookEvent::PendingStateRestored)
                    .expect("signal restored pending state");
                worker_restore_barrier.wait();
            }
            _ => {}
        }
        Ok(())
    }));

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

    let restore_position_event = restore_hook_rx.recv_timeout(Duration::from_secs(2));
    let restore_holds_position_before_pending_release = matches!(
        restore_position_event,
        Ok(LockOrderHookEvent::LocalPositionAcquired)
    );
    if !restore_holds_position_before_pending_release {
        drop(pending_guard);
        loop {
            let event = restore_hook_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("restore reaches the guarded test boundary");
            if event == LockOrderHookEvent::PendingStateRestored {
                break;
            }
        }
        restore_barrier.wait();
        restore_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("pending state restoration completes")
            .expect("restore pending state");
        restore_worker.join().expect("join restore worker");
        assert!(
            restore_holds_position_before_pending_release,
            "pending restoration must acquire local_position_lock before pending_state"
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
        let contending = matches!(
            ack_pg.local_position_lock.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        ack_ready_tx
            .send(contending)
            .expect("signal acknowledgement lock state");
        let result =
            ack_pg.acknowledge_pending_state_if_unchanged(expected_state, event_start, event_end);
        ack_tx.send(result).expect("send acknowledgement result");
    });
    ack_start.wait();
    let ack_waited_for_restore = ack_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("acknowledgement reaches the lock acquisition boundary");
    drop(pending_guard);

    let restore_hook_event = restore_hook_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("restore reports its completed state transition");
    restore_barrier.wait();
    restore_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("pending state restoration completes")
        .expect("restore pending state");
    ack_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("local acknowledgement completes")
        .expect("acknowledge pending state");
    restore_worker.join().expect("join restore worker");
    ack_worker.join().expect("join acknowledgement worker");

    assert!(
        restore_holds_position_before_pending_release,
        "pending restoration must acquire local_position_lock before pending_state"
    );
    assert!(
        ack_waited_for_restore,
        "local acknowledgement must contend while restore holds local_position_lock"
    );
    assert_eq!(restore_hook_event, LockOrderHookEvent::PendingStateRestored);
}
