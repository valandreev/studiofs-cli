use super::*;

#[test]
fn persisted_pg_client_slot_roundtrips_in_local_counter() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("initial client slot"),
        None
    );
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("reload client slot"),
        Some(7)
    );
}

#[test]
fn prime_local_id_ranges_persists_initial_inode_window_when_missing() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 8_000,
        next: 8_000,
        end: 258_000,
    })]);

    PgMetaEngine::persist_client_slot(&pg.local, 3).expect("persist client slot");
    persist_inode_state(&pg.local, InodeAllocatorState::default());
    pg.prime_local_id_ranges(3)
        .expect("prime local id allocator state");

    let inode_state = load_inode_state(&pg.local);

    assert!(
        inode_state.current
            == InodeWindow {
                start: 8_000,
                next: 8_000,
                end: 258_000,
            },
        "prime must persist the first inode window locally when none exists"
    );
    assert!(inode_state.prefetched.is_empty());
}

#[test]
fn prime_local_id_ranges_migrates_legacy_inode_ranges_to_durable_windows() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    persist_inode_state(&pg.local, InodeAllocatorState::default());
    persist_legacy_inode_range_state(
        &pg.local,
        LocalIdRangeState {
            next: 1_203,
            active_start: 1_000,
            active_end: 2_000,
            reserved_end: 3_000,
        },
    );

    pg.prime_local_id_ranges(0)
        .expect("prime must migrate legacy inode state");

    assert_eq!(
        load_inode_state(&pg.local),
        InodeAllocatorState {
            current: InodeWindow {
                start: 1_000,
                next: 1_203,
                end: 2_000,
            },
            prefetched: InodeWindow {
                start: 2_000,
                next: 2_000,
                end: 3_000,
            },
        },
        "legacy inode ranges should be migrated into durable current/prefetched windows"
    );
}

#[test]
fn open_with_persisted_slot_and_ranges_survives_pg_unavailable() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen",
            "offline-reopen-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    persist_inode_state(
        &local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 4_000,
                next: 4_002,
                end: 4_008,
            },
            prefetched: InodeWindow::default(),
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    let reopened = reopened.expect("reopen from persisted allocator state");
    assert_eq!(
        load_inode_state(&reopened.local).current,
        InodeWindow {
            start: 4_000,
            next: 4_002,
            end: 4_008,
        },
        "reopen must reuse the persisted current inode window without reserving a fresh one"
    );
}

#[test]
fn sfs315_reopen_consumes_persisted_inode_and_object_headroom_without_pg() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "sfs315-offline-reopen-headroom",
            "sfs315-offline-reopen-headroom-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    persist_inode_state(
        &local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 8_000,
                next: 8_001,
                end: 8_005,
            },
            prefetched: InodeWindow::default(),
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("reopen from persisted local allocation state");

    let status = MetaReplication::metadata_authority_status(&*reopened);
    assert!(
        status.inode_id_headroom,
        "reopened allocator should report remaining inode headroom"
    );
    assert!(
        status.object_id_headroom,
        "reopened allocator should report remaining object ID headroom separately"
    );

    let ino = reopened
        .next_inode()
        .expect("allocate inode from persisted headroom");
    assert_eq!(ino, 8_001);
    let object_ids = reopened
        .next_object_ids(2)
        .expect("allocate object IDs from persisted headroom");
    assert_eq!(object_ids, vec![8_002, 8_003]);

    assert_eq!(
        load_inode_state(&reopened.local).current.next,
        8_004,
        "inode/object allocation progress must persist after offline reopen"
    );
}

#[test]
fn sfs315_failed_object_batch_preserves_smaller_inode_headroom() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("pg local inode window exhausted".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 9_000,
                next: 9_000,
                end: 9_002,
            },
            prefetched: InodeWindow::default(),
        },
    );
    pg.update_inode_headroom_flag(load_inode_state(&pg.local));

    let object_error = pg
        .next_object_ids(3)
        .expect_err("object batch should fail when it cannot fit in local headroom");
    assert!(
        object_error
            .to_string()
            .contains("failed to ensure current local inode window"),
        "object batch failure should report refill failure without declaring smaller local headroom exhausted: {object_error}"
    );

    let status = MetaReplication::metadata_authority_status(&pg);
    assert!(
        status.inode_id_headroom,
        "failed larger object batch must not consume the smaller inode headroom that remains"
    );
    assert!(
        status.object_id_headroom,
        "object ID status should still expose the remaining smaller local ID headroom"
    );

    let ino = pg
        .next_inode()
        .expect("smaller inode allocation should still use remaining local headroom");
    assert_eq!(ino, 9_000);
    assert_eq!(
        load_inode_state(&pg.local).current.next,
        9_001,
        "failed object batch must not burn the tail of the persisted window"
    );
}

#[test]
fn open_with_persisted_slot_and_legacy_inode_ranges_survives_pg_unavailable() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-legacy-inodes",
            "offline-reopen-legacy-inodes-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    let legacy_inode_base =
        PgMetaEngine::reserve_counter_range_base(5, LEGACY_PG_INODE_ID_STRIDE, ROOT_INO + 1);
    persist_legacy_inode_range_state(
        &local,
        LocalIdRangeState {
            next: legacy_inode_base + 2,
            active_start: legacy_inode_base,
            active_end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
            reserved_end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW * 2,
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    let reopened = reopened.expect("reopen from persisted legacy inode state");
    assert_eq!(
        load_inode_state(&reopened.local),
        InodeAllocatorState {
            current: InodeWindow {
                start: legacy_inode_base,
                next: legacy_inode_base + 2,
                end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
            },
            prefetched: InodeWindow {
                start: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
                next: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
                end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW * 2,
            },
        },
        "offline reopen must accept legacy inode ranges and migrate them into durable windows"
    );
}

#[test]
fn open_with_persisted_slot_without_ranges_still_requires_pg() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-no-ranges",
            "offline-reopen-no-ranges-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    assert!(
        reopened.is_err(),
        "offline reopen must not mint fresh headroom from a legacy client slot without persisted ranges"
    );
}

#[test]
fn open_with_persisted_ranges_does_not_mask_non_transient_schema_errors() {
    let _schema_error = set_schema_ensure_test_override(
        "postgres file lock lease schema ensure failed: permission denied (sqlstate=42501)",
    );
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-sqlstate",
            "offline-reopen-sqlstate-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    persist_inode_state(
        &local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 7_000,
                next: 7_002,
                end: 7_008,
            },
            prefetched: InodeWindow::default(),
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    assert!(
        reopened.is_err(),
        "non-transient schema/bootstrap failures must not be silently treated as offline-mode reopens"
    );
}

#[test]
fn next_inode_switches_to_prefetched_window_without_pg_roundtrip() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 101,
                end: 102,
            },
            prefetched: InodeWindow {
                start: 500,
                next: 500,
                end: 503,
            },
        },
    );

    let allocated = pg.next_inode().expect("allocate boundary inode");
    assert_eq!(allocated, 101);
    assert_eq!(
        load_inode_state(&pg.local),
        InodeAllocatorState {
            current: InodeWindow {
                start: 500,
                next: 500,
                end: 503,
            },
            prefetched: InodeWindow::default(),
        },
        "exhausting the current window must switch to the prefetched window locally"
    );
}

#[test]
fn next_object_ids_share_inode_allocation_stream() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 100,
                end: 110,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let file_ino = pg.next_inode().expect("allocate inode");
    let object_ids = pg.next_object_ids(3).expect("allocate object IDs");
    let next_ino = pg.next_inode().expect("allocate next inode");

    assert_eq!(file_ino, 100);
    assert_eq!(object_ids, vec![101, 102, 103]);
    assert_eq!(next_ino, 104);
    assert_eq!(load_inode_state(&pg.local).current.next, 105);
}

#[test]
fn next_object_ids_preserves_contiguous_batch_at_window_boundary() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 101,
                end: 102,
            },
            prefetched: InodeWindow {
                start: 500,
                next: 500,
                end: 503,
            },
        },
    );

    let object_ids = pg.next_object_ids(2).expect("allocate object IDs");

    assert_eq!(object_ids, vec![500, 501]);
    assert_eq!(load_inode_state(&pg.local).current.next, 502);
}

#[test]
fn next_object_ids_reserves_new_window_when_tail_is_too_small() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 500,
        next: 500,
        end: 510,
    })]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 100,
                end: 102,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let object_ids = pg.next_object_ids(3).expect("allocate object IDs");

    assert_eq!(object_ids, vec![500, 501, 502]);
    assert_eq!(load_inode_state(&pg.local).current.next, 503);
}

#[test]
fn sfs315_large_object_batch_replaces_undersized_prefetched_window() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([
        Ok(InodeWindow {
            start: 500,
            next: 500,
            end: 510,
        }),
        Err("second reservation should not be needed".into()),
    ]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 101,
                end: 102,
            },
            prefetched: InodeWindow {
                start: 200,
                next: 200,
                end: 202,
            },
        },
    );

    let object_ids = pg
        .next_object_ids(3)
        .expect("allocate object IDs from a replacement window");

    assert_eq!(object_ids, vec![500, 501, 502]);
    assert_eq!(
        load_inode_state(&pg.local).current.next,
        503,
        "a too-small prefetched window must not block applying a larger replacement"
    );
}

#[test]
fn next_object_ids_allows_batch_larger_than_default_window() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let count = PG_INODE_WINDOW_SIZE + 1;
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 500,
        next: 500,
        end: 500 + count,
    })]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 100,
                end: 102,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let object_ids = pg
        .next_object_ids(u32::try_from(count).expect("test count fits u32"))
        .expect("allocate object IDs");

    assert_eq!(object_ids.len(), count as usize);
    assert_eq!(object_ids.first().copied(), Some(500));
    assert_eq!(object_ids.last().copied(), Some(500 + count - 1));
    assert_eq!(load_inode_state(&pg.local).current.next, 500 + count);
}

#[test]
fn next_object_ids_reports_inode_window_exhaustion() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("pg local inode window exhausted".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 3_000,
                next: 3_001,
                end: 3_001,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let error = pg
        .next_object_ids(1)
        .expect_err("object allocation must fail when the shared inode window is exhausted");

    assert!(
        error
            .to_string()
            .contains("pg local inode window exhausted"),
        "unexpected error: {error}"
    );
}

#[test]
fn next_inode_prefetches_single_future_window_at_low_watermark() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 9_000,
        next: 9_000,
        end: 59_500,
    })]);

    let start = 1_000;
    let end = start + PG_INODE_PREFETCH_LOW_WATERMARK + 1;
    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start,
                next: start,
                end,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let allocated = pg
        .next_inode()
        .expect("allocate and trigger low-water prefetch");
    assert_eq!(allocated, start);

    let state = load_inode_state(&pg.local);
    assert_eq!(state.current.next, start + 1);
    assert_eq!(
        state.prefetched,
        InodeWindow {
            start: 9_000,
            next: 9_000,
            end: 59_500,
        },
        "reaching the low-watermark should reserve exactly one prefetched inode window"
    );
}

#[test]
fn next_inode_reserves_replacement_current_window_when_pg_is_available() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 9_000,
        next: 9_000,
        end: 70_000,
    })]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 4_000,
                next: 4_000,
                end: 4_001,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let first = pg
        .next_inode()
        .expect("allocate final inode before current-window refill");
    assert_eq!(first, 4_000);
    assert!(
        load_inode_state(&pg.local).current
            == InodeWindow {
                start: 9_000,
                next: 9_000,
                end: 70_000,
            },
        "when PostgreSQL is available, exhausting the current window should refill it immediately"
    );
}

#[test]
fn create_file_respects_inode_headroom_exhaustion() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("pg local inode window exhausted".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 3_000,
                next: 3_001,
                end: 3_001,
            },
            prefetched: InodeWindow::default(),
        },
    );
    pg.update_inode_headroom_flag(load_inode_state(&pg.local));

    let result = pg.create_file(ROOT_INO, b"exhausted.txt", 0, 0, 0o644);
    assert!(
        matches!(result, Err(MetaError::Storage(ref message)) if message.contains("inode window exhausted")),
        "create_file must honor inode headroom exhaustion instead of bypassing the PG allocator: {result:?}"
    );
}

#[test]
fn next_inode_reports_read_only_headroom_once_windows_are_exhausted() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([
        Err("pg local inode window exhausted".into()),
        Err("pg local inode window exhausted".into()),
    ]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 5_000,
                next: 5_000,
                end: 5_001,
            },
            prefetched: InodeWindow::default(),
        },
    );
    let first = pg.next_inode().expect("final inode within local headroom");
    assert_eq!(first, 5_000);

    let error = pg
        .next_inode()
        .expect_err("headroom should hard-stop new inode allocation once no local window remains");
    assert!(
        matches!(error, MetaError::Storage(ref message) if message.contains("inode window exhausted")),
        "unexpected exhaustion error: {error}"
    );

    let authority = MetaReplication::metadata_authority_status(&pg);
    assert!(
        !authority.allows_new_mutations(),
        "inode window exhaustion should surface a read-only metadata authority status"
    );
}

#[test]
fn next_inode_distinguishes_pg_reservation_failures_from_true_exhaustion() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("connect timeout".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 6_000,
                next: 6_001,
                end: 6_001,
            },
            prefetched: InodeWindow::default(),
        },
    );
    pg.update_inode_headroom_flag(load_inode_state(&pg.local));

    let error = pg
        .next_inode()
        .expect_err("allocation must report the PostgreSQL reservation failure");
    assert!(
        matches!(error, MetaError::Storage(ref message) if message.contains("failed to reserve a new inode window from PostgreSQL after exhausting local inode windows")),
        "unexpected PostgreSQL reservation error: {error}"
    );
}

#[test]
fn restore_pending_state_from_local_log_rebuilds_unpublished_range() {
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
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_clients_reserve_disjoint_local_id_ranges() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-client-slots");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let a_slot = PgMetaEngine::load_persisted_client_slot(&a.local)
        .expect("read a slot")
        .expect("a slot");
    let b_slot = PgMetaEngine::load_persisted_client_slot(&b.local)
        .expect("read b slot")
        .expect("b slot");
    assert_ne!(
        a_slot, b_slot,
        "distinct clients must reserve distinct slots"
    );

    let a_next_inode = a
        .local
        .get_counter(counters::NEXT_INODE)
        .expect("a next inode");
    let b_next_inode = b
        .local
        .get_counter(counters::NEXT_INODE)
        .expect("b next inode");
    assert_ne!(a_next_inode, b_next_inode);

    let a_file = a
        .create_file(ROOT_INO, b"a.txt", 0, 0, 0o644)
        .expect("a create file");
    let b_file = b
        .create_file(ROOT_INO, b"b.txt", 0, 0, 0o644)
        .expect("b create file");
    assert_ne!(
        a_file.ino, b_file.ino,
        "new inode allocations must not collide"
    );
    let a_objects = a.next_object_ids(2).expect("a object ids");
    let b_objects = b.next_object_ids(2).expect("b object ids");
    assert!(
        a_objects.iter().all(|id| !b_objects.contains(id)),
        "new object ID allocations must not collide"
    );
    assert_eq!(a_objects, vec![a_file.ino + 1, a_file.ino + 2]);
    assert_eq!(b_objects, vec![b_file.ino + 1, b_file.ino + 2]);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_inode_windows_are_scoped_per_volume() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume_a = PgTestVolume::new(&dsn, "pg-inode-window-volume-a");
    let volume_b = PgTestVolume::new(&dsn, "pg-inode-window-volume-b");
    seed_local(&db_a, &volume_a.format_info());
    seed_local(&db_b, &volume_b.format_info());

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let a_window = load_inode_state(&a.local);
    let b_window = load_inode_state(&b.local);
    assert_eq!(
        a_window.current.start,
        ROOT_INO + 1,
        "first inode window for a new volume should start at the root successor"
    );
    assert_eq!(
        b_window.current.start,
        ROOT_INO + 1,
        "inode windows must be scoped per volume rather than globally across postgres"
    );

    let a_file = a
        .create_file(ROOT_INO, b"a-volume.txt", 0, 0, 0o644)
        .expect("a create file");
    let b_file = b
        .create_file(ROOT_INO, b"b-volume.txt", 0, 0, 0o644)
        .expect("b create file");
    assert_eq!(
        a_file.ino, b_file.ino,
        "different volumes may reuse inode values because each volume owns its own namespace"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_rename_overwrite_converges_to_source_inode() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-rename-overwrite");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let src = create_published_small_file(&a, ROOT_INO, b"src.txt", 50_304);
    let dst = create_published_small_file(&a, ROOT_INO, b"dst.txt", 50_305);
    a.flush_metadata().expect("flush seed files");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"src.txt",
        Duration::from_secs(10)
    ));
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"dst.txt",
        Duration::from_secs(10)
    ));

    a.rename(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
        .expect("rename overwrite on a");
    a.flush_metadata().expect("flush rename overwrite");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"src.txt").is_err()
            && b.lookup(ROOT_INO, b"dst.txt")
                .map(|entry| entry.ino == src.ino)
                .unwrap_or(false)
    }));

    let on_b = b.lookup(ROOT_INO, b"dst.txt").expect("lookup dst on b");
    assert_eq!(on_b.ino, src.ino);
    assert_ne!(on_b.ino, dst.ino);
    assert!(b.lookup(ROOT_INO, b"src.txt").is_err());
    assert!(
        b.get_attr(dst.ino).is_err(),
        "overwritten inode should be removed"
    );
}
