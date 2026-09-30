use super::*;
use redb::StorageBackend;
use std::sync::atomic::{AtomicU64, Ordering};

// redb 4.3 branch-page discriminant from tree_store/btree_base.rs.
const BRANCH_PAGE_TYPE: u8 = 2;

#[derive(Clone, Debug, Default)]
struct RemoveFaultBackend {
    inner: Arc<redb::backends::InMemoryBackend>,
    last_branch_read: Arc<AtomicU64>,
    corrupt_read_at: Arc<AtomicU64>,
}

impl StorageBackend for RemoveFaultBackend {
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.inner.read(offset, out)?;
        if out.first() == Some(&BRANCH_PAGE_TYPE) {
            self.last_branch_read.store(offset, Ordering::SeqCst);
            if self.corrupt_read_at.load(Ordering::SeqCst) == offset {
                // redb 4.3 branch layout: 8-byte header, per-child 16-byte
                // checksums, then 8-byte pointers. Give child 0 an invalid
                // page order, as in redb's corrupted_btree_descent regression.
                // Only the read buffer changes; stored bytes remain intact.
                let header = out.get(2..4).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "redb branch header is truncated",
                    )
                })?;
                let num_keys = usize::from(u16::from_le_bytes([header[0], header[1]]));
                let first_child = 8 + 16 * (num_keys + 1);
                let target = out.get_mut(first_child + 7).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "redb branch child pointer is outside the read buffer",
                    )
                })?;
                if self
                    .corrupt_read_at
                    .compare_exchange(offset, u64::MAX, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    *target |= 0xF8;
                }
            }
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, data)
    }
}

fn removal_fault_engine(backend: &RemoveFaultBackend) -> RedbMetaEngine {
    let db = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend.clone())
        .unwrap();
    RedbMetaEngine {
        db: Some(Arc::new(RwLock::new(db))),
        read_only_db: None,
        metadata_path: PathBuf::new(),
        local_storage_compaction: Mutex::default(),
        local_event_log_prune: Mutex::default(),
        local_storage_page_stats: Mutex::default(),
        local_storage_page_stats_refresh: Mutex::default(),
        write_metrics: Arc::new(MetadataWriteMetrics::default()),
        relaxed_durability: None,
        write_txn_lock: Arc::new(Mutex::new(())),
        invalidation_journal: Mutex::default(),
        invalidation_notify: Arc::new(tokio::sync::Notify::new()),
        allocator_state_write_hook: Mutex::new(None),
    }
}

fn acl_removal_fault_fixture(
    fault_table: TableDefinition<&[u8], u8>,
) -> (RedbMetaEngine, RemoveFaultBackend, Vec<u8>) {
    let backend = RemoveFaultBackend::default();
    let engine = removal_fault_engine(&backend);
    let key = encode_acl_rule_key(ROOT_INO, "principal-0000");
    let txn = engine.begin_write_txn().unwrap();
    {
        let root = encode(&InodeAttr::new_dir(ROOT_INO, ROOT_INO, 0, 0, 0o755)).unwrap();
        txn.open_table(INODES_TABLE)
            .unwrap()
            .insert(ROOT_INO, root.as_slice())
            .unwrap();
        txn.open_table(BASE_INODES_TABLE).unwrap();
        txn.open_table(INODE_TOMBSTONES_TABLE).unwrap();
        txn.open_table(INODE_LIFECYCLE_TABLE).unwrap();
        txn.open_table(SETTINGS_TABLE).unwrap();
        txn.open_table(EVENT_LOG_TABLE).unwrap();
        txn.open_table(ACL_RULE_TOMBSTONES_TABLE).unwrap();
        txn.open_table(ACL_RULES_TABLE)
            .unwrap()
            .insert(key.as_slice(), AccessLevel::ReadOnly as u8)
            .unwrap();
        let mut table = txn.open_table(fault_table).unwrap();
        for index in 0..1024 {
            let key = encode_acl_rule_key(ROOT_INO, &format!("principal-{index:04}"));
            table
                .insert(key.as_slice(), AccessLevel::ReadOnly as u8)
                .unwrap();
        }
        txn.open_table(COUNTERS_TABLE)
            .unwrap()
            .insert(counters::NEXT_EVENT_SEQ, 7)
            .unwrap();
    }
    txn.commit().unwrap();
    let read = engine.begin_read_txn().unwrap();
    assert!(
        read.open_table(fault_table)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .is_some()
    );
    drop(read);
    let branch = backend.last_branch_read.load(Ordering::SeqCst);
    assert_ne!(branch, 0, "fixture must have an ACL branch page");
    backend.corrupt_read_at.store(branch, Ordering::SeqCst);
    (engine, backend, key)
}

#[test]
fn replica_removal_error_aborts_prior_metadata_writes() {
    let backend = RemoveFaultBackend::default();
    let engine = removal_fault_engine(&backend);
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut pending = txn.open_table(PENDING_DELETIONS_TABLE).unwrap();
        for object_id in 0..1024 {
            pending.insert(object_id, 7).unwrap();
        }
        txn.open_table(COUNTERS_TABLE)
            .unwrap()
            .insert("replica-removal-regression", 7)
            .unwrap();
    }
    txn.commit().unwrap();
    let read = engine.begin_read_txn().unwrap();
    assert_eq!(
        read.open_table(PENDING_DELETIONS_TABLE)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        7
    );
    drop(read);
    let branch = backend.last_branch_read.load(Ordering::SeqCst);
    assert_ne!(
        branch, 0,
        "fixture must have a pending-deletion branch page"
    );

    let result = (|| -> MetaResult<()> {
        let txn = engine.begin_write_txn()?;
        RedbMetaEngine::update_counter_in_txn(&txn, "replica-removal-regression", |value| {
            value + 1
        })?;
        backend.corrupt_read_at.store(branch, Ordering::SeqCst);
        RedbMetaEngine::apply_replica_aux_state_in_txn(
            &txn,
            &[MetaEvent::RemovePendingDeletion { object_id: 0 }],
        )?;
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(())
    })();

    assert_eq!(backend.corrupt_read_at.load(Ordering::SeqCst), u64::MAX);
    drop(engine);
    let reopened = Database::builder().create_with_backend(backend).unwrap();
    let read = reopened.begin_read().unwrap();
    assert_eq!(
        read.open_table(COUNTERS_TABLE)
            .unwrap()
            .get("replica-removal-regression")
            .unwrap()
            .unwrap()
            .value(),
        7,
        "failed replica removal must roll back prior metadata writes"
    );
    assert_eq!(
        read.open_table(PENDING_DELETIONS_TABLE)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        7
    );
    assert!(
        matches!(&result, Err(MetaError::Storage(message)) if message.contains("has order greater than the maximum")),
        "{result:?}"
    );
}

#[test]
fn acl_rule_removal_error_aborts_tombstone_and_event_writes() {
    let (engine, backend, key) = acl_removal_fault_fixture(ACL_RULES_TABLE);

    let result = engine.remove_acl_rule(ROOT_INO, "principal-0000");

    assert_eq!(backend.corrupt_read_at.load(Ordering::SeqCst), u64::MAX);
    drop(engine);
    let reopened = Database::builder().create_with_backend(backend).unwrap();
    let read = reopened.begin_read().unwrap();
    assert_eq!(
        read.open_table(COUNTERS_TABLE)
            .unwrap()
            .get(counters::NEXT_EVENT_SEQ)
            .unwrap()
            .unwrap()
            .value(),
        7
    );
    assert!(
        read.open_table(EVENT_LOG_TABLE)
            .unwrap()
            .get(7)
            .unwrap()
            .is_none()
    );
    assert!(
        read.open_table(ACL_RULE_TOMBSTONES_TABLE)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        read.open_table(ACL_RULES_TABLE)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .unwrap()
            .value(),
        AccessLevel::ReadOnly as u8
    );
    assert!(
        matches!(&result, Err(MetaError::Storage(message)) if message.contains("has order greater than the maximum")),
        "{result:?}"
    );
}

#[test]
fn acl_tombstone_removal_error_rolls_back_rule_update() {
    let (engine, backend, key) = acl_removal_fault_fixture(ACL_RULE_TOMBSTONES_TABLE);

    let result = engine.set_acl_rule(ROOT_INO, "principal-0000", AccessLevel::ReadWrite);

    assert_eq!(backend.corrupt_read_at.load(Ordering::SeqCst), u64::MAX);
    drop(engine);
    let reopened = Database::builder().create_with_backend(backend).unwrap();
    let read = reopened.begin_read().unwrap();
    assert_eq!(
        read.open_table(COUNTERS_TABLE)
            .unwrap()
            .get(counters::NEXT_EVENT_SEQ)
            .unwrap()
            .unwrap()
            .value(),
        7
    );
    assert!(
        read.open_table(EVENT_LOG_TABLE)
            .unwrap()
            .get(7)
            .unwrap()
            .is_none()
    );
    assert!(
        read.open_table(ACL_RULE_TOMBSTONES_TABLE)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        read.open_table(ACL_RULES_TABLE)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .unwrap()
            .value(),
        AccessLevel::ReadOnly as u8
    );
    assert!(
        matches!(&result, Err(MetaError::Storage(message)) if message.contains("has order greater than the maximum")),
        "{result:?}"
    );
}

#[test]
fn removal_fault_corrupts_only_first_child_order_once() {
    let backend = RemoveFaultBackend::default();
    let offset = 8;
    // redb 4.3 branch: one key, two child checksums and pointers, then key data.
    let mut stored = [0; 61];
    stored[0] = BRANCH_PAGE_TYPE;
    stored[2] = 1;
    stored[56..60].copy_from_slice(&61_u32.to_le_bytes());
    stored[60] = b'a';
    backend.set_len(offset + stored.len() as u64).unwrap();
    backend.write(offset, &stored).unwrap();
    backend.corrupt_read_at.store(offset, Ordering::SeqCst);
    let mut out = [0; 61];

    backend.read(offset, &mut out).unwrap();

    let mut expected = stored;
    expected[47] = 0xF8; // High byte of child 0's page number.
    assert_eq!(out, expected);
    assert_eq!(backend.corrupt_read_at.load(Ordering::SeqCst), u64::MAX);

    backend.read(offset, &mut out).unwrap();
    assert_eq!(
        out, stored,
        "injection must be one-shot and preserve stored bytes"
    );
}

#[test]
fn removal_fault_rejects_invalid_branch_layout() {
    for bytes in [
        &[BRANCH_PAGE_TYPE, 0, 0][..],
        &[BRANCH_PAGE_TYPE, 0, 0, 0, 0, 0, 0, 0][..],
        &[BRANCH_PAGE_TYPE, 0, 0xFF, 0xFF, 0, 0, 0, 0][..],
    ] {
        let backend = RemoveFaultBackend::default();
        let offset = 8;
        backend.set_len(offset + bytes.len() as u64).unwrap();
        backend.write(offset, bytes).unwrap();
        backend.corrupt_read_at.store(offset, Ordering::SeqCst);
        let mut out = vec![0; bytes.len()];

        let error = backend.read(offset, &mut out).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{bytes:?}");
        assert_eq!(
            backend.corrupt_read_at.load(Ordering::SeqCst),
            offset,
            "{bytes:?}"
        );
    }
}

#[test]
fn table_removal_error_aborts_metadata_write_transaction() {
    let backend = RemoveFaultBackend::default();
    let db = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend.clone())
        .unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut symlinks = txn.open_table(SYMLINKS_TABLE).unwrap();
        // Force a branch page so removal must follow a child pointer.
        for ino in 0..1024 {
            symlinks.insert(ino, b"original-target".as_slice()).unwrap();
        }
        txn.open_table(BASE_SYMLINKS_TABLE).unwrap();
        txn.open_table(SYMLINK_TOMBSTONES_TABLE).unwrap();
        txn.open_table(COUNTERS_TABLE)
            .unwrap()
            .insert("removal-regression", 7)
            .unwrap();
    }
    txn.commit().unwrap();

    // Discover the symlink branch from an actual lookup, so the fault cannot
    // land in open_table's metadata lookup or in an unrelated table.
    // redb 4.3 lookup and removal of this first key follow the same branch.
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(SYMLINKS_TABLE)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        b"original-target"
    );
    drop(read);
    let branch = backend.last_branch_read.load(Ordering::SeqCst);
    assert_ne!(branch, 0, "fixture must have a symlink branch page");

    let result = (|| -> MetaResult<()> {
        let txn = db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        RedbMetaEngine::update_counter_in_txn(&txn, "removal-regression", |value| value + 1)?;
        backend.corrupt_read_at.store(branch, Ordering::SeqCst);
        RedbMetaEngine::remove_symlink_overlay_in_txn(&txn, 0)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    })();

    assert_eq!(backend.corrupt_read_at.load(Ordering::SeqCst), u64::MAX);
    // Reopen proves the earlier counter update and the deletion did not persist.
    drop(db);
    let reopened = Database::builder().create_with_backend(backend).unwrap();
    let read = reopened.begin_read().unwrap();
    assert_eq!(
        read.open_table(COUNTERS_TABLE)
            .unwrap()
            .get("removal-regression")
            .unwrap()
            .unwrap()
            .value(),
        7,
        "a failed removal must roll back prior metadata writes"
    );
    assert_eq!(
        read.open_table(SYMLINKS_TABLE)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        b"original-target"
    );
    assert!(
        matches!(&result, Err(MetaError::Storage(message))
            if message.contains("has order greater than the maximum")),
        "{result:?}"
    );
}

#[test]
fn dentry_removal_tolerates_an_absent_reverse_index() {
    let engine = temp_engine();
    let key = encode_dentry_key(ROOT_INO, b"missing-index");
    let entry = encode(&DirEntryValue {
        ino: 999,
        kind: FileType::RegularFile,
    })
    .unwrap();
    let txn = engine.begin_write_txn().unwrap();
    txn.open_table(DENTRIES_TABLE)
        .unwrap()
        .insert(key.as_slice(), entry.as_slice())
        .unwrap();
    // Seed only the primary row; removing the missing index must be a no-op.
    for _ in 0..2 {
        RedbMetaEngine::remove_dentry_overlay_in_txn(&txn, ROOT_INO, b"missing-index").unwrap();
    }
    txn.commit().unwrap();

    assert!(matches!(
        engine.lookup(ROOT_INO, b"missing-index"),
        Err(MetaError::EntryNotFound { .. })
    ));
}

#[test]
fn table_removals_of_absent_keys_remain_idempotent() {
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    let replica_removals = [
        MetaEvent::RemoveAttr { ino: 999 },
        MetaEvent::RemoveXattr {
            ino: 999,
            name: b"user.absent".to_vec(),
        },
        MetaEvent::RemoveAclRule {
            ino: 999,
            principal: "absent".to_string(),
        },
        MetaEvent::RemovePendingDeletion { object_id: 999 },
    ];
    for _ in 0..2 {
        RedbMetaEngine::set_remote_namespace_visibility_filter_in_txn(&txn, false).unwrap();
        RedbMetaEngine::remove_inode_overlay_in_txn(&txn, 999).unwrap();
        RedbMetaEngine::remove_inode_mirror_in_txn(&txn, 999).unwrap();
        RedbMetaEngine::remove_dentry_overlay_in_txn(&txn, ROOT_INO, b"absent").unwrap();
        RedbMetaEngine::remove_dentry_mirror_in_txn(&txn, ROOT_INO, b"absent").unwrap();
        RedbMetaEngine::remove_base_dentry_in_txn(&txn, ROOT_INO, b"absent").unwrap();
        RedbMetaEngine::remove_symlink_overlay_in_txn(&txn, 999).unwrap();
        RedbMetaEngine::remove_symlink_mirror_in_txn(&txn, 999).unwrap();
        RedbMetaEngine::remove_xattr_overlay_in_txn(&txn, 999, b"user.absent").unwrap();
        RedbMetaEngine::remove_xattr_mirror_in_txn(&txn, 999, b"user.absent").unwrap();
        RedbMetaEngine::apply_replica_aux_state_in_txn(&txn, &replica_removals).unwrap();
        RedbMetaEngine::apply_replica_events_to_base_in_txn(
            &txn,
            &replica_removals,
            &HashSet::new(),
        )
        .unwrap();
        RedbMetaEngine::apply_replica_serving_state_in_txn(&txn, &replica_removals, false).unwrap();
    }
    txn.commit().unwrap();
    for _ in 0..2 {
        engine.remove_acl_rule(999, "absent").unwrap();
    }
    assert!(engine.get_attr(ROOT_INO).is_ok());
    assert!(matches!(
        engine.lookup(ROOT_INO, b"absent"),
        Err(MetaError::EntryNotFound { .. })
    ));
}
