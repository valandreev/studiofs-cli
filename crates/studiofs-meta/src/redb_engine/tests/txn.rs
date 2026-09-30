use super::*;
use redb::StorageBackend;
use std::sync::atomic::{AtomicU64, Ordering};

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
        if out.first() == Some(&2) {
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

#[test]
fn removal_fault_rejects_invalid_branch_layout() {
    for bytes in [
        &[2, 0, 0][..],
        &[2, 0, 0, 0, 0, 0, 0, 0][..],
        &[2, 0, 0xFF, 0xFF, 0, 0, 0, 0][..],
    ] {
        let backend = RemoveFaultBackend::default();
        let offset = 8;
        backend.set_len(offset + bytes.len() as u64).unwrap();
        backend.write(offset, bytes).unwrap();
        backend.corrupt_read_at.store(offset, Ordering::SeqCst);
        let mut out = vec![0; bytes.len()];

        let error = backend.read(offset, &mut out).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(backend.corrupt_read_at.load(Ordering::SeqCst), offset);
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
    }
    txn.commit().unwrap();
    assert!(engine.get_attr(ROOT_INO).is_ok());
    assert!(matches!(
        engine.lookup(ROOT_INO, b"absent"),
        Err(MetaError::EntryNotFound { .. })
    ));
}
