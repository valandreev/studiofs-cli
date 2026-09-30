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
            if self
                .corrupt_read_at
                .compare_exchange(offset, u64::MAX, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // redb 4.3 branch layout: 8-byte header, per-child 16-byte
                // checksums, then 8-byte pointers. Give child 0 an invalid
                // page order, as in redb's corrupted_btree_descent regression.
                // Only the read buffer changes; stored bytes remain intact.
                let num_keys = usize::from(u16::from_le_bytes([out[2], out[3]]));
                let first_child = 8 + 16 * (num_keys + 1);
                out[first_child + 7] |= 0xF8;
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
    assert!(matches!(result, Err(MetaError::Storage(_))), "{result:?}");
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
