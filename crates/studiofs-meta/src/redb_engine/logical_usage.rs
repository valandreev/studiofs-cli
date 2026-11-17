use super::*;

// Inodes start at ROOT_INO (1). Key zero stores the sum at the same revision.
pub(super) const LOGICAL_USAGE: TableDefinition<u64, u128> =
    TableDefinition::new("local_logical_usage");

/// Mutations of local writer metadata can intentionally suppress cache invalidation.
pub(super) fn record_inode_size(txn: &WriteTransaction, ino: Ino, size: u64) -> MetaResult<()> {
    let mut index = txn
        .open_table(LOGICAL_USAGE)
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    let Some(mut total) = index.get(0)?.map(|v| v.value()) else {
        return Ok(());
    };
    if adjust_charge(&mut index, &mut total, ino, u128::from(size))? {
        index.insert(0, total)?;
    }
    Ok(())
}

fn adjust_charge(
    index: &mut redb::Table<'_, u64, u128>,
    total: &mut u128,
    ino: Ino,
    size: u128,
) -> MetaResult<bool> {
    let old = index.get(ino)?.map_or(0, |v| v.value());
    if old == size {
        return Ok(false);
    }
    if size == 0 {
        index.remove(ino)?;
    } else {
        index.insert(ino, size)?;
    }
    *total = total
        .checked_sub(old)
        .ok_or_else(|| MetaError::Storage("inconsistent logical usage index".into()))?
        + size;
    Ok(true)
}

impl RedbWriteTransaction<'_> {
    pub(super) fn update_logical_usage(&self) -> MetaResult<()> {
        let mut index = self
            .open_table(LOGICAL_USAGE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let previous = index.get(0)?.map(|v| v.value());
        let changed = self.invalidations.borrow().affected_inodes();
        if previous.is_some() && changed.as_ref().is_some_and(HashSet::is_empty) {
            return Ok(());
        }
        let local = self
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = self
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = self
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let (mut total, inodes) = match (previous, changed) {
            (Some(total), Some(inodes)) => (total, inodes),
            _ => {
                index.retain(|_, _| false)?;
                let mut inodes = HashSet::new();
                for table in [&local, &base] {
                    for item in table.iter()? {
                        inodes.insert(item?.0.value());
                    }
                }
                (0, inodes)
            }
        };
        for ino in inodes {
            let size = match RedbMetaEngine::resolve_attr_from_opened_tables(
                &base,
                &local,
                &tombstones,
                ino,
                true,
            ) {
                Ok(attr) if attr.kind == FileType::RegularFile => u128::from(attr.size),
                Ok(_) | Err(MetaError::InodeNotFound(_)) => 0,
                Err(e) => return Err(e),
            };
            adjust_charge(&mut index, &mut total, ino, size)?;
        }
        index.insert(0, total)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_usage_migrates_existing_rows_and_rolls_back_with_inode() {
        let tmp = studiofs_test_support::workspace_target_tempdir("quota-index-");
        let path = tmp.path().join("meta.redb");
        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root(0, 0).unwrap();
        let mut file = engine.create_file(ROOT_INO, b"file", 0, 0, 0o600).unwrap();
        file.size = 123;
        engine.set_attr(&file).unwrap();
        // Simulate a cache created by a binary predating the derived index.
        drop(engine);
        {
            let db = Database::open(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.delete_table(LOGICAL_USAGE).unwrap();
            txn.commit().unwrap();
        }
        let engine = RedbMetaEngine::open(&path).unwrap();
        assert_eq!(engine.logical_usage(&HashMap::new()).unwrap(), 123);
        let txn = engine.begin_write_txn().unwrap();
        file.size = 456;
        RedbMetaEngine::set_inode_in_txn(&txn, &file).unwrap();
        txn.abort().unwrap();
        assert_eq!(engine.logical_usage(&HashMap::new()).unwrap(), 123);
        assert_eq!(
            engine
                .logical_usage(&HashMap::from([(file.ino, 130)]))
                .unwrap(),
            130
        );
        let replica = RedbMetaEngine::open(tmp.path().join("replica.redb")).unwrap();
        replica
            .import_snapshot(&engine.export_snapshot().unwrap())
            .unwrap();
        assert_eq!(replica.logical_usage(&HashMap::new()).unwrap(), 123);
    }
}
