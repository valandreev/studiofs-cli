use super::*;
use redb::CompactionError;

impl MetaGc for RedbMetaEngine {
    fn compact_local_storage(&self) -> MetaResult<LocalStorageCompactionResult> {
        let mut db = match self.db.try_write() {
            Ok(db) => db,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Ok(LocalStorageCompactionResult::SkippedBusy);
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(MetaError::Storage("redb lock poisoned".to_string()));
            }
        };
        match db.compact() {
            Ok(true) => Ok(LocalStorageCompactionResult::Compacted),
            Ok(false) => Ok(LocalStorageCompactionResult::NoWork),
            Err(CompactionError::TransactionInProgress) => {
                Ok(LocalStorageCompactionResult::SkippedBusy)
            }
            Err(error) => Err(MetaError::Storage(format!(
                "failed to compact local redb: {error}"
            ))),
        }
    }

    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(object_id, timestamp)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .remove(object_id)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemovePendingDeletion { object_id },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(PENDING_DELETIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let deletions: Vec<(u64, i64)> = table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.value(), v.value()))
            .collect();

        Ok(deletions)
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        let txn = self.begin_read_txn()?;
        let snapshots = txn
            .open_table(SNAPSHOTS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut refs = HashSet::new();
        Self::regular_file_graph_object_ids_from_table(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            &mut refs,
        )?;
        Self::regular_file_graph_object_ids_from_table(&txn, REGULAR_FILE_BASE_TABLE, &mut refs)?;
        for item in snapshots
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let snapshot: MetadataSnapshot = decode(v.value())?;
            refs.extend(snapshot.pinned_object_ids);
        }
        Ok(refs)
    }
}
