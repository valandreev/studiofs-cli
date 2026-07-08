use super::*;

impl MetaSymlink for RedbMetaEngine {
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            Self::set_symlink_overlay_in_txn(&txn, ino, target)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetSymlinkTarget {
                    ino,
                    target: target.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        let db = self.read_db()?;
        let txn = begin_redb_read_txn(&db)?;
        self.merged_symlink_target(&txn, ino)?
            .ok_or(MetaError::InodeNotFound(ino))
    }
}
