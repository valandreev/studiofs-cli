use super::*;

impl RedbMetaEngine {
    pub(super) fn read_format_info_from_read_txn(
        txn: &redb::ReadTransaction,
    ) -> MetaResult<FormatInfo> {
        let table = match txn.open_table(SETTINGS_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Err(MetaError::NotFormatted),
            Err(error) => return Err(MetaError::Storage(error.to_string())),
        };
        let entry = table
            .get("format")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::NotFormatted)?;
        crate::decode_format_info(entry.value())
    }

    /// Read persisted format metadata from a redb file without opening the full engine.
    pub fn read_format_info_readonly(path: impl AsRef<Path>) -> MetaResult<FormatInfo> {
        let db = match ReadOnlyDatabase::open(path.as_ref()) {
            Ok(db) => db,
            Err(redb::DatabaseError::Storage(redb::StorageError::Io(err)))
                if err.kind() == ErrorKind::NotFound =>
            {
                return Err(MetaError::NotFormatted);
            }
            Err(redb::DatabaseError::RepairAborted) => {
                return Err(MetaError::RepairRequired(
                    "redb database requires repair".to_string(),
                ));
            }
            Err(e) => {
                return Err(MetaError::Storage(format!(
                    "failed to open redb read-only: {e}"
                )));
            }
        };
        let txn = db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::read_format_info_from_read_txn(&txn)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn format_info_from_write_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<FormatInfo> {
        let table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry = table
            .get("format")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::NotFormatted)?;
        crate::decode_format_info(entry.value())
    }
}

impl MetaFormat for RedbMetaEngine {
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        info.validate_format_contract()?;
        let json = serde_json::to_vec(info).map_err(|e| MetaError::Serialization(e.to_string()))?;
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert("format", json.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        let txn = self.begin_read_txn()?;
        Self::read_format_info_from_read_txn(&txn)
    }
}
