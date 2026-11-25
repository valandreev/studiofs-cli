use super::*;

impl RedbMetaEngine {
    /// Read a stopped cache's migration header while retaining exclusive database ownership.
    /// This deliberately accepts only the previous and current format.
    pub fn format_info_for_upgrade(&self) -> MetaResult<FormatInfo> {
        let txn = self.begin_read_txn()?;
        Self::read_format_info_for_upgrade_from_read_txn(&txn)
    }

    pub(super) fn read_format_info_for_upgrade_from_read_txn(
        txn: &redb::ReadTransaction,
    ) -> MetaResult<FormatInfo> {
        let table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let bytes = table.get("format")?.ok_or(MetaError::NotFormatted)?;
        let info: FormatInfo = serde_json::from_slice(bytes.value())
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        if !matches!(info.format_version, 5 | 6) {
            return Err(MetaError::UnsupportedFormatVersion {
                found: info.format_version,
                expected: 6,
            });
        }
        info.validate_format_contract()?;
        Ok(info)
    }

    /// Finish a stopped local cache's header upgrade after shared authority was
    /// upgraded. No journal, accepted payload state, inode or counter is reset.
    /// The caller must supply the format fetched from upgraded shared authority.
    pub fn upgrade_local_format5(&self, shared: &FormatInfo) -> MetaResult<()> {
        crate::ensure_supported_format_version(shared)?;
        shared.validate_format_contract()?;
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            let mut local: FormatInfo = {
                let bytes = table.get("format")?.ok_or(MetaError::NotFormatted)?;
                serde_json::from_slice(bytes.value())
                    .map_err(|error| MetaError::Serialization(error.to_string()))?
            };
            if !matches!(local.format_version, 5 | 6) {
                return Err(MetaError::UnsupportedFormatVersion {
                    found: local.format_version,
                    expected: 5,
                });
            }
            local.format_version = 6;
            if serde_json::to_value(&local)
                .map_err(|error| MetaError::Serialization(error.to_string()))?
                != serde_json::to_value(shared)
                    .map_err(|error| MetaError::Serialization(error.to_string()))?
            {
                return Err(MetaError::Storage(
                    "local and shared format identity differ; cache was not changed".into(),
                ));
            }
            let bytes = serde_json::to_vec(&local)
                .map_err(|error| MetaError::Serialization(error.to_string()))?;
            table.insert("format", bytes.as_slice())?;
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

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
        crate::decode_format_info_as(entry.value(), crate::version::LOCAL_METADATA_LAYOUT)
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
        crate::decode_format_info_as(entry.value(), crate::version::LOCAL_METADATA_LAYOUT)
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
