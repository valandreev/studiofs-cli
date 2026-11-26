use super::*;

pub(super) const LOCAL_UPGRADE_STAGE: &str = "local_redb_upgrade_stage";

pub(super) fn local_header(info: &FormatInfo, previous: Option<&[u8]>) -> MetaResult<Vec<u8>> {
    if cfg!(feature = "test-successor")
        && let Some(previous) = previous
    {
        let previous: serde_json::Value = serde_json::from_slice(previous)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        if let Some(revision) = previous.get(crate::LOCAL_REDB_REVISION) {
            let mut header = serde_json::to_value(info)
                .map_err(|error| MetaError::Serialization(error.to_string()))?;
            header[crate::LOCAL_REDB_REVISION] = revision.clone();
            return serde_json::to_vec(&header)
                .map_err(|error| MetaError::Serialization(error.to_string()));
        }
    }
    serde_json::to_vec(info).map_err(|error| MetaError::Serialization(error.to_string()))
}

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
        let version: crate::FormatVersionOnly = serde_json::from_slice(bytes.value())
            .map_err(|error| crate::invalid_format_info_json(&error))?;
        if version.format_version != 5 {
            return crate::decode_local_format_info(bytes.value());
        }
        let info: FormatInfo =
            serde_json::from_slice(bytes.value()).map_err(crate::format_info_decode_error)?;
        info.validate_format_contract()?;
        Ok(info)
    }

    /// Advance the stopped local metadata database to the compiled local revision.
    #[cfg(feature = "test-successor")]
    pub fn upgrade_local_revision(&self) -> MetaResult<()> {
        self.advance_local_revision(|_| Ok(()))
    }

    #[cfg(feature = "test-successor")]
    pub(crate) fn advance_local_revision(
        &self,
        checkpoint: impl Fn(u8) -> MetaResult<()>,
    ) -> MetaResult<()> {
        checkpoint(0)?;
        loop {
            let txn = self.begin_immediate_write_txn()?;
            let next = {
                let mut table = txn
                    .open_table(SETTINGS_TABLE)
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
                let mut header: serde_json::Value = {
                    let bytes = table.get("format")?.ok_or(MetaError::NotFormatted)?;
                    crate::decode_local_format_info(bytes.value())?;
                    serde_json::from_slice(bytes.value())
                        .map_err(|error| crate::invalid_format_info_json(&error))?
                };
                let stage = table
                    .get(LOCAL_UPGRADE_STAGE)?
                    .map(|stage| stage.value().to_vec());
                let activated = header.get(crate::LOCAL_REDB_REVISION).is_some();
                let next = match (stage.as_deref(), activated) {
                    (None, true) => return Ok(()),
                    (None, false) => 1,
                    (Some([1]), false) => 2,
                    (Some([2]), false) => {
                        header[crate::LOCAL_REDB_REVISION] =
                            crate::version::LOCAL_METADATA_LAYOUT.current.into();
                        let bytes = serde_json::to_vec(&header)
                            .map_err(|error| MetaError::Serialization(error.to_string()))?;
                        table.insert("format", bytes.as_slice())?;
                        3
                    }
                    (Some([3]), true) => {
                        checkpoint(4)?;
                        table.remove(LOCAL_UPGRADE_STAGE)?;
                        0
                    }
                    _ => {
                        return Err(MetaError::RepairRequired(
                            "invalid local redb upgrade stage".into(),
                        ));
                    }
                };
                if next != 0 {
                    table.insert(LOCAL_UPGRADE_STAGE, [next].as_slice())?;
                }
                next
            };
            txn.commit()
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            if next == 0 {
                return Ok(());
            }
            checkpoint(next)?;
        }
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
            let (mut local, previous) = {
                let bytes = table.get("format")?.ok_or(MetaError::NotFormatted)?;
                let version: crate::FormatVersionOnly = serde_json::from_slice(bytes.value())
                    .map_err(|error| crate::invalid_format_info_json(&error))?;
                let local: FormatInfo = if version.format_version == 5 {
                    serde_json::from_slice(bytes.value())
                        .map_err(crate::format_info_decode_error)?
                } else {
                    crate::decode_local_format_info(bytes.value())?
                };
                (local, bytes.value().to_vec())
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
            let bytes = local_header(&local, Some(&previous))?;
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
        if cfg!(feature = "test-successor") && table.get(LOCAL_UPGRADE_STAGE)?.is_some() {
            return Err(MetaError::UpgradeRequired {
                reason: crate::UpgradeRequiredReason::LocalRedbUpgradePending,
            });
        }
        let entry = table
            .get("format")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::NotFormatted)?;
        crate::decode_local_format_info(entry.value())
    }

    /// Read persisted format metadata from a redb file without opening the full engine.
    pub fn read_format_info_readonly(path: impl AsRef<Path>) -> MetaResult<FormatInfo> {
        let txn = Self::open_readonly(path)?
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::read_format_info_from_read_txn(&txn)
    }

    /// Like [`Self::format_info_for_upgrade`], read-only and without
    /// creating tables, for `studiofs upgrade --dry-run`.
    pub fn read_format_info_for_upgrade_readonly(path: impl AsRef<Path>) -> MetaResult<FormatInfo> {
        let txn = Self::open_readonly(path)?
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::read_format_info_for_upgrade_from_read_txn(&txn)
    }

    fn open_readonly(path: impl AsRef<Path>) -> MetaResult<ReadOnlyDatabase> {
        match ReadOnlyDatabase::open(path.as_ref()) {
            Ok(db) => Ok(db),
            Err(redb::DatabaseError::Storage(redb::StorageError::Io(err)))
                if err.kind() == ErrorKind::NotFound =>
            {
                Err(MetaError::NotFormatted)
            }
            Err(redb::DatabaseError::RepairAborted) => Err(MetaError::RepairRequired(
                "redb database requires repair".to_string(),
            )),
            Err(e) => Err(MetaError::Storage(format!(
                "failed to open redb read-only: {e}"
            ))),
        }
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
        crate::decode_local_format_info(entry.value())
    }
}

impl MetaFormat for RedbMetaEngine {
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        info.validate_format_contract()?;
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let json = {
                let previous = table.get("format")?;
                local_header(info, previous.as_ref().map(|value| value.value()))?
            };
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
