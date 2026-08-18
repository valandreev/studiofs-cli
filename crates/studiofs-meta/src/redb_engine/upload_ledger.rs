use super::*;

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LocalUploadedFragmentLedgerRecord {
    pub(crate) entry: UploadedFragmentLedgerEntry,
    pub(crate) begin_replicated: bool,
    pub(crate) confirmed: bool,
}

fn ledger_key(entry: &UploadedFragmentLedgerEntry) -> [u8; 20] {
    let mut key = [0; 20];
    key[..8].copy_from_slice(&entry.volume_id.to_le_bytes());
    key[8..16].copy_from_slice(&entry.object_id.to_le_bytes());
    key[16..].copy_from_slice(&entry.fragment_index.to_le_bytes());
    key
}

impl RedbMetaEngine {
    pub(crate) fn record_local_upload_ledger_begin(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut table = txn
                .open_table(LOCAL_UPLOAD_LEDGER_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            for entry in entries {
                let record = LocalUploadedFragmentLedgerRecord {
                    entry: entry.clone(),
                    begin_replicated: false,
                    confirmed: false,
                };
                let key = ledger_key(entry);
                let value = encode(&record)?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    pub(crate) fn record_local_upload_ledger_confirm(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut table = txn
                .open_table(LOCAL_UPLOAD_LEDGER_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            for entry in entries {
                let key = ledger_key(entry);
                let begin_replicated = table
                    .get(key.as_slice())
                    .map_err(|error| MetaError::Storage(error.to_string()))?
                    .map(|value| decode::<LocalUploadedFragmentLedgerRecord>(value.value()))
                    .transpose()?
                    .is_some_and(|record| record.entry == *entry && record.begin_replicated);
                let record = LocalUploadedFragmentLedgerRecord {
                    entry: entry.clone(),
                    begin_replicated,
                    confirmed: true,
                };
                let value = encode(&record)?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    pub(crate) fn pending_local_upload_ledger(
        &self,
        limit: usize,
    ) -> MetaResult<Vec<LocalUploadedFragmentLedgerRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(LOCAL_UPLOAD_LEDGER_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        table
            .iter()
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .take(limit)
            .map(|row| {
                let (_, value) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                decode(value.value())
            })
            .collect()
    }

    pub(crate) fn mark_local_upload_ledger_begun(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut table = txn
                .open_table(LOCAL_UPLOAD_LEDGER_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            for entry in entries {
                let key = ledger_key(entry);
                let current = table
                    .get(key.as_slice())
                    .map_err(|error| MetaError::Storage(error.to_string()))?
                    .map(|value| decode::<LocalUploadedFragmentLedgerRecord>(value.value()))
                    .transpose()?
                    .filter(|record| record.entry == *entry);
                let Some(current) = current else {
                    continue;
                };
                if current.confirmed {
                    let value = encode(&LocalUploadedFragmentLedgerRecord {
                        entry: current.entry,
                        begin_replicated: true,
                        confirmed: true,
                    })?;
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(|error| MetaError::Storage(error.to_string()))?;
                } else {
                    table
                        .remove(key.as_slice())
                        .map_err(|error| MetaError::Storage(error.to_string()))?;
                }
            }
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    pub(crate) fn remove_local_upload_ledger_confirmed(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut table = txn
                .open_table(LOCAL_UPLOAD_LEDGER_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            for entry in entries {
                let key = ledger_key(entry);
                let current = table
                    .get(key.as_slice())
                    .map_err(|error| MetaError::Storage(error.to_string()))?
                    .map(|value| decode::<LocalUploadedFragmentLedgerRecord>(value.value()))
                    .transpose()?
                    .is_some_and(|record| {
                        record.entry == *entry && record.begin_replicated && record.confirmed
                    });
                if current {
                    table
                        .remove(key.as_slice())
                        .map_err(|error| MetaError::Storage(error.to_string()))?;
                }
            }
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }
}
