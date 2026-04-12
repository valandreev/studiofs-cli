impl RedbMetaEngine {
    fn add_pending_create_intent(&self, intent: &PendingCreateRecord) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let encoded = encode(intent)?;
            table
                .insert(intent.ino, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let undo = UndoLogRecord {
                op_id: Self::next_undo_op_id_in_txn(&txn)?,
                kind: UndoLogKind::PendingCreate,
                phase: UndoLogPhase::Started,
                ino: intent.ino,
                parent: intent.parent,
                name: intent.name.clone(),
                created_at: intent.created_at,
            };
            Self::append_undo_log_in_txn(&txn, &undo)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_pending_create_intent(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = table
                .remove(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::remove_undo_logs_for_in_txn(&txn, UndoLogKind::PendingCreate, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.cleanup_stale_undo_logs()?;
        Ok(())
    }

    fn list_pending_create_intents(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(PENDING_CREATES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(decode(v.value())?);
        }
        Ok(out)
    }

    fn recover_pending_create_intents(&self) -> MetaResult<Vec<Ino>> {
        let intents = self.list_pending_create_intents()?;
        if intents.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut healed = 0u64;
        let mut touched_parents: HashSet<Ino> = HashSet::new();
        for intent in intents {
            Self::set_undo_log_phase_for_in_txn(
                &txn,
                UndoLogKind::PendingCreate,
                intent.ino,
                UndoLogPhase::Replaying,
            )?;
            let entry = Self::lookup_merged_in_write_txn(&txn, intent.parent, &intent.name)?;
            let inode_exists = Self::get_attr_merged_in_write_txn(&txn, intent.ino).is_ok();

            if let Some(entry) = entry
                && entry.ino == intent.ino
                && !inode_exists
            {
                Self::remove_dentry_overlay_in_txn(&txn, intent.parent, &intent.name)?;
                healed = healed.saturating_add(1);
                touched_parents.insert(intent.parent);
            }

            let mut table = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = table
                .remove(intent.ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::remove_undo_logs_for_in_txn(&txn, UndoLogKind::PendingCreate, intent.ino)?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if healed > 0 {
            debug!(healed, "recovered pending create intents");
        }
        Ok(touched_parents.into_iter().collect())
    }

    fn cleanup_stale_undo_logs(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let undo_entries: Vec<(u64, UndoLogRecord)> = {
            let undo = txn
                .open_table(UNDO_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut entries = Vec::new();
            for row in undo.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let rec: UndoLogRecord = decode(v.value())?;
                entries.push((k.value(), rec));
            }
            entries
        };

        if undo_entries.is_empty() {
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(());
        }

        let mut stale_op_ids = Vec::new();
        {
            let pending = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let deferred = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (op_id, rec) in &undo_entries {
                if rec.phase == UndoLogPhase::Completed {
                    stale_op_ids.push(*op_id);
                    continue;
                }
                let is_live = match rec.kind {
                    UndoLogKind::PendingCreate => pending
                        .get(rec.ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .is_some(),
                    UndoLogKind::DeferredDelete => deferred
                        .get(rec.ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .is_some(),
                };
                if !is_live {
                    stale_op_ids.push(*op_id);
                }
            }
        }

        if !stale_op_ids.is_empty() {
            let mut undo = txn
                .open_table(UNDO_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for op_id in stale_op_ids {
                let _ = undo
                    .remove(op_id)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl MetaRecovery for RedbMetaEngine {
    fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let encoded = encode(record)?;
            table
                .insert(record.ino, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = table
                .remove(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::remove_undo_logs_for_in_txn(&txn, UndoLogKind::DeferredDelete, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.cleanup_stale_undo_logs()?;
        Ok(())
    }

    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(DEFERRED_DELETES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(decode(value.value())?);
        }
        Ok(out)
    }

    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>> {
        self.recover_pending_create_intents()
    }

    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>> {
        let mut touched_parents: HashSet<Ino> =
            self.recover_pending_create_intents()?.into_iter().collect();

        let deferred_by_ino: std::collections::HashMap<Ino, DeferredDeleteRecord> = self
            .list_deferred_deletes()?
            .into_iter()
            .map(|record| (record.ino, record))
            .collect();
        let undo_logs = self.list_undo_logs()?;
        if !undo_logs.is_empty() {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            for record in undo_logs {
                if record.kind != UndoLogKind::DeferredDelete {
                    continue;
                }
                if record.phase == UndoLogPhase::Completed {
                    continue;
                }
                let Some(deferred) = deferred_by_ino.get(&record.ino) else {
                    continue;
                };
                self.set_undo_log_phase_for(
                    record.kind.clone(),
                    record.ino,
                    UndoLogPhase::Replaying,
                )?;

                let blocks = match self.get_file_blocks(record.ino) {
                    Ok(blocks) => blocks,
                    Err(MetaError::InodeNotFound(_)) => Vec::new(),
                    Err(err) => return Err(err),
                };
                let total_bytes: u64 = blocks.iter().map(|(_, b)| b.length as u64).sum();
                let removed_ids = match self.remove_file_blocks(record.ino) {
                    Ok(ids) => ids,
                    Err(MetaError::InodeNotFound(_)) => Vec::new(),
                    Err(err) => return Err(err),
                };
                for object_id in removed_ids {
                    self.add_pending_deletion(object_id, timestamp)?;
                }
                if total_bytes > 0 {
                    let _ = self.decr_counter(counters::USED_SPACE, total_bytes)?;
                }
                match self.remove_attr(record.ino) {
                    Ok(()) => {}
                    Err(MetaError::InodeNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
                self.remove_deferred_delete(record.ino)?;
                touched_parents.insert(deferred.parent);
            }
        }

        self.cleanup_stale_undo_logs()?;
        Ok(touched_parents.into_iter().collect())
    }

    fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()> {
        self.add_pending_create_intent(record)
    }

    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()> {
        self.remove_pending_create_intent(ino)?;
        self.cleanup_stale_undo_logs()?;
        Ok(())
    }

    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        self.list_pending_create_intents()
    }

    fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::append_undo_log_in_txn(&txn, record)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_undo_logs_for_in_txn(&txn, kind, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(UNDO_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(decode(v.value())?);
        }
        Ok(out)
    }
}
