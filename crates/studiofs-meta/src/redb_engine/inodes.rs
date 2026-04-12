impl RedbMetaEngine {
    fn local_overlay_inodes_from_txn(txn: &redb::ReadTransaction) -> MetaResult<HashSet<Ino>> {
        let mut overlay = HashSet::new();

        let file_blocks = txn
            .open_table(FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let committed = txn
            .open_table(COMMITTED_FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in file_blocks
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (ino, _block_index) = decode_file_block_key(key.value());
            let committed_value = committed
                .get(key.value())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            match committed_value {
                Some(committed_value) if committed_value.value() == value.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }

        let regular_overlay =
            Self::regular_file_chunk_states_from_txn(txn, REGULAR_FILE_OVERLAY_TABLE)?;
        let regular_base = Self::regular_file_chunk_states_from_txn(txn, REGULAR_FILE_BASE_TABLE)?;
        for (ino, overlay_chunks) in regular_overlay {
            let overlay_signature = Self::regular_file_graph_signature(&overlay_chunks);
            if overlay_signature.is_empty() {
                continue;
            }
            let base_signature = regular_base
                .get(&ino)
                .map(Self::regular_file_graph_signature)
                .unwrap_or_default();
            if overlay_signature != base_signature {
                overlay.insert(ino);
            }
        }

        Ok(overlay)
    }


    fn local_overlay_attr_inodes_from_txn(txn: &redb::ReadTransaction) -> MetaResult<HashSet<Ino>> {
        let current = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, current_bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ino = ino.value();
            match base
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_bytes) if base_bytes.value() == current_bytes.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }

        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(ino.value());
        }

        Ok(overlay)
    }

    fn resolve_attr_in_read_snapshot(
        txn: &redb::ReadTransaction,
        ino: Ino,
        overlay_block_inodes: &HashSet<Ino>,
        overlay_attr_inodes: &HashSet<Ino>,
    ) -> MetaResult<InodeAttr> {
        if Self::has_u64_marker(txn, INODE_TOMBSTONES_TABLE, ino)? {
            return Err(MetaError::InodeNotFound(ino));
        }

        if overlay_attr_inodes.contains(&ino) {
            return Self::get_attr_from_table(txn, INODES_TABLE, ino)?
                .ok_or(MetaError::InodeNotFound(ino));
        }

        if overlay_block_inodes.contains(&ino) {
            if let Some(attr) = Self::get_attr_from_table(txn, INODES_TABLE, ino)? {
                return Ok(attr);
            }
            if let Some(attr) = Self::get_attr_from_table(txn, BASE_INODES_TABLE, ino)? {
                return Ok(attr);
            }
            return Err(MetaError::InodeNotFound(ino));
        }

        if let Some(attr) = Self::get_attr_from_table(txn, BASE_INODES_TABLE, ino)? {
            return Ok(attr);
        }

        Self::get_attr_from_table(txn, INODES_TABLE, ino)?.ok_or(MetaError::InodeNotFound(ino))
    }


    pub fn local_overlay_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::local_overlay_inodes_from_txn(&txn)
    }

    /// Returns inode attrs that diverged from the last imported/applied base view.
    pub fn local_overlay_attr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::local_overlay_attr_inodes_from_txn(&txn)
    }

}

impl MetaInodes for RedbMetaEngine {
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match table
            .get(name)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let new_val = Self::update_counter_in_txn(&txn, name, |current| current + delta)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let new_val =
            Self::update_counter_in_txn(&txn, name, |current| current.saturating_sub(delta))?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(name, value)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let ino;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            ino = match table
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => ROOT_INO + 1,
            };
            table
                .insert(counters::NEXT_INODE, ino + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(ino)
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;
        Self::resolve_attr_in_read_snapshot(&txn, ino, &overlay_block_inodes, &overlay_attr_inodes)
    }

    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;
        let mut attrs = Vec::with_capacity(inos.len());
        for &ino in inos {
            match Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let committed = txn
                    .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let has_committed_manifest =
                    !Self::committed_manifest_in_txn(&committed, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Self::set_inode_overlay_in_txn(&txn, &normalized)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: normalized.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_inode_overlay_in_txn(&txn, ino)?;

            Self::clear_inode_overlay_aux_in_txn(&txn, ino)?;

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveAttr { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;

        let mut inos = std::collections::BTreeSet::new();
        for table_def in [BASE_INODES_TABLE, INODES_TABLE] {
            let table = txn
                .open_table(table_def)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (ino, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                inos.insert(ino.value());
            }
        }

        let mut attrs = Vec::with_capacity(inos.len());
        for ino in inos {
            match Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }
}
