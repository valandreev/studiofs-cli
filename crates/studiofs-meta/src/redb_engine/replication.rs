use super::*;

struct RenameDentryContext<'a> {
    source_parent: Ino,
    source_name: &'a [u8],
    destination_parent: Ino,
    destination_name: &'a [u8],
}

impl RedbMetaEngine {
    fn unique_dentry_key_for_entry_in_write_table(
        txn: &redb::WriteTransaction,
        table_def: redb::TableDefinition<&'static [u8], &'static [u8]>,
        index_def: redb::TableDefinition<&'static [u8], u8>,
        entry: &DirEntryValue,
        context: RenameDentryContext<'_>,
    ) -> MetaResult<Option<Vec<u8>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut matched_keys = Vec::new();
        for key in Self::dentry_keys_for_inode_in_write_index(txn, index_def, entry.ino)? {
            let Some(value) = table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            else {
                continue;
            };
            let current: DirEntryValue = decode(value.value())?;
            if current.ino == entry.ino && current.kind == entry.kind {
                matched_keys.push(key);
            }
        }
        if matched_keys.len() > 1 {
            tracing::warn!(
                ino = entry.ino,
                source_parent = context.source_parent,
                source_name = %String::from_utf8_lossy(context.source_name),
                destination_parent = context.destination_parent,
                destination_name = %String::from_utf8_lossy(context.destination_name),
                candidate_count = matched_keys.len(),
                "skipping rename conflict relocation for inode with multiple dentries"
            );
            return Ok(None);
        }
        Ok(matched_keys.pop())
    }

    /// Export a snapshot projected to the remote-visible published namespace.
    pub fn export_remote_snapshot(&self) -> MetaResult<RedbSnapshot> {
        let mut snapshot = self.export_snapshot()?;
        let mut published_regular_inodes = HashSet::new();
        for (ino, bytes) in &snapshot.regular_file_base {
            let regular: RegularFileBaseState = decode(bytes)?;
            if regular.chunks.values().any(|chunk| {
                chunk.slices.values().any(|slice| {
                    slice.payload_state == PayloadRefState::Published && slice.payload.is_some()
                })
            }) {
                published_regular_inodes.insert(*ino);
            }
        }

        let mut projected_inodes = Vec::with_capacity(snapshot.inodes.len());
        let mut hidden_regular_inodes = HashSet::new();
        for (ino, bytes) in snapshot.inodes.drain(..) {
            let mut attr: InodeAttr = decode(&bytes)?;
            if attr.kind == FileType::RegularFile {
                let visible_in_remote_namespace =
                    Self::regular_file_remote_visible_without_published_base(&attr)
                        || published_regular_inodes.contains(&ino);
                if !visible_in_remote_namespace {
                    hidden_regular_inodes.insert(ino);
                    continue;
                }
                attr.size = attr.committed_size;
            }
            projected_inodes.push((ino, encode(&attr)?));
        }
        snapshot.inodes = projected_inodes;
        snapshot.dentries = snapshot
            .dentries
            .drain(..)
            .filter_map(|(key, bytes)| {
                decode::<DirEntryValue>(&bytes)
                    .ok()
                    .filter(|entry| !hidden_regular_inodes.contains(&entry.ino))
                    .map(|_| (key, bytes))
            })
            .collect();
        snapshot
            .regular_file_overlay
            .retain(|(ino, _)| !hidden_regular_inodes.contains(ino));
        snapshot
            .regular_file_base
            .retain(|(ino, _)| !hidden_regular_inodes.contains(ino));

        let mut referenced_objects = BTreeMap::new();
        for (_, bytes) in &snapshot.regular_file_base {
            let regular: RegularFileBaseState = decode(bytes)?;
            for chunk in regular.chunks.values() {
                for slice in chunk.slices.values() {
                    if let Some(block) = slice.payload.as_ref() {
                        referenced_objects
                            .entry(block.object_id)
                            .or_insert(block.length as u64);
                    }
                }
            }
        }
        let committed_used_space = referenced_objects
            .values()
            .fold(0_u64, |acc, len| acc.saturating_add(*len));

        snapshot.regular_file_overlay = snapshot.regular_file_base.clone();
        snapshot.event_log.clear();
        snapshot
            .counters
            .retain(|(name, _)| name.as_str() != "nextEventSeq");
        if let Some((_, value)) = snapshot
            .counters
            .iter_mut()
            .find(|(name, _)| name.as_str() == counters::USED_SPACE)
        {
            *value = committed_used_space;
        } else {
            snapshot
                .counters
                .push((counters::USED_SPACE.to_string(), committed_used_space));
        }

        snapshot.snapshots.retain(|(_id, raw)| {
            decode::<MetadataSnapshot>(raw)
                .map(|snapshot| snapshot.state == SnapshotState::Synced)
                .unwrap_or(false)
        });

        Ok(snapshot)
    }

    /// Return symlink inode ids whose local state differs from the imported base view.
    pub fn local_overlay_symlink_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
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

    /// Return encoded xattr keys whose local state differs from the imported base view.
    pub fn local_overlay_xattr_keys(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, current_value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == current_value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }

        Ok(overlay)
    }

    /// Return inode ids that currently have locally divergent xattr state.
    pub fn local_overlay_xattr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        self.local_overlay_xattr_keys()?
            .into_iter()
            .map(|key| decode_inode_prefix(&key, "xattr"))
            .collect()
    }

    /// Return encoded ACL keys whose local state differs from the imported base view.
    pub fn local_overlay_acl_keys(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(ACL_RULE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, current_value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == current_value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }
        Ok(overlay)
    }

    /// Returns true if local metadata still contains overlay state that is not
    /// yet remote-visible in the base view.
    pub fn has_local_overlay_state(&self) -> MetaResult<bool> {
        if !self.local_overlay_inodes()?.is_empty() {
            return Ok(true);
        }

        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let has_base_metadata = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .next()
            .is_some()
            || txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(REGULAR_FILE_BASE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some();

        if !has_base_metadata {
            return Ok(false);
        }

        Ok(!self.local_overlay_attr_inodes()?.is_empty()
            || !self.local_overlay_dentries()?.is_empty()
            || !self.local_overlay_symlink_inodes()?.is_empty()
            || !self.local_overlay_xattr_keys()?.is_empty()
            || !self.local_overlay_acl_keys()?.is_empty())
    }

    /// Read decoded events from local event log in [start_seq, end_seq).
    pub fn read_events_range(
        &self,
        start_seq: u64,
        end_seq: u64,
    ) -> MetaResult<Vec<crate::events::MetaEvent>> {
        if end_seq <= start_seq {
            return Ok(Vec::new());
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ev: crate::events::MetaEvent = decode(bytes.value())?;
            out.push(ev);
        }
        Ok(out)
    }

    /// Apply only remote-visible auxiliary state from foreign replica events
    /// without re-appending it to the local event log. Namespace/attr state is
    /// kept in `base_*`.
    pub fn apply_replica_aux_state(&self, events: &[crate::events::MetaEvent]) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { .. } => {}
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    let mut symlinks = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = symlinks.remove(*ino);
                    drop(symlinks);

                    let mut xattrs = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = xattr_prefix(*ino);
                    let keys_to_remove: Vec<Vec<u8>> = {
                        let range = xattrs
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in keys_to_remove {
                        let _ = xattrs.remove(key.as_slice());
                    }

                    Self::clear_regular_file_graph_in_txn(&txn, *ino)?;
                }
                crate::events::MetaEvent::AddEntry { .. } => {}
                crate::events::MetaEvent::RemoveEntry { .. } => {}
                crate::events::MetaEvent::RenameEntry { .. } => {}
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            &txn,
                            REGULAR_FILE_BASE_TABLE,
                            false,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::RemoveRegularFileGraph { ino } => {
                    Self::clear_regular_file_graph_in_txn(&txn, *ino)?;
                }
                crate::events::MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
                    Self::replace_regular_file_chunk_rows_from_chunk_slices_in_txn(
                        &txn,
                        REGULAR_FILE_BASE_TABLE,
                        *ino,
                        chunk_slices.as_slice(),
                        false,
                    )?;
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    let mut table = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*ino, target.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_value_in_table_in_txn(
                        &txn,
                        BASE_XATTRS_TABLE,
                        *ino,
                        name,
                        value,
                    )?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                } => {
                    let mut table = txn
                        .open_table(PENDING_DELETIONS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*object_id, *timestamp)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemovePendingDeletion { object_id } => {
                    let mut table = txn
                        .open_table(PENDING_DELETIONS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table.remove(*object_id);
                }
                crate::events::MetaEvent::CreateSnapshot { snapshot } => {
                    let mut table = txn
                        .open_table(SNAPSHOTS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let bytes = encode(snapshot)?;
                    table
                        .insert(snapshot.id.as_str(), bytes.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::UpdateSnapshotState { id, state } => {
                    let mut table = txn
                        .open_table(SNAPSHOTS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let existing_bytes = table
                        .get(id.as_str())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|existing| existing.value().to_vec());
                    if let Some(existing_bytes) = existing_bytes {
                        let mut snapshot: MetadataSnapshot = decode(existing_bytes.as_slice())?;
                        snapshot.state = *state;
                        let bytes = encode(&snapshot)?;
                        table
                            .insert(id.as_str(), bytes.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Apply only namespace/attr portions of replica events into the remote-visible base view.
    pub fn apply_replica_events_to_base(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    let data = encode(attr)?;
                    let mut table = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(attr.ino, data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    let mut table = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table.remove(*ino);

                    let mut symlinks = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = symlinks.remove(*ino);
                    drop(symlinks);

                    let mut xattrs = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = xattr_prefix(*ino);
                    let keys_to_remove: Vec<Vec<u8>> = {
                        let range = xattrs
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in keys_to_remove {
                        let _ = xattrs.remove(key.as_slice());
                    }

                    let mut acl = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = acl_rule_prefix(*ino);
                    let keys_to_remove: Vec<Vec<u8>> = {
                        let range = acl
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in keys_to_remove {
                        let _ = acl.remove(key.as_slice());
                    }
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    Self::set_base_dentry_in_txn(&txn, *parent, name, entry)?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    Self::remove_base_dentry_in_txn(&txn, *parent, name)?;
                }
                crate::events::MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry,
                    displaced_set_attr,
                    displaced_remove_attr_ino,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                } => {
                    let src_key = encode_dentry_key(*parent, name);

                    let src_matches = {
                        let dentries = txn
                            .open_table(BASE_DENTRIES_TABLE)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        dentries
                            .get(src_key.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?
                            .map(|value| decode::<DirEntryValue>(value.value()))
                            .transpose()?
                            .map(|current| current.ino == entry.ino && current.kind == entry.kind)
                            .unwrap_or(false)
                    };
                    let current_key = if src_matches {
                        Some(src_key.clone())
                    } else {
                        Self::unique_dentry_key_for_entry_in_write_table(
                            &txn,
                            BASE_DENTRIES_TABLE,
                            BASE_DENTRY_BY_INODE_TABLE,
                            entry,
                            RenameDentryContext {
                                source_parent: *parent,
                                source_name: name,
                                destination_parent: *new_parent,
                                destination_name: new_name,
                            },
                        )?
                    };
                    let Some(current_key) = current_key else {
                        continue;
                    };

                    let (current_parent, current_name) = decode_dentry_key(&current_key)?;
                    Self::remove_base_dentry_in_txn(&txn, current_parent, current_name)?;
                    Self::remove_base_dentry_in_txn(&txn, *new_parent, new_name)?;
                    Self::set_base_dentry_in_txn(&txn, *new_parent, new_name, entry)?;

                    let mut inodes = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    if let Some(attr) = displaced_set_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        let _ = inodes.remove(*ino);
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    let mut table = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*ino, target.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_value_in_table_in_txn(
                        &txn,
                        BASE_XATTRS_TABLE,
                        *ino,
                        name,
                        value,
                    )?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            &txn,
                            REGULAR_FILE_BASE_TABLE,
                            false,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
                    Self::replace_regular_file_chunk_rows_from_chunk_slices_in_txn(
                        &txn,
                        REGULAR_FILE_BASE_TABLE,
                        *ino,
                        chunk_slices.as_slice(),
                        false,
                    )?;
                }
                crate::events::MetaEvent::RemoveRegularFileGraph { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. }
                | crate::events::MetaEvent::CreateSnapshot { .. }
                | crate::events::MetaEvent::UpdateSnapshotState { .. } => {}
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Sync the foreground serving tables to a remote-visible state for objects
    /// that do not currently have local overlay conflicts.
    pub fn apply_replica_serving_state(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    Self::set_inode_mirror_in_txn(&txn, attr)?;
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    Self::remove_inode_mirror_in_txn(&txn, *ino)?;
                    Self::remove_symlink_mirror_in_txn(&txn, *ino)?;

                    let current_keys =
                        Self::keys_for_inode_in_write_table(&txn, XATTRS_TABLE, *ino)?;
                    for key in current_keys {
                        let name = xattr_name_from_key(&key).to_vec();
                        Self::remove_xattr_mirror_in_txn(&txn, *ino, &name)?;
                    }
                    let base_keys =
                        Self::keys_for_inode_in_write_table(&txn, BASE_XATTRS_TABLE, *ino)?;
                    for key in base_keys {
                        let name = xattr_name_from_key(&key).to_vec();
                        Self::remove_xattr_mirror_in_txn(&txn, *ino, &name)?;
                    }

                    let prefix = acl_rule_prefix(*ino);
                    let mut acl_rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let acl_rule_keys: Vec<Vec<u8>> = {
                        let range = acl_rules
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in acl_rule_keys {
                        let _ = acl_rules.remove(key.as_slice());
                    }
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    Self::set_dentry_mirror_in_txn(&txn, *parent, name, entry)?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    Self::remove_dentry_mirror_in_txn(&txn, *parent, name)?;
                }
                crate::events::MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry,
                    displaced_set_attr,
                    displaced_remove_attr_ino,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                } => {
                    let src_key = encode_dentry_key(*parent, name);
                    let src_matches = txn
                        .open_table(DENTRIES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .get(src_key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|value| decode::<DirEntryValue>(value.value()))
                        .transpose()?
                        .map(|current| current.ino == entry.ino && current.kind == entry.kind)
                        .unwrap_or(false);
                    let current_key = if src_matches {
                        Some(src_key)
                    } else {
                        Self::unique_dentry_key_for_entry_in_write_table(
                            &txn,
                            DENTRIES_TABLE,
                            DENTRY_BY_INODE_TABLE,
                            entry,
                            RenameDentryContext {
                                source_parent: *parent,
                                source_name: name,
                                destination_parent: *new_parent,
                                destination_name: new_name,
                            },
                        )?
                    };
                    let Some(current_key) = current_key else {
                        continue;
                    };
                    let (current_parent, current_name) = decode_dentry_key(&current_key)?;

                    Self::remove_dentry_mirror_in_txn(&txn, current_parent, current_name)?;
                    Self::remove_dentry_mirror_in_txn(&txn, *new_parent, new_name)?;
                    Self::set_dentry_mirror_in_txn(&txn, *new_parent, new_name, entry)?;

                    if let Some(attr) = displaced_set_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        Self::remove_inode_mirror_in_txn(&txn, *ino)?;
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    Self::set_symlink_mirror_in_txn(&txn, *ino, target)?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_mirror_in_txn(&txn, *ino, name, value)?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    Self::remove_xattr_mirror_in_txn(&txn, *ino, name)?;
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let mut tombstones = txn
                        .open_table(ACL_RULE_TOMBSTONES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = tombstones.remove(key.as_slice());
                    rules
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let mut tombstones = txn
                        .open_table(ACL_RULE_TOMBSTONES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = rules.remove(key.as_slice());
                    tombstones
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            &txn,
                            REGULAR_FILE_OVERLAY_TABLE,
                            true,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::PublishRegularFileBaseSlices { .. } => {}
                crate::events::MetaEvent::RemoveRegularFileGraph { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. }
                | crate::events::MetaEvent::CreateSnapshot { .. }
                | crate::events::MetaEvent::UpdateSnapshotState { .. } => {}
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Reconcile a rename in the serving overlay using the authoritative base namespace.
    pub fn reconcile_serving_rename_from_base(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let src_in_base = Self::lookup_in_write_table(&txn, BASE_DENTRIES_TABLE, parent, name)?
            .map(|value| value.ino == entry.ino && value.kind == entry.kind)
            .unwrap_or(false);
        let dst_in_base =
            Self::lookup_in_write_table(&txn, BASE_DENTRIES_TABLE, new_parent, new_name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
        let current_key = Self::unique_dentry_key_for_entry_in_write_table(
            &txn,
            DENTRIES_TABLE,
            DENTRY_BY_INODE_TABLE,
            entry,
            RenameDentryContext {
                source_parent: parent,
                source_name: name,
                destination_parent: new_parent,
                destination_name: new_name,
            },
        )?;

        if let Some(current_key) = current_key.as_ref() {
            let (current_parent, current_name) = decode_dentry_key(current_key)?;
            Self::remove_dentry_mirror_in_txn(&txn, current_parent, current_name)?;
        }

        if dst_in_base {
            Self::remove_dentry_mirror_in_txn(&txn, parent, name)?;
            Self::remove_dentry_mirror_in_txn(&txn, new_parent, new_name)?;
            Self::set_dentry_mirror_in_txn(&txn, new_parent, new_name, entry)?;
        } else if src_in_base {
            Self::remove_dentry_mirror_in_txn(&txn, new_parent, new_name)?;
            Self::set_dentry_mirror_in_txn(&txn, parent, name, entry)?;
        } else {
            Self::remove_dentry_mirror_in_txn(&txn, parent, name)?;
            Self::remove_dentry_mirror_in_txn(&txn, new_parent, new_name)?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl MetaReplication for RedbMetaEngine {
    fn check_consistency(&self) -> MetaResult<()> {
        Ok(())
    }

    fn flush_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        None
    }

    fn request_eager_flush(&self) {}

    fn metadata_version_token(&self) -> Option<String> {
        None
    }

    fn namespace_version_token(&self) -> Option<String> {
        self.metadata_version_token()
    }

    fn replication_backlog(&self) -> MetadataReplicationBacklog {
        MetadataReplicationBacklog::default()
    }

    fn metadata_authority_status(&self) -> MetadataAuthorityStatus {
        MetadataAuthorityStatus::writable()
    }

    fn remote_metadata_size(&self) -> MetaResult<Option<u64>> {
        Ok(None)
    }
}
