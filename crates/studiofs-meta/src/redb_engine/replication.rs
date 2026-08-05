use super::*;
#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static REGULAR_FILE_SERVING_RECONCILE_CALLS: Cell<usize> = const { Cell::new(0) };
}

impl RedbMetaEngine {
    #[cfg(test)]
    pub(crate) fn reset_regular_file_serving_reconcile_call_count_for_test() {
        REGULAR_FILE_SERVING_RECONCILE_CALLS.set(0);
    }

    #[cfg(test)]
    pub(crate) fn regular_file_serving_reconcile_call_count_for_test() -> usize {
        REGULAR_FILE_SERVING_RECONCILE_CALLS.get()
    }

    fn remove_dentry_mirrors_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let keys = Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)?;
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
        }
        Ok(())
    }

    fn remove_base_dentries_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let keys =
            Self::dentry_keys_for_inode_in_write_index(txn, BASE_DENTRIES_BY_INODE_TABLE, ino)?;
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            Self::remove_base_dentry_in_txn(txn, parent, name)?;
        }
        Ok(())
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
                let lifecycle = self.get_inode_lifecycle(ino)?;
                let visible_in_remote_namespace = published_regular_inodes.contains(&ino)
                    || (lifecycle
                        .as_ref()
                        .is_none_or(|record| record.has_published_base)
                        && Self::regular_file_remote_visible_without_published_base(&attr));
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
        snapshot.counters.retain(|(name, _)| {
            name.as_str() != "nextEventSeq"
                && !crate::pg_engine::is_local_pg_snapshot_state_counter(name)
        });
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
        let txn = self.begin_read_txn()?;
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
        let txn = self.begin_read_txn()?;
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
        let txn = self.begin_read_txn()?;
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

        let txn = self.begin_read_txn()?;
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
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ev = crate::events::decode_event(bytes.value())?;
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

        let txn = self.begin_write_txn()?;

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
                    Self::merge_regular_file_base_publish_in_txn(
                        &txn,
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
        self.apply_replica_events_to_base_inner(events, &HashSet::new())
    }

    /// Apply replica events while preserving protected local base ranges for stale publish batches.
    pub fn apply_replica_events_to_base_preserving_protected_base_ranges(
        &self,
        events: &[crate::events::MetaEvent],
        protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<()> {
        self.apply_replica_events_to_base_inner(events, protected_regular_file_inodes)
    }

    fn apply_replica_events_to_base_inner(
        &self,
        events: &[crate::events::MetaEvent],
        protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self.begin_write_txn()?;

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
                    Self::remove_base_dentries_for_inode_in_txn(&txn, *ino)?;

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
                    let src_matches =
                        Self::lookup_in_write_table(&txn, BASE_DENTRIES_TABLE, *parent, name)?
                            .map(|current| current.ino == entry.ino && current.kind == entry.kind)
                            .unwrap_or(false);
                    if src_matches {
                        Self::remove_base_dentry_in_txn(&txn, *parent, name)?;
                    } else {
                        let prior_keys = Self::base_dentry_keys_for_rename_entry(&txn, entry)?;
                        if prior_keys.len() > 1 {
                            tracing::warn!(
                                ino = entry.ino,
                                source_parent = *parent,
                                source_name = %String::from_utf8_lossy(name),
                                destination_parent = *new_parent,
                                destination_name = %String::from_utf8_lossy(new_name),
                                prior_key_count = prior_keys.len(),
                                "replica rename found multiple base dentries for inode; preserving existing links and applying destination"
                            );
                        } else if let Some(prior_key) = prior_keys.first() {
                            let (prior_parent, prior_name) = decode_dentry_key(prior_key)?;
                            Self::remove_base_dentry_in_txn(&txn, prior_parent, prior_name)?;
                        }
                    }
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
                    Self::merge_regular_file_base_publish_in_txn(
                        &txn,
                        *ino,
                        chunk_slices.as_slice(),
                        protected_regular_file_inodes.contains(ino),
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

        let txn = self.begin_write_txn()?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    if let Some(base_attr) =
                        Self::stale_ready_regular_file_base_attr_in_txn(&txn, attr)?
                    {
                        let current = Self::get_attr_merged_in_write_txn(&txn, attr.ino)?;
                        let current_is_newer = current.content_epoch > attr.content_epoch
                            || (current.content_epoch == attr.content_epoch
                                && current.size > attr.size);
                        if !current_is_newer {
                            Self::set_inode_mirror_in_txn(&txn, &base_attr)?;
                            Self::clear_regular_file_serving_overlay_in_txn(&txn, attr.ino)?;
                        }
                        continue;
                    }
                    Self::set_inode_mirror_in_txn(&txn, attr)?;
                    if Self::ready_regular_file_attr_can_follow_base_in_txn(&txn, attr)? {
                        Self::clear_regular_file_serving_overlay_in_txn(&txn, attr.ino)?;
                    }
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    Self::remove_dentry_mirrors_for_inode_in_txn(&txn, *ino)?;
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
                    Self::clear_regular_file_graph_in_txn(&txn, *ino)?;
                    Self::clear_regular_file_serving_overlay_in_txn(&txn, *ino)?;
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    Self::set_dentry_mirror_in_txn(&txn, *parent, name, entry)?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    Self::remove_dentry_overlay_in_txn(&txn, *parent, name)?;
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
                    Self::reconcile_serving_rename_from_base_in_txn(
                        &txn,
                        *parent,
                        name,
                        *new_parent,
                        new_name,
                        entry,
                    )?;

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
                crate::events::MetaEvent::PublishRegularFileBaseSlices { ino, .. } => {
                    if Self::serving_overlay_can_follow_base_publish_in_txn(&txn, *ino)? {
                        Self::clear_regular_file_serving_overlay_in_txn(&txn, *ino)?;
                    }
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

    /// Reconcile stale regular-file serving mirrors to the imported base view
    /// after the PG layer has proven that no local event-log overlay still owns
    /// those inodes.
    pub fn reconcile_regular_file_serving_from_base(
        &self,
        inodes: &HashSet<Ino>,
    ) -> MetaResult<bool> {
        self.reconcile_regular_file_serving_from_base_with_protected_dentries(
            inodes,
            &HashSet::new(),
        )
    }

    /// Reconcile stale regular-file serving mirrors while preserving dentry keys
    /// that the PG layer still considers local overlay state.
    pub fn reconcile_regular_file_serving_from_base_with_protected_dentries(
        &self,
        inodes: &HashSet<Ino>,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<bool> {
        if inodes.is_empty() {
            return Ok(false);
        }
        #[cfg(test)]
        REGULAR_FILE_SERVING_RECONCILE_CALLS
            .set(REGULAR_FILE_SERVING_RECONCILE_CALLS.get().saturating_add(1));

        let txn = self.begin_write_txn()?;
        let mut changed = false;

        for ino in inodes {
            let base_attr = {
                let base = txn
                    .open_table(BASE_INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                base.get(*ino)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|value| decode::<InodeAttr>(value.value()))
                    .transpose()?
            };

            match base_attr {
                Some(attr) if attr.kind == FileType::RegularFile => {
                    Self::set_inode_mirror_in_txn(&txn, &attr)?;
                    Self::remove_stale_dentries_for_base_entry(
                        &txn,
                        &DirEntryValue {
                            ino: *ino,
                            kind: attr.kind,
                        },
                        protected_dentry_keys,
                    )?;
                    Self::install_base_dentries_for_entry_in_txn(
                        &txn,
                        &DirEntryValue {
                            ino: *ino,
                            kind: attr.kind,
                        },
                        protected_dentry_keys,
                    )?;
                    if attr.upload_state == UploadState::Ready
                        && attr.content_epoch == attr.committed_content_epoch
                        && attr.size == attr.committed_size
                    {
                        Self::clear_regular_file_serving_overlay_in_txn(&txn, *ino)?;
                    }
                    changed = true;
                }
                Some(_) => {}
                None => {
                    Self::remove_dentry_mirrors_for_inode_in_txn(&txn, *ino)?;
                    Self::remove_inode_mirror_in_txn(&txn, *ino)?;
                    Self::clear_regular_file_serving_overlay_in_txn(&txn, *ino)?;
                    changed = true;
                }
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(changed)
    }

    /// Reconcile exact dentry keys from the authoritative base namespace into
    /// serving after a local namespace overlay for those keys has been flushed.
    pub fn reconcile_serving_dentries_from_base(
        &self,
        dentry_keys: &HashSet<Vec<u8>>,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<bool> {
        if dentry_keys.is_empty() {
            return Ok(false);
        }

        let txn = self.begin_write_txn()?;
        let mut changed = false;

        for key in dentry_keys {
            if protected_dentry_keys.contains(key) {
                continue;
            }
            let (parent, name) = decode_dentry_key(key)?;
            let base_entry = {
                let base = txn
                    .open_table(BASE_DENTRIES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                Self::lookup_in_opened_dentry_table(&base, parent, name)?
            };
            if let Some(entry) = base_entry {
                Self::set_dentry_mirror_in_txn(&txn, parent, name, &entry)?;
            } else {
                Self::remove_dentry_mirror_in_txn(&txn, parent, name)?;
            }
            changed = true;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(changed)
    }

    fn serving_overlay_can_follow_base_publish_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        let attr = match Self::get_attr_merged_in_write_txn(txn, ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(true),
            Err(err) => return Err(err),
        };
        Ok(attr.kind != FileType::RegularFile
            || (attr.content_epoch == attr.committed_content_epoch
                && attr.size == attr.committed_size))
    }

    fn ready_regular_file_attr_can_follow_base_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<bool> {
        if attr.kind != FileType::RegularFile
            || attr.upload_state != UploadState::Ready
            || attr.content_epoch != attr.committed_content_epoch
            || attr.size != attr.committed_size
        {
            return Ok(false);
        }

        if attr.committed_size == 0 {
            return Ok(true);
        }

        let base = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(attr.ino);
        let mut rows = base
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match rows.next() {
            Some(row) => {
                let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                Ok(key.value().starts_with(prefix.as_ref()))
            }
            None => Ok(false),
        }
    }

    fn stale_ready_regular_file_base_attr_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<Option<InodeAttr>> {
        if attr.kind != FileType::RegularFile || attr.upload_state != UploadState::Ready {
            return Ok(None);
        }

        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let Some(base_attr) = Self::get_attr_from_opened_table(&base, attr.ino)? else {
            return Ok(None);
        };

        Ok((base_attr.kind == FileType::RegularFile
            && (base_attr.committed_content_epoch > attr.committed_content_epoch
                || base_attr.committed_size > attr.committed_size))
            .then_some(base_attr))
    }

    fn clear_regular_file_serving_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)?;
        Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
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
        let txn = self.begin_write_txn()?;

        Self::reconcile_serving_rename_from_base_in_txn(
            &txn, parent, name, new_parent, new_name, entry,
        )?;

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn reconcile_serving_rename_from_base_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let src_in_base = Self::lookup_in_write_table(txn, BASE_DENTRIES_TABLE, parent, name)?
            .map(|value| value.ino == entry.ino && value.kind == entry.kind)
            .unwrap_or(false);
        let dst_in_base =
            Self::lookup_in_write_table(txn, BASE_DENTRIES_TABLE, new_parent, new_name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
        Self::remove_stale_dentries_for_base_entry(txn, entry, &HashSet::new())?;

        if dst_in_base {
            let local_removed_dentries = Self::local_removed_dentries_in_txn(txn)?;
            // The base rename winner moved this logical source to the
            // destination.  Remove any serving entry still occupying the old
            // source name, even if it points at a local overlay inode; payload
            // and inode protection are handled separately from namespace
            // visibility.
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
            if local_removed_dentries.contains(&encode_dentry_key(new_parent, new_name)) {
                Self::remove_dentry_overlay_in_txn(txn, new_parent, new_name)?;
            } else {
                Self::set_dentry_mirror_in_txn(txn, new_parent, new_name, entry)?;
            }
        } else if src_in_base {
            Self::remove_dentry_mirror_if_matches_in_txn(txn, new_parent, new_name, entry)?;
            Self::set_dentry_mirror_in_txn(txn, parent, name, entry)?;
        } else {
            Self::remove_dentry_mirror_if_matches_in_txn(txn, parent, name, entry)?;
            Self::remove_dentry_mirror_if_matches_in_txn(txn, new_parent, new_name, entry)?;
        }

        Ok(())
    }

    fn local_removed_dentries_in_txn(txn: &redb::WriteTransaction) -> MetaResult<HashSet<Vec<u8>>> {
        let counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let end_seq = counters
            .get("nextEventSeq")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(1);
        drop(counters);

        let mut locally_created = HashSet::new();
        let mut locally_renamed = HashSet::new();
        let mut removed = HashSet::new();
        let events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let start_seq = events
            .first()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|(seq, _)| seq.value())
            .unwrap_or(end_seq);
        if end_seq <= start_seq {
            return Ok(removed);
        }
        for row in events
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            match crate::events::decode_event(bytes.value())? {
                crate::events::MetaEvent::AddEntry { parent, name, .. } => {
                    let key = encode_dentry_key(parent, &name);
                    locally_renamed.remove(&key);
                    removed.remove(&key);
                    locally_created.insert(key);
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    let key = encode_dentry_key(parent, &name);
                    if locally_created.remove(&key) {
                        removed.remove(&key);
                    } else {
                        locally_renamed.remove(&key);
                        removed.insert(key);
                    }
                }
                crate::events::MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    ..
                } => {
                    let source_key = encode_dentry_key(parent, &name);
                    let destination_key = encode_dentry_key(new_parent, &new_name);
                    let was_created = locally_created.remove(&source_key);
                    locally_renamed.remove(&source_key);
                    locally_created.remove(&destination_key);
                    locally_renamed.remove(&destination_key);
                    removed.remove(&destination_key);
                    if was_created {
                        locally_created.insert(destination_key);
                    } else {
                        locally_renamed.insert(destination_key);
                    }
                }
                _ => {}
            }
        }
        Ok(removed)
    }

    fn remove_dentry_mirror_if_matches_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let matches = Self::lookup_in_write_table(txn, DENTRIES_TABLE, parent, name)?
            .map(|current| current.ino == entry.ino && current.kind == entry.kind)
            .unwrap_or(false);
        if matches {
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
        }
        Ok(())
    }

    fn remove_stale_dentries_for_base_entry(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<()> {
        let keys =
            Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, entry.ino)?;
        let protected = Self::pending_local_alias_dentry_keys_for_inode_in_txn(txn, entry)?;
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut stale = Vec::new();
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            let current_matches = Self::lookup_in_opened_dentry_table(&current, parent, name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
            if !current_matches {
                continue;
            }

            let base_matches = Self::lookup_in_opened_dentry_table(&base, parent, name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
            if !base_matches && !protected.contains(&key) && !protected_dentry_keys.contains(&key) {
                stale.push((parent, name.to_vec()));
            }
        }
        drop(base);
        drop(current);

        for (parent, name) in stale {
            Self::remove_dentry_mirror_in_txn(txn, parent, &name)?;
        }
        Ok(())
    }

    fn install_base_dentries_for_entry_in_txn(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<()> {
        let keys = Self::base_dentry_keys_for_rename_entry(txn, entry)?;
        for key in keys {
            if protected_dentry_keys.contains(&key) {
                continue;
            }
            let (parent, name) = decode_dentry_key(&key)?;
            Self::set_dentry_mirror_in_txn(txn, parent, name, entry)?;
        }
        Ok(())
    }

    // Pending local AddEntry aliases are local namespace state, not stale
    // remote rename losers, so foreign rename reconciliation must preserve them.
    fn pending_local_alias_dentry_keys_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
    ) -> MetaResult<HashSet<Vec<u8>>> {
        let counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let start_seq = counters
            .get("pgLastFlushedLocalEventSeq")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(1);
        let end_seq = counters
            .get("nextEventSeq")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(1);
        drop(counters);

        let mut aliases = HashSet::new();
        if end_seq <= start_seq {
            return Ok(aliases);
        }

        let events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in events
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            match crate::events::decode_event(bytes.value())? {
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry: added,
                } if added.ino == entry.ino && added.kind == entry.kind => {
                    aliases.insert(encode_dentry_key(parent, &name));
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    aliases.remove(&encode_dentry_key(parent, &name));
                }
                crate::events::MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry: renamed,
                    ..
                } if renamed.ino == entry.ino && renamed.kind == entry.kind => {
                    let old_key = encode_dentry_key(parent, &name);
                    if aliases.remove(&old_key) {
                        aliases.insert(encode_dentry_key(new_parent, &new_name));
                    }
                }
                _ => {}
            }
        }
        Ok(aliases)
    }

    fn base_dentry_keys_for_rename_entry(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let keys = Self::dentry_keys_for_inode_in_write_index(
            txn,
            BASE_DENTRIES_BY_INODE_TABLE,
            entry.ino,
        )?;
        let mut matching = Vec::new();
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            let base_matches = Self::lookup_in_opened_dentry_table(&base, parent, name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
            if base_matches {
                matching.push(key);
            }
        }
        Ok(matching)
    }

    /// Return the lowest sequence number still present in the local event log.
    pub(crate) fn first_event_seq(&self) -> MetaResult<Option<u64>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let first = table
            .first()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(first.map(|(seq, _)| seq.value()))
    }
}

impl MetaReplication for RedbMetaEngine {
    fn prepare_for_close(&self) {
        self.seal_relaxed_durability_for_close();
    }

    fn check_consistency(&self) -> MetaResult<()> {
        Ok(())
    }

    fn flush_metadata(&self) -> MetaResult<()> {
        self.flush_local_metadata()
    }

    fn flush_local_metadata(&self) -> MetaResult<()> {
        RedbMetaEngine::flush_local_metadata(self)
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn sync_local_metadata(&self) -> MetaResult<()> {
        RedbMetaEngine::sync_local_metadata(self)
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
