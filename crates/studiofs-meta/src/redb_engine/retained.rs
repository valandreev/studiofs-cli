use super::*;
use crate::trash::{RetainedEntry, RetainedEntryState, RetainedPurgeReason, VolumeSettings};

// Only the private receipt directory carries this record; native drafts never do.
use crate::trash::RETAINED_RECEIPT_XATTR as RECEIPT_XATTR;
const DATA_NAME: &[u8] = b"data";

/// Outcome of one [`RedbMetaEngine::settle_retained_purge`] attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetainedSettlement {
    /// The data tree was unlinked; ordinary publication retires its objects.
    Settled,
    /// Base holds no shared `PurgeRequested` record for this receipt yet.
    NotAcknowledged,
    /// No `data` link to the purged inode remains (settled or restored).
    NothingToSettle,
    /// An open handle or a concurrent change defers settlement.
    Busy,
}

struct RetainedSubtreeEntry {
    parent: Ino,
    name: Vec<u8>,
    ino: Ino,
    is_dir: bool,
}

impl crate::trash::MetaRetainedStore for RedbMetaEngine {
    fn protect_retained_file_payload(
        &self,
        receipt: Ino,
        source: crate::trash::RetainedSourceFence,
        stage: &[u8],
    ) -> MetaResult<InodeAttr> {
        Self::protect_retained_file_payload(self, receipt, source, stage)
    }
    fn retained_entry(&self, receipt: Ino) -> MetaResult<Option<RetainedEntry>> {
        Self::retained_entry(self, receipt)
    }
    fn retain_namespace_entry(
        &self,
        parent: Ino,
        name: &[u8],
        store: Ino,
        receipt: Ino,
    ) -> MetaResult<RetainedEntry> {
        Self::retain_namespace_entry(self, parent, name, store, receipt)
    }
    fn restore_retained_entry(
        &self,
        receipt: Ino,
        parent: Ino,
        name: &[u8],
        stage: &[u8],
        sources: &[crate::trash::RetainedSourceFence],
    ) -> MetaResult<crate::trash::RetainedRestoreResult> {
        Self::restore_retained_entry(self, receipt, parent, name, stage, sources)
    }
    fn request_retained_purge(
        &self,
        receipt: Ino,
        reason: RetainedPurgeReason,
    ) -> MetaResult<RetainedEntry> {
        Self::request_retained_purge(self, receipt, reason)
    }
    fn purge_retained_child(
        &self,
        receipt: Ino,
        parent_source_ino: Ino,
        name: &[u8],
        expected_ino: Ino,
        is_dir: bool,
    ) -> MetaResult<crate::trash::RetainedChildPurgeOutcome> {
        Self::purge_retained_child(self, receipt, parent_source_ino, name, expected_ino, is_dir)
    }
}

/// Child inodes of unacknowledged `purge_retained_child` spans, scanned
/// incrementally across the write transactions of one finalize pass.
#[derive(Debug, Default)]
pub(crate) struct UnackedChildPurges {
    /// Exclusive journal end already scanned.
    scanned_end: u64,
    inos: HashSet<Ino>,
    /// Journal events decoded so far (work-bound assertions).
    #[cfg(test)]
    decoded: usize,
}

impl RedbMetaEngine {
    /// Atomically install a full local payload fork on the retained inode.
    /// Runtime session exclusion and stage-body durability belong to the caller.
    pub fn protect_retained_file_payload(
        &self,
        receipt: Ino,
        source: crate::trash::RetainedSourceFence,
        stage: &[u8],
    ) -> MetaResult<InodeAttr> {
        let txn = self.begin_write_txn()?;
        let entry = Self::retained_entry_in_txn(&txn, receipt)?;
        if entry.state != RetainedEntryState::Retained || stage == DATA_NAME {
            return Err(MetaError::TargetChanged);
        }
        let root = Self::lookup_merged_in_write_txn(&txn, receipt, DATA_NAME)?
            .ok_or(MetaError::InodeNotFound(source.ino))?;
        let tree = Self::collect_subtree_entries_in_txn(&txn, receipt, DATA_NAME, root)?;
        if !tree.iter().any(|node| node.ino == source.ino) {
            return Err(MetaError::TargetChanged);
        }
        let staged = Self::lookup_merged_in_write_txn(&txn, receipt, stage)?
            .ok_or(MetaError::TargetChanged)?;
        let protected = self.protect_retained_file_payload_in_txn(&txn, source, staged.ino)?;
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(protected)
    }

    fn protect_retained_file_payload_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        source: crate::trash::RetainedSourceFence,
        staged_ino: Ino,
    ) -> MetaResult<InodeAttr> {
        let mut attr = Self::get_attr_merged_in_write_txn(txn, source.ino)?;
        if attr.kind != FileType::RegularFile
            || attr.ctime != source.ctime
            || attr.size != source.size
            || attr.content_epoch != source.content_epoch
        {
            return Err(MetaError::TargetChanged);
        }
        let staged_attr = Self::get_attr_merged_in_write_txn(txn, staged_ino)?;
        if staged_ino == source.ino
            || staged_attr.kind != FileType::RegularFile
            // Older restore stages recorded an EOF. New stages retain zero EOF;
            // the gap-free slice coverage below must still match the fenced source.
            || (staged_attr.size != 0 && staged_attr.size != attr.size)
            || Self::has_regular_file_chunk_rows_in_write_txn(
                txn,
                REGULAR_FILE_BASE_TABLE,
                staged_ino,
            )?
        {
            return Err(MetaError::TargetChanged);
        }
        let chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            staged_ino,
        )?;
        let mut old_objects = HashSet::new();
        for table in [REGULAR_FILE_BASE_TABLE, REGULAR_FILE_OVERLAY_TABLE] {
            for chunk in
                Self::regular_file_chunk_states_for_inode_in_write_txn(txn, table, source.ino)?
                    .values()
            {
                old_objects.extend(
                    chunk
                        .slices
                        .values()
                        .filter_map(|slice| slice.payload.map(|payload| payload.object_id)),
                );
            }
        }
        let mut covered = 0_u64;
        for (&chunk_id, chunk) in &chunks {
            let start =
                crate::schema::chunk_start_offset(chunk_id, crate::schema::DEFAULT_CHUNK_SIZE);
            let mut ranges: Vec<_> = chunk.slices.values().map(|slice| slice.range).collect();
            ranges.sort_by_key(|range| range.start);
            for slice in chunk.slices.values() {
                if !matches!(
                    slice.payload_state,
                    PayloadRefState::LocalOnly | PayloadRefState::Uploading
                ) || slice
                    .payload
                    .is_some_and(|payload| old_objects.contains(&payload.object_id))
                {
                    return Err(MetaError::TargetChanged);
                }
            }
            for range in ranges {
                let range_start = start + u64::from(range.start);
                let range_end = start + u64::from(range.end);
                if range_start > covered || range_end > attr.size {
                    return Err(MetaError::TargetChanged);
                }
                covered = covered.max(range_end);
            }
        }
        if covered != attr.size {
            return Err(MetaError::TargetChanged);
        }
        attr.content_epoch = attr
            .content_epoch
            .checked_add(1)
            .ok_or_else(|| MetaError::Storage("retained payload epoch overflow".into()))?;
        // Peer purge may retire the old generation. A restore publishes only
        // after these fresh local payloads own a complete uploaded generation.
        attr.committed_size = 0;
        attr.committed_content_epoch = 0;
        let slices: Vec<_> = chunks
            .into_iter()
            .map(|(id, chunk)| (id, chunk.slices.into_values().collect()))
            .collect();
        let (protected, applied) =
            self.apply_regular_file_overlay_mutation_in_txn(txn, source.ino, &slices, Some(&attr))?;
        let protected = protected
            .filter(|_| applied)
            .ok_or(MetaError::TargetChanged)?;
        Ok(protected)
    }
    /// Atomically retain a namespace entry using a caller-reserved receipt inode.
    /// The application must authorize the original parent and keep the store private.
    /// No native sidecar supplies the parent, inode or deletion timestamp.
    pub fn retain_namespace_entry(
        &self,
        parent: Ino,
        name: &[u8],
        store: Ino,
        receipt: Ino,
    ) -> MetaResult<RetainedEntry> {
        let txn = self.begin_write_txn()?;
        let settings = Self::retained_settings_in_txn(&txn)?;
        if !settings.trash.enabled() {
            return Err(MetaError::Storage("trash retention is disabled".into()));
        }
        // Never let a reserved receipt overwrite an existing inode or its references.
        match Self::get_attr_merged_in_write_txn(&txn, receipt) {
            Err(MetaError::InodeNotFound(_)) => {}
            Ok(_) => {
                return Err(MetaError::Storage(
                    "trash receipt inode is already in use".into(),
                ));
            }
            Err(error) => return Err(error),
        }
        let source = Self::lookup_merged_in_write_txn(&txn, parent, name)?.ok_or_else(|| {
            MetaError::EntryNotFound {
                parent,
                name: String::from_utf8_lossy(name).into(),
            }
        })?;
        let attr = Self::get_attr_merged_in_write_txn(&txn, source.ino)?;
        super::fs_ops::NamespaceCreateTxn::new(&txn).execute(
            &super::fs_ops::NamespaceCreateSpec::new_dir(
                receipt,
                store,
                receipt.to_string().as_bytes(),
                attr.uid,
                attr.gid,
                0o700,
            ),
        )?;
        let entry = RetainedEntry {
            version: 1,
            revision: 0,
            receipt,
            inode: source.ino,
            original_parent: parent,
            original_name: name.to_vec(),
            deleted_at_unix_secs: super::fs_ops::current_unix_seconds().max(0) as u64,
            state: RetainedEntryState::Retained,
        };
        Self::write_retained_entry_in_txn(&txn, &entry)?;
        Self::rename_in_txn(&txn, parent, name, receipt, DATA_NAME, false, false)?;
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(entry)
    }

    /// Read a neutral receipt for an application-authorized native view.
    pub fn retained_entry(&self, receipt: Ino) -> MetaResult<Option<RetainedEntry>> {
        self.get_xattr(receipt, RECEIPT_XATTR)?
            .map(|bytes| Self::decode_retained_entry(receipt, &bytes))
            .transpose()
    }

    fn retained_settings_in_txn(txn: &RedbWriteTransaction<'_>) -> MetaResult<VolumeSettings> {
        Ok({
            let table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            let format = table.get("format")?.ok_or(MetaError::NotFormatted)?;
            crate::decode_local_format_info(format.value())?;
            table
                .get(super::volume_settings::VOLUME_SETTINGS_KEY)?
                .or(table.get(super::volume_settings::BASE_VOLUME_SETTINGS_KEY)?)
                .map(|value| VolumeSettings::decode_json(value.value()))
                .transpose()?
                .unwrap_or_default()
        })
    }

    /// The merged (overlay, then base, minus tombstone) stored receipt record.
    fn stored_receipt_record_in_txn(
        txn: &redb::WriteTransaction,
        receipt: Ino,
    ) -> MetaResult<Option<Vec<u8>>> {
        let key = encode_xattr_key(receipt, RECEIPT_XATTR);
        if txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .get(key.as_slice())?
            .is_some()
        {
            return Ok(None);
        }
        Ok(txn
            .open_table(XATTRS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .get(key.as_slice())?
            .map(|value| value.value().to_vec())
            .or(txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?
                .get(key.as_slice())?
                .map(|value| value.value().to_vec())))
    }

    /// Shared metadata keeps every receipt inode (PG `reject_receipt_removal`
    /// refuses its `RemoveAttr`), so a local rmdir/unlink of a receipt, or a
    /// rename displacing one, is refused here before it can reach the journal.
    pub(super) fn reject_receipt_removal_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        match Self::stored_receipt_record_in_txn(txn, ino)? {
            Some(_) => Self::reject_reserved_xattr(ino, RECEIPT_XATTR),
            None => Ok(()),
        }
    }

    fn retained_entry_in_txn(
        txn: &RedbWriteTransaction<'_>,
        receipt: Ino,
    ) -> MetaResult<RetainedEntry> {
        let bytes = Self::stored_receipt_record_in_txn(txn, receipt)?
            .ok_or(MetaError::InodeNotFound(receipt))?;
        let bytes = decode_xattr_value_from_storage_owned(RECEIPT_XATTR, bytes);
        Self::decode_retained_entry(receipt, &bytes)
    }

    /// A shared terminal receipt record is final, but a retired local loser
    /// keeps its overlay record. Remote snapshots and checkpoints are cut at
    /// the shared head, so base already holds the winner: export it instead.
    /// That includes a receipt key the overlay tombstoned (`xattrs` is the
    /// merged, key-sorted list, so such a key is absent from it).
    pub(super) fn project_shared_receipt_records(
        txn: &ReadTransaction,
        xattrs: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> MetaResult<()> {
        let base = txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let shared_terminal = |key: &[u8]| -> MetaResult<Option<Vec<u8>>> {
            let Some(stored) = base.get(key)? else {
                return Ok(None);
            };
            let shared = decode_xattr_value_from_storage(RECEIPT_XATTR, stored.value());
            let receipt = Ino::from_le_bytes(
                key.get(..8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| MetaError::Storage("corrupt receipt xattr key".into()))?,
            );
            Ok(Self::decode_retained_entry(receipt, &shared)
                .is_ok_and(|entry| entry.state != RetainedEntryState::Retained)
                .then_some(shared))
        };
        for (key, value) in xattrs.iter_mut() {
            if xattr_name_from_key(key) != RECEIPT_XATTR {
                continue;
            }
            if let Some(shared) = shared_terminal(key)? {
                *value = shared;
            }
        }
        let tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let mut restored = Vec::new();
        for row in tombstones.iter()? {
            let (key, _) = row?;
            let key = key.value();
            if xattr_name_from_key(key) != RECEIPT_XATTR
                || xattrs
                    .binary_search_by(|(merged, _)| merged.as_slice().cmp(key))
                    .is_ok()
            {
                continue;
            }
            if let Some(shared) = shared_terminal(key)? {
                restored.push((key.to_vec(), shared));
            }
        }
        if !restored.is_empty() {
            xattrs.extend(restored);
            xattrs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        }
        Ok(())
    }

    /// The receipt record replayed into base from the shared journal, ignoring
    /// local overlay and tombstones.
    pub(crate) fn base_retained_record_in_txn(
        txn: &RedbWriteTransaction<'_>,
        receipt: Ino,
    ) -> MetaResult<Option<Vec<u8>>> {
        let key = encode_xattr_key(receipt, RECEIPT_XATTR);
        Ok(txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .get(key.as_slice())?
            .map(|value| {
                decode_xattr_value_from_storage_owned(RECEIPT_XATTR, value.value().to_vec())
            }))
    }

    pub(crate) fn decode_retained_entry(receipt: Ino, bytes: &[u8]) -> MetaResult<RetainedEntry> {
        let entry: RetainedEntry = serde_json::from_slice(bytes)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        let expected_revision = u64::from(!matches!(entry.state, RetainedEntryState::Retained));
        let valid_component = |name: &[u8]| {
            !name.is_empty()
                && name != b"."
                && name != b".."
                && !name.contains(&0)
                && !name.contains(&b'/')
        };
        let valid_destination = match &entry.state {
            RetainedEntryState::Restored { parent, name } => {
                *parent >= ROOT_INO && valid_component(name)
            }
            _ => true,
        };
        if entry.version != 1
            || entry.revision != expected_revision
            || entry.receipt != receipt
            || receipt <= ROOT_INO
            || entry.inode <= ROOT_INO
            || entry.original_parent < ROOT_INO
            || !valid_component(&entry.original_name)
            || !valid_destination
        {
            return Err(MetaError::Storage("invalid retained entry receipt".into()));
        }
        Ok(entry)
    }

    fn write_retained_entry_in_txn(
        txn: &RedbWriteTransaction<'_>,
        entry: &RetainedEntry,
    ) -> MetaResult<()> {
        let value = serde_json::to_vec(entry)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        Self::decode_retained_entry(entry.receipt, &value)?;
        Self::set_xattr_in_txn(txn, entry.receipt, RECEIPT_XATTR, &value)?;
        Self::append_event_in_txn(
            txn,
            &crate::MetaEvent::SetXattr {
                ino: entry.receipt,
                name: RECEIPT_XATTR.to_vec(),
                value,
                flags: 0,
            },
        )
    }

    fn collect_subtree_entries_in_txn(
        txn: &RedbWriteTransaction<'_>,
        root_parent: Ino,
        root_name: &[u8],
        root: DirEntryValue,
    ) -> MetaResult<Vec<RetainedSubtreeEntry>> {
        // Pre-order collection so `.rev()` yields post-order (children before directory).
        let mut entries = Vec::new();
        let mut pending = vec![(root_parent, root_name.to_vec(), root.ino, root.kind)];
        while let Some((parent, name, ino, kind)) = pending.pop() {
            let is_dir = kind == FileType::Directory;
            entries.push(RetainedSubtreeEntry {
                parent,
                name,
                ino,
                is_dir,
            });
            if is_dir {
                let (children, _) = Self::merged_children_in_write_txn(txn, ino)?;
                pending.extend(
                    children
                        .into_iter()
                        .map(|child| (ino, child.name, child.ino, child.kind)),
                );
            }
        }
        Ok(entries)
    }

    fn delete_subtree_postorder_in_txn(
        txn: &RedbWriteTransaction<'_>,
        entries: &[RetainedSubtreeEntry],
    ) -> MetaResult<bool> {
        use super::fs_ops::{NamespaceDeleteResult, NamespaceDeleteSpec, NamespaceDeleteTxn};
        // ponytail: one transaction per receipt tree; split very large trees if it matters.
        for entry in entries.iter().rev() {
            let spec = if entry.is_dir {
                NamespaceDeleteSpec::new_rmdir(entry.parent, &entry.name)
            } else {
                NamespaceDeleteSpec::new_unlink(entry.parent, &entry.name)
            };
            let result =
                NamespaceDeleteTxn::new(txn).execute(&spec.with_expected_ino(Some(entry.ino)))?;
            if matches!(result, NamespaceDeleteResult::TargetChanged) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Atomically adopt staged payloads and move the original `receipt/data`
    /// identity to `(parent, name)`, including its pending reconstruction journal.
    /// Failed collisions retain `receipt/data`; terminal retries are idempotent.
    pub fn restore_retained_entry(
        &self,
        receipt: Ino,
        parent: Ino,
        name: &[u8],
        stage: &[u8],
        sources: &[crate::trash::RetainedSourceFence],
    ) -> MetaResult<crate::trash::RetainedRestoreResult> {
        let txn = self.begin_write_txn()?;
        let mut entry = Self::retained_entry_in_txn(&txn, receipt)?;
        if let RetainedEntryState::Restored {
            parent: previous_parent,
            name: previous_name,
        } = &entry.state
        {
            if *previous_parent == parent && previous_name == name {
                return Ok(crate::trash::RetainedRestoreResult {
                    entry,
                    upload_epochs: HashMap::new(),
                });
            }
            return Err(MetaError::Storage(
                "retained entry was already restored elsewhere".into(),
            ));
        }
        if matches!(entry.state, RetainedEntryState::PurgeRequested { .. }) {
            return Err(MetaError::Storage(
                "retained entry has a pending purge request".into(),
            ));
        }
        let source = Self::lookup_merged_in_write_txn(&txn, receipt, DATA_NAME)?
            .ok_or(MetaError::InodeNotFound(entry.inode))?;
        if source.ino != entry.inode {
            return Err(MetaError::Storage(
                "retained payload identity changed".into(),
            ));
        }
        if Self::lookup_merged_in_write_txn(&txn, parent, name)?.is_some() {
            return Err(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).into(),
            });
        }
        let valid_stage = !stage.is_empty()
            && stage != b"."
            && stage != b".."
            && !stage.contains(&0)
            && !stage.contains(&b'/')
            && stage != DATA_NAME;
        if !valid_stage {
            return Err(MetaError::Storage(
                "invalid retained restore stage name".into(),
            ));
        }
        let staged = Self::lookup_merged_in_write_txn(&txn, receipt, stage)?.ok_or_else(|| {
            MetaError::EntryNotFound {
                parent: receipt,
                name: String::from_utf8_lossy(stage).into(),
            }
        })?;
        if staged.ino == entry.inode || staged.kind != source.kind {
            return Err(MetaError::Storage(
                "retained restore stage must use a new inode".into(),
            ));
        }
        let doomed = Self::collect_subtree_entries_in_txn(&txn, receipt, DATA_NAME, source)?;
        // The application holds source session gates; an open native handle keeps
        // its identity and is not a reason to reject this namespace move.
        let fences: HashMap<_, _> = sources.iter().map(|f| (f.ino, *f)).collect();
        let staged_entries =
            Self::collect_subtree_entries_in_txn(&txn, receipt, stage, staged.clone())?;
        let source_ids: HashSet<_> = doomed.iter().map(|node| node.ino).collect();
        if doomed.len() != staged_entries.len()
            || source_ids.len() != fences.len()
            || staged_entries
                .iter()
                .any(|node| source_ids.contains(&node.ino))
        {
            return Err(MetaError::TargetChanged);
        }
        // Validate the closed source set before adoption changes any epoch;
        // later hardlink aliases observe that same inode's newly adopted graph.
        for ino in &source_ids {
            let current = Self::get_attr_merged_in_write_txn(&txn, *ino)?;
            let fence = fences.get(ino).ok_or(MetaError::TargetChanged)?;
            if current.ctime != fence.ctime
                || current.size != fence.size
                || current.content_epoch != fence.content_epoch
            {
                return Err(MetaError::TargetChanged);
            }
        }
        let mut paired = HashMap::from([(entry.inode, staged.ino)]);
        let mut adopted = HashSet::new();
        let mut upload_epochs = HashMap::new();
        entry.state = RetainedEntryState::Restored {
            parent,
            name: name.to_vec(),
        };
        entry.revision = 1;
        Self::write_retained_entry_in_txn(&txn, &entry)?;
        for node in &doomed {
            let current = Self::get_attr_merged_in_write_txn(&txn, node.ino)?;
            let fence = fences.get(&node.ino).ok_or(MetaError::TargetChanged)?;
            let staged_ino = if node.ino == entry.inode {
                staged.ino
            } else {
                let stage_parent = *paired.get(&node.parent).ok_or(MetaError::TargetChanged)?;
                let copied = Self::lookup_merged_in_write_txn(&txn, stage_parent, &node.name)?
                    .ok_or(MetaError::TargetChanged)?;
                if copied.kind != current.kind {
                    return Err(MetaError::TargetChanged);
                }
                if let Some(previous) = paired.insert(node.ino, copied.ino)
                    && previous != copied.ino
                {
                    return Err(MetaError::TargetChanged);
                }
                copied.ino
            };
            if current.kind == FileType::RegularFile && adopted.insert(node.ino) {
                // Each hardlinked inode is adopted exactly once.
                let protected =
                    self.protect_retained_file_payload_in_txn(&txn, *fence, staged_ino)?;
                upload_epochs.insert(node.ino, protected.content_epoch);
                Self::clear_regular_file_graph_in_txn(&txn, staged_ino)?;
            }
        }
        Self::rename_in_txn(&txn, receipt, DATA_NAME, parent, name, false, false)?;
        self.journal_restored_tree_in_txn(&txn, &source_ids)?;
        if !Self::delete_subtree_postorder_in_txn(&txn, &staged_entries)? {
            return Err(MetaError::TargetChanged);
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(crate::trash::RetainedRestoreResult {
            entry,
            upload_epochs,
        })
    }

    /// Reconcile a restore rejected by the immutable shared Restored winner.
    /// Retire the unsent losing namespace and pristine adoption records. Keep
    /// later accepted content as a complete pending overlay before foreign base
    /// replacement, preserving tree identity and unrelated later events.
    pub(crate) fn reconcile_losing_retained_restore(
        &self,
        loser: &RetainedEntry,
        winner: &RetainedEntry,
        unpublished_start: u64,
        shared_content_epochs: &HashMap<Ino, u64>,
    ) -> MetaResult<()> {
        let (
            RetainedEntryState::Restored { parent, name },
            RetainedEntryState::Restored {
                parent: winner_parent,
                name: winner_name,
            },
        ) = (&loser.state, &winner.state)
        else {
            return Ok(());
        };
        if loser.receipt != winner.receipt
            || loser.inode != winner.inode
            || (parent == winner_parent && name == winner_name)
        {
            return Ok(());
        }
        let txn = self.begin_write_txn()?;
        let rows = {
            let table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .range(unpublished_start.max(1)..)?
                .map(|row| {
                    let (seq, bytes) = row?;
                    Ok((seq.value(), crate::events::decode_event(bytes.value())?))
                })
                .collect::<MetaResult<Vec<_>>>()?
        };
        let Some(start) = rows.iter().position(|(_, event)| matches!(event,
            crate::MetaEvent::SetXattr { ino, name, value, .. }
                if *ino == loser.receipt && name == RECEIPT_XATTR
                    && Self::decode_retained_entry(*ino, value).is_ok_and(|record| record == *loser)))
            else { return Ok(()); };
        // The stage root is the last postorder deletion in the same restore txn.
        let end = rows[start + 1..]
            .iter()
            .position(|(_, event)| {
                matches!(
                    event,
                    crate::MetaEvent::RemoveEntry { parent, name }
                        | crate::MetaEvent::RemoveLocalEntry { parent, name }
                        if *parent == loser.receipt && name != DATA_NAME
                )
            })
            .map(|offset| start + 1 + offset)
            .ok_or(MetaError::TargetChanged)?;
        let losing_file_forks: HashMap<_, _> = rows[start..=end]
            .iter()
            .filter_map(|(_, event)| match event {
                crate::MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => {
                    Some((attr.ino, attr))
                }
                _ => None,
            })
            .collect();
        let noop = crate::events::encode_event(&crate::MetaEvent::RemoveLocalEntry {
            parent: *parent,
            name: name.clone(),
        })?;
        let mut replacements = Vec::new();
        for (seq, event) in &rows[start..=end] {
            let replacement = match event {
                crate::MetaEvent::SetXattr { ino, name: key, .. }
                    if *ino == loser.receipt && key == RECEIPT_XATTR =>
                {
                    Some(crate::MetaEvent::RemoveLocalEntry {
                        parent: *parent,
                        name: name.clone(),
                    })
                }
                crate::MetaEvent::RenameEntry {
                    parent: from,
                    name: from_name,
                    new_parent,
                    new_name,
                    entry,
                    ..
                } if *from == loser.receipt
                    && from_name == DATA_NAME
                    && new_parent == parent
                    && new_name == name
                    && entry.ino == loser.inode =>
                {
                    Some(crate::MetaEvent::RemoveLocalEntry {
                        parent: *parent,
                        name: name.clone(),
                    })
                }
                crate::MetaEvent::AddEntry {
                    parent: to,
                    name: to_name,
                    entry,
                } if to == parent && to_name == name && entry.ino == loser.inode => {
                    Some(crate::MetaEvent::RemoveLocalEntry {
                        parent: *parent,
                        name: name.clone(),
                    })
                }
                crate::MetaEvent::SetAttr { attr }
                    if attr.ino == loser.inode && attr.kind == FileType::Directory =>
                {
                    let mut attr = attr.clone();
                    attr.parent = *winner_parent;
                    Some(crate::MetaEvent::SetAttr { attr })
                }
                crate::MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                    if losing_file_forks.contains_key(ino) =>
                {
                    None
                }
                crate::MetaEvent::SetAttr { attr } if losing_file_forks.contains_key(&attr.ino) => {
                    Some(crate::MetaEvent::RemoveLocalEntry {
                        parent: *parent,
                        name: name.clone(),
                    })
                }
                _ => None,
            };
            if matches!(event, crate::MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                if losing_file_forks.contains_key(ino))
            {
                replacements.push((*seq, noop.clone()));
            } else if let Some(event) = replacement {
                replacements.push((*seq, crate::events::encode_event(&event)?));
            }
        }
        if !losing_file_forks.is_empty() {
            for (&ino, restored_attr) in &losing_file_forks {
                let restore_epoch = restored_attr.content_epoch;
                let current = match Self::get_attr_merged_in_write_txn(&txn, ino) {
                    Ok(attr) if attr.kind == FileType::RegularFile => attr,
                    Ok(_) | Err(MetaError::InodeNotFound(_)) => continue,
                    Err(error) => return Err(error),
                };
                let content_signature = |chunk, slices: &[crate::schema::SliceRecord]| {
                    let mut state = Self::regular_file_chunk_state_from_slices(slices)?;
                    for slice in state.slices.values_mut() {
                        slice.payload_state = PayloadRefState::LocalOnly;
                    }
                    // Resolve overlaps and contiguous pieces, ignoring bookkeeping identities/state.
                    let compacted = Self::compact_regular_file_chunk_state(chunk, &state)?;
                    Ok::<_, MetaError>(
                        compacted
                            .into_iter()
                            .flat_map(|chunk| chunk.slices.into_values())
                            .map(|slice| (slice.range, slice.payload))
                            .collect::<Vec<_>>(),
                    )
                };
                let restored_chunks: HashMap<_, _> = rows[start..=end]
                    .iter()
                    .filter_map(|(_, event)| match event {
                        crate::MetaEvent::SetRegularFileOverlaySlices {
                            ino: event_ino,
                            chunk_slices,
                        } if *event_ino == ino => Some(chunk_slices),
                        _ => None,
                    })
                    .flatten()
                    .map(|(chunk, slices)| Ok((*chunk, content_signature(*chunk, slices)?)))
                    .collect::<MetaResult<_>>()?;
                let mut later_content_write = false;
                for (_, event) in &rows[end + 1..] {
                    match event {
                        crate::MetaEvent::SetAttr { attr }
                            if attr.ino == ino && attr.size != restored_attr.size =>
                        {
                            later_content_write = true;
                        }
                        crate::MetaEvent::SetRegularFileOverlaySlices {
                            ino: event_ino,
                            chunk_slices,
                        } if *event_ino == ino => {
                            for (chunk, slices) in chunk_slices {
                                if restored_chunks.get(chunk).map(Vec::as_slice).unwrap_or(&[])
                                    != content_signature(*chunk, slices)?.as_slice()
                                {
                                    later_content_write = true;
                                }
                            }
                        }
                        _ => {}
                    }
                    if later_content_write {
                        break;
                    }
                }
                if current.content_epoch > restore_epoch {
                    continue;
                }
                if later_content_write {
                    // A local publication is only a private base until shared ack.
                    // Keep the complete accepted graph as an overlay while the winner replaces base.
                    let table = if Self::is_fully_published_regular_file(&current) {
                        REGULAR_FILE_BASE_TABLE
                    } else {
                        REGULAR_FILE_OVERLAY_TABLE
                    };
                    let chunks =
                        Self::regular_file_chunk_rows_for_inode_in_write_txn(&txn, table, ino)?;
                    let mut rebased = current;
                    rebased.content_epoch = rebased
                        .content_epoch
                        .max(shared_content_epochs.get(&ino).copied().unwrap_or_default())
                        .checked_add(1)
                        .ok_or_else(|| MetaError::Storage("content epoch overflow".into()))?;
                    rebased.committed_content_epoch = 0;
                    rebased.committed_size = 0;
                    rebased.upload_state = crate::UploadState::Updating;
                    for (seq, event) in &rows[end + 1..] {
                        if matches!(event,
                            crate::MetaEvent::SetRegularFileOverlaySlices { ino: event_ino, .. }
                                | crate::MetaEvent::PublishRegularFileBaseSlices { ino: event_ino, .. }
                                if *event_ino == ino)
                            || matches!(event, crate::MetaEvent::SetAttr { attr } if attr.ino == ino)
                        {
                            replacements.push((*seq, noop.clone()));
                        }
                    }
                    let (_, applied) = self.apply_regular_file_overlay_mutation_in_txn(
                        &txn,
                        ino,
                        &chunks,
                        Some(&rebased),
                    )?;
                    if !applied {
                        return Err(MetaError::TargetChanged);
                    }
                    continue;
                }
                for (seq, event) in &rows[end + 1..] {
                    if matches!(event, crate::MetaEvent::SetRegularFileOverlaySlices { ino: event_ino, .. } if *event_ino == ino)
                    {
                        replacements.push((*seq, noop.clone()));
                    }
                }
                let mut idx = end + 1;
                while idx + 1 < rows.len() {
                    if let (
                        (
                            pub_seq,
                            crate::MetaEvent::PublishRegularFileBaseSlices {
                                ino: event_ino, ..
                            },
                        ),
                        (attr_seq, crate::MetaEvent::SetAttr { attr }),
                    ) = (&rows[idx], &rows[idx + 1])
                        && *event_ino == ino
                        && attr.ino == ino
                        && attr.kind == FileType::RegularFile
                        && attr.content_epoch == restore_epoch
                        && attr.committed_content_epoch == attr.content_epoch
                    {
                        replacements.push((*pub_seq, noop.clone()));
                        replacements.push((*attr_seq, noop.clone()));
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                }
                if current.content_epoch == restore_epoch && current.committed_content_epoch == 0 {
                    Self::clear_regular_file_chunk_rows_in_txn(
                        &txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        ino,
                    )?;
                    let base_attr = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .get(ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|g| decode::<InodeAttr>(g.value()))
                        .transpose()?;
                    if let Some(base_attr) = base_attr {
                        let mut reset = current;
                        reset.size = base_attr.size;
                        reset.committed_size = base_attr.committed_size;
                        reset.content_epoch = base_attr.content_epoch;
                        reset.committed_content_epoch = base_attr.committed_content_epoch;
                        reset.upload_state = base_attr.upload_state;
                        Self::set_inode_in_txn(&txn, &reset)?;
                        Self::sync_inode_lifecycle_in_txn(&txn, ino)?;
                    }
                }
            }
        }
        // A later explicit operation on this name owns it; do not remove a real alias.
        let later_name_owner = rows[end + 1..].iter().any(|(_, event)| match event {
            crate::MetaEvent::AddEntry {
                parent: to,
                name: to_name,
                ..
            }
            | crate::MetaEvent::RemoveEntry {
                parent: to,
                name: to_name,
            }
            | crate::MetaEvent::RemoveLocalEntry {
                parent: to,
                name: to_name,
            } => to == parent && to_name == name,
            crate::MetaEvent::RenameEntry {
                parent: from,
                name: from_name,
                new_parent,
                new_name,
                ..
            } => {
                (from == parent && from_name == name) || (new_parent == parent && new_name == name)
            }
            _ => false,
        });
        if !later_name_owner {
            let loser_is_dir = match Self::get_attr_merged_in_write_txn(&txn, loser.inode) {
                Ok(attr) => attr.kind == FileType::Directory,
                Err(MetaError::InodeNotFound(_)) => false,
                Err(err) => return Err(err),
            };
            if Self::lookup_merged_in_write_txn(&txn, *parent, name)?
                .is_some_and(|entry| entry.ino == loser.inode)
            {
                Self::remove_dentry_overlay_in_txn(&txn, *parent, name)?;
                if loser_is_dir {
                    let mut parent_attr = Self::get_attr_merged_in_write_txn(&txn, *parent)?;
                    parent_attr.nlink = parent_attr.nlink.saturating_sub(1);
                    Self::set_inode_in_txn(&txn, &parent_attr)?;
                }
            }
            if let Ok(mut attr) = Self::get_attr_merged_in_write_txn(&txn, loser.inode)
                && attr.kind == FileType::Directory
                && attr.parent == *parent
            {
                attr.parent = *winner_parent;
                Self::set_inode_in_txn(&txn, &attr)?;
            }
            if loser_is_dir {
                let adjust_parent_attr = |attr: &mut InodeAttr| -> bool {
                    if attr.ino == *parent && attr.kind == FileType::Directory {
                        attr.nlink = attr.nlink.saturating_sub(1).max(2);
                        true
                    } else {
                        false
                    }
                };
                for (seq, event) in &rows[end + 1..] {
                    let mut adjusted = event.clone();
                    let changed = match &mut adjusted {
                        crate::MetaEvent::SetAttr { attr } => adjust_parent_attr(attr),
                        crate::MetaEvent::RenameEntry {
                            displaced_set_attr,
                            moved_dir_attr,
                            old_parent_attr,
                            new_parent_attr,
                            ..
                        } => {
                            let mut changed = false;
                            if let Some(attr) = displaced_set_attr.as_mut().as_mut() {
                                changed |= adjust_parent_attr(attr);
                            }
                            if let Some(attr) = moved_dir_attr.as_mut().as_mut() {
                                changed |= adjust_parent_attr(attr);
                            }
                            if let Some(attr) = old_parent_attr.as_mut().as_mut() {
                                changed |= adjust_parent_attr(attr);
                            }
                            if let Some(attr) = new_parent_attr.as_mut().as_mut() {
                                changed |= adjust_parent_attr(attr);
                            }
                            changed
                        }
                        _ => false,
                    };
                    if changed {
                        replacements.push((*seq, crate::events::encode_event(&adjusted)?));
                    }
                }
            }
        }
        Self::set_xattr_in_txn(
            &txn,
            winner.receipt,
            RECEIPT_XATTR,
            &serde_json::to_vec(winner).map_err(|e| MetaError::Serialization(e.to_string()))?,
        )?;
        {
            let mut table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (seq, bytes) in replacements {
                table.insert(seq, bytes.as_slice())?;
            }
        }
        txn.commit().map_err(|e| MetaError::Storage(e.to_string()))
    }

    /// Only an unacknowledged restore namespace owns this exception to peer
    /// deletion. The local receipt survives terminal-record retirement/restart.
    pub(crate) fn pending_restored_inodes_in_txn(
        txn: &RedbWriteTransaction<'_>,
        pending_dentries: &HashSet<Vec<u8>>,
    ) -> MetaResult<HashSet<Ino>> {
        let mut protected = HashSet::new();
        if pending_dentries.is_empty() {
            return Ok(protected);
        }
        let xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in xattrs.iter()? {
            let (key, value) = row?;
            let key = key.value();
            if xattr_name_from_key(key) != RECEIPT_XATTR || tombstones.get(key)?.is_some() {
                continue;
            }
            let receipt = Ino::from_le_bytes(
                key[..8]
                    .try_into()
                    .map_err(|_| MetaError::Serialization("invalid receipt key".into()))?,
            );
            let entry = Self::decode_retained_entry(receipt, value.value())?;
            let RetainedEntryState::Restored { parent, name } = entry.state else {
                continue;
            };
            if !pending_dentries.contains(&encode_dentry_key(parent, &name)) {
                continue;
            }
            let Some(root) = Self::lookup_merged_in_write_txn(txn, parent, &name)?
                .filter(|root| root.ino == entry.inode)
            else {
                continue;
            };
            protected.extend(
                Self::collect_subtree_entries_in_txn(txn, parent, &name, root)?
                    .into_iter()
                    .map(|node| node.ino),
            );
        }
        Ok(protected)
    }

    fn journal_restored_tree_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        inodes: &HashSet<Ino>,
    ) -> MetaResult<()> {
        for &ino in inodes {
            let attr = Self::get_attr_merged_in_write_txn(txn, ino)?;
            Self::append_event_in_txn(txn, &crate::MetaEvent::SetAttr { attr: attr.clone() })?;
            let mut links =
                Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)?;
            links.extend(Self::dentry_keys_for_inode_in_write_index(
                txn,
                BASE_DENTRIES_BY_INODE_TABLE,
                ino,
            )?);
            links.sort();
            links.dedup();
            for key in links {
                let (parent, name) = decode_dentry_key(&key)?;
                if let Some(entry) = Self::lookup_merged_in_write_txn(txn, parent, name)?
                    && entry.ino == ino
                {
                    Self::append_event_in_txn(
                        txn,
                        &crate::MetaEvent::AddEntry {
                            parent,
                            name: name.to_vec(),
                            entry,
                        },
                    )?;
                }
            }
            if attr.kind == FileType::Symlink {
                let local = txn
                    .open_table(SYMLINKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let base = txn
                    .open_table(BASE_SYMLINKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let target = local
                    .get(ino)?
                    .or(base.get(ino)?)
                    .ok_or(MetaError::InodeNotFound(ino))?;
                Self::append_event_in_txn(
                    txn,
                    &crate::MetaEvent::SetSymlinkTarget {
                        ino,
                        target: target.value().to_vec(),
                    },
                )?;
            }
            let mut keys = Self::keys_for_inode_in_write_table(txn, BASE_XATTRS_TABLE, ino)?;
            keys.extend(Self::keys_for_inode_in_write_table(txn, XATTRS_TABLE, ino)?);
            keys.sort();
            keys.dedup();
            for key in keys {
                if txn
                    .open_table(XATTR_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .get(key.as_slice())?
                    .is_some()
                {
                    continue;
                }
                let local = txn
                    .open_table(XATTRS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let base = txn
                    .open_table(BASE_XATTRS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                if let Some(value) = local.get(key.as_slice())?.or(base.get(key.as_slice())?) {
                    let name = xattr_name_from_key(&key);
                    Self::append_event_in_txn(
                        txn,
                        &crate::MetaEvent::SetXattr {
                            ino,
                            name: name.to_vec(),
                            value: decode_xattr_value_from_storage(name, value.value()),
                            flags: 0,
                        },
                    )?;
                }
            }
            let mut keys = Self::keys_for_inode_in_write_table(txn, BASE_ACL_RULES_TABLE, ino)?;
            keys.extend(Self::keys_for_inode_in_write_table(
                txn,
                ACL_RULES_TABLE,
                ino,
            )?);
            keys.sort();
            keys.dedup();
            for key in keys {
                if txn
                    .open_table(ACL_RULE_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .get(key.as_slice())?
                    .is_some()
                {
                    continue;
                }
                let local = txn
                    .open_table(ACL_RULES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let base = txn
                    .open_table(BASE_ACL_RULES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                if let Some(value) = local.get(key.as_slice())?.or(base.get(key.as_slice())?) {
                    let principal = String::from_utf8(key[8..].to_vec())
                        .map_err(|e| MetaError::Serialization(e.to_string()))?;
                    let access_level = AccessLevel::from_u8(value.value())
                        .ok_or_else(|| MetaError::Serialization("invalid retained ACL".into()))?;
                    Self::append_event_in_txn(
                        txn,
                        &crate::MetaEvent::SetAclRule {
                            ino,
                            principal,
                            access_level,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Physically settle a purge whose `PurgeRequested` record is the shared
    /// winner replayed into base. It removes only the `receipt/data` tree,
    /// still bound to `entry.inode`, through ordinary namespace deletion, so
    /// publication retires its objects to the janitor queue and hard links,
    /// snapshot pins and unpublished-upload handling keep their semantics. The
    /// receipt directory and its record stay as the shared tombstone. An open
    /// inode anywhere in the tree defers the whole settlement to a later sweep.
    pub(crate) fn settle_retained_purge(
        &self,
        receipt: Ino,
        is_open: impl Fn(Ino) -> bool,
    ) -> MetaResult<RetainedSettlement> {
        let txn = self.begin_write_txn()?;
        let shared = Self::base_retained_record_in_txn(&txn, receipt)?
            .map(|bytes| Self::decode_retained_entry(receipt, &bytes))
            .transpose()?;
        let Some(entry) =
            shared.filter(|entry| matches!(entry.state, RetainedEntryState::PurgeRequested { .. }))
        else {
            return Ok(RetainedSettlement::NotAcknowledged);
        };
        // Never by inode alone: a losing offline restore may own the inode now.
        let Some(data) = Self::lookup_merged_in_write_txn(&txn, receipt, DATA_NAME)?
            .filter(|data| data.ino == entry.inode)
        else {
            return Ok(RetainedSettlement::NothingToSettle);
        };
        let doomed = Self::collect_subtree_entries_in_txn(&txn, receipt, DATA_NAME, data)?;
        if doomed.iter().any(|entry| is_open(entry.ino)) {
            return Ok(RetainedSettlement::Busy);
        }
        if !Self::delete_subtree_postorder_in_txn(&txn, &doomed)? {
            return Ok(RetainedSettlement::Busy);
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(RetainedSettlement::Settled)
    }

    /// Persist a local purge/expiry request without removing any data reference.
    /// This alone never authorizes local reclamation or remote object deletion.
    pub fn request_retained_purge(
        &self,
        receipt: Ino,
        reason: RetainedPurgeReason,
    ) -> MetaResult<RetainedEntry> {
        self.request_retained_purge_at(
            receipt,
            reason,
            super::fs_ops::current_unix_seconds().max(0) as u64,
        )
    }

    pub(crate) fn request_retained_purge_at(
        &self,
        receipt: Ino,
        reason: RetainedPurgeReason,
        now: u64,
    ) -> MetaResult<RetainedEntry> {
        let txn = self.begin_write_txn()?;
        let settings = Self::retained_settings_in_txn(&txn)?;
        let mut entry = Self::retained_entry_in_txn(&txn, receipt)?;
        match entry.state {
            RetainedEntryState::PurgeRequested { .. } => return Ok(entry),
            RetainedEntryState::Restored { .. } => {
                return Err(MetaError::Storage(
                    "retained entry was already restored".into(),
                ));
            }
            RetainedEntryState::Retained => {}
        }
        if let RetainedPurgeReason::Expired { settings_revision } = reason
            && (settings_revision != settings.revision
                || !settings.trash.is_expired(entry.deleted_at_unix_secs, now))
        {
            return Err(MetaError::Storage("retained entry has not expired".into()));
        }
        entry.state = RetainedEntryState::PurgeRequested { reason };
        entry.revision = 1;
        Self::write_retained_entry_in_txn(&txn, &entry)?;
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(entry)
    }

    /// Remove a local expiry proposal that shared commit rejected as stale, so
    /// the base `Retained` record is visible again. It writes no journal event:
    /// the caller retires that proposal's journal range, and no namespace moves.
    /// Only the exact stale proposal is reverted; anything else is left as is.
    pub(crate) fn discard_stale_retained_expiry(
        &self,
        receipt: Ino,
        settings_revision: u64,
    ) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        let key = encode_xattr_key(receipt, RECEIPT_XATTR);
        let overlay = txn
            .open_table(XATTRS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .get(key.as_slice())?
            .map(|value| {
                decode_xattr_value_from_storage_owned(RECEIPT_XATTR, value.value().to_vec())
            });
        let Some(overlay) = overlay else {
            return Ok(());
        };
        let overlay = Self::decode_retained_entry(receipt, &overlay)?;
        let stale = overlay.state
            == RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::Expired { settings_revision },
            };
        let base_retained = Self::base_retained_record_in_txn(&txn, receipt)?
            .map(|bytes| Self::decode_retained_entry(receipt, &bytes))
            .transpose()?
            .is_some_and(|base| base.state == RetainedEntryState::Retained);
        if stale && base_retained {
            txn.open_table(XATTRS_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?
                .remove(key.as_slice())?;
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(())
    }

    /// Permanently delete a single child beneath `receipt/data` in one local commit.
    /// Appends the `Retained` receipt authority fence immediately before the
    /// `NamespaceDeleteTxn` events so PG rejects any purge racing against a
    /// winning peer terminal transition on `receipt`.
    pub fn purge_retained_child(
        &self,
        receipt: Ino,
        parent_source_ino: Ino,
        name: &[u8],
        expected_ino: Ino,
        is_dir: bool,
    ) -> MetaResult<crate::trash::RetainedChildPurgeOutcome> {
        use super::fs_ops::{NamespaceDeleteResult, NamespaceDeleteSpec, NamespaceDeleteTxn};
        use crate::trash::RetainedChildPurgeOutcome;

        let txn = self.begin_write_txn()?;
        let entry = match Self::retained_entry_in_txn(&txn, receipt) {
            Ok(entry) => entry,
            Err(MetaError::InodeNotFound(_)) => {
                return Ok(RetainedChildPurgeOutcome::TargetChanged);
            }
            Err(error) => return Err(error),
        };
        if entry.state != RetainedEntryState::Retained {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        }
        let Some(data_root) = Self::lookup_merged_in_write_txn(&txn, receipt, DATA_NAME)? else {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        };
        if data_root.ino != entry.inode || data_root.kind != FileType::Directory {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        }

        // Verify `parent_source_ino` is a directory currently parented under `data_root.ino`
        // in O(depth) without walking the entire retained subtree.
        let mut cur = parent_source_ino;
        let mut reached_data_root = false;
        let mut seen = HashSet::new();
        for _ in 0..=256 {
            if cur == data_root.ino {
                reached_data_root = true;
                break;
            }
            if cur <= ROOT_INO || cur == receipt || !seen.insert(cur) {
                break;
            }
            let Ok(cur_attr) = Self::get_attr_merged_in_write_txn(&txn, cur) else {
                break;
            };
            if cur_attr.kind != FileType::Directory {
                break;
            }
            cur = cur_attr.parent;
        }
        if !reached_data_root {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        }
        let Ok(parent_attr) = Self::get_attr_merged_in_write_txn(&txn, parent_source_ino) else {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        };
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent_source_ino));
        }

        let Some(child) = Self::lookup_merged_in_write_txn(&txn, parent_source_ino, name)? else {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        };
        if child.ino != expected_ino || child.ino == entry.inode {
            return Ok(RetainedChildPurgeOutcome::TargetChanged);
        }
        if is_dir {
            if child.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(child.ino));
            }
            Self::ensure_directory_empty_in_write_txn(&txn, child.ino)?;
        } else if child.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(child.ino));
        }

        // Emit authority fence event immediately before NamespaceDeleteTxn events.
        // We intentionally do not rewrite XATTRS_TABLE since `receipt`'s local xattr is unchanged.
        let encoded = serde_json::to_vec(&entry)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        let fence_seq = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(crate::schema::counters::NEXT_EVENT_SEQ)?
            .map_or(1, |value| value.value());
        Self::append_event_in_txn(
            &txn,
            &crate::MetaEvent::SetXattr {
                ino: receipt,
                name: RECEIPT_XATTR.to_vec(),
                value: encoded,
                flags: 0,
            },
        )?;
        // Lets the orphan guard skip its journal scan when no purge is unacknowledged.
        Self::update_counter_in_txn(
            &txn,
            crate::schema::counters::LOCAL_RETAINED_CHILD_PURGE_SEQ,
            |_| Ok(fence_seq),
        )?;

        // A file purge keeps its graph (and accepted overlay bytes) as a
        // deferred delete: it can still lose to a peer restore, and
        // `finalize_orphan_inode` removes the graph only once that is settled.
        let spec = if is_dir {
            NamespaceDeleteSpec::new_rmdir(parent_source_ino, name)
        } else {
            NamespaceDeleteSpec::new_unlink_open_handle(parent_source_ino, name)
        }
        .with_expected_ino(Some(expected_ino));

        let result = NamespaceDeleteTxn::new(&txn).execute(&spec)?;
        match result {
            NamespaceDeleteResult::TargetChanged => Ok(RetainedChildPurgeOutcome::TargetChanged),
            NamespaceDeleteResult::File(attr) => {
                txn.commit()
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
                Ok(RetainedChildPurgeOutcome::Removed(Some(attr)))
            }
            NamespaceDeleteResult::Directory(_, removed_attr) => {
                txn.commit()
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
                Ok(RetainedChildPurgeOutcome::Removed(removed_attr))
            }
        }
    }

    /// Whether `ino` was unlinked by a `purge_retained_child` span that shared
    /// authority has not acknowledged yet (a losing span retires to a no-op and
    /// no longer matches). The span itself is the origin, so a later local
    /// restore that moves the purge parent out of the receipt cannot hide it.
    pub(super) fn unacknowledged_retained_child_purge_in_txn(
        txn: &RedbWriteTransaction<'_>,
        unacked: &mut UnackedChildPurges,
        ino: Ino,
    ) -> MetaResult<bool> {
        let counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let counter = |name: &str| -> MetaResult<Option<u64>> {
            Ok(counters.get(name)?.map(|value| value.value()))
        };
        let flushed = counter(crate::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ)?
            .unwrap_or(1)
            .max(1);
        // O(1) for the common case: no child purge since the ack boundary.
        let last_purge = counter(crate::schema::counters::LOCAL_RETAINED_CHILD_PURGE_SEQ)?;
        if last_purge.is_none_or(|seq| seq < flushed) {
            return Ok(false);
        }
        let end = counter(crate::schema::counters::NEXT_EVENT_SEQ)?.unwrap_or(1);
        // Commits append whole spans, so the delta since the last scan starts on a
        // span boundary. Acks and loser retires only shrink the true set, so a
        // cached ino can at worst defer (never delete) until the next pass.
        let start = flushed.max(unacked.scanned_end);
        if start < end {
            let events = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .range(start..end)?
                .map(|row| crate::events::decode_event(row?.1.value()))
                .collect::<MetaResult<Vec<_>>>()?;
            #[cfg(test)]
            {
                unacked.decoded += events.len();
            }
            unacked.inos.extend(
                (0..events.len())
                    .filter_map(|index| Self::child_purge_event_span(&events, index))
                    .map(|span| span.child_ino),
            );
        }
        unacked.scanned_end = unacked.scanned_end.max(end);
        Ok(unacked.inos.contains(&ino))
    }

    /// Recognizes the exact journal span emitted by `purge_retained_child` starting at `start`.
    pub(crate) fn child_purge_event_span(
        events: &[crate::events::MetaEvent],
        start: usize,
    ) -> Option<RetainedChildPurgeSpan> {
        use crate::events::MetaEvent;

        let MetaEvent::SetXattr {
            ino, name, value, ..
        } = events.get(start)?
        else {
            return None;
        };
        if name != RECEIPT_XATTR {
            return None;
        }
        let entry = Self::decode_retained_entry(*ino, value).ok()?;
        if entry.state != RetainedEntryState::Retained {
            return None;
        }
        let receipt = entry.receipt;
        // Unlinking a child with uploaded, unpublished overlay bytes first queues
        // their local-only deletion markers in the same commit; they belong to the span.
        let markers = events
            .get(start + 1..)?
            .iter()
            .take_while(|event| matches!(event, MetaEvent::AddPendingDeletion { .. }))
            .count();
        let start = start + markers;
        let tail = events.get(start + 1..)?;
        match tail {
            // Case 1: rmdir -> [SetAttr(parent), RemoveAttr(child), RemoveEntry(parent, name)]
            [
                MetaEvent::SetAttr { attr: p_attr },
                MetaEvent::RemoveAttr { ino: c_ino },
                MetaEvent::RemoveEntry { parent, name },
                ..,
            ] if p_attr.kind == FileType::Directory
                && *parent == p_attr.ino
                && *c_ino != receipt =>
            {
                Some(RetainedChildPurgeSpan {
                    receipt,
                    end: start + 3,
                    parent: *parent,
                    name: name.clone(),
                    child_ino: *c_ino,
                    touches_parent_attr: true,
                })
            }
            // Case 2: unlink base rollback -> [RemoveEntry|RemoveLocalEntry, PublishRegularFileBaseSlices, SetAttr]
            [
                MetaEvent::RemoveEntry { parent, name }
                | MetaEvent::RemoveLocalEntry { parent, name },
                MetaEvent::PublishRegularFileBaseSlices { ino: c1, .. },
                MetaEvent::SetAttr { attr: c2 },
                ..,
            ] if *c1 == c2.ino && *parent != receipt && !name.is_empty() => {
                Some(RetainedChildPurgeSpan {
                    receipt,
                    end: start + 3,
                    parent: *parent,
                    name: name.clone(),
                    child_ino: *c1,
                    touches_parent_attr: false,
                })
            }
            // Case 3: unlink unpublished draft -> [RemoveLocalEntry, RemoveRegularFileGraph, RemoveAttr]
            [
                MetaEvent::RemoveLocalEntry { parent, name },
                MetaEvent::RemoveRegularFileGraph { ino: c1 },
                MetaEvent::RemoveAttr { ino: c2 },
                ..,
            ] if *c1 == *c2 && *parent != receipt && !name.is_empty() => {
                Some(RetainedChildPurgeSpan {
                    receipt,
                    end: start + 3,
                    parent: *parent,
                    name: name.clone(),
                    child_ino: *c1,
                    touches_parent_attr: false,
                })
            }
            // Case 4: unlink open handle / hardlink -> [RemoveEntry, SetAttr(child), SetAttr(parent)]
            [
                MetaEvent::RemoveEntry { parent, name },
                MetaEvent::SetAttr { attr: c_attr },
                MetaEvent::SetAttr { attr: p_attr },
                ..,
            ] if *parent == p_attr.ino
                && p_attr.kind == FileType::Directory
                && c_attr.ino != *parent =>
            {
                Some(RetainedChildPurgeSpan {
                    receipt,
                    end: start + 3,
                    parent: *parent,
                    name: name.clone(),
                    child_ino: c_attr.ino,
                    touches_parent_attr: true,
                })
            }
            // Case 5: unlink normal -> [RemoveEntry, RemoveRegularFileGraph, RemoveAttr, SetAttr(parent)]
            [
                MetaEvent::RemoveEntry { parent, name },
                MetaEvent::RemoveRegularFileGraph { ino: c1 },
                MetaEvent::RemoveAttr { ino: c2 },
                MetaEvent::SetAttr { attr: p_attr },
                ..,
            ] if *c1 == *c2 && *parent == p_attr.ino && p_attr.kind == FileType::Directory => {
                Some(RetainedChildPurgeSpan {
                    receipt,
                    end: start + 4,
                    parent: *parent,
                    name: name.clone(),
                    child_ino: *c1,
                    touches_parent_attr: true,
                })
            }
            _ => None,
        }
    }

    /// Reconcile local `purge_retained_child` journal entries on `receipt` after PG
    /// committed a terminal transition (`winner.state != Retained`) first.
    /// `retire` names the span start sequences the caller proved lost; a span
    /// already committed to PG (published, awaiting its self-ack) must stay intact.
    pub(crate) fn reconcile_losing_retained_child_purges(
        &self,
        receipt: Ino,
        winner: &RetainedEntry,
        unpublished_start: u64,
        retire: &dyn Fn(u64) -> bool,
    ) -> MetaResult<()> {
        if winner.state == RetainedEntryState::Retained || winner.receipt != receipt {
            return Ok(());
        }
        let txn = self.begin_write_txn()?;
        let mut dummy_dentries = HashSet::new();
        let mut dummy_inodes = HashSet::new();
        let changed = Self::reconcile_losing_retained_child_purges_in_txn(
            &txn,
            receipt,
            winner,
            unpublished_start,
            retire,
            true,
            &mut dummy_dentries,
            &mut dummy_inodes,
        )?;
        if changed {
            txn.commit()
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one reconcile shared by the flush classifier and foreign apply"
    )]
    pub(crate) fn reconcile_losing_retained_child_purges_in_txn(
        txn: &RedbWriteTransaction<'_>,
        receipt: Ino,
        winner: &RetainedEntry,
        unpublished_start: u64,
        retire: &dyn Fn(u64) -> bool,
        update_overlay_to_winner: bool,
        reconcile_dentry_keys: &mut HashSet<Vec<u8>>,
        reconcile_inodes: &mut HashSet<Ino>,
    ) -> MetaResult<bool> {
        use crate::events::MetaEvent;

        if winner.state == RetainedEntryState::Retained || winner.receipt != receipt {
            return Ok(false);
        }
        let (seqs, events): (Vec<u64>, Vec<MetaEvent>) = {
            let table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .range(unpublished_start.max(1)..)?
                .map(|row| {
                    let (seq, bytes) = row?;
                    Ok((seq.value(), crate::events::decode_event(bytes.value())?))
                })
                .collect::<MetaResult<Vec<_>>>()?
                .into_iter()
                .unzip()
        };
        let mut spans = Vec::new();
        let mut retired_indices = HashSet::new();
        let mut idx = 0usize;
        while idx < events.len() {
            if let Some(span) = Self::child_purge_event_span(&events, idx)
                && span.receipt == receipt
            {
                let end = span.end;
                if retire(seqs[idx]) {
                    retired_indices.extend(idx..=end);
                    spans.push(span);
                }
                idx = end + 1;
                continue;
            }
            idx += 1;
        }
        if spans.is_empty() {
            return Ok(false);
        }

        // Encode a inert placeholder (`RemoveLocalEntry { parent: receipt, name: b"" }`)
        // that never collides with real namespace names and is stripped by remote projection.
        let noop = crate::events::encode_event(&MetaEvent::RemoveLocalEntry {
            parent: receipt,
            name: Vec::new(),
        })?;
        {
            let mut table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for &pos in &retired_indices {
                table.insert(seqs[pos], noop.as_slice())?;
            }
        }

        // One pass over the surviving journal records each key's latest touch;
        // the per-span checks below are O(1). Only a touch after the span owns
        // the state; an earlier one is the overlay the span replaced.
        let mut last_dentry: HashMap<(Ino, &[u8]), usize> = HashMap::new();
        let mut last_inode: HashMap<Ino, usize> = HashMap::new();
        let mut last_set_attr: HashMap<Ino, &InodeAttr> = HashMap::new();
        // The latest binding an earlier event left at a name (`None` = removed).
        let mut last_binding: HashMap<(Ino, &[u8]), Option<&DirEntryValue>> = HashMap::new();
        // The losing span's own GC markers name bytes the restored child keeps.
        let mut retired_markers = Vec::new();
        for (i, ev) in events.iter().enumerate() {
            if retired_indices.contains(&i) {
                if let MetaEvent::AddPendingDeletion { object_id, .. } = ev {
                    retired_markers.push(*object_id);
                }
                continue;
            }
            match ev {
                MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    last_dentry.insert((*parent, name.as_slice()), i);
                    last_binding.insert((*parent, name.as_slice()), Some(entry));
                }
                MetaEvent::RemoveEntry { parent, name }
                | MetaEvent::RemoveLocalEntry { parent, name } => {
                    last_dentry.insert((*parent, name.as_slice()), i);
                    last_binding.insert((*parent, name.as_slice()), None);
                }
                MetaEvent::SetAttr { attr } => {
                    last_inode.insert(attr.ino, i);
                    last_set_attr.insert(attr.ino, attr);
                }
                MetaEvent::RemoveAttr { ino }
                | MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                | MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                | MetaEvent::RemoveRegularFileGraph { ino }
                | MetaEvent::SetSymlinkTarget { ino, .. }
                | MetaEvent::SetXattr { ino, .. }
                | MetaEvent::RemoveXattr { ino, .. }
                | MetaEvent::SetAclRule { ino, .. }
                | MetaEvent::RemoveAclRule { ino, .. } => {
                    last_inode.insert(*ino, i);
                    if matches!(ev, MetaEvent::RemoveAttr { .. }) {
                        last_set_attr.remove(ino);
                    }
                }
                MetaEvent::RenameEntry {
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
                    ..
                } => {
                    last_dentry.insert((*parent, name.as_slice()), i);
                    last_dentry.insert((*new_parent, new_name.as_slice()), i);
                    last_binding.insert((*parent, name.as_slice()), None);
                    last_binding.insert((*new_parent, new_name.as_slice()), Some(entry));
                    last_inode.insert(entry.ino, i);
                    if let Some(ino) = displaced_remove_attr_ino {
                        last_inode.insert(*ino, i);
                        last_set_attr.remove(ino);
                    }
                    for attr in [
                        displaced_set_attr,
                        moved_dir_attr,
                        old_parent_attr,
                        new_parent_attr,
                    ]
                    .into_iter()
                    .filter_map(|attr| attr.as_ref().as_ref())
                    {
                        last_inode.insert(attr.ino, i);
                        last_set_attr.insert(attr.ino, attr);
                    }
                }
                _ => {}
            }
        }

        let mut dentry_tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut inode_tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut deferred_deletes = txn
            .open_table(DEFERRED_DELETES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_inodes = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        // Restores the overlay row the span replaced: the latest earlier
        // `SetAttr`, or the base row when the overlay held nothing else.
        let mut restore_inode = |ino: Ino| -> MetaResult<()> {
            if let Some(attr) = last_set_attr.get(&ino) {
                inodes.insert(ino, encode(*attr)?.as_slice())?;
            } else if base_inodes.get(ino)?.is_some() {
                inodes.remove(ino)?;
            }
            Ok(())
        };
        let mut restored_bindings = Vec::new();
        for span in &spans {
            let owned_after = |touch: Option<&usize>| touch.is_some_and(|&i| i > span.end);
            if !owned_after(last_dentry.get(&(span.parent, span.name.as_slice()))) {
                let dentry_key = encode_dentry_key(span.parent, &span.name);
                dentry_tombstones.remove(dentry_key.as_slice())?;
                reconcile_dentry_keys.insert(dentry_key);
                // An earlier local add or rename bound this name in the overlay.
                if let Some(Some(entry)) = last_binding.get(&(span.parent, span.name.as_slice())) {
                    restored_bindings.push((span.parent, span.name.as_slice(), *entry));
                }
            }
            if !owned_after(last_inode.get(&span.child_ino)) {
                inode_tombstones.remove(span.child_ino)?;
                lifecycle.remove(span.child_ino)?;
                deferred_deletes.remove(span.child_ino)?;
                restore_inode(span.child_ino)?;
                reconcile_inodes.insert(span.child_ino);
            }
            if span.touches_parent_attr && !owned_after(last_inode.get(&span.parent)) {
                restore_inode(span.parent)?;
                reconcile_inodes.insert(span.parent);
            }
        }
        drop(dentry_tombstones);
        drop(inode_tombstones);
        drop(lifecycle);
        drop(deferred_deletes);
        drop(inodes);
        drop(base_inodes);
        for (parent, name, entry) in restored_bindings {
            Self::set_dentry_in_txn(txn, parent, name, entry)?;
        }
        // ponytail: markers are keyed by object id without provenance, so a packed
        // object another delete also queued loses that marker here. Bounded leak only
        // (never deletion): `studiofs janitor` orphan discovery reclaims it. Add marker
        // provenance if packed sharing makes this common.
        if !retired_markers.is_empty() {
            let mut pending = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for object_id in retired_markers {
                pending.remove(object_id)?;
            }
        }

        let key = encode_xattr_key(receipt, RECEIPT_XATTR);
        if update_overlay_to_winner {
            let encoded_winner =
                serde_json::to_vec(winner).map_err(|e| MetaError::Serialization(e.to_string()))?;
            Self::set_xattr_in_txn(txn, receipt, RECEIPT_XATTR, &encoded_winner)?;
        } else {
            txn.open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .remove(key.as_slice())?;
            txn.open_table(XATTR_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .remove(key.as_slice())?;
        }
        reconcile_inodes.insert(receipt);
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RetainedChildPurgeSpan {
    pub(crate) receipt: Ino,
    pub(crate) end: usize,
    pub(crate) parent: Ino,
    pub(crate) name: Vec<u8>,
    pub(crate) child_ino: Ino,
    pub(crate) touches_parent_attr: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trash::MetaVolumeSettings;

    fn engine(path: &Path) -> RedbMetaEngine {
        let engine = RedbMetaEngine::open(path).unwrap();
        engine.init_root(0, 0).unwrap();
        let format = FormatInfo::new(
            "retained-test".into(),
            "test".into(),
            crate::FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9".into(),
                bucket: "test".into(),
                access_key: "test".into(),
                secret_key: "test".into(),
                region: "test".into(),
            },
            4,
        )
        .unwrap();
        engine.set_format_info(&format).unwrap();
        engine
    }

    #[test]
    fn retained_entry_disabled_leaves_source_and_journal_untouched() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-disabled-");
        let engine = engine(&temp.path().join("db"));
        let source = engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert!(
            engine
                .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
                .is_err()
        );
        assert_eq!(engine.lookup(ROOT_INO, b"source").unwrap().ino, source.ino);
        assert!(engine.get_attr(receipt).is_err());
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
    }

    #[test]
    fn retained_entry_read_only_capability_lists_but_denies_all_mutations() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-read-only-");
        let engine = Arc::new(engine(&temp.path().join("db")));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let private = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let entry = engine
            .retain_namespace_entry(ROOT_INO, b"source", private.ino, receipt)
            .unwrap();
        let read_only = crate::ReadOnlyMetaEngine::new(engine.clone());
        let store = read_only.retained_store().unwrap();
        assert_eq!(store.retained_entry(receipt).unwrap(), Some(entry.clone()));
        assert_eq!(
            store.volume_settings().unwrap(),
            engine.volume_settings().unwrap()
        );
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert!(matches!(
            store.restore_retained_entry(receipt, ROOT_INO, b"restored", b"stage", &[]),
            Err(MetaError::AccessDenied { .. })
        ));
        assert!(matches!(
            store.protect_retained_file_payload(
                receipt,
                crate::trash::RetainedSourceFence {
                    ino: entry.inode,
                    ctime: 0,
                    size: 0,
                    content_epoch: 0,
                },
                b"stage"
            ),
            Err(MetaError::AccessDenied { .. })
        ));
        assert!(matches!(
            store.request_retained_purge(receipt, RetainedPurgeReason::User),
            Err(MetaError::AccessDenied { .. })
        ));
        assert!(matches!(
            store.retain_namespace_entry(ROOT_INO, b"other", private.ino, receipt + 1),
            Err(MetaError::AccessDenied { .. })
        ));
        assert!(matches!(
            store.compare_and_set_trash_days(1, 0),
            Err(MetaError::AccessDenied { .. })
        ));
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
    }

    trait RestoreTestEntry {
        fn restore_test_entry(
            &self,
            receipt: Ino,
            parent: Ino,
            name: &[u8],
            stage: &[u8],
            unused: &[crate::trash::RetainedSourceFence],
        ) -> MetaResult<RetainedEntry>;
    }
    impl RestoreTestEntry for RedbMetaEngine {
        fn restore_test_entry(
            &self,
            receipt: Ino,
            parent: Ino,
            name: &[u8],
            stage: &[u8],
            _unused: &[crate::trash::RetainedSourceFence],
        ) -> MetaResult<RetainedEntry> {
            let mut fences = Vec::new();
            if let Ok(root) = self.lookup(receipt, DATA_NAME) {
                let mut pending = vec![root.ino];
                let mut seen = HashSet::new();
                while let Some(ino) = pending.pop() {
                    if !seen.insert(ino) {
                        continue;
                    }
                    let attr = self.get_attr(ino)?;
                    fences.push(crate::trash::RetainedSourceFence {
                        ino,
                        ctime: attr.ctime,
                        size: attr.size,
                        content_epoch: attr.content_epoch,
                    });
                    if attr.kind == FileType::Directory {
                        pending.extend(
                            self.readdir(ino)?
                                .into_iter()
                                .filter(|e| e.name != b"." && e.name != b"..")
                                .map(|e| e.ino),
                        );
                    }
                }
            }
            self.restore_retained_entry(receipt, parent, name, stage, &fences)
                .map(|result| result.entry)
        }
    }

    #[test]
    fn retained_entry_restart_hardlinks_collision_and_restore_retry() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-restore-");
        let path = temp.path().join("db");
        let engine = engine(&path);
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let parent = engine.create_dir(ROOT_INO, b"parent", 0, 0, 0o700).unwrap();
        let source = engine
            .create_file(parent.ino, b"source", 0, 0, 0o600)
            .unwrap();
        engine.link(source.ino, parent.ino, b"live-link").unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let entry = engine
            .retain_namespace_entry(parent.ino, b"source", store.ino, receipt)
            .unwrap();
        assert_eq!(entry.original_parent, parent.ino);
        assert_eq!(entry.original_name, b"source");
        assert_eq!(entry.inode, source.ino);
        assert!(engine.lookup(parent.ino, b"source").is_err());
        assert_eq!(engine.get_attr(source.ino).unwrap().nlink, 2);
        assert_eq!(engine.lookup(receipt, DATA_NAME).unwrap().ino, source.ino);
        drop(engine);
        let engine = RedbMetaEngine::open(&path).unwrap();
        assert_eq!(engine.retained_entry(receipt).unwrap(), Some(entry.clone()));
        let collision = engine
            .create_file(parent.ino, b"source", 0, 0, 0o600)
            .unwrap();
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert!(matches!(
            engine.restore_test_entry(receipt, parent.ino, b"source", b"stage", &[]),
            Err(MetaError::EntryExists { .. })
        ));
        assert_eq!(
            engine.lookup(parent.ino, b"source").unwrap().ino,
            collision.ino
        );
        assert_eq!(engine.retained_entry(receipt).unwrap(), Some(entry));
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
        let staged = engine.create_file(receipt, b"stage", 0, 0, 0o600).unwrap();
        let restored = engine
            .restore_test_entry(receipt, parent.ino, b"restored", b"stage", &[])
            .unwrap();
        assert_eq!(
            engine.lookup(parent.ino, b"restored").unwrap().ino,
            source.ino
        );
        assert_eq!(
            engine.lookup(parent.ino, b"live-link").unwrap().ino,
            source.ino
        );
        assert_eq!(engine.get_attr(source.ino).unwrap().nlink, 2);
        assert!(engine.get_attr(staged.ino).is_err());
        assert!(engine.lookup(receipt, DATA_NAME).is_err());
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            engine
                .restore_test_entry(receipt, parent.ino, b"restored", b"stage", &[])
                .unwrap(),
            restored
        );
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
    }

    #[test]
    fn retained_restore_rejects_stage_hardlink_to_doomed_payload() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-stage-hardlink-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let source = engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let private = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"source", private.ino, receipt)
            .unwrap();
        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        engine.link(source.ino, stage.ino, b"alias").unwrap();

        assert!(matches!(
            engine.restore_test_entry(receipt, ROOT_INO, b"restored", b"stage", &[]),
            Err(MetaError::Storage(message)) if message.contains("stage")
        ));
        assert_eq!(
            engine.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Retained
        );
        assert!(engine.lookup(ROOT_INO, b"restored").is_err());
        assert_eq!(engine.lookup(receipt, b"data").unwrap().ino, source.ino);
        assert_eq!(engine.lookup(receipt, b"stage").unwrap().ino, stage.ino);
    }

    #[test]
    fn retained_entry_journal_failure_rolls_back_receipt_and_move() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-rollback-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let source = engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .set_counter(counters::NEXT_EVENT_SEQ, u64::MAX)
            .unwrap();
        assert!(
            engine
                .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
                .is_err()
        );
        assert_eq!(engine.lookup(ROOT_INO, b"source").unwrap().ino, source.ino);
        assert!(engine.get_attr(receipt).is_err());
        assert!(
            engine
                .lookup(store.ino, receipt.to_string().as_bytes())
                .is_err()
        );
    }

    #[test]
    fn retained_entry_disabled_policy_expires_with_last_positive_period_without_reclaiming() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-expiry-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let entry = engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        engine.compare_and_set_trash_days(1, 0).unwrap().unwrap();
        let deadline = entry.deleted_at_unix_secs + 7 * 86_400;
        assert!(
            engine
                .request_retained_purge_at(
                    receipt,
                    RetainedPurgeReason::Expired {
                        settings_revision: 2
                    },
                    deadline - 1
                )
                .is_err()
        );
        assert_eq!(engine.retained_entry(receipt).unwrap(), Some(entry));
        // Due, but judged under a superseded period: the local request is refused too.
        assert!(
            engine
                .request_retained_purge_at(
                    receipt,
                    RetainedPurgeReason::Expired {
                        settings_revision: 1
                    },
                    deadline
                )
                .is_err()
        );
        let requested = engine
            .request_retained_purge_at(
                receipt,
                RetainedPurgeReason::Expired {
                    settings_revision: 2,
                },
                deadline,
            )
            .unwrap();
        assert!(matches!(
            requested.state,
            RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::Expired {
                    settings_revision: 2
                }
            }
        ));
        assert_eq!(engine.lookup(receipt, DATA_NAME).unwrap().ino, payload.ino);
        assert_eq!(engine.get_attr(payload.ino).unwrap().nlink, 1);
        assert!(
            engine
                .restore_test_entry(receipt, ROOT_INO, b"restored", b"stage", &[])
                .is_err()
        );
    }

    #[test]
    fn retained_entry_concurrent_local_restore_and_purge_have_one_transition() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-race-");
        let engine = Arc::new(engine(&temp.path().join("db")));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        let staged = engine.create_file(receipt, b"stage", 0, 0, 0o600).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let restore = {
            let engine = engine.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                engine.restore_test_entry(receipt, ROOT_INO, b"restored", b"stage", &[])
            })
        };
        let purge = {
            let engine = engine.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                engine.request_retained_purge(receipt, RetainedPurgeReason::User)
            })
        };
        barrier.wait();
        let restored = restore.join().unwrap();
        let purged = purge.join().unwrap();
        assert_ne!(restored.is_ok(), purged.is_ok());
        if restored.is_ok() {
            assert_eq!(
                engine.lookup(ROOT_INO, b"restored").unwrap().ino,
                payload.ino
            );
            assert_eq!(engine.get_attr(payload.ino).unwrap().nlink, 1);
            assert!(engine.get_attr(staged.ino).is_err());
        } else {
            assert_eq!(engine.lookup(receipt, DATA_NAME).unwrap().ino, payload.ino);
            assert_eq!(engine.get_attr(payload.ino).unwrap().nlink, 1);
        }
    }

    #[test]
    fn retained_entry_existing_replay_and_snapshot_transport_keep_receipt_and_reference() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-replay-");
        let source = engine(&temp.path().join("source"));
        source.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = source
            .create_dir(ROOT_INO, b"directory", 0, 0, 0o700)
            .unwrap();
        let child = source
            .create_dir(payload.ino, b"child", 0, 0, 0o700)
            .unwrap();
        let store = source
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = source.next_inode().unwrap();
        let expected = source
            .retain_namespace_entry(ROOT_INO, b"directory", store.ino, receipt)
            .unwrap();
        let replica = engine(&temp.path().join("replica"));
        let events = source.read_events_range(0, u64::MAX).unwrap();
        replica.apply_replica_events_to_base(&events).unwrap();
        replica.apply_replica_aux_state(&events).unwrap();
        replica.apply_replica_serving_state(&events).unwrap();
        assert_eq!(
            replica.retained_entry(receipt).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            replica.lookup(payload.ino, b"child").unwrap().ino,
            child.ino
        );
        let snapshot = replica.export_remote_snapshot().unwrap();
        let bootstrap = engine(&temp.path().join("bootstrap"));
        bootstrap.import_remote_snapshot(&snapshot).unwrap();
        assert_eq!(bootstrap.retained_entry(receipt).unwrap(), Some(expected));
        assert_eq!(
            bootstrap.lookup(receipt, DATA_NAME).unwrap().ino,
            payload.ino
        );
        assert_eq!(
            bootstrap.lookup(payload.ino, b"child").unwrap().ino,
            child.ino
        );
        let checkpoint = replica.export_serving_checkpoint_bytes().unwrap();
        let thin = engine(&temp.path().join("thin-checkpoint"));
        thin.import_serving_checkpoint_bytes(&checkpoint).unwrap();
        assert_eq!(
            thin.retained_entry(receipt).unwrap(),
            bootstrap.retained_entry(receipt).unwrap()
        );
        assert_eq!(thin.lookup(receipt, DATA_NAME).unwrap().ino, payload.ino);
    }

    #[test]
    fn retained_checkpoint_exports_shared_winner_not_local_losing_record() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-checkpoint-");
        let loser = engine(&temp.path().join("loser"));
        loser.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        loser
            .create_dir(ROOT_INO, b"directory", 0, 0, 0o700)
            .unwrap();
        let store = loser.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = loser.next_inode().unwrap();
        let retained = loser
            .retain_namespace_entry(ROOT_INO, b"directory", store.ino, receipt)
            .unwrap();
        // Everything up to the admission is shared base.
        let admitted = loser.read_events_range(0, u64::MAX).unwrap();
        loser.apply_replica_events_to_base(&admitted).unwrap();
        loser.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        let local = loser
            .restore_test_entry(receipt, ROOT_INO, b"restored", b"stage", &[])
            .unwrap();
        // A peer's purge won shared authority and replayed into base.
        let winner = RetainedEntry {
            state: RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::User,
            },
            revision: 1,
            ..retained
        };
        let foreign = [crate::MetaEvent::SetXattr {
            ino: receipt,
            name: RECEIPT_XATTR.to_vec(),
            value: serde_json::to_vec(&winner).unwrap(),
            flags: 0,
        }];
        // PG foreign apply keeps a locally journaled overlay, so the winner
        // reaches base only (see test_pg_retained_restore_purge_conflict_*).
        loser.apply_replica_events_to_base(&foreign).unwrap();
        // The accepted local outcome stays visible to this client.
        assert_eq!(loser.retained_entry(receipt).unwrap(), Some(local));

        let checkpoint = loser.export_serving_checkpoint_bytes().unwrap();
        let thin = engine(&temp.path().join("thin"));
        thin.import_serving_checkpoint_bytes(&checkpoint).unwrap();
        assert_eq!(thin.retained_entry(receipt).unwrap(), Some(winner.clone()));
        let full = engine(&temp.path().join("full"));
        full.import_remote_snapshot(&loser.export_remote_snapshot().unwrap())
            .unwrap();
        assert_eq!(full.retained_entry(receipt).unwrap(), Some(winner));
    }

    #[test]
    fn retained_snapshot_exports_shared_winner_over_overlay_receipt_tombstone() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-tombstone-");
        let local = engine(&temp.path().join("local"));
        local.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        local
            .create_dir(ROOT_INO, b"directory", 0, 0, 0o700)
            .unwrap();
        let store = local.create_dir(ROOT_INO, b"private", 0, 0, 0o700).unwrap();
        let receipt = local.next_inode().unwrap();
        let retained = local
            .retain_namespace_entry(ROOT_INO, b"directory", store.ino, receipt)
            .unwrap();
        let admitted = local.read_events_range(0, u64::MAX).unwrap();
        local.apply_replica_events_to_base(&admitted).unwrap();
        let winner = RetainedEntry {
            state: RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::User,
            },
            revision: 1,
            ..retained
        };
        local
            .apply_replica_events_to_base(&[crate::MetaEvent::SetXattr {
                ino: receipt,
                name: RECEIPT_XATTR.to_vec(),
                value: serde_json::to_vec(&winner).unwrap(),
                flags: 0,
            }])
            .unwrap();
        // The public API refuses this; only a legacy or forged overlay has it.
        let txn = local.begin_write_txn().unwrap();
        RedbMetaEngine::remove_xattr_overlay_in_txn(&txn, receipt, RECEIPT_XATTR).unwrap();
        txn.commit().unwrap();
        assert_eq!(local.retained_entry(receipt).unwrap(), None);

        let full = engine(&temp.path().join("full"));
        full.import_remote_snapshot(&local.export_remote_snapshot().unwrap())
            .unwrap();
        assert_eq!(full.retained_entry(receipt).unwrap(), Some(winner));
    }

    #[test]
    fn retained_receipt_xattr_rejects_user_set_and_remove() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-reserved-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let forged = engine
            .create_file(ROOT_INO, b"forged", 0, 0, 0o600)
            .unwrap();
        engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let entry = engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        let record = serde_json::to_vec(&entry).unwrap();
        for result in [
            engine.set_xattr(forged.ino, RECEIPT_XATTR, &record, 0),
            engine.set_xattr(receipt, RECEIPT_XATTR, &record, 0),
            engine.remove_xattr(receipt, RECEIPT_XATTR),
        ] {
            assert!(
                matches!(result, Err(MetaError::AccessDenied { .. })),
                "{result:?}"
            );
        }
        assert_eq!(engine.retained_entry(receipt).unwrap(), Some(entry));
        assert_eq!(engine.get_xattr(forged.ino, RECEIPT_XATTR).unwrap(), None);
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
        // Ordinary names on the receipt stay user-managed.
        engine.set_xattr(receipt, b"user.note", b"v", 0).unwrap();
        engine.remove_xattr(receipt, b"user.note").unwrap();
    }

    /// Retain `name` under ROOT and request its purge locally.
    fn purge_requested(engine: &RedbMetaEngine, store: Ino, name: &[u8]) -> RetainedEntry {
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, name, store, receipt)
            .unwrap();
        engine
            .request_retained_purge(receipt, RetainedPurgeReason::User)
            .unwrap()
    }

    /// Shared acknowledgement: replay the whole local journal into base.
    fn acknowledge(engine: &RedbMetaEngine) {
        let events = engine.read_events_range(0, u64::MAX).unwrap();
        engine.apply_replica_events_to_base(&events).unwrap();
    }

    #[test]
    fn retained_settlement_waits_for_shared_ack_then_unlinks_only_the_data_tree() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-settle-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let tree = engine.create_dir(ROOT_INO, b"tree", 0, 0, 0o700).unwrap();
        let nested = engine.create_dir(tree.ino, b"nested", 0, 0, 0o700).unwrap();
        let leaf = engine
            .create_file(nested.ino, b"leaf", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let entry = purge_requested(&engine, store.ino, b"tree");
        let stage = engine
            .create_dir(entry.receipt, b"restore-999", 0, 0, 0o700)
            .unwrap();
        let stage_child = engine
            .create_file(stage.ino, b"staged-leaf", 0, 0, 0o600)
            .unwrap();
        let never_open = |_| false;

        // A local purge request alone never authorizes settlement.
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            engine
                .settle_retained_purge(entry.receipt, never_open)
                .unwrap(),
            RetainedSettlement::NotAcknowledged
        );
        assert_eq!(
            engine.lookup(entry.receipt, DATA_NAME).unwrap().ino,
            tree.ino
        );
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);

        acknowledge(&engine);
        assert_eq!(
            engine
                .settle_retained_purge(entry.receipt, never_open)
                .unwrap(),
            RetainedSettlement::Settled
        );
        assert!(engine.lookup(entry.receipt, DATA_NAME).is_err());
        for ino in [tree.ino, nested.ino, leaf.ino] {
            assert!(
                engine.get_attr(ino).is_err(),
                "inode {ino} must be unlinked"
            );
        }
        // Settlement must never remove stage directories inside the receipt.
        assert_eq!(
            engine.lookup(entry.receipt, b"restore-999").unwrap().ino,
            stage.ino
        );
        assert_eq!(
            engine.lookup(stage.ino, b"staged-leaf").unwrap().ino,
            stage_child.ino
        );
        // The receipt and its record stay as the shared tombstone.
        assert_eq!(
            engine.retained_entry(entry.receipt).unwrap(),
            Some(entry.clone())
        );
        let name = entry.receipt.to_string();
        assert_eq!(
            engine.lookup(store.ino, name.as_bytes()).unwrap().ino,
            entry.receipt
        );
        let settled = engine
            .read_events_range(journal.len() as u64, u64::MAX)
            .unwrap();
        assert!(settled.iter().any(|event| matches!(
            event,
            crate::MetaEvent::RemoveEntry { parent, name } if *parent == entry.receipt && name == DATA_NAME
        )));
        assert!(!settled.iter().any(|event| matches!(
            event,
            crate::MetaEvent::RemoveAttr { ino } if *ino == entry.receipt
        ) || matches!(
            event,
            crate::MetaEvent::RemoveXattr { .. }
        )));

        // Idempotent: a repeated sweep finds nothing and journals nothing.
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            engine
                .settle_retained_purge(entry.receipt, never_open)
                .unwrap(),
            RetainedSettlement::NothingToSettle
        );
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
    }

    #[test]
    fn retained_settlement_keeps_hard_links_and_defers_open_inodes() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-settle-links-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let linked = engine
            .create_file(ROOT_INO, b"linked", 0, 0, 0o600)
            .unwrap();
        engine.link(linked.ino, ROOT_INO, b"live-link").unwrap();
        let open = engine.create_file(ROOT_INO, b"open", 0, 0, 0o600).unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let linked_entry = purge_requested(&engine, store.ino, b"linked");
        let open_entry = purge_requested(&engine, store.ino, b"open");
        acknowledge(&engine);

        assert_eq!(
            engine
                .settle_retained_purge(linked_entry.receipt, |_| false)
                .unwrap(),
            RetainedSettlement::Settled
        );
        assert_eq!(
            engine.lookup(ROOT_INO, b"live-link").unwrap().ino,
            linked.ino
        );
        assert_eq!(engine.get_attr(linked.ino).unwrap().nlink, 1);

        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            engine
                .settle_retained_purge(open_entry.receipt, |ino| ino == open.ino)
                .unwrap(),
            RetainedSettlement::Busy
        );
        assert_eq!(
            engine.lookup(open_entry.receipt, DATA_NAME).unwrap().ino,
            open.ino
        );
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
        // After the last handle closes, a later sweep settles it.
        assert_eq!(
            engine
                .settle_retained_purge(open_entry.receipt, |_| false)
                .unwrap(),
            RetainedSettlement::Settled
        );
        assert!(engine.get_attr(open.ino).is_err());
    }

    #[test]
    fn retained_settlement_never_unlinks_an_inode_a_losing_restore_moved() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-settle-loser-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let payload = engine.create_dir(ROOT_INO, b"source", 0, 0, 0o700).unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let retained = engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        acknowledge(&engine);
        // Accepted offline restore, then the peer's purge wins and replays.
        let staged = engine.create_dir(receipt, b"stage", 0, 0, 0o700).unwrap();
        engine
            .restore_test_entry(receipt, ROOT_INO, b"restored", b"stage", &[])
            .unwrap();
        assert!(engine.get_attr(staged.ino).is_err());
        let winner = RetainedEntry {
            state: RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::Expired {
                    settings_revision: 2,
                },
            },
            revision: 1,
            ..retained
        };
        engine
            .apply_replica_events_to_base(&[crate::MetaEvent::SetXattr {
                ino: receipt,
                name: RECEIPT_XATTR.to_vec(),
                value: serde_json::to_vec(&winner).unwrap(),
                flags: 0,
            }])
            .unwrap();
        // A changed `data` binding (same name, other inode) is not settled either.
        let decoy = engine.create_dir(receipt, DATA_NAME, 0, 0, 0o700).unwrap();
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            engine.settle_retained_purge(receipt, |_| false).unwrap(),
            RetainedSettlement::NothingToSettle
        );
        assert_eq!(
            engine.lookup(ROOT_INO, b"restored").unwrap().ino,
            payload.ino
        );
        assert_eq!(engine.lookup(receipt, DATA_NAME).unwrap().ino, decoy.ino);
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
    }

    #[test]
    fn retained_receipt_rejects_rmdir_and_displacing_rename() {
        let temp = studiofs_test_support::workspace_target_tempdir("retained-receipt-remove-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o600)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let name = receipt.to_string();
        engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        // Restore leaves the receipt directory empty.
        engine.create_file(receipt, b"stage", 0, 0, 0o600).unwrap();
        let restored = engine
            .restore_test_entry(receipt, ROOT_INO, b"restored", b"stage", &[])
            .unwrap();
        let empty = engine.create_dir(store.ino, b"empty", 0, 0, 0o700).unwrap();
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        for result in [
            engine.rmdir(store.ino, name.as_bytes()).map(drop),
            engine.unlink(store.ino, name.as_bytes()).map(drop),
            engine
                .rename_atomic(store.ino, b"empty", store.ino, name.as_bytes(), true, false)
                .map(drop),
        ] {
            assert!(
                matches!(result, Err(MetaError::AccessDenied { .. })),
                "{result:?}"
            );
        }
        assert_eq!(engine.retained_entry(receipt).unwrap(), Some(restored));
        assert_eq!(
            engine.lookup(store.ino, name.as_bytes()).unwrap().ino,
            receipt
        );
        assert_eq!(engine.lookup(store.ino, b"empty").unwrap().ino, empty.ino);
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
        // Ordinary entries in the store keep their namespace semantics.
        engine.rmdir(store.ino, b"empty").unwrap();
    }

    #[test]
    fn reconcile_losing_retained_restore_later_name_owner_preserves_new_owner_and_nlink() {
        let temp = studiofs_test_support::workspace_target_tempdir("reconcile-later-owner-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let winner_parent = engine
            .create_dir(ROOT_INO, b"winner-p", 0, 0, 0o755)
            .unwrap();
        let loser_parent = engine
            .create_dir(ROOT_INO, b"loser-p", 0, 0, 0o755)
            .unwrap();
        let source = engine.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let retained = engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        acknowledge(&engine);
        let unpublished_start = engine.read_events_range(0, u64::MAX).unwrap().len() as u64 + 1;

        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
        let loser = engine
            .restore_test_entry(receipt, loser_parent.ino, b"alias", b"stage", &[])
            .unwrap();
        assert!(engine.get_attr(stage.ino).is_err());

        // Later explicit operations replace the losing (loser_parent, "alias") slot.
        engine.rmdir(loser_parent.ino, b"alias").unwrap();
        let replacement = engine
            .create_dir(loser_parent.ino, b"alias", 0, 0, 0o755)
            .unwrap();
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 3);

        let winner = RetainedEntry {
            state: RetainedEntryState::Restored {
                parent: winner_parent.ino,
                name: b"winner".to_vec(),
            },
            revision: 1,
            ..retained
        };
        engine
            .reconcile_losing_retained_restore(&loser, &winner, unpublished_start, &HashMap::new())
            .unwrap();

        assert_eq!(
            engine.lookup(loser_parent.ino, b"alias").unwrap().ino,
            replacement.ino
        );
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 3);
        assert_eq!(
            engine.retained_entry(receipt).unwrap(),
            Some(winner.clone())
        );

        // Reconciling a file with a post-move rename over the same losing name must also preserve the replacement.
        let other_dir = engine.create_dir(ROOT_INO, b"other", 0, 0, 0o755).unwrap();
        let f_source = engine.create_file(ROOT_INO, b"fsrc", 0, 0, 0o644).unwrap();
        let f_receipt = engine.next_inode().unwrap();
        let f_retained = engine
            .retain_namespace_entry(ROOT_INO, b"fsrc", store.ino, f_receipt)
            .unwrap();
        let f_stage = engine
            .create_file(f_receipt, b"fstage", 0, 0, 0o644)
            .unwrap();
        let f_loser = engine
            .restore_test_entry(f_receipt, loser_parent.ino, b"falias", b"fstage", &[])
            .unwrap();
        let _ = f_stage;
        engine
            .rename_atomic(
                loser_parent.ino,
                b"falias",
                other_dir.ino,
                b"moved-falias",
                false,
                false,
            )
            .unwrap();
        let f_replacement = engine
            .create_file(loser_parent.ino, b"falias", 0, 0, 0o644)
            .unwrap();
        let f_winner = RetainedEntry {
            state: RetainedEntryState::Restored {
                parent: winner_parent.ino,
                name: b"fwinner".to_vec(),
            },
            revision: 1,
            ..f_retained
        };
        engine
            .reconcile_losing_retained_restore(
                &f_loser,
                &f_winner,
                unpublished_start,
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            engine.lookup(loser_parent.ino, b"falias").unwrap().ino,
            f_replacement.ino
        );
        assert_eq!(
            engine.lookup(other_dir.ino, b"moved-falias").unwrap().ino,
            f_source.ino
        );
        let _ = source;
    }

    #[test]
    fn reconcile_losing_retained_restore_boundaries_adjust_nlink_keep_links_and_are_idempotent() {
        let temp = studiofs_test_support::workspace_target_tempdir("reconcile-boundaries-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let winner_parent = engine
            .create_dir(ROOT_INO, b"winner-p", 0, 0, 0o755)
            .unwrap();
        let loser_parent = engine
            .create_dir(ROOT_INO, b"loser-p", 0, 0, 0o755)
            .unwrap();
        let source = engine.create_dir(ROOT_INO, b"source", 0, 0, 0o755).unwrap();
        let file = engine
            .create_file(source.ino, b"file", 0, 0, 0o644)
            .unwrap();
        engine.link(file.ino, ROOT_INO, b"outside").unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        let retained = engine
            .retain_namespace_entry(ROOT_INO, b"source", store.ino, receipt)
            .unwrap();
        acknowledge(&engine);
        let events_before_restore = engine.read_events_range(0, u64::MAX).unwrap().len() as u64;

        let stage = engine.create_dir(receipt, b"stage", 0, 0, 0o755).unwrap();
        engine.create_file(stage.ino, b"file", 0, 0, 0o644).unwrap();
        let loser = engine
            .restore_test_entry(receipt, loser_parent.ino, b"loser", b"stage", &[])
            .unwrap();
        let restore_end_seq = engine.read_events_range(0, u64::MAX).unwrap().len() as u64;

        // Post-restore hardlink and child directory under loser_parent.
        engine.link(file.ino, ROOT_INO, b"later-link").unwrap();
        let after_dir = engine
            .create_dir(loser_parent.ino, b"after-dir", 0, 0, 0o755)
            .unwrap();
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 4);

        let winner = RetainedEntry {
            state: RetainedEntryState::Restored {
                parent: winner_parent.ino,
                name: b"winner".to_vec(),
            },
            revision: 1,
            ..retained
        };

        // unpublished_start past the losing SetXattr must be a safe no-op.
        engine
            .reconcile_losing_retained_restore(
                &loser,
                &winner,
                restore_end_seq + 1,
                &HashMap::new(),
            )
            .unwrap();
        assert!(engine.lookup(loser_parent.ino, b"loser").is_ok());
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 4);

        // Correct unpublished_start reconciles local serving overlay, parent nlink, and later journal SetAttrs.
        let unpublished_start = events_before_restore + 1;
        engine
            .reconcile_losing_retained_restore(&loser, &winner, unpublished_start, &HashMap::new())
            .unwrap();
        assert!(engine.lookup(loser_parent.ino, b"loser").is_err());
        assert_eq!(
            engine.lookup(loser_parent.ino, b"after-dir").unwrap().ino,
            after_dir.ino
        );
        assert_eq!(engine.lookup(ROOT_INO, b"outside").unwrap().ino, file.ino);
        assert_eq!(
            engine.lookup(ROOT_INO, b"later-link").unwrap().ino,
            file.ino
        );
        assert_eq!(engine.get_attr(file.ino).unwrap().nlink, 3);
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 3);
        assert_eq!(
            engine.get_attr(source.ino).unwrap().parent,
            winner_parent.ino
        );
        assert_eq!(
            engine.retained_entry(receipt).unwrap(),
            Some(winner.clone())
        );

        let journal_after = engine
            .read_events_range(unpublished_start, u64::MAX)
            .unwrap();
        assert!(
            !journal_after.iter().any(|ev| matches!(
                ev,
                crate::MetaEvent::SetAttr { attr }
                    if attr.ino == loser_parent.ino && attr.nlink > 3
            )),
            "later journal SetAttr for loser_parent must not keep stale inflated nlink: {journal_after:?}"
        );

        // Second call is completely idempotent (does not double-decrement parent nlink).
        engine
            .reconcile_losing_retained_restore(&loser, &winner, unpublished_start, &HashMap::new())
            .unwrap();
        assert_eq!(engine.get_attr(loser_parent.ino).unwrap().nlink, 3);
        assert_eq!(
            engine
                .read_events_range(unpublished_start, u64::MAX)
                .unwrap(),
            journal_after
        );
    }

    #[test]
    fn retained_restore_receipt_marker_precedes_adopted_payload_journal() {
        use crate::schema::{
            ChunkId, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
        };

        let temp = studiofs_test_support::workspace_target_tempdir("retained-marker-order-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let file = engine
            .create_file(ROOT_INO, b"source", 0, 0, 0o644)
            .unwrap();
        let mut file_attr = engine.get_attr(file.ino).unwrap();
        file_attr.size = 64;
        file_attr.content_epoch = 1;
        engine.set_attr(&file_attr).unwrap();
        let private = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"source", private.ino, receipt)
            .unwrap();
        let file_attr = engine.get_attr(file.ino).unwrap();
        let unpublished_start = engine.read_events_range(0, u64::MAX).unwrap().len() as u64 + 1;

        let stage = engine.create_file(receipt, b"stage", 0, 0, 0o644).unwrap();
        let mut stage_attr = engine.get_attr(stage.ino).unwrap();
        stage_attr.size = 64;
        stage_attr.content_epoch = 1;
        engine.set_attr(&stage_attr).unwrap();
        let mut slice = SliceRecord::new(
            SliceId(0),
            SliceOrder(0),
            SliceRange::new(0, 64).unwrap(),
            crate::schema::PayloadRef::new_partial(50_001, 64),
        );
        slice.payload_state = PayloadRefState::LocalOnly;
        engine
            .flush_regular_file_overlay_slices(stage.ino, &[(ChunkId(0), vec![slice])])
            .unwrap();

        engine
            .restore_retained_entry(
                receipt,
                ROOT_INO,
                b"restored",
                b"stage",
                &[crate::trash::RetainedSourceFence {
                    ino: file.ino,
                    ctime: file_attr.ctime,
                    size: file_attr.size,
                    content_epoch: file_attr.content_epoch,
                }],
            )
            .unwrap();

        let events = engine
            .read_events_range(unpublished_start, u64::MAX)
            .unwrap();
        let marker = events
            .iter()
            .position(|event| matches!(event,
                crate::MetaEvent::SetXattr { ino, name, value, .. }
                    if *ino == receipt
                        && name == RECEIPT_XATTR
                        && RedbMetaEngine::decode_retained_entry(*ino, value)
                            .is_ok_and(|entry| matches!(entry.state, RetainedEntryState::Restored { .. }))))
            .expect("restore receipt journal marker");
        let adopted_payload = events
            .iter()
            .position(|event| {
                matches!(event,
                crate::MetaEvent::SetRegularFileOverlaySlices { ino, .. } if *ino == file.ino)
            })
            .expect("adopted source payload journal");
        assert!(
            marker < adopted_payload,
            "losing-restore range must include adoption events: marker={marker}, payload={adopted_payload}"
        );
    }

    /// A flush through an unlinked handle rewrites `nlink=1`; the guard must
    /// still finalize it, while a hard link or a receipt payload stays live.
    #[test]
    fn orphan_guard_decides_liveness_from_bindings_not_nlink() {
        use crate::engine::OrphanInodeFinalization;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-orphan-binding-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();

        let orphan = engine
            .create_file(ROOT_INO, b"orphan", 0, 0, 0o644)
            .unwrap();
        engine.unlink(ROOT_INO, b"orphan").unwrap();
        engine
            .flush_regular_file_overlay_slices_with_attr(orphan.ino, &[], Some(&orphan))
            .unwrap();
        assert_eq!(engine.get_attr(orphan.ino).unwrap().nlink, 1);
        assert!(matches!(
            engine
                .finalize_orphan_inode_guarded(orphan.ino, 1, None)
                .unwrap(),
            OrphanInodeFinalization::Removed { .. }
        ));
        assert!(engine.get_attr(orphan.ino).is_err());

        let linked = engine.create_file(ROOT_INO, b"a", 0, 0, 0o644).unwrap();
        engine.link(linked.ino, ROOT_INO, b"b").unwrap();
        engine.unlink(ROOT_INO, b"a").unwrap();
        assert_eq!(
            engine
                .finalize_orphan_inode_guarded(linked.ino, 1, None)
                .unwrap(),
            OrphanInodeFinalization::Live
        );

        let kept = engine.create_file(ROOT_INO, b"kept", 0, 0, 0o644).unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"kept", store.ino, receipt)
            .unwrap();
        assert_eq!(engine.lookup(receipt, b"data").unwrap().ino, kept.ino);
        assert_eq!(
            engine
                .finalize_orphan_inode_guarded(kept.ino, 1, None)
                .unwrap(),
            OrphanInodeFinalization::Live
        );
    }

    /// PG classifies a losing child purge only from its exact journal span, so
    /// every delete shape the purge can emit must be recognized end to end.
    #[test]
    fn orphan_guard_scans_unacked_purges_once_per_pass_and_skips_when_acked() {
        use crate::engine::OrphanInodeFinalization;
        use crate::trash::RetainedChildPurgeOutcome;
        const CHILDREN: usize = 300;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-orphan-pass-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = engine.create_dir(ROOT_INO, b"folder", 0, 0, 0o755).unwrap();
        let children: Vec<_> = (0..=CHILDREN)
            .map(|index| {
                let name = format!("f{index}").into_bytes();
                let ino = engine.create_file(dir.ino, &name, 0, 0, 0o644).unwrap().ino;
                (name, ino)
            })
            .collect();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"folder", store.ino, receipt)
            .unwrap();
        let (late, purged) = children.split_last().unwrap();
        for (name, ino) in purged {
            assert!(matches!(
                engine
                    .purge_retained_child(receipt, dir.ino, name, *ino, false)
                    .unwrap(),
                RetainedChildPurgeOutcome::Removed(_)
            ));
        }
        let journal = engine.read_events_range(1, u64::MAX).unwrap().len();
        let mut unacked = UnackedChildPurges::default();
        for (index, (_, ino)) in purged.iter().enumerate() {
            if index == CHILDREN / 2 {
                // A purge committed mid-pass is found by the next delta scan.
                assert!(matches!(
                    engine
                        .purge_retained_child(receipt, dir.ino, &late.0, late.1, false)
                        .unwrap(),
                    RetainedChildPurgeOutcome::Removed(_)
                ));
                assert_eq!(
                    engine
                        .finalize_orphan_inode_guarded(late.1, 1, Some(&mut unacked))
                        .unwrap(),
                    OrphanInodeFinalization::Deferred
                );
            }
            assert_eq!(
                engine
                    .finalize_orphan_inode_guarded(*ino, 1, Some(&mut unacked))
                    .unwrap(),
                OrphanInodeFinalization::Deferred
            );
        }
        let total = engine.read_events_range(1, u64::MAX).unwrap().len();
        assert!(
            unacked.decoded <= total,
            "one pass decodes each journal event at most once: {} > {total} (initial {journal})",
            unacked.decoded
        );

        // Past the ack boundary the guard skips the scan and finalizes.
        let end = engine
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        engine
            .set_counter(crate::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ, end)
            .unwrap();
        let mut acked = UnackedChildPurges::default();
        assert!(matches!(
            engine
                .finalize_orphan_inode_guarded(purged[0].1, 1, Some(&mut acked))
                .unwrap(),
            OrphanInodeFinalization::Removed { .. }
        ));
        assert_eq!(acked.decoded, 0, "an acknowledged journal is not scanned");
    }

    #[test]
    fn retained_child_purge_spans_are_recognized_for_every_delete_shape() {
        use crate::trash::RetainedChildPurgeOutcome;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-child-span-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = engine.create_dir(ROOT_INO, b"folder", 0, 0, 0o755).unwrap();
        let empty = engine.create_dir(dir.ino, b"empty", 0, 0, 0o755).unwrap();
        let draft = engine.create_file(dir.ino, b"draft", 0, 0, 0o644).unwrap();
        let linked = engine.create_file(dir.ino, b"linked", 0, 0, 0o644).unwrap();
        engine.link(linked.ino, ROOT_INO, b"outside").unwrap();
        let opened = engine.create_file(dir.ino, b"opened", 0, 0, 0o644).unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"folder", store.ino, receipt)
            .unwrap();
        // Every file purge is a deferred delete, open handle or not.
        for (name, ino, is_dir) in [
            (&b"empty"[..], empty.ino, true),
            (&b"draft"[..], draft.ino, false),
            (&b"linked"[..], linked.ino, false),
            (&b"opened"[..], opened.ino, false),
        ] {
            let start = engine
                .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                .unwrap();
            assert!(matches!(
                engine
                    .purge_retained_child(receipt, dir.ino, name, ino, is_dir)
                    .unwrap(),
                RetainedChildPurgeOutcome::Removed(_)
            ));
            let events = engine.read_events_range(start, u64::MAX).unwrap();
            let span = RedbMetaEngine::child_purge_event_span(&events, 0).unwrap_or_else(|| {
                panic!("unrecognized child purge span for {name:?}: {events:?}")
            });
            assert_eq!(span.receipt, receipt);
            assert_eq!(span.parent, dir.ino);
            assert_eq!(span.name, name);
            assert_eq!(span.child_ino, ino);
            assert_eq!(
                span.end + 1,
                events.len(),
                "span must cover the whole commit"
            );
            assert!(engine.lookup(dir.ino, name).is_err());
        }
        assert_eq!(engine.lookup(ROOT_INO, b"outside").unwrap().ino, linked.ino);
        assert_eq!(engine.get_attr(opened.ino).unwrap().nlink, 0);
        // A mismatched expected inode never deletes by name alone.
        let replacement = engine.create_file(dir.ino, b"draft", 0, 0, 0o644).unwrap();
        assert_eq!(
            engine
                .purge_retained_child(receipt, dir.ino, b"draft", draft.ino, false)
                .unwrap(),
            RetainedChildPurgeOutcome::TargetChanged
        );
        assert_eq!(
            engine.lookup(dir.ino, b"draft").unwrap().ino,
            replacement.ino
        );
    }

    /// Published-base standard delete, published base with a local overlay, and
    /// symlink purges emit the same recognizable spans as drafts and directories.
    #[test]
    fn retained_child_purge_spans_cover_published_base_overlay_and_symlink() {
        use crate::trash::RetainedChildPurgeOutcome;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-child-span-pub-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = engine.create_dir(ROOT_INO, b"folder", 0, 0, 0o755).unwrap();
        let set_raw = |attr: &InodeAttr| {
            let txn = engine.begin_write_txn().unwrap();
            RedbMetaEngine::set_inode_in_txn(&txn, attr).unwrap();
            txn.commit().unwrap();
        };
        let mut published = engine
            .create_file(dir.ino, b"published", 0, 0, 0o644)
            .unwrap();
        published.size = 4096;
        published.committed_size = 4096;
        published.content_epoch = 1;
        published.committed_content_epoch = 1;
        set_raw(&published);
        let mut overlaid = engine
            .create_file(dir.ino, b"overlaid", 0, 0, 0o644)
            .unwrap();
        overlaid.committed_size = 4096;
        overlaid.committed_content_epoch = 1;
        overlaid.size = 8192;
        overlaid.content_epoch = 2;
        set_raw(&overlaid);
        let link = engine
            .create_symlink(dir.ino, b"link", b"published", 0, 0)
            .unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"folder", store.ino, receipt)
            .unwrap();
        for (name, ino, base_rollback) in [
            (&b"published"[..], published.ino, false),
            (&b"overlaid"[..], overlaid.ino, true),
            (&b"link"[..], link.ino, false),
        ] {
            let start = engine
                .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                .unwrap();
            assert!(matches!(
                engine
                    .purge_retained_child(receipt, dir.ino, name, ino, false)
                    .unwrap(),
                RetainedChildPurgeOutcome::Removed(_)
            ));
            let events = engine.read_events_range(start, u64::MAX).unwrap();
            let span = RedbMetaEngine::child_purge_event_span(&events, 0).unwrap_or_else(|| {
                panic!("unrecognized child purge span for {name:?}: {events:?}")
            });
            assert_eq!(
                (span.receipt, span.parent, span.child_ino),
                (receipt, dir.ino, ino)
            );
            assert_eq!(span.name, name);
            assert_eq!(span.end + 1, events.len(), "span must cover the commit");
            assert_eq!(
                events.iter().any(|event| matches!(
                    event,
                    crate::events::MetaEvent::PublishRegularFileBaseSlices { .. }
                )),
                base_rollback,
                "{name:?}: only a published base with a local overlay rolls back to base"
            );
            assert!(engine.lookup(dir.ino, name).is_err());
        }
    }

    fn child_purge_span_count(engine: &RedbMetaEngine, receipt: Ino) -> usize {
        let events = engine.read_events_range(1, u64::MAX).unwrap();
        (0..events.len())
            .filter_map(|index| RedbMetaEngine::child_purge_event_span(&events, index))
            .filter(|span| span.receipt == receipt)
            .count()
    }

    fn terminal_winner(engine: &RedbMetaEngine, receipt: Ino) -> RetainedEntry {
        let mut winner = engine.retained_entry(receipt).unwrap().unwrap();
        winner.state = RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        };
        winner.revision = 1;
        winner
    }

    /// Reconcile retires exactly the spans the caller names: never another
    /// receipt's spans, an unrelated unlink, or an unnamed span on the same parent.
    #[test]
    fn retained_child_purge_reconcile_retires_only_selected_spans() {
        use crate::trash::RetainedChildPurgeOutcome;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-child-mixed-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let mut receipts = Vec::new();
        for folder in [&b"one"[..], b"two"] {
            let dir = engine.create_dir(ROOT_INO, folder, 0, 0, 0o755).unwrap();
            let a = engine.create_dir(dir.ino, b"a", 0, 0, 0o755).unwrap();
            let b = engine.create_dir(dir.ino, b"b", 0, 0, 0o755).unwrap();
            let receipt = engine.next_inode().unwrap();
            engine
                .retain_namespace_entry(ROOT_INO, folder, store.ino, receipt)
                .unwrap();
            receipts.push((receipt, dir.ino, a.ino, b.ino));
        }
        let plain = engine.create_dir(ROOT_INO, b"plain", 0, 0, 0o755).unwrap();
        let unrelated = engine.create_file(plain.ino, b"gone", 0, 0, 0o644).unwrap();
        let keep = engine.create_file(plain.ino, b"keep", 0, 0, 0o644).unwrap();
        let mut first_span_seq = None;
        for &(receipt, dir, a, b) in &receipts {
            for (name, ino) in [(&b"a"[..], a), (&b"b"[..], b)] {
                let seq = engine
                    .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
                    .unwrap();
                first_span_seq.get_or_insert(seq);
                assert!(matches!(
                    engine
                        .purge_retained_child(receipt, dir, name, ino, true)
                        .unwrap(),
                    RetainedChildPurgeOutcome::Removed(_)
                ));
            }
        }
        engine.unlink(plain.ino, b"gone").unwrap();
        let journal_before = engine.read_events_range(1, u64::MAX).unwrap();
        let (one, _, _, _) = receipts[0];
        let (two, _, _, _) = receipts[1];
        assert_eq!(child_purge_span_count(&engine, one), 2);

        // Retire only receipt one's first span ("a"); its sibling "b" shares the parent.
        let first_span_seq = first_span_seq.unwrap();
        engine
            .reconcile_losing_retained_child_purges(
                one,
                &terminal_winner(&engine, one),
                1,
                &|seq| seq == first_span_seq,
            )
            .unwrap();
        assert_eq!(child_purge_span_count(&engine, one), 1);
        assert_eq!(child_purge_span_count(&engine, two), 2);
        let journal_after = engine.read_events_range(1, u64::MAX).unwrap();
        assert_eq!(journal_before.len(), journal_after.len());
        let changed: Vec<usize> = (0..journal_before.len())
            .filter(|&index| journal_before[index] != journal_after[index])
            .collect();
        let first_index = usize::try_from(first_span_seq - 1).unwrap();
        assert_eq!(
            changed,
            (first_index..first_index + 4).collect::<Vec<_>>(),
            "only the selected span's four events may change"
        );
        assert!(engine.lookup(plain.ino, b"gone").is_err());
        assert_eq!(engine.lookup(plain.ino, b"keep").unwrap().ino, keep.ino);
        assert!(engine.get_attr(unrelated.ino).is_err());
    }

    /// Reconcile of a large partial Clear is one journal pass plus O(1) checks
    /// per span, so 10k spans finish inside the write transaction.
    #[test]
    fn retained_child_purge_reconcile_handles_ten_thousand_spans() {
        use crate::events::MetaEvent;
        const SPANS: u64 = 10_000;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-child-10k-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = engine.create_dir(ROOT_INO, b"folder", 0, 0, 0o755).unwrap();
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"folder", store.ino, receipt)
            .unwrap();
        let fence = serde_json::to_vec(&engine.retained_entry(receipt).unwrap().unwrap()).unwrap();
        let parent = engine.get_attr(dir.ino).unwrap();
        let start = engine
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        // The exact rmdir span shape `purge_retained_child` journals, written in one commit.
        let txn = engine.begin_write_txn().unwrap();
        for index in 0..SPANS {
            for event in [
                MetaEvent::SetXattr {
                    ino: receipt,
                    name: RECEIPT_XATTR.to_vec(),
                    value: fence.clone(),
                    flags: 0,
                },
                MetaEvent::SetAttr {
                    attr: parent.clone(),
                },
                MetaEvent::RemoveAttr {
                    ino: 1_000_000 + index,
                },
                MetaEvent::RemoveEntry {
                    parent: dir.ino,
                    name: format!("child-{index}").into_bytes(),
                },
            ] {
                RedbMetaEngine::append_event_in_txn(&txn, &event).unwrap();
            }
        }
        txn.commit().unwrap();
        assert_eq!(child_purge_span_count(&engine, receipt), 10_000);
        let started = std::time::Instant::now();
        engine
            .reconcile_losing_retained_child_purges(
                receipt,
                &terminal_winner(&engine, receipt),
                start,
                &|_| true,
            )
            .unwrap();
        eprintln!(
            "reconciled {SPANS} child purge spans in {:?}",
            started.elapsed()
        );
        assert_eq!(child_purge_span_count(&engine, receipt), 0);
        let noop = MetaEvent::RemoveLocalEntry {
            parent: receipt,
            name: Vec::new(),
        };
        let retired = engine
            .read_events_range(start, u64::MAX)
            .unwrap()
            .into_iter()
            .filter(|event| *event == noop)
            .count();
        assert_eq!(retired, 40_000);
    }

    /// A child whose overlay bytes are uploaded but unpublished queues their
    /// deletion markers inside the purge commit; the span must still be
    /// recognized, or a losing purge could never retire and would pin the journal.
    /// Retiring the span keeps the local marker: the purge already dropped the
    /// only local reference, and abandoned-upload retirement re-checks shared
    /// references in PG, so a published object stays protected.
    #[test]
    fn retained_child_purge_uploaded_overlay_span_is_recognized_and_retires() {
        use crate::trash::RetainedChildPurgeOutcome;
        let temp = studiofs_test_support::workspace_target_tempdir("retained-child-uploaded-");
        let engine = engine(&temp.path().join("db"));
        engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = engine.create_dir(ROOT_INO, b"folder", 0, 0, 0o755).unwrap();
        let mut attr = engine
            .create_file(dir.ino, b"uploaded", 0, 0, 0o644)
            .unwrap();
        attr.content_epoch = 7;
        attr.size = 4096;
        let object_id = 900_001;
        engine
            .flush_regular_file_overlay_slices_with_attr(
                attr.ino,
                &[(
                    ChunkId(0),
                    vec![SliceRecord::new(
                        SliceId::default(),
                        SliceOrder::default(),
                        SliceRange::new(0, 4096).unwrap(),
                        PayloadRef::new_partial(object_id, 4096),
                    )],
                )],
                Some(&attr),
            )
            .unwrap();
        assert!(
            engine
                .mark_regular_file_overlay_object_uploaded_if_matches(attr.ino, 7, object_id)
                .unwrap()
        );
        let store = engine
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let receipt = engine.next_inode().unwrap();
        engine
            .retain_namespace_entry(ROOT_INO, b"folder", store.ino, receipt)
            .unwrap();
        let start = engine
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        assert!(matches!(
            engine
                .purge_retained_child(receipt, dir.ino, b"uploaded", attr.ino, false)
                .unwrap(),
            RetainedChildPurgeOutcome::Removed(_)
        ));
        let pending = |engine: &RedbMetaEngine| {
            let txn = engine.begin_read_txn().unwrap();
            let table = txn.open_table(PENDING_DELETIONS_TABLE).unwrap();
            table.get(object_id).unwrap().is_some()
        };
        assert!(
            pending(&engine),
            "the purge queues the uploaded overlay object"
        );
        let events = engine.read_events_range(start, u64::MAX).unwrap();
        let span = RedbMetaEngine::child_purge_event_span(&events, 0).unwrap_or_else(|| {
            panic!("unrecognized uploaded-overlay child purge span: {events:?}")
        });
        assert_eq!((span.child_ino, span.end + 1), (attr.ino, events.len()));

        engine
            .reconcile_losing_retained_child_purges(
                receipt,
                &terminal_winner(&engine, receipt),
                start,
                &|_| true,
            )
            .unwrap();
        // The losing purge kept the graph as a deferred delete, so its accepted
        // overlay bytes stay referenced and its own GC marker is withdrawn.
        assert!(
            !pending(&engine),
            "a losing purge's marker must not let GC delete kept bytes"
        );
        assert!(
            engine
                .get_local_referenced_object_ids()
                .unwrap()
                .contains(&object_id),
            "the restored child still references its accepted overlay"
        );
        assert_eq!(child_purge_span_count(&engine, receipt), 0);
    }
}
