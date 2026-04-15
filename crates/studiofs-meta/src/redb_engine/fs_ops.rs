use super::*;

pub(super) fn current_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(super) enum NamespaceCreateKind<'a> {
    File { mode: u16 },
    Directory { mode: u16 },
    Symlink { target: &'a [u8] },
}

pub(super) struct NamespaceCreateSpec<'a> {
    ino: Ino,
    parent: Ino,
    name: &'a [u8],
    uid: u32,
    gid: u32,
    created_at: i64,
    kind: NamespaceCreateKind<'a>,
}

impl<'a> NamespaceCreateSpec<'a> {
    pub(super) fn new_file(
        ino: Ino,
        parent: Ino,
        name: &'a [u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Self {
        Self {
            ino,
            parent,
            name,
            uid,
            gid,
            created_at: current_unix_seconds(),
            kind: NamespaceCreateKind::File { mode },
        }
    }

    pub(super) fn new_dir(
        ino: Ino,
        parent: Ino,
        name: &'a [u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Self {
        Self {
            ino,
            parent,
            name,
            uid,
            gid,
            created_at: current_unix_seconds(),
            kind: NamespaceCreateKind::Directory { mode },
        }
    }

    pub(super) fn new_symlink(
        ino: Ino,
        parent: Ino,
        name: &'a [u8],
        target: &'a [u8],
        uid: u32,
        gid: u32,
    ) -> Self {
        Self {
            ino,
            parent,
            name,
            uid,
            gid,
            created_at: current_unix_seconds(),
            kind: NamespaceCreateKind::Symlink { target },
        }
    }

    pub(super) fn kind(&self) -> FileType {
        match self.kind {
            NamespaceCreateKind::File { .. } => FileType::RegularFile,
            NamespaceCreateKind::Directory { .. } => FileType::Directory,
            NamespaceCreateKind::Symlink { .. } => FileType::Symlink,
        }
    }

    pub(super) fn updates_parent_attr(&self) -> bool {
        matches!(self.kind, NamespaceCreateKind::Directory { .. })
    }

    pub(super) fn symlink_target(&self) -> Option<&'a [u8]> {
        match self.kind {
            NamespaceCreateKind::Symlink { target } => Some(target),
            NamespaceCreateKind::File { .. } | NamespaceCreateKind::Directory { .. } => None,
        }
    }

    pub(super) fn dir_entry_value(&self) -> DirEntryValue {
        DirEntryValue {
            ino: self.ino,
            kind: self.kind(),
        }
    }

    pub(super) fn initial_attr(&self) -> InodeAttr {
        match self.kind {
            NamespaceCreateKind::File { mode } => {
                InodeAttr::new_file(self.ino, self.parent, self.uid, self.gid, mode)
            }
            NamespaceCreateKind::Directory { mode } => {
                InodeAttr::new_dir(self.ino, self.parent, self.uid, self.gid, mode)
            }
            NamespaceCreateKind::Symlink { target } => {
                InodeAttr::new_symlink(self.ino, self.parent, self.uid, self.gid, target.len())
            }
        }
    }

    pub(super) fn pending_create_record(&self) -> PendingCreateRecord {
        PendingCreateRecord {
            ino: self.ino,
            parent: self.parent,
            name: self.name.to_vec(),
            kind: self.kind(),
            created_at: self.created_at,
        }
    }

    pub(super) fn extra_event(&self) -> Option<crate::events::MetaEvent> {
        self.symlink_target()
            .map(|target| crate::events::MetaEvent::SetSymlinkTarget {
                ino: self.ino,
                target: target.to_vec(),
            })
    }
}

pub(super) struct NamespaceCreateTxn<'a> {
    txn: &'a redb::WriteTransaction,
}

impl<'a> NamespaceCreateTxn<'a> {
    pub(super) fn new(txn: &'a redb::WriteTransaction) -> Self {
        Self { txn }
    }

    pub(super) fn execute(&self, spec: &NamespaceCreateSpec<'_>) -> MetaResult<InodeAttr> {
        let mut parent_attr = RedbMetaEngine::get_attr_merged_in_write_txn(self.txn, spec.parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(spec.parent));
        }
        if RedbMetaEngine::lookup_merged_in_write_txn(self.txn, spec.parent, spec.name)?.is_some() {
            return Err(MetaError::EntryExists {
                parent: spec.parent,
                name: String::from_utf8_lossy(spec.name).to_string(),
            });
        }

        self.increment_total_inodes()?;

        let attr = spec.initial_attr();
        self.insert_attr(&attr)?;
        self.clear_inode_tombstone(spec.ino)?;
        if attr.kind == FileType::RegularFile {
            RedbMetaEngine::sync_inode_lifecycle_in_txn(self.txn, attr.ino)?;
        }

        let dentry_key = encode_dentry_key(spec.parent, spec.name);
        self.insert_dentry(dentry_key.as_slice(), &spec.dir_entry_value())?;
        self.clear_dentry_tombstone(dentry_key.as_slice())?;

        if let Some(target) = spec.symlink_target() {
            RedbMetaEngine::set_symlink_overlay_in_txn(self.txn, spec.ino, target)?;
        }

        let parent_attr_event = if spec.updates_parent_attr() {
            parent_attr.nlink += 1;
            parent_attr.touch_mtime();
            self.insert_attr(&parent_attr)?;
            self.clear_inode_tombstone(spec.parent)?;
            Some(parent_attr.clone())
        } else {
            None
        };

        self.append_create_events(spec, &attr, parent_attr_event)?;
        Ok(attr)
    }

    pub(super) fn increment_total_inodes(&self) -> MetaResult<()> {
        let mut counters = self
            .txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let total_inodes = match counters
            .get(counters::TOTAL_INODES)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => v.value(),
            None => 0,
        };
        counters
            .insert(counters::TOTAL_INODES, total_inodes + 1)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn insert_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let mut inodes = self
            .txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let attr_data = encode(attr)?;
        inodes
            .insert(attr.ino, attr_data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn clear_inode_tombstone(&self, ino: Ino) -> MetaResult<()> {
        let mut inode_tombstones = self
            .txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = inode_tombstones
            .remove(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn insert_dentry(&self, dentry_key: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        let mut dentries = self
            .txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry_val = encode(entry)?;
        dentries
            .insert(dentry_key, entry_val.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn clear_dentry_tombstone(&self, dentry_key: &[u8]) -> MetaResult<()> {
        let mut dentry_tombstones = self
            .txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = dentry_tombstones
            .remove(dentry_key)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn append_create_events(
        &self,
        spec: &NamespaceCreateSpec<'_>,
        attr: &InodeAttr,
        parent_attr_event: Option<InodeAttr>,
    ) -> MetaResult<()> {
        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
        )?;
        if let Some(extra_event) = spec.extra_event() {
            RedbMetaEngine::append_event_in_txn(self.txn, &extra_event)?;
        }
        if let Some(parent_attr) = parent_attr_event {
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::SetAttr { attr: parent_attr },
            )?;
        }
        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::AddEntry {
                parent: spec.parent,
                name: spec.name.to_vec(),
                entry: spec.dir_entry_value(),
            },
        )?;
        Ok(())
    }
}

pub(super) enum NamespaceDeleteKind {
    Unlink,
    UnlinkOpenHandle,
    RemoveDir,
}

pub(super) struct NamespaceDeleteSpec<'a> {
    parent: Ino,
    name: &'a [u8],
    kind: NamespaceDeleteKind,
    created_at: i64,
}

impl<'a> NamespaceDeleteSpec<'a> {
    pub(super) fn new_unlink(parent: Ino, name: &'a [u8]) -> Self {
        Self {
            parent,
            name,
            kind: NamespaceDeleteKind::Unlink,
            created_at: current_unix_seconds(),
        }
    }

    pub(super) fn new_unlink_open_handle(parent: Ino, name: &'a [u8]) -> Self {
        Self {
            parent,
            name,
            kind: NamespaceDeleteKind::UnlinkOpenHandle,
            created_at: current_unix_seconds(),
        }
    }

    pub(super) fn new_rmdir(parent: Ino, name: &'a [u8]) -> Self {
        Self {
            parent,
            name,
            kind: NamespaceDeleteKind::RemoveDir,
            created_at: current_unix_seconds(),
        }
    }

    pub(super) fn records_deferred_delete(&self) -> bool {
        matches!(self.kind, NamespaceDeleteKind::UnlinkOpenHandle)
    }

    pub(super) fn updates_parent_nlink(&self) -> bool {
        matches!(self.kind, NamespaceDeleteKind::RemoveDir)
    }

    pub(super) fn allows_directory_target(&self) -> bool {
        matches!(self.kind, NamespaceDeleteKind::RemoveDir)
    }
}

pub(super) enum NamespaceDeleteResult {
    File(InodeAttr),
    Directory,
}

pub(super) struct NamespaceDeleteTxn<'a> {
    txn: &'a redb::WriteTransaction,
}

impl<'a> NamespaceDeleteTxn<'a> {
    pub(super) fn new(txn: &'a redb::WriteTransaction) -> Self {
        Self { txn }
    }

    pub(super) fn execute(
        &self,
        spec: &NamespaceDeleteSpec<'_>,
    ) -> MetaResult<NamespaceDeleteResult> {
        let entry = RedbMetaEngine::lookup_merged_in_write_txn(self.txn, spec.parent, spec.name)?
            .ok_or(MetaError::EntryNotFound {
            parent: spec.parent,
            name: String::from_utf8_lossy(spec.name).to_string(),
        })?;

        if spec.allows_directory_target() {
            self.execute_rmdir(spec, entry)
        } else {
            self.execute_unlink(spec, entry)
        }
    }

    pub(super) fn execute_unlink(
        &self,
        spec: &NamespaceDeleteSpec<'_>,
        entry: DirEntryValue,
    ) -> MetaResult<NamespaceDeleteResult> {
        let mut attr = match RedbMetaEngine::get_attr_merged_in_write_txn(self.txn, entry.ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) if entry.kind == FileType::Directory => {
                return Err(MetaError::IsADirectory(entry.ino));
            }
            Err(err) => return Err(err),
        };
        if attr.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(entry.ino));
        }
        let mut parent_attr = RedbMetaEngine::get_attr_merged_in_write_txn(self.txn, spec.parent)?;

        RedbMetaEngine::remove_dentry_overlay_in_txn(self.txn, spec.parent, spec.name)?;

        attr.nlink = attr.nlink.saturating_sub(1);
        let deleted_before_publish = if attr.nlink == 0
            && attr.kind == FileType::RegularFile
            && (attr.content_epoch != attr.committed_content_epoch
                || attr.size != attr.committed_size
                || RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
                    self.txn,
                    REGULAR_FILE_OVERLAY_TABLE,
                    entry.ino,
                )?) {
            let uploaded_overlay_object_ids =
                self.uploaded_overlay_object_ids_in_write_txn(entry.ino)?;
            if !uploaded_overlay_object_ids.is_empty() {
                self.queue_pending_deletions_in_txn(
                    uploaded_overlay_object_ids.as_slice(),
                    spec.created_at,
                )?;
            }
            Some(RedbMetaEngine::record_deleted_before_publish_in_txn(
                self.txn,
                &attr,
                uploaded_overlay_object_ids,
            )?)
        } else {
            None
        };
        let published_manifest = if deleted_before_publish
            .as_ref()
            .is_some_and(|record| record.has_published_base)
        {
            Some(RedbMetaEngine::base_manifest_from_graph_rows_in_txn(
                self.txn, entry.ino,
            )?)
        } else {
            None
        };

        if spec.records_deferred_delete() {
            RedbMetaEngine::set_inode_overlay_in_txn(self.txn, &attr)?;
            if deleted_before_publish.is_none() {
                RedbMetaEngine::sync_inode_lifecycle_in_txn(self.txn, attr.ino)?;
            }
        } else if attr.nlink == 0 {
            RedbMetaEngine::remove_inode_overlay_in_txn(self.txn, entry.ino)?;
            RedbMetaEngine::clear_inode_overlay_aux_in_txn(self.txn, entry.ino)?;
            self.remove_file_blocks_for_inode(entry.ino)?;
            if deleted_before_publish.is_none() {
                RedbMetaEngine::remove_inode_lifecycle_in_txn(self.txn, entry.ino)?;
            }
        } else {
            RedbMetaEngine::set_inode_overlay_in_txn(self.txn, &attr)?;
            RedbMetaEngine::sync_inode_lifecycle_in_txn(self.txn, attr.ino)?;
        }

        parent_attr.touch_mtime();
        RedbMetaEngine::set_inode_overlay_in_txn(self.txn, &parent_attr)?;

        if spec.records_deferred_delete() {
            self.record_deferred_delete(spec, entry.ino)?;
        }

        self.append_unlink_events(
            spec,
            &attr,
            &parent_attr,
            deleted_before_publish.as_ref(),
            published_manifest,
        )?;
        Ok(NamespaceDeleteResult::File(attr))
    }

    pub(super) fn execute_rmdir(
        &self,
        spec: &NamespaceDeleteSpec<'_>,
        entry: DirEntryValue,
    ) -> MetaResult<NamespaceDeleteResult> {
        let target_kind = match RedbMetaEngine::get_attr_merged_in_write_txn(self.txn, entry.ino) {
            Ok(attr) => attr.kind,
            Err(MetaError::InodeNotFound(_)) => entry.kind,
            Err(err) => return Err(err),
        };
        if target_kind != FileType::Directory {
            return Err(MetaError::NotADirectory(entry.ino));
        }
        if RedbMetaEngine::has_entries_merged_in_write_txn(self.txn, entry.ino)? {
            return Err(MetaError::DirNotEmpty(entry.ino));
        }

        let mut parent_attr = RedbMetaEngine::get_attr_merged_in_write_txn(self.txn, spec.parent)?;

        RedbMetaEngine::remove_dentry_overlay_in_txn(self.txn, spec.parent, spec.name)?;
        RedbMetaEngine::remove_inode_overlay_in_txn(self.txn, entry.ino)?;
        RedbMetaEngine::clear_inode_overlay_aux_in_txn(self.txn, entry.ino)?;

        if spec.updates_parent_nlink() {
            parent_attr.nlink = parent_attr.nlink.saturating_sub(1);
        }
        parent_attr.touch_mtime();
        RedbMetaEngine::set_inode_overlay_in_txn(self.txn, &parent_attr)?;

        self.append_rmdir_events(spec, entry.ino, &parent_attr)?;
        Ok(NamespaceDeleteResult::Directory)
    }

    pub(super) fn remove_file_blocks_for_inode(&self, ino: Ino) -> MetaResult<()> {
        RedbMetaEngine::clear_regular_file_graph_in_txn(self.txn, ino)
    }

    pub(super) fn record_deferred_delete(
        &self,
        spec: &NamespaceDeleteSpec<'_>,
        ino: Ino,
    ) -> MetaResult<()> {
        let record = DeferredDeleteRecord {
            ino,
            parent: spec.parent,
            name: spec.name.to_vec(),
            created_at: spec.created_at,
        };
        let mut deferred = self
            .txn
            .open_table(DEFERRED_DELETES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let encoded = encode(&record)?;
        deferred
            .insert(ino, encoded.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let undo = UndoLogRecord {
            op_id: RedbMetaEngine::next_undo_op_id_in_txn(self.txn)?,
            kind: UndoLogKind::DeferredDelete,
            phase: UndoLogPhase::Started,
            ino,
            parent: spec.parent,
            name: spec.name.to_vec(),
            created_at: spec.created_at,
        };
        RedbMetaEngine::append_undo_log_in_txn(self.txn, &undo)
    }

    pub(super) fn uploaded_overlay_object_ids_in_write_txn(
        &self,
        ino: Ino,
    ) -> MetaResult<Vec<u64>> {
        let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            self.txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let mut object_ids = std::collections::BTreeSet::new();
        for chunk_state in overlay_chunks.values() {
            for slice in chunk_state.slices.values() {
                if slice.block_state == BlockRefState::Uploaded
                    && let Some(block) = slice.block.as_ref()
                {
                    object_ids.insert(block.object_id);
                }
            }
        }
        Ok(object_ids.into_iter().collect())
    }

    pub(super) fn queue_pending_deletions_in_txn(
        &self,
        object_ids: &[u64],
        timestamp: i64,
    ) -> MetaResult<()> {
        if object_ids.is_empty() {
            return Ok(());
        }

        let mut pending_deletions = self
            .txn
            .open_table(PENDING_DELETIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for &object_id in object_ids {
            pending_deletions
                .insert(object_id, timestamp)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        drop(pending_deletions);
        for &object_id in object_ids {
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                },
            )?;
        }
        Ok(())
    }

    pub(super) fn append_unlink_events(
        &self,
        spec: &NamespaceDeleteSpec<'_>,
        attr: &InodeAttr,
        parent_attr: &InodeAttr,
        deleted_before_publish: Option<&InodeLifecycleRecord>,
        published_manifest: Option<Vec<(u32, BlockRef)>>,
    ) -> MetaResult<()> {
        if deleted_before_publish.is_some_and(|record| record.has_published_base) {
            let published_manifest = published_manifest.ok_or_else(|| {
                MetaError::Storage(
                    "missing published manifest for deleted-before-publish rollback".to_string(),
                )
            })?;
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::PublishFileManifest {
                    ino: attr.ino,
                    blocks: published_manifest,
                },
            )?;
            let mut remote_attr = attr.clone();
            remote_attr.nlink = remote_attr.nlink.saturating_add(1);
            remote_attr.size = remote_attr.committed_size;
            remote_attr.content_epoch = remote_attr.committed_content_epoch;
            remote_attr.upload_state = UploadState::Ready;
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::SetAttr { attr: remote_attr },
            )?;
            return Ok(());
        }

        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::RemoveEntry {
                parent: spec.parent,
                name: spec.name.to_vec(),
            },
        )?;
        if spec.records_deferred_delete() || attr.nlink > 0 {
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        } else {
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::RemoveFileBlocks { ino: attr.ino },
            )?;
            RedbMetaEngine::append_event_in_txn(
                self.txn,
                &crate::events::MetaEvent::RemoveAttr { ino: attr.ino },
            )?;
        }
        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::SetAttr {
                attr: parent_attr.clone(),
            },
        )
    }

    pub(super) fn append_rmdir_events(
        &self,
        spec: &NamespaceDeleteSpec<'_>,
        ino: Ino,
        parent_attr: &InodeAttr,
    ) -> MetaResult<()> {
        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::SetAttr {
                attr: parent_attr.clone(),
            },
        )?;
        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::RemoveAttr { ino },
        )?;
        RedbMetaEngine::append_event_in_txn(
            self.txn,
            &crate::events::MetaEvent::RemoveEntry {
                parent: spec.parent,
                name: spec.name.to_vec(),
            },
        )
    }
}

impl RedbMetaEngine {
    pub(super) fn execute_namespace_create(
        &self,
        spec: NamespaceCreateSpec<'_>,
    ) -> MetaResult<InodeAttr> {
        let intent = spec.pending_create_record();
        self.add_pending_create(&intent)?;

        let create_result = (|| -> MetaResult<InodeAttr> {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let result_attr = NamespaceCreateTxn::new(&txn).execute(&spec)?;
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok(result_attr)
        })();

        match create_result {
            Ok(attr) => {
                self.remove_pending_create(spec.ino)?;
                Ok(attr)
            }
            Err(err) => {
                let _ = self.remove_pending_create(spec.ino);
                Err(err)
            }
        }
    }

    pub(super) fn execute_namespace_delete(
        &self,
        spec: NamespaceDeleteSpec<'_>,
    ) -> MetaResult<NamespaceDeleteResult> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let result = NamespaceDeleteTxn::new(&txn).execute(&spec)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result)
    }
}

impl MetaFsOps for RedbMetaEngine {
    // --- Atomic Rename ---------------------------------------------------

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let src_entry = Self::lookup_merged_in_write_txn(&txn, parent, name)?.ok_or_else(|| {
            MetaError::EntryNotFound {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            }
        })?;
        let dst_entry_opt = Self::lookup_merged_in_write_txn(&txn, new_parent, new_name)?;
        if let Some(dst_entry) = &dst_entry_opt
            && dst_entry.kind == FileType::Directory
            && !Self::readdir_merged_in_write_txn(&txn, dst_entry.ino)?.is_empty()
        {
            return Err(MetaError::DirNotEmpty(dst_entry.ino));
        }

        let moved_dir_seed = if src_entry.kind == FileType::Directory {
            Some(Self::get_attr_merged_in_write_txn(&txn, src_entry.ino)?)
        } else {
            None
        };
        let old_parent_seed = if src_entry.kind == FileType::Directory && parent != new_parent {
            Some(Self::get_attr_merged_in_write_txn(&txn, parent)?)
        } else {
            None
        };
        let new_parent_seed = if src_entry.kind == FileType::Directory && parent != new_parent {
            Some(Self::get_attr_merged_in_write_txn(&txn, new_parent)?)
        } else {
            None
        };
        let displaced_attr_seed = match &dst_entry_opt {
            Some(dst_entry) if dst_entry.kind != FileType::Directory => {
                Some(Self::get_attr_merged_in_write_txn(&txn, dst_entry.ino)?)
            }
            _ => None,
        };

        let displaced_ino: Option<Ino>;
        let mut displaced_attr_event: Option<crate::events::MetaEvent> = None;
        let mut moved_dir_attr = moved_dir_seed;
        let mut old_parent_attr = old_parent_seed;
        let mut new_parent_attr = new_parent_seed;
        {
            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;
            displaced_ino = if let Some(dst_entry) = dst_entry_opt.clone() {
                if dst_entry.kind == FileType::Directory {
                    Self::remove_inode_overlay_in_txn(&txn, dst_entry.ino)?;
                    Self::clear_inode_overlay_aux_in_txn(&txn, dst_entry.ino)?;
                } else {
                    if let Some(mut attr) = displaced_attr_seed.clone() {
                        attr.nlink = attr.nlink.saturating_sub(1);
                        if attr.nlink == 0 {
                            Self::remove_inode_overlay_in_txn(&txn, dst_entry.ino)?;
                            Self::clear_inode_overlay_aux_in_txn_with_options(
                                &txn,
                                dst_entry.ino,
                                attr.kind != FileType::RegularFile,
                            )?;
                            displaced_attr_event =
                                Some(crate::events::MetaEvent::RemoveAttr { ino: dst_entry.ino });
                        } else {
                            Self::set_inode_overlay_in_txn(&txn, &attr)?;
                            displaced_attr_event =
                                Some(crate::events::MetaEvent::SetAttr { attr: attr.clone() });
                        }
                    }
                }
                Self::remove_dentry_overlay_in_txn(&txn, new_parent, new_name)?;
                Some(dst_entry.ino)
            } else {
                None
            };

            Self::set_dentry_overlay_in_txn(&txn, new_parent, new_name, &src_entry)?;

            if src_entry.kind == FileType::Directory && parent != new_parent {
                if let Some(mut attr) = moved_dir_attr.take() {
                    attr.parent = new_parent;
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    moved_dir_attr = Some(attr);
                }
                if let Some(mut attr) = old_parent_attr.take() {
                    attr.nlink = attr.nlink.saturating_sub(1);
                    attr.touch_mtime();
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    old_parent_attr = Some(attr);
                }
                if let Some(mut attr) = new_parent_attr.take() {
                    attr.nlink += 1;
                    attr.touch_mtime();
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    new_parent_attr = Some(attr);
                }
            }

            let displaced_set_attr = match &displaced_attr_event {
                Some(crate::events::MetaEvent::SetAttr { attr }) => Some(attr.clone()),
                _ => None,
            };
            let displaced_remove_attr_ino = match &displaced_attr_event {
                Some(crate::events::MetaEvent::RemoveAttr { ino }) => Some(*ino),
                _ => None,
            };
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RenameEntry {
                    parent,
                    name: name.to_vec(),
                    new_parent,
                    new_name: new_name.to_vec(),
                    entry: src_entry.clone(),
                    displaced_set_attr: Box::new(displaced_set_attr),
                    displaced_remove_attr_ino,
                    moved_dir_attr: Box::new(moved_dir_attr),
                    old_parent_attr: Box::new(old_parent_attr),
                    new_parent_attr: Box::new(new_parent_attr),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(RenameAtomicResult {
            moved_ino: src_entry.ino,
            displaced_ino,
        })
    }

    // --- Hardlinks -------------------------------------------------------

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let mut attr = self.get_attr(ino)?;
        let mut parent_attr = self.get_attr(new_parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(new_parent));
        }
        if self.lookup(new_parent, new_name).is_ok() {
            return Err(MetaError::EntryExists {
                parent: new_parent,
                name: String::from_utf8_lossy(new_name).to_string(),
            });
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let result;
        let parent_attr_event: Option<InodeAttr>;
        {
            attr.nlink += 1;
            attr.touch_ctime();
            Self::set_inode_overlay_in_txn(&txn, &attr)?;
            let entry = DirEntryValue {
                ino,
                kind: attr.kind,
            };
            Self::set_dentry_overlay_in_txn(&txn, new_parent, new_name, &entry)?;

            parent_attr.touch_mtime();
            Self::set_inode_overlay_in_txn(&txn, &parent_attr)?;
            parent_attr_event = Some(parent_attr.clone());

            result = attr;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: result.clone(),
                },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::AddEntry {
                    parent: new_parent,
                    name: new_name.to_vec(),
                    entry: DirEntryValue {
                        ino,
                        kind: result.kind,
                    },
                },
            )?;
            if let Some(parent_attr) = &parent_attr_event {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr {
                        attr: parent_attr.clone(),
                    },
                )?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result)
    }

    // --- High-Level Operations (ACID Atomicity) --------------------------

    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.execute_namespace_create(NamespaceCreateSpec::new_file(
            ino, parent, name, uid, gid, mode,
        ))
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.execute_namespace_create(NamespaceCreateSpec::new_dir(
            ino, parent, name, uid, gid, mode,
        ))
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.execute_namespace_create(NamespaceCreateSpec::new_symlink(
            ino, parent, name, target, uid, gid,
        ))
    }

    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        match self.execute_namespace_delete(NamespaceDeleteSpec::new_unlink(parent, name))? {
            NamespaceDeleteResult::File(attr) => Ok(attr),
            NamespaceDeleteResult::Directory => unreachable!("unlink cannot remove directories"),
        }
    }

    fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        match self
            .execute_namespace_delete(NamespaceDeleteSpec::new_unlink_open_handle(parent, name))?
        {
            NamespaceDeleteResult::File(attr) => Ok(attr),
            NamespaceDeleteResult::Directory => {
                unreachable!("unlink_open_handle cannot remove directories")
            }
        }
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        match self.execute_namespace_delete(NamespaceDeleteSpec::new_rmdir(parent, name))? {
            NamespaceDeleteResult::Directory => Ok(()),
            NamespaceDeleteResult::File(_) => unreachable!("rmdir must only remove directories"),
        }
    }
}
