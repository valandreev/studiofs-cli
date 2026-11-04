use super::{
    AccessLevel, FileSessionOperation, FileType, Ino, MetaError, ROOT_INO, StudioVfs, VfsError,
    VfsResult, debug, warn,
};
use crate::runtime::run_local_cache_io_blocking;

impl StudioVfs {
    /// Get an extended attribute value.
    pub fn get_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<Option<Vec<u8>>> {
        if let Err(err) = self.enforce_access(ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                return Ok(None);
            }
            return Err(err);
        }
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(None);
        }
        Ok(self.sync_meta().get_xattr(ino, name)?)
    }

    /// Get an extended attribute value size without returning the value bytes.
    pub fn get_xattr_size(&self, ino: Ino, name: &[u8]) -> VfsResult<Option<usize>> {
        if let Err(err) = self.enforce_access(ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                return Ok(None);
            }
            return Err(err);
        }
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(None);
        }
        Ok(self.sync_meta().get_xattr_size(ino, name)?)
    }

    /// Set an extended attribute.
    pub fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> VfsResult<()> {
        let operation = self.try_mutating_operation_admission()?;
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(());
        }
        if value.len() > self.max_xattr_bytes {
            return Err(VfsError::Meta(MetaError::XattrValueTooLarge {
                actual: value.len(),
                limit: self.max_xattr_bytes,
            }));
        }
        self.commit_xattr_mutation(&operation, ino, || {
            self.sync_meta().set_xattr(ino, name, value, flags)
        })
    }

    /// Remove an extended attribute.
    pub fn remove_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<()> {
        let operation = self.try_mutating_operation_admission()?;
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(());
        }
        self.commit_xattr_mutation(&operation, ino, || self.sync_meta().remove_xattr(ino, name))
    }

    /// Run an xattr mutation, whose ctime touch updates the inode row.
    fn commit_xattr_mutation(
        &self,
        operation: &MutatingOperationAdmission<'_>,
        ino: Ino,
        mutate: impl FnOnce() -> Result<(), MetaError>,
    ) -> VfsResult<()> {
        if self.is_runtime_stats_file_ino(ino) {
            return Err(Self::runtime_stats_access_denied_error());
        }
        // A queued create has not committed the inode row yet.
        if self.has_deferred_namespace_metadata(ino) {
            self.run_sync_future_with_admission(
                operation,
                self.drain_deferred_namespace_metadata_for_barrier(),
            )?;
        }
        self.ensure_no_deferred_namespace_error(ino)?;
        // A flush persisting its dirty attr snapshot would overwrite the new ctime.
        let cleanup = self.file_session_cleanup(ino);
        let permit = self.run_sync_future_with_admission(operation, async {
            Ok(self
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await)
        })?;
        let _cleanup = cleanup.with_permit(permit);
        mutate()?;
        // getattr prefers a dirty attr, and a later flush persists it over the inode row.
        let has_dirty_attr = self.state.namespace.dirty_attrs.lock().contains_key(&ino);
        if has_dirty_attr {
            // The mutation already committed, so a failed read must not report it as failed.
            match self.sync_meta().get_attr(ino) {
                Ok(committed) => {
                    if let Some(attr) = self.state.namespace.dirty_attrs.lock().get_mut(&ino) {
                        attr.ctime = committed.ctime;
                    }
                }
                Err(error) => warn!(ino, %error, "xattr ctime not copied to dirty attr"),
            }
        }
        self.invalidate_attr_read_cache([ino]);
        Ok(())
    }

    /// List all extended attribute names for an inode.
    pub fn list_xattrs(&self, ino: Ino) -> VfsResult<Vec<Vec<u8>>> {
        if let Err(err) = self.enforce_access(ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                return Ok(Vec::new());
            }
            return Err(err);
        }
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(Vec::new());
        }
        Ok(self.sync_meta().list_xattrs(ino)?)
    }

    // Once this guard is held, the operation may finish even if shutdown starts.
    pub(super) fn try_mutating_operation_admission(
        &self,
    ) -> VfsResult<MutatingOperationAdmission<'_>> {
        let operation = self
            .state
            .shutdown
            .mutating_operation_gate
            .try_read()
            .map_err(|_| VfsError::Other("VFS is shutting down".to_string()))?;
        if self.shutdown_requested() {
            return Err(VfsError::Other("VFS is shutting down".to_string()));
        }
        let activity = self.state.sync_safety_activity.begin();
        Ok(MutatingOperationAdmission {
            _shutdown_guard: operation,
            _activity_guard: activity,
        })
    }

    pub(super) fn run_sync_future<T>(
        &self,
        future: impl std::future::Future<Output = VfsResult<T>>,
    ) -> VfsResult<T> {
        let operation = self.try_mutating_operation_admission()?;
        self.run_sync_future_with_admission(&operation, future)
    }

    pub(super) fn run_sync_future_with_admission<T>(
        &self,
        _operation: &MutatingOperationAdmission<'_>,
        future: impl std::future::Future<Output = VfsResult<T>>,
    ) -> VfsResult<T> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else if let Some(handle) = self.state.mount_runtime.get() {
            // Platform dispatcher threads (libfuse, macFUSE, WinFSP) run
            // outside any Tokio context; tasks the operation spawns must land
            // on the mount runtime rather than a temporary one dropped here.
            handle.block_on(future)
        } else {
            #[cfg(test)]
            self.state
                .sync_future_runtime_fallbacks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for synchronous VFS operation")
                .block_on(future)
        }
    }

    #[cfg(test)]
    pub(super) fn sync_future_runtime_fallbacks(&self) -> u64 {
        self.state
            .sync_future_runtime_fallbacks
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reports whether `dir_ino` lies under a trash-like directory in the root.
    ///
    /// Walks directory parents to the root child and checks only that child's
    /// own bindings, so the cost tracks path depth rather than the size of the
    /// root directory.
    pub(super) fn directory_has_probable_trash_ancestor(&self, dir_ino: Ino) -> VfsResult<bool> {
        let meta = self.sync_meta();
        let mut current = dir_ino;
        let mut hops = 0usize;
        // Directory `parent` is authoritative; hard links never apply to directories.
        let root_child = loop {
            if current == 0 || current == ROOT_INO || hops >= 1024 {
                return Ok(false);
            }
            let parent = match meta.get_attr(current) {
                Ok(attr) => attr.parent,
                Err(MetaError::InodeNotFound(_)) => return Ok(false),
                Err(err) => return Err(err.into()),
            };
            if parent == ROOT_INO {
                break current;
            }
            if parent == 0 || parent == current {
                return Ok(false);
            }
            current = parent;
            hops += 1;
        };

        // Bindings are candidates; confirm each through the merged lookup.
        for (parent, name) in meta.entry_bindings_for_inode(root_child)? {
            if parent != ROOT_INO || !super::is_probable_trash_component(&name) {
                continue;
            }
            match meta.lookup(ROOT_INO, &name) {
                Ok(entry) if entry.ino == root_child && entry.kind == FileType::Directory => {
                    return Ok(true);
                }
                Ok(_) | Err(MetaError::EntryNotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(false)
    }

    /// Immediately evict a single block from every cache tier.
    ///
    /// Called by `truncate_blocks()` for each block that is removed by a
    /// truncation.
    pub(super) fn evict_block_from_caches(&self, ino: Ino, block_index: u32, object_id: u64) {
        self.clear_pending_payload(ino, block_index);
        self.remove_live_extent(ino, block_index);

        // Committed cache + L1/L2 upload cache (defer if pinned for pending upload).
        if self.cached_store.is_pinned(object_id) {
            self.state.upload.pending_evictions.lock().insert(object_id);
            debug!(
                "evict_block: deferred pinned object_id={} ino={} block={}",
                object_id, ino, block_index
            );
        } else {
            if let Err(error) = self.local_payload_backend.remove(object_id) {
                self.state.upload.pending_evictions.lock().insert(object_id);
                self.schedule_deferred_block_cache_eviction(ino, block_index, object_id);
                warn!(
                    ino,
                    block_index,
                    object_id,
                    error = %error,
                    "immediate block cache eviction failed; deferred cleanup"
                );
            }
        }
    }

    fn schedule_deferred_block_cache_eviction(&self, ino: Ino, block_index: u32, object_id: u64) {
        let local_payload_backend = self.local_payload_backend.clone();
        let state = self.state.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        self.spawn_background_cleanup_task_on(&handle, async move {
            let result = run_local_cache_io_blocking(move || {
                local_payload_backend
                    .remove(object_id)
                    .map_err(VfsError::LocalPayload)
            })
            .await;
            match result {
                Ok(((), _)) => {
                    state.upload.pending_evictions.lock().remove(&object_id);
                    debug!(
                        ino,
                        block_index, object_id, "deferred block cache eviction completed"
                    );
                }
                Err(error) => {
                    warn!(
                        ino,
                        block_index,
                        object_id,
                        error = %error,
                        "deferred block cache eviction failed; retaining pending cleanup"
                    );
                }
            }
        });
    }
}

pub(super) struct MutatingOperationAdmission<'a> {
    _shutdown_guard: tokio::sync::RwLockReadGuard<'a, ()>,
    _activity_guard: super::SyncSafetyActivityGuard,
}
