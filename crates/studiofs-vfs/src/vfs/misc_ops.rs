use super::{
    AccessLevel, FileType, Ino, MetaError, ROOT_INO, StudioVfs, VfsError, VfsResult, debug,
};
use std::collections::HashSet;

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

    /// Set an extended attribute.
    pub fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> VfsResult<()> {
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
        self.sync_meta().set_xattr(ino, name, value, flags)?;
        Ok(())
    }

    /// Remove an extended attribute.
    pub fn remove_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<()> {
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(());
        }
        self.sync_meta().remove_xattr(ino, name)?;
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

    pub(super) fn run_sync_future<T>(future: impl std::future::Future<Output = T>) -> T {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for synchronous VFS operation")
                .block_on(future)
        }
    }

    pub(super) fn directory_has_probable_trash_ancestor(&self, dir_ino: Ino) -> VfsResult<bool> {
        let trash_roots = self.root_trash_dir_inodes()?;
        if trash_roots.is_empty() {
            return Ok(false);
        }
        let mut current = dir_ino;
        let mut hops = 0usize;
        while current != 0 && hops < 1024 {
            if trash_roots.contains(&current) {
                return Ok(true);
            }
            let attr = self.sync_meta().get_attr(current)?;
            let parent = attr.parent;
            if parent == 0 || parent == current {
                break;
            }
            if trash_roots.contains(&parent) {
                return Ok(true);
            }
            current = parent;
            hops += 1;
        }
        Ok(false)
    }

    fn root_trash_dir_inodes(&self) -> VfsResult<HashSet<Ino>> {
        let mut trash_roots = HashSet::new();
        for entry in self
            .sync_meta()
            .readdir(ROOT_INO)?
            .into_iter()
            .filter(|entry| entry.kind == FileType::Directory)
        {
            if super::is_probable_trash_component(&entry.name) {
                trash_roots.insert(entry.ino);
            }
        }
        Ok(trash_roots)
    }

    /// Immediately evict a single block from every cache tier.
    ///
    /// Called by `truncate_blocks()` for each block that is removed by a
    /// truncation.
    pub(super) fn evict_block_from_caches(&self, ino: Ino, block_index: u32, object_id: u64) {
        self.clear_pending_block(ino, block_index);
        self.remove_live_extent(ino, block_index);

        // Committed cache + L1/L2 upload cache (defer if pinned for pending upload).
        if self.block_cache.is_pinned(object_id) {
            self.state.upload.pending_evictions.lock().insert(object_id);
            debug!(
                "evict_block: deferred pinned object_id={} ino={} block={}",
                object_id, ino, block_index
            );
        } else {
            self.block_cache.remove_sync(object_id);
            self.local_block_backend.remove(object_id);
        }
    }
}
