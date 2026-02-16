//! MetaEngine trait — abstract interface for metadata storage.
//!
//! This trait defines all metadata operations. The default implementation is `RedbMetaEngine`,
//! but the design supports swapping to PostgreSQL/Redis for multi-user mode.

use crate::types::*;

/// Errors from the metadata engine.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("inode not found: {0}")]
    InodeNotFound(Ino),

    #[error("entry not found: {name:?} in parent {parent}")]
    EntryNotFound { parent: Ino, name: String },

    #[error("entry already exists: {name:?} in parent {parent}")]
    EntryExists { parent: Ino, name: String },

    #[error("not a directory: inode {0}")]
    NotADirectory(Ino),

    #[error("directory not empty: inode {0}")]
    DirNotEmpty(Ino),

    #[error("is a directory: inode {0}")]
    IsADirectory(Ino),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("format info not found — run `studiofs init` first")]
    NotFormatted,
}

pub type MetaResult<T> = Result<T, MetaError>;

/// Abstract metadata engine interface.
///
/// All operations are synchronous (redb is sync). The VFS layer wraps them
/// in `spawn_blocking` for async compatibility.
pub trait MetaEngine: Send + Sync {
    // ─── Format / Settings ───────────────────────────────────────────────

    /// Store format info (called during `studiofs init`).
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()>;

    /// Load format info.
    fn get_format_info(&self) -> MetaResult<FormatInfo>;

    // ─── Counters ────────────────────────────────────────────────────────

    /// Get a counter value, returns 0 if not set.
    fn get_counter(&self, name: &str) -> MetaResult<u64>;

    /// Increment a counter atomically, returns the new value.
    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>;

    /// Set a counter to a specific value.
    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()>;

    // ─── Inodes ──────────────────────────────────────────────────────────

    /// Allocate a new inode number.
    fn next_inode(&self) -> MetaResult<Ino>;

    /// Get inode attributes.
    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr>;

    /// Set (create or update) inode attributes.
    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()>;

    /// Remove an inode's attributes.
    fn remove_attr(&self, ino: Ino) -> MetaResult<()>;

    // ─── Directory Entries ───────────────────────────────────────────────

    /// Look up a child entry by name in a parent directory.
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>;

    /// List all entries in a directory.
    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>>;

    /// Add a directory entry. Returns error if it already exists.
    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()>;

    /// Remove a directory entry by name. Returns the removed entry.
    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>;

    // ─── Chunks / Slices ─────────────────────────────────────────────────

    /// Get all slices for a chunk.
    fn get_chunk_slices(&self, key: &ChunkKey) -> MetaResult<Vec<Slice>>;

    /// Append a slice to a chunk.
    fn append_chunk_slice(&self, key: &ChunkKey, slice: &Slice) -> MetaResult<()>;

    /// Replace all slices in a chunk (used after compaction).
    fn set_chunk_slices(&self, key: &ChunkKey, slices: &[Slice]) -> MetaResult<()>;

    /// Remove all chunk data for an inode (used when deleting a file).
    fn remove_file_chunks(&self, ino: Ino) -> MetaResult<()>;

    /// Allocate a new globally unique slice ID.
    fn next_slice_id(&self) -> MetaResult<u64>;

    // ─── Pending Slices (Writeback) ──────────────────────────────────────

    /// Mark a slice as pending upload.
    fn add_pending_slice(&self, slice_id: u64) -> MetaResult<()>;

    /// Remove a slice from pending list (after successful upload).
    fn remove_pending_slice(&self, slice_id: u64) -> MetaResult<()>;

    /// Get all pending slices.
    fn get_pending_slices(&self) -> MetaResult<Vec<u64>>;

    // ─── High-Level Operations ───────────────────────────────────────────

    /// Create a new file: allocate inode, set attrs, add dentry.
    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        // Check parent is a directory
        let parent_attr = self.get_attr(parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent));
        }

        // Check entry doesn't exist
        if self.lookup(parent, name).is_ok() {
            return Err(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }

        let ino = self.next_inode()?;
        let attr = InodeAttr::new_file(ino, parent, uid, gid, mode);
        self.set_attr(&attr)?;
        self.add_entry(
            parent,
            name,
            &DirEntryValue {
                ino,
                kind: FileType::RegularFile,
            },
        )?;
        self.incr_counter(counters::TOTAL_INODES, 1)?;
        Ok(attr)
    }

    /// Create a new directory: allocate inode, set attrs, add dentry, update parent nlink.
    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let parent_attr = self.get_attr(parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent));
        }

        if self.lookup(parent, name).is_ok() {
            return Err(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }

        let ino = self.next_inode()?;
        let attr = InodeAttr::new_dir(ino, parent, uid, gid, mode);
        self.set_attr(&attr)?;
        self.add_entry(
            parent,
            name,
            &DirEntryValue {
                ino,
                kind: FileType::Directory,
            },
        )?;

        // Update parent nlink (+1 for subdirectory '..')
        let mut parent_attr = parent_attr;
        parent_attr.nlink += 1;
        parent_attr.touch_mtime();
        self.set_attr(&parent_attr)?;

        self.incr_counter(counters::TOTAL_INODES, 1)?;
        Ok(attr)
    }

    /// Unlink a file: remove dentry, decrement nlink, clean up if nlink=0.
    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        let entry = self.remove_entry(parent, name)?;
        if entry.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(entry.ino));
        }

        let mut attr = self.get_attr(entry.ino)?;
        attr.nlink = attr.nlink.saturating_sub(1);
        if attr.nlink == 0 {
            self.remove_attr(entry.ino)?;
            self.remove_file_chunks(entry.ino)?;
        } else {
            self.set_attr(&attr)?;
        }

        // Update parent mtime
        if let Ok(mut parent_attr) = self.get_attr(parent) {
            parent_attr.touch_mtime();
            let _ = self.set_attr(&parent_attr);
        }

        Ok(attr)
    }

    /// Remove an empty directory.
    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        let entry = self.lookup(parent, name)?;
        if entry.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(entry.ino));
        }

        // Check if empty
        let children = self.readdir(entry.ino)?;
        if !children.is_empty() {
            return Err(MetaError::DirNotEmpty(entry.ino));
        }

        self.remove_entry(parent, name)?;
        self.remove_attr(entry.ino)?;

        // Update parent nlink (-1 for removed '..')
        if let Ok(mut parent_attr) = self.get_attr(parent) {
            parent_attr.nlink = parent_attr.nlink.saturating_sub(1);
            parent_attr.touch_mtime();
            let _ = self.set_attr(&parent_attr);
        }

        Ok(())
    }

    /// Rename a file or directory.
    fn rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<()> {
        let entry = self.remove_entry(parent, name)?;

        // If target exists, remove it first
        if let Ok(existing) = self.lookup(new_parent, new_name) {
            if existing.kind == FileType::Directory {
                // Check if target dir is empty
                let children = self.readdir(existing.ino)?;
                if !children.is_empty() {
                    // Re-add original entry since we already removed it
                    self.add_entry(parent, name, &entry)?;
                    return Err(MetaError::DirNotEmpty(existing.ino));
                }
                self.remove_entry(new_parent, new_name)?;
                self.remove_attr(existing.ino)?;
            } else {
                self.remove_entry(new_parent, new_name)?;
                let mut attr = self.get_attr(existing.ino)?;
                attr.nlink = attr.nlink.saturating_sub(1);
                if attr.nlink == 0 {
                    self.remove_attr(existing.ino)?;
                    self.remove_file_chunks(existing.ino)?;
                } else {
                    self.set_attr(&attr)?;
                }
            }
        }

        self.add_entry(new_parent, new_name, &entry)?;

        // Update parent inode on the moved entry if it's a dir
        if entry.kind == FileType::Directory {
            if let Ok(mut attr) = self.get_attr(entry.ino) {
                attr.parent = new_parent;
                let _ = self.set_attr(&attr);
            }
            // Update nlinks for parent directories
            if parent != new_parent {
                if let Ok(mut old_parent_attr) = self.get_attr(parent) {
                    old_parent_attr.nlink = old_parent_attr.nlink.saturating_sub(1);
                    old_parent_attr.touch_mtime();
                    let _ = self.set_attr(&old_parent_attr);
                }
                if let Ok(mut new_parent_attr) = self.get_attr(new_parent) {
                    new_parent_attr.nlink += 1;
                    new_parent_attr.touch_mtime();
                    let _ = self.set_attr(&new_parent_attr);
                }
            }
        }

        Ok(())
    }
}
