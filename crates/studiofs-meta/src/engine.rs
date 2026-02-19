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

    /// Decrement a counter atomically (saturating at 0), returns the new value.
    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>;

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

    // ─── File Blocks (New Architecture) ──────────────────────────────────

    /// Get a specific block reference for a file.
    /// Key: (inode, block_index)
    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>>;

    /// Set a block reference for a file.
    /// Returns the old block reference if it existed (for GC marking).
    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>>;

    /// Get all block references for a file.
    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>>;

    /// Remove all blocks for a file (used when deleting).
    /// Returns the list of removed block IDs for GC.
    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>>;

    /// Remove a single block for a file (used in truncate).
    /// Returns the removed block ID if it existed.
    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>>;

    /// Allocate a new sequential block ID.
    fn next_block_id(&self) -> MetaResult<u64>;

    // ─── Pending Deletions (GC) ───────────────────────

    /// Add a block to pending deletions (mark for GC).
    /// The timestamp is used for grace period.
    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()>;

    /// Remove a block from pending deletions (after GC).
    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()>;

    /// Get all pending deletions (object_id, timestamp).
    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>>;

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
            let _ = self.remove_file_blocks(entry.ino)?;
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

    /// Atomic rename: move a directory entry in a single transaction.
    ///
    /// Returns `Some(displaced_ino)` if the target entry was overwritten
    /// (the displaced inode's nlink was decremented and — if zero — its
    /// attr was removed).  The caller (VFS) is responsible for scheduling
    /// block GC for the displaced inode.
    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<Option<Ino>>;

    /// Rename a file or directory (delegates to `rename_atomic`).
    fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8]) -> MetaResult<()> {
        self.rename_atomic(parent, name, new_parent, new_name)?;
        Ok(())
    }

    // ─── Hardlinks ───────────────────────────────────────────────────────

    /// Create a hard link in a single atomic transaction.
    ///
    /// Increments the inode's nlink and adds a new directory entry pointing
    /// to the existing inode.  Directories cannot be hardlinked.
    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr>;

    /// Create a hard link (validates and delegates to `link_atomic`).
    fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let attr = self.get_attr(ino)?;
        if attr.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(ino));
        }
        self.link_atomic(ino, new_parent, new_name)
    }

    // ─── Symlinks ────────────────────────────────────────────────────────

    /// Store a symlink target for the given inode.
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()>;

    /// Get the symlink target for the given inode.
    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>>;

    // ─── Extended Attributes (xattr) ─────────────────────────────────────

    /// Get an extended attribute value.
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>>;

    /// Set an extended attribute.
    /// Flags: 1 = XATTR_CREATE (fail if exists), 2 = XATTR_REPLACE (fail if not exists).
    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()>;

    /// Remove an extended attribute.
    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()>;

    /// List all extended attribute names for an inode.
    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>>;

    /// Create a symlink: allocate inode, store target, add dentry.
    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
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
        let attr = InodeAttr::new_symlink(ino, parent, uid, gid, target.len());
        self.set_attr(&attr)?;
        self.set_symlink_target(ino, target)?;
        self.add_entry(
            parent,
            name,
            &DirEntryValue {
                ino,
                kind: FileType::Symlink,
            },
        )?;
        self.incr_counter(counters::TOTAL_INODES, 1)?;
        Ok(attr)
    }
}
