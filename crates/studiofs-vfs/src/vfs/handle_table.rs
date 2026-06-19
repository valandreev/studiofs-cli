use crate::access::AccessClass;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use studiofs_meta::types::{Ino, InodeAttr};

use super::{OpenFile, OpenFlags};

/// Per-VFS handle table tracking all open file descriptors.
pub(super) struct HandleTable {
    /// Monotonically increasing file handle counter.
    next_fh: AtomicU64,
    /// All open handles: fh -> OpenFile.
    handles: DashMap<u64, OpenFile>,
    /// Inode -> number of currently open file descriptors.
    open_counts: Mutex<HashMap<Ino, u32>>,
    /// Inodes that were unlinked while still open (deferred deletion).
    orphans: Mutex<HashSet<Ino>>,
}

impl HandleTable {
    pub(super) fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: DashMap::new(),
            open_counts: Mutex::new(HashMap::new()),
            orphans: Mutex::new(HashSet::new()),
        }
    }

    /// Allocate a new file handle and register it.
    pub(super) fn open(
        &self,
        ino: Ino,
        flags: OpenFlags,
        open_attr: Option<InodeAttr>,
        active_open_overlay: bool,
    ) -> OpenFile {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let open_file = OpenFile::new(fh, ino, flags, open_attr, active_open_overlay);
        self.handles.insert(fh, open_file.clone());
        *self.open_counts.lock().entry(ino).or_insert(0) += 1;
        open_file
    }

    /// Look up an open file handle.
    pub(super) fn get(&self, fh: u64) -> Option<OpenFile> {
        self.handles.get(&fh).map(|open_file| open_file.clone())
    }

    /// Close a file handle. Returns `(ino, remaining_open_count)`.
    pub(super) fn close(&self, fh: u64) -> Option<(OpenFile, u32)> {
        let (_, open_file) = self.handles.remove(&fh)?;
        let mut counts = self.open_counts.lock();
        let count = counts.get_mut(&open_file.ino)?;
        *count = count.saturating_sub(1);
        let remaining = *count;
        if remaining == 0 {
            counts.remove(&open_file.ino);
        }
        Some((open_file, remaining))
    }

    /// Get the number of open handles for an inode.
    pub(super) fn open_count(&self, ino: Ino) -> u32 {
        self.open_counts.lock().get(&ino).copied().unwrap_or(0)
    }

    /// Returns true when an inode still has a mutating handle whose local view
    /// must survive namespace removal until the handle is released.
    pub(super) fn has_active_open_overlay(&self, ino: Ino) -> bool {
        self.handles
            .iter()
            .any(|open_file| open_file.ino == ino && open_file.active_open_overlay)
    }

    /// Mark an inode as orphan (unlinked while open).
    pub(super) fn mark_orphan(&self, ino: Ino) {
        self.orphans.lock().insert(ino);
    }

    /// Check if inode is an orphan and remove the mark. Returns true if it was.
    pub(super) fn take_orphan(&self, ino: Ino) -> bool {
        self.orphans.lock().remove(&ino)
    }

    /// Return the [`AccessClass`] of every currently open file handle.
    ///
    /// Used during shutdown to drain `active_access_classes` counters for
    /// handles that were never explicitly closed via `release_fh` (e.g. the
    /// kernel unmounted the filesystem without flushing all file descriptors).
    pub(super) fn collect_open_access_classes(&self) -> Vec<AccessClass> {
        self.handles
            .iter()
            .map(|f| f.current_access_class())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_table_open_close() {
        let source = include_str!("handle_table.rs");
        let implementation = source
            .split("#[cfg(test)]")
            .next()
            .expect("handle table implementation should be present");
        assert!(implementation.contains("handles: DashMap<u64, OpenFile>"));
        assert!(!implementation.contains("RwLock"));

        let ht = HandleTable::new();
        let fh1 = ht.open(10, OpenFlags::RDONLY, None, false);
        let fh2 = ht.open(10, OpenFlags::RDWR, None, true);
        let fh3 = ht.open(20, OpenFlags::WRONLY, None, true);

        assert_ne!(fh1.fh, fh2.fh);
        assert_ne!(fh2.fh, fh3.fh);

        assert_eq!(ht.open_count(10), 2);
        assert_eq!(ht.open_count(20), 1);
        assert_eq!(ht.open_count(99), 0);
        assert!(ht.has_active_open_overlay(10));
        assert!(ht.has_active_open_overlay(20));
        assert!(!ht.has_active_open_overlay(99));

        let of1 = ht.get(fh1.fh).unwrap();
        assert_eq!(of1.ino, 10);
        assert!(of1.flags.contains(OpenFlags::RDONLY));

        let of3 = ht.get(fh3.fh).unwrap();
        assert_eq!(of3.ino, 20);

        assert!(ht.get(99999).is_none());

        let (closed1, remaining) = ht.close(fh1.fh).unwrap();
        assert_eq!(closed1.ino, 10);
        assert_eq!(remaining, 1);
        assert_eq!(ht.open_count(10), 1);

        let (closed2, remaining) = ht.close(fh2.fh).unwrap();
        assert_eq!(closed2.ino, 10);
        assert_eq!(remaining, 0);
        assert_eq!(ht.open_count(10), 0);
        assert!(!ht.has_active_open_overlay(10));

        assert!(ht.close(fh1.fh).is_none());
    }

    #[test]
    fn test_handle_table_orphans() {
        let ht = HandleTable::new();
        let _fh = ht.open(42, OpenFlags::RDWR, None, true);

        assert!(!ht.take_orphan(42));

        ht.mark_orphan(42);

        assert!(ht.take_orphan(42));
        assert!(!ht.take_orphan(42));
    }
}
