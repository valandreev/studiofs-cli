use crate::access::AccessClass;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use studiofs_meta::types::{Ino, InodeAttr};

use super::{OpenFile, OpenFlags};

#[derive(Debug, Default)]
struct HandleCounts {
    open: u32,
    write: u32,
}

/// Per-VFS handle table tracking all open file descriptors.
pub(super) struct HandleTable {
    /// Monotonically increasing file handle counter. Zero is reserved for
    /// bridge callbacks that use `fh == 0` as "no VFS handle".
    next_fh: AtomicU64,
    /// All open handles: fh -> OpenFile.
    handles: RwLock<HashMap<u64, OpenFile>>,
    /// Open handle ids grouped by inode, avoiding a full handle-table scan
    /// when an inode attribute snapshot changes.
    handles_by_inode: Mutex<HashMap<Ino, HashSet<u64>>>,
    /// Inode -> currently open file descriptor counters.
    counts: Mutex<HashMap<Ino, HandleCounts>>,
    /// Inodes that were unlinked while still open (deferred deletion).
    orphans: Mutex<HashSet<Ino>>,
}

impl HandleTable {
    pub(super) fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: RwLock::new(HashMap::new()),
            handles_by_inode: Mutex::new(HashMap::new()),
            counts: Mutex::new(HashMap::new()),
            orphans: Mutex::new(HashSet::new()),
        }
    }

    fn flags_allow_write(flags: OpenFlags) -> bool {
        flags.contains(OpenFlags::WRONLY) || flags.contains(OpenFlags::RDWR)
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
        let mut handles = self.handles.write();
        handles.insert(fh, open_file.clone());
        self.handles_by_inode
            .lock()
            .entry(ino)
            .or_default()
            .insert(fh);
        drop(handles);
        let mut counts = self.counts.lock();
        let counts_for_inode = counts.entry(ino).or_default();
        counts_for_inode.open = counts_for_inode.open.saturating_add(1);
        if Self::flags_allow_write(flags) {
            counts_for_inode.write = counts_for_inode.write.saturating_add(1);
        }
        open_file
    }

    /// Look up an open file handle.
    pub(super) fn get(&self, fh: u64) -> Option<OpenFile> {
        self.handles.read().get(&fh).cloned()
    }

    /// Refresh the attribute snapshot carried by an already-open handle.
    pub(super) fn update_open_attr(&self, fh: u64, attr: InodeAttr) {
        if let Some(open_file) = self.handles.write().get_mut(&fh) {
            open_file.open_attr = Some(attr);
        }
    }

    /// Refresh the attribute snapshot carried by every handle for an inode.
    pub(super) fn update_open_attrs_for_inode(&self, ino: Ino, attr: &InodeAttr) {
        let mut handles = self.handles.write();
        let handles_by_inode = self.handles_by_inode.lock();
        if let Some(handle_ids) = handles_by_inode.get(&ino) {
            for fh in handle_ids {
                if let Some(open_file) = handles.get_mut(fh) {
                    open_file.open_attr = Some(attr.clone());
                }
            }
        }
    }

    /// Close a file handle. Returns `(ino, remaining_open_count)`.
    pub(super) fn close(&self, fh: u64) -> Option<(OpenFile, u32)> {
        let mut handles = self.handles.write();
        let open_file = handles.remove(&fh)?;
        let mut handles_by_inode = self.handles_by_inode.lock();
        if let Some(handle_ids) = handles_by_inode.get_mut(&open_file.ino) {
            handle_ids.remove(&fh);
            if handle_ids.is_empty() {
                handles_by_inode.remove(&open_file.ino);
            }
        }
        drop(handles_by_inode);
        drop(handles);
        let mut counts = self.counts.lock();
        let counts_for_inode = counts.get_mut(&open_file.ino)?;
        counts_for_inode.open = counts_for_inode.open.saturating_sub(1);
        if Self::flags_allow_write(open_file.flags) {
            assert!(
                counts_for_inode.write > 0,
                "write-capable handle closed without write-open count"
            );
            counts_for_inode.write = counts_for_inode.write.saturating_sub(1);
        }
        let remaining = counts_for_inode.open;
        if counts_for_inode.open == 0 {
            counts.remove(&open_file.ino);
        }
        Some((open_file, remaining))
    }

    /// Get the number of open handles for an inode.
    pub(super) fn open_count(&self, ino: Ino) -> u32 {
        self.counts
            .lock()
            .get(&ino)
            .map(|counts| counts.open)
            .unwrap_or(0)
    }

    /// Get the number of write-capable open handles for an inode.
    pub(super) fn write_open_count(&self, ino: Ino) -> u32 {
        self.counts
            .lock()
            .get(&ino)
            .map(|counts| counts.write)
            .unwrap_or(0)
    }

    /// Returns true when an inode still has a mutating handle whose local view
    /// must survive namespace removal until the handle is released.
    pub(super) fn has_active_open_overlay(&self, ino: Ino) -> bool {
        self.handles
            .read()
            .values()
            .any(|open_file| open_file.ino == ino && open_file.active_open_overlay)
    }

    pub(super) fn clear_read_windows_for_inode(&self, ino: Ino) {
        for open_file in self
            .handles
            .read()
            .values()
            .filter(|open_file| open_file.ino == ino)
        {
            open_file.clear_read_window();
        }
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
            .read()
            .values()
            .map(|f| f.current_access_class())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_table_open_close() {
        let ht = HandleTable::new();
        let fh1 = ht.open(10, OpenFlags::RDONLY, None, false);
        let fh2 = ht.open(10, OpenFlags::RDWR, None, true);
        let fh3 = ht.open(20, OpenFlags::WRONLY, None, true);

        assert_ne!(fh1.fh, fh2.fh);
        assert_ne!(fh2.fh, fh3.fh);

        assert_eq!(ht.open_count(10), 2);
        assert_eq!(ht.write_open_count(10), 1);
        assert_eq!(ht.open_count(20), 1);
        assert_eq!(ht.write_open_count(20), 1);
        assert_eq!(ht.handles_by_inode.lock().get(&10).unwrap().len(), 2);
        assert_eq!(ht.open_count(99), 0);
        assert_eq!(ht.write_open_count(99), 0);
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
        assert_eq!(ht.write_open_count(10), 1);

        let (closed2, remaining) = ht.close(fh2.fh).unwrap();
        assert_eq!(closed2.ino, 10);
        assert_eq!(remaining, 0);
        assert_eq!(ht.open_count(10), 0);
        assert_eq!(ht.write_open_count(10), 0);
        assert!(!ht.handles_by_inode.lock().contains_key(&10));
        assert!(!ht.has_active_open_overlay(10));

        let (closed3, remaining) = ht.close(fh3.fh).unwrap();
        assert_eq!(closed3.ino, 20);
        assert_eq!(remaining, 0);
        assert_eq!(ht.open_count(20), 0);
        assert_eq!(ht.write_open_count(20), 0);

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
