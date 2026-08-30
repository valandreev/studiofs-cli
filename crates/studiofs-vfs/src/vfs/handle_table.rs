use crate::access::AccessClass;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use studiofs_meta::types::{Ino, InodeAttr};

use super::{OpenFile, OpenFlags, ReadAheadOwner, ReadAheadToken};

#[derive(Debug, Default)]
struct HandleCounts {
    open: u32,
    write: u32,
}

#[derive(Debug, Default)]
struct HandleTableData {
    handles: HashMap<u64, OpenFile>,
    handles_by_inode: HashMap<Ino, HashSet<u64>>,
    /// Inode -> currently open file descriptor counters.
    counts: HashMap<Ino, HandleCounts>,
}

/// Per-VFS handle table tracking all open file descriptors.
pub(super) struct HandleTable {
    /// Monotonically increasing file handle counter. Zero is reserved for
    /// bridge callbacks that use `fh == 0` as "no VFS handle".
    next_fh: AtomicU64,
    /// All open handles: fh -> OpenFile.
    handles: RwLock<HandleTableData>,
    /// Inodes that were unlinked while still open (deferred deletion).
    orphans: Mutex<HashSet<Ino>>,
}

impl HandleTable {
    pub(super) fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: RwLock::new(HandleTableData::default()),
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
        handles.handles.insert(fh, open_file.clone());
        handles.handles_by_inode.entry(ino).or_default().insert(fh);
        let counts_for_inode = handles.counts.entry(ino).or_default();
        counts_for_inode.open = counts_for_inode.open.saturating_add(1);
        if Self::flags_allow_write(flags) {
            counts_for_inode.write = counts_for_inode.write.saturating_add(1);
        }
        drop(handles);
        open_file
    }

    /// Look up an open file handle.
    pub(super) fn get(&self, fh: u64) -> Option<OpenFile> {
        self.handles.read().handles.get(&fh).cloned()
    }

    /// Mutate ownership only while `fh` is live in the table.
    ///
    /// The exclusive table lock is the linearization point: concurrent release
    /// or owner changes either win before this returns `Some` or observe the
    /// completed owner mutation and captured token afterwards.
    pub(super) fn set_read_ahead_owner_if_live(
        &self,
        fh: u64,
        owner: ReadAheadOwner,
    ) -> Option<(OpenFile, ReadAheadOwner, Option<ReadAheadToken>)> {
        // An exclusive table lock keeps concurrent owner changes and release
        // from interleaving between the mutation and owner-token capture.
        let handles = self.handles.write();
        let open_file = handles.handles.get(&fh)?;
        let previous = open_file.set_read_ahead_owner(owner);
        let owner_token = open_file.vfs_read_ahead_token();
        Some((open_file.clone(), previous, owner_token))
    }

    /// Refresh the attribute snapshot carried by an already-open handle.
    pub(super) fn update_open_attr(&self, fh: u64, attr: InodeAttr) {
        if let Some(open_file) = self.handles.read().handles.get(&fh) {
            open_file.set_open_attr(attr);
        }
    }

    /// Refresh the attribute snapshot carried by every handle for an inode.
    pub(super) fn update_open_attrs_for_inode(&self, ino: Ino, attr: &InodeAttr) {
        let handles = self.handles.read();
        if let Some(handle_ids) = handles.handles_by_inode.get(&ino) {
            let attr = Arc::new(attr.clone());
            for fh in handle_ids {
                if let Some(open_file) = handles.handles.get(fh) {
                    open_file.set_shared_open_attr(Arc::clone(&attr));
                }
            }
        }
    }

    /// Close a file handle. Returns `(ino, remaining_open_count)`.
    #[cfg(test)]
    pub(super) fn close(&self, fh: u64) -> Option<(OpenFile, u32)> {
        self.close_with(fh, |_, _| {})
    }

    /// Close a file handle, invoking `before_remove` while the table lock still
    /// makes the handle and its last-handle status visible.
    pub(super) fn close_with<F>(&self, fh: u64, before_remove: F) -> Option<(OpenFile, u32)>
    where
        F: FnOnce(&OpenFile, u32),
    {
        let mut handles = self.handles.write();
        let open_file = handles.handles.get(&fh)?;
        let remaining = handles
            .counts
            .get(&open_file.ino)
            .expect("inode must exist in counts if handle existed")
            .open
            .saturating_sub(1);
        before_remove(open_file, remaining);
        let open_file = handles
            .handles
            .remove(&fh)
            .expect("handle must remain present while the table is locked");
        if let Some(handle_ids) = handles.handles_by_inode.get_mut(&open_file.ino) {
            handle_ids.remove(&fh);
            if handle_ids.is_empty() {
                handles.handles_by_inode.remove(&open_file.ino);
            }
        }
        let counts_for_inode = handles
            .counts
            .get_mut(&open_file.ino)
            .expect("inode must exist in counts if handle existed");
        counts_for_inode.open = counts_for_inode.open.saturating_sub(1);
        if Self::flags_allow_write(open_file.flags) {
            assert!(
                counts_for_inode.write > 0,
                "write-capable handle closed without write-open count"
            );
            counts_for_inode.write = counts_for_inode.write.saturating_sub(1);
        }
        debug_assert_eq!(counts_for_inode.open, remaining);
        if counts_for_inode.open == 0 {
            handles.counts.remove(&open_file.ino);
        }
        Some((open_file, remaining))
    }

    /// Get the number of open handles for an inode.
    pub(super) fn open_count(&self, ino: Ino) -> u32 {
        self.handles
            .read()
            .counts
            .get(&ino)
            .map(|counts| counts.open)
            .unwrap_or(0)
    }

    /// Get the number of write-capable open handles for an inode.
    pub(super) fn write_open_count(&self, ino: Ino) -> u32 {
        self.handles
            .read()
            .counts
            .get(&ino)
            .map(|counts| counts.write)
            .unwrap_or(0)
    }

    /// Returns true when an inode still has a mutating handle whose local view
    /// must survive namespace removal until the handle is released.
    pub(super) fn has_active_open_overlay(&self, ino: Ino) -> bool {
        let handles = self.handles.read();
        handles
            .handles_by_inode
            .get(&ino)
            .is_some_and(|handle_ids| {
                handle_ids.iter().any(|fh| {
                    handles
                        .handles
                        .get(fh)
                        .is_some_and(|open_file| open_file.active_open_overlay)
                })
            })
    }

    pub(super) fn clear_read_windows_for_inode(&self, ino: Ino) {
        for open_file in self
            .handles
            .read()
            .handles
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
    ///
    /// This is transient last-handle release routing, not durable cleanup-retry
    /// state; object reachability GC does not consult the handle table.
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
            .handles
            .values()
            .map(|f| f.current_access_class())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn handle_counts_are_stored_with_handle_table_state() {
        let ht = HandleTable::new();
        let opened = ht.open(42, OpenFlags::RDWR, None, false);

        {
            let table = ht.handles.read();
            let counts = table
                .counts
                .get(&42)
                .expect("open handle must have an inode count");
            assert_eq!(counts.open, 1);
            assert_eq!(counts.write, 1);
        }

        let (_, remaining) = ht.close(opened.fh).expect("opened handle must be closable");
        assert_eq!(remaining, 0);
        assert!(!ht.handles.read().counts.contains_key(&42));
    }

    #[test]
    fn close_with_calls_last_handle_callback_before_removing_handle() {
        let ht = HandleTable::new();
        let opened = ht.open(42, OpenFlags::RDWR, None, false);

        let closed = ht.close_with(opened.fh, |open_file, remaining| {
            assert_eq!(open_file.fh, opened.fh);
            assert_eq!(remaining, 0);
        });

        assert!(closed.is_some());
        assert!(ht.get(opened.fh).is_none());
    }

    #[test]
    fn concurrent_open_and_close_preserve_handle_counts() {
        for _ in 0..100 {
            let ht = Arc::new(HandleTable::new());
            let opener_ht = Arc::clone(&ht);
            let opener = thread::spawn(move || opener_ht.open(42, OpenFlags::RDWR, None, false));

            let deadline = Instant::now() + Duration::from_secs(1);
            while ht.next_fh.load(Ordering::Relaxed) <= 1 {
                assert!(
                    Instant::now() < deadline,
                    "open thread did not allocate a handle"
                );
                thread::yield_now();
            }

            let closer_ht = Arc::clone(&ht);
            let closer = thread::spawn(move || {
                loop {
                    if Instant::now() >= deadline {
                        return Err("timeout");
                    }
                    let close_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            closer_ht.close(1)
                        }));
                    match close_result {
                        Ok(Some(_)) => return Ok(()),
                        Ok(None) => thread::yield_now(),
                        Err(_) => return Err("panic"),
                    }
                }
            });

            let opened = opener.join().expect("open thread must not panic");
            match closer.join() {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => panic!("concurrent close failed: {reason}"),
                Err(_) => panic!("close thread must not panic"),
            }
            assert_eq!(ht.open_count(42), 0);
            assert_eq!(ht.write_open_count(42), 0);
            assert_eq!(opened.ino, 42);
        }
    }

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
        assert_eq!(
            ht.handles.read().handles_by_inode.get(&10).unwrap().len(),
            2
        );
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
        assert!(!ht.handles.read().handles_by_inode.contains_key(&10));
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
