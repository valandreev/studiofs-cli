//! Attributes of directories the kernel still holds open.
//!
//! A replacement rename or `rmdir` removes the directory inode from metadata
//! while an `opendir` handle may still reference it. Linux sends `fstat` on that
//! handle as an inode-only FUSE_GETATTR, so the adapter answers from the last
//! attributes it observed with `nlink = 0`, matching local filesystems. A
//! removed directory the kernel still references by lookup (for example a
//! process's working directory) stays observable the same way until FORGET.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use studiofs_meta::{FileType, InodeAttr};

use crate::invalidation::{LIVENESS_SHARDS, liveness_shard, lock_poison_tolerant};

struct OpenDirectory {
    handles: u64,
    removed: bool,
    attr: InodeAttr,
}

type Shard = Mutex<HashMap<u64, OpenDirectory>>;

pub(crate) struct OpenDirectoryAttrs {
    shards: Arc<[Shard; LIVENESS_SHARDS]>,
}

impl Default for OpenDirectoryAttrs {
    fn default() -> Self {
        Self {
            shards: Arc::new(std::array::from_fn(|_| Mutex::new(HashMap::new()))),
        }
    }
}

impl OpenDirectoryAttrs {
    /// Track one directory handle; the snapshot lives until every guard drops.
    pub(crate) fn open(&self, attr: &InodeAttr) -> OpenDirectoryAttrGuard {
        let ino = attr.ino;
        lock_poison_tolerant(shard(&self.shards, ino))
            .entry(ino)
            .and_modify(|open| {
                open.handles += 1;
                open.attr = attr.clone();
            })
            .or_insert_with(|| OpenDirectory {
                handles: 1,
                removed: false,
                attr: attr.clone(),
            });
        OpenDirectoryAttrGuard {
            shards: Arc::clone(&self.shards),
            ino,
        }
    }

    /// Refresh the snapshot of an open directory from attributes replied to the kernel.
    pub(crate) fn observe(&self, attr: &InodeAttr) {
        if attr.kind != FileType::Directory {
            return;
        }
        if let Some(open) = lock_poison_tolerant(shard(&self.shards, attr.ino)).get_mut(&attr.ino) {
            open.attr = attr.clone();
        }
    }

    /// Keep a removed directory observable while the kernel still looks it up.
    pub(crate) fn retain_removed(&self, attr: &InodeAttr) {
        let mut shard = lock_poison_tolerant(shard(&self.shards, attr.ino));
        let entry = shard.entry(attr.ino).or_insert_with(|| OpenDirectory {
            handles: 0,
            removed: true,
            attr: attr.clone(),
        });
        entry.removed = true;
    }

    /// Drop a removed directory's snapshot once the kernel forgot it.
    pub(crate) fn release_removed(&self, ino: u64) {
        let mut shard = lock_poison_tolerant(shard(&self.shards, ino));
        if let Some(entry) = shard.get_mut(&ino) {
            entry.removed = false;
            if entry.handles == 0 {
                shard.remove(&ino);
            }
        }
    }

    /// Attributes of an open directory whose inode left the namespace.
    pub(crate) fn displaced(&self, ino: u64) -> Option<InodeAttr> {
        lock_poison_tolerant(shard(&self.shards, ino))
            .get(&ino)
            .map(|open| {
                let mut attr = open.attr.clone();
                attr.nlink = 0;
                attr
            })
    }
}

pub(crate) struct OpenDirectoryAttrGuard {
    shards: Arc<[Shard; LIVENESS_SHARDS]>,
    ino: u64,
}

impl Drop for OpenDirectoryAttrGuard {
    fn drop(&mut self) {
        let mut shard = lock_poison_tolerant(shard(&self.shards, self.ino));
        if let Some(open) = shard.get_mut(&self.ino) {
            open.handles -= 1;
            if open.handles == 0 && !open.removed {
                shard.remove(&self.ino);
            }
        }
    }
}

fn shard(shards: &[Shard; LIVENESS_SHARDS], ino: u64) -> &Shard {
    &shards[liveness_shard(ino)]
}
