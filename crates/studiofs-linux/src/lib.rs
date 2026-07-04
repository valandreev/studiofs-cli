//! Clean Linux bridge shell for StudioFS.
//!
//! This crate intentionally stops before the real `/dev/fuse` transport. It
//! owns the Linux-only boundary shape and enough lifecycle/root operations to
//! prove the new path is isolated from the deleted legacy backend.

#![forbid(unsafe_code)]

use std::io;
use std::sync::Arc;

use studiofs_bridge::posix::{BridgeFileAttr, to_bridge_attr, vfs_err_to_errno};
use studiofs_meta::ROOT_INO;
use studiofs_vfs::StudioVfs;

const DEFAULT_BLOCK_SIZE: u32 = 4096;

/// Linux bridge launch request owned by the CLI boundary.
pub struct LinuxMountRequest {
    /// Shared VFS instance used by the Linux bridge boundary.
    pub vfs: Arc<StudioVfs>,
}

/// Minimal Linux mount shell for lifecycle and root namespace smoke checks.
pub struct LinuxMountShell {
    request: LinuxMountRequest,
}

/// Directory entry shape returned by the Linux shell boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxDirEntry {
    /// StudioFS inode number.
    pub ino: u64,
    /// Raw POSIX entry name bytes.
    pub name: Vec<u8>,
}

/// Start the Linux shell and return the clear unsupported transport error.
pub fn mount_with_request(_request: LinuxMountRequest) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "StudioFS Linux /dev/fuse transport is not implemented yet",
    ))
}

impl LinuxMountShell {
    /// Create an uninitialized Linux shell from a mount request.
    pub fn new(request: LinuxMountRequest) -> Self {
        Self { request }
    }

    /// Accept the Linux init lifecycle callback.
    pub fn init(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return bridge attributes for an inode.
    pub fn getattr(&self, ino: u64) -> io::Result<BridgeFileAttr> {
        self.request
            .vfs
            .getattr(ino)
            .map(|attr| to_bridge_attr(&attr, 0, 0, DEFAULT_BLOCK_SIZE))
            .map_err(io_error_from_vfs)
    }

    /// Resolve a child name under a parent inode.
    pub fn lookup(&self, parent: u64, name: &[u8]) -> io::Result<BridgeFileAttr> {
        if parent == ROOT_INO && matches!(name, b"." | b"") {
            return self.getattr(ROOT_INO);
        }
        self.request
            .vfs
            .lookup(parent, name)
            .map(|attr| to_bridge_attr(&attr, 0, 0, DEFAULT_BLOCK_SIZE))
            .map_err(io_error_from_vfs)
    }

    /// Read directory entries for an inode.
    pub fn readdir(&self, ino: u64) -> io::Result<Vec<LinuxDirEntry>> {
        self.request
            .vfs
            .readdir(ino)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| LinuxDirEntry {
                        ino: entry.ino,
                        name: entry.name,
                    })
                    .collect()
            })
            .map_err(io_error_from_vfs)
    }

    /// Accept the Linux destroy lifecycle callback.
    pub fn destroy(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return `ENOSYS` for shell operations not implemented yet.
    pub fn unsupported() -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

fn io_error_from_vfs(error: studiofs_vfs::VfsError) -> io::Error {
    io::Error::from_raw_os_error(vfs_err_to_errno(&error))
}
