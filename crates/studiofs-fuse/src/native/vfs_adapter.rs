//! Native Linux FUSE operation adapter backed by StudioFS VFS contracts.

use std::sync::Arc;

use studiofs_bridge::posix::vfs_err_to_errno;
use studiofs_meta::{FileType, InodeAttr};
use studiofs_vfs::StudioVfs;

use crate::native::abi::{error_reply, ok_reply};
use crate::native::dispatcher::FuseOperationHandler;
use crate::native::protocol::{FuseOpcode, FuseReply, FuseRequest};

const FUSE_ENTRY_OUT_PAYLOAD_LEN: usize = 120;
const FUSE_ATTR_PAYLOAD_LEN: usize = 88;
const POSIX_STAT_BLOCK_SIZE: u64 = 512;

/// File kind used by the native operation adapter before Linux ABI encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeFileType {
    /// Regular file.
    RegularFile,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

/// Minimal native attribute shape needed to build Linux FUSE replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAttr {
    /// StudioFS inode number.
    pub ino: u64,
    /// File size in bytes.
    pub size: u64,
    /// File kind.
    pub kind: NativeFileType,
    /// POSIX permission bits.
    pub perm: u16,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
}

/// VFS operations required by the native Linux bridge operation layer.
pub trait NativeVfs {
    /// Resolve `name` under `parent`.
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32>;

    /// Fetch attributes for `ino`.
    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32>;
}

impl<T: NativeVfs + ?Sized> NativeVfs for Arc<T> {
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
        (**self).lookup(parent, name)
    }

    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32> {
        (**self).getattr(ino)
    }
}

impl NativeVfs for StudioVfs {
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
        StudioVfs::lookup(self, parent, name)
            .map(native_attr_from_inode_attr)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32> {
        StudioVfs::getattr(self, ino)
            .map(native_attr_from_inode_attr)
            .map_err(|error| vfs_err_to_errno(&error))
    }
}

/// Transport-independent Linux native bridge operation handler.
#[derive(Debug)]
pub struct LinuxNativeBridge<V> {
    vfs: V,
}

impl<V> LinuxNativeBridge<V> {
    /// Build an operation adapter around a VFS backend.
    pub fn new(vfs: V) -> Self {
        Self { vfs }
    }
}

impl<V: NativeVfs> FuseOperationHandler for LinuxNativeBridge<V> {
    fn handle(&mut self, request: FuseRequest) -> FuseReply {
        match request.opcode {
            FuseOpcode::Lookup => self.handle_lookup(request),
            FuseOpcode::Getattr => self.handle_getattr(request),
            _ => FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, libc::ENOSYS).to_vec(),
            ),
        }
    }
}

impl<V: NativeVfs> LinuxNativeBridge<V> {
    fn handle_lookup(&self, request: FuseRequest) -> FuseReply {
        let name = match lookup_name(&request.payload) {
            Some(name) => name,
            None => {
                return FuseReply::contiguous(
                    request.header.unique,
                    error_reply(request.header.unique, libc::EINVAL).to_vec(),
                );
            }
        };

        match self.vfs.lookup(request.header.nodeid, name) {
            Ok(attr) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &entry_payload(attr)),
            ),
            Err(errno) => FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, errno).to_vec(),
            ),
        }
    }

    fn handle_getattr(&self, request: FuseRequest) -> FuseReply {
        match self.vfs.getattr(request.header.nodeid) {
            Ok(attr) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &attr_out_payload(attr)),
            ),
            Err(errno) => FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, errno).to_vec(),
            ),
        }
    }
}

fn lookup_name(payload: &[u8]) -> Option<&[u8]> {
    let name = payload.strip_suffix(&[0]).unwrap_or(payload);
    (!name.is_empty()).then_some(name)
}

fn native_attr_from_inode_attr(attr: InodeAttr) -> NativeAttr {
    NativeAttr {
        ino: attr.ino,
        size: attr.size,
        kind: match attr.kind {
            FileType::RegularFile => NativeFileType::RegularFile,
            FileType::Directory => NativeFileType::Directory,
            FileType::Symlink => NativeFileType::Symlink,
        },
        perm: attr.mode & 0o7777,
        uid: attr.uid,
        gid: attr.gid,
    }
}

fn entry_payload(attr: NativeAttr) -> Vec<u8> {
    let mut payload = Vec::with_capacity(FUSE_ENTRY_OUT_PAYLOAD_LEN);
    payload.extend_from_slice(&attr.ino.to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    append_attr_payload(&mut payload, attr);
    payload
}

fn attr_out_payload(attr: NativeAttr) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16 + FUSE_ATTR_PAYLOAD_LEN);
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    append_attr_payload(&mut payload, attr);
    payload
}

fn append_attr_payload(payload: &mut Vec<u8>, attr: NativeAttr) {
    payload.reserve(FUSE_ATTR_PAYLOAD_LEN);
    payload.extend_from_slice(&attr.ino.to_le_bytes());
    payload.extend_from_slice(&attr.size.to_le_bytes());
    payload.extend_from_slice(&attr.size.div_ceil(POSIX_STAT_BLOCK_SIZE).to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&linux_mode(attr).to_le_bytes());
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&attr.uid.to_le_bytes());
    payload.extend_from_slice(&attr.gid.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&4096_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
}

fn linux_mode(attr: NativeAttr) -> u32 {
    let file_type = match attr.kind {
        NativeFileType::RegularFile => libc::S_IFREG,
        NativeFileType::Directory => libc::S_IFDIR,
        NativeFileType::Symlink => libc::S_IFLNK,
    };
    file_type | u32::from(attr.perm)
}
