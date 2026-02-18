#![cfg(windows)]
//! FileSystemContext implementation — bridges WinFSP calls to StudioVfs.

#![allow(deprecated)]
use std::ffi::c_void;
use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_meta::{FileType, Ino, InodeAttr, ROOT_INO};
use studiofs_vfs::StudioVfs;
use tokio::runtime::Runtime;
use tracing::{debug, error};
use windows::Win32::Foundation::NTSTATUS;
use winfsp::filesystem::*;
use winfsp::U16CStr;
use winfsp_sys::{FILE_ACCESS_RIGHTS, FILE_FLAGS_AND_ATTRIBUTES};

// ─── NTSTATUS constants ──────────────────────────────────────────────────────

const STATUS_OBJECT_NAME_NOT_FOUND: NTSTATUS = NTSTATUS(0xC000_0034_u32 as i32);
const STATUS_OBJECT_NAME_COLLISION: NTSTATUS = NTSTATUS(0xC000_0035_u32 as i32);
const STATUS_DIRECTORY_NOT_EMPTY: NTSTATUS = NTSTATUS(0xC000_0101_u32 as i32);
const STATUS_NOT_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_0103_u32 as i32);
const STATUS_UNSUCCESSFUL: NTSTATUS = NTSTATUS(0xC000_0001_u32 as i32);

// ─── File Handle ─────────────────────────────────────────────────────────────

/// Per-open-file context.
pub struct StudioFileHandle {
    ino: Ino,
    #[allow(dead_code)]
    is_dir: bool,
    /// Cached directory entries for readdir pagination.
    dir_entries: Mutex<Option<Vec<studiofs_meta::DirEntry>>>,
}

impl StudioFileHandle {
    fn new(ino: Ino, is_dir: bool) -> Self {
        Self {
            ino,
            is_dir,
            dir_entries: Mutex::new(None),
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert InodeAttr to WinFSP FileInfo.
fn attr_to_file_info(attr: &InodeAttr) -> FileInfo {
    let mut info = FileInfo::default();
    info.file_size = attr.size;
    info.allocation_size = (attr.size + 4095) & !4095;

    info.file_attributes = match attr.kind {
        FileType::Directory => 0x10, // FILE_ATTRIBUTE_DIRECTORY
        _ => 0x80,                   // FILE_ATTRIBUTE_NORMAL
    };

    // Convert timestamps (nanoseconds since epoch → Windows FILETIME)
    // FILETIME is 100ns intervals since 1601-01-01
    info.creation_time = unix_nanos_to_filetime(attr.ctime);
    info.last_access_time = unix_nanos_to_filetime(attr.atime);
    info.last_write_time = unix_nanos_to_filetime(attr.mtime);
    info.change_time = unix_nanos_to_filetime(attr.mtime);

    // Use inode as index
    info.index_number = attr.ino;
    info.hard_links = 0; // WinFSP doesn't use hard links

    info
}

/// Convert Unix timestamp (nanoseconds since epoch) to Windows FILETIME (100ns since 1601-01-01).
fn unix_nanos_to_filetime(unix_nanos: i64) -> u64 {
    const EPOCH_DIFF: u64 = 116_444_736_000_000_000; // Difference between 1601-01-01 and 1970-01-01 in 100ns intervals
    const NANOS_PER_100NS: i64 = 100;

    // Convert nanoseconds to 100ns intervals
    let intervals = unix_nanos / NANOS_PER_100NS;
    (intervals as u64).saturating_add(EPOCH_DIFF)
}

/// Resolve a WinFSP path to an inode by walking the directory tree.
fn resolve_path(vfs: &StudioVfs, path: &U16CStr) -> winfsp::Result<InodeAttr> {
    let s = path.to_string_lossy();
    let s = s.trim_start_matches('\\');

    if s.is_empty() {
        return vfs.getattr(ROOT_INO).map_err(vfs_to_winfsp_err);
    }

    let mut current_ino = ROOT_INO;
    for component in s.split('\\') {
        if component.is_empty() {
            continue;
        }
        let attr = vfs
            .lookup(current_ino, component.as_bytes())
            .map_err(vfs_to_winfsp_err)?;
        current_ino = attr.ino;
    }

    vfs.getattr(current_ino).map_err(vfs_to_winfsp_err)
}

/// Resolve the parent directory inode for a given path.
fn resolve_parent(vfs: &StudioVfs, path: &U16CStr) -> winfsp::Result<(Ino, Vec<u8>)> {
    let s = path.to_string_lossy();
    let s = s.trim_start_matches('\\');

    let components: Vec<&str> = s.split('\\').filter(|c| !c.is_empty()).collect();
    if components.is_empty() {
        return Err(winfsp::FspError::from(STATUS_OBJECT_NAME_NOT_FOUND));
    }

    let name = components.last().unwrap().as_bytes().to_vec();
    let mut parent_ino = ROOT_INO;

    for component in &components[..components.len() - 1] {
        let attr = vfs
            .lookup(parent_ino, component.as_bytes())
            .map_err(vfs_to_winfsp_err)?;
        parent_ino = attr.ino;
    }

    Ok((parent_ino, name))
}

/// Map VfsError to WinFSP NTSTATUS error.
fn vfs_to_winfsp_err(e: studiofs_vfs::VfsError) -> winfsp::FspError {
    match &e {
        studiofs_vfs::VfsError::Meta(me) => match me {
            studiofs_meta::MetaError::InodeNotFound(_) => {
                winfsp::FspError::from(STATUS_OBJECT_NAME_NOT_FOUND)
            }
            studiofs_meta::MetaError::EntryNotFound { .. } => {
                winfsp::FspError::from(STATUS_OBJECT_NAME_NOT_FOUND)
            }
            studiofs_meta::MetaError::EntryExists { .. } => {
                winfsp::FspError::from(STATUS_OBJECT_NAME_COLLISION)
            }
            studiofs_meta::MetaError::DirNotEmpty(_) => {
                winfsp::FspError::from(STATUS_DIRECTORY_NOT_EMPTY)
            }
            studiofs_meta::MetaError::NotADirectory(_) => {
                winfsp::FspError::from(STATUS_NOT_A_DIRECTORY)
            }
            _ => {
                error!("metadata error: {e}");
                winfsp::FspError::from(STATUS_UNSUCCESSFUL)
            }
        },
        _ => {
            error!("vfs error: {e}");
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
    }
}

// ─── FileSystemContext ───────────────────────────────────────────────────────

/// The WinFSP filesystem context — holds VFS and tokio runtime.
pub struct StudioFsContext {
    vfs: Arc<StudioVfs>,
    rt: Arc<Runtime>,
}

impl StudioFsContext {
    pub fn new(vfs: Arc<StudioVfs>, rt: Arc<Runtime>) -> Self {
        Self { vfs, rt }
    }
}

impl FileSystemContext for StudioFsContext {
    type FileContext = StudioFileHandle;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        resolve_reparse_points: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        if let Some(security) = resolve_reparse_points(file_name) {
            return Ok(security);
        }

        let attr = resolve_path(&self.vfs, file_name)?;
        let file_attributes = match attr.kind {
            FileType::Directory => 0x10u32,
            _ => 0x80u32,
        };

        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes: file_attributes,
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let attr = resolve_path(&self.vfs, file_name)?;
        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;

        debug!(ino = attr.ino, "open {:?}", file_name.to_string_lossy());
        Ok(StudioFileHandle::new(
            attr.ino,
            attr.kind == FileType::Directory,
        ))
    }

    fn close(&self, context: Self::FileContext) {
        debug!(ino = context.ino, "close");
        drop(context);
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        _file_attributes: FILE_FLAGS_AND_ATTRIBUTES,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        _extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let (parent_ino, name) = resolve_parent(&self.vfs, file_name)?;

        let is_dir = (create_options & 0x01) != 0; // FILE_DIRECTORY_FILE
        let attr = if is_dir {
            self.vfs
                .mkdir(parent_ino, &name, 0, 0, 0o755)
                .map_err(vfs_to_winfsp_err)?
        } else {
            self.vfs
                .create(parent_ino, &name, 0, 0, 0o644)
                .map_err(vfs_to_winfsp_err)?
        };

        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;

        debug!(
            ino = attr.ino,
            is_dir,
            "create {:?}",
            file_name.to_string_lossy()
        );
        Ok(StudioFileHandle::new(attr.ino, is_dir))
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn read(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        let data = self
            .rt
            .block_on(self.vfs.read(context.ino, offset, buffer.len() as u32))
            .map_err(vfs_to_winfsp_err)?;

        let len = data.len().min(buffer.len());
        buffer[..len].copy_from_slice(&data[..len]);

        debug!(ino = context.ino, offset, len, "read");
        Ok(len as u32)
    }

    fn write(
        &self,
        context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        _write_to_eof: bool,
        _constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<u32> {
        let written = self
            .rt
            .block_on(self.vfs.write(context.ino, offset, buffer))
            .map_err(vfs_to_winfsp_err)?;

        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);

        debug!(ino = context.ino, offset, written, "write");
        Ok(written)
    }

    fn read_directory(
        &self,
        context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        marker: DirMarker,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        // Cache entries for continued readdir
        let mut cached = context.dir_entries.lock();
        if cached.is_none() {
            let entries = self.vfs.readdir(context.ino).map_err(vfs_to_winfsp_err)?;
            *cached = Some(entries);
        }

        let entries = cached.as_ref().unwrap();
        let mut cursor = 0u32;

        let has_marker = !marker.is_none();

        // Add "." and ".." first (only on first call, no marker)
        if !has_marker {
            let mut dot = DirInfo::<255>::new();
            if dot.set_name(std::ffi::OsStr::new(".")).is_ok() {
                let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
                *dot.file_info_mut() = attr_to_file_info(&attr);
                if !dot.append_to_buffer(buffer, &mut cursor) {
                    DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
                    return Ok(cursor);
                }
            }

            let mut dotdot = DirInfo::<255>::new();
            if dotdot.set_name(std::ffi::OsStr::new("..")).is_ok() {
                if let Ok(attr) = self.vfs.getattr(context.ino) {
                    *dotdot.file_info_mut() = attr_to_file_info(&attr);
                    if !dotdot.append_to_buffer(buffer, &mut cursor) {
                        DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
                        return Ok(cursor);
                    }
                }
            }
        }

        // Get the marker name for comparison
        let marker_str: Option<String> = marker
            .inner_as_cstr()
            .map(|c| c.to_string_lossy().to_string());

        for entry in entries {
            let entry_name = String::from_utf8_lossy(&entry.name).to_string();

            // Skip entries at or before the marker
            if let Some(ref m) = marker_str {
                if entry_name.as_str() <= m.as_str() {
                    continue;
                }
            }

            let os_name = std::ffi::OsString::from(&entry_name);
            let mut dir_info = DirInfo::<255>::new();
            if dir_info.set_name(&os_name).is_err() {
                continue;
            }

            let child_attr = match self.vfs.getattr(entry.ino) {
                Ok(a) => a,
                Err(_) => continue,
            };
            *dir_info.file_info_mut() = attr_to_file_info(&child_attr);

            if !dir_info.append_to_buffer(buffer, &mut cursor) {
                break;
            }
        }

        DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
        Ok(cursor)
    }

    fn set_delete(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        delete_file: bool,
    ) -> winfsp::Result<()> {
        if delete_file && context.is_dir {
            // Check if directory is empty
            let entries = self.vfs.readdir(context.ino).map_err(vfs_to_winfsp_err)?;
            if !entries.is_empty() {
                return Err(winfsp::FspError::from(STATUS_DIRECTORY_NOT_EMPTY));
            }
        }
        Ok(())
    }

    fn cleanup(&self, context: &Self::FileContext, file_name: Option<&U16CStr>, flags: u32) {
        // Clear cached dir entries
        *context.dir_entries.lock() = None;

        // FspCleanupDelete = 0x01
        if flags & 0x01 != 0 {
            if let Some(fname) = file_name {
                if let Ok((parent_ino, name)) = resolve_parent(&self.vfs, fname) {
                    let res = if context.is_dir {
                        self.vfs.rmdir(parent_ino, &name)
                    } else {
                        self.vfs.unlink(parent_ino, &name)
                    };

                    if let Err(e) = res {
                        error!("cleanup delete failed: {:?}", e);
                    } else {
                        debug!(ino = context.ino, "cleanup delete");
                    }
                }
            }
        } else {
            // Not deleting — flush buffered writes
            // WinFSP "Cleanup" with no delete flag is the standard place to flush.
            // We use block_on because we are in a WinFSP thread context.
            if let Err(e) = self.rt.block_on(self.vfs.flush(context.ino)) {
                error!(ino = context.ino, "cleanup flush failed: {:?}", e);
            }
        }
    }

    fn overwrite(
        &self,
        context: &Self::FileContext,
        _file_attributes: FILE_FLAGS_AND_ATTRIBUTES,
        _replace_file_attributes: bool,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // Truncate to 0
        let attr = self
            .vfs
            .setattr(context.ino, Some(0))
            .map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        _last_access_time: u64,
        _last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn set_file_size(
        &self,
        context: &Self::FileContext,
        new_size: u64,
        _set_allocation_size: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self
            .vfs
            .setattr(context.ino, Some(new_size))
            .map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn rename(
        &self,
        _context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        _replace_if_exists: bool,
    ) -> winfsp::Result<()> {
        let (old_parent, old_name) = resolve_parent(&self.vfs, file_name)?;
        let (new_parent, new_name) = resolve_parent(&self.vfs, new_file_name)?;

        self.vfs
            .rename(old_parent, &old_name, new_parent, &new_name)
            .map_err(vfs_to_winfsp_err)?;

        debug!(
            "rename {:?} -> {:?}",
            file_name.to_string_lossy(),
            new_file_name.to_string_lossy()
        );
        Ok(())
    }

    fn flush(
        &self,
        context: Option<&Self::FileContext>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if let Some(ctx) = context {
            // Flush pending writes to S3
            self.rt
                .block_on(self.vfs.flush(ctx.ino))
                .map_err(vfs_to_winfsp_err)?;

            let attr = self.vfs.getattr(ctx.ino).map_err(vfs_to_winfsp_err)?;
            *file_info = attr_to_file_info(&attr);
            debug!(ino = ctx.ino, "flush");
        }
        Ok(())
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> winfsp::Result<()> {
        out_volume_info.total_size = 1024 * 1024 * 1024 * 1024; // 1 TB
        out_volume_info.free_size = 1024 * 1024 * 1024 * 512; // 512 GB

        // Use volume name from VFS
        let name = self
            .vfs
            .volume_name()
            .unwrap_or_else(|_| "StudioFS".to_string());
        out_volume_info.set_volume_label(&name);
        Ok(())
    }
}
