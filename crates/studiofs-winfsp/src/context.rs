#![cfg(windows)]
//! FileSystemContext implementation — bridges WinFSP calls to StudioVfs.

use std::ffi::c_void;
use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_meta::types::{SetattrArgs, TimeSpec};
use studiofs_meta::{FileType, Ino, InodeAttr, ROOT_INO};
use studiofs_vfs::{OpenFlags, StudioVfs};

// Windows reparse point constants
const IO_REPARSE_TAG_SYMLINK: u32 = 0xA000_000C;
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
use tokio::runtime::Runtime;
use tracing::{debug, error};
use windows::Win32::Foundation::NTSTATUS;
use winfsp::U16CStr;
use winfsp::filesystem::*;
use winfsp_sys::{FILE_ACCESS_RIGHTS, FILE_FLAGS_AND_ATTRIBUTES};

// ─── NTSTATUS constants ──────────────────────────────────────────────────────

const STATUS_OBJECT_NAME_NOT_FOUND: NTSTATUS = NTSTATUS(0xC000_0034_u32 as i32);
const STATUS_OBJECT_NAME_COLLISION: NTSTATUS = NTSTATUS(0xC000_0035_u32 as i32);
const STATUS_DIRECTORY_NOT_EMPTY: NTSTATUS = NTSTATUS(0xC000_0101_u32 as i32);
const STATUS_NOT_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_0103_u32 as i32);
const STATUS_FILE_IS_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_00BA_u32 as i32);
const STATUS_UNRECOGNIZED_VOLUME: NTSTATUS = NTSTATUS(0xC000_014F_u32 as i32);
const STATUS_UNSUCCESSFUL: NTSTATUS = NTSTATUS(0xC000_0001_u32 as i32);
const STATUS_INVALID_PARAMETER: NTSTATUS = NTSTATUS(0xC000_000D_u32 as i32);
const STATUS_NOT_A_REPARSE_POINT: NTSTATUS = NTSTATUS(0xC000_0275_u32 as i32);

// ─── File Handle ─────────────────────────────────────────────────────────────

/// Per-open-file context.
pub struct StudioFileHandle {
    ino: Ino,
    #[allow(dead_code)]
    is_dir: bool,
    /// VFS file handle (0 for directories).
    fh: u64,
    /// Cached directory entries for readdir pagination.
    dir_entries: Mutex<Option<Vec<studiofs_meta::DirEntry>>>,
}

impl StudioFileHandle {
    fn new(ino: Ino, is_dir: bool, fh: u64) -> Self {
        Self {
            ino,
            is_dir,
            fh,
            dir_entries: Mutex::new(None),
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert InodeAttr to WinFSP FileInfo.
fn attr_to_file_info(attr: &InodeAttr) -> FileInfo {
    // Directories always report at least one allocation unit; files round up.
    let mut info = FileInfo {
        file_size: attr.size,
        allocation_size: if attr.kind == FileType::Directory {
            4096u64.max((attr.size + 4095) & !4095)
        } else {
            (attr.size + 4095) & !4095
        },
        file_attributes: match attr.kind {
            FileType::Directory => 0x10, // FILE_ATTRIBUTE_DIRECTORY
            FileType::Symlink => FILE_ATTRIBUTE_REPARSE_POINT,
            _ => 0x80, // FILE_ATTRIBUTE_NORMAL
        },
        ..Default::default()
    };

    if attr.kind == FileType::Symlink {
        info.reparse_tag = IO_REPARSE_TAG_SYMLINK;
    }

    // Convert timestamps (nanoseconds since epoch → Windows FILETIME)
    // FILETIME is 100ns intervals since 1601-01-01
    info.creation_time = unix_nanos_to_filetime(attr.ctime);
    info.last_access_time = unix_nanos_to_filetime(attr.atime);
    info.last_write_time = unix_nanos_to_filetime(attr.mtime);
    info.change_time = unix_nanos_to_filetime(attr.mtime);

    // Use inode as index
    info.index_number = attr.ino;
    // Report actual nlink count (must be ≥1 for files, ≥2 for dirs)
    info.hard_links = if attr.nlink > 0 { attr.nlink } else { 1 };

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
///
/// Handles `.` (current dir), `..` (parent dir), and strips trailing
/// dots/spaces from path components (standard Windows path semantics).
fn resolve_path(vfs: &StudioVfs, path: &U16CStr) -> winfsp::Result<InodeAttr> {
    let s = path.to_string_lossy();
    let s = s.trim_start_matches('\\');

    if s.is_empty() {
        return vfs.getattr(ROOT_INO).map_err(vfs_to_winfsp_err);
    }

    let mut ino_stack: Vec<u64> = vec![ROOT_INO];

    for component in s.split('\\') {
        if component.is_empty() || component == "." {
            continue;
        }
        if component == ".." {
            if ino_stack.len() > 1 {
                ino_stack.pop();
            }
            continue;
        }
        // Strip trailing dots and spaces (Win32 path normalisation)
        let component = component.trim_end_matches(['.', ' ']);
        if component.is_empty() {
            continue;
        }
        let current_ino = *ino_stack.last().unwrap();
        let attr = vfs
            .lookup(current_ino, component.as_bytes())
            .map_err(vfs_to_winfsp_err)?;
        ino_stack.push(attr.ino);
    }

    let final_ino = *ino_stack.last().unwrap();
    vfs.getattr(final_ino).map_err(vfs_to_winfsp_err)
}

/// Resolve the parent directory inode for a given path.
///
/// Handles `.`, `..`, and trailing-dot/space stripping like `resolve_path`.
fn resolve_parent(vfs: &StudioVfs, path: &U16CStr) -> winfsp::Result<(Ino, Vec<u8>)> {
    let s = path.to_string_lossy();
    let s = s.trim_start_matches('\\');

    // Normalise components: skip `.`, resolve `..`, strip trailing dots/spaces.
    let mut components: Vec<String> = Vec::new();
    for raw in s.split('\\') {
        if raw.is_empty() || raw == "." {
            continue;
        }
        if raw == ".." {
            components.pop();
            continue;
        }
        let trimmed = raw.trim_end_matches(['.', ' ']);
        if !trimmed.is_empty() {
            components.push(trimmed.to_string());
        }
    }

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

/// Encode a symlink target as a Windows REPARSE_DATA_BUFFER (IO_REPARSE_TAG_SYMLINK).
///
/// Layout:
///   ReparseTag                  (u32) = IO_REPARSE_TAG_SYMLINK
///   ReparseDataLength           (u16)
///   Reserved                    (u16) = 0
///   SubstituteNameOffset        (u16)
///   SubstituteNameLength        (u16)
///   PrintNameOffset             (u16)
///   PrintNameLength             (u16)
///   Flags                       (u32) = SYMLINK_FLAG_RELATIVE (1)
///   PathBuffer                  (WCHAR[])
fn encode_symlink_reparse(target: &[u8]) -> Vec<u8> {
    // Convert target from UTF-8 bytes to a Windows path string
    let target_str = String::from_utf8_lossy(target);
    // Replace / with \ for Windows
    let win_path = target_str.replace('/', "\\");
    // Encode as UTF-16LE
    let utf16: Vec<u16> = win_path.encode_utf16().collect();
    let name_bytes = utf16.len() * 2; // byte length of UTF-16 name

    // SubstituteName = target, PrintName = target (same content, different offsets)
    let substitute_offset: u16 = 0;
    let substitute_len = name_bytes as u16;
    let print_offset = substitute_len;
    let print_len = substitute_len;

    // Symlink-specific data = 4 offsets (2 bytes each) + flags (4 bytes) + 2× path
    let data_len: u16 = 4 * 2 + 4 + substitute_len + print_len;

    // Total: 8 (header) + data_len
    let total = 8 + data_len as usize;
    let mut buf = vec![0u8; total];

    // ReparseTag
    buf[0..4].copy_from_slice(&IO_REPARSE_TAG_SYMLINK.to_le_bytes());
    // ReparseDataLength
    buf[4..6].copy_from_slice(&data_len.to_le_bytes());
    // Reserved = 0 (already zero)
    // SubstituteNameOffset
    buf[8..10].copy_from_slice(&substitute_offset.to_le_bytes());
    // SubstituteNameLength
    buf[10..12].copy_from_slice(&substitute_len.to_le_bytes());
    // PrintNameOffset
    buf[12..14].copy_from_slice(&print_offset.to_le_bytes());
    // PrintNameLength
    buf[14..16].copy_from_slice(&print_len.to_le_bytes());
    // Flags = SYMLINK_FLAG_RELATIVE = 1
    buf[16..20].copy_from_slice(&1u32.to_le_bytes());
    // PathBuffer: SubstituteName then PrintName
    let path_buf_offset = 20;
    for (i, &wc) in utf16.iter().enumerate() {
        let off = path_buf_offset + i * 2;
        buf[off..off + 2].copy_from_slice(&wc.to_le_bytes());
    }
    // PrintName is same content at print_offset
    let print_start = path_buf_offset + substitute_len as usize;
    for (i, &wc) in utf16.iter().enumerate() {
        let off = print_start + i * 2;
        buf[off..off + 2].copy_from_slice(&wc.to_le_bytes());
    }

    buf
}

/// Decode a symlink target from a Windows REPARSE_DATA_BUFFER (IO_REPARSE_TAG_SYMLINK).
/// Returns the target as UTF-8 bytes with back-slashes converted to forward slashes.
fn decode_symlink_reparse(buffer: &[u8]) -> Option<Vec<u8>> {
    if buffer.len() < 20 {
        return None;
    }
    let tag = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    if tag != IO_REPARSE_TAG_SYMLINK {
        return None;
    }
    let print_offset = u16::from_le_bytes([buffer[12], buffer[13]]) as usize;
    let print_len = u16::from_le_bytes([buffer[14], buffer[15]]) as usize;
    let path_buf_start = 20; // after the symlink header
    let start = path_buf_start + print_offset;
    let end = start + print_len;
    if end > buffer.len() {
        return None;
    }
    // Decode UTF-16LE
    let utf16: Vec<u16> = buffer[start..end]
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    let s = String::from_utf16_lossy(&utf16);
    // Convert \\ back to / for internal storage
    let unix_path = s.replace('\\', "/");
    Some(unix_path.into_bytes())
}

/// Convert a Windows FILETIME (100ns intervals since 1601-01-01) to a TimeSpec.
/// Returns `TimeSpec::Omit` if the value is 0 (unchanged).
fn filetime_to_timespec(ft: u64) -> TimeSpec {
    if ft == 0 {
        return TimeSpec::Omit;
    }
    const EPOCH_DIFF: u64 = 116_444_736_000_000_000; // 100ns intervals between 1601 and 1970
    let intervals_since_epoch = (ft).saturating_sub(EPOCH_DIFF);
    let nanos = intervals_since_epoch as i64 * 100; // 100ns → ns
    TimeSpec::Set(nanos)
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
            studiofs_meta::MetaError::IsADirectory(_) => {
                winfsp::FspError::from(STATUS_FILE_IS_A_DIRECTORY)
            }
            studiofs_meta::MetaError::NotFormatted => {
                winfsp::FspError::from(STATUS_UNRECOGNIZED_VOLUME)
            }
            _ => {
                error!("metadata error: {e}");
                winfsp::FspError::from(STATUS_UNSUCCESSFUL)
            }
        },
        studiofs_vfs::VfsError::InvalidName(_) => winfsp::FspError::from(STATUS_INVALID_PARAMETER),
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
        let path_str = file_name.to_string_lossy();
        debug!("get_security_by_name path={:?}", path_str);
        if let Some(security) = resolve_reparse_points(file_name) {
            debug!("  -> reparse point, returning");
            return Ok(security);
        }

        let attr = match resolve_path(&self.vfs, file_name) {
            Ok(a) => a,
            Err(e) => {
                debug!("  -> resolve_path FAILED: {:?}", e);
                return Err(e);
            }
        };
        let file_attributes = match attr.kind {
            FileType::Directory => 0x10u32,
            FileType::Symlink => FILE_ATTRIBUTE_REPARSE_POINT,
            _ => 0x80u32,
        };

        Ok(FileSecurity {
            reparse: attr.kind == FileType::Symlink,
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
        let path_str = file_name.to_string_lossy();
        debug!(
            "open path={:?} create_options={:#x} granted_access={:?}",
            path_str, _create_options, _granted_access
        );
        let attr = resolve_path(&self.vfs, file_name)?;
        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;

        let is_dir = attr.kind == FileType::Directory;
        let fh = if is_dir {
            0
        } else {
            self.vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .map_err(vfs_to_winfsp_err)?
        };

        debug!(
            ino = attr.ino,
            fh, "open SUCCESS path={:?} is_dir={}", path_str, is_dir
        );
        Ok(StudioFileHandle::new(attr.ino, is_dir, fh))
    }

    fn close(&self, context: Self::FileContext) {
        debug!(ino = context.ino, fh = context.fh, "close");
        if context.fh != 0
            && let Err(e) = self.rt.block_on(self.vfs.release_fh(context.fh))
        {
            error!(
                ino = context.ino,
                fh = context.fh,
                "release_fh failed: {:?}",
                e
            );
        }
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
        extra_buffer: Option<&[u8]>,
        extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let (parent_ino, name) = resolve_parent(&self.vfs, file_name)?;

        // Handle symlink creation via reparse point buffer
        if extra_buffer_is_reparse_point
            && let Some(buf) = extra_buffer
            && let Some(target) = decode_symlink_reparse(buf)
        {
            let attr = self
                .vfs
                .symlink(parent_ino, &name, &target, 0, 0)
                .map_err(vfs_to_winfsp_err)?;
            let info = attr_to_file_info(&attr);
            *file_info.as_mut() = info;
            debug!(
                ino = attr.ino,
                "create symlink {:?}",
                file_name.to_string_lossy()
            );
            return Ok(StudioFileHandle::new(attr.ino, false, 0));
        }

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

        let fh = if is_dir {
            0
        } else {
            self.vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .map_err(vfs_to_winfsp_err)?
        };

        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;

        debug!(
            ino = attr.ino,
            is_dir,
            fh,
            "create {:?}",
            file_name.to_string_lossy()
        );
        Ok(StudioFileHandle::new(attr.ino, is_dir, fh))
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        debug!(
            ino = context.ino,
            "get_file_info kind={:?} size={}", attr.kind, attr.size
        );
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn read(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        // P1.3: Use read_into() to avoid intermediate Bytes allocation and memcpy
        let len = self
            .rt
            .block_on(self.vfs.read_into(context.ino, offset, buffer))
            .map_err(vfs_to_winfsp_err)?;

        debug!(ino = context.ino, offset, len, "read");
        Ok(len)
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
        debug!(ino = context.ino, "read_directory called");
        // Cache entries for continued readdir
        let mut cached = context.dir_entries.lock();
        if cached.is_none() {
            let entries = self.vfs.readdir(context.ino).map_err(vfs_to_winfsp_err)?;
            debug!(
                ino = context.ino,
                count = entries.len(),
                "read_directory loaded {} entries",
                entries.len()
            );
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
                    debug!("buffer full after adding '.'");
                    DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
                    return Ok(cursor);
                }
                debug!("added '.' entry, cursor now at {}", cursor);
            }

            let mut dotdot = DirInfo::<255>::new();
            if dotdot.set_name(std::ffi::OsStr::new("..")).is_ok()
                && let Ok(attr) = self.vfs.getattr(context.ino)
            {
                *dotdot.file_info_mut() = attr_to_file_info(&attr);
                if !dotdot.append_to_buffer(buffer, &mut cursor) {
                    debug!("buffer full after adding '..'");
                    DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
                    return Ok(cursor);
                }
                debug!("added '..' entry, cursor now at {}", cursor);
            }
        }

        // Get the marker name for comparison
        let marker_str: Option<String> = marker
            .inner_as_cstr()
            .map(|c| c.to_string_lossy().to_string());

        for entry in entries {
            let entry_name = String::from_utf8_lossy(&entry.name).to_string();

            // Skip entries at or before the marker
            if let Some(ref m) = marker_str
                && entry_name.as_str() <= m.as_str()
            {
                continue;
            }

            let os_name = std::ffi::OsString::from(&entry_name);
            let mut dir_info = DirInfo::<255>::new();
            if dir_info.set_name(&os_name).is_err() {
                debug!("failed to set name for entry: {}", entry_name);
                continue;
            }

            let child_attr = match self.vfs.getattr(entry.ino) {
                Ok(a) => a,
                Err(e) => {
                    debug!("failed to get attr for {}: {:?}", entry_name, e);
                    continue;
                }
            };
            *dir_info.file_info_mut() = attr_to_file_info(&child_attr);

            if !dir_info.append_to_buffer(buffer, &mut cursor) {
                debug!(
                    "buffer full after adding entry '{}' (tried to add at cursor {})",
                    entry_name, cursor
                );
                break;
            }
            debug!("added entry '{}', cursor now at {}", entry_name, cursor);
        }

        DirInfo::<255>::finalize_buffer(buffer, &mut cursor);
        debug!(
            ino = context.ino,
            bytes_returned = cursor,
            "read_directory returning {} bytes",
            cursor
        );
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
            if let Some(fname) = file_name
                && let Ok((parent_ino, name)) = resolve_parent(&self.vfs, fname)
            {
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
        } else {
            // Not deleting — flush buffered writes
            // WinFSP "Cleanup" with no delete flag is the standard place to flush.
            if context.fh != 0 {
                if let Err(e) = self.rt.block_on(self.vfs.flush_fh(context.fh)) {
                    error!(ino = context.ino, "cleanup flush_fh failed: {:?}", e);
                }
            } else if let Err(e) = self.rt.block_on(self.vfs.flush(context.ino)) {
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
            .setattr(context.ino, SetattrArgs::size_only(0))
            .map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        last_access_time: u64,
        last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let args = SetattrArgs {
            atime: filetime_to_timespec(last_access_time),
            mtime: filetime_to_timespec(last_write_time),
            ..SetattrArgs::default()
        };
        let attr = self
            .vfs
            .setattr(context.ino, args)
            .map_err(vfs_to_winfsp_err)?;
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
            .setattr(context.ino, SetattrArgs::size_only(new_size))
            .map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn get_reparse_point_by_name(
        &self,
        file_name: &U16CStr,
        _is_directory: bool,
        buffer: &mut [u8],
    ) -> winfsp::Result<u64> {
        let attr = resolve_path(&self.vfs, file_name)?;
        if attr.kind != FileType::Symlink {
            return Err(winfsp::FspError::from(STATUS_NOT_A_REPARSE_POINT));
        }
        let target = self.vfs.readlink(attr.ino).map_err(vfs_to_winfsp_err)?;
        let encoded = encode_symlink_reparse(&target);
        let len = encoded.len().min(buffer.len());
        buffer[..len].copy_from_slice(&encoded[..len]);
        Ok(encoded.len() as u64)
    }

    fn get_reparse_point(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        buffer: &mut [u8],
    ) -> winfsp::Result<u64> {
        let target = self.vfs.readlink(context.ino).map_err(vfs_to_winfsp_err)?;
        let encoded = encode_symlink_reparse(&target);
        let len = encoded.len().min(buffer.len());
        buffer[..len].copy_from_slice(&encoded[..len]);
        Ok(encoded.len() as u64)
    }

    fn set_reparse_point(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        buffer: &[u8],
    ) -> winfsp::Result<()> {
        // We don't support changing an existing file into a reparse point.
        // Symlinks are created at create() time via extra_buffer_is_reparse_point.
        let _ = context;
        let _ = buffer;
        Err(winfsp::FspError::from(STATUS_UNSUCCESSFUL))
    }

    fn delete_reparse_point(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        _buffer: &[u8],
    ) -> winfsp::Result<()> {
        // Reparse points (symlinks) are removed via normal unlink in cleanup.
        let _ = context;
        Err(winfsp::FspError::from(STATUS_UNSUCCESSFUL))
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
        let st = self.vfs.statfs().map_err(vfs_to_winfsp_err)?;
        out_volume_info.total_size = st.blocks * st.bsize as u64;
        out_volume_info.free_size = st.bfree * st.bsize as u64;

        // Use volume name from VFS
        let name = self
            .vfs
            .volume_name()
            .unwrap_or_else(|_| "StudioFS".to_string());
        out_volume_info.set_volume_label(&name);
        Ok(())
    }
}
