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
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x10;
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x20;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x80;
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
        file_attributes: effective_win_attrs(attr),
        ..Default::default()
    };

    if attr.kind == FileType::Symlink {
        info.reparse_tag = IO_REPARSE_TAG_SYMLINK;
    }

    // Convert timestamps (nanoseconds since epoch → Windows FILETIME)
    // FILETIME is 100ns intervals since 1601-01-01
    info.creation_time = unix_nanos_to_filetime(attr.effective_crtime());
    info.last_access_time = unix_nanos_to_filetime(attr.atime);
    info.last_write_time = unix_nanos_to_filetime(attr.mtime);
    info.change_time = unix_nanos_to_filetime(attr.mtime);

    // Use inode as index
    info.index_number = attr.ino;
    // Report actual nlink count (must be ≥1 for files, ≥2 for dirs)
    info.hard_links = if attr.nlink > 0 { attr.nlink } else { 1 };

    info
}

/// Derive effective Windows file attributes from InodeAttr.
/// Uses stored win_attrs if set, otherwise derives from file type.
fn effective_win_attrs(attr: &InodeAttr) -> u32 {
    match attr.kind {
        FileType::Directory => {
            // Always include DIRECTORY bit
            if attr.win_attrs != 0 {
                attr.win_attrs | FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_DIRECTORY
            }
        }
        FileType::Symlink => {
            FILE_ATTRIBUTE_REPARSE_POINT
        }
        FileType::RegularFile => {
            if attr.win_attrs != 0 {
                // Stored attributes — strip type-derived bits
                let a = attr.win_attrs & !(FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT);
                if a == 0 { FILE_ATTRIBUTE_NORMAL } else { a }
            } else {
                // Default: ARCHIVE for regular files
                FILE_ATTRIBUTE_ARCHIVE
            }
        }
    }
}

/// Convert Unix timestamp (nanoseconds since epoch) to Windows FILETIME (100ns since 1601-01-01).
fn unix_nanos_to_filetime(unix_nanos: i64) -> u64 {
    const EPOCH_DIFF: u64 = 116_444_736_000_000_000; // Difference between 1601-01-01 and 1970-01-01 in 100ns intervals
    const NANOS_PER_100NS: i64 = 100;

    // Convert nanoseconds to 100ns intervals
    let intervals = unix_nanos / NANOS_PER_100NS;
    (intervals as u64).saturating_add(EPOCH_DIFF)
}

// ─── Path resolution (case-insensitive with fallback) ────────────────────────

/// Parse a WinFSP path into normalised components.
///
/// Handles `.` (skip), `..` (pop), trailing dots/spaces (strip).
fn parse_components(path: &U16CStr) -> Vec<String> {
    let s = path.to_string_lossy();
    let s = s.trim_start_matches('\\');
    let mut out = Vec::new();
    for raw in s.split('\\') {
        if raw.is_empty() || raw == "." {
            continue;
        }
        if raw == ".." {
            out.pop();
            continue;
        }
        let trimmed = raw.trim_end_matches(['.', ' ']);
        if !trimmed.is_empty() {
            out.push(trimmed.to_string());
        }
    }
    out
}

/// Walk path components starting from ROOT_INO, with case-insensitive fallback.
///
/// Returns `(ino, canonical_name)` for each successfully resolved component.
/// On exact-match miss, scans the parent directory for a case-insensitive match.
fn walk_components(vfs: &StudioVfs, components: &[&str]) -> winfsp::Result<Vec<(Ino, String)>> {
    let mut result = Vec::with_capacity(components.len());
    let mut current_ino = ROOT_INO;

    for &comp in components {
        match vfs.lookup(current_ino, comp.as_bytes()) {
            Ok(attr) => {
                result.push((attr.ino, comp.to_string()));
                current_ino = attr.ino;
            }
            Err(e) => {
                // Only try case-insensitive fallback for EntryNotFound.
                let is_not_found = matches!(
                    &e,
                    studiofs_vfs::VfsError::Meta(
                        studiofs_meta::MetaError::EntryNotFound { .. }
                    )
                );
                if !is_not_found {
                    return Err(vfs_to_winfsp_err(e));
                }
                // Case-insensitive scan of parent directory
                let entries = vfs.readdir(current_ino).map_err(vfs_to_winfsp_err)?;
                let lower = comp.to_lowercase();
                match entries.iter().find(|entry| {
                    String::from_utf8_lossy(&entry.name).to_lowercase() == lower
                }) {
                    Some(entry) => {
                        let canonical =
                            String::from_utf8_lossy(&entry.name).to_string();
                        current_ino = entry.ino;
                        result.push((entry.ino, canonical));
                    }
                    None => {
                        return Err(winfsp::FspError::from(
                            STATUS_OBJECT_NAME_NOT_FOUND,
                        ));
                    }
                }
            }
        }
    }
    Ok(result)
}

/// Build a UTF-16 normalised name from walked canonical components.
///
/// The result does NOT include a leading backslash — pass
/// `Some(b'\\' as u16)` as `prefix` to `OpenFileInfo::set_normalized_name`.
fn build_normalized(walked: &[(Ino, String)]) -> Vec<u16> {
    walked
        .iter()
        .map(|(_, n)| n.as_str())
        .collect::<Vec<_>>()
        .join("\\")
        .encode_utf16()
        .collect()
}

/// Resolve a WinFSP path to an inode (case-insensitive) and its
/// canonical normalised name (UTF-16, without leading backslash).
fn resolve_path(
    vfs: &StudioVfs,
    path: &U16CStr,
) -> winfsp::Result<(InodeAttr, Vec<u16>)> {
    let components = parse_components(path);
    if components.is_empty() {
        let attr = vfs.getattr(ROOT_INO).map_err(vfs_to_winfsp_err)?;
        return Ok((attr, vec![]));
    }
    let refs: Vec<&str> = components.iter().map(|s| s.as_str()).collect();
    let walked = walk_components(vfs, &refs)?;
    let ino = walked.last().unwrap().0;
    let attr = vfs.getattr(ino).map_err(vfs_to_winfsp_err)?;
    Ok((attr, build_normalized(&walked)))
}

/// Resolve the parent directory inode for a given path.
///
/// Returns `(parent_ino, child_name_bytes, full_normalized_utf16)`.
/// The normalised name includes the child component (as provided by the caller,
/// since the child may not exist yet for create operations).
fn resolve_parent(
    vfs: &StudioVfs,
    path: &U16CStr,
) -> winfsp::Result<(Ino, Vec<u8>, Vec<u16>)> {
    let components = parse_components(path);
    if components.is_empty() {
        return Err(winfsp::FspError::from(STATUS_OBJECT_NAME_NOT_FOUND));
    }
    let child_name = components.last().unwrap().clone();

    if components.len() == 1 {
        // Parent is root — normalised name is just the child.
        let normalized: Vec<u16> = child_name.encode_utf16().collect();
        return Ok((ROOT_INO, child_name.into_bytes(), normalized));
    }

    let parent_refs: Vec<&str> =
        components[..components.len() - 1].iter().map(|s| s.as_str()).collect();
    let walked = walk_components(vfs, &parent_refs)?;
    let parent_ino = walked.last().unwrap().0;

    // Full normalised path = canonical parent components + child name
    let mut parts: Vec<&str> = walked.iter().map(|(_, n)| n.as_str()).collect();
    parts.push(&child_name);
    let normalized: Vec<u16> = parts.join("\\").encode_utf16().collect();

    Ok((parent_ino, child_name.into_bytes(), normalized))
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

// ─── Security Descriptor ─────────────────────────────────────────────────────

// Win32 FFI for creating security descriptor from SDDL string.
unsafe extern "system" {
    fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
        StringSecurityDescriptor: *const u16,
        StringSDRevision: u32,
        SecurityDescriptor: *mut *mut c_void,
        SecurityDescriptorSize: *mut u32,
    ) -> i32;

    fn LocalFree(hMem: *mut c_void) -> *mut c_void;
}

/// Create a binary security descriptor granting full access to Everyone.
///
/// SDDL `O:BAG:BAD:(A;OICI;FA;;;WD)`:
///   Owner = Built-in Administrators,
///   Group = Built-in Administrators,
///   DACL  = Allow, Object-Inherit + Container-Inherit, Full Access, Everyone.
fn create_full_access_sd() -> Vec<u8> {
    // "O:BAG:BAD:(A;OICI;FA;;;WD)" null-terminated UTF-16
    let sddl = "O:BAG:BAD:(A;OICI;FA;;;WD)";
    let sddl_wide: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();

    let mut sd_ptr: *mut c_void = std::ptr::null_mut();
    let mut sd_size: u32 = 0;

    let ok = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            1, // SDDL_REVISION_1
            &mut sd_ptr,
            &mut sd_size,
        )
    };

    if ok == 0 || sd_ptr.is_null() {
        error!("failed to create security descriptor from SDDL, falling back to empty");
        return Vec::new();
    }

    let sd = unsafe { std::slice::from_raw_parts(sd_ptr as *const u8, sd_size as usize) }.to_vec();
    unsafe { LocalFree(sd_ptr) };
    sd
}

// ─── FileSystemContext ───────────────────────────────────────────────────────

/// The WinFSP filesystem context — holds VFS and tokio runtime.
pub struct StudioFsContext {
    vfs: Arc<StudioVfs>,
    rt: Arc<Runtime>,
    /// Cached binary security descriptor (full access for Everyone).
    security_descriptor: Vec<u8>,
}

impl StudioFsContext {
    pub fn new(vfs: Arc<StudioVfs>, rt: Arc<Runtime>) -> Self {
        let security_descriptor = create_full_access_sd();
        debug!(
            "created security descriptor ({} bytes)",
            security_descriptor.len()
        );
        Self {
            vfs,
            rt,
            security_descriptor,
        }
    }
}

impl FileSystemContext for StudioFsContext {
    type FileContext = StudioFileHandle;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        security_descriptor: Option<&mut [c_void]>,
        resolve_reparse_points: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        let path_str = file_name.to_string_lossy();
        debug!("get_security_by_name path={:?}", path_str);
        if let Some(security) = resolve_reparse_points(file_name) {
            debug!("  -> reparse point, returning");
            return Ok(security);
        }

        let (attr, _normalized) = match resolve_path(&self.vfs, file_name) {
            Ok(a) => a,
            Err(e) => {
                debug!("  -> resolve_path FAILED: {:?}", e);
                return Err(e);
            }
        };
        let file_attributes = effective_win_attrs(&attr);

        // Copy cached security descriptor into the provided buffer
        let sd_len = self.security_descriptor.len() as u64;
        if let Some(buf) = security_descriptor {
            let copy_len = (sd_len as usize).min(buf.len());
            if copy_len > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.security_descriptor.as_ptr(),
                        buf.as_mut_ptr() as *mut u8,
                        copy_len,
                    );
                }
            }
        }

        Ok(FileSecurity {
            reparse: attr.kind == FileType::Symlink,
            sz_security_descriptor: sd_len,
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
        let (attr, normalized) = resolve_path(&self.vfs, file_name)?;
        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;

        // Report canonical name for case-insensitive mode.
        file_info.set_normalized_name(&normalized, Some(b'\\' as u16));

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
        if context.fh != 0 {
            if let Err(e) = self.rt.block_on(self.vfs.release_fh(context.fh)) {
                error!(
                    ino = context.ino,
                    fh = context.fh,
                    "release_fh failed: {:?}",
                    e
                );
            }
        }
        drop(context);
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        file_attributes: FILE_FLAGS_AND_ATTRIBUTES,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        extra_buffer: Option<&[u8]>,
        extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let (parent_ino, name, normalized) = resolve_parent(&self.vfs, file_name)?;

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
            file_info.set_normalized_name(&normalized, Some(b'\\' as u16));
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

        // Apply Windows file attributes if provided (ARCHIVE, HIDDEN, etc.)
        let raw_attrs = file_attributes;
        if raw_attrs != 0 {
            let sanitised = raw_attrs
                & !(FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_NORMAL);
            let wa = if sanitised == 0 {
                if is_dir { FILE_ATTRIBUTE_DIRECTORY } else { FILE_ATTRIBUTE_ARCHIVE }
            } else if is_dir {
                sanitised | FILE_ATTRIBUTE_DIRECTORY
            } else {
                sanitised
            };
            let _ = self.vfs.setattr(
                attr.ino,
                SetattrArgs {
                    win_attrs: Some(wa),
                    ..SetattrArgs::default()
                },
            );
        }

        let fh = if is_dir {
            0
        } else {
            self.vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .map_err(vfs_to_winfsp_err)?
        };

        // Re-read attr after potential win_attrs update
        let attr = self.vfs.getattr(attr.ino).map_err(vfs_to_winfsp_err)?;
        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;
        file_info.set_normalized_name(&normalized, Some(b'\\' as u16));

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

    fn get_security(
        &self,
        _context: &Self::FileContext,
        security_descriptor: Option<&mut [c_void]>,
    ) -> winfsp::Result<u64> {
        let sd_len = self.security_descriptor.len() as u64;
        if let Some(buf) = security_descriptor {
            let copy_len = (sd_len as usize).min(buf.len());
            if copy_len > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.security_descriptor.as_ptr(),
                        buf.as_mut_ptr() as *mut u8,
                        copy_len,
                    );
                }
            }
        }
        Ok(sd_len)
    }

    fn set_security(
        &self,
        _context: &Self::FileContext,
        _security_information: u32,
        _modification_descriptor: ModificationDescriptor,
    ) -> winfsp::Result<()> {
        // Accept silently — we use a fixed full-access descriptor.
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
        write_to_eof: bool,
        _constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<u32> {
        // Resolve actual write offset.
        // write_to_eof means "append" — the provided offset is meaningless (-1).
        let actual_offset = if write_to_eof {
            self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?.size
        } else {
            offset
        };

        // NOTE: We intentionally do NOT enforce constrained_io clamping here.
        // The VFS layer handles file extent management correctly: writes within
        // or beyond EOF are fine. Attempting to clamp based on getattr() creates
        // a race condition with set_file_size() — the Cache Manager may issue
        // constrained writes on a different thread before set_file_size's redb
        // transaction commits, causing getattr() to see stale size=0 and
        // silently dropping all data.

        let written = self
            .rt
            .block_on(self.vfs.write(context.ino, actual_offset, buffer))
            .map_err(vfs_to_winfsp_err)?;

        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);

        debug!(ino = context.ino, offset = actual_offset, written, "write");
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
                && let Ok((parent_ino, name, _)) = resolve_parent(&self.vfs, fname)
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
        file_attributes: FILE_FLAGS_AND_ATTRIBUTES,
        replace_file_attributes: bool,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // Truncate to 0
        let mut args = SetattrArgs::size_only(0);

        // Apply file attributes on overwrite
        let raw = file_attributes;
        if raw != 0 && replace_file_attributes {
            let sanitised = raw
                & !(FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_NORMAL);
            args.win_attrs = Some(if sanitised == 0 {
                FILE_ATTRIBUTE_ARCHIVE
            } else {
                sanitised
            });
        }

        let attr = self
            .vfs
            .setattr(context.ino, args)
            .map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        file_attributes: u32,
        creation_time: u64,
        last_access_time: u64,
        last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // Sanitise file_attributes: strip type-derived bits that the FS manages.
        let win_attrs = if file_attributes != 0 && file_attributes != u32::MAX {
            // Strip DIRECTORY and REPARSE_POINT — those are derived from kind.
            let sanitised = file_attributes
                & !(FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_NORMAL);
            Some(if sanitised == 0 {
                FILE_ATTRIBUTE_NORMAL
            } else {
                sanitised
            })
        } else {
            None // 0 or INVALID_FILE_ATTRIBUTES = don't change
        };

        let args = SetattrArgs {
            atime: filetime_to_timespec(last_access_time),
            mtime: filetime_to_timespec(last_write_time),
            crtime: filetime_to_timespec(creation_time),
            win_attrs,
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
        set_allocation_size: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if set_allocation_size {
            // The Cache Manager is reserving capacity. We don't track allocation
            // size separately, but we MUST report allocation_size >= new_size
            // so the Cache Manager knows it can write up to that point.
            let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
            *file_info = attr_to_file_info(&attr);
            let requested_alloc = (new_size + 4095) & !4095;
            if file_info.allocation_size < requested_alloc {
                file_info.allocation_size = requested_alloc;
            }
        } else {
            // Actually extend/truncate the logical file size (EOF).
            let attr = self
                .vfs
                .setattr(context.ino, SetattrArgs::size_only(new_size))
                .map_err(vfs_to_winfsp_err)?;
            *file_info = attr_to_file_info(&attr);
        }
        Ok(())
    }

    fn get_reparse_point_by_name(
        &self,
        file_name: &U16CStr,
        _is_directory: bool,
        buffer: &mut [u8],
    ) -> winfsp::Result<u64> {
        let (attr, _) = resolve_path(&self.vfs, file_name)?;
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
        replace_if_exists: bool,
    ) -> winfsp::Result<()> {
        let (old_parent, old_name, _) = resolve_parent(&self.vfs, file_name)?;
        let (new_parent, new_name, _) = resolve_parent(&self.vfs, new_file_name)?;

        // If replace_if_exists is false, check whether the target already exists
        // and reject with STATUS_OBJECT_NAME_COLLISION if so.
        if !replace_if_exists {
            if self.vfs.lookup(new_parent, &new_name).is_ok() {
                return Err(winfsp::FspError::from(STATUS_OBJECT_NAME_COLLISION));
            }
        }

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
