#![cfg(windows)]
//! FileSystemContext implementation — bridges WinFSP calls to StudioVfs.

use std::ffi::c_void;
use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_meta::types::{SetattrArgs, TimeSpec};
use studiofs_meta::{FileType, Ino, InodeAttr};
use studiofs_vfs::{KernelInvalidator, NamespacePathStyle, OpenFlags, StudioVfs};
use tokio::runtime::Runtime;
use tracing::{debug, error, trace, warn};
use windows::Win32::Foundation::NTSTATUS;
use windows::Win32::Security::{
    Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW,
    GetSecurityDescriptorLength, PSECURITY_DESCRIPTOR,
};
use winfsp::U16CStr;
use winfsp::filesystem::DirBuffer;
use winfsp::filesystem::*;
use winfsp::notify::{Notifier, NotifyInfo, NotifyingFileSystemContext};
use winfsp_sys::{FILE_ACCESS_RIGHTS, FILE_FLAGS_AND_ATTRIBUTES};

// Windows reparse point constants
const IO_REPARSE_TAG_SYMLINK: u32 = 0xA000_000C;
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
const FILE_DIRECTORY_FILE_OPTION: u32 = 0x0000_0001;
const FILE_WRITE_THROUGH_OPTION: u32 = 0x0000_0002;
const FILE_NO_INTERMEDIATE_BUFFERING_OPTION: u32 = 0x0000_0008;

// ─── NTSTATUS constants ──────────────────────────────────────────────────────

const STATUS_OBJECT_NAME_NOT_FOUND: NTSTATUS = NTSTATUS(0xC000_0034_u32 as i32);
const STATUS_OBJECT_NAME_COLLISION: NTSTATUS = NTSTATUS(0xC000_0035_u32 as i32);
const STATUS_DIRECTORY_NOT_EMPTY: NTSTATUS = NTSTATUS(0xC000_0101_u32 as i32);
const STATUS_NOT_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_0103_u32 as i32);
const STATUS_FILE_IS_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_00BA_u32 as i32);
const STATUS_UNRECOGNIZED_VOLUME: NTSTATUS = NTSTATUS(0xC000_014F_u32 as i32);
const STATUS_UNSUCCESSFUL: NTSTATUS = NTSTATUS(0xC000_0001_u32 as i32);
const STATUS_INVALID_PARAMETER: NTSTATUS = NTSTATUS(0xC000_000D_u32 as i32);
const STATUS_ACCESS_DENIED: NTSTATUS = NTSTATUS(0xC000_0022_u32 as i32);
const STATUS_NOT_A_REPARSE_POINT: NTSTATUS = NTSTATUS(0xC000_0275_u32 as i32);
const STATUS_EA_TOO_LARGE: NTSTATUS = NTSTATUS(0xC000_0050_u32 as i32);
const MAX_WINDOWS_COMPONENT_UTF16_UNITS: usize = 255;

// ─── File Handle ─────────────────────────────────────────────────────────────

/// Per-open-file context.
pub struct StudioFileHandle {
    ino: Ino,
    is_dir: bool,
    /// VFS file handle (0 for directories).
    fh: u64,
    /// Open flags derived from WinFSP `CreateOptions`.
    open_flags: OpenFlags,
    /// WinFSP directory buffer for efficient directory enumeration.
    dir_buffer: DirBuffer,
}

impl StudioFileHandle {
    fn new(ino: Ino, is_dir: bool, fh: u64, open_flags: OpenFlags) -> Self {
        Self {
            ino,
            is_dir,
            fh,
            open_flags,
            dir_buffer: DirBuffer::new(),
        }
    }
}

#[derive(Copy, Clone)]
struct SendMutBuffer {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: The wrapper only marks transport capability across threads.
// Call sites must guarantee pointer validity, exclusivity, and lifetime.
unsafe impl Send for SendMutBuffer {}

impl SendMutBuffer {
    fn from_slice(slice: &mut [u8]) -> Self {
        Self {
            ptr: slice.as_mut_ptr(),
            len: slice.len(),
        }
    }

    unsafe fn as_mut_slice<'a>(self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

#[derive(Copy, Clone)]
struct SendConstBuffer {
    ptr: *const u8,
    len: usize,
}

// SAFETY: The wrapper only marks transport capability across threads.
// Call sites must guarantee pointer validity and lifetime.
unsafe impl Send for SendConstBuffer {}

impl SendConstBuffer {
    fn from_slice(slice: &[u8]) -> Self {
        Self {
            ptr: slice.as_ptr(),
            len: slice.len(),
        }
    }

    unsafe fn as_slice<'a>(self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

fn open_flags_from_create_options(create_options: u32) -> OpenFlags {
    let mut flags = OpenFlags::RDWR;
    if create_options & FILE_NO_INTERMEDIATE_BUFFERING_OPTION != 0 {
        flags = flags | OpenFlags::DIRECT_IO;
    }
    if create_options & FILE_WRITE_THROUGH_OPTION != 0 {
        flags = flags | OpenFlags::WRITE_THROUGH;
    }
    flags
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Compute Win32 FILE_ATTRIBUTE_* flags from inode attributes.
///
/// If `win_attrs` is stored (non-zero), use it but ensure kind-specific bits
/// (DIRECTORY, REPARSE_POINT) are always present. Otherwise derive defaults.
fn compute_file_attributes(attr: &InodeAttr) -> u32 {
    if attr.win_attrs != 0 {
        match attr.kind {
            FileType::Directory => attr.win_attrs | 0x10,
            FileType::Symlink => attr.win_attrs | FILE_ATTRIBUTE_REPARSE_POINT,
            _ => {
                let a = attr.win_attrs;
                // Clear NORMAL (0x80) if other meaningful flags are present.
                if a & !0x80 != 0 { a & !0x80 } else { a }
            }
        }
    } else {
        match attr.kind {
            FileType::Directory => 0x10, // FILE_ATTRIBUTE_DIRECTORY
            FileType::Symlink => FILE_ATTRIBUTE_REPARSE_POINT,
            _ => 0x20, // FILE_ATTRIBUTE_ARCHIVE (default, like NTFS)
        }
    }
}

/// Convert InodeAttr to WinFSP FileInfo.
fn attr_to_file_info(attr: &InodeAttr) -> FileInfo {
    let file_attributes = compute_file_attributes(attr);

    let mut info = FileInfo {
        file_size: attr.size,
        allocation_size: if attr.kind == FileType::Directory {
            4096u64.max((attr.size + 4095) & !4095)
        } else {
            (attr.size + 4095) & !4095
        },
        file_attributes,
        ..Default::default()
    };

    if attr.kind == FileType::Symlink {
        info.reparse_tag = IO_REPARSE_TAG_SYMLINK;
    }

    // Convert timestamps (nanoseconds since epoch → Windows FILETIME)
    // FILETIME is 100ns intervals since 1601-01-01
    // Use effective_crtime() for creation_time: actual birth time, falling back to ctime
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
    vfs.namespace_resolver(NamespacePathStyle::Windows)
        .resolve_path(&s)
        .map_err(vfs_to_winfsp_err)
}

/// Resolve the parent directory inode for a given path.
///
/// Handles `.`, `..`, and trailing-dot/space stripping like `resolve_path`.
fn resolve_parent(vfs: &StudioVfs, path: &U16CStr) -> winfsp::Result<(Ino, Vec<u8>)> {
    let s = path.to_string_lossy();
    let parent = vfs
        .namespace_resolver(NamespacePathStyle::Windows)
        .resolve_parent(&s)
        .map_err(vfs_to_winfsp_err)?;
    Ok((parent.parent, parent.name))
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
    let target_str = String::from_utf8_lossy(target);
    let win_path = target_str.replace('/', "\\");
    let utf16: Vec<u16> = win_path.encode_utf16().collect();
    let name_bytes = utf16.len() * 2;

    let substitute_len = name_bytes as u16;
    let print_len = substitute_len;
    // 4 offsets (2B each) + flags (4B) + 2× path
    let data_len: u16 = 4 * 2 + 4 + substitute_len + print_len;
    let total = 8 + data_len as usize;
    let mut buf = vec![0u8; total];

    buf[0..4].copy_from_slice(&IO_REPARSE_TAG_SYMLINK.to_le_bytes());
    buf[4..6].copy_from_slice(&data_len.to_le_bytes());
    // SubstituteNameOffset = 0 (already zero)
    buf[10..12].copy_from_slice(&substitute_len.to_le_bytes());
    buf[12..14].copy_from_slice(&substitute_len.to_le_bytes()); // PrintNameOffset
    buf[14..16].copy_from_slice(&print_len.to_le_bytes());
    buf[16..20].copy_from_slice(&1u32.to_le_bytes()); // SYMLINK_FLAG_RELATIVE

    // Write both SubstituteName and PrintName via byte slice copy.
    let src = unsafe { std::slice::from_raw_parts(utf16.as_ptr() as *const u8, name_bytes) };
    let path_buf_offset = 20;
    buf[path_buf_offset..path_buf_offset + name_bytes].copy_from_slice(src);
    let print_start = path_buf_offset + name_bytes;
    buf[print_start..print_start + name_bytes].copy_from_slice(src);

    buf
}

/// Encode a symlink reparse buffer and copy it into the output buffer.
/// Returns the total reparse data length.
fn write_reparse_to_buffer(target: &[u8], buffer: &mut [u8]) -> u64 {
    let encoded = encode_symlink_reparse(target);
    let len = encoded.len().min(buffer.len());
    buffer[..len].copy_from_slice(&encoded[..len]);
    encoded.len() as u64
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

/// Create a binary security descriptor granting full access to Everyone.
///
/// SDDL: Owner=BUILTIN\Administrators, Group=same, DACL=Protected:
///   - Full Access for SYSTEM
///   - Full Access for BUILTIN\Administrators
///   - Full Access for Everyone
fn create_default_security_descriptor() -> Vec<u8> {
    let sddl = windows::core::w!("O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)");
    let mut psd = PSECURITY_DESCRIPTOR::default();
    unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl, 1, // SDDL_REVISION_1
            &mut psd, None,
        )
        .expect("Failed to create security descriptor from SDDL");
        let len = GetSecurityDescriptorLength(psd) as usize;
        let bytes = std::slice::from_raw_parts(psd.0 as *const u8, len).to_vec();
        // Free the LocalAlloc'd descriptor
        windows::Win32::Foundation::LocalFree(Some(windows::Win32::Foundation::HLOCAL(
            psd.0 as *mut _,
        )));
        bytes
    }
}

/// Map VfsError to WinFSP NTSTATUS error.
fn vfs_to_winfsp_err(e: studiofs_vfs::VfsError) -> winfsp::FspError {
    match &e {
        studiofs_vfs::VfsError::Join(join_err) => {
            error!(
                error = %join_err,
                "critical runtime failure (JoinError), requesting emergency unmount"
            );
            crate::request_shutdown();
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
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
            studiofs_meta::MetaError::XattrValueTooLarge { .. } => {
                winfsp::FspError::from(STATUS_EA_TOO_LARGE)
            }
            _ => {
                error!("metadata error: {e}");
                winfsp::FspError::from(STATUS_UNSUCCESSFUL)
            }
        },
        studiofs_vfs::VfsError::InvalidName(_) => winfsp::FspError::from(STATUS_INVALID_PARAMETER),
        studiofs_vfs::VfsError::Io(ioe) => match ioe.kind() {
            std::io::ErrorKind::NotFound => winfsp::FspError::from(STATUS_OBJECT_NAME_NOT_FOUND),
            std::io::ErrorKind::PermissionDenied => winfsp::FspError::from(STATUS_ACCESS_DENIED),
            std::io::ErrorKind::AlreadyExists => {
                winfsp::FspError::from(STATUS_OBJECT_NAME_COLLISION)
            }
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => {
                winfsp::FspError::from(STATUS_INVALID_PARAMETER)
            }
            _ => winfsp::FspError::from(STATUS_UNSUCCESSFUL),
        },
        studiofs_vfs::VfsError::WouldBlock => {
            debug!("vfs lock conflict: {e}");
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
        studiofs_vfs::VfsError::RemoteContentPending => {
            debug!("vfs remote content pending: {e}");
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
        _ => {
            error!("vfs error: {e}");
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
    }
}

fn is_benign_cleanup_delete_error(e: &studiofs_vfs::VfsError, deleting_dir: bool) -> bool {
    match e {
        studiofs_vfs::VfsError::Meta(me) => match me {
            studiofs_meta::MetaError::InodeNotFound(_)
            | studiofs_meta::MetaError::EntryNotFound { .. } => true,
            studiofs_meta::MetaError::NotADirectory(_) => deleting_dir,
            studiofs_meta::MetaError::IsADirectory(_) => !deleting_dir,
            _ => false,
        },
        _ => false,
    }
}

fn encode_entry_name_utf16_lossy(entry_name: &[u8], out: &mut Vec<u16>) -> bool {
    const REPLACEMENT_CHAR: u16 = 0xFFFD;
    out.clear();
    let mut had_invalid_utf8 = false;
    let mut rest = entry_name;

    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                out.extend(valid.encode_utf16());
                break;
            }
            Err(err) => {
                had_invalid_utf8 = true;
                let valid_up_to = err.valid_up_to();
                if valid_up_to > 0 {
                    // SAFETY: `valid_up_to` is guaranteed by `Utf8Error` to mark
                    // the end of the valid UTF-8 prefix. The slice `&rest[..valid_up_to]`
                    // contains only valid UTF-8 bytes by contract of `from_utf8`'s error.
                    let valid = unsafe { std::str::from_utf8_unchecked(&rest[..valid_up_to]) };
                    out.extend(valid.encode_utf16());
                }

                // Invalid sequence maps to U+FFFD in lossy conversion semantics.
                out.push(REPLACEMENT_CHAR);

                let skip = err.error_len().unwrap_or(1);
                if valid_up_to + skip > rest.len() {
                    break;
                }
                rest = &rest[valid_up_to + skip..];
            }
        }
    }

    had_invalid_utf8
}

// ─── FileSystemContext ───────────────────────────────────────────────────────

/// The WinFSP filesystem context — holds VFS and tokio runtime.
pub struct StudioFsContext {
    vfs: Arc<StudioVfs>,
    rt: Arc<Runtime>,
    /// Cached binary security descriptor (Everyone = Full Access).
    default_sd: Vec<u8>,
    /// Pending kernel cache invalidation queue (shared with
    /// `WinfspKernelInvalidator` so metadata refresh events can enqueue inodes
    /// and the WinFSP timer can drain them).
    invalidation_queue: Arc<Mutex<Vec<Ino>>>,
}

impl StudioFsContext {
    pub fn new(
        vfs: Arc<StudioVfs>,
        rt: Arc<Runtime>,
        invalidation_queue: Arc<Mutex<Vec<Ino>>>,
    ) -> Self {
        let default_sd = create_default_security_descriptor();
        Self {
            vfs,
            rt,
            default_sd,
            invalidation_queue,
        }
    }

    /// Copy the cached security descriptor into the provided buffer.
    /// Returns the full SD length (even if the buffer was smaller).
    fn copy_sd_to_buffer(&self, buf: Option<&mut [c_void]>) -> u64 {
        let sd = &self.default_sd;
        if let Some(buf) = buf {
            let copy_len = sd.len().min(buf.len());
            unsafe {
                std::ptr::copy_nonoverlapping(sd.as_ptr(), buf.as_mut_ptr() as *mut u8, copy_len);
            }
        }
        sd.len() as u64
    }

    /// Spawn an async future on the tokio runtime and block until it completes.
    ///
    /// Unlike `Runtime::block_on()`, this runs the future on a tokio worker
    /// thread (where the IO reactor is guaranteed to be running), then delivers
    /// the result via a sync channel.  The WinFSP dispatch thread only blocks
    /// on the channel --- it does NOT participate in polling the future.
    ///
    /// This is important because WinFSP uses IOCP with a limited concurrency.
    /// `block_on` makes the current thread drive the future, which can interact
    /// poorly with the tokio IO reactor when called from non-runtime threads.
    /// Expected operational failures must be propagated as `Result`, not panics.
    fn spawn_wait<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel::<T>(1);
        self.rt.spawn(async move {
            let result = f.await;
            let _ = tx.send(result);
        });
        // Park the WinFSP thread until the runtime worker finishes.
        // unwrap: the channel can only fail if the spawned task panics,
        // which results in an immediate WinFSP error.
        rx.recv().expect("tokio task panicked")
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
        if let Some(security) = resolve_reparse_points(file_name) {
            return Ok(security);
        }

        let attr = resolve_path(&self.vfs, file_name)?;
        let file_attributes = compute_file_attributes(&attr);
        let sd_len = self.copy_sd_to_buffer(security_descriptor);

        Ok(FileSecurity {
            reparse: attr.kind == FileType::Symlink,
            sz_security_descriptor: sd_len,
            attributes: file_attributes,
        })
    }

    fn get_security(
        &self,
        _context: &Self::FileContext,
        security_descriptor: Option<&mut [c_void]>,
    ) -> winfsp::Result<u64> {
        Ok(self.copy_sd_to_buffer(security_descriptor))
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: FILE_ACCESS_RIGHTS,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let attr = resolve_path(&self.vfs, file_name)?;
        *file_info.as_mut() = attr_to_file_info(&attr);
        let open_flags = open_flags_from_create_options(create_options);

        let is_dir = attr.kind == FileType::Directory;
        let fh = if is_dir {
            0
        } else {
            self.vfs
                .open_file(attr.ino, open_flags, false)
                .map_err(vfs_to_winfsp_err)?
        };

        debug!(
            ino = attr.ino,
            fh,
            is_dir,
            open_flags = open_flags.bits(),
            unbuffered = open_flags.requests_unbuffered_io(),
            "open"
        );
        Ok(StudioFileHandle::new(attr.ino, is_dir, fh, open_flags))
    }

    fn close(&self, context: Self::FileContext) {
        debug!(ino = context.ino, fh = context.fh, "close");
        if context.fh != 0 {
            let vfs = self.vfs.clone();
            let fh = context.fh;
            let ino = context.ino;
            if let Err(e) = self.spawn_wait(async move { vfs.release_fh(fh).await }) {
                error!(ino, fh, "release_fh failed: {:?}", e);
            }
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
            return Ok(StudioFileHandle::new(
                attr.ino,
                false,
                0,
                open_flags_from_create_options(create_options),
            ));
        }

        let is_dir = (create_options & FILE_DIRECTORY_FILE_OPTION) != 0;
        let open_flags = open_flags_from_create_options(create_options);
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
                .open_file(attr.ino, open_flags, false)
                .map_err(vfs_to_winfsp_err)?
        };

        let info = attr_to_file_info(&attr);
        *file_info.as_mut() = info;

        debug!(
            ino = attr.ino,
            is_dir,
            fh,
            open_flags = open_flags.bits(),
            unbuffered = open_flags.requests_unbuffered_io(),
            "create {:?}",
            file_name.to_string_lossy()
        );
        Ok(StudioFileHandle::new(attr.ino, is_dir, fh, open_flags))
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        trace!(ino = context.ino, size = attr.size, "get_file_info");
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn read(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        if let Some(n) = self
            .vfs
            .read_into_fh_sync_cached(context.fh, context.ino, offset, buffer)
            .map_err(vfs_to_winfsp_err)?
        {
            trace!(
                ino = context.ino,
                fh = context.fh,
                offset,
                n,
                open_flags = context.open_flags.bits(),
                unbuffered = context.open_flags.requests_unbuffered_io(),
                "read (sync cache fast-path)"
            );
            return Ok(n);
        }

        // Spawn the async read on a tokio worker thread so the WinFSP
        // dispatch thread doesn't participate in IO polling.
        let ino = context.ino;
        let fh = context.fh;
        let out_buf = SendMutBuffer::from_slice(buffer);
        let vfs = self.vfs.clone();

        let result: Result<u32, _> = self.spawn_wait(async move {
            // SAFETY:
            // - `out_buf` originates from the WinFSP-provided `buffer` and remains
            //   valid for the duration of this callback.
            // - `spawn_wait` blocks until this task completes, so the caller cannot
            //   observe or alias `buffer` concurrently while this mutable slice exists.
            // - `u8` alignment is always valid (alignment = 1).
            let out = unsafe { out_buf.as_mut_slice() };
            vfs.read_into_fh(fh, ino, offset, out).await
        });

        match result {
            Ok(n) => {
                trace!(
                    ino,
                    fh,
                    offset,
                    n,
                    open_flags = context.open_flags.bits(),
                    unbuffered = context.open_flags.requests_unbuffered_io(),
                    "read"
                );
                Ok(n)
            }
            Err(e) => Err(vfs_to_winfsp_err(e)),
        }
    }

    fn write(
        &self,
        context: &Self::FileContext,
        mut buffer: &[u8],
        offset: u64,
        write_to_eof: bool,
        constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<u32> {
        // Resolve actual write offset
        let actual_offset = if write_to_eof {
            let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
            attr.size
        } else {
            offset
        };

        // Constrained I/O: paging writes must not extend the file beyond its current size
        if constrained_io {
            let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
            let fsize = attr.size;
            if actual_offset >= fsize {
                return Ok(0);
            }
            let max_len = (fsize - actual_offset) as usize;
            if buffer.len() > max_len {
                buffer = &buffer[..max_len];
            }
        }

        let ino = context.ino;
        let fh = context.fh;
        let in_buf = SendConstBuffer::from_slice(buffer);
        let vfs = self.vfs.clone();
        let written = self
            .spawn_wait(async move {
                // SAFETY:
                // - `in_buf` points at WinFSP-provided request bytes that stay valid
                //   for the duration of this callback.
                // - `spawn_wait` blocks until completion, so the source memory
                //   is not accessed after this callback returns.
                // - `u8` alignment is always valid (alignment = 1).
                let data = unsafe { in_buf.as_slice() };
                vfs.write_fh(fh, ino, actual_offset, data).await
            })
            .map_err(vfs_to_winfsp_err)?;

        let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
        *file_info = attr_to_file_info(&attr);

        trace!(
            ino = context.ino,
            fh = context.fh,
            offset = actual_offset,
            written,
            open_flags = context.open_flags.bits(),
            unbuffered = context.open_flags.requests_unbuffered_io(),
            "write"
        );
        Ok(written)
    }

    fn read_directory(
        &self,
        context: &Self::FileContext,
        _pattern: Option<&U16CStr>,
        marker: DirMarker,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        // Constant UTF-16 names for "." and ".." — avoids heap allocation.
        const DOT: &[u16] = &[b'.' as u16];
        const DOTDOT: &[u16] = &[b'.' as u16, b'.' as u16];

        // On first call (no marker), populate the DirBuffer with all entries.
        if marker.is_none() {
            let lock = context.dir_buffer.acquire(true, None)?;

            // Add "." entry
            {
                let mut dot = DirInfo::<MAX_WINDOWS_COMPONENT_UTF16_UNITS>::new();
                dot.set_name_raw(DOT)?;
                let attr = self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?;
                *dot.file_info_mut() = attr_to_file_info(&attr);
                let _ = lock.write(&mut dot);
            }

            // Add ".." entry
            {
                let mut dotdot = DirInfo::<MAX_WINDOWS_COMPONENT_UTF16_UNITS>::new();
                dotdot.set_name_raw(DOTDOT)?;
                if let Ok(attr) = self.vfs.getattr(context.ino) {
                    *dotdot.file_info_mut() = attr_to_file_info(&attr);
                    let _ = lock.write(&mut dotdot);
                }
            }

            // Add all directory entries
            let entries = self.vfs.readdir(context.ino).map_err(vfs_to_winfsp_err)?;
            debug!(ino = context.ino, count = entries.len(), "read_directory");
            let mut name_u16 = Vec::<u16>::new();

            for entry in &entries {
                let had_invalid_utf8 = encode_entry_name_utf16_lossy(&entry.name, &mut name_u16);
                if name_u16.len() > MAX_WINDOWS_COMPONENT_UTF16_UNITS {
                    warn!(
                        ino = context.ino,
                        entry_ino = entry.ino,
                        utf16_len = name_u16.len(),
                        max_utf16_len = MAX_WINDOWS_COMPONENT_UTF16_UNITS,
                        "read_directory skipping overlong entry name"
                    );
                    continue;
                }

                let mut dir_info = DirInfo::<MAX_WINDOWS_COMPONENT_UTF16_UNITS>::new();
                if dir_info.set_name_raw(name_u16.as_slice()).is_err() {
                    let display_name = String::from_utf8_lossy(&entry.name);
                    warn!(
                        "failed to set name for entry '{}': bytes={} utf16_len={}",
                        display_name,
                        entry.name.len(),
                        name_u16.len()
                    );
                    continue;
                }

                let child_attr = match self.vfs.getattr(entry.ino) {
                    Ok(a) => a,
                    Err(e) => {
                        let display_name = String::from_utf8_lossy(&entry.name);
                        warn!(
                            "failed to get attr for '{}': {:?} (had_invalid_utf8={})",
                            display_name, e, had_invalid_utf8
                        );
                        continue;
                    }
                };
                *dir_info.file_info_mut() = attr_to_file_info(&child_attr);

                if let Err(e) = lock.write(&mut dir_info) {
                    let display_name = String::from_utf8_lossy(&entry.name);
                    warn!("DirBuffer write failed for '{}': {:?}", display_name, e);
                    break;
                }
            }
        }

        Ok(context.dir_buffer.read(marker, buffer))
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
        // FspCleanupDelete = 0x01
        if flags & 0x01 != 0 {
            if let Some(fname) = file_name {
                match resolve_parent(&self.vfs, fname) {
                    Ok((parent_ino, name)) => {
                        let res = if context.is_dir {
                            self.vfs.rmdir(parent_ino, &name)
                        } else {
                            self.vfs.unlink(parent_ino, &name)
                        };

                        match res {
                            Ok(()) => {
                                debug!(ino = context.ino, "cleanup delete");
                            }
                            Err(e) if is_benign_cleanup_delete_error(&e, context.is_dir) => {
                                debug!(
                                    ino = context.ino,
                                    is_dir = context.is_dir,
                                    error = ?e,
                                    "cleanup delete skipped stale race"
                                );
                            }
                            Err(e) => {
                                error!("cleanup delete failed: {:?}", e);
                            }
                        }
                    }
                    Err(_) => {
                        debug!(
                            ino = context.ino,
                            is_dir = context.is_dir,
                            "cleanup delete skipped: path already gone"
                        );
                    }
                }
            }
        } else {
            // Not deleting — flush buffered writes synchronously before WinFSP
            // proceeds to close() → release_fh().
            //
            // Must use spawn_wait (blocking) rather than rt.spawn (fire-and-forget)
            // because WinFSP calls cleanup() immediately followed by close(), which
            // calls release_fh() and drops the handle from the table. A background
            // flush_fh task that races past release_fh will find the handle already
            // gone, silently skip the flush body, and — critically for
            // DIRECT_IO/WRITE_THROUGH handles — never call wait_for_ingress_drain,
            // breaking the direct-IO durability guarantee.
            let vfs = self.vfs.clone();
            let ino = context.ino;
            let fh = context.fh;
            if fh != 0 {
                if let Err(e) = self.spawn_wait(async move { vfs.flush_fh(fh).await }) {
                    error!(ino, fh, "cleanup flush_fh failed: {:?}", e);
                }
            } else {
                if let Err(e) = self.spawn_wait(async move { vfs.flush(ino).await }) {
                    error!(ino, "cleanup flush failed: {:?}", e);
                }
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
        file_attributes: u32,
        creation_time: u64,
        last_access_time: u64,
        last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // WinFSP convention: INVALID_FILE_ATTRIBUTES (0xFFFFFFFF) means "don't change".
        // 0 also means "don't change" (per MSDN FILE_BASIC_INFO).
        let win_attrs = if file_attributes != 0 && file_attributes != u32::MAX {
            Some(file_attributes)
        } else {
            None
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
        // WinFSP can call this with set_allocation_size=true to reserve capacity
        // without changing logical EOF. We don't track allocation size separately,
        // so treat allocation-size updates as a no-op for file length.
        let attr = if set_allocation_size {
            self.vfs.getattr(context.ino).map_err(vfs_to_winfsp_err)?
        } else {
            self.vfs
                .setattr(context.ino, SetattrArgs::size_only(new_size))
                .map_err(vfs_to_winfsp_err)?
        };
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
        Ok(write_reparse_to_buffer(&target, buffer))
    }

    fn get_reparse_point(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        buffer: &mut [u8],
    ) -> winfsp::Result<u64> {
        let target = self.vfs.readlink(context.ino).map_err(vfs_to_winfsp_err)?;
        Ok(write_reparse_to_buffer(&target, buffer))
    }

    fn set_reparse_point(
        &self,
        _context: &Self::FileContext,
        _file_name: &U16CStr,
        _buffer: &[u8],
    ) -> winfsp::Result<()> {
        // Symlinks are created at create() time via extra_buffer_is_reparse_point.
        Err(winfsp::FspError::from(STATUS_UNSUCCESSFUL))
    }

    fn delete_reparse_point(
        &self,
        _context: &Self::FileContext,
        _file_name: &U16CStr,
        _buffer: &[u8],
    ) -> winfsp::Result<()> {
        // Reparse points (symlinks) are removed via normal unlink in cleanup.
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
            let vfs = self.vfs.clone();
            let ino = ctx.ino;
            self.spawn_wait(async move { vfs.flush(ino).await })
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

// ─── WinFSP Notify (Kernel Cache Invalidation) ──────────────────────────────

/// Windows FILE_NOTIFY_CHANGE_* / FILE_ACTION_* constants.
const FILE_NOTIFY_CHANGE_FILE_NAME: u32 = 0x0000_0001;
const FILE_NOTIFY_CHANGE_DIR_NAME: u32 = 0x0000_0002;
const FILE_NOTIFY_CHANGE_ATTRIBUTES: u32 = 0x0000_0004;
const FILE_NOTIFY_CHANGE_SIZE: u32 = 0x0000_0008;
const FILE_NOTIFY_CHANGE_LAST_WRITE: u32 = 0x0000_0010;
const FILE_ACTION_MODIFIED: u32 = 0x0000_0003;

fn notify_filter_for_kind(kind: FileType) -> u32 {
    if kind == FileType::Directory {
        FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_ATTRIBUTES
    } else {
        FILE_NOTIFY_CHANGE_FILE_NAME
            | FILE_NOTIFY_CHANGE_SIZE
            | FILE_NOTIFY_CHANGE_ATTRIBUTES
            | FILE_NOTIFY_CHANGE_LAST_WRITE
    }
}

fn build_notify_info(path: &str, kind: FileType) -> Option<NotifyInfo<512>> {
    let mut info = NotifyInfo::<512>::new();
    info.filter = notify_filter_for_kind(kind);
    info.action = FILE_ACTION_MODIFIED;
    info.set_name(path).ok()?;
    Some(info)
}

/// Resolve an inode number to a Windows-style path (`\dir\file`) by walking
/// the parent chain.  Returns `None` if the inode or any ancestor no longer
/// exists (e.g. already deleted).
fn ino_to_windows_path(vfs: &StudioVfs, ino: Ino) -> Option<String> {
    vfs.namespace_resolver(NamespacePathStyle::Windows)
        .path_for_inode(ino)
}

/// Bridges VFS kernel cache invalidation to WinFSP's
/// `FspFileSystemNotify` mechanism.
///
/// Metadata refreshes push inode numbers into a shared queue.
/// The WinFSP timer drains the queue and sends Windows change notifications
/// so that Explorer and other applications see remote changes promptly.
pub struct WinfspKernelInvalidator {
    queue: Arc<Mutex<Vec<Ino>>>,
}

impl WinfspKernelInvalidator {
    pub fn new(queue: Arc<Mutex<Vec<Ino>>>) -> Self {
        Self { queue }
    }
}

impl KernelInvalidator for WinfspKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.queue.lock().push(ino);
    }
}

impl NotifyingFileSystemContext<Vec<Ino>> for StudioFsContext {
    fn should_notify(&self) -> Option<Vec<Ino>> {
        let mut queue = self.invalidation_queue.lock();
        if queue.is_empty() {
            return None;
        }
        // Drain and deduplicate.
        let mut inodes: Vec<Ino> = std::mem::take(&mut *queue);
        inodes.sort_unstable();
        inodes.dedup();
        Some(inodes)
    }

    fn notify(&self, inodes: Vec<Ino>, notifier: &Notifier) {
        for ino in &inodes {
            let Some(path) = ino_to_windows_path(&self.vfs, *ino) else {
                trace!(ino, "skipping notify — inode no longer resolvable");
                continue;
            };

            let kind = self
                .vfs
                .meta()
                .get_attr(*ino)
                .map(|a| a.kind)
                .unwrap_or(FileType::RegularFile);

            let Some(info) = build_notify_info(&path, kind) else {
                warn!(ino, path, "notify path too long, skipping");
                continue;
            };
            notifier.notify(&info);
            debug!(ino, path, "WinFSP notify sent");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_benign_cleanup_delete_error;
    use super::{
        FILE_ACTION_MODIFIED, FILE_NO_INTERMEDIATE_BUFFERING_OPTION, FILE_NOTIFY_CHANGE_ATTRIBUTES,
        FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME, FILE_NOTIFY_CHANGE_LAST_WRITE,
        FILE_NOTIFY_CHANGE_SIZE, FILE_WRITE_THROUGH_OPTION, STATUS_ACCESS_DENIED,
        STATUS_EA_TOO_LARGE, STATUS_INVALID_PARAMETER, STATUS_OBJECT_NAME_NOT_FOUND,
        StudioFsContext, WinfspKernelInvalidator, build_notify_info, ino_to_windows_path,
        notify_filter_for_kind, open_flags_from_create_options, vfs_to_winfsp_err,
    };
    use parking_lot::Mutex;
    use std::sync::Arc;
    use studiofs_data::store::DataStoreConfig;
    use studiofs_meta::MetaError;
    use studiofs_meta::{
        CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, ROOT_INO, RedbMetaEngine,
    };
    use studiofs_vfs::VfsError;
    use studiofs_vfs::{KernelInvalidator, OpenFlags, StudioVfs, VfsConfig};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("winfsp-test-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("winfsp-test-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_test_vfs(tmp: &TempDir, volume_suffix: &str, rt: &Runtime) -> Arc<StudioVfs> {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let _guard = rt.enter();
        StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs")
    }

    fn make_test_context(
        volume_suffix: &str,
    ) -> (
        TempDir,
        Arc<StudioVfs>,
        StudioFsContext,
        Arc<Mutex<Vec<u64>>>,
    ) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let queue = Arc::new(Mutex::new(Vec::new()));
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let vfs = make_test_vfs(&tmp, volume_suffix, &rt);
        let ctx = StudioFsContext::new(vfs.clone(), rt, queue.clone());
        (tmp, vfs, ctx, queue)
    }

    #[test]
    fn cleanup_delete_not_found_is_benign() {
        let err = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "gone".into(),
        });
        assert!(is_benign_cleanup_delete_error(&err, false));
        assert!(is_benign_cleanup_delete_error(&err, true));
    }

    #[test]
    fn create_options_map_to_unbuffered_open_flags() {
        let plain = open_flags_from_create_options(0);
        assert!(!plain.contains(OpenFlags::DIRECT_IO));
        assert!(!plain.contains(OpenFlags::WRITE_THROUGH));
        assert!(!plain.requests_unbuffered_io());

        let unbuffered = open_flags_from_create_options(
            FILE_NO_INTERMEDIATE_BUFFERING_OPTION | FILE_WRITE_THROUGH_OPTION,
        );
        assert!(unbuffered.contains(OpenFlags::DIRECT_IO));
        assert!(unbuffered.contains(OpenFlags::WRITE_THROUGH));
        assert!(unbuffered.requests_unbuffered_io());
    }

    #[test]
    fn vfs_error_maps_to_expected_ntstatus() {
        let not_found = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "missing".into(),
        });
        let status = vfs_to_winfsp_err(not_found).to_ntstatus();
        assert_eq!(status, STATUS_OBJECT_NAME_NOT_FOUND.0);

        let invalid = VfsError::InvalidName("bad".into());
        let status = vfs_to_winfsp_err(invalid).to_ntstatus();
        assert_eq!(status, STATUS_INVALID_PARAMETER.0);

        let xattr_too_big = VfsError::Meta(MetaError::XattrValueTooLarge {
            actual: 17,
            limit: 16,
        });
        let status = vfs_to_winfsp_err(xattr_too_big).to_ntstatus();
        assert_eq!(status, STATUS_EA_TOO_LARGE.0);
    }

    #[test]
    fn io_permission_denied_maps_to_access_denied() {
        let err = VfsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        let status = vfs_to_winfsp_err(err).to_ntstatus();
        assert_eq!(status, STATUS_ACCESS_DENIED.0);
    }

    #[test]
    fn cleanup_delete_race_directory_is_idempotent() {
        let file_delete = VfsError::Meta(MetaError::IsADirectory(42));
        assert!(is_benign_cleanup_delete_error(&file_delete, false));
        assert!(!is_benign_cleanup_delete_error(&file_delete, true));

        let dir_delete = VfsError::Meta(MetaError::NotADirectory(42));
        assert!(is_benign_cleanup_delete_error(&dir_delete, true));
        assert!(!is_benign_cleanup_delete_error(&dir_delete, false));
    }

    #[test]
    fn winfsp_invalidator_enqueues_inode_numbers() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let invalidator = WinfspKernelInvalidator::new(queue.clone());

        invalidator.inval_inode(42);
        invalidator.inval_inode(7);

        assert_eq!(*queue.lock(), vec![42, 7]);
    }

    #[test]
    fn should_notify_drains_and_deduplicates_queue() {
        let (_tmp, _vfs, ctx, queue) = make_test_context("notify_dedup");
        queue.lock().extend([9, 2, 9, 7, 2]);

        let first = <StudioFsContext as winfsp::notify::NotifyingFileSystemContext<Vec<u64>>>::should_notify(&ctx);
        assert_eq!(first, Some(vec![2, 7, 9]));
        assert!(
            queue.lock().is_empty(),
            "queue should be drained after should_notify"
        );

        let second = <StudioFsContext as winfsp::notify::NotifyingFileSystemContext<Vec<u64>>>::should_notify(&ctx);
        assert_eq!(second, None);
    }

    #[test]
    fn ino_to_windows_path_resolves_nested_entries() {
        let (_tmp, vfs, _ctx, _queue) = make_test_context("nested_path");

        let dir = vfs.mkdir(ROOT_INO, b"parent", 0, 0, 0o755).expect("mkdir");
        let file = vfs
            .create(dir.ino, b"child.txt", 0, 0, 0o644)
            .expect("create");

        assert_eq!(ino_to_windows_path(&vfs, ROOT_INO), Some("\\".to_string()));
        assert_eq!(
            ino_to_windows_path(&vfs, dir.ino),
            Some("\\parent".to_string())
        );
        assert_eq!(
            ino_to_windows_path(&vfs, file.ino),
            Some("\\parent\\child.txt".to_string())
        );
    }

    #[test]
    fn ino_to_windows_path_returns_none_after_unlink() {
        let (_tmp, vfs, _ctx, _queue) = make_test_context("deleted_path");

        let file = vfs
            .create(ROOT_INO, b"gone.txt", 0, 0, 0o644)
            .expect("create");
        assert_eq!(
            ino_to_windows_path(&vfs, file.ino),
            Some("\\gone.txt".to_string())
        );

        vfs.unlink(ROOT_INO, b"gone.txt").expect("unlink");
        assert_eq!(ino_to_windows_path(&vfs, file.ino), None);
    }

    #[test]
    fn notify_filter_matches_inode_kind() {
        assert_eq!(
            notify_filter_for_kind(studiofs_meta::FileType::Directory),
            FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_ATTRIBUTES
        );
        assert_eq!(
            notify_filter_for_kind(studiofs_meta::FileType::RegularFile),
            FILE_NOTIFY_CHANGE_FILE_NAME
                | FILE_NOTIFY_CHANGE_SIZE
                | FILE_NOTIFY_CHANGE_ATTRIBUTES
                | FILE_NOTIFY_CHANGE_LAST_WRITE
        );
    }

    #[test]
    fn build_notify_info_rejects_overlong_paths() {
        let info = build_notify_info("\\short.txt", studiofs_meta::FileType::RegularFile)
            .expect("notify info");
        assert_eq!(info.action, FILE_ACTION_MODIFIED);
        assert_eq!(
            info.filter,
            FILE_NOTIFY_CHANGE_FILE_NAME
                | FILE_NOTIFY_CHANGE_SIZE
                | FILE_NOTIFY_CHANGE_ATTRIBUTES
                | FILE_NOTIFY_CHANGE_LAST_WRITE
        );

        let overlong = format!("\\{}", "a".repeat(1024));
        assert!(
            build_notify_info(&overlong, studiofs_meta::FileType::RegularFile).is_none(),
            "WinFSP notify helper should skip overlong paths"
        );
    }
}
