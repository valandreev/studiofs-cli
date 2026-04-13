#![cfg(windows)]
//! FileSystemContext implementation — bridges WinFSP calls to StudioVfs.

use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_meta::types::{SetattrArgs, TimeSpec};
use studiofs_meta::{FileType, Ino, InodeAttr};
use studiofs_vfs::{
    BarrierObservabilityPort, BridgeVfsErrorKind, DeferredCleanupHintPort, DeleteCleanupPort,
    FlushGatePort, HandleDrainBarrier, HandleDrainDeleteRequest, HandleDrainRequest,
    HandleReleasePort, KernelInvalidator, LocalSyncBarrierPort, NamespacePathStyle, OpenFlags,
    StudioVfs, classify_vfs_error, normalize_attr_times,
};
use tokio::runtime::Handle;
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

// ─── NTSTATUS constants ──────────────────────────────────────────────────────

const STATUS_OBJECT_NAME_NOT_FOUND: NTSTATUS = NTSTATUS(0xC000_0034_u32 as i32);
const STATUS_OBJECT_NAME_COLLISION: NTSTATUS = NTSTATUS(0xC000_0035_u32 as i32);
const STATUS_DIRECTORY_NOT_EMPTY: NTSTATUS = NTSTATUS(0xC000_0101_u32 as i32);
const STATUS_NOT_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_0103_u32 as i32);
const STATUS_FILE_IS_A_DIRECTORY: NTSTATUS = NTSTATUS(0xC000_00BA_u32 as i32);
const STATUS_UNRECOGNIZED_VOLUME: NTSTATUS = NTSTATUS(0xC000_014F_u32 as i32);
const STATUS_DATA_ERROR: NTSTATUS = NTSTATUS(0xC000_003E_u32 as i32);
const STATUS_IO_TIMEOUT: NTSTATUS = NTSTATUS(0xC000_00B5_u32 as i32);
const STATUS_INSUFFICIENT_RESOURCES: NTSTATUS = NTSTATUS(0xC000_009A_u32 as i32);
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

    /// # Safety
    ///
    /// `self.ptr` must be valid, properly aligned, and point to `self.len`
    /// initialised bytes.  The caller must ensure exclusive access to the
    /// backing memory for the entire lifetime `'a`.
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

/// Map WinFSP create options to the single supported Windows handle mode.
///
/// As part of SFS-122, StudioFS removed the Windows direct-I/O product surface.
/// `FILE_NO_INTERMEDIATE_BUFFERING` and `FILE_WRITE_THROUGH` therefore no longer
/// select an alternate VFS path and are intentionally ignored here so all
/// Windows handles use the buffered runtime model.
fn open_flags_from_create_options(_create_options: u32) -> OpenFlags {
    OpenFlags::RDWR
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CreateCallbackKind {
    Symlink(Vec<u8>),
    Directory,
    RegularFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileSizeCallbackAction {
    KeepLogicalSize,
    SetLogicalSize(u64),
}

struct CreateCallbackPlan {
    parent_ino: Ino,
    name: Vec<u8>,
    open_flags: OpenFlags,
    kind: CreateCallbackKind,
}

struct RenameCallbackPlan {
    old_parent: Ino,
    old_name: Vec<u8>,
    new_parent: Ino,
    new_name: Vec<u8>,
}

fn resolve_create_callback_kind(
    create_options: u32,
    extra_buffer: Option<&[u8]>,
    extra_buffer_is_reparse_point: bool,
) -> CreateCallbackKind {
    if extra_buffer_is_reparse_point
        && let Some(buf) = extra_buffer
        && let Some(target) = decode_symlink_reparse(buf)
    {
        return CreateCallbackKind::Symlink(target);
    }

    if (create_options & FILE_DIRECTORY_FILE_OPTION) != 0 {
        CreateCallbackKind::Directory
    } else {
        CreateCallbackKind::RegularFile
    }
}

fn resolve_file_size_callback_action(
    new_size: u64,
    set_allocation_size: bool,
) -> FileSizeCallbackAction {
    if set_allocation_size {
        FileSizeCallbackAction::KeepLogicalSize
    } else {
        FileSizeCallbackAction::SetLogicalSize(new_size)
    }
}

fn build_basic_info_setattr_args(
    file_attributes: u32,
    creation_time: u64,
    last_access_time: u64,
    last_write_time: u64,
) -> SetattrArgs {
    // WinFSP convention: INVALID_FILE_ATTRIBUTES (0xFFFFFFFF) means "don't
    // change". 0 also means "don't change" (per MSDN FILE_BASIC_INFO).
    let win_attrs = if file_attributes != 0 && file_attributes != u32::MAX {
        Some(file_attributes)
    } else {
        None
    };

    SetattrArgs {
        atime: filetime_to_timespec(last_access_time),
        mtime: filetime_to_timespec(last_write_time),
        crtime: filetime_to_timespec(creation_time),
        win_attrs,
        ..SetattrArgs::default()
    }
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
    let times = normalize_attr_times(attr);
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
    info.creation_time = unix_nanos_to_filetime(times.creation_time_unix_nanos);
    info.last_access_time = unix_nanos_to_filetime(times.atime_unix_nanos);
    info.last_write_time = unix_nanos_to_filetime(times.mtime_unix_nanos);
    info.change_time = unix_nanos_to_filetime(times.ctime_unix_nanos);

    // Use inode as index
    info.index_number = attr.ino;
    // Report actual nlink count (must be ≥1 for files, ≥2 for dirs)
    info.hard_links = if attr.nlink > 0 { attr.nlink } else { 1 };

    info
}

/// Convert Unix timestamp (nanoseconds since epoch) to Windows FILETIME (100ns since 1601-01-01).
fn unix_nanos_to_filetime(unix_nanos: u64) -> u64 {
    const EPOCH_DIFF: u64 = 116_444_736_000_000_000; // Difference between 1601-01-01 and 1970-01-01 in 100ns intervals
    const NANOS_PER_100NS: u64 = 100;

    // Convert nanoseconds to 100ns intervals
    let intervals = unix_nanos / NANOS_PER_100NS;
    intervals.saturating_add(EPOCH_DIFF)
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

fn note_critical_vfs_error(error: &studiofs_vfs::VfsError) {
    if matches!(
        classify_vfs_error(error),
        BridgeVfsErrorKind::CriticalRuntimeFailure
    ) && let studiofs_vfs::VfsError::Join(join_err) = error
    {
        error!(
            error = %join_err,
            "critical runtime failure (JoinError), requesting emergency unmount"
        );
        crate::request_shutdown();
    }
}

/// Map VfsError to WinFSP NTSTATUS error.
fn vfs_to_winfsp_err(e: studiofs_vfs::VfsError) -> winfsp::FspError {
    note_critical_vfs_error(&e);
    match classify_vfs_error(&e) {
        BridgeVfsErrorKind::NotFound => winfsp::FspError::from(STATUS_OBJECT_NAME_NOT_FOUND),
        BridgeVfsErrorKind::AlreadyExists => winfsp::FspError::from(STATUS_OBJECT_NAME_COLLISION),
        BridgeVfsErrorKind::DirectoryNotEmpty => winfsp::FspError::from(STATUS_DIRECTORY_NOT_EMPTY),
        BridgeVfsErrorKind::NotDirectory => winfsp::FspError::from(STATUS_NOT_A_DIRECTORY),
        BridgeVfsErrorKind::IsDirectory => winfsp::FspError::from(STATUS_FILE_IS_A_DIRECTORY),
        BridgeVfsErrorKind::UnrecognizedVolume => {
            winfsp::FspError::from(STATUS_UNRECOGNIZED_VOLUME)
        }
        BridgeVfsErrorKind::XattrValueTooLarge => winfsp::FspError::from(STATUS_EA_TOO_LARGE),
        BridgeVfsErrorKind::InvalidInput => winfsp::FspError::from(STATUS_INVALID_PARAMETER),
        BridgeVfsErrorKind::PermissionDenied => winfsp::FspError::from(STATUS_ACCESS_DENIED),
        BridgeVfsErrorKind::TimedOut => winfsp::FspError::from(STATUS_IO_TIMEOUT),
        BridgeVfsErrorKind::InsufficientResources => {
            winfsp::FspError::from(STATUS_INSUFFICIENT_RESOURCES)
        }
        BridgeVfsErrorKind::DataCorruption => winfsp::FspError::from(STATUS_DATA_ERROR),
        BridgeVfsErrorKind::Retryable => {
            match &e {
                studiofs_vfs::VfsError::WouldBlock => debug!("vfs lock conflict: {e}"),
                studiofs_vfs::VfsError::RemoteContentPending => {
                    debug!("vfs remote content pending: {e}")
                }
                _ => debug!("retryable vfs error: {e}"),
            }
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
        BridgeVfsErrorKind::CriticalRuntimeFailure => winfsp::FspError::from(STATUS_UNSUCCESSFUL),
        BridgeVfsErrorKind::GenericFailure => {
            if matches!(e, studiofs_vfs::VfsError::Meta(_)) {
                error!("metadata error: {e}");
            } else {
                error!("vfs error: {e}");
            }
            winfsp::FspError::from(STATUS_UNSUCCESSFUL)
        }
    }
}

fn is_benign_cleanup_delete_error(e: &studiofs_vfs::VfsError, deleting_dir: bool) -> bool {
    match classify_vfs_error(e) {
        BridgeVfsErrorKind::NotFound => true,
        BridgeVfsErrorKind::NotDirectory => deleting_dir,
        BridgeVfsErrorKind::IsDirectory => !deleting_dir,
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
    rt: Handle,
    /// Cached binary security descriptor (Everyone = Full Access).
    default_sd: Vec<u8>,
    /// Pending kernel cache invalidation queue (shared with
    /// `WinfspKernelInvalidator` so metadata refresh events can enqueue inodes
    /// and the WinFSP timer can drain them).
    invalidation_queue: Arc<Mutex<Vec<Ino>>>,
    callback_barrier_state: CallbackBarrierState,
}

struct CallbackBarrierState {
    flush_gates: FlushGateRegistry,
    deferred_cleanup_hints: DeferredCleanupHints,
}

struct FlushGateRegistry {
    /// Per-inode gates used to collapse concurrent callback-driven flush
    /// barriers into one effective in-flight flush.
    gates: Mutex<HashMap<Ino, Arc<Mutex<()>>>>,
}

struct DeferredCleanupHints {
    /// Inodes whose non-delete `cleanup` observed dirty local state and
    /// intentionally deferred the barrier to a later explicit `flush` or
    /// last-handle `close`/`release_fh`.
    inodes: Mutex<HashSet<Ino>>,
}

impl CallbackBarrierState {
    fn new() -> Self {
        Self {
            flush_gates: FlushGateRegistry::new(),
            deferred_cleanup_hints: DeferredCleanupHints::new(),
        }
    }

    fn flush_gate_for_inode(&self, ino: Ino) -> Arc<Mutex<()>> {
        self.flush_gates.gate_for_inode(ino)
    }

    fn maybe_release_flush_gate(&self, ino: Ino, gate: &Arc<Mutex<()>>) {
        self.flush_gates.maybe_release(ino, gate);
    }

    fn set_deferred_cleanup_hint(&self, ino: Ino) {
        self.deferred_cleanup_hints.set(ino);
    }

    fn clear_deferred_cleanup_hint(&self, ino: Ino) {
        self.deferred_cleanup_hints.clear(ino);
    }

    fn take_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.deferred_cleanup_hints.take(ino)
    }

    fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.deferred_cleanup_hints.contains(ino)
    }

    #[cfg(test)]
    fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.deferred_cleanup_hints.contains(ino)
    }

    #[cfg(test)]
    fn flush_gate_count(&self) -> usize {
        self.flush_gates.len()
    }
}

impl FlushGateRegistry {
    fn new() -> Self {
        Self {
            gates: Mutex::new(HashMap::new()),
        }
    }

    fn gate_for_inode(&self, ino: Ino) -> Arc<Mutex<()>> {
        self.gates
            .lock()
            .entry(ino)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn maybe_release(&self, ino: Ino, gate: &Arc<Mutex<()>>) {
        let mut gates = self.gates.lock();
        let should_remove = gates
            .get(&ino)
            .is_some_and(|current| Arc::ptr_eq(current, gate) && Arc::strong_count(current) == 2);
        if should_remove {
            gates.remove(&ino);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.gates.lock().len()
    }
}

impl DeferredCleanupHints {
    fn new() -> Self {
        Self {
            inodes: Mutex::new(HashSet::new()),
        }
    }

    fn set(&self, ino: Ino) {
        self.inodes.lock().insert(ino);
    }

    fn clear(&self, ino: Ino) {
        self.inodes.lock().remove(&ino);
    }

    fn take(&self, ino: Ino) -> bool {
        self.inodes.lock().remove(&ino)
    }

    fn contains(&self, ino: Ino) -> bool {
        self.inodes.lock().contains(&ino)
    }
}

struct FlushGateRelease<'a> {
    state: &'a CallbackBarrierState,
    ino: Ino,
    gate: Arc<Mutex<()>>,
}

impl<'a> FlushGateRelease<'a> {
    fn new(state: &'a CallbackBarrierState, ino: Ino, gate: Arc<Mutex<()>>) -> Self {
        Self { state, ino, gate }
    }
}

impl Drop for FlushGateRelease<'_> {
    fn drop(&mut self) {
        self.state.maybe_release_flush_gate(self.ino, &self.gate);
    }
}

enum CleanupCallbackAction {
    Delete(HandleDrainDeleteRequest),
    SkipDeletePath { ino: Ino, is_dir: bool },
    NonDelete { ino: Ino, fh: u64 },
}

type WinfspHandleDrainBarrier<'a> = HandleDrainBarrier<
    WinfspLocalSyncBarrierPort<'a>,
    WinfspHandleReleasePort<'a>,
    WinfspDeferredCleanupHintPort<'a>,
    WinfspFlushGatePort<'a>,
    WinfspDeleteCleanupPort<'a>,
    WinfspBarrierObservabilityPort<'a>,
>;

#[derive(Clone, Copy)]
struct WinfspLocalSyncBarrierPort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspHandleReleasePort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspDeferredCleanupHintPort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspFlushGatePort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspDeleteCleanupPort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspBarrierObservabilityPort<'a> {
    context: &'a StudioFsContext,
}

impl LocalSyncBarrierPort for WinfspLocalSyncBarrierPort<'_> {
    fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
        self.context.vfs.inode_has_local_dirty_state(ino)
    }

    fn explicit_flush(
        &self,
        request: HandleDrainRequest,
    ) -> studiofs_vfs::VfsResult<studiofs_vfs::ExplicitFlushOutcome> {
        let vfs = self.context.vfs.clone();
        self.context.spawn_wait(async move {
            if request.fh != 0 {
                vfs.flush_fh_with_outcome(request.fh).await
            } else {
                vfs.flush_with_outcome(request.ino).await
            }
        })
    }
}

impl HandleReleasePort for WinfspHandleReleasePort<'_> {
    fn release_handle(
        &self,
        request: HandleDrainRequest,
    ) -> studiofs_vfs::VfsResult<studiofs_vfs::ReleaseFhOutcome> {
        let vfs = self.context.vfs.clone();
        self.context
            .spawn_wait(async move { vfs.release_fh_with_outcome(request.fh).await })
    }
}

impl DeferredCleanupHintPort for WinfspDeferredCleanupHintPort<'_> {
    fn set_deferred_cleanup_hint(&self, ino: Ino) {
        self.context
            .callback_barrier_state
            .set_deferred_cleanup_hint(ino);
    }

    fn clear_deferred_cleanup_hint(&self, ino: Ino) {
        self.context
            .callback_barrier_state
            .clear_deferred_cleanup_hint(ino);
    }

    fn take_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.context
            .callback_barrier_state
            .take_deferred_cleanup_hint(ino)
    }

    fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.context
            .callback_barrier_state
            .has_deferred_cleanup_hint(ino)
    }
}

impl FlushGatePort for WinfspFlushGatePort<'_> {
    fn with_flush_gate<T, F>(&self, ino: Ino, op: F) -> studiofs_vfs::VfsResult<T>
    where
        F: FnOnce() -> studiofs_vfs::VfsResult<T>,
    {
        // Tokio recommends a blocking mutex for plain data when the guard is
        // not held across `.await`. This gate lives entirely on the synchronous
        // WinFSP callback thread and only serializes per-inode barrier entry
        // into `spawn_wait`; the async flush work itself runs after the gate is
        // acquired and never holds this guard across an `.await`.
        let gate = self
            .context
            .callback_barrier_state
            .flush_gate_for_inode(ino);
        let _flush_guard = gate.lock();
        let _gate_release = FlushGateRelease::new(&self.context.callback_barrier_state, ino, gate);
        op()
    }
}

impl DeleteCleanupPort for WinfspDeleteCleanupPort<'_> {
    fn cleanup_delete(&self, request: HandleDrainDeleteRequest) {
        let result = if request.is_dir {
            self.context.vfs.rmdir(request.parent_ino, &request.name)
        } else {
            self.context.vfs.unlink(request.parent_ino, &request.name)
        };

        match result {
            Ok(()) => {
                debug!(ino = request.ino, "cleanup delete");
            }
            Err(error) if is_benign_cleanup_delete_error(&error, request.is_dir) => {
                debug!(
                    ino = request.ino,
                    is_dir = request.is_dir,
                    error = ?error,
                    "cleanup delete skipped stale race"
                );
            }
            Err(error) => {
                error!("cleanup delete failed: {:?}", error);
            }
        }
    }
}

impl BarrierObservabilityPort for WinfspBarrierObservabilityPort<'_> {
    fn note_cleanup_callback(&self) {
        self.context.vfs.note_winfsp_cleanup_callback();
    }

    fn note_flush_callback(&self) {
        self.context.vfs.note_winfsp_flush_callback();
    }

    fn note_flush_barrier_outcome(&self, flushed_local_dirty_state: bool) {
        self.context
            .vfs
            .note_winfsp_flush_barrier_outcome(flushed_local_dirty_state);
    }

    fn note_close_callback(&self) {
        self.context.vfs.note_winfsp_close_callback();
    }

    fn note_release_success(
        &self,
        request: HandleDrainRequest,
        hint_was_taken: bool,
        outcome: studiofs_vfs::ReleaseFhOutcome,
    ) {
        self.context
            .vfs
            .note_winfsp_close_barrier_outcome(outcome.flushed_local_dirty_state);
        if hint_was_taken && outcome.flushed_local_dirty_state {
            debug!(
                ino = request.ino,
                fh = request.fh,
                "close owned deferred cleanup barrier on the last handle"
            );
        } else if hint_was_taken {
            trace!(
                ino = request.ino,
                fh = request.fh,
                "close observed deferred cleanup hint but no last-handle barrier was needed"
            );
        }
    }

    fn note_release_error(
        &self,
        request: HandleDrainRequest,
        cleanup_deferred_hint: bool,
        error: &studiofs_vfs::VfsError,
    ) {
        error!(
            ino = request.ino,
            fh = request.fh,
            cleanup_deferred = cleanup_deferred_hint,
            "release_fh failed after WinFSP close: {:?}",
            error
        );
    }
}

impl StudioFsContext {
    pub fn new(vfs: Arc<StudioVfs>, rt: Handle, invalidation_queue: Arc<Mutex<Vec<Ino>>>) -> Self {
        let default_sd = create_default_security_descriptor();
        Self {
            vfs,
            rt,
            default_sd,
            invalidation_queue,
            callback_barrier_state: CallbackBarrierState::new(),
        }
    }

    #[cfg(test)]
    fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.callback_barrier_state.has_deferred_cleanup_hint(ino)
    }

    #[cfg(test)]
    fn flush_gate_count(&self) -> usize {
        self.callback_barrier_state.flush_gate_count()
    }

    fn handle_drain_barrier(&self) -> WinfspHandleDrainBarrier<'_> {
        HandleDrainBarrier::new(
            WinfspLocalSyncBarrierPort { context: self },
            WinfspHandleReleasePort { context: self },
            WinfspDeferredCleanupHintPort { context: self },
            WinfspFlushGatePort { context: self },
            WinfspDeleteCleanupPort { context: self },
            WinfspBarrierObservabilityPort { context: self },
        )
    }

    #[cfg(test)]
    fn flush_callback_path_if_dirty(
        &self,
        ino: Ino,
        fh: Option<u64>,
    ) -> studiofs_vfs::VfsResult<bool> {
        let local_sync = WinfspLocalSyncBarrierPort { context: self };
        WinfspFlushGatePort { context: self }.with_flush_gate(ino, || {
            if !local_sync.inode_has_local_dirty_state(ino) {
                return Ok(false);
            }
            let outcome = local_sync.explicit_flush(HandleDrainRequest {
                ino,
                fh: fh.unwrap_or(0),
            })?;
            Ok(outcome.flushed_local_dirty_state)
        })
    }

    fn run_cleanup_non_delete_callback(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<()> {
        self.handle_drain_barrier()
            .on_cleanup_non_delete(HandleDrainRequest { ino, fh });
        Ok(())
    }

    fn resolve_cleanup_callback_action(
        &self,
        context: &StudioFileHandle,
        file_name: Option<&U16CStr>,
        flags: u32,
    ) -> CleanupCallbackAction {
        if flags & 0x01 != 0 {
            if let Some(file_name) = file_name {
                return match resolve_parent(&self.vfs, file_name) {
                    Ok((parent_ino, name)) => {
                        CleanupCallbackAction::Delete(HandleDrainDeleteRequest {
                            ino: context.ino,
                            parent_ino,
                            name,
                            is_dir: context.is_dir,
                        })
                    }
                    Err(_) => CleanupCallbackAction::SkipDeletePath {
                        ino: context.ino,
                        is_dir: context.is_dir,
                    },
                };
            }
            return CleanupCallbackAction::SkipDeletePath {
                ino: context.ino,
                is_dir: context.is_dir,
            };
        }

        CleanupCallbackAction::NonDelete {
            ino: context.ino,
            fh: context.fh,
        }
    }

    fn execute_cleanup_callback_action(&self, action: CleanupCallbackAction) {
        match action {
            CleanupCallbackAction::Delete(request) => {
                self.handle_drain_barrier().on_cleanup_delete(request);
            }
            CleanupCallbackAction::SkipDeletePath { ino, is_dir } => {
                self.vfs.note_winfsp_cleanup_callback();
                debug!(ino, is_dir, "cleanup delete skipped: no path provided");
            }
            CleanupCallbackAction::NonDelete { ino, fh } => {
                let _ = self.run_cleanup_non_delete_callback(ino, fh);
            }
        }
    }

    fn run_flush_callback(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<()> {
        self.handle_drain_barrier()
            .on_flush(HandleDrainRequest { ino, fh })
    }

    fn run_close_callback(&self, ino: Ino, fh: u64) {
        self.handle_drain_barrier()
            .on_close(HandleDrainRequest { ino, fh });
    }

    fn open_file_handle_for_attr(
        &self,
        attr: &InodeAttr,
        open_flags: OpenFlags,
    ) -> winfsp::Result<StudioFileHandle> {
        let is_dir = attr.kind == FileType::Directory;
        let fh = if is_dir {
            0
        } else {
            self.vfs
                .open_file(attr.ino, open_flags, false)
                .map_err(vfs_to_winfsp_err)?
        };
        Ok(StudioFileHandle::new(attr.ino, is_dir, fh, open_flags))
    }

    fn write_open_file_info(out: &mut OpenFileInfo, attr: &InodeAttr) {
        *out.as_mut() = attr_to_file_info(attr);
    }

    fn write_file_info(out: &mut FileInfo, attr: &InodeAttr) {
        *out = attr_to_file_info(attr);
    }

    fn resolve_create_callback_plan(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        extra_buffer: Option<&[u8]>,
        extra_buffer_is_reparse_point: bool,
    ) -> winfsp::Result<CreateCallbackPlan> {
        let (parent_ino, name) = resolve_parent(&self.vfs, file_name)?;
        Ok(CreateCallbackPlan {
            parent_ino,
            name,
            open_flags: open_flags_from_create_options(create_options),
            kind: resolve_create_callback_kind(
                create_options,
                extra_buffer,
                extra_buffer_is_reparse_point,
            ),
        })
    }

    fn execute_create_callback_plan(
        &self,
        plan: CreateCallbackPlan,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<StudioFileHandle> {
        let opens_symlink = matches!(plan.kind, CreateCallbackKind::Symlink(_));
        let attr = match &plan.kind {
            CreateCallbackKind::Symlink(target) => self
                .vfs
                .symlink(plan.parent_ino, &plan.name, target, 0, 0)
                .map_err(vfs_to_winfsp_err)?,
            CreateCallbackKind::Directory => self
                .vfs
                .mkdir(plan.parent_ino, &plan.name, 0, 0, 0o755)
                .map_err(vfs_to_winfsp_err)?,
            CreateCallbackKind::RegularFile => self
                .vfs
                .create(plan.parent_ino, &plan.name, 0, 0, 0o644)
                .map_err(vfs_to_winfsp_err)?,
        };
        Self::write_open_file_info(file_info, &attr);
        if opens_symlink {
            Ok(StudioFileHandle::new(attr.ino, false, 0, plan.open_flags))
        } else {
            self.open_file_handle_for_attr(&attr, plan.open_flags)
        }
    }

    fn execute_basic_info_callback(
        &self,
        ino: Ino,
        args: SetattrArgs,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self.vfs.setattr(ino, args).map_err(vfs_to_winfsp_err)?;
        Self::write_file_info(file_info, &attr);
        Ok(())
    }

    fn execute_file_size_callback(
        &self,
        ino: Ino,
        action: FileSizeCallbackAction,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = match action {
            FileSizeCallbackAction::KeepLogicalSize => {
                self.vfs.getattr(ino).map_err(vfs_to_winfsp_err)?
            }
            FileSizeCallbackAction::SetLogicalSize(new_size) => self
                .vfs
                .setattr(ino, SetattrArgs::size_only(new_size))
                .map_err(vfs_to_winfsp_err)?,
        };
        Self::write_file_info(file_info, &attr);
        Ok(())
    }

    fn resolve_rename_callback_plan(
        &self,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
    ) -> winfsp::Result<RenameCallbackPlan> {
        let (old_parent, old_name) = resolve_parent(&self.vfs, file_name)?;
        let (new_parent, new_name) = resolve_parent(&self.vfs, new_file_name)?;
        Ok(RenameCallbackPlan {
            old_parent,
            old_name,
            new_parent,
            new_name,
        })
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
    /// Local cache I/O runs inline once inside this runtime boundary, so we
    /// avoid per-operation blocking-pool hops in the hot read/write path.
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
        let open_flags = open_flags_from_create_options(create_options);
        Self::write_open_file_info(file_info, &attr);
        let handle = self.open_file_handle_for_attr(&attr, open_flags)?;

        debug!(
            ino = attr.ino,
            fh = handle.fh,
            is_dir = handle.is_dir,
            open_flags = open_flags.bits(),
            "open"
        );
        Ok(handle)
    }

    fn close(&self, context: Self::FileContext) {
        debug!(ino = context.ino, fh = context.fh, "close");
        self.run_close_callback(context.ino, context.fh);
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
        let plan = self.resolve_create_callback_plan(
            file_name,
            create_options,
            extra_buffer,
            extra_buffer_is_reparse_point,
        )?;
        let handle = self.execute_create_callback_plan(plan, file_info)?;

        debug!(
            ino = handle.ino,
            is_dir = handle.is_dir,
            fh = handle.fh,
            open_flags = handle.open_flags.bits(),
            "create {:?}",
            file_name.to_string_lossy()
        );
        Ok(handle)
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
        self.vfs.note_winfsp_write_callback();
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
        // Current dirty-handle ordering is:
        //   write/overwrite -> optional flush -> cleanup(non-delete) -> close -> release_fh
        //
        // Non-delete `cleanup` no longer owns a foreground barrier. It is
        // still counted for observability, but dirty local state stays
        // deferred until an explicit `flush` or the mandatory last-handle
        // `close`/`release_fh` fallback drain.
        let action = self.resolve_cleanup_callback_action(context, file_name, flags);
        self.execute_cleanup_callback_action(action);
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
        self.vfs.note_winfsp_overwrite_callback();
        // `Overwrite` is an immediate EOF transition, not a backgroundable
        // flush barrier. Later reads/writes in the same open sequence depend on
        // size=0 being visible before the callback returns.
        self.execute_file_size_callback(
            context.ino,
            FileSizeCallbackAction::SetLogicalSize(0),
            file_info,
        )
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
        self.execute_basic_info_callback(
            context.ino,
            build_basic_info_setattr_args(
                file_attributes,
                creation_time,
                last_access_time,
                last_write_time,
            ),
            file_info,
        )
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
        self.execute_file_size_callback(
            context.ino,
            resolve_file_size_callback_action(new_size, set_allocation_size),
            file_info,
        )
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
        let plan = self.resolve_rename_callback_plan(file_name, new_file_name)?;

        self.vfs
            .rename(
                plan.old_parent,
                &plan.old_name,
                plan.new_parent,
                &plan.new_name,
            )
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
            self.run_flush_callback(ctx.ino, ctx.fh)
                .map_err(vfs_to_winfsp_err)?;

            let attr = self.vfs.getattr(ctx.ino).map_err(vfs_to_winfsp_err)?;
            *file_info = attr_to_file_info(&attr);
            debug!(ino = ctx.ino, "flush");
        } else {
            self.vfs.note_winfsp_flush_callback();
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
        CreateCallbackKind, FILE_ACTION_MODIFIED, FILE_DIRECTORY_FILE_OPTION,
        FILE_NOTIFY_CHANGE_ATTRIBUTES, FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME,
        FILE_NOTIFY_CHANGE_LAST_WRITE, FILE_NOTIFY_CHANGE_SIZE, FileSizeCallbackAction,
        STATUS_ACCESS_DENIED, STATUS_DATA_ERROR, STATUS_EA_TOO_LARGE, STATUS_INVALID_PARAMETER,
        STATUS_IO_TIMEOUT, STATUS_OBJECT_NAME_NOT_FOUND, StudioFsContext, WinfspKernelInvalidator,
        attr_to_file_info, build_basic_info_setattr_args, build_notify_info,
        encode_symlink_reparse, ino_to_windows_path, notify_filter_for_kind,
        open_flags_from_create_options, resolve_create_callback_kind,
        resolve_file_size_callback_action, unix_nanos_to_filetime, vfs_to_winfsp_err,
    };
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::sync::Barrier;
    use studiofs_data::store::DataStoreConfig;
    use studiofs_meta::MetaError;
    use studiofs_meta::{
        CURRENT_FORMAT_VERSION, FormatInfo, InodeAttr, MetaFormat, MetadataConfig, ROOT_INO,
        RedbMetaEngine, SetattrArgs, TimeSpec,
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
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        vfs.start().expect("start vfs background workers");
        vfs
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
        let ctx = StudioFsContext::new(vfs.clone(), rt.handle().clone(), queue.clone());
        (tmp, vfs, ctx, queue)
    }

    fn flush_ordered_calls(vfs: &StudioVfs) -> u64 {
        vfs.write_path_observability_snapshot().flush_ordered_calls
    }

    fn winfsp_callbacks(vfs: &StudioVfs) -> studiofs_vfs::VfsWritePathWinfspCallbackStats {
        vfs.write_path_observability_snapshot().winfsp_callbacks
    }

    fn winfsp_foreground_flushes(
        vfs: &StudioVfs,
    ) -> studiofs_vfs::VfsWritePathWinfspForegroundFlushStats {
        vfs.write_path_observability_snapshot()
            .winfsp_foreground_flushes
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
    fn create_options_ignore_unbuffered_bits() {
        const FILE_WRITE_THROUGH_OPTION: u32 = 0x0000_0002;
        const FILE_NO_INTERMEDIATE_BUFFERING_OPTION: u32 = 0x0000_0008;

        let plain = open_flags_from_create_options(0);
        assert_eq!(plain.bits(), OpenFlags::RDWR.bits());

        let unbuffered = open_flags_from_create_options(
            FILE_NO_INTERMEDIATE_BUFFERING_OPTION | FILE_WRITE_THROUGH_OPTION,
        );
        assert_eq!(unbuffered.bits(), OpenFlags::RDWR.bits());
    }

    #[test]
    fn resolve_create_callback_kind_prefers_decoded_reparse_target() {
        let encoded = encode_symlink_reparse(b"/target/path");
        assert_eq!(
            resolve_create_callback_kind(FILE_DIRECTORY_FILE_OPTION, Some(&encoded), true),
            CreateCallbackKind::Symlink(b"/target/path".to_vec())
        );
    }

    #[test]
    fn resolve_create_callback_kind_falls_back_to_directory_or_file() {
        assert_eq!(
            resolve_create_callback_kind(FILE_DIRECTORY_FILE_OPTION, None, false),
            CreateCallbackKind::Directory
        );
        assert_eq!(
            resolve_create_callback_kind(0, None, false),
            CreateCallbackKind::RegularFile
        );
        assert_eq!(
            resolve_create_callback_kind(0, Some(&[1, 2, 3]), true),
            CreateCallbackKind::RegularFile,
            "invalid reparse payload should fall back to regular file creation"
        );
    }

    #[test]
    fn resolve_file_size_callback_action_distinguishes_allocation_only_requests() {
        assert_eq!(
            resolve_file_size_callback_action(123, true),
            FileSizeCallbackAction::KeepLogicalSize
        );
        assert_eq!(
            resolve_file_size_callback_action(456, false),
            FileSizeCallbackAction::SetLogicalSize(456)
        );
    }

    #[test]
    fn build_basic_info_setattr_args_ignores_invalid_windows_attributes() {
        let ignored_zero = build_basic_info_setattr_args(0, 0, 0, 0);
        assert_eq!(ignored_zero.win_attrs, None);
        assert!(matches!(ignored_zero.atime, TimeSpec::Omit));
        assert!(matches!(ignored_zero.mtime, TimeSpec::Omit));
        assert!(matches!(ignored_zero.crtime, TimeSpec::Omit));

        let ignored_invalid = build_basic_info_setattr_args(u32::MAX, 0, 0, 0);
        assert_eq!(ignored_invalid.win_attrs, None);

        let populated = build_basic_info_setattr_args(0x20, 1, 2, 3);
        assert_eq!(populated.win_attrs, Some(0x20));
        assert!(matches!(populated.crtime, TimeSpec::Set(_)));
        assert!(matches!(populated.atime, TimeSpec::Set(_)));
        assert!(matches!(populated.mtime, TimeSpec::Set(_)));
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

        let denied = VfsError::Meta(MetaError::AccessDenied {
            ino: 7,
            principal: "user".into(),
            required: studiofs_meta::AccessLevel::ReadWrite,
            actual: studiofs_meta::AccessLevel::ReadOnly,
        });
        let status = vfs_to_winfsp_err(denied).to_ntstatus();
        assert_eq!(status, STATUS_ACCESS_DENIED.0);

        let fragmented = VfsError::Meta(MetaError::FragmentedBlock {
            ino: 7,
            block_index: 3,
        });
        let status = vfs_to_winfsp_err(fragmented).to_ntstatus();
        assert_eq!(status, STATUS_DATA_ERROR.0);

        let exhausted = VfsError::Fetch(studiofs_vfs::fetch::FetchError::Exhausted {
            attempts: 3,
            source: studiofs_data::store::DataError::Storage("timeout".into()),
        });
        let status = vfs_to_winfsp_err(exhausted).to_ntstatus();
        assert_eq!(status, STATUS_IO_TIMEOUT.0);
    }

    #[test]
    fn io_permission_denied_maps_to_access_denied() {
        let err = VfsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        let status = vfs_to_winfsp_err(err).to_ntstatus();
        assert_eq!(status, STATUS_ACCESS_DENIED.0);
    }

    #[test]
    fn attr_to_file_info_keeps_winfsp_specific_allocation_and_reparse_behavior() {
        let mut file = InodeAttr::new_file(21, ROOT_INO, 1000, 1000, 0o644);
        file.size = 4097;
        file.nlink = 0;
        file.mtime = 123;
        file.ctime = 456;
        let file_info = attr_to_file_info(&file);
        assert_eq!(file_info.allocation_size, 8192);
        assert_eq!(file_info.hard_links, 1);
        assert_eq!(file_info.index_number, 21);
        assert_eq!(file_info.last_write_time, unix_nanos_to_filetime(123));
        assert_eq!(file_info.change_time, unix_nanos_to_filetime(456));

        let mut dir = InodeAttr::new_dir(22, ROOT_INO, 1000, 1000, 0o755);
        dir.size = 0;
        let dir_info = attr_to_file_info(&dir);
        assert_eq!(dir_info.allocation_size, 4096);

        let symlink = InodeAttr::new_symlink(23, ROOT_INO, 1000, 1000, 33);
        let symlink_info = attr_to_file_info(&symlink);
        assert_eq!(symlink_info.reparse_tag, super::IO_REPARSE_TAG_SYMLINK);
        assert_eq!(symlink_info.allocation_size, 4096);
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
    fn cleanup_non_delete_skips_sync_flush_for_clean_inode() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_skip_clean");
        let attr = vfs
            .create(ROOT_INO, b"clean-callback.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.flush(attr.ino).await }
        })
        .expect("pre-clean flush");

        let before = flush_ordered_calls(&vfs);
        let flushed = ctx
            .flush_callback_path_if_dirty(attr.ino, Some(fh))
            .expect("callback flush guard");
        let after = flush_ordered_calls(&vfs);
        assert!(
            !flushed,
            "clean inode should skip callback-driven synchronous flush"
        );
        assert_eq!(
            after, before,
            "cleanup should skip redundant synchronous flush when inode is clean"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn clean_inode_fast_path_does_not_allocate_flush_gate() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_fast_path_gate");
        let attr = vfs
            .create(ROOT_INO, b"clean-fast-path.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.flush(attr.ino).await }
        })
        .expect("pre-clean flush");

        assert!(
            ctx.flush_gate_count() == 0,
            "test setup should not have lingering gates"
        );
        let flushed = ctx
            .flush_callback_path_if_dirty(attr.ino, Some(fh))
            .expect("callback flush guard");
        assert!(!flushed, "clean inode should skip flush");
        assert!(
            ctx.flush_gate_count() == 0,
            "fast path should not allocate per-inode flush gates"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn cleanup_non_delete_defers_dirty_inode_flush_to_last_handle_release() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_defer_dirty");
        let attr = vfs
            .create(ROOT_INO, b"dirty-callback.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_ordered_calls(&vfs);

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("cleanup callback");
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should leave a deferred-close hint for dirty inodes"
        );
        let callbacks_after_cleanup = winfsp_callbacks(&vfs);
        let barriers_after_cleanup = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_cleanup = flush_ordered_calls(&vfs);
        assert_eq!(
            callbacks_after_cleanup.cleanup,
            callbacks_before.cleanup + 1,
            "cleanup callback should still be counted for dirty handles"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup, barriers_before.cleanup,
            "non-delete cleanup should no longer own a foreground barrier"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup_skipped_clean, barriers_before.cleanup_skipped_clean,
            "dirty cleanup should not be attributed as a clean-skip barrier either"
        );
        assert_eq!(
            flush_calls_after_cleanup, flush_calls_before,
            "cleanup should not flush local dirty state before close"
        );

        ctx.run_close_callback(attr.ino, fh);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "final close should clear the deferred cleanup hint"
        );
        let callbacks_after_close = winfsp_callbacks(&vfs);
        let barriers_after_close = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_close = flush_ordered_calls(&vfs);
        assert_eq!(
            callbacks_after_close.close,
            callbacks_before.close + 1,
            "close should still be counted after deferred cleanup"
        );
        assert_eq!(
            barriers_after_close.close,
            barriers_before.close + 1,
            "last-handle close should own the deferred cleanup barrier"
        );
        assert_eq!(
            barriers_after_close.close_skipped_clean, barriers_before.close_skipped_clean,
            "dirty last-handle close should not look like a clean release"
        );
        assert_eq!(
            flush_calls_after_close,
            flush_calls_before + 1,
            "release fallback should drain local dirty state exactly once"
        );
    }

    #[test]
    fn flush_callback_skips_sync_flush_for_clean_inode() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("flush_skip_clean");
        let attr = vfs
            .create(ROOT_INO, b"clean-flush-callback.txt", 0, 0, 0o644)
            .expect("create");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.flush(attr.ino).await }
        })
        .expect("pre-clean flush");

        let before = flush_ordered_calls(&vfs);
        let flushed = ctx
            .flush_callback_path_if_dirty(attr.ino, None)
            .expect("callback flush guard");
        let after = flush_ordered_calls(&vfs);
        assert!(!flushed, "clean inode should skip flush callback barrier");
        assert_eq!(
            after, before,
            "flush callback should skip redundant synchronous flush when inode is clean"
        );
    }

    #[test]
    fn concurrent_cleanup_callbacks_do_not_add_foreground_flushes() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_defer_concurrent");
        let attr = vfs
            .create(ROOT_INO, b"coalesce.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move {
                for i in 0..16u64 {
                    let offset = i * (4 * 1024 * 1024);
                    vfs.write(attr.ino, offset, b"x").await?;
                }
                Ok::<(), VfsError>(())
            }
        })
        .expect("seed dirty blocks");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_ordered_calls(&vfs);
        let ctx = Arc::new(ctx);
        let start = Arc::new(Barrier::new(4));
        let mut joins = Vec::new();
        for _ in 0..4 {
            let ctx = Arc::clone(&ctx);
            let start = Arc::clone(&start);
            let ino = attr.ino;
            joins.push(std::thread::spawn(move || {
                start.wait();
                ctx.run_cleanup_non_delete_callback(ino, fh)
                    .expect("concurrent cleanup callback");
            }));
        }
        for join in joins {
            join.join().expect("cleanup thread join");
        }
        let callbacks_after_cleanup = winfsp_callbacks(&vfs);
        let barriers_after_cleanup = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_cleanup = flush_ordered_calls(&vfs);
        assert_eq!(
            callbacks_after_cleanup.cleanup,
            callbacks_before.cleanup + 4,
            "all concurrent cleanup callbacks should still be observable"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup, barriers_before.cleanup,
            "cleanup callbacks should not add foreground barriers"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup_skipped_clean, barriers_before.cleanup_skipped_clean,
            "cleanup callbacks should not add clean-skip barrier attribution"
        );
        assert_eq!(
            flush_calls_after_cleanup, flush_calls_before,
            "cleanup callbacks should not trigger foreground flush work"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn repeated_flush_callback_records_barrier_then_clean_skip() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("flush_callback_repeated");
        let attr = vfs
            .create(ROOT_INO, b"flush-callback-repeated.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);

        ctx.run_flush_callback(attr.ino, fh)
            .expect("first flush callback");
        let after_first_callbacks = winfsp_callbacks(&vfs);
        let after_first_barriers = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            after_first_callbacks.flush,
            callbacks_before.flush + 1,
            "first flush callback should be counted"
        );
        assert_eq!(
            after_first_barriers.flush,
            barriers_before.flush + 1,
            "first flush callback should attribute one foreground barrier to the flush path"
        );
        assert_eq!(
            after_first_barriers.flush_skipped_clean, barriers_before.flush_skipped_clean,
            "first dirty flush should not look like a clean skip"
        );

        ctx.run_flush_callback(attr.ino, fh)
            .expect("second flush callback");
        let after_second_callbacks = winfsp_callbacks(&vfs);
        let after_second_barriers = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            after_second_callbacks.flush,
            after_first_callbacks.flush + 1,
            "second flush callback should still be counted"
        );
        assert_eq!(
            after_second_barriers.flush, after_first_barriers.flush,
            "second flush callback should not add a barrier once the inode is already clean"
        );
        assert_eq!(
            after_second_barriers.flush_skipped_clean,
            after_first_barriers.flush_skipped_clean + 1,
            "second flush callback should be attributed as a clean skip"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn concurrent_flush_callbacks_coalesce_to_one_barrier_and_clean_skips() {
        const CONCURRENT_FLUSH_CALLBACKS: usize = 4;
        let (_tmp, vfs, ctx, _queue) = make_test_context("flush_callback_concurrent");
        let attr = vfs
            .create(ROOT_INO, b"flush-callback-concurrent.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_ordered_calls(&vfs);
        let ctx = Arc::new(ctx);
        // Keep the fanout intentionally small so constrained Windows runners
        // still exercise the coalescing path without oversubscribing threads.
        let start = Arc::new(Barrier::new(CONCURRENT_FLUSH_CALLBACKS));
        let mut joins = Vec::new();
        for _ in 0..CONCURRENT_FLUSH_CALLBACKS {
            let ctx = Arc::clone(&ctx);
            let start = Arc::clone(&start);
            let ino = attr.ino;
            joins.push(std::thread::spawn(move || {
                start.wait();
                ctx.run_flush_callback(ino, fh)
                    .expect("concurrent flush callback");
            }));
        }
        for join in joins {
            join.join().expect("flush thread join");
        }

        let callbacks_after = winfsp_callbacks(&vfs);
        let barriers_after = winfsp_foreground_flushes(&vfs);
        let flush_calls_after = flush_ordered_calls(&vfs);
        assert_eq!(
            callbacks_after.flush,
            callbacks_before.flush + CONCURRENT_FLUSH_CALLBACKS as u64,
            "all concurrent flush callbacks should still be observable"
        );
        assert_eq!(
            barriers_after.flush,
            barriers_before.flush + 1,
            "the per-inode gate should collapse concurrent dirty flush callbacks into one barrier"
        );
        assert_eq!(
            barriers_after.flush_skipped_clean,
            barriers_before.flush_skipped_clean + (CONCURRENT_FLUSH_CALLBACKS - 1) as u64,
            "duplicate concurrent flush callbacks should turn into clean skips once the gate owner drains the inode"
        );
        assert_eq!(
            flush_calls_after,
            flush_calls_before + 1,
            "concurrent explicit flush callbacks should trigger one ordered flush"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn explicit_flush_callback_clears_deferred_cleanup_hint() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_hint_cleared_by_flush");
        let attr = vfs
            .create(ROOT_INO, b"cleanup-hint-cleared-by-flush.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("cleanup callback");
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should leave a deferred-close hint before explicit flush"
        );

        ctx.run_flush_callback(attr.ino, fh)
            .expect("explicit flush callback");
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "explicit flush should clear the deferred cleanup hint"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn cleanup_then_flush_then_close_turns_final_close_into_clean_skip() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_flush_close_clean_skip");
        let attr = vfs
            .create(ROOT_INO, b"cleanup-flush-close-clean-skip.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_ordered_calls(&vfs);

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("cleanup callback");
        let callbacks_after_cleanup = winfsp_callbacks(&vfs);
        let barriers_after_cleanup = winfsp_foreground_flushes(&vfs);
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should leave a deferred-close hint before the explicit flush"
        );
        assert_eq!(
            callbacks_after_cleanup.cleanup,
            callbacks_before.cleanup + 1,
            "cleanup should be observable in the callback counters"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup, barriers_before.cleanup,
            "cleanup should not claim a foreground barrier before the explicit flush"
        );

        ctx.run_flush_callback(attr.ino, fh)
            .expect("explicit flush callback");
        let callbacks_after_flush = winfsp_callbacks(&vfs);
        let barriers_after_flush = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_flush = flush_ordered_calls(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "explicit flush should clear the deferred cleanup hint before close"
        );
        assert_eq!(
            callbacks_after_flush.flush,
            callbacks_before.flush + 1,
            "explicit flush should be counted as a WinFSP flush callback"
        );
        assert_eq!(
            barriers_after_flush.flush,
            barriers_before.flush + 1,
            "explicit flush should own one foreground barrier"
        );
        assert_eq!(
            flush_calls_after_flush,
            flush_calls_before + 1,
            "explicit flush should drain the dirty inode exactly once"
        );

        ctx.run_close_callback(attr.ino, fh);
        let callbacks_after_close = winfsp_callbacks(&vfs);
        let barriers_after_close = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_close = flush_ordered_calls(&vfs);
        assert_eq!(
            callbacks_after_close.close,
            callbacks_before.close + 1,
            "close should still be observed after cleanup and explicit flush"
        );
        assert_eq!(
            barriers_after_close.close, barriers_before.close,
            "final close should not claim a fallback barrier once explicit flush drained the inode"
        );
        assert_eq!(
            barriers_after_close.close_skipped_clean,
            barriers_before.close_skipped_clean + 1,
            "final close should be attributed as a clean skip after the explicit flush"
        );
        assert_eq!(
            flush_calls_after_close, flush_calls_after_flush,
            "final close should not trigger another ordered flush after the explicit flush"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "close should still release the last handle after the explicit flush path"
        );
    }

    #[test]
    fn nonfinal_close_preserves_deferred_cleanup_hint_until_last_handle() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_nonfinal_close_then_final");
        let attr = vfs
            .create(
                ROOT_INO,
                b"cleanup-nonfinal-close-then-final.txt",
                0,
                0,
                0o644,
            )
            .expect("create");
        let fh1 = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open first handle");
        let fh2 = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open second handle");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_ordered_calls(&vfs);

        ctx.run_cleanup_non_delete_callback(attr.ino, fh1)
            .expect("cleanup callback");
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should defer the barrier until the final handle closes"
        );

        ctx.run_close_callback(attr.ino, fh1);
        let callbacks_after_first_close = winfsp_callbacks(&vfs);
        let barriers_after_first_close = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_first_close = flush_ordered_calls(&vfs);
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "closing a non-final handle must preserve the deferred cleanup hint"
        );
        assert_eq!(
            callbacks_after_first_close.close,
            callbacks_before.close + 1,
            "non-final close should still be counted"
        );
        assert_eq!(
            barriers_after_first_close.close, barriers_before.close,
            "non-final close must not claim the last-handle fallback barrier"
        );
        assert_eq!(
            barriers_after_first_close.close_skipped_clean, barriers_before.close_skipped_clean,
            "non-final close should not look like a clean last-handle release"
        );
        assert_eq!(
            flush_calls_after_first_close, flush_calls_before,
            "non-final close must not drain dirty local state"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            1,
            "one writable handle should remain open after the non-final close"
        );

        ctx.run_close_callback(attr.ino, fh2);
        let callbacks_after_second_close = winfsp_callbacks(&vfs);
        let barriers_after_second_close = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_second_close = flush_ordered_calls(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "the final close should consume the deferred cleanup hint"
        );
        assert_eq!(
            callbacks_after_second_close.close,
            callbacks_before.close + 2,
            "both close callbacks should be observable"
        );
        assert_eq!(
            barriers_after_second_close.close,
            barriers_before.close + 1,
            "the final close should own the deferred cleanup barrier"
        );
        assert_eq!(
            barriers_after_second_close.close_skipped_clean, barriers_before.close_skipped_clean,
            "dirty final close should not be classified as a clean skip"
        );
        assert_eq!(
            flush_calls_after_second_close,
            flush_calls_before + 1,
            "the final close should drain dirty local state exactly once"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "all handles should be closed after the final close"
        );
    }

    #[test]
    fn cleanup_non_delete_with_fh_zero_does_not_record_deferred_close_hint() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_hint_skips_fh_zero");
        let attr = vfs
            .mkdir(ROOT_INO, b"cleanup-hint-dir", 0, 0, 0o755)
            .expect("mkdir");

        vfs.setattr(
            attr.ino,
            SetattrArgs {
                mtime: TimeSpec::Now,
                ..Default::default()
            },
        )
        .expect("setattr");

        ctx.run_cleanup_non_delete_callback(attr.ino, 0)
            .expect("cleanup callback");
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "fh==0 cleanup should not leave a deferred-close hint"
        );

        ctx.run_close_callback(attr.ino, 0);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "fh==0 close should not retain deferred-cleanup state"
        );
    }

    #[test]
    fn repeated_cleanup_callback_records_callbacks_without_foreground_barriers() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_callback_repeated");
        let attr = vfs
            .create(ROOT_INO, b"cleanup-callback-repeated.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("first cleanup callback");
        let after_first_callbacks = winfsp_callbacks(&vfs);
        let after_first_barriers = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            after_first_callbacks.cleanup,
            callbacks_before.cleanup + 1,
            "first cleanup callback should be counted"
        );
        assert_eq!(
            after_first_barriers.cleanup, barriers_before.cleanup,
            "cleanup callback should no longer attribute a foreground barrier"
        );
        assert_eq!(
            after_first_barriers.cleanup_skipped_clean, barriers_before.cleanup_skipped_clean,
            "cleanup callback should not be recorded as a clean-skip barrier either"
        );

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("second cleanup callback");
        let after_second_callbacks = winfsp_callbacks(&vfs);
        let after_second_barriers = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            after_second_callbacks.cleanup,
            after_first_callbacks.cleanup + 1,
            "second cleanup callback should still be counted"
        );
        assert_eq!(
            after_second_barriers.cleanup, after_first_barriers.cleanup,
            "repeated cleanup callbacks should not add foreground barriers"
        );
        assert_eq!(
            after_second_barriers.cleanup_skipped_clean, after_first_barriers.cleanup_skipped_clean,
            "repeated cleanup callbacks should not create clean-skip attribution"
        );

        let flush_calls_before_close = flush_ordered_calls(&vfs);
        ctx.run_close_callback(attr.ino, fh);
        let after_close_barriers = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_close = flush_ordered_calls(&vfs);
        assert_eq!(
            after_close_barriers.close,
            barriers_before.close + 1,
            "close should own the fallback barrier after repeated cleanup callbacks"
        );
        assert_eq!(
            flush_calls_after_close,
            flush_calls_before_close + 1,
            "close fallback should still drain the dirty inode once"
        );
    }

    #[test]
    fn dirty_handle_lifecycle_defers_barrier_from_cleanup_to_close() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("dirty_handle_cleanup_close");
        let attr = vfs
            .create(ROOT_INO, b"dirty-handle-close.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("cleanup callback");
        let after_cleanup_callbacks = winfsp_callbacks(&vfs);
        let after_cleanup_barriers = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            after_cleanup_callbacks.cleanup,
            callbacks_before.cleanup + 1,
            "cleanup should be counted in the dirty-handle lifecycle"
        );
        assert_eq!(
            after_cleanup_barriers.cleanup, barriers_before.cleanup,
            "dirty-handle lifecycle should not attribute a foreground barrier to cleanup"
        );

        ctx.run_close_callback(attr.ino, fh);
        let after_close_callbacks = winfsp_callbacks(&vfs);
        let after_close_barriers = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            after_close_callbacks.close,
            callbacks_before.close + 1,
            "close should be counted after cleanup"
        );
        assert_eq!(
            after_close_barriers.close,
            barriers_before.close + 1,
            "close should own the deferred dirty-handle barrier after cleanup returns"
        );
        assert_eq!(
            after_close_barriers.close_skipped_clean, barriers_before.close_skipped_clean,
            "dirty last-handle close should not be attributed as a clean release"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "close should release the handle"
        );
    }

    #[test]
    fn dirty_last_handle_close_falls_back_to_release_barrier_when_cleanup_did_not_run() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("dirty_last_handle_close_fallback");
        let attr = vfs
            .create(ROOT_INO, b"dirty-close-fallback.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_ordered_calls(&vfs);

        ctx.run_close_callback(attr.ino, fh);

        let callbacks_after = winfsp_callbacks(&vfs);
        let barriers_after = winfsp_foreground_flushes(&vfs);
        let flush_calls_after = flush_ordered_calls(&vfs);
        assert_eq!(
            callbacks_after.close,
            callbacks_before.close + 1,
            "close should still be counted when it owns the last-handle fallback path"
        );
        assert_eq!(
            barriers_after.close,
            barriers_before.close + 1,
            "close should attribute the fallback last-handle barrier when cleanup did not run"
        );
        assert_eq!(
            barriers_after.close_skipped_clean, barriers_before.close_skipped_clean,
            "dirty last-handle close should not look like a clean release"
        );
        assert_eq!(
            flush_calls_after,
            flush_calls_before + 1,
            "release_fh should still drain dirty local state before the last handle disappears"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "close should release the last handle even when it performs the fallback barrier"
        );
    }

    #[test]
    fn duplicate_close_after_deferred_cleanup_is_a_clean_noop() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_close_duplicate_close");
        let attr = vfs
            .create(ROOT_INO, b"cleanup-close-duplicate-close.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        ctx.run_cleanup_non_delete_callback(attr.ino, fh)
            .expect("cleanup callback");
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should leave a deferred-close hint before the last-handle close"
        );

        let flush_calls_before = flush_ordered_calls(&vfs);
        let close_barriers_before = winfsp_foreground_flushes(&vfs);

        ctx.run_close_callback(attr.ino, fh);

        let flush_calls_after_first_close = flush_ordered_calls(&vfs);
        let close_barriers_after_first_close = winfsp_foreground_flushes(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "the first close should consume the deferred-cleanup hint"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "the first close should release the last handle"
        );
        assert_eq!(
            flush_calls_after_first_close,
            flush_calls_before + 1,
            "the first close should still drain local dirty state exactly once"
        );
        assert_eq!(
            close_barriers_after_first_close.close,
            close_barriers_before.close + 1,
            "the first close should own the deferred cleanup barrier"
        );

        ctx.run_close_callback(attr.ino, fh);

        let flush_calls_after_second_close = flush_ordered_calls(&vfs);
        let close_barriers_after_second_close = winfsp_foreground_flushes(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "duplicate close must not resurrect deferred-cleanup state"
        );
        assert_eq!(
            flush_calls_after_second_close, flush_calls_after_first_close,
            "duplicate close should not trigger another ordered flush"
        );
        assert_eq!(
            close_barriers_after_second_close.close, close_barriers_after_first_close.close,
            "duplicate close should not claim another last-handle barrier"
        );
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
