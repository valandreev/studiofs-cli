use std::ffi::CString;
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::raw::{c_char, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver},
};

use libc::iovec;
use parking_lot::Mutex;
use studiofs_meta::{DirEntry, FileType, SetattrArgs, TimeSpec};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::adapter::{CallbackState, StudioVfsAdapter};
use crate::{MacFuseMountRequest, low_level};

const FUSE_KERNEL_VERSION: u32 = 7;
const FUSE_KERNEL_MINOR_VERSION: u32 = 19;
const FUSE_ASYNC_READ: u32 = 1 << 0;
const FUSE_BIG_WRITES: u32 = 1 << 5;
const FOPEN_DIRECT_IO: u32 = 1 << 0;

const FUSE_LOOKUP: u32 = 1;
const FUSE_FORGET: u32 = 2;
const FUSE_GETATTR: u32 = 3;
const FUSE_SETATTR: u32 = 4;
const FUSE_READLINK: u32 = 5;
const FUSE_SYMLINK: u32 = 6;
const FUSE_MKNOD: u32 = 8;
const FUSE_MKDIR: u32 = 9;
const FUSE_UNLINK: u32 = 10;
const FUSE_RMDIR: u32 = 11;
const FUSE_RENAME: u32 = 12;
const FUSE_LINK: u32 = 13;
const FUSE_OPEN: u32 = 14;
const FUSE_READ: u32 = 15;
const FUSE_WRITE: u32 = 16;
const FUSE_STATFS: u32 = 17;
const FUSE_RELEASE: u32 = 18;
const FUSE_FSYNC: u32 = 20;
const FUSE_SETXATTR: u32 = 21;
const FUSE_GETXATTR: u32 = 22;
const FUSE_LISTXATTR: u32 = 23;
const FUSE_REMOVEXATTR: u32 = 24;
const FUSE_FLUSH: u32 = 25;
const FUSE_INIT: u32 = 26;
const FUSE_OPENDIR: u32 = 27;
const FUSE_READDIR: u32 = 28;
const FUSE_RELEASEDIR: u32 = 29;
const FUSE_FSYNCDIR: u32 = 30;
const FUSE_ACCESS: u32 = 34;
const FUSE_CREATE: u32 = 35;
const FUSE_INTERRUPT: u32 = 36;
const FUSE_DESTROY: u32 = 38;

const FUSE_NOTIFY_INVAL_INODE: i32 = 2;
const FUSE_NOTIFY_INVAL_ENTRY: i32 = 3;

const MF_MOUNT_RESULT_UNSUPPORTED_OS_VERSION: i32 = 1;
const MF_MOUNT_RESULT_HELPER_TOOLS_FAILED: i32 = 2;
const MF_MOUNT_RESULT_EXTENSION_NOT_FOUND: i32 = 3;
const MF_MOUNT_RESULT_APPROVAL_REQUIRED: i32 = 4;

const FATTR_MODE: u32 = 1 << 0;
const FATTR_UID: u32 = 1 << 1;
const FATTR_GID: u32 = 1 << 2;
const FATTR_SIZE: u32 = 1 << 3;
const FATTR_ATIME: u32 = 1 << 4;
const FATTR_MTIME: u32 = 1 << 5;
const FATTR_FH: u32 = 1 << 6;
const FATTR_ATIME_NOW: u32 = 1 << 7;
const FATTR_MTIME_NOW: u32 = 1 << 8;
const FATTR_LOCKOWNER: u32 = 1 << 9;
const FATTR_BKUPTIME: u32 = 1 << 30;
const FATTR_CRTIME: u32 = 1 << 28;
const FATTR_CHGTIME: u32 = 1 << 29;
const FATTR_FLAGS: u32 = 1 << 31;
const FATTR_SUPPORTED: u32 = FATTR_MODE
    | FATTR_UID
    | FATTR_GID
    | FATTR_SIZE
    | FATTR_ATIME
    | FATTR_MTIME
    | FATTR_FH
    | FATTR_ATIME_NOW
    | FATTR_MTIME_NOW
    | FATTR_LOCKOWNER
    | FATTR_BKUPTIME
    | FATTR_CHGTIME
    | FATTR_CRTIME
    | FATTR_FLAGS;

#[link(name = "MFMount", kind = "framework")]
unsafe extern "C" {
    fn MFRetain(reference: *mut c_void) -> *mut c_void;
    fn MFRelease(reference: *mut c_void);
    fn MFChannelCreate() -> *mut c_void;
    fn MFChannelInterrupt(channel: *mut c_void) -> bool;
    fn MFChannelCopyNextMessage(channel: *mut c_void) -> *mut c_void;
    fn MFChannelSendMessage(channel: *mut c_void, buffers: *const iovec, count: usize) -> isize;
    fn MFChannelClose(channel: *mut c_void) -> bool;
    fn MFMessageGetBodySize(message: *mut c_void) -> isize;
    fn MFMessageGetBodyBuffers(message: *mut c_void, buffers: *mut *const iovec) -> isize;
    fn MFMessageGetReplyBuffer(message: *mut c_void, buffer: *mut *mut c_void) -> isize;
    fn MFMount(
        channel: *mut c_void,
        mount_point: *const c_char,
        options: *const c_char,
        quiet: bool,
    ) -> i32;
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseInHeader {
    len: u32,
    opcode: u32,
    unique: u64,
    nodeid: u64,
    uid: u32,
    gid: u32,
    pid: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseOutHeader {
    len: u32,
    error: i32,
    unique: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseNotifyInvalInodeOut {
    ino: u64,
    off: i64,
    len: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseNotifyInvalEntryOut {
    parent: u64,
    namelen: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseAttr {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: u64,
    mtime: u64,
    ctime: u64,
    crtime: u64,
    atimensec: u32,
    mtimensec: u32,
    ctimensec: u32,
    crtimensec: u32,
    mode: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    flags: u32,
    blksize: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseEntryOut {
    nodeid: u64,
    generation: u64,
    entry_valid: u64,
    attr_valid: u64,
    entry_valid_nsec: u32,
    attr_valid_nsec: u32,
    attr: FuseAttr,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseAttrOut {
    attr_valid: u64,
    attr_valid_nsec: u32,
    dummy: u32,
    attr: FuseAttr,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseInitIn {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseInitOut {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: u32,
    max_background: u16,
    congestion_threshold: u16,
    max_write: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseForgetIn {
    nlookup: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseOpenIn {
    flags: u32,
    unused: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseAccessIn {
    mask: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseOpenOut {
    fh: u64,
    open_flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseReadIn {
    fh: u64,
    offset: u64,
    size: u32,
    read_flags: u32,
    lock_owner: u64,
    flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseWriteIn {
    fh: u64,
    offset: u64,
    size: u32,
    write_flags: u32,
    lock_owner: u64,
    flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseWriteOut {
    size: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseReleaseIn {
    fh: u64,
    flags: u32,
    release_flags: u32,
    lock_owner: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseFlushIn {
    fh: u64,
    unused: u32,
    padding: u32,
    lock_owner: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseFsyncIn {
    fh: u64,
    fsync_flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseMkdirIn {
    mode: u32,
    umask: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseMknodIn {
    mode: u32,
    rdev: u32,
    umask: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseRenameIn {
    newdir: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseLinkIn {
    oldnodeid: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseCreateIn {
    flags: u32,
    mode: u32,
    umask: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseSetattrIn {
    valid: u32,
    padding: u32,
    fh: u64,
    size: u64,
    lock_owner: u64,
    atime: u64,
    mtime: u64,
    unused2: u64,
    atimensec: u32,
    mtimensec: u32,
    unused3: u32,
    mode: u32,
    unused4: u32,
    uid: u32,
    gid: u32,
    unused5: u32,
    bkuptime: u64,
    chgtime: u64,
    crtime: u64,
    bkuptimensec: u32,
    chgtimensec: u32,
    crtimensec: u32,
    flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseGetxattrIn {
    size: u32,
    padding: u32,
    position: u32,
    padding2: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseSetxattrIn {
    size: u32,
    flags: u32,
    position: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseXattrOut {
    size: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseReplyBufOut {
    size: u32,
    padding: u32,
}

#[derive(Debug)]
struct MessageBody {
    header: FuseInHeader,
    bytes: Vec<u8>,
}

fn parse_message_body(bytes: Vec<u8>) -> Result<MessageBody, (i32, Vec<u8>)> {
    let header: FuseInHeader = match read_copy(&bytes, 0) {
        Ok(header) => header,
        Err(errno) => return Err((errno, bytes)),
    };
    if usize::try_from(header.len).ok() != Some(bytes.len())
        || header.len < size_of::<FuseInHeader>() as u32
    {
        return Err((libc::EINVAL, bytes));
    }
    Ok(MessageBody { header, bytes })
}

fn read_copy<T: Copy>(bytes: &[u8], offset: usize) -> Result<T, i32> {
    let end = offset.checked_add(size_of::<T>()).ok_or(libc::EOVERFLOW)?;
    if end > bytes.len() {
        return Err(libc::EINVAL);
    }
    // SAFETY: The range was bounds-checked. `read_unaligned` accepts the byte
    // alignment of a serialized FUSE body, and `T: Copy` has no drop state.
    Ok(unsafe { ptr::read_unaligned(bytes.as_ptr().add(offset).cast::<T>()) })
}

fn encode_copy<T: Copy>(value: T) -> Vec<u8> {
    let mut storage = MaybeUninit::<T>::zeroed();
    storage.write(value);
    // SAFETY: `write` initialized the value and zeroed padding remains valid
    // serialized bytes for these repr(C) protocol structures.
    unsafe { slice::from_raw_parts(storage.as_ptr().cast::<u8>(), size_of::<T>()).to_vec() }
}

fn parse_c_string(bytes: &[u8], offset: usize) -> Result<&[u8], i32> {
    let bytes = bytes.get(offset..).ok_or(libc::EINVAL)?;
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(libc::EINVAL)?;
    if end == 0 {
        return Err(libc::EINVAL);
    }
    Ok(&bytes[..end])
}

fn parse_two_c_strings(bytes: &[u8], offset: usize) -> Result<(&[u8], &[u8]), i32> {
    let first = parse_c_string(bytes, offset)?;
    let second_offset = offset
        .checked_add(first.len())
        .and_then(|value| value.checked_add(1))
        .ok_or(libc::EOVERFLOW)?;
    let second = parse_c_string(bytes, second_offset)?;
    Ok((first, second))
}

fn checked_slice(bytes: &[u8], offset: usize, size: usize) -> Result<&[u8], i32> {
    let end = offset.checked_add(size).ok_or(libc::EINVAL)?;
    bytes.get(offset..end).ok_or(libc::EINVAL)
}

fn errno_error(errno: i32) -> io::Error {
    io::Error::from_raw_os_error(if errno == 0 { libc::EIO } else { errno.abs() })
}

#[derive(Debug)]
struct FsKitMessage {
    raw: NonNull<c_void>,
}

#[derive(Debug)]
struct FsKitChannelRef(NonNull<c_void>);

// The framework's channel reference is opaque, and this retained reference is
// only moved to the dedicated MFMount worker or used for one FFI operation at a
// time.
unsafe impl Send for FsKitChannelRef {}

impl FsKitChannelRef {
    fn retain(channel: NonNull<c_void>) -> Self {
        // SAFETY: the control state owns a live channel reference.
        unsafe {
            let _ = MFRetain(channel.as_ptr());
        }
        Self(channel)
    }

    fn as_ptr(&self) -> *mut c_void {
        self.0.as_ptr()
    }
}

impl Drop for FsKitChannelRef {
    fn drop(&mut self) {
        // SAFETY: retain created this balanced reference.
        unsafe { MFRelease(self.0.as_ptr()) };
    }
}

// MFMessageRef is a retained, opaque reference. The bridge API keeps its body
// and optional reply buffer alive until MFRelease; this wrapper moves the sole
// owner to a runtime task and never shares mutable message state.
unsafe impl Send for FsKitMessage {}

impl Drop for FsKitMessage {
    fn drop(&mut self) {
        // SAFETY: `raw` came from MFChannelCopyNextMessage and this wrapper owns
        // exactly one retained reference until Drop.
        unsafe { MFRelease(self.raw.as_ptr()) };
    }
}

impl FsKitMessage {
    fn receive(channel: &FsKitChannelRef) -> Option<Self> {
        // SAFETY: the retained channel reference remains valid for the call.
        let raw = unsafe { MFChannelCopyNextMessage(channel.as_ptr()) };
        NonNull::new(raw).map(|raw| Self { raw })
    }

    fn body(&self) -> Result<Vec<u8>, i32> {
        // SAFETY: the message is retained by self for the complete body copy.
        let body_size = unsafe { MFMessageGetBodySize(self.raw.as_ptr()) };
        let body_size = usize::try_from(body_size).map_err(|_| last_errno())?;
        if body_size < size_of::<FuseInHeader>() {
            return Err(libc::EINVAL);
        }
        let mut buffers = ptr::null();
        // SAFETY: the output pointer is valid and the message is retained.
        let count = unsafe { MFMessageGetBodyBuffers(self.raw.as_ptr(), &mut buffers) };
        let count = usize::try_from(count).map_err(|_| last_errno())?;
        if count == 0 || buffers.is_null() {
            return Err(libc::EINVAL);
        }
        // SAFETY: MFMount returns a borrowed iovec array valid while self lives.
        let buffers = unsafe { slice::from_raw_parts(buffers, count) };
        let total = buffers.iter().try_fold(0usize, |total, buffer| {
            total.checked_add(buffer.iov_len).ok_or(libc::EOVERFLOW)
        })?;
        if total != body_size {
            return Err(libc::EINVAL);
        }
        let mut body = Vec::with_capacity(body_size);
        for buffer in buffers {
            if buffer.iov_len == 0 {
                continue;
            }
            if buffer.iov_base.is_null() {
                return Err(libc::EINVAL);
            }
            // SAFETY: each iovec is borrowed from the retained MFMessageRef and
            // its declared range is owned by the framework for this scope.
            let bytes =
                unsafe { slice::from_raw_parts(buffer.iov_base.cast::<u8>(), buffer.iov_len) };
            body.extend_from_slice(bytes);
        }
        Ok(body)
    }

    fn reply_buffer(&self) -> Result<(*mut c_void, usize), i32> {
        let mut buffer = ptr::null_mut();
        // SAFETY: the message is retained and the output pointer is valid.
        let capacity = unsafe { MFMessageGetReplyBuffer(self.raw.as_ptr(), &mut buffer) };
        let capacity = usize::try_from(capacity).map_err(|_| last_errno())?;
        if capacity > 0 && buffer.is_null() {
            return Err(libc::EINVAL);
        }
        Ok((buffer, capacity))
    }
}

fn last_errno() -> i32 {
    normalize_errno(io::Error::last_os_error().raw_os_error())
}

fn normalize_errno(errno: Option<i32>) -> i32 {
    match errno {
        Some(0) | None => libc::EIO,
        Some(errno) => errno,
    }
}

fn setattr_flags_supported(valid: u32) -> bool {
    (valid & !FATTR_SUPPORTED) == 0
}

#[derive(Debug)]
struct FsKitControl {
    channel: Mutex<Option<NonNull<c_void>>>,
    send_lock: Mutex<()>,
    stop_requested: AtomicBool,
    mounted: AtomicBool,
}

// Channel lifetime is guarded by the channel mutex plus per-operation retained
// references. Sends and close are serialized; MFChannelInterrupt may run from
// the signal worker while the receive loop is blocked, as documented by
// MFMount.
unsafe impl Send for FsKitControl {}
unsafe impl Sync for FsKitControl {}

impl FsKitControl {
    fn new(channel: NonNull<c_void>) -> Arc<Self> {
        Arc::new(Self {
            channel: Mutex::new(Some(channel)),
            send_lock: Mutex::new(()),
            stop_requested: AtomicBool::new(false),
            mounted: AtomicBool::new(false),
        })
    }

    fn retained_channel(&self) -> Option<FsKitChannelRef> {
        let channel_guard = self.channel.lock();
        let channel = (*channel_guard)?;
        let retained = FsKitChannelRef::retain(channel);
        drop(channel_guard);
        Some(retained)
    }

    fn request_exit(&self) {
        self.request_exit_with(|| {
            let Some(channel) = self.retained_channel() else {
                return;
            };
            // SAFETY: the channel remains owned and open under the control state.
            let interrupted = unsafe { MFChannelInterrupt(channel.as_ptr()) };
            if !interrupted {
                let errno = last_errno();
                if !matches!(errno, libc::EINTR | libc::ENODEV | libc::EINVAL) {
                    warn!(errno, "macFUSE FSKit channel interrupt failed");
                }
            }
        });
    }

    fn request_exit_with(&self, interrupt: impl FnOnce()) {
        self.stop_requested.store(true, Ordering::Release);
        if !self.mounted.load(Ordering::Acquire) {
            self.close();
            return;
        }
        interrupt();
    }

    fn mark_mounted(&self) {
        self.mounted.store(true, Ordering::Release);
    }

    fn send(&self, buffers: &[iovec]) -> io::Result<()> {
        let _send_lock = self.send_lock.lock();
        let Some(channel) = self.retained_channel() else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "macFUSE FSKit channel is closed",
            ));
        };
        // SAFETY: all iovec storage is owned by the caller for this synchronous
        // call, and the channel is retained until send returns.
        let sent =
            unsafe { MFChannelSendMessage(channel.as_ptr(), buffers.as_ptr(), buffers.len()) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        let expected = buffers
            .iter()
            .try_fold(0usize, |total, buffer| total.checked_add(buffer.iov_len));
        if expected != Some(sent as usize) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "macFUSE FSKit channel sent a partial reply",
            ));
        }
        Ok(())
    }

    fn send_message(&self, message: FsKitMessage, buffers: &[iovec]) -> io::Result<()> {
        let result = self.send(buffers);
        drop(message);
        result
    }

    fn is_stopping(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }

    fn close(&self) {
        let _send_lock = self.send_lock.lock();
        let Some(channel) = self.channel.lock().take() else {
            return;
        };
        // SAFETY: this releases the control state's owned reference. Any
        // concurrent operation holds its own MFRetain reference until its FFI
        // call returns.
        unsafe {
            let _ = MFChannelClose(channel.as_ptr());
            MFRelease(channel.as_ptr());
        }
    }
}

struct FsKitSession {
    control: Arc<FsKitControl>,
    mount_result: Receiver<Result<(), i32>>,
    mount_thread: Option<std::thread::JoinHandle<()>>,
}

impl FsKitSession {
    fn mount(mount_point: &Path, volume_name: &str) -> io::Result<Self> {
        if !mount_point.starts_with(Path::new("/Volumes")) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "macFUSE FSKit mounts must be below /Volumes",
            ));
        }
        let mount_point = CString::new(mount_point.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount point contains NUL"))?;
        let options = mount_options(volume_name)?;
        // SAFETY: MFChannelCreate returns an owned channel or NULL.
        let channel = NonNull::new(unsafe { MFChannelCreate() }).ok_or_else(|| {
            io::Error::other(format!(
                "MFChannelCreate failed: {}",
                io::Error::last_os_error()
            ))
        })?;
        let control = FsKitControl::new(channel);
        let mount_channel = FsKitChannelRef::retain(channel);
        let (mount_sender, mount_result) = mpsc::channel();
        let mount_control = Arc::clone(&control);
        let mount_thread = std::thread::Builder::new()
            .name("macfuse-fskit-mount".into())
            .spawn(move || {
                // SAFETY: both C strings and the channel remain owned by this
                // worker/control state for the complete MFMount call.
                let result = unsafe {
                    MFMount(
                        mount_channel.as_ptr(),
                        mount_point.as_ptr(),
                        options.as_ptr(),
                        true,
                    )
                };
                if result == 0 {
                    mount_control.mark_mounted();
                }
                let _ = mount_sender.send(if result == 0 { Ok(()) } else { Err(result) });
                if result != 0 {
                    mount_control.request_exit();
                }
            })
            .map_err(|error| {
                control.close();
                io::Error::other(format!("failed to start FSKit mount worker: {error}"))
            })?;
        Ok(Self {
            control,
            mount_result,
            mount_thread: Some(mount_thread),
        })
    }

    fn run(&self, dispatcher: &FsKitDispatcher) -> io::Result<()> {
        let mut mounted = false;
        loop {
            match self.mount_result.try_recv() {
                Ok(Ok(())) => mounted = true,
                Ok(Err(result)) => return Err(mount_result_error(result)),
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) if !mounted => {
                    return Err(io::Error::other(
                        "FSKit mount worker exited without a result",
                    ));
                }
                Err(mpsc::TryRecvError::Disconnected) => {}
            }
            let Some(channel) = self.control.retained_channel() else {
                return Ok(());
            };
            let Some(message) = FsKitMessage::receive(&channel) else {
                match self.mount_result.try_recv() {
                    Ok(Ok(())) => {}
                    Ok(Err(result)) => return Err(mount_result_error(result)),
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) if !mounted => {
                        return Err(io::Error::other(
                            "FSKit mount worker exited without a result",
                        ));
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {}
                }
                let errno = last_errno();
                if self.control.is_stopping() || matches!(errno, libc::EINTR | libc::ENODEV) {
                    return Ok(());
                }
                return Err(errno_error(errno));
            };
            dispatcher.dispatch(message);
        }
    }
}

impl Drop for FsKitSession {
    fn drop(&mut self) {
        self.control.request_exit();
        self.control.close();
        if let Some(mount_thread) = self.mount_thread.take() {
            let _ = mount_thread.join();
        }
    }
}

fn mount_options(volume_name: &str) -> io::Result<CString> {
    if volume_name.contains(',') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "volume name cannot contain a comma for MFMount options",
        ));
    }
    CString::new(format!(
        "volname={volume_name},local,novncache,auto_cache,iosize={},max_read={},defer_permissions,sparse",
        low_level::MACOS_FUSE_IOSIZE_BYTES,
        low_level::MACOS_FUSE_IOSIZE_BYTES,
    ))
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "volume name contains NUL"))
}

fn mount_result_error(result: i32) -> io::Error {
    match result {
        MF_MOUNT_RESULT_UNSUPPORTED_OS_VERSION => io::Error::new(
            io::ErrorKind::Unsupported,
            "MFMount rejected FSKit: macOS version is unsupported",
        ),
        MF_MOUNT_RESULT_HELPER_TOOLS_FAILED => {
            io::Error::other("MFMount could not install the required helper tools")
        }
        MF_MOUNT_RESULT_EXTENSION_NOT_FOUND => io::Error::new(
            io::ErrorKind::NotFound,
            "MFMount could not find the macFUSE filesystem extension",
        ),
        MF_MOUNT_RESULT_APPROVAL_REQUIRED => io::Error::new(
            io::ErrorKind::PermissionDenied,
            "MFMount requires macFUSE filesystem-extension approval",
        ),
        _ => io::Error::other(format!(
            "MFMount failed with result {result}: {}",
            io::Error::last_os_error()
        )),
    }
}

fn opcode_allows_reply(opcode: u32) -> bool {
    !matches!(opcode, FUSE_FORGET | FUSE_DESTROY | FUSE_INTERRUPT)
}

struct FsKitRequest {
    message: Option<FsKitMessage>,
    body: MessageBody,
    sender: Arc<FsKitControl>,
}

impl FsKitRequest {
    fn new(message: FsKitMessage, body: MessageBody, sender: Arc<FsKitControl>) -> Self {
        Self {
            message: Some(message),
            body,
            sender,
        }
    }

    fn header(&self) -> FuseInHeader {
        self.body.header
    }

    fn operation<T: Copy>(&self, offset: usize) -> Result<T, i32> {
        read_copy(&self.body.bytes, offset)
    }

    fn take_message(&mut self) -> io::Result<FsKitMessage> {
        self.message
            .take()
            .ok_or_else(|| io::Error::other("FSKit request was already replied"))
    }

    fn reply_error(mut self, errno: i32) -> io::Result<()> {
        let message = self.take_message()?;
        if !opcode_allows_reply(self.header().opcode) {
            drop(message);
            return Ok(());
        }
        send_error(&self.sender, message, self.header().unique, errno)
    }

    fn reply_bytes(mut self, payload: Vec<u8>) -> io::Result<()> {
        let message = self.take_message()?;
        send_bytes(&self.sender, message, self.header().unique, &payload)
    }

    fn reply_read(mut self, payload: Vec<u8>, requested_size: u32) -> io::Result<()> {
        let message = self.take_message()?;
        send_read_bytes(
            &self.sender,
            message,
            self.header().unique,
            &payload,
            requested_size,
        )
    }

    fn reply_struct<T: Copy>(self, value: T) -> io::Result<()> {
        self.reply_bytes(encode_copy(value))
    }

    fn no_reply(mut self) {
        drop(self.message.take());
    }
}

impl Drop for FsKitRequest {
    fn drop(&mut self) {
        let Some(message) = self.message.take() else {
            return;
        };
        if !opcode_allows_reply(self.header().opcode) {
            return;
        }
        let _ = send_error(&self.sender, message, self.header().unique, libc::EIO);
    }
}

fn reply_header(unique: u64, errno: i32, payload_len: usize) -> Result<FuseOutHeader, io::Error> {
    let len = size_of::<FuseOutHeader>()
        .checked_add(payload_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "reply is too large"))?;
    Ok(FuseOutHeader {
        len: u32::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "reply is too large"))?,
        error: if errno == 0 { 0 } else { -errno.abs() },
        unique,
    })
}

fn send_error(
    sender: &FsKitControl,
    message: FsKitMessage,
    unique: u64,
    errno: i32,
) -> io::Result<()> {
    let header = reply_header(unique, errno, 0)?;
    let header = encode_copy(header);
    let buffers = [iovec {
        iov_base: header.as_ptr().cast_mut().cast(),
        iov_len: header.len(),
    }];
    sender.send_message(message, &buffers)
}

fn send_bytes(
    sender: &FsKitControl,
    message: FsKitMessage,
    unique: u64,
    payload: &[u8],
) -> io::Result<()> {
    let header = reply_header(unique, 0, payload.len())?;
    let header = encode_copy(header);
    if payload.is_empty() {
        let buffers = [iovec {
            iov_base: header.as_ptr().cast_mut().cast(),
            iov_len: header.len(),
        }];
        return sender.send_message(message, &buffers);
    }
    let buffers = [
        iovec {
            iov_base: header.as_ptr().cast_mut().cast(),
            iov_len: header.len(),
        },
        iovec {
            iov_base: payload.as_ptr().cast_mut().cast(),
            iov_len: payload.len(),
        },
    ];
    sender.send_message(message, &buffers)
}

fn send_read_bytes(
    sender: &FsKitControl,
    message: FsKitMessage,
    unique: u64,
    payload: &[u8],
    requested_size: u32,
) -> io::Result<()> {
    if !read_reply_uses_transport_buffer(payload, requested_size) {
        return send_bytes(sender, message, unique, payload);
    }
    let (reply_buffer, capacity) = match message.reply_buffer() {
        Ok(buffer) => buffer,
        Err(_) => return send_error(sender, message, unique, libc::EIO),
    };
    if capacity > 0 {
        if payload.len() > capacity || payload.len() > u32::MAX as usize {
            return send_error(sender, message, unique, libc::EIO);
        }
        // SAFETY: MFMessageGetReplyBuffer returned a valid borrowed range and
        // message remains retained until MFChannelSendMessage returns.
        unsafe {
            ptr::copy_nonoverlapping(payload.as_ptr(), reply_buffer.cast::<u8>(), payload.len());
        }
        let header = encode_copy(reply_header(unique, 0, size_of::<FuseReplyBufOut>())?);
        let reply_buffer_out = encode_copy(FuseReplyBufOut {
            size: payload.len() as u32,
            padding: 0,
        });
        let buffers = [
            iovec {
                iov_base: header.as_ptr().cast_mut().cast(),
                iov_len: header.len(),
            },
            iovec {
                iov_base: reply_buffer_out.as_ptr().cast_mut().cast(),
                iov_len: reply_buffer_out.len(),
            },
        ];
        return sender.send_message(message, &buffers);
    }
    send_bytes(sender, message, unique, payload)
}

fn read_reply_uses_transport_buffer(payload: &[u8], requested_size: u32) -> bool {
    !payload.is_empty() || requested_size == 0
}

type CancellationMap = Arc<Mutex<std::collections::HashMap<u64, Arc<CancellationToken>>>>;

struct CancellationGuard {
    cancellations: CancellationMap,
    unique: u64,
    token: Arc<CancellationToken>,
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        let mut cancellations = self.cancellations.lock();
        if cancellations
            .get(&self.unique)
            .is_some_and(|current| Arc::ptr_eq(current, &self.token))
        {
            cancellations.remove(&self.unique);
        }
    }
}

struct FsKitDispatcher {
    callback_state: Arc<CallbackState>,
    adapter: Arc<StudioVfsAdapter>,
    runtime_handle: Handle,
    control: Arc<FsKitControl>,
    cancellations: CancellationMap,
}

impl FsKitDispatcher {
    fn new(
        callback_state: Arc<CallbackState>,
        runtime_handle: Handle,
        control: Arc<FsKitControl>,
    ) -> Self {
        Self {
            adapter: callback_state.adapter(),
            callback_state,
            runtime_handle,
            control,
            cancellations: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn dispatch(&self, message: FsKitMessage) {
        let sender = Arc::clone(&self.control);
        let bytes = match message.body() {
            Ok(bytes) => bytes,
            Err(errno) => {
                warn!(errno, "macFUSE FSKit message body rejected");
                return;
            }
        };
        let body = match parse_message_body(bytes) {
            Ok(body) => body,
            Err((errno, bytes)) => {
                if let Ok(header) = read_copy::<FuseInHeader>(&bytes, 0) {
                    let request = FsKitRequest::new(message, MessageBody { header, bytes }, sender);
                    let _ = request.reply_error(errno);
                } else {
                    warn!(errno, "macFUSE FSKit message has no usable header");
                }
                return;
            }
        };
        self.dispatch_request(FsKitRequest::new(message, body, sender));
    }

    fn dispatch_request(&self, request: FsKitRequest) {
        match request.header().opcode {
            FUSE_INIT => self.init(request),
            FUSE_LOOKUP => self.lookup(request),
            FUSE_FORGET => self.forget(request),
            FUSE_GETATTR => self.getattr(request),
            FUSE_OPEN => self.open(request),
            FUSE_READ => self.read(request),
            FUSE_WRITE => self.write(request),
            FUSE_RELEASE => self.release(request),
            FUSE_FLUSH => self.flush(request),
            FUSE_FSYNC => self.fsync(request),
            FUSE_OPENDIR => self.opendir(request),
            FUSE_READDIR => self.readdir(request),
            FUSE_RELEASEDIR => self.releasedir(request),
            FUSE_FSYNCDIR => self.empty_success(request),
            FUSE_STATFS => self.statfs(request),
            FUSE_READLINK => self.readlink(request),
            FUSE_MKDIR => self.mkdir(request),
            FUSE_MKNOD => self.mknod(request),
            FUSE_UNLINK => self.unlink(request),
            FUSE_RMDIR => self.rmdir(request),
            FUSE_RENAME => self.rename(request),
            FUSE_LINK => self.link(request),
            FUSE_SYMLINK => self.symlink(request),
            FUSE_CREATE => self.create(request),
            FUSE_ACCESS => self.access(request),
            FUSE_SETXATTR => self.setxattr(request),
            FUSE_GETXATTR => self.getxattr(request),
            FUSE_LISTXATTR => self.listxattr(request),
            FUSE_REMOVEXATTR => self.removexattr(request),
            FUSE_INTERRUPT => self.interrupt(request),
            FUSE_DESTROY => self.destroy(request),
            FUSE_SETATTR => self.setattr(request),
            _ => self.unsupported(request, "opcode"),
        }
    }

    fn init(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseInitIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let reply = FuseInitOut {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION.min(input.minor),
            max_readahead: input.max_readahead,
            // SFS-669 deliberately uses owned message/reply buffers. The
            // Darwin direct-buffer flags belong to SFS-671's negotiation.
            flags: input.flags & (FUSE_ASYNC_READ | FUSE_BIG_WRITES),
            max_background: 0,
            congestion_threshold: 0,
            max_write: low_level::MACOS_FUSE_IOSIZE_BYTES,
        };
        let _ = request.reply_struct(reply);
    }

    fn lookup(&self, request: FsKitRequest) {
        let header = request.header();
        let name = match parse_c_string(&request.body.bytes, size_of::<FuseInHeader>()) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self.adapter.lookup(header.nodeid, &name) {
            Ok(attr) => {
                let _ = request
                    .reply_struct(entry_from_stat(attr.ino, self.adapter.attr_to_stat(&attr)));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn forget(&self, request: FsKitRequest) {
        let header = request.header();
        match request.operation::<FuseForgetIn>(size_of::<FuseInHeader>()) {
            Ok(input) => self.adapter.forget(header.nodeid, input.nlookup),
            Err(errno) => warn!(errno, "macFUSE FSKit FORGET body rejected"),
        }
        request.no_reply();
    }

    fn getattr(&self, request: FsKitRequest) {
        match self.adapter.getattr(request.header().nodeid) {
            Ok(stat) => {
                let _ = request.reply_struct(attr_out_from_stat(stat));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn open(&self, request: FsKitRequest) {
        let header = request.header();
        let input = match request.operation::<FuseOpenIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self.adapter.open(header.nodeid, input.flags as i32) {
            Ok(open) => {
                let file_handle = open.file_handle();
                // FSKit notifications are unavailable in this slice. Force
                // direct I/O and keep novncache as the explicit fallback.
                let result = request.reply_struct(FuseOpenOut {
                    fh: file_handle,
                    open_flags: FOPEN_DIRECT_IO,
                    padding: 0,
                });
                if result.is_err() {
                    self.adapter
                        .reject_prepared_open(header.nodeid, file_handle);
                }
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn read(&self, request: FsKitRequest) {
        let header = request.header();
        let input = match request.operation::<FuseReadIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let cancellation = Arc::new(CancellationToken::new());
        if let Some(previous) = self
            .cancellations
            .lock()
            .insert(header.unique, Arc::clone(&cancellation))
        {
            previous.cancel();
        }
        let adapter = Arc::clone(&self.adapter);
        let task_guard = self.callback_state.track_reply_task();
        let cancellation_guard = CancellationGuard {
            cancellations: Arc::clone(&self.cancellations),
            unique: header.unique,
            token: Arc::clone(&cancellation),
        };
        let requested_size = input.size;
        self.runtime_handle.spawn(async move {
            let _task_guard = task_guard;
            let _cancellation_guard = cancellation_guard;
            let result = adapter
                .read_for_fskit(
                    header.nodeid,
                    input.fh,
                    input.offset,
                    input.size,
                    cancellation.as_ref().clone(),
                )
                .await;
            let reply = match result {
                Ok(bytes) => request.reply_read(bytes, requested_size),
                Err(errno) => request.reply_error(errno),
            };
            if let Err(error) = reply {
                warn!(unique = header.unique, %error, "macFUSE FSKit READ reply failed");
            }
        });
    }

    fn write(&self, request: FsKitRequest) {
        let header = request.header();
        let input = match request.operation::<FuseWriteIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let payload_offset = size_of::<FuseInHeader>() + size_of::<FuseWriteIn>();
        let size = input.size as usize;
        let data = match checked_slice(&request.body.bytes, payload_offset, size) {
            Ok(data) => data.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let offset = match i64::try_from(input.offset) {
            Ok(offset) => offset,
            Err(_) => {
                let _ = request.reply_error(libc::EOVERFLOW);
                return;
            }
        };
        match self.adapter.write(header.nodeid, input.fh, offset, &data) {
            Ok(written) => {
                let _ = request.reply_struct(FuseWriteOut {
                    size: written,
                    padding: 0,
                });
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn release(&self, request: FsKitRequest) {
        let header = request.header();
        match request.operation::<FuseReleaseIn>(size_of::<FuseInHeader>()) {
            Ok(input) => match self.adapter.release(header.nodeid, input.fh) {
                Ok(()) => self.empty_success(request),
                Err(errno) => {
                    let _ = request.reply_error(errno);
                }
            },
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn flush(&self, request: FsKitRequest) {
        let header = request.header();
        match request.operation::<FuseFlushIn>(size_of::<FuseInHeader>()) {
            Ok(input) => match self.adapter.flush(header.nodeid, input.fh) {
                Ok(()) => self.empty_success(request),
                Err(errno) => {
                    let _ = request.reply_error(errno);
                }
            },
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn fsync(&self, request: FsKitRequest) {
        let header = request.header();
        match request.operation::<FuseFsyncIn>(size_of::<FuseInHeader>()) {
            Ok(input) => {
                match self
                    .adapter
                    .fsync(Some(input.fh), header.nodeid, input.fsync_flags != 0)
                {
                    Ok(()) => self.empty_success(request),
                    Err(errno) => {
                        let _ = request.reply_error(errno);
                    }
                }
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn opendir(&self, request: FsKitRequest) {
        match self.adapter.opendir(request.header().nodeid) {
            Ok(file_handle) => {
                if request
                    .reply_struct(FuseOpenOut {
                        fh: file_handle,
                        open_flags: 0,
                        padding: 0,
                    })
                    .is_err()
                {
                    let _ = self.adapter.releasedir(file_handle);
                }
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn readdir(&self, request: FsKitRequest) {
        let header = request.header();
        let input = match request.operation::<FuseReadIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let entries = match self.adapter.directory_entries_for_fskit(input.fh) {
            Ok(entries) => entries,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match encode_dirents(
            header.nodeid,
            entries.parent_ino,
            input.offset,
            input.size as usize,
            &entries.entries,
        ) {
            Ok(bytes) => {
                let _ = request.reply_bytes(bytes);
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn releasedir(&self, request: FsKitRequest) {
        match request.operation::<FuseReleaseIn>(size_of::<FuseInHeader>()) {
            Ok(input) => match self.adapter.releasedir(input.fh) {
                Ok(()) => self.empty_success(request),
                Err(errno) => {
                    let _ = request.reply_error(errno);
                }
            },
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn statfs(&self, request: FsKitRequest) {
        match self.adapter.statfs() {
            Ok(stats) => {
                let _ = request.reply_struct(statfs_out_from_statvfs(stats));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn readlink(&self, request: FsKitRequest) {
        match self.adapter.readlink(request.header().nodeid) {
            Ok(target) => {
                let _ = request.reply_bytes(target);
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn mkdir(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseMkdirIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let name = match parse_c_string(
            &request.body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseMkdirIn>(),
        ) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self
            .adapter
            .mkdir(request.header().nodeid, &name, input.mode as libc::mode_t)
        {
            Ok(attr) => {
                let _ = request
                    .reply_struct(entry_from_stat(attr.ino, self.adapter.attr_to_stat(&attr)));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn mknod(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseMknodIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let name = match parse_c_string(
            &request.body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseMknodIn>(),
        ) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self
            .adapter
            .mknod(request.header().nodeid, &name, input.mode as libc::mode_t)
        {
            Ok(attr) => {
                let _ = request
                    .reply_struct(entry_from_stat(attr.ino, self.adapter.attr_to_stat(&attr)));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn unlink(&self, request: FsKitRequest) {
        let name = match parse_c_string(&request.body.bytes, size_of::<FuseInHeader>()) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self.adapter.unlink(request.header().nodeid, &name) {
            Ok(()) => self.empty_success(request),
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn rmdir(&self, request: FsKitRequest) {
        let name = match parse_c_string(&request.body.bytes, size_of::<FuseInHeader>()) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self.adapter.rmdir(request.header().nodeid, &name) {
            Ok(()) => self.empty_success(request),
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn rename(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseRenameIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let (name, new_name) = match parse_two_c_strings(
            &request.body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseRenameIn>(),
        ) {
            Ok(names) => (names.0.to_vec(), names.1.to_vec()),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self
            .adapter
            .rename(request.header().nodeid, &name, input.newdir, &new_name)
        {
            Ok(()) => self.empty_success(request),
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn link(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseLinkIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let name = match parse_c_string(
            &request.body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseLinkIn>(),
        ) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self
            .adapter
            .link(input.oldnodeid, request.header().nodeid, &name)
        {
            Ok(attr) => {
                let _ = request
                    .reply_struct(entry_from_stat(attr.ino, self.adapter.attr_to_stat(&attr)));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn symlink(&self, request: FsKitRequest) {
        let (name, target) =
            match parse_two_c_strings(&request.body.bytes, size_of::<FuseInHeader>()) {
                Ok(names) => (names.0.to_vec(), names.1.to_vec()),
                Err(errno) => {
                    let _ = request.reply_error(errno);
                    return;
                }
            };
        match self
            .adapter
            .symlink(request.header().nodeid, &name, &target)
        {
            Ok(attr) => {
                let _ = request
                    .reply_struct(entry_from_stat(attr.ino, self.adapter.attr_to_stat(&attr)));
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn create(&self, request: FsKitRequest) {
        let header = request.header();
        let input = match request.operation::<FuseCreateIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let name = match parse_c_string(
            &request.body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseCreateIn>(),
        ) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self.adapter.create_for_fskit(
            header.nodeid,
            &name,
            input.mode as libc::mode_t,
            input.flags as i32,
        ) {
            Ok((attr, open)) => {
                let file_handle = open.file_handle();
                let entry =
                    encode_copy(entry_from_stat(attr.ino, self.adapter.attr_to_stat(&attr)));
                let open = encode_copy(FuseOpenOut {
                    fh: file_handle,
                    open_flags: FOPEN_DIRECT_IO,
                    padding: 0,
                });
                if request.reply_bytes([entry, open].concat()).is_err() {
                    self.adapter.reject_prepared_open(attr.ino, file_handle);
                }
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn access(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseAccessIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self
            .adapter
            .access(request.header().nodeid, input.mask as i32)
        {
            Ok(()) => self.empty_success(request),
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn setattr(&self, request: FsKitRequest) {
        let header = request.header();
        let input = match request.operation::<FuseSetattrIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        if !setattr_flags_supported(input.valid) {
            let _ = request.reply_error(libc::ENOTSUP);
            return;
        }
        let mut args = SetattrArgs::default();
        if input.valid & FATTR_SIZE != 0 {
            args.size = Some(input.size);
        }
        if input.valid & FATTR_MODE != 0 {
            args.mode = Some((input.mode & 0o7777) as u16);
        }
        if input.valid & FATTR_UID != 0 {
            args.uid = Some(input.uid);
        }
        if input.valid & FATTR_GID != 0 {
            args.gid = Some(input.gid);
        }
        args.atime = match setattr_time(
            input.valid,
            FATTR_ATIME,
            FATTR_ATIME_NOW,
            input.atime,
            input.atimensec,
        ) {
            Ok(time) => time,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        args.mtime = match setattr_time(
            input.valid,
            FATTR_MTIME,
            FATTR_MTIME_NOW,
            input.mtime,
            input.mtimensec,
        ) {
            Ok(time) => time,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        if input.valid & FATTR_CRTIME != 0 {
            args.crtime = match time_to_nanos(input.crtime, input.crtimensec) {
                Ok(nanos) => TimeSpec::Set(nanos),
                Err(errno) => {
                    let _ = request.reply_error(errno);
                    return;
                }
            };
        }
        if input.valid & (FATTR_BKUPTIME | FATTR_CHGTIME) != 0 {
            debug!(
                ino = header.nodeid,
                "FSKit setattr ignores Darwin backup/change time fields"
            );
        }
        let file_handle = (input.valid & FATTR_FH != 0).then_some(input.fh);
        let attr = if args_have_values(&args) {
            match self.adapter.setattr(file_handle, header.nodeid, args) {
                Ok(attr) => attr,
                Err(errno) => {
                    let _ = request.reply_error(errno);
                    return;
                }
            }
        } else {
            if input.valid & FATTR_FLAGS != 0 {
                self.adapter
                    .remember_fskit_file_flags(header.nodeid, input.flags);
            }
            match self.adapter.getattr(header.nodeid) {
                Ok(stat) => {
                    let _ = request.reply_struct(attr_out_from_stat(stat));
                    return;
                }
                Err(errno) => {
                    let _ = request.reply_error(errno);
                    return;
                }
            }
        };
        // FSKit file flags describe the successfully applied attribute state;
        // do not cache them when the requested setattr failed.
        if input.valid & FATTR_FLAGS != 0 {
            self.adapter
                .remember_fskit_file_flags(header.nodeid, input.flags);
        }
        let _ = request.reply_struct(attr_out_from_stat(self.adapter.attr_to_stat(&attr)));
    }

    fn setxattr(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseSetxattrIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        if input.position != 0 {
            let _ = request.reply_error(libc::ENOTSUP);
            return;
        }
        let name_offset = size_of::<FuseInHeader>() + size_of::<FuseSetxattrIn>();
        let name = match parse_c_string(&request.body.bytes, name_offset) {
            Ok(name) => name,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let value_offset = match name_offset
            .checked_add(name.len())
            .and_then(|offset| offset.checked_add(1))
        {
            Some(offset) => offset,
            None => {
                let _ = request.reply_error(libc::EINVAL);
                return;
            }
        };
        let value = match checked_slice(&request.body.bytes, value_offset, input.size as usize) {
            Ok(value) => value,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self
            .adapter
            .set_xattr(request.header().nodeid, name, value, input.flags)
        {
            Ok(()) => self.empty_success(request),
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn getxattr(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseGetxattrIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        if input.position != 0 {
            let _ = request.reply_error(libc::ENOTSUP);
            return;
        }
        let name = match parse_c_string(
            &request.body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseGetxattrIn>(),
        ) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let value = match self.adapter.get_xattr(request.header().nodeid, &name) {
            Ok(Some(value)) => value,
            Ok(None) => {
                let _ = request.reply_error(libc::ENODATA);
                return;
            }
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        if input.size == 0 {
            let _ = request.reply_struct(FuseXattrOut {
                size: value.len() as u32,
                padding: 0,
            });
        } else if value.len() > input.size as usize {
            let _ = request.reply_error(libc::ERANGE);
        } else {
            let _ = request.reply_bytes(value);
        }
    }

    fn listxattr(&self, request: FsKitRequest) {
        let input = match request.operation::<FuseGetxattrIn>(size_of::<FuseInHeader>()) {
            Ok(input) => input,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let names = match self.adapter.list_xattrs(request.header().nodeid) {
            Ok(names) => names,
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        let mut payload = Vec::new();
        for name in names {
            payload.extend_from_slice(&name);
            payload.push(0);
        }
        if input.size == 0 {
            let _ = request.reply_struct(FuseXattrOut {
                size: payload.len() as u32,
                padding: 0,
            });
        } else if payload.len() > input.size as usize {
            let _ = request.reply_error(libc::ERANGE);
        } else {
            let _ = request.reply_bytes(payload);
        }
    }

    fn removexattr(&self, request: FsKitRequest) {
        let name = match parse_c_string(&request.body.bytes, size_of::<FuseInHeader>()) {
            Ok(name) => name.to_vec(),
            Err(errno) => {
                let _ = request.reply_error(errno);
                return;
            }
        };
        match self.adapter.remove_xattr(request.header().nodeid, &name) {
            Ok(()) => self.empty_success(request),
            Err(errno) => {
                let _ = request.reply_error(errno);
            }
        }
    }

    fn interrupt(&self, request: FsKitRequest) {
        let target = match request.operation::<u64>(size_of::<FuseInHeader>()) {
            Ok(target) => target,
            Err(errno) => {
                warn!(errno, "macFUSE FSKit INTERRUPT body rejected");
                request.no_reply();
                return;
            }
        };
        if let Some(cancellation) = self.cancellations.lock().get(&target).cloned() {
            cancellation.cancel();
        }
        request.no_reply();
    }

    fn destroy(&self, request: FsKitRequest) {
        request.no_reply();
        self.control.request_exit();
    }

    fn empty_success(&self, request: FsKitRequest) {
        let _ = request.reply_bytes(Vec::new());
    }

    fn unsupported(&self, request: FsKitRequest, operation: &'static str) {
        debug!(
            operation,
            "macFUSE FSKit operation is outside the SFS-669 slice"
        );
        let _ = request.reply_error(libc::ENOSYS);
    }

    fn cancel_pending(&self) {
        for cancellation in self.cancellations.lock().values() {
            cancellation.cancel();
        }
    }
}

fn invalidation_inode_bytes(ino: u64, off: i64, len: i64) -> Vec<u8> {
    let payload = encode_copy(FuseNotifyInvalInodeOut { ino, off, len });
    let total = size_of::<FuseOutHeader>() + payload.len();
    let header = encode_copy(FuseOutHeader {
        len: total as u32,
        error: FUSE_NOTIFY_INVAL_INODE,
        unique: 0,
    });
    [header, payload].concat()
}

fn invalidation_entry_bytes(parent: u64, name: &[u8]) -> Result<Vec<u8>, i32> {
    let namelen = u32::try_from(name.len()).map_err(|_| libc::EOVERFLOW)?;
    let payload = encode_copy(FuseNotifyInvalEntryOut {
        parent,
        namelen,
        padding: 0,
    });
    let total = size_of::<FuseOutHeader>()
        .checked_add(payload.len())
        .and_then(|total| total.checked_add(name.len()))
        .ok_or(libc::EOVERFLOW)?;
    let header = encode_copy(FuseOutHeader {
        len: u32::try_from(total).map_err(|_| libc::EOVERFLOW)?,
        error: FUSE_NOTIFY_INVAL_ENTRY,
        unique: 0,
    });
    let mut frame = [header, payload].concat();
    frame.extend_from_slice(name);
    Ok(frame)
}

struct FsKitKernelInvalidator {
    control: Arc<FsKitControl>,
}

impl FsKitKernelInvalidator {
    fn send_frame(&self, frame: Vec<u8>) {
        let buffer = iovec {
            iov_base: frame.as_ptr().cast_mut().cast(),
            iov_len: frame.len(),
        };
        if let Err(error) = self.control.send(std::slice::from_ref(&buffer)) {
            debug!(?error, "FSKit kernel invalidation could not be sent");
        }
    }
}

impl studiofs_vfs::KernelInvalidator for FsKitKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.send_frame(invalidation_inode_bytes(ino, 0, 0));
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.send_frame(invalidation_inode_bytes(ino, -1, 0));
    }

    fn inval_entry(&self, parent: u64, name: &[u8]) {
        match invalidation_entry_bytes(parent, name) {
            Ok(frame) => self.send_frame(frame),
            Err(errno) => {
                debug!(parent, errno, "FSKit entry invalidation name is too large");
            }
        }
    }
}

fn attr_from_stat(stat: libc::stat) -> FuseAttr {
    let nonnegative = |value: i64| u64::try_from(value).unwrap_or(0);
    FuseAttr {
        ino: stat.st_ino,
        size: nonnegative(stat.st_size),
        blocks: nonnegative(stat.st_blocks),
        atime: nonnegative(stat.st_atime),
        mtime: nonnegative(stat.st_mtime),
        ctime: nonnegative(stat.st_ctime),
        crtime: nonnegative(stat.st_birthtime),
        atimensec: stat.st_atime_nsec as u32,
        mtimensec: stat.st_mtime_nsec as u32,
        ctimensec: stat.st_ctime_nsec as u32,
        crtimensec: stat.st_birthtime_nsec as u32,
        mode: stat.st_mode as u32,
        nlink: stat.st_nlink as u32,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        flags: stat.st_flags,
        blksize: stat.st_blksize as u32,
        padding: 0,
    }
}

fn setattr_time(
    valid: u32,
    value_bit: u32,
    now_bit: u32,
    seconds: u64,
    nanos: u32,
) -> Result<TimeSpec, i32> {
    if valid & now_bit != 0 {
        Ok(TimeSpec::Now)
    } else if valid & value_bit != 0 {
        Ok(TimeSpec::Set(time_to_nanos(seconds, nanos)?))
    } else {
        Ok(TimeSpec::Omit)
    }
}

fn time_to_nanos(seconds: u64, nanos: u32) -> Result<i64, i32> {
    let seconds = i64::try_from(seconds).map_err(|_| libc::EOVERFLOW)?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(i64::from(nanos)))
        .ok_or(libc::EOVERFLOW)
}

fn args_have_values(args: &SetattrArgs) -> bool {
    args.size.is_some()
        || args.mode.is_some()
        || args.uid.is_some()
        || args.gid.is_some()
        || args.atime.resolve().is_some()
        || args.mtime.resolve().is_some()
        || args.crtime.resolve().is_some()
        || args.win_attrs.is_some()
}

fn entry_from_stat(ino: u64, stat: libc::stat) -> FuseEntryOut {
    FuseEntryOut {
        nodeid: ino,
        generation: 0,
        entry_valid: 1,
        attr_valid: 1,
        entry_valid_nsec: 0,
        attr_valid_nsec: 0,
        attr: attr_from_stat(stat),
    }
}

fn attr_out_from_stat(stat: libc::stat) -> FuseAttrOut {
    FuseAttrOut {
        attr_valid: 1,
        attr_valid_nsec: 0,
        dummy: 0,
        attr: attr_from_stat(stat),
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseKstatfs {
    blocks: u64,
    bfree: u64,
    bavail: u64,
    files: u64,
    ffree: u64,
    bsize: u32,
    namelen: u32,
    frsize: u32,
    padding: u32,
    spare: [u32; 6],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct FuseStatfsOut {
    st: FuseKstatfs,
}

fn statfs_out_from_statvfs(stats: libc::statvfs) -> FuseStatfsOut {
    FuseStatfsOut {
        st: FuseKstatfs {
            blocks: stats.f_blocks as u64,
            bfree: stats.f_bfree as u64,
            bavail: stats.f_bavail as u64,
            files: stats.f_files as u64,
            ffree: stats.f_ffree as u64,
            bsize: stats.f_bsize as u32,
            namelen: u32::try_from(stats.f_namemax).unwrap_or(u32::MAX),
            frsize: stats.f_frsize as u32,
            padding: 0,
            spare: [0; 6],
        },
    }
}

fn encode_dirents(
    ino: u64,
    parent_ino: u64,
    offset: u64,
    size: usize,
    entries: &[DirEntry],
) -> Result<Vec<u8>, i32> {
    let mut output = Vec::with_capacity(size);
    let mut index = usize::try_from(offset).map_err(|_| libc::EOVERFLOW)?;
    let mut push = |entry_ino: u64, name: &[u8], kind: u32, next: usize| -> Result<bool, i32> {
        let base_len = size_of::<u64>() * 2 + size_of::<u32>() * 2 + name.len();
        let record_len = align8(base_len);
        if record_len > size.saturating_sub(output.len()) {
            return Ok(false);
        }
        let offset = u64::try_from(next).map_err(|_| libc::EOVERFLOW)?;
        output.extend_from_slice(&entry_ino.to_ne_bytes());
        output.extend_from_slice(&offset.to_ne_bytes());
        output.extend_from_slice(
            &u32::try_from(name.len())
                .map_err(|_| libc::EOVERFLOW)?
                .to_ne_bytes(),
        );
        output.extend_from_slice(&kind.to_ne_bytes());
        output.extend_from_slice(name);
        output.resize(output.len() + record_len - base_len, 0);
        Ok(true)
    };
    if index == 0 {
        if !push(ino, b".", libc::DT_DIR as u32, 1)? {
            return Ok(output);
        }
        index = 1;
    }
    if index == 1 {
        if !push(parent_ino, b"..", libc::DT_DIR as u32, 2)? {
            return Ok(output);
        }
        index = 2;
    }
    for (entry_index, entry) in entries.iter().enumerate().skip(index.saturating_sub(2)) {
        let next = entry_index + 3;
        let kind = match entry.kind {
            FileType::RegularFile => libc::DT_REG,
            FileType::Directory => libc::DT_DIR,
            FileType::Symlink => libc::DT_LNK,
        } as u32;
        if !push(entry.ino, &entry.name, kind, next)? {
            break;
        }
    }
    Ok(output)
}

fn align8(value: usize) -> usize {
    value.saturating_add(7) & !7
}

pub(crate) fn mount_with_request(request: MacFuseMountRequest) -> io::Result<()> {
    let MacFuseMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        // CLI daemonization is handled before the backend is invoked. Once
        // here, the FSKit session always runs synchronously until unmount.
        run_in_foreground: _,
        signal_handling,
        unmount_signal,
    } = request;
    crate::SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    let session = FsKitSession::mount(&mount_point, &volume_name)?;
    let callback_state = Arc::new(CallbackState::new(
        Arc::clone(&vfs),
        runtime_handle.clone(),
        read_only,
    ));
    let dispatcher = FsKitDispatcher::new(
        Arc::clone(&callback_state),
        runtime_handle,
        Arc::clone(&session.control),
    );
    let control = Arc::clone(&session.control);
    let signal_watcher = crate::spawn_signal_watcher(
        move || {
            control.request_exit();
        },
        signal_handling,
        unmount_signal,
    )?;
    vfs.set_kernel_invalidator(callback_state.kernel_invalidator(Arc::new(
        FsKitKernelInvalidator {
            control: Arc::clone(&session.control),
        },
    )));
    let result = session.run(&dispatcher);
    drop(signal_watcher);
    dispatcher.cancel_pending();
    vfs.clear_kernel_invalidator();
    callback_state.wait_for_reply_tasks();
    drop(dispatcher);
    session.control.close();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_vfs::KernelInvalidator;

    #[test]
    fn message_parser_rejects_a_body_length_that_does_not_match_the_header() {
        let mut body = vec![0_u8; size_of::<FuseInHeader>()];
        body[..4].copy_from_slice(&39_u32.to_ne_bytes());

        assert!(parse_message_body(body).is_err());
    }

    #[test]
    fn symlink_payload_order_is_name_then_target() {
        let mut body = vec![0_u8; size_of::<FuseInHeader>()];
        body.extend_from_slice(b"link\0target\0");

        let (name, target) =
            parse_two_c_strings(&body, size_of::<FuseInHeader>()).expect("parse symlink payload");
        assert_eq!(name, b"link");
        assert_eq!(target, b"target");
    }

    #[test]
    fn checked_slice_rejects_overflow_and_out_of_bounds() {
        let bytes = [0_u8, 1, 2, 3];
        let payload = checked_slice(&bytes, 1, 2).expect("valid payload");

        assert_eq!(payload, &[1, 2]);
        assert_eq!(checked_slice(&bytes, usize::MAX, 1), Err(libc::EINVAL));
        assert_eq!(checked_slice(&bytes, 3, 2), Err(libc::EINVAL));
    }

    #[test]
    fn request_exit_does_not_wait_for_send_lock() {
        // SAFETY: this test-only channel is never passed to an FFI call.
        let control = FsKitControl::new(NonNull::dangling());
        control.mark_mounted();
        let send_guard = control.send_lock.lock();
        let (done_sender, done_receiver) = mpsc::channel();
        let worker_control = Arc::clone(&control);
        let worker = std::thread::spawn(move || {
            worker_control.request_exit_with(|| {
                done_sender
                    .send(())
                    .expect("report request exit completion");
            });
        });

        let completed = done_receiver
            .recv_timeout(std::time::Duration::from_millis(250))
            .is_ok();
        drop(send_guard);
        worker.join().expect("join request exit worker");

        assert!(completed, "request_exit waited for the send lock");
    }

    #[test]
    fn cancellation_guard_does_not_remove_a_replacement_read() {
        let cancellations = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let first = Arc::new(CancellationToken::new());
        cancellations.lock().insert(7, Arc::clone(&first));
        let first_guard = CancellationGuard {
            cancellations: Arc::clone(&cancellations),
            unique: 7,
            token: Arc::clone(&first),
        };

        let second = Arc::new(CancellationToken::new());
        cancellations.lock().insert(7, Arc::clone(&second));
        drop(first_guard);
        assert!(
            cancellations
                .lock()
                .get(&7)
                .is_some_and(|current| Arc::ptr_eq(current, &second))
        );

        let second_guard = CancellationGuard {
            cancellations: Arc::clone(&cancellations),
            unique: 7,
            token: second,
        };
        drop(second_guard);
        assert!(cancellations.lock().is_empty());
    }

    #[test]
    fn one_way_opcodes_never_get_implicit_error_replies() {
        for opcode in [FUSE_FORGET, FUSE_DESTROY, FUSE_INTERRUPT] {
            assert!(!opcode_allows_reply(opcode));
        }
        assert!(opcode_allows_reply(FUSE_LOOKUP));
    }

    #[test]
    fn zero_or_missing_errno_falls_back_to_io_error() {
        assert_eq!(normalize_errno(None), libc::EIO);
        assert_eq!(normalize_errno(Some(0)), libc::EIO);
        assert_eq!(normalize_errno(Some(libc::EINVAL)), libc::EINVAL);
    }

    #[test]
    fn statfs_name_limit_saturates_to_wire_width() {
        let mut stats = unsafe { std::mem::zeroed::<libc::statvfs>() };
        stats.f_namemax = (u32::MAX as libc::c_ulong) + 1;

        assert_eq!(statfs_out_from_statvfs(stats).st.namelen, u32::MAX);
    }

    #[test]
    fn protocol_layout_matches_the_darwin_fuse_wire_contract() {
        assert_eq!(size_of::<FuseInHeader>(), 40);
        assert_eq!(size_of::<FuseOutHeader>(), 16);
        assert_eq!(size_of::<FuseAttr>(), 104);
        assert_eq!(size_of::<FuseEntryOut>(), 144);
        assert_eq!(size_of::<FuseAttrOut>(), 120);
        assert_eq!(size_of::<FuseReadIn>(), 40);
        assert_eq!(size_of::<FuseWriteIn>(), 40);
    }

    #[test]
    fn rename_request_keeps_names_after_the_eight_byte_header() {
        assert_eq!(size_of::<FuseRenameIn>(), size_of::<u64>());

        let mut bytes = vec![0_u8; size_of::<FuseInHeader>()];
        bytes.extend_from_slice(&77_u64.to_ne_bytes());
        bytes.extend_from_slice(b"old\0new\0");
        let len = u32::try_from(bytes.len()).expect("rename body length");
        bytes[..size_of::<u32>()].copy_from_slice(&len.to_ne_bytes());

        let body = parse_message_body(bytes).expect("parse rename body");
        let input = read_copy::<FuseRenameIn>(&body.bytes, size_of::<FuseInHeader>())
            .expect("parse rename input");
        let (name, new_name) = parse_two_c_strings(
            &body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseRenameIn>(),
        )
        .expect("parse rename names");

        assert_eq!(input.newdir, 77);
        assert_eq!(name, b"old");
        assert_eq!(new_name, b"new");
    }

    #[test]
    fn fskit_link_request_uses_the_fuse_wire_layout() {
        assert_eq!(FUSE_LINK, 13);
        assert_eq!(size_of::<FuseLinkIn>(), size_of::<u64>());

        let mut bytes = vec![0_u8; size_of::<FuseInHeader>()];
        bytes.extend_from_slice(&77_u64.to_ne_bytes());
        bytes.extend_from_slice(b"alias\0");
        let len = u32::try_from(bytes.len()).expect("link body length");
        bytes[..size_of::<u32>()].copy_from_slice(&len.to_ne_bytes());

        let body = parse_message_body(bytes).expect("parse link body");
        let input = read_copy::<FuseLinkIn>(&body.bytes, size_of::<FuseInHeader>())
            .expect("parse link input");
        let name = parse_c_string(
            &body.bytes,
            size_of::<FuseInHeader>() + size_of::<FuseLinkIn>(),
        )
        .expect("parse link name");

        assert_eq!(input.oldnodeid, 77);
        assert_eq!(name, b"alias");
    }

    #[test]
    fn fskit_invalidation_frames_match_fuse_wire_layout() {
        let inode = invalidation_inode_bytes(42, -1, 0);
        let header =
            read_copy::<FuseOutHeader>(&inode, 0).expect("inode invalidation header should decode");
        assert_eq!(header.len as usize, inode.len());
        assert_eq!(header.error, FUSE_NOTIFY_INVAL_INODE);
        assert_eq!(header.unique, 0);
        let payload = read_copy::<FuseNotifyInvalInodeOut>(&inode, size_of::<FuseOutHeader>())
            .expect("inode invalidation payload should decode");
        assert_eq!((payload.ino, payload.off, payload.len), (42, -1, 0));

        let entry = invalidation_entry_bytes(7, b"alias").expect("entry fits");
        let entry_header =
            read_copy::<FuseOutHeader>(&entry, 0).expect("entry invalidation header should decode");
        assert_eq!(entry_header.len as usize, entry.len());
        assert_eq!(entry_header.error, FUSE_NOTIFY_INVAL_ENTRY);
        let entry_payload =
            read_copy::<FuseNotifyInvalEntryOut>(&entry, size_of::<FuseOutHeader>())
                .expect("entry invalidation payload should decode");
        assert_eq!((entry_payload.parent, entry_payload.namelen), (7, 5));
        assert_eq!(
            &entry[size_of::<FuseOutHeader>() + size_of::<FuseNotifyInvalEntryOut>()..],
            b"alias"
        );
    }

    #[test]
    fn fskit_invalidator_tolerates_a_closed_control() {
        let control = Arc::new(FsKitControl {
            channel: Mutex::new(None),
            send_lock: Mutex::new(()),
            stop_requested: AtomicBool::new(false),
            mounted: AtomicBool::new(false),
        });
        let invalidator = FsKitKernelInvalidator { control };

        invalidator.inval_inode(42);
        invalidator.inval_inode_attrs(42);
        invalidator.inval_entry(7, b"alias");
    }

    #[test]
    fn access_request_preserves_its_requested_mask() {
        assert_eq!(size_of::<FuseAccessIn>(), size_of::<u64>());

        let mut bytes = vec![0_u8; size_of::<FuseInHeader>()];
        bytes.extend_from_slice(&(libc::W_OK as u32).to_ne_bytes());
        bytes.extend_from_slice(&0_u32.to_ne_bytes());
        let len = u32::try_from(bytes.len()).expect("access body length");
        bytes[..size_of::<u32>()].copy_from_slice(&len.to_ne_bytes());

        let body = parse_message_body(bytes).expect("parse access body");
        let input = read_copy::<FuseAccessIn>(&body.bytes, size_of::<FuseInHeader>())
            .expect("parse access input");

        assert_eq!(input.mask, libc::W_OK as u32);
    }

    #[test]
    fn eof_read_uses_the_regular_success_reply_path() {
        assert!(!read_reply_uses_transport_buffer(&[], 4096));
        assert!(read_reply_uses_transport_buffer(&[], 0));
        assert!(read_reply_uses_transport_buffer(&[1], 4096));
    }

    #[test]
    fn directory_records_are_eight_byte_aligned_and_resume_by_offset() {
        let entries = vec![DirEntry {
            name: b"alpha".to_vec(),
            ino: 42,
            kind: FileType::RegularFile,
        }];
        let all = encode_dirents(1, 1, 0, 1024, &entries).expect("encode directory");
        assert_eq!(all.len() % 8, 0);
        let resumed = encode_dirents(1, 1, 2, 1024, &entries).expect("resume directory");
        assert_eq!(resumed.len() % 8, 0);
        assert!(resumed.len() < all.len());
    }

    #[test]
    fn directory_records_use_parent_inode_for_dot_dot() {
        let output = encode_dirents(42, 7, 0, 1024, &[]).expect("encode directory");
        let dot_record_len = align8(size_of::<u64>() * 2 + size_of::<u32>() * 2 + 1);
        let parent_ino = u64::from_ne_bytes(
            output[dot_record_len..dot_record_len + size_of::<u64>()]
                .try_into()
                .expect("read dot-dot inode"),
        );

        assert_eq!(parent_ino, 7);
    }

    #[test]
    fn mount_options_keep_the_fskit_fallback_explicit() {
        let options = mount_options("StudioFS").expect("valid options");
        let options = options.to_str().expect("ASCII options");
        assert!(options.contains("local"));
        assert!(options.contains("novncache"));
        assert!(options.contains("volname=StudioFS"));
    }

    #[test]
    fn fskit_mount_preflight_rejects_paths_outside_volumes() {
        let result = FsKitSession::mount(Path::new("/tmp/studiofs-fskit-test"), "StudioFS");

        let error = match result {
            Ok(_) => panic!("FSKit must reject a non-/Volumes path before FFI"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn fskit_mount_options_reject_ambiguous_volume_names() {
        assert!(mount_options("Studio,FS").is_err());
        assert!(mount_options("Studio\0FS").is_err());
    }

    #[test]
    fn fskit_setattr_time_prefers_now_and_rejects_overflow() {
        assert!(matches!(
            setattr_time(
                FATTR_ATIME | FATTR_ATIME_NOW,
                FATTR_ATIME,
                FATTR_ATIME_NOW,
                1,
                0,
            ),
            Ok(TimeSpec::Now)
        ));
        assert!(matches!(
            setattr_time(0, FATTR_ATIME, FATTR_ATIME_NOW, 0, 0),
            Ok(TimeSpec::Omit)
        ));
        assert_eq!(time_to_nanos(u64::MAX, 0), Err(libc::EOVERFLOW));
    }

    #[test]
    fn fskit_setattr_accepts_lock_owner_modifier() {
        assert!(setattr_flags_supported(FATTR_LOCKOWNER));
        assert!(!setattr_flags_supported(1 << 10));
    }
}
