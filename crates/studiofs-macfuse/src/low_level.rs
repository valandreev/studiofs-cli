//! Safe wrappers around the macFUSE low-level C API.
//!
//! This module is the only place in `studiofs-macfuse` that declares raw
//! low-level macFUSE FFI. Public types copy callback-owned values before they
//! can cross async boundaries and keep request/reply ownership explicit.

use std::ffi::{CString, OsStr};
use std::io;
use std::marker::PhantomData;
use std::os::raw::{c_char, c_double, c_int, c_uint, c_ulong, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

use libc::{off_t, size_t, stat};
use thiserror::Error;

pub(crate) mod raw {
    use super::*;

    /// Low-level FUSE inode number.
    pub type FuseIno = u64;

    /// Opaque low-level FUSE request handle.
    pub enum FuseReq {}

    /// Opaque low-level FUSE session handle.
    pub enum FuseSession {}

    /// Opaque low-level FUSE channel handle.
    pub enum FuseChan {}

    /// Opaque low-level FUSE argument vector.
    #[repr(C)]
    pub struct FuseArgs {
        /// Argument count.
        pub argc: c_int,
        /// Argument vector.
        pub argv: *mut *mut c_char,
        /// Whether libfuse owns `argv`.
        pub allocated: c_int,
    }

    /// macFUSE low-level entry parameters.
    #[repr(C)]
    pub struct FuseEntryParam {
        /// Unique inode number.
        pub ino: FuseIno,
        /// Generation number for this entry.
        pub generation: c_ulong,
        /// Inode attributes.
        pub attr: stat,
        /// Attribute validity timeout in seconds.
        pub attr_timeout: c_double,
        /// Name validity timeout in seconds.
        pub entry_timeout: c_double,
    }

    /// macFUSE low-level file-open information.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct FuseFileInfo {
        /// Open flags.
        pub flags: c_int,
        /// Legacy file handle slot.
        pub fh_old: c_ulong,
        /// Writepage marker.
        pub writepage: c_int,
        bitfield: c_uint,
        /// File handle.
        pub fh: u64,
        /// Lock owner.
        pub lock_owner: u64,
    }

    impl FuseFileInfo {
        /// Construct test-only callback data with the fields used by wrappers.
        #[cfg(test)]
        pub fn for_test(flags: c_int, fh: u64, direct_io: bool, keep_cache: bool) -> Self {
            let mut bitfield = 0;
            if direct_io {
                bitfield |= 1;
            }
            if keep_cache {
                bitfield |= 1 << 1;
            }
            Self {
                flags,
                fh_old: 0,
                writepage: 0,
                bitfield,
                fh,
                lock_owner: 0,
            }
        }

        /// Mutate test-only cache flag bits.
        #[cfg(test)]
        pub fn set_cache_flags_for_test(&mut self, direct_io: bool, keep_cache: bool) {
            self.bitfield = 0;
            if direct_io {
                self.bitfield |= 1;
            }
            if keep_cache {
                self.bitfield |= 1 << 1;
            }
        }

        pub(super) fn direct_io(self) -> bool {
            self.bitfield & 1 != 0
        }

        pub(super) fn keep_cache(self) -> bool {
            self.bitfield & (1 << 1) != 0
        }

        pub(super) fn from_open_reply(reply: OpenReply) -> Self {
            let mut bitfield = 0;
            if reply.direct_io {
                bitfield |= 1;
            }
            if reply.keep_cache {
                bitfield |= 1 << 1;
            }
            Self {
                flags: reply.flags,
                fh_old: 0,
                writepage: 0,
                bitfield,
                fh: reply.file_handle,
                lock_owner: 0,
            }
        }
    }

    /// macFUSE low-level callback table.
    #[repr(C)]
    pub struct FuseLowLevelOps {
        _private: [u8; 0],
    }

    #[link(name = "fuse")]
    unsafe extern "C" {
        /// Reply with a POSIX errno.
        pub fn fuse_reply_err(req: *mut FuseReq, err: c_int) -> c_int;
        /// Reply with an entry.
        pub fn fuse_reply_entry(req: *mut FuseReq, entry: *const FuseEntryParam) -> c_int;
        /// Reply with attributes.
        pub fn fuse_reply_attr(req: *mut FuseReq, attr: *const stat, timeout: c_double) -> c_int;
        /// Reply with open parameters.
        pub fn fuse_reply_open(req: *mut FuseReq, fi: *const FuseFileInfo) -> c_int;
        /// Reply with a read or directory buffer.
        pub fn fuse_reply_buf(req: *mut FuseReq, buf: *const c_char, size: size_t) -> c_int;
        /// Reply with the number of written bytes.
        pub fn fuse_reply_write(req: *mut FuseReq, count: size_t) -> c_int;
        /// Append or size a directory entry.
        pub fn fuse_add_direntry(
            req: *mut FuseReq,
            buf: *mut c_char,
            bufsize: size_t,
            name: *const c_char,
            stbuf: *const stat,
            off: off_t,
        ) -> size_t;
        /// Create a low-level session from callback operations.
        pub fn fuse_lowlevel_new(
            args: *mut FuseArgs,
            op: *const FuseLowLevelOps,
            op_size: size_t,
            userdata: *mut c_void,
        ) -> *mut FuseSession;
        /// Mount a FUSE channel at a mount point.
        pub fn fuse_mount(mountpoint: *const c_char, args: *mut FuseArgs) -> *mut FuseChan;
        /// Attach a channel to a session.
        pub fn fuse_session_add_chan(session: *mut FuseSession, channel: *mut FuseChan);
        /// Destroy a low-level session and its attached channel.
        pub fn fuse_session_destroy(session: *mut FuseSession);
        /// Ask a session loop to exit.
        pub fn fuse_session_exit(session: *mut FuseSession);
    }
}

unsafe extern "C" {
    fn studiofs_macfuse_empty_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_macfuse_low_level_ops_size() -> size_t;
}

/// Low-level callback operation table provided by macFUSE headers.
#[derive(Debug, Clone, Copy)]
pub struct LowLevelOperations {
    raw: *const raw::FuseLowLevelOps,
    size: size_t,
}

impl LowLevelOperations {
    /// Empty operation table for lifecycle smoke tests.
    pub fn empty() -> Self {
        // SAFETY: The C shim returns a pointer to a static table and its exact
        // header-derived size.
        unsafe {
            Self {
                raw: studiofs_macfuse_empty_low_level_ops(),
                size: studiofs_macfuse_low_level_ops_size(),
            }
        }
    }

    pub(crate) fn as_ptr(self) -> *const raw::FuseLowLevelOps {
        self.raw
    }

    /// Header-derived callback table size.
    pub fn size(self) -> size_t {
        self.size
    }
}

/// Owned copy of callback-scoped `fuse_file_info` data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileInfoSnapshot {
    flags: c_int,
    file_handle: u64,
    direct_io: bool,
    keep_cache: bool,
}

/// Owned mount/session configuration for low-level macFUSE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    run_in_foreground: bool,
}

impl SessionConfig {
    /// Build a low-level session configuration.
    pub fn new(mount_point: impl Into<PathBuf>, volume_name: impl Into<String>) -> Self {
        Self {
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            run_in_foreground: false,
        }
    }

    /// Set whether the filesystem is mounted read-only.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set whether the session stays in the foreground.
    pub fn with_run_in_foreground(mut self, run_in_foreground: bool) -> Self {
        self.run_in_foreground = run_in_foreground;
        self
    }

    /// Mount point for this session.
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Requested volume name retained for higher-level macOS integration.
    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }

    /// Whether the eventual session runner should stay in the foreground.
    pub fn run_in_foreground(&self) -> bool {
        self.run_in_foreground
    }

    /// Build owned argv strings for libfuse.
    pub fn argv_strings(&self) -> Vec<String> {
        let mut args = vec!["studiofs-macfuse".to_owned()];
        if self.read_only {
            args.push("-oro".to_owned());
        }
        args
    }

    fn mount_point_cstring(&self) -> io::Result<CString> {
        CString::new(self.mount_point.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "macFUSE mount point contains an interior NUL byte",
            )
        })
    }
}

struct OwnedFuseArgs {
    _strings: Vec<CString>,
    _argv: Vec<*mut c_char>,
    raw: raw::FuseArgs,
}

impl OwnedFuseArgs {
    fn new(config: &SessionConfig) -> io::Result<Self> {
        let strings = config
            .argv_strings()
            .into_iter()
            .map(|arg| {
                CString::new(arg).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "macFUSE option contains an interior NUL byte",
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut argv = strings
            .iter()
            .map(|arg| arg.as_ptr().cast_mut())
            .collect::<Vec<_>>();
        let raw = raw::FuseArgs {
            argc: argv.len().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "too many macFUSE arguments")
            })?,
            argv: argv.as_mut_ptr(),
            allocated: 0,
        };
        Ok(Self {
            _strings: strings,
            _argv: argv,
            raw,
        })
    }

    fn as_mut_ptr(&mut self) -> *mut raw::FuseArgs {
        &mut self.raw
    }
}

/// Mounted low-level macFUSE session.
pub struct Session {
    raw: NonNull<raw::FuseSession>,
}

impl Session {
    /// Create and mount a low-level macFUSE session.
    ///
    /// # Safety
    ///
    /// `ops` must contain a valid `fuse_lowlevel_ops` table whose callback
    /// function pointers remain valid for the returned session lifetime.
    /// `userdata` must remain valid until the session is destroyed, unless the
    /// callback table intentionally handles a null user-data pointer.
    pub unsafe fn mount_with_ops(
        config: &SessionConfig,
        ops: LowLevelOperations,
        userdata: *mut c_void,
    ) -> io::Result<Self> {
        let mut args = OwnedFuseArgs::new(config)?;
        let mount_point = config.mount_point_cstring()?;
        // SAFETY: `args`, `ops`, and `userdata` satisfy the caller contract for
        // `fuse_lowlevel_new`; `args` and callback table live through the call.
        let session = NonNull::new(unsafe {
            raw::fuse_lowlevel_new(args.as_mut_ptr(), ops.as_ptr(), ops.size(), userdata)
        })
        .ok_or_else(|| io::Error::other("macFUSE failed to create low-level session"))?;
        // SAFETY: `mount_point` and `args` are valid for this call.
        let channel =
            NonNull::new(unsafe { raw::fuse_mount(mount_point.as_ptr(), args.as_mut_ptr()) });
        match channel {
            Some(channel) => {
                // SAFETY: Both pointers were returned by macFUSE and are live.
                unsafe { raw::fuse_session_add_chan(session.as_ptr(), channel.as_ptr()) };
                Ok(Self { raw: session })
            }
            None => {
                // SAFETY: `session` is live and has not been returned.
                unsafe { raw::fuse_session_destroy(session.as_ptr()) };
                Err(io::Error::other(
                    "macFUSE failed to mount low-level channel",
                ))
            }
        }
    }

    /// Request session loop shutdown.
    pub fn request_exit(&self) {
        // SAFETY: `self.raw` is owned by this wrapper and remains live until Drop.
        unsafe { raw::fuse_session_exit(self.raw.as_ptr()) };
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // SAFETY: `self.raw` is owned by this wrapper and destroyed exactly once.
        unsafe { raw::fuse_session_destroy(self.raw.as_ptr()) };
    }
}

impl FileInfoSnapshot {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "called by low-level callbacks added after this boundary"
        )
    )]
    pub(crate) fn copy_from_callback(info: &raw::FuseFileInfo) -> Self {
        let copied = *info;
        Self {
            flags: copied.flags,
            file_handle: copied.fh,
            direct_io: copied.direct_io(),
            keep_cache: copied.keep_cache(),
        }
    }

    /// Open flags from the callback.
    pub fn flags(self) -> c_int {
        self.flags
    }

    /// File handle carried by the callback.
    pub fn file_handle(self) -> u64 {
        self.file_handle
    }

    /// Whether direct I/O was requested.
    pub fn direct_io(self) -> bool {
        self.direct_io
    }

    /// Whether kernel cache should be kept.
    pub fn keep_cache(self) -> bool {
        self.keep_cache
    }
}

/// Open reply parameters owned by Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenReply {
    flags: c_int,
    file_handle: u64,
    direct_io: bool,
    keep_cache: bool,
}

impl OpenReply {
    /// Create an open reply.
    pub fn new(flags: c_int, file_handle: u64) -> Self {
        Self {
            flags,
            file_handle,
            direct_io: false,
            keep_cache: false,
        }
    }

    /// Set whether direct I/O should be requested.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// Set whether the kernel should keep cached data.
    pub fn with_keep_cache(mut self, keep_cache: bool) -> Self {
        self.keep_cache = keep_cache;
        self
    }
}

/// Owned low-level request token.
pub struct Request {
    raw: NonNull<raw::FuseReq>,
    _not_send_sync: PhantomData<*mut ()>,
}

impl Request {
    /// Wrap a callback-provided request pointer.
    ///
    /// # Safety
    ///
    /// `raw` must be a live macFUSE request pointer for a callback that has not
    /// been replied to. The returned wrapper must be consumed by exactly one
    /// reply method.
    pub unsafe fn from_raw(raw: *mut raw::FuseReq) -> Option<Self> {
        NonNull::new(raw).map(|raw| Self {
            raw,
            _not_send_sync: PhantomData,
        })
    }

    fn as_ptr(&self) -> *mut raw::FuseReq {
        self.raw.as_ptr()
    }
}

/// Low-level reply sender that consumes request ownership.
pub struct Reply {
    request: Request,
}

impl Reply {
    /// Create a reply sender for an owned request token.
    pub fn new(request: Request) -> Self {
        Self { request }
    }

    /// Reply with a POSIX errno.
    pub fn error(self, errno: c_int) -> io::Result<()> {
        // SAFETY: `Request` guarantees a live, unreplied low-level request and
        // this method consumes it, so macFUSE sees at most one reply.
        reply_result(unsafe { raw::fuse_reply_err(self.request.as_ptr(), errno) })
    }

    /// Reply with an entry.
    pub fn entry(self, entry: &Entry) -> io::Result<()> {
        // SAFETY: `entry.raw` points to initialized C-compatible data for the
        // duration of the call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_entry(self.request.as_ptr(), &entry.raw) })
    }

    /// Reply with attributes.
    pub fn attr(self, attr: &stat, timeout: f64) -> io::Result<()> {
        // SAFETY: `attr` is initialized and borrowed for the duration of the
        // call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_attr(self.request.as_ptr(), attr, timeout) })
    }

    /// Reply with open parameters.
    pub fn open(self, open: OpenReply) -> io::Result<()> {
        let file_info = raw::FuseFileInfo::from_open_reply(open);
        // SAFETY: `file_info` is initialized and borrowed for the duration of
        // the call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_open(self.request.as_ptr(), &file_info) })
    }

    /// Reply with a read buffer.
    pub fn read_buffer(self, bytes: &[u8]) -> io::Result<()> {
        // SAFETY: `bytes` is valid for the duration of the call and macFUSE
        // copies/sends it synchronously; `self` consumes the single reply.
        reply_result(unsafe {
            raw::fuse_reply_buf(
                self.request.as_ptr(),
                bytes.as_ptr().cast::<c_char>(),
                bytes.len(),
            )
        })
    }

    /// Reply with a write byte count.
    pub fn write_count(self, count: usize) -> io::Result<()> {
        // SAFETY: `self` owns a live, unreplied request and is consumed here.
        reply_result(unsafe { raw::fuse_reply_write(self.request.as_ptr(), count) })
    }

    /// Reply with owned directory entries.
    pub fn directory_entries(self, entries: &DirectoryEntryBuffer) -> io::Result<()> {
        self.read_buffer(entries.as_slice())
    }
}

/// Owned low-level entry reply data.
pub struct Entry {
    raw: raw::FuseEntryParam,
}

impl Entry {
    /// Create entry reply data.
    pub fn new(
        ino: u64,
        generation: c_ulong,
        attr: stat,
        attr_timeout: f64,
        entry_timeout: f64,
    ) -> Self {
        Self {
            raw: raw::FuseEntryParam {
                ino,
                generation,
                attr,
                attr_timeout,
                entry_timeout,
            },
        }
    }
}

/// Owned readdir reply buffer.
pub struct DirectoryEntryBuffer {
    bytes: Vec<u8>,
    capacity: usize,
}

impl DirectoryEntryBuffer {
    /// Create a directory entry buffer with a fixed maximum size.
    pub fn new(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Current buffer length.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Borrow the initialized directory entry bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Append an entry using macFUSE's layout helper.
    pub fn push(
        &mut self,
        request: &Request,
        name: &OsStr,
        attr: &stat,
        next_offset: off_t,
    ) -> Result<(), DirectoryEntryError> {
        let name = CString::new(name.as_bytes()).map_err(|_| DirectoryEntryError::InteriorNul)?;
        let available = self.capacity.saturating_sub(self.bytes.len());
        // SAFETY: Passing a null buffer with zero size asks macFUSE for the
        // required entry size. `name` and `attr` remain valid for the call.
        let required = unsafe {
            raw::fuse_add_direntry(
                request.as_ptr(),
                std::ptr::null_mut(),
                0,
                name.as_ptr(),
                attr,
                next_offset,
            )
        };
        if required > available {
            return Err(DirectoryEntryError::InsufficientCapacity {
                required,
                available,
            });
        }

        let start = self.bytes.len();
        self.bytes.resize(start + required, 0);
        // SAFETY: The resized vector exposes `required` initialized bytes at
        // `start`, and macFUSE writes at most the provided remaining capacity.
        unsafe {
            raw::fuse_add_direntry(
                request.as_ptr(),
                self.bytes[start..].as_mut_ptr().cast::<c_char>(),
                required,
                name.as_ptr(),
                attr,
                next_offset,
            );
        }
        Ok(())
    }

    /// Test-only append that preserves owned names without a live request.
    #[cfg(test)]
    pub fn push_for_test(
        &mut self,
        ino: u64,
        name: &OsStr,
        next_offset: off_t,
    ) -> Result<(), DirectoryEntryError> {
        let name = CString::new(name.as_bytes()).map_err(|_| DirectoryEntryError::InteriorNul)?;
        let bytes = name.as_bytes_with_nul();
        let required = std::mem::size_of::<u64>() + std::mem::size_of::<off_t>() + bytes.len();
        let available = self.capacity.saturating_sub(self.bytes.len());
        if required > available {
            return Err(DirectoryEntryError::InsufficientCapacity {
                required,
                available,
            });
        }
        self.bytes.extend_from_slice(&ino.to_ne_bytes());
        self.bytes.extend_from_slice(&next_offset.to_ne_bytes());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

/// Directory-entry buffer construction error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectoryEntryError {
    /// Entry name contains an interior NUL byte.
    #[error("directory entry name contains an interior NUL byte")]
    InteriorNul,
    /// Entry does not fit in the remaining reply buffer.
    #[error("directory entry requires {required} bytes, but only {available} remain")]
    InsufficientCapacity {
        /// Required entry bytes.
        required: usize,
        /// Remaining buffer bytes.
        available: usize,
    },
}

fn reply_result(result: c_int) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(-result))
    }
}
