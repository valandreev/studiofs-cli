//! fuser::Filesystem implementation — bridges FUSE kernel calls to StudioVfs.

mod handle_drain;

#[cfg(target_os = "macos")]
use std::borrow::Cow;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "macos")]
use fuser::ReplyIoctl;
use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr,
    Request,
};
use handle_drain::FuseHandleDrainFacade;
use studiofs_meta::FileType;
#[cfg(not(target_os = "macos"))]
use studiofs_meta::ROOT_INO;
#[cfg(target_os = "macos")]
use studiofs_meta::engine::{XATTR_CREATE_FLAG, XATTR_REPLACE_FLAG};
#[cfg(target_os = "macos")]
use studiofs_meta::schema::DirEntry;
use studiofs_meta::types::{SetattrArgs, TimeSpec};
#[cfg(not(target_os = "macos"))]
use studiofs_vfs::is_hidden_non_macos_root_metadata_name;
use studiofs_vfs::{
    BridgeVfsErrorKind, KernelInvalidator, LocalSyncPolicy, LockKind, OpenFlags, StudioVfs,
    VfsError, classify_vfs_error, normalize_attr_times,
};
use tokio::runtime::Handle;
use tracing::{debug, error, warn};
#[cfg(target_os = "macos")]
use unicode_normalization::UnicodeNormalization;

#[cfg(target_os = "macos")]
const TTL: Duration = Duration::from_secs(0);
#[cfg(not(target_os = "macos"))]
const TTL: Duration = Duration::from_secs(1);
const DEFAULT_FUSE_MAX_WRITE_BYTES: u32 = 16 * 1024 * 1024;
#[cfg(target_os = "macos")]
const DEFAULT_FUSE_MAX_READAHEAD_BYTES: u32 = 1024 * 1024;
#[cfg(not(target_os = "macos"))]
const DEFAULT_FUSE_MAX_READAHEAD_BYTES: u32 = 16 * 1024 * 1024;
#[cfg(target_os = "macos")]
const FUSE_AUTO_INVAL_DATA_FLAG: u64 = 1 << 12;

fn fuse_open_reply_flags(_open_flags: OpenFlags) -> u32 {
    let mut flags = 0;
    #[cfg(target_os = "macos")]
    {
        // Same family of fix as the WinFSP cache bypass, but stricter for
        // macFUSE: stale kernel EOF can survive sparse size changes even on
        // read-only handles.
        flags |= fuser::consts::FOPEN_DIRECT_IO;
    }
    flags
}

fn parse_bridge_u32_env(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(raw) => {
            let trimmed = raw.trim();
            match trimmed.parse::<u32>() {
                Ok(value) if value > 0 => value,
                Ok(_) => {
                    warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_value = default,
                        "bridge tuning override must be > 0; using default"
                    );
                    default
                }
                Err(error) => {
                    warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_value = default,
                        error = %error,
                        "invalid bridge tuning override; using default"
                    );
                    default
                }
            }
        }
        Err(_) => default,
    }
}

fn os_str_bytes(name: &OsStr) -> &[u8] {
    name.as_bytes()
}

fn debug_name(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(not(target_os = "macos"))]
fn should_hide_non_macos_root_metadata_entry(parent: u64, name: &[u8]) -> bool {
    parent == ROOT_INO && is_hidden_non_macos_root_metadata_name(name)
}

#[cfg(target_os = "macos")]
fn should_hide_non_macos_root_metadata_entry(_parent: u64, _name: &[u8]) -> bool {
    false
}

fn heal_hidden_non_macos_root_metadata_entry(vfs: &StudioVfs, parent: u64, name: &[u8]) {
    if !should_hide_non_macos_root_metadata_entry(parent, name) {
        return;
    }

    if let Err(error) = vfs.heal_dangling_entry_if_inode_missing(parent, name) {
        debug!(
            parent,
            name = %debug_name(name),
            "failed to heal hidden non-macOS root metadata entry: {error}"
        );
    }
}

fn is_missing_xattr_error(err: &VfsError) -> bool {
    matches!(
        err,
        VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
    )
}

fn missing_xattr_errno() -> i32 {
    #[cfg(target_os = "macos")]
    {
        libc::ENOATTR
    }
    #[cfg(not(target_os = "macos"))]
    {
        libc::ENODATA
    }
}

fn note_critical_vfs_error(e: &VfsError) {
    if matches!(
        classify_vfs_error(e),
        BridgeVfsErrorKind::CriticalRuntimeFailure
    ) && let VfsError::Join(join_err) = e
    {
        error!(
            error = %join_err,
            "critical runtime failure (JoinError), requesting emergency unmount"
        );
        super::SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(target_os = "macos")]
fn merge_xattr_write(existing: Option<Vec<u8>>, position: u32, value: &[u8]) -> Vec<u8> {
    let offset = position as usize;
    let existing_len = existing.as_ref().map_or(0, Vec::len);
    let new_len = existing_len.max(offset.saturating_add(value.len()));
    let mut merged = vec![0; new_len];

    if let Some(existing) = existing {
        merged[..existing.len()].copy_from_slice(&existing);
    }
    merged[offset..offset + value.len()].copy_from_slice(value);
    merged
}

#[cfg(target_os = "macos")]
fn normalize_macos_xattr_flags(flags: i32) -> u32 {
    const MACOS_XATTR_CREATE: i32 = 0x0002;
    const MACOS_XATTR_REPLACE: i32 = 0x0004;
    let mut normalized = 0;
    if flags & MACOS_XATTR_CREATE != 0 {
        normalized |= XATTR_CREATE_FLAG;
    }
    if flags & MACOS_XATTR_REPLACE != 0 {
        normalized |= XATTR_REPLACE_FLAG;
    }
    normalized
}

fn normalize_unix_mode(mode: u32) -> u16 {
    // VFS stores only Unix permission bits; file type is tracked separately.
    u16::try_from(mode & 0o7777).unwrap_or(0)
}

fn system_time_to_timespec(time: SystemTime) -> TimeSpec {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or(i64::MAX);
    TimeSpec::Set(secs)
}

#[cfg(target_os = "macos")]
fn macos_truncated_rename_suffix_is_safe(candidate: &[u8], old_name: &[u8]) -> bool {
    const MIN_BOUNDARY_SUFFIX_LEN: usize = 3;
    const MIN_BOUNDARY_ALNUM_SUFFIX_LEN: usize = 6;
    const MIN_ALNUM_SUFFIX_LEN: usize = 9;

    if candidate.len() <= old_name.len() || !candidate.ends_with(old_name) {
        return false;
    }
    let Some(first_suffix_byte) = old_name.first() else {
        return false;
    };
    if !first_suffix_byte.is_ascii_alphanumeric() {
        return old_name.len() >= MIN_BOUNDARY_SUFFIX_LEN;
    }

    let prefix_len = candidate.len() - old_name.len();
    if candidate
        .get(prefix_len.saturating_sub(1))
        .is_some_and(|byte| !byte.is_ascii_alphanumeric())
    {
        return old_name.len() >= MIN_BOUNDARY_ALNUM_SUFFIX_LEN;
    }

    old_name.len() >= MIN_ALNUM_SUFFIX_LEN
        && old_name.iter().any(|byte| !byte.is_ascii_alphanumeric())
}

#[cfg(target_os = "macos")]
fn macos_visible_readdir_names(entries: &[DirEntry]) -> impl Iterator<Item = &[u8]> {
    entries
        .iter()
        .map(|entry| entry.name.as_slice())
        .filter(|name| !matches!(*name, b"." | b".."))
}

#[cfg(target_os = "macos")]
fn recover_macos_truncated_rename_source(old_name: &[u8], entries: &[DirEntry]) -> Option<Vec<u8>> {
    // macFUSE can deliver FUSE_RENAME with an empty old-name payload or a
    // payload missing an observed variable-length prefix. Only recover when
    // the remaining suffix identifies exactly one existing entry. Non-empty
    // suffixes must either include enough separator structure or start after
    // a separator in the visible entry; otherwise a normal failed rename of
    // "a.txt" could incorrectly target "prefix-a.txt".
    if old_name.is_empty() {
        let mut candidates = macos_visible_readdir_names(entries);
        let recovered = candidates.next()?;
        if candidates.next().is_some() {
            return None;
        }
        return Some(recovered.to_vec());
    }

    let mut candidates = entries
        .iter()
        .map(|entry| entry.name.as_slice())
        .filter(|candidate| !matches!(*candidate, b"." | b".."))
        .filter(|candidate| macos_truncated_rename_suffix_is_safe(candidate, old_name));

    let recovered = candidates.next()?;
    if candidates.next().is_some() {
        return None;
    }
    Some(recovered.to_vec())
}

#[cfg(target_os = "macos")]
fn recover_macos_normalized_name(name: &[u8], entries: &[DirEntry]) -> Option<Vec<u8>> {
    // Policy: keep the VFS namespace byte-exact, but at the macOS FUSE boundary
    // resolve name lookup/rename by canonical Unicode equivalence (NFD key).
    let key_str = std::str::from_utf8(name).ok()?;
    let key_nfd = key_str.nfd().collect::<String>();
    let mut canonical_match: Option<&[u8]> = None;
    for entry in entries {
        if matches!(entry.name.as_slice(), b"." | b"..") {
            continue;
        }
        if entry.name.as_slice() == name {
            return None;
        }
        let Ok(entry_str) = std::str::from_utf8(&entry.name) else {
            continue;
        };
        if !entry_str.nfd().eq(key_nfd.chars()) {
            continue;
        }
        if canonical_match.is_some() {
            return None;
        }
        canonical_match = Some(entry.name.as_slice());
    }
    canonical_match.map(ToOwned::to_owned)
}

#[cfg(target_os = "macos")]
fn resolve_macos_namespace_name<'a>(name: &'a [u8], entries: &[DirEntry]) -> Cow<'a, [u8]> {
    recover_macos_normalized_name(name, entries)
        .map(Cow::Owned)
        .unwrap_or(Cow::Borrowed(name))
}

#[cfg(target_os = "macos")]
fn lookup_macos_normalized_forms<'a>(
    vfs: &StudioVfs,
    parent: u64,
    requested_name: &'a [u8],
) -> Option<Cow<'a, [u8]>> {
    let requested = std::str::from_utf8(requested_name).ok()?;
    let nfc = requested.nfc().collect::<String>();
    if nfc.as_bytes() != requested_name && vfs.lookup(parent, nfc.as_bytes()).is_ok() {
        return Some(Cow::Owned(nfc.into_bytes()));
    }
    let nfd = requested.nfd().collect::<String>();
    if nfd.as_bytes() != requested_name && vfs.lookup(parent, nfd.as_bytes()).is_ok() {
        return Some(Cow::Owned(nfd.into_bytes()));
    }
    None
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MacosMutationTargetState {
    Exists,
    Missing,
}

#[cfg(target_os = "macos")]
fn resolve_macos_existing_mutation_source<'a>(
    vfs: &StudioVfs,
    parent: u64,
    requested_name: &'a [u8],
) -> Cow<'a, [u8]> {
    if vfs.lookup(parent, requested_name).is_ok() {
        return Cow::Borrowed(requested_name);
    }
    let Ok(entries) = vfs.readdir(parent) else {
        return Cow::Borrowed(requested_name);
    };
    let normalized_name = resolve_macos_namespace_name(requested_name, &entries);
    if normalized_name.as_ref() != requested_name {
        return normalized_name;
    }
    recover_macos_truncated_rename_source(requested_name, &entries)
        .map(Cow::Owned)
        .unwrap_or(Cow::Borrowed(requested_name))
}

#[cfg(target_os = "macos")]
fn resolve_macos_mutation_target_name_with_state<'a>(
    vfs: &StudioVfs,
    parent: u64,
    requested_name: &'a [u8],
) -> studiofs_vfs::VfsResult<(Cow<'a, [u8]>, MacosMutationTargetState)> {
    match vfs.lookup(parent, requested_name) {
        Ok(_) => {
            return Ok((
                Cow::Borrowed(requested_name),
                MacosMutationTargetState::Exists,
            ));
        }
        Err(VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })) => {}
        Err(error) => return Err(error),
    }
    if requested_name.is_ascii() {
        return Ok((
            Cow::Borrowed(requested_name),
            MacosMutationTargetState::Missing,
        ));
    }
    if let Some(name) = lookup_macos_normalized_forms(vfs, parent, requested_name) {
        return Ok((name, MacosMutationTargetState::Exists));
    }
    Ok((
        Cow::Borrowed(requested_name),
        MacosMutationTargetState::Missing,
    ))
}

#[cfg(target_os = "macos")]
fn resolve_macos_mutation_target_name<'a>(
    vfs: &StudioVfs,
    parent: u64,
    requested_name: &'a [u8],
) -> Cow<'a, [u8]> {
    resolve_macos_mutation_target_name_with_state(vfs, parent, requested_name)
        .map(|(name, _state)| name)
        .unwrap_or(Cow::Borrowed(requested_name))
}

#[cfg(target_os = "macos")]
fn macos_exchange_or_rename(
    vfs: &StudioVfs,
    parent: u64,
    name: &[u8],
    newparent: u64,
    newname: &[u8],
) -> studiofs_vfs::VfsResult<()> {
    let old_name = resolve_macos_existing_mutation_source(vfs, parent, name);
    let (new_name, target_state) =
        resolve_macos_mutation_target_name_with_state(vfs, newparent, newname)?;
    // macFUSE reports some ordinary Finder renames as RENAME_EXCHANGE even when
    // the destination is absent. We must probe first to preserve real exchange
    // failures for existing targets; this fallback is therefore not atomic if a
    // concurrent writer creates the target between lookup and rename.
    match target_state {
        MacosMutationTargetState::Exists => Err(VfsError::Io(std::io::Error::from_raw_os_error(
            libc::ENOSYS,
        ))),
        MacosMutationTargetState::Missing => vfs.rename(parent, &old_name, newparent, &new_name),
    }
}

#[cfg(target_os = "macos")]
fn normalize_macos_readdir_name(name: &[u8]) -> Vec<u8> {
    // Finder/macFUSE expect decomposed (NFD) names in directory listings.
    let Some(name) = std::str::from_utf8(name).ok() else {
        return name.to_vec();
    };
    if name.nfd().eq(name.chars()) {
        return name.as_bytes().to_vec();
    }
    let mut normalized = Vec::with_capacity(name.len());
    for ch in name.nfd() {
        let mut utf8 = [0_u8; 4];
        normalized.extend_from_slice(ch.encode_utf8(&mut utf8).as_bytes());
    }
    normalized
}

/// Convert POSIX O_* flags to our OpenFlags.
fn posix_flags_to_open_flags(flags: i32) -> (OpenFlags, bool) {
    let access = flags & libc::O_ACCMODE;
    let mut f = match access {
        libc::O_RDONLY => OpenFlags::RDONLY,
        libc::O_WRONLY => OpenFlags::WRONLY,
        libc::O_RDWR => OpenFlags::RDWR,
        _ => OpenFlags::RDONLY,
    };
    if flags & libc::O_APPEND != 0 {
        f = f | OpenFlags::APPEND;
    }
    let trunc = flags & libc::O_TRUNC != 0;
    (f, trunc)
}

// ─── Error Mapping ───────────────────────────────────────────────────────────

/// Map VfsError to the correct libc errno for FUSE replies.
fn vfs_err_to_errno(e: &VfsError) -> i32 {
    note_critical_vfs_error(e);
    match classify_vfs_error(e) {
        BridgeVfsErrorKind::NotFound => libc::ENOENT,
        BridgeVfsErrorKind::AlreadyExists => libc::EEXIST,
        BridgeVfsErrorKind::NotDirectory => libc::ENOTDIR,
        BridgeVfsErrorKind::IsDirectory => libc::EISDIR,
        BridgeVfsErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
        BridgeVfsErrorKind::UnrecognizedVolume => libc::ENODEV,
        BridgeVfsErrorKind::XattrValueTooLarge => libc::E2BIG,
        BridgeVfsErrorKind::InvalidInput => libc::EINVAL,
        BridgeVfsErrorKind::PermissionDenied => libc::EACCES,
        BridgeVfsErrorKind::Retryable => libc::EAGAIN,
        BridgeVfsErrorKind::TimedOut => libc::ETIMEDOUT,
        BridgeVfsErrorKind::InsufficientResources => libc::ENOMEM,
        BridgeVfsErrorKind::DataCorruption
        | BridgeVfsErrorKind::CriticalRuntimeFailure
        | BridgeVfsErrorKind::GenericFailure => libc::EIO,
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert InodeAttr to fuser FileAttr.
/// `mount_uid`/`mount_gid` are used to remap inodes that were created with
/// uid=0/gid=0 (the old default) so that the mounting user owns them.
fn to_fuse_attr(attr: &studiofs_meta::InodeAttr, mount_uid: u32, mount_gid: u32) -> FileAttr {
    let times = normalize_attr_times(attr);
    let uid = if attr.uid == 0 { mount_uid } else { attr.uid };
    let gid = if attr.gid == 0 { mount_gid } else { attr.gid };
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: UNIX_EPOCH + Duration::from_nanos(times.atime_unix_nanos),
        mtime: UNIX_EPOCH + Duration::from_nanos(times.mtime_unix_nanos),
        ctime: UNIX_EPOCH + Duration::from_nanos(times.ctime_unix_nanos),
        crtime: UNIX_EPOCH + Duration::from_nanos(times.creation_time_unix_nanos),
        kind: match attr.kind {
            FileType::RegularFile => FuseFileType::RegularFile,
            FileType::Directory => FuseFileType::Directory,
            FileType::Symlink => FuseFileType::Symlink,
        },
        perm: attr.mode & 0o7777,
        nlink: attr.nlink,
        uid,
        gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

fn to_u64_offset(offset: i64) -> Result<u64, libc::c_int> {
    u64::try_from(offset).map_err(|_| libc::EINVAL)
}

fn clamp_offset_to_usize(offset: i64) -> Result<usize, libc::c_int> {
    usize::try_from(offset).map_err(|_| libc::EINVAL)
}

fn fuse_file_type(kind: FileType) -> FuseFileType {
    match kind {
        FileType::RegularFile => FuseFileType::RegularFile,
        FileType::Directory => FuseFileType::Directory,
        FileType::Symlink => FuseFileType::Symlink,
    }
}

// ─── StudioFuseFs ────────────────────────────────────────────────────────────

/// FUSE filesystem adapter that forwards kernel callbacks to `StudioVfs`.
pub struct StudioFuseFs {
    vfs: Arc<StudioVfs>,
    rt: Handle,
    /// UID of the user that mounted the filesystem. Inodes owned by uid=0 in
    /// the DB are remapped to this so macOS permission checks don't block writes.
    mount_uid: u32,
    /// GID of the user that mounted the filesystem.
    mount_gid: u32,
}

impl StudioFuseFs {
    /// Create a FUSE adapter backed by a shared VFS and Tokio runtime handle.
    pub fn new(vfs: Arc<StudioVfs>, rt: Handle) -> Self {
        // SAFETY: getuid/getgid read process-wide credential values from libc and are
        // side-effect-free.
        let mount_uid = unsafe { libc::getuid() };
        // SAFETY: same safety rationale as getuid.
        let mount_gid = unsafe { libc::getgid() };
        Self {
            vfs,
            rt,
            mount_uid,
            mount_gid,
        }
    }

    fn handle_drain(&self) -> FuseHandleDrainFacade<'_> {
        FuseHandleDrainFacade::new(self)
    }
}

impl Filesystem for StudioFuseFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        let requested_max_write = parse_bridge_u32_env(
            "STUDIOFS_FUSE_MAX_WRITE_BYTES",
            DEFAULT_FUSE_MAX_WRITE_BYTES,
        );
        if let Err(max_supported) = config.set_max_write(requested_max_write) {
            warn!(
                requested = requested_max_write,
                max_supported, "requested FUSE max_write exceeds kernel/libfuse limit; clamping"
            );
            if config.set_max_write(max_supported).is_err() {
                return Err(libc::EINVAL);
            }
        }

        let requested_readahead = parse_bridge_u32_env(
            "STUDIOFS_FUSE_MAX_READAHEAD_BYTES",
            DEFAULT_FUSE_MAX_READAHEAD_BYTES,
        );
        if let Err(max_supported) = config.set_max_readahead(requested_readahead) {
            warn!(
                requested = requested_readahead,
                max_supported,
                "requested FUSE max_readahead exceeds kernel/libfuse limit; clamping"
            );
            if config.set_max_readahead(max_supported).is_err() {
                return Err(libc::EINVAL);
            }
        }

        #[cfg(target_os = "macos")]
        if let Err(unsupported) = config.add_capabilities(FUSE_AUTO_INVAL_DATA_FLAG) {
            warn!(
                unsupported,
                "macFUSE did not advertise FUSE_AUTO_INVAL_DATA; direct I/O remains enabled"
            );
        }

        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        #[cfg(target_os = "macos")]
        let attr_result = self.vfs.lookup(parent, &name_bytes).or_else(|err| {
            if !matches!(
                err,
                VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
            ) || name_bytes.is_ascii()
            {
                return Err(err);
            }
            if let Ok(entries) = self.vfs.readdir(parent) {
                let resolved = resolve_macos_namespace_name(&name_bytes, &entries);
                if resolved.as_ref() != name_bytes.as_slice() {
                    return self.vfs.lookup(parent, resolved.as_ref());
                }
            }
            Err(err)
        });
        #[cfg(not(target_os = "macos"))]
        let attr_result = self.vfs.lookup(parent, &name_bytes);

        match attr_result {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                debug!("lookup({parent}, {:?}) -> {e}", name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.vfs.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid)),
            Err(e) => {
                debug!("getattr({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.mkdir(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!(
                    parent,
                    uid = req.uid(),
                    gid = req.gid(),
                    mode,
                    umask = _umask,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "mkdir -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.create(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!("mknod({parent}, {:?}) -> {e}", name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.create(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => {
                // Open a handle for the newly created file
                let (open_flags, _trunc) = posix_flags_to_open_flags(flags);
                match self.vfs.open_file(attr.ino, open_flags, false) {
                    Ok(fh) => {
                        reply.created(
                            &TTL,
                            &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                            0,
                            fh,
                            fuse_open_reply_flags(open_flags),
                        );
                    }
                    Err(e) => {
                        error!("create open_file({}) -> {e}", attr.ino);
                        reply.error(vfs_err_to_errno(&e));
                    }
                }
            }
            Err(e) => {
                error!(
                    parent,
                    uid = req.uid(),
                    gid = req.gid(),
                    mode,
                    umask = _umask,
                    flags,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "create -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.unlink(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "unlink -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.rmdir(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "rmdir -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let name = os_str_bytes(link_name).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &name) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &name);
            reply.error(libc::ENOENT);
            return;
        }
        let tgt = target.as_os_str().as_bytes().to_vec();
        match self.vfs.symlink(parent, &name, &tgt, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!("symlink({parent}, {:?}) -> {e}", link_name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.vfs.readlink(ino) {
            Ok(target) => reply.data(&target),
            Err(e) => {
                error!("readlink({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let name = os_str_bytes(newname).to_vec();
        if should_hide_non_macos_root_metadata_entry(newparent, &name) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, newparent, &name);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.link(ino, newparent, &name) {
            Ok(attr) => {
                let fattr = to_fuse_attr(&attr, self.mount_uid, self.mount_gid);
                reply.entry(&TTL, &fattr, 0);
            }
            Err(e) => {
                error!("link({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let requested_old_name = os_str_bytes(name).to_vec();
        let requested_new_name = os_str_bytes(newname).to_vec();
        if should_hide_non_macos_root_metadata_entry(parent, &requested_old_name) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, parent, &requested_old_name);
            reply.error(libc::ENOENT);
            return;
        }
        if should_hide_non_macos_root_metadata_entry(newparent, &requested_new_name) {
            heal_hidden_non_macos_root_metadata_entry(&self.vfs, newparent, &requested_new_name);
            reply.error(libc::ENOENT);
            return;
        }
        #[cfg(target_os = "macos")]
        let old_name =
            resolve_macos_existing_mutation_source(&self.vfs, parent, &requested_old_name);
        #[cfg(not(target_os = "macos"))]
        let old_name = requested_old_name.clone();
        #[cfg(target_os = "macos")]
        let new_name =
            resolve_macos_mutation_target_name(&self.vfs, newparent, &requested_new_name);
        #[cfg(not(target_os = "macos"))]
        let new_name = requested_new_name.clone();

        match self.vfs.rename(parent, &old_name, newparent, &new_name) {
            Ok(()) => reply.ok(),
            Err(e) => {
                #[cfg(target_os = "macos")]
                if matches!(
                    e,
                    VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
                ) && let Ok(entries) = self.vfs.readdir(parent)
                    && let Some(recovered_old_name) =
                        recover_macos_truncated_rename_source(&old_name, &entries)
                {
                    debug!(
                        parent,
                        newparent,
                        flags = _flags,
                        old_name = %debug_name(&old_name),
                        recovered_old_name = %debug_name(&recovered_old_name),
                        new_name = %debug_name(&new_name),
                        old_name_bytes = ?old_name,
                        recovered_old_name_bytes = ?recovered_old_name,
                        new_name_bytes = ?new_name,
                        "rename retry with recovered macOS source name"
                    );
                    match self
                        .vfs
                        .rename(parent, &recovered_old_name, newparent, &new_name)
                    {
                        Ok(()) => {
                            reply.ok();
                            return;
                        }
                        Err(retry_err) => {
                            error!(
                                parent,
                                newparent,
                                flags = _flags,
                                old_name = %debug_name(&old_name),
                                recovered_old_name = %debug_name(&recovered_old_name),
                                new_name = %debug_name(&new_name),
                                old_name_bytes = ?old_name,
                                recovered_old_name_bytes = ?recovered_old_name,
                                new_name_bytes = ?new_name,
                                "rename retry -> {retry_err}"
                            );
                            reply.error(vfs_err_to_errno(&retry_err));
                            return;
                        }
                    }
                }
                error!(
                    parent,
                    newparent,
                    flags = _flags,
                    requested_old_name = %debug_name(&requested_old_name),
                    requested_new_name = %debug_name(&requested_new_name),
                    old_name = %debug_name(&old_name),
                    new_name = %debug_name(&new_name),
                    requested_old_name_bytes = ?requested_old_name,
                    requested_new_name_bytes = ?requested_new_name,
                    old_name_bytes = ?old_name,
                    new_name_bytes = ?new_name,
                    "rename -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn exchange(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        options: u64,
        reply: ReplyEmpty,
    ) {
        let old_name = os_str_bytes(name).to_vec();
        let new_name = os_str_bytes(newname).to_vec();
        match macos_exchange_or_rename(&self.vfs, parent, &old_name, newparent, &new_name) {
            Ok(()) => reply.ok(),
            Err(error) => {
                warn!(
                    parent,
                    newparent,
                    options,
                    old_name = %debug_name(&old_name),
                    new_name = %debug_name(&new_name),
                    old_name_bytes = ?old_name,
                    new_name_bytes = ?new_name,
                    "macOS exchange fallback -> {error}"
                );
                reply.error(vfs_err_to_errno(&error));
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        match self.vfs.open_file(ino, open_flags, trunc) {
            Ok(fh) => reply.opened(fh, fuse_open_reply_flags(open_flags)),
            Err(e) => {
                if matches!(e, VfsError::WouldBlock | VfsError::RemoteContentPending) {
                    debug!("open({ino}) -> {e}");
                } else {
                    error!("open({ino}) -> {e}");
                }
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let offset = match to_u64_offset(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let clamped_size = match self.vfs.getattr(ino) {
            Ok(attr) => attr.size.saturating_sub(offset).min(u64::from(size)),
            Err(e) => {
                error!("read getattr({ino}, off={offset}, sz={size}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };
        let clamped_size = match usize::try_from(clamped_size) {
            Ok(clamped_size) => clamped_size,
            Err(_) => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        if clamped_size == 0 {
            reply.data(&[]);
            return;
        }

        // fuser's ReplyData API accepts an immutable reply slice instead of
        // exposing a mutable kernel buffer, so FUSE needs this boundary buffer.
        let mut out = vec![0u8; clamped_size];
        match self
            .rt
            .block_on(self.vfs.read_into_fh(fh, ino, offset, &mut out))
        {
            Ok(n) => {
                let n = match usize::try_from(n) {
                    Ok(n) => n,
                    Err(_) => {
                        reply.error(libc::EOVERFLOW);
                        return;
                    }
                };
                reply.data(&out[..n]);
            }
            Err(e) => {
                error!("read({ino}, off={offset}, sz={size}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let offset = match to_u64_offset(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self
            .rt
            .block_on(self.vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                offset,
                data,
                LocalSyncPolicy::Opportunistic,
            )) {
            Ok(written) => reply.written(written),
            Err(e) => {
                error!("write({ino}, off={offset}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let started = Instant::now();
        debug!(ino, fh, "flush started");
        let result = self.handle_drain().flush(ino, fh);
        match result {
            Ok(()) => {
                debug!(
                    ino,
                    fh,
                    elapsed_ms = started.elapsed().as_millis(),
                    "flush completed"
                );
                reply.ok();
            }
            Err(e) => {
                if fh != 0 {
                    error!("flush_fh({fh}) -> {e}");
                } else {
                    error!("flush({ino}) -> {e}");
                }
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let started = Instant::now();
        debug!(ino, fh, "release started");
        match self.handle_drain().release(ino, fh) {
            Ok(()) => {
                debug!(
                    ino,
                    fh,
                    elapsed_ms = started.elapsed().as_millis(),
                    "release completed"
                );
                reply.ok();
            }
            Err(e) => {
                error!("release({ino}, fh={fh}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let started = Instant::now();
        debug!(ino, fh, datasync, "fsync started");
        let result = if fh != 0 {
            // Handle-backed FUSE fsync follows the same local-durability
            // contract as flush_fh: it persists dirty state locally, waits for
            // any deferred release barrier, and leaves asynchronous upload
            // sticky errors isolated from this callback. Inode-only fsync
            // below is the explicit sticky-error reporting path used when no
            // file handle is available.
            self.rt.block_on(self.vfs.flush_fh(fh))
        } else {
            self.rt.block_on(self.vfs.fsync(ino, datasync))
        };
        match result {
            Ok(()) => {
                debug!(
                    ino,
                    fh,
                    datasync,
                    elapsed_ms = started.elapsed().as_millis(),
                    "fsync completed"
                );
                reply.ok();
            }
            Err(e) => {
                error!("fsync({ino}, fh={fh}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.vfs.readdir(ino) {
            Ok(e) => e,
            Err(e) => {
                error!("readdir({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };

        // Offset-based iteration (offset 0 = ".", 1 = "..", 2+ = entries)
        let mut idx = match clamp_offset_to_usize(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if idx == 0 {
            if reply.add(ino, 1, FuseFileType::Directory, ".") {
                reply.ok();
                return;
            }
            idx = 1;
        }
        if idx == 1 {
            // Parent (simplified: use same ino)
            if reply.add(ino, 2, FuseFileType::Directory, "..") {
                reply.ok();
                return;
            }
            idx = 2;
        }

        for (i, entry) in entries.iter().enumerate() {
            if should_hide_non_macos_root_metadata_entry(ino, &entry.name) {
                heal_hidden_non_macos_root_metadata_entry(&self.vfs, ino, &entry.name);
                continue;
            }
            let entry_idx = i + 2;
            if entry_idx < idx {
                continue;
            }
            let ft = fuse_file_type(entry.kind);
            #[cfg(target_os = "macos")]
            let entry_name = normalize_macos_readdir_name(&entry.name);
            #[cfg(not(target_os = "macos"))]
            let entry_name = entry.name.clone();
            let next_offset = match i64::try_from(entry_idx + 1) {
                Ok(next_offset) => next_offset,
                Err(_) => {
                    reply.error(libc::EOVERFLOW);
                    return;
                }
            };
            if reply.add(entry.ino, next_offset, ft, OsStr::from_bytes(&entry_name)) {
                break;
            }
        }

        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let entries = match self.vfs.readdir(ino) {
            Ok(e) => e,
            Err(e) => {
                error!("readdirplus({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };

        let mut idx = match clamp_offset_to_usize(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        // "." entry
        if idx == 0 {
            let dot_attr = match self.vfs.getattr(ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(e) => {
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            };
            if reply.add(ino, 1, ".", &TTL, &dot_attr, 0) {
                reply.ok();
                return;
            }
            idx = 1;
        }

        // ".." entry
        if idx == 1 {
            let dotdot_attr = match self.vfs.getattr(ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(e) => {
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            };
            if reply.add(ino, 2, "..", &TTL, &dotdot_attr, 0) {
                reply.ok();
                return;
            }
            idx = 2;
        }

        // Regular entries with full attributes
        for (i, entry) in entries.iter().enumerate() {
            if should_hide_non_macos_root_metadata_entry(ino, &entry.name) {
                heal_hidden_non_macos_root_metadata_entry(&self.vfs, ino, &entry.name);
                continue;
            }
            let entry_idx = i + 2;
            if entry_idx < idx {
                continue;
            }
            let next_offset = match i64::try_from(entry_idx + 1) {
                Ok(next_offset) => next_offset,
                Err(_) => {
                    reply.error(libc::EOVERFLOW);
                    return;
                }
            };
            let fattr = match self.vfs.getattr(entry.ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(_) => continue, // skip entries we can't stat
            };
            #[cfg(target_os = "macos")]
            let entry_name = normalize_macos_readdir_name(&entry.name);
            #[cfg(not(target_os = "macos"))]
            let entry_name = entry.name.clone();
            if reply.add(
                entry.ino,
                next_offset,
                OsStr::from_bytes(&entry_name),
                &TTL,
                &fattr,
                0,
            ) {
                break;
            }
        }

        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let args = SetattrArgs {
            size,
            mode: mode.map(normalize_unix_mode),
            uid,
            gid,
            atime: match atime {
                Some(fuser::TimeOrNow::SpecificTime(st)) => system_time_to_timespec(st),
                Some(fuser::TimeOrNow::Now) => TimeSpec::Now,
                None => TimeSpec::Omit,
            },
            mtime: match mtime {
                Some(fuser::TimeOrNow::SpecificTime(st)) => system_time_to_timespec(st),
                Some(fuser::TimeOrNow::Now) => TimeSpec::Now,
                None => TimeSpec::Omit,
            },
            crtime: match _crtime {
                Some(st) => system_time_to_timespec(st),
                None => TimeSpec::Omit,
            },
            win_attrs: None,
        };
        match self.vfs.setattr_with_handle(fh, ino, args) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid)),
            Err(e) => {
                error!(
                    ino,
                    mode = ?mode,
                    uid = ?uid,
                    gid = ?gid,
                    size = ?size,
                    atime = ?atime,
                    mtime = ?mtime,
                    crtime = ?_crtime,
                    chgtime = ?_chgtime,
                    bkuptime = ?_bkuptime,
                    flags = ?_flags,
                    "setattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        match self.vfs.statfs() {
            Ok(st) => reply.statfs(
                st.blocks, st.bfree, st.bavail, st.files, st.ffree, st.bsize, st.namelen, st.frsize,
            ),
            Err(e) => {
                error!("statfs -> {e}");
                // Fallback to hardcoded values on error
                reply.statfs(
                    1024 * 1024 * 1024,
                    512 * 1024 * 1024,
                    512 * 1024 * 1024,
                    1_000_000,
                    500_000,
                    4096,
                    255,
                    4096,
                );
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyXTimes) {
        match self.vfs.getattr(ino) {
            Ok(attr) => {
                let times = normalize_attr_times(&attr);
                let crtime = UNIX_EPOCH + Duration::from_nanos(times.creation_time_unix_nanos);
                let bkuptime = UNIX_EPOCH + Duration::from_nanos(times.mtime_unix_nanos);
                reply.xtimes(bkuptime, crtime);
            }
            Err(e) => {
                error!(ino, "getxtimes -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    // macOS calls access() to determine if writing is allowed.
    // With DefaultPermissions set the kernel already checked mode bits,
    // so we just say OK here.
    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    #[cfg(target_os = "macos")]
    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) {
        // macFUSE issues control-plane ioctls on root/unmount paths during mount-cycle
        // events. We don't currently need custom behavior for them, but we reply with
        // ENOTTY instead of the default warn/error spam so kernel probes do not look
        // like errors in logs.
        debug!(
            ino,
            fh,
            flags,
            cmd,
            in_len = in_data.len(),
            out_size,
            "ioctl not implemented in StudioFS FUSE backend"
        );
        reply.error(libc::ENOTTY);
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let key = name.as_encoded_bytes();

        match self.vfs.get_xattr(ino, key) {
            Ok(Some(val)) => {
                if size == 0 {
                    let data_len = match u32::try_from(val.len()) {
                        Ok(data_len) => data_len,
                        Err(_) => {
                            reply.error(libc::EOVERFLOW);
                            return;
                        }
                    };
                    reply.size(data_len);
                } else if val.len() <= size as usize {
                    reply.data(&val);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Ok(None) => {
                reply.error(missing_xattr_errno());
            }
            Err(e) => {
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    size,
                    "getxattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let key = name.as_encoded_bytes();

        #[cfg(target_os = "macos")]
        let normalized_flags = normalize_macos_xattr_flags(flags);
        #[cfg(not(target_os = "macos"))]
        let normalized_flags = match u32::try_from(flags) {
            Ok(normalized_flags) => normalized_flags,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        #[cfg(target_os = "macos")]
        let value_buf = if _position > 0 {
            match self.vfs.get_xattr(ino, key) {
                Ok(existing) => merge_xattr_write(existing, _position, value),
                Err(e) => {
                    error!(
                        ino,
                        key = %debug_name(key),
                        key_bytes = ?key,
                        position = _position,
                        value_len = value.len(),
                        flags,
                        "setxattr preload -> {e}"
                    );
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            }
        } else {
            value.to_vec()
        };

        #[cfg(not(target_os = "macos"))]
        let value_buf = value.to_vec();

        match self.vfs.set_xattr(ino, key, &value_buf, normalized_flags) {
            Ok(()) => reply.ok(),
            Err(e) => {
                if is_missing_xattr_error(&e) {
                    debug!(
                        ino,
                        key = %debug_name(key),
                        key_bytes = ?key,
                        flags,
                        normalized_flags,
                        position = _position,
                        "setxattr missing xattr"
                    );
                    reply.error(missing_xattr_errno());
                    return;
                }
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    value_len = value.len(),
                    stored_value_len = value_buf.len(),
                    flags,
                    normalized_flags,
                    position = _position,
                    "setxattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn removexattr(&mut self, _req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let key = name.as_encoded_bytes();

        match self.vfs.remove_xattr(ino, key) {
            Ok(()) => reply.ok(),
            Err(e) => {
                if is_missing_xattr_error(&e) {
                    debug!(
                        ino,
                        key = %debug_name(key),
                        key_bytes = ?key,
                        "removexattr missing xattr"
                    );
                    reply.error(missing_xattr_errno());
                    return;
                }
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    "removexattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        match self.vfs.list_xattrs(ino) {
            Ok(names) => {
                // xattr list format: null-terminated name strings concatenated
                let data: Vec<u8> = names
                    .iter()
                    .flat_map(|n| n.iter().cloned().chain(std::iter::once(0u8)))
                    .collect();
                if size == 0 {
                    let data_len = match u32::try_from(data.len()) {
                        Ok(data_len) => data_len,
                        Err(_) => {
                            reply.error(libc::EOVERFLOW);
                            return;
                        }
                    };
                    reply.size(data_len);
                } else if data.len() <= size as usize {
                    reply.data(&data);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Err(e) => reply.error(vfs_err_to_errno(&e)),
        }
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    #[allow(clippy::unnecessary_cast)]
    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: i32,
        _pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        // File-level semantics only (byte ranges ignored)
        let op = if typ == libc::F_RDLCK as i32 {
            LockKind::Shared
        } else if typ == libc::F_WRLCK as i32 {
            LockKind::Exclusive
        } else if typ == libc::F_UNLCK as i32 {
            LockKind::Unlock
        } else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.rt.block_on(self.vfs.flock(ino, op, lock_owner, sleep)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(vfs_err_to_errno(&e)),
        }
    }

    #[allow(clippy::unnecessary_cast)]
    fn getlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        // Report no conflicting locks (single-node simplification)
        reply.locked(start, end, libc::F_UNLCK as i32, 0);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        // For cloud FS, preallocation is a no-op — no local disk to reserve.
        // We only extend the file size if FALLOC_FL_KEEP_SIZE is not set.
        if mode == 0 {
            // FALLOC_FL_KEEP_SIZE not set → extend size if needed
            match self.vfs.getattr(ino) {
                Ok(attr) => {
                    let offset = match to_u64_offset(offset) {
                        Ok(offset) => offset,
                        Err(errno) => {
                            reply.error(errno);
                            return;
                        }
                    };
                    let length = match u64::try_from(length) {
                        Ok(length) => length,
                        Err(_) => {
                            reply.error(libc::EINVAL);
                            return;
                        }
                    };
                    let new_size = match offset.checked_add(length) {
                        Some(new_size) => new_size,
                        None => {
                            reply.error(libc::EINVAL);
                            return;
                        }
                    };
                    if new_size > attr.size {
                        let args = SetattrArgs {
                            size: Some(new_size),
                            ..Default::default()
                        };
                        if let Err(e) = self.vfs.setattr_with_handle(Some(fh), ino, args) {
                            error!("fallocate setattr({ino}) -> {e}");
                            reply.error(vfs_err_to_errno(&e));
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("fallocate getattr({ino}) -> {e}");
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            }
        }
        reply.ok();
    }
}

// ─── Kernel Cache Invalidator ────────────────────────────────────────────────

/// Bridges `fuser::Notifier` to the VFS `KernelInvalidator` trait so that
/// Metadata refreshes can immediately clear the Linux kernel's
/// FUSE dentry and attribute caches.
pub struct FuseKernelInvalidator {
    notifier: fuser::Notifier,
}

impl FuseKernelInvalidator {
    /// Create a kernel invalidator from a live FUSE notifier.
    pub fn new(notifier: fuser::Notifier) -> Self {
        Self { notifier }
    }
}

fn should_log_invalidation_error(err: &std::io::Error) -> bool {
    !matches!(
        err.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotConnected
    )
}

impl KernelInvalidator for FuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        // offset=0, len=-1 invalidates the entire inode (attributes + all
        // cached page data / directory listing).
        if let Err(e) = self.notifier.inval_inode(ino, 0, -1) {
            // ENOENT is harmless (kernel already evicted the entry).
            if should_log_invalidation_error(&e) {
                warn!(ino, error = %e, "FUSE notify_inval_inode failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_FUSE_MAX_READAHEAD_BYTES, StudioFuseFs, TTL, fuse_open_reply_flags,
        is_missing_xattr_error, missing_xattr_errno, normalize_unix_mode,
        posix_flags_to_open_flags, should_log_invalidation_error, to_fuse_attr, vfs_err_to_errno,
    };
    #[cfg(target_os = "macos")]
    use studiofs_meta::engine::{XATTR_CREATE_FLAG, XATTR_REPLACE_FLAG};

    #[cfg(target_os = "macos")]
    use super::{
        macos_exchange_or_rename, merge_xattr_write, normalize_macos_readdir_name,
        normalize_macos_xattr_flags, recover_macos_normalized_name,
        recover_macos_truncated_rename_source, resolve_macos_mutation_target_name,
        resolve_macos_namespace_name,
    };
    use std::sync::Arc;
    use studiofs_data::store::DataStoreConfig;
    use studiofs_meta::InodeAttr;
    use studiofs_meta::MetaError;
    #[cfg(target_os = "macos")]
    use studiofs_meta::schema::DirEntry;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use studiofs_vfs::{OpenFlags, StudioVfs, VfsConfig, VfsError};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;
    #[cfg(target_os = "macos")]
    use unicode_normalization::UnicodeNormalization;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("fuse-barrier-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("fuse-barrier-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    pub(super) fn make_test_fuse_fs(
        volume_suffix: &str,
    ) -> (TempDir, Arc<Runtime>, Arc<StudioVfs>, StudioFuseFs) {
        let tmp = TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
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
        let fs = StudioFuseFs::new(vfs.clone(), rt.handle().clone());
        (tmp, rt, vfs, fs)
    }

    #[test]
    fn normalize_unix_mode_strips_file_type_bits() {
        assert_eq!(normalize_unix_mode(0o40755), 0o755);
        assert_eq!(normalize_unix_mode(0o100644), 0o644);
        assert_eq!(normalize_unix_mode(0o100644 | 0o4000), 0o4644);
    }

    #[test]
    fn default_fuse_max_readahead_matches_platform_expectation() {
        #[cfg(target_os = "macos")]
        assert_eq!(DEFAULT_FUSE_MAX_READAHEAD_BYTES, 1024 * 1024);
        #[cfg(not(target_os = "macos"))]
        assert_eq!(DEFAULT_FUSE_MAX_READAHEAD_BYTES, 16 * 1024 * 1024);
    }

    #[test]
    fn fuse_kernel_cache_ttl_matches_platform_policy() {
        #[cfg(target_os = "macos")]
        assert_eq!(TTL, std::time::Duration::from_secs(0));
        #[cfg(not(target_os = "macos"))]
        assert_eq!(TTL, std::time::Duration::from_secs(1));
    }

    #[test]
    fn fuse_open_reply_flags_match_platform_cache_policy() {
        #[cfg(target_os = "macos")]
        {
            assert_eq!(
                fuse_open_reply_flags(OpenFlags::RDONLY),
                fuser::consts::FOPEN_DIRECT_IO
            );
            assert_eq!(
                fuse_open_reply_flags(OpenFlags::WRONLY),
                fuser::consts::FOPEN_DIRECT_IO
            );
            assert_eq!(
                fuse_open_reply_flags(OpenFlags::RDWR),
                fuser::consts::FOPEN_DIRECT_IO
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(fuse_open_reply_flags(OpenFlags::RDONLY), 0);
            assert_eq!(fuse_open_reply_flags(OpenFlags::WRONLY), 0);
            assert_eq!(fuse_open_reply_flags(OpenFlags::RDWR), 0);
        }
    }

    #[test]
    fn fuse_invalidation_not_found_is_benign() {
        let not_found = std::io::Error::from(std::io::ErrorKind::NotFound);
        let not_connected = std::io::Error::from(std::io::ErrorKind::NotConnected);
        let other = std::io::Error::from(std::io::ErrorKind::PermissionDenied);

        assert!(!should_log_invalidation_error(&not_found));
        assert!(!should_log_invalidation_error(&not_connected));
        assert!(should_log_invalidation_error(&other));
    }

    #[test]
    fn vfs_error_maps_to_expected_errno_codes() {
        let not_found = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "missing".into(),
        });
        assert_eq!(vfs_err_to_errno(&not_found), libc::ENOENT);

        let exists = VfsError::Meta(MetaError::EntryExists {
            parent: 1,
            name: "dup".into(),
        });
        assert_eq!(vfs_err_to_errno(&exists), libc::EEXIST);

        let invalid = VfsError::InvalidName("bad".into());
        assert_eq!(vfs_err_to_errno(&invalid), libc::EINVAL);

        let blocked = VfsError::WouldBlock;
        assert_eq!(vfs_err_to_errno(&blocked), libc::EAGAIN);

        let xattr_too_big = VfsError::Meta(MetaError::XattrValueTooLarge {
            actual: 17,
            limit: 16,
        });
        assert_eq!(vfs_err_to_errno(&xattr_too_big), libc::E2BIG);

        let fragmented = VfsError::Meta(MetaError::NonContiguousRegularFileGraph {
            ino: 7,
            block_index: 3,
        });
        assert_eq!(vfs_err_to_errno(&fragmented), libc::EIO);

        let exhausted = VfsError::Fetch(studiofs_vfs::fetch::FetchError::Exhausted {
            attempts: 3,
            source: studiofs_data::store::DataError::Storage("timeout".into()),
        });
        assert_eq!(vfs_err_to_errno(&exhausted), libc::ETIMEDOUT);
    }

    #[test]
    fn io_permission_denied_maps_to_eacces() {
        let err = VfsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(vfs_err_to_errno(&err), libc::EACCES);
    }

    #[test]
    fn to_fuse_attr_remaps_legacy_root_owner_and_rounds_blocks() {
        let mut attr = InodeAttr::new_file(9, 1, 0, 0, 0o640);
        attr.size = 513;

        let file_attr = to_fuse_attr(&attr, 501, 20);

        assert_eq!(file_attr.uid, 501);
        assert_eq!(file_attr.gid, 20);
        assert_eq!(file_attr.blocks, 2);
        assert_eq!(file_attr.perm, 0o640);
    }

    #[test]
    fn posix_open_flags_translate_access_and_truncate_bits() {
        let (rdonly, trunc) = posix_flags_to_open_flags(libc::O_RDONLY);
        assert!(rdonly.contains(OpenFlags::RDONLY));
        assert!(!trunc);

        let (append, trunc) = posix_flags_to_open_flags(libc::O_WRONLY | libc::O_APPEND);
        assert!(append.contains(OpenFlags::WRONLY));
        assert!(append.contains(OpenFlags::APPEND));
        assert!(!trunc);

        let (rdwr, trunc) = posix_flags_to_open_flags(libc::O_RDWR | libc::O_TRUNC);
        assert!(rdwr.contains(OpenFlags::RDWR));
        assert!(trunc);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn merge_xattr_write_overwrites_from_offset() {
        let merged = merge_xattr_write(Some(b"abcdef".to_vec()), 2, b"ZZ");
        assert_eq!(merged, b"abZZef");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn merge_xattr_write_zero_fills_gap() {
        let merged = merge_xattr_write(None, 3, b"xy");
        assert_eq!(merged, vec![0, 0, 0, b'x', b'y']);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn normalize_macos_xattr_flags_maps_create_and_replace_bits() {
        // macOS passes XATTR_CREATE=0x0002 and XATTR_REPLACE=0x0004.
        // StudioVfs expects CREATE=1 and REPLACE=2.
        assert_eq!(normalize_macos_xattr_flags(0), 0);
        assert_eq!(normalize_macos_xattr_flags(0x0002), XATTR_CREATE_FLAG);
        assert_eq!(normalize_macos_xattr_flags(0x0004), XATTR_REPLACE_FLAG);
        assert_eq!(
            normalize_macos_xattr_flags(0x0002 | 0x0004),
            XATTR_CREATE_FLAG | XATTR_REPLACE_FLAG
        );
        assert_eq!(normalize_macos_xattr_flags(0x0010), 0);
    }

    #[test]
    fn detects_missing_xattr_errors() {
        let missing = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "com.apple.FinderInfo".into(),
        });
        assert!(is_missing_xattr_error(&missing));

        let other = VfsError::Meta(MetaError::Storage("boom".into()));
        assert!(!is_missing_xattr_error(&other));
    }

    #[test]
    fn missing_xattr_errno_matches_platform() {
        #[cfg(target_os = "macos")]
        assert_eq!(missing_xattr_errno(), libc::ENOATTR);
        #[cfg(not(target_os = "macos"))]
        assert_eq!(missing_xattr_errno(), libc::ENODATA);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn hidden_non_macos_root_metadata_entry_matches_trashes_only() {
        assert!(super::should_hide_non_macos_root_metadata_entry(
            studiofs_meta::ROOT_INO,
            b".Trashes"
        ));
        assert!(!super::should_hide_non_macos_root_metadata_entry(
            studiofs_meta::ROOT_INO,
            b".Trash"
        ));
        assert!(!super::should_hide_non_macos_root_metadata_entry(
            studiofs_meta::ROOT_INO + 1,
            b".Trashes"
        ));
        assert!(!super::should_hide_non_macos_root_metadata_entry(
            studiofs_meta::ROOT_INO,
            b"visible"
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_with_unique_suffix_match() {
        let entries = vec![
            DirEntry {
                name: b"Untitled folder".to_vec(),
                ino: 1,
                kind: studiofs_meta::FileType::Directory,
            },
            DirEntry {
                name: b"other".to_vec(),
                ino: 2,
                kind: studiofs_meta::FileType::Directory,
            },
        ];

        let recovered = recover_macos_truncated_rename_source(b" folder", &entries);
        assert_eq!(recovered, Some(b"Untitled folder".to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_with_unique_short_suffix_match() {
        let entries = vec![
            DirEntry {
                name: b"source-alpha.data".to_vec(),
                ino: 1,
                kind: studiofs_meta::FileType::RegularFile,
            },
            DirEntry {
                name: b"source-beta.txt".to_vec(),
                ino: 2,
                kind: studiofs_meta::FileType::RegularFile,
            },
        ];

        let recovered = recover_macos_truncated_rename_source(b".data", &entries);
        assert_eq!(recovered, Some(b"source-alpha.data".to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_allows_long_suffix_after_name_boundary() {
        let entries = vec![
            DirEntry {
                name: b"source-alpha.data".to_vec(),
                ino: 1,
                kind: studiofs_meta::FileType::RegularFile,
            },
            DirEntry {
                name: b"source-beta.txt".to_vec(),
                ino: 2,
                kind: studiofs_meta::FileType::RegularFile,
            },
        ];

        let recovered = recover_macos_truncated_rename_source(b"alpha.data", &entries);
        assert_eq!(recovered, Some(b"source-alpha.data".to_vec()));

        let recovered = recover_macos_truncated_rename_source(b"lpha.data", &entries);
        assert_eq!(recovered, Some(b"source-alpha.data".to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_allows_boundary_alnum_suffix() {
        let entries = vec![
            DirEntry {
                name: b"link-to-target".to_vec(),
                ino: 1,
                kind: studiofs_meta::FileType::Symlink,
            },
            DirEntry {
                name: b"target-v1.txt".to_vec(),
                ino: 2,
                kind: studiofs_meta::FileType::RegularFile,
            },
        ];

        let recovered = recover_macos_truncated_rename_source(b"target", &entries);
        assert_eq!(recovered, Some(b"link-to-target".to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_rejects_short_or_mid_token_suffixes() {
        let entries = vec![DirEntry {
            name: b"prefix-a.txt".to_vec(),
            ino: 1,
            kind: studiofs_meta::FileType::RegularFile,
        }];

        assert_eq!(recover_macos_truncated_rename_source(b"a", &entries), None);
        assert_eq!(
            recover_macos_truncated_rename_source(b"a.txt", &entries),
            None
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_returns_none_when_ambiguous() {
        let entries = vec![
            DirEntry {
                name: b"Untitled folder".to_vec(),
                ino: 1,
                kind: studiofs_meta::FileType::Directory,
            },
            DirEntry {
                name: b"Whatever folder".to_vec(),
                ino: 2,
                kind: studiofs_meta::FileType::Directory,
            },
        ];

        assert_eq!(
            recover_macos_truncated_rename_source(b" folder", &entries),
            None
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_returns_none_without_match() {
        let entries = vec![DirEntry {
            name: b"plain".to_vec(),
            ino: 1,
            kind: studiofs_meta::FileType::Directory,
        }];

        assert_eq!(
            recover_macos_truncated_rename_source(b" folder", &entries),
            None
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_recovers_empty_source_from_single_parent_entry() {
        let entries = vec![DirEntry {
            name: b"file.txt".to_vec(),
            ino: 1,
            kind: studiofs_meta::FileType::RegularFile,
        }];

        let recovered = recover_macos_truncated_rename_source(b"", &entries);

        assert_eq!(recovered, Some(b"file.txt".to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_rename_source_ignores_dot_entries_for_empty_source() {
        let entries = vec![
            DirEntry {
                name: b".".to_vec(),
                ino: 1,
                kind: studiofs_meta::FileType::Directory,
            },
            DirEntry {
                name: b"..".to_vec(),
                ino: 2,
                kind: studiofs_meta::FileType::Directory,
            },
            DirEntry {
                name: b"file.txt".to_vec(),
                ino: 3,
                kind: studiofs_meta::FileType::RegularFile,
            },
        ];

        let recovered = recover_macos_truncated_rename_source(b"", &entries);

        assert_eq!(recovered, Some(b"file.txt".to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_exchange_falls_back_to_rename_when_target_is_missing() {
        let (_tmp, _rt, vfs, _fs) = make_test_fuse_fs("macos-exchange-rename");
        let parent = vfs
            .mkdir(studiofs_meta::ROOT_INO, b"scan", 0, 0, 0o755)
            .expect("create scan dir");
        let source = vfs
            .create(parent.ino, b"a.txt", 0, 0, 0o644)
            .expect("create exchange source");

        macos_exchange_or_rename(&vfs, parent.ino, b"a.txt", parent.ino, b"c.txt")
            .expect("macOS exchange with a missing target should behave like rename");

        assert!(
            matches!(
                vfs.lookup(parent.ino, b"a.txt"),
                Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
            ),
            "source dentry must be removed"
        );
        let renamed = vfs
            .lookup(parent.ino, b"c.txt")
            .expect("target dentry should exist");
        assert_eq!(renamed.ino, source.ino);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_exchange_reports_unsupported_when_target_exists() {
        let (_tmp, _rt, vfs, _fs) = make_test_fuse_fs("macos-exchange-existing-target");
        let parent = vfs
            .mkdir(studiofs_meta::ROOT_INO, b"scan", 0, 0, 0o755)
            .expect("create scan dir");
        let source = vfs
            .create(parent.ino, b"a.txt", 0, 0, 0o644)
            .expect("create exchange source");
        let target = vfs
            .create(parent.ino, b"c.txt", 0, 0, 0o644)
            .expect("create exchange target");

        let error = macos_exchange_or_rename(&vfs, parent.ino, b"a.txt", parent.ino, b"c.txt")
            .expect_err("macOS exchange with an existing target should stay unsupported");

        match error {
            VfsError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::ENOSYS)),
            other => panic!("expected raw ENOSYS I/O error, got {other:?}"),
        }
        assert_eq!(vfs.lookup(parent.ino, b"a.txt").unwrap().ino, source.ino);
        assert_eq!(vfs.lookup(parent.ino, b"c.txt").unwrap().ino, target.ino);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn resolve_macos_mutation_target_name_uses_direct_normalized_lookup() {
        let (_tmp, _rt, vfs, _fs) = make_test_fuse_fs("macos-target-normalized-lookup");
        let parent = vfs
            .mkdir(studiofs_meta::ROOT_INO, b"scan", 0, 0, 0o755)
            .expect("create scan dir");
        let latin_nfc = "café";
        let latin_nfd = latin_nfc.nfd().collect::<String>();
        vfs.create(parent.ino, latin_nfc.as_bytes(), 0, 0, 0o644)
            .expect("create NFC target");

        let resolved = resolve_macos_mutation_target_name(&vfs, parent.ino, latin_nfd.as_bytes());

        assert_eq!(resolved.as_ref(), latin_nfc.as_bytes());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn resolve_macos_mutation_target_name_borrows_missing_non_ascii_target() {
        let (_tmp, _rt, vfs, _fs) = make_test_fuse_fs("macos-target-missing-borrowed");
        let parent = vfs
            .mkdir(studiofs_meta::ROOT_INO, b"scan", 0, 0, 0o755)
            .expect("create scan dir");
        let missing = "новый".as_bytes();

        let resolved = resolve_macos_mutation_target_name(&vfs, parent.ino, missing);

        assert!(
            matches!(resolved, std::borrow::Cow::Borrowed(_)),
            "missing target should avoid an owned recovery path"
        );
        assert_eq!(resolved.as_ref(), missing);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_normalized_name_maps_nfd_lookup_to_nfc_entry() {
        let latin_nfc = "café";
        let latin_nfd = latin_nfc.nfd().collect::<String>();
        let entries = vec![DirEntry {
            name: latin_nfc.as_bytes().to_vec(),
            ino: 10,
            kind: studiofs_meta::FileType::RegularFile,
        }];

        let recovered = recover_macos_normalized_name(latin_nfd.as_bytes(), &entries);
        assert_eq!(recovered, Some(latin_nfc.as_bytes().to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn recover_macos_normalized_name_maps_nfc_lookup_to_nfd_entry() {
        let cyrillic_nfc = "Йога";
        let cyrillic_nfd = cyrillic_nfc.nfd().collect::<String>();
        let entries = vec![DirEntry {
            name: cyrillic_nfd.as_bytes().to_vec(),
            ino: 11,
            kind: studiofs_meta::FileType::Directory,
        }];

        let recovered = recover_macos_normalized_name(cyrillic_nfc.as_bytes(), &entries);
        assert_eq!(recovered, Some(cyrillic_nfd.as_bytes().to_vec()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn normalize_macos_readdir_name_returns_nfd_for_finder_compatibility() {
        let latin_nfc = "café";
        let cyrillic_nfc = "Йога";
        let latin_nfd = latin_nfc.nfd().collect::<String>();
        let cyrillic_nfd = cyrillic_nfc.nfd().collect::<String>();

        assert_eq!(
            normalize_macos_readdir_name(latin_nfc.as_bytes()),
            latin_nfd.as_bytes()
        );
        assert_eq!(
            normalize_macos_readdir_name(cyrillic_nfc.as_bytes()),
            cyrillic_nfd.as_bytes()
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn resolve_macos_namespace_name_supports_top_level_rename_nfc_nfd_roundtrip() {
        let latin_nfc = "café";
        let latin_nfd = latin_nfc.nfd().collect::<String>();

        let source_entries = vec![DirEntry {
            name: latin_nfc.as_bytes().to_vec(),
            ino: 100,
            kind: studiofs_meta::FileType::RegularFile,
        }];
        let target_entries = vec![DirEntry {
            name: latin_nfd.as_bytes().to_vec(),
            ino: 101,
            kind: studiofs_meta::FileType::RegularFile,
        }];

        let old_for_nfd_request =
            resolve_macos_namespace_name(latin_nfd.as_bytes(), &source_entries);
        let new_for_nfc_request =
            resolve_macos_namespace_name(latin_nfc.as_bytes(), &target_entries);
        assert_eq!(old_for_nfd_request, latin_nfc.as_bytes());
        assert_eq!(new_for_nfc_request, latin_nfd.as_bytes());

        let old_for_nfc_request =
            resolve_macos_namespace_name(latin_nfc.as_bytes(), &target_entries);
        let new_for_nfd_request =
            resolve_macos_namespace_name(latin_nfd.as_bytes(), &source_entries);
        assert_eq!(old_for_nfc_request, latin_nfd.as_bytes());
        assert_eq!(new_for_nfd_request, latin_nfc.as_bytes());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn resolve_macos_namespace_name_supports_nested_directory_rename_with_cyrillic() {
        let cyrillic_nfc = "Йога";
        let cyrillic_nfd = cyrillic_nfc.nfd().collect::<String>();

        let nested_parent_entries = vec![DirEntry {
            name: cyrillic_nfd.as_bytes().to_vec(),
            ino: 201,
            kind: studiofs_meta::FileType::Directory,
        }];
        let nested_target_entries = vec![DirEntry {
            name: cyrillic_nfc.as_bytes().to_vec(),
            ino: 202,
            kind: studiofs_meta::FileType::Directory,
        }];

        let old_for_nfc_request =
            resolve_macos_namespace_name(cyrillic_nfc.as_bytes(), &nested_parent_entries);
        let new_for_nfd_request =
            resolve_macos_namespace_name(cyrillic_nfd.as_bytes(), &nested_target_entries);

        assert_eq!(old_for_nfc_request, cyrillic_nfd.as_bytes());
        assert_eq!(new_for_nfd_request, cyrillic_nfc.as_bytes());
    }
}
