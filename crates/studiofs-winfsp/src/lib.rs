//! StudioFS WinFSP backend for Windows.
//!
//! Implements `winfsp::filesystem::FileSystemContext` to expose the VFS
//! as a Windows drive letter via the WinFSP (Windows File System Proxy) driver.

#![cfg(windows)]

mod context;

use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::OnceLock;

pub use context::{StudioFsContext, WinfspKernelInvalidator};
use thiserror::Error;
use windows::Win32::System::LibraryLoader::LoadLibraryW;

const DEFAULT_FILE_INFO_TIMEOUT_MS: u32 = u32::MAX;
const DEFAULT_DIR_INFO_TIMEOUT_MS: u32 = 1000;
const DEFAULT_VOLUME_INFO_TIMEOUT_MS: u32 = 1000;
const DEFAULT_SECTOR_SIZE: u16 = 4096;
const DEFAULT_ALLOCATION_UNIT_SIZE: u32 = 1024 * 1024;
const DEFAULT_MAX_DISPATCHER_THREADS: u32 = 16;
const DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE: bool = true;
const DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP: bool = false;
const DEFAULT_PERSISTENT_ACLS: bool = false;
const WIN32_FALSE: i32 = 0;
const WIN32_TRUE: i32 = 1;
const DEFAULT_SECTORS_PER_ALLOCATION_UNIT: u16 =
    (DEFAULT_ALLOCATION_UNIT_SIZE / DEFAULT_SECTOR_SIZE as u32) as u16;
static WINFSP_RUNTIME_DLL_LOAD: OnceLock<Result<(), String>> = OnceLock::new();
const _: () = {
    assert!(DEFAULT_SECTOR_SIZE > 0);
    assert!(DEFAULT_ALLOCATION_UNIT_SIZE > 0);
    assert!(
        (DEFAULT_ALLOCATION_UNIT_SIZE / DEFAULT_SECTOR_SIZE as u32) * DEFAULT_SECTOR_SIZE as u32
            == DEFAULT_ALLOCATION_UNIT_SIZE
    );
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct WinfspMountOptions {
    volume_label: String,
    file_info_timeout_ms: u32,
    dir_info_timeout_ms: u32,
    volume_info_timeout_ms: u32,
    sector_size: u16,
    sectors_per_allocation_unit: u16,
    dispatcher_threads: u32,
    allow_open_in_kernel_mode: bool,
    flush_and_purge_on_cleanup: bool,
    persistent_acls: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum MountOptionError {
    #[error("volume label cannot be empty")]
    EmptyVolumeLabel,
    #[error("volume label cannot contain NUL")]
    VolumeLabelContainsNul,
    #[error("invalid mount point '{0}': WinFSP requires a drive letter (for example `Z:`)")]
    InvalidDriveMountPoint(String),
}

/// Parameters required to mount StudioFS via WinFSP on Windows.
pub struct WinfspMountRequest {
    /// VFS instance that will be exposed via the WinFSP mount.
    pub vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    /// Tokio runtime handle used by the WinFSP backend for async work.
    pub runtime_handle: tokio::runtime::Handle,
    /// Target mount point (drive root), for example `Z:`.
    pub mount_point: String,
    /// Volume label reported to Windows for the mounted filesystem.
    pub volume_label: String,
    /// Whether the mount should be read-only.
    pub read_only: bool,
    /// Optional named shutdown event used to trigger a graceful unmount/shutdown.
    pub shutdown_event_name: Option<String>,
}

fn winfsp_dll_stem_for_arch(target_arch: &str) -> Option<&'static str> {
    match target_arch {
        "x86_64" => Some("winfsp-x64"),
        "x86" => Some("winfsp-x86"),
        "aarch64" => Some("winfsp-a64"),
        _ => None,
    }
}

fn winfsp_dll_name() -> Result<&'static str, String> {
    winfsp_dll_stem_for_arch(std::env::consts::ARCH).ok_or_else(|| {
        format!(
            "unsupported Windows architecture '{}' for WinFSP runtime",
            std::env::consts::ARCH
        )
    })
}

fn push_candidate_path(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths.iter().any(|path| path == &candidate) {
        paths.push(candidate);
    }
}

fn winfsp_dll_candidate_paths(dll_name: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        push_candidate_path(&mut candidates, parent.join(dll_name));
    }

    if let Some(program_files_x86) = std::env::var_os("ProgramFiles(x86)") {
        push_candidate_path(
            &mut candidates,
            PathBuf::from(program_files_x86)
                .join("WinFsp")
                .join("bin")
                .join(dll_name),
        );
    }

    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        push_candidate_path(
            &mut candidates,
            PathBuf::from(program_files)
                .join("WinFsp")
                .join("bin")
                .join(dll_name),
        );
    }

    candidates
}

fn load_winfsp_runtime_dll() -> Result<(), String> {
    let dll_name = format!("{}.dll", winfsp_dll_name()?);
    let candidates = winfsp_dll_candidate_paths(&dll_name);
    let mut last_error = None;

    for candidate in &candidates {
        if !candidate.is_file() {
            continue;
        }

        let wide_path: Vec<u16> = candidate
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let load_result = unsafe { LoadLibraryW(windows::core::PCWSTR(wide_path.as_ptr())) };

        match load_result {
            Ok(_) => return Ok(()),
            Err(err) => last_error = Some(format!("{}: {err}", candidate.display())),
        }
    }

    let searched_paths = candidates
        .iter()
        .map(|candidate| candidate.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = last_error
        .map(|err| format!(" Last LoadLibraryW error: {err}."))
        .unwrap_or_default();
    Err(format!(
        "failed to preload WinFSP runtime DLL '{dll_name}'. Searched: [{searched_paths}]. Install WinFSP or add its bin directory to PATH.{suffix}"
    ))
}

pub(crate) fn ensure_winfsp_runtime_loaded() -> std::io::Result<()> {
    WINFSP_RUNTIME_DLL_LOAD
        .get_or_init(load_winfsp_runtime_dll)
        .as_ref()
        .map(|_| ())
        .map_err(|message| std::io::Error::other(message.clone()))
}

pub(crate) fn ensure_winfsp_runtime_loaded_or_panic() {
    if let Err(err) = ensure_winfsp_runtime_loaded() {
        panic!("WinFSP runtime preload failed before using WinFSP FFI types: {err}");
    }
}

fn parse_mount_options(volume_label: &str) -> Result<WinfspMountOptions, MountOptionError> {
    let trimmed = volume_label.trim();
    if trimmed.is_empty() {
        return Err(MountOptionError::EmptyVolumeLabel);
    }
    if trimmed.contains('\0') {
        return Err(MountOptionError::VolumeLabelContainsNul);
    }

    let file_info_timeout_ms = parse_timeout_env(
        "STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS",
        DEFAULT_FILE_INFO_TIMEOUT_MS,
    );
    let dir_info_timeout_ms = parse_timeout_env(
        "STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS",
        DEFAULT_DIR_INFO_TIMEOUT_MS,
    );
    let volume_info_timeout_ms = parse_timeout_env(
        "STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS",
        DEFAULT_VOLUME_INFO_TIMEOUT_MS,
    );
    let (sector_size, sectors_per_allocation_unit) = parse_geometry_env();
    let dispatcher_threads = default_dispatcher_threads();
    let allow_open_in_kernel_mode = parse_bool_env(
        "STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE",
        DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE,
    );
    let flush_and_purge_on_cleanup = parse_bool_env(
        "STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP",
        DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP,
    );
    let persistent_acls =
        parse_bool_env("STUDIOFS_WINFSP_PERSISTENT_ACLS", DEFAULT_PERSISTENT_ACLS);

    Ok(WinfspMountOptions {
        volume_label: trimmed.to_string(),
        file_info_timeout_ms,
        dir_info_timeout_ms,
        volume_info_timeout_ms,
        sector_size,
        sectors_per_allocation_unit,
        dispatcher_threads,
        allow_open_in_kernel_mode,
        flush_and_purge_on_cleanup,
        persistent_acls,
    })
}

fn default_dispatcher_threads() -> u32 {
    let cpus = std::thread::available_parallelism()
        .map(|count| count.get() as u32)
        .unwrap_or(1);
    cpus.saturating_mul(2)
        .clamp(1, DEFAULT_MAX_DISPATCHER_THREADS)
}

fn parse_geometry_env() -> (u16, u16) {
    let sector_size_raw =
        parse_positive_u32_env("STUDIOFS_WINFSP_SECTOR_SIZE", DEFAULT_SECTOR_SIZE as u32);
    let allocation_unit_raw = parse_positive_u32_env(
        "STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE",
        DEFAULT_ALLOCATION_UNIT_SIZE,
    );

    let sector_size = match u16::try_from(sector_size_raw) {
        Ok(value) => value,
        Err(_) => {
            tracing::warn!(
                env_key = "STUDIOFS_WINFSP_SECTOR_SIZE",
                env_value = sector_size_raw,
                fallback_value = DEFAULT_SECTOR_SIZE,
                "WinFSP sector size exceeds u16; using default"
            );
            DEFAULT_SECTOR_SIZE
        }
    };
    if sector_size < 512 || !sector_size.is_power_of_two() {
        tracing::warn!(
            env_key = "STUDIOFS_WINFSP_SECTOR_SIZE",
            env_value = sector_size,
            fallback_value = DEFAULT_SECTOR_SIZE,
            "WinFSP sector size must be a power-of-two and at least 512 bytes; using default"
        );
        return (DEFAULT_SECTOR_SIZE, DEFAULT_SECTORS_PER_ALLOCATION_UNIT);
    }

    if !allocation_unit_raw.is_multiple_of(u32::from(sector_size)) {
        tracing::warn!(
            env_key = "STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE",
            env_value = allocation_unit_raw,
            sector_size,
            fallback_value = DEFAULT_ALLOCATION_UNIT_SIZE,
            "WinFSP allocation unit must be a multiple of sector size; using default"
        );
        return (DEFAULT_SECTOR_SIZE, DEFAULT_SECTORS_PER_ALLOCATION_UNIT);
    }

    let sectors = allocation_unit_raw / u32::from(sector_size);
    if sectors == 0 || sectors > u32::from(u16::MAX) {
        tracing::warn!(
            env_key = "STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE",
            env_value = allocation_unit_raw,
            sector_size,
            fallback_value = DEFAULT_ALLOCATION_UNIT_SIZE,
            "WinFSP sectors-per-allocation-unit out of range; using default"
        );
        return (DEFAULT_SECTOR_SIZE, DEFAULT_SECTORS_PER_ALLOCATION_UNIT);
    }

    let sectors = match u16::try_from(sectors) {
        Ok(value) => value,
        Err(_) => {
            tracing::warn!(
                env_key = "STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE",
                env_value = allocation_unit_raw,
                sector_size,
                fallback_value = DEFAULT_ALLOCATION_UNIT_SIZE,
                "WinFSP sectors-per-allocation-unit overflowed u16; using default"
            );
            return (DEFAULT_SECTOR_SIZE, DEFAULT_SECTORS_PER_ALLOCATION_UNIT);
        }
    };

    (sector_size, sectors)
}

fn mount_option_env_var(key: &str) -> Option<String> {
    #[cfg(test)]
    if let Some(value) = test_mount_option_env_var(key) {
        return value;
    }
    std::env::var(key).ok()
}

#[cfg(test)]
thread_local! {
    static TEST_MOUNT_OPTION_ENV: std::cell::RefCell<std::collections::HashMap<&'static str, Option<String>>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

#[cfg(test)]
fn test_mount_option_env_var(key: &str) -> Option<Option<String>> {
    TEST_MOUNT_OPTION_ENV.with(|env| env.borrow().get(key).cloned())
}

#[cfg(test)]
fn set_test_mount_option_env(key: &'static str, value: impl Into<String>) {
    TEST_MOUNT_OPTION_ENV.with(|env| {
        env.borrow_mut().insert(key, Some(value.into()));
    });
}

#[cfg(test)]
fn remove_test_mount_option_env(key: &'static str) {
    TEST_MOUNT_OPTION_ENV.with(|env| {
        env.borrow_mut().insert(key, None);
    });
}

fn parse_positive_u32_env(key: &str, default: u32) -> u32 {
    match mount_option_env_var(key) {
        Some(raw) => {
            let trimmed = raw.trim();
            match trimmed.parse::<u32>() {
                Ok(value) if value > 0 => value,
                Ok(_) => {
                    tracing::warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_value = default,
                        "WinFSP tuning override must be > 0; using default"
                    );
                    default
                }
                Err(error) => {
                    tracing::warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_value = default,
                        error = %error,
                        "invalid WinFSP tuning override; using default"
                    );
                    default
                }
            }
        }
        None => default,
    }
}

fn parse_timeout_env(key: &str, default: u32) -> u32 {
    match mount_option_env_var(key) {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.eq_ignore_ascii_case("-1") || trimmed.eq_ignore_ascii_case("max") {
                return u32::MAX;
            }
            match trimmed.parse::<u32>() {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_timeout_ms = default,
                        error = %error,
                        "invalid WinFSP timeout override; using default"
                    );
                    default
                }
            }
        }
        None => default,
    }
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    match mount_option_env_var(key) {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.eq_ignore_ascii_case("1")
                || trimmed.eq_ignore_ascii_case("true")
                || trimmed.eq_ignore_ascii_case("yes")
                || trimmed.eq_ignore_ascii_case("on")
            {
                return true;
            }
            if trimmed.eq_ignore_ascii_case("0")
                || trimmed.eq_ignore_ascii_case("false")
                || trimmed.eq_ignore_ascii_case("no")
                || trimmed.eq_ignore_ascii_case("off")
            {
                return false;
            }
            tracing::warn!(
                env_key = key,
                env_value = trimmed,
                fallback_value = default,
                "invalid WinFSP boolean override; using default"
            );
            default
        }
        None => default,
    }
}

fn apply_volume_params(
    volume_params: &mut winfsp::host::VolumeParams,
    options: &WinfspMountOptions,
    mount_point: &str,
) -> Result<(), MountOptionError> {
    let _ = canonical_drive_mount_point(mount_point)?;

    volume_params.filesystem_name("StudioFS");
    // Intentionally avoid setting `Prefix` so WinFSP uses local disk-device
    // semantics for drive-letter mounts (not network redirector mode).
    volume_params.sector_size(options.sector_size);
    volume_params.sectors_per_allocation_unit(options.sectors_per_allocation_unit);
    volume_params.file_info_timeout(options.file_info_timeout_ms);
    volume_params.dir_info_timeout(options.dir_info_timeout_ms);
    volume_params.volume_info_timeout(options.volume_info_timeout_ms);
    // Keep WinFSP metadata timeouts configurable while regular file data handles
    // opt out of the OS file cache at Create/Open time. The VFS owns payload
    // caching and cross-client metadata invalidation; WinFSP file data caching
    // can otherwise retain same-size remote overwrites after notifications.
    volume_params.flush_and_purge_on_cleanup(options.flush_and_purge_on_cleanup);
    Ok(())
}

fn is_drive_root_mount_point(mount_point: &str) -> bool {
    let bytes = mount_point.as_bytes();
    if bytes.len() < 2 {
        return false;
    }

    if !(bytes[0].is_ascii_alphabetic() && bytes[1] == b':') {
        return false;
    }

    if bytes.len() == 2 {
        return true;
    }

    if bytes.len() == 3 && (bytes[2] == b'\\' || bytes[2] == b'/') {
        return true;
    }

    false
}

fn canonical_drive_mount_point(mount_point: &str) -> Result<String, MountOptionError> {
    if !is_drive_root_mount_point(mount_point) {
        return Err(MountOptionError::InvalidDriveMountPoint(
            mount_point.to_string(),
        ));
    }

    let letter = mount_point
        .chars()
        .next()
        .ok_or_else(|| MountOptionError::InvalidDriveMountPoint(mount_point.to_string()))?
        .to_ascii_uppercase();
    Ok(format!("{letter}:"))
}

fn mount_manager_mount_point(mount_point: &str) -> Result<String, MountOptionError> {
    let drive = canonical_drive_mount_point(mount_point)?;
    Ok(format!(r"\\.\{drive}"))
}

fn invalid_input_io_error(err: MountOptionError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
}

trait WinfspShutdownHost {
    fn stop_for_shutdown(&mut self);
    #[cfg(test)]
    fn unmount_after_shutdown(&mut self);
}

impl<T: winfsp::filesystem::FileSystemContext> WinfspShutdownHost
    for winfsp::host::FileSystemHost<T>
{
    fn stop_for_shutdown(&mut self) {
        self.stop();
    }

    #[cfg(test)]
    fn unmount_after_shutdown(&mut self) {
        self.unmount();
    }
}

#[cfg(test)]
struct WinfspUnmountGuard<'a, H: WinfspShutdownHost + ?Sized> {
    host: Option<&'a mut H>,
}

#[cfg(test)]
impl<'a, H: WinfspShutdownHost + ?Sized> WinfspUnmountGuard<'a, H> {
    fn new(host: &'a mut H) -> Self {
        Self { host: Some(host) }
    }

    fn unmount_now(mut self) {
        self.unmount_if_needed();
    }

    fn unmount_if_needed(&mut self) {
        if let Some(host) = self.host.take() {
            host.unmount_after_shutdown();
        }
    }
}

#[cfg(test)]
impl<H: WinfspShutdownHost + ?Sized> Drop for WinfspUnmountGuard<'_, H> {
    fn drop(&mut self) {
        self.unmount_if_needed();
    }
}

#[cfg(test)]
fn stop_host_before_vfs_shutdown(host: &mut impl WinfspShutdownHost, shutdown_vfs: impl FnOnce()) {
    host.stop_for_shutdown();
    let unmount_guard = WinfspUnmountGuard::new(host);
    shutdown_vfs();
    unmount_guard.unmount_now();
}

#[cfg(test)]
fn stop_host_before_vfs_shutdown_and_process_exit(
    mut host: impl WinfspShutdownHost,
    shutdown_vfs: impl FnOnce(),
) {
    let shutdown_started = stop_host_and_shutdown_vfs(&mut host, shutdown_vfs);
    tracing::info!(
        total_elapsed_ms = shutdown_started.elapsed().as_millis(),
        "WinFSP shutdown: dropping host"
    );
    drop(host);
    tracing::info!(
        total_elapsed_ms = shutdown_started.elapsed().as_millis(),
        "WinFSP shutdown: host dropped"
    );
}

#[cfg(not(test))]
fn stop_host_before_vfs_shutdown_and_process_exit<H>(mut host: H, shutdown_vfs: impl FnOnce())
where
    H: WinfspShutdownHost + Send + 'static,
{
    let shutdown_started = stop_host_and_shutdown_vfs(&mut host, shutdown_vfs);
    drop_host_before_process_exit(host, shutdown_started);
}

fn stop_host_and_shutdown_vfs(
    host: &mut impl WinfspShutdownHost,
    shutdown_vfs: impl FnOnce(),
) -> std::time::Instant {
    let shutdown_started = std::time::Instant::now();
    tracing::info!("WinFSP shutdown: stopping host before VFS shutdown");
    let stop_started = std::time::Instant::now();
    host.stop_for_shutdown();
    tracing::info!(
        elapsed_ms = stop_started.elapsed().as_millis(),
        total_elapsed_ms = shutdown_started.elapsed().as_millis(),
        "WinFSP shutdown: host stop returned"
    );
    let vfs_shutdown_started = std::time::Instant::now();
    tracing::info!("WinFSP shutdown: running VFS shutdown");
    shutdown_vfs();
    tracing::info!(
        elapsed_ms = vfs_shutdown_started.elapsed().as_millis(),
        total_elapsed_ms = shutdown_started.elapsed().as_millis(),
        "WinFSP shutdown: VFS shutdown returned"
    );
    shutdown_started
}

#[cfg(not(test))]
fn drop_host_before_process_exit<H>(host: H, shutdown_started: std::time::Instant)
where
    H: WinfspShutdownHost + Send + 'static,
{
    let drop_started = std::time::Instant::now();
    let timeout = winfsp_host_drop_timeout();
    tracing::info!(
        timeout_ms = timeout.as_millis(),
        total_elapsed_ms = shutdown_started.elapsed().as_millis(),
        "WinFSP shutdown: dropping host"
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let spawn_result = std::thread::Builder::new()
        .name("winfsp-host-drop".to_string())
        .spawn(move || {
            drop(host);
            let _ = tx.send(());
        });

    let Ok(handle) = spawn_result else {
        tracing::warn!(
            total_elapsed_ms = shutdown_started.elapsed().as_millis(),
            "WinFSP shutdown: failed to spawn host drop thread; exiting mount process"
        );
        terminate_current_process(0);
    };

    match rx.recv_timeout(timeout) {
        Ok(()) => {
            let _ = handle.join();
            tracing::info!(
                elapsed_ms = drop_started.elapsed().as_millis(),
                total_elapsed_ms = shutdown_started.elapsed().as_millis(),
                "WinFSP shutdown: host dropped"
            );
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            tracing::warn!(
                timeout_ms = timeout.as_millis(),
                total_elapsed_ms = shutdown_started.elapsed().as_millis(),
                "WinFSP shutdown: host drop timed out; exiting mount process"
            );
            terminate_current_process(0);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            tracing::warn!(
                total_elapsed_ms = shutdown_started.elapsed().as_millis(),
                "WinFSP shutdown: host drop thread ended before completion signal"
            );
            terminate_current_process(1);
        }
    }
}

#[cfg(all(windows, not(test)))]
fn terminate_current_process(exit_code: u32) -> ! {
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn TerminateProcess(hProcess: *mut core::ffi::c_void, uExitCode: u32) -> i32;
    }

    // SAFETY: `GetCurrentProcess` returns the documented pseudo-handle for this
    // process. Passing it to `TerminateProcess` is the Windows-supported hard
    // termination path and avoids `ExitProcess` DLL detach work that can wedge
    // while WinFSP host teardown is already stuck.
    unsafe {
        TerminateProcess(GetCurrentProcess(), exit_code);
    }
    std::process::exit(exit_code as i32)
}

#[cfg(all(not(windows), not(test)))]
fn terminate_current_process(exit_code: u32) -> ! {
    std::process::exit(exit_code as i32)
}

#[cfg(not(test))]
fn winfsp_host_drop_timeout() -> std::time::Duration {
    const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    std::env::var("STUDIOFS_WINFSP_HOST_DROP_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(std::time::Duration::from_millis)
        .unwrap_or(DEFAULT_TIMEOUT)
}

/// Start the WinFSP filesystem, mount, and block until unmounted.
///
/// `shutdown_event_name` — if `Some`, a Win32 named event is created so that an
/// external process (`studiofs umount`) can signal a graceful shutdown.
pub fn mount_and_serve(request: WinfspMountRequest) -> std::io::Result<()> {
    use studiofs_meta::Ino;
    use winfsp::host::{FileSystemHost, FileSystemParams, VolumeParams};

    use crate::context::PreparedWinfspNotifications;

    ensure_winfsp_runtime_loaded()?;

    let WinfspMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_label,
        read_only,
        shutdown_event_name,
    } = request;

    let options = parse_mount_options(&volume_label).map_err(invalid_input_io_error)?;
    let mut volume_params = VolumeParams::new();
    volume_params.case_sensitive_search(true);
    volume_params.case_preserved_names(true);
    volume_params.unicode_on_disk(true);
    volume_params.persistent_acls(options.persistent_acls);
    volume_params.reparse_points(true);
    volume_params.named_streams(false);
    volume_params.read_only_volume(read_only);
    volume_params.post_cleanup_when_modified_only(true);
    volume_params.allow_open_in_kernel_mode(options.allow_open_in_kernel_mode);
    volume_params.max_component_length(255);
    volume_params.volume_serial_number(0x5354_4644); // "STFD"
    // WinFSP layer tuning:
    // - per-timeout environment overrides are supported for runtime tuning.
    // - local-drive geometry is explicit for tooling that queries sector info.
    apply_volume_params(&mut volume_params, &options, &mount_point)
        .map_err(invalid_input_io_error)?;

    // Shared invalidation queue: the `WinfspKernelInvalidator` pushes inode
    // IDs from metadata refresh events; the WinFSP timer drains and sends OS notifications.
    let invalidation_queue = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<Ino>::new()));

    let context = StudioFsContext::new(
        vfs.clone(),
        runtime_handle.clone(),
        invalidation_queue.clone(),
    )
    .map_err(|err| std::io::Error::other(format!("failed to initialize WinFSP context: {err}")))?;

    // Use `new_with_timer_async` so read/write/readdir can return
    // STATUS_PENDING and complete from the Tokio runtime without parking
    // WinFSP dispatcher threads behind local cache I/O. The timer still
    // periodically calls
    // our `NotifyingFileSystemContext` impl, which sends `FspFileSystemNotify`
    // for metadata-driven invalidations. The const generic (250) is the
    // timer interval in milliseconds — short enough for snappy Explorer
    // refreshes, long enough to batch rapid sequences.
    let mut host = FileSystemHost::new_with_timer_async::<PreparedWinfspNotifications, 250>(
        FileSystemParams::default_params(volume_params),
        context,
    )
    .map_err(|err| std::io::Error::other(format!("failed to initialize WinFSP host: {err}")))?;
    let mount_manager_path =
        mount_manager_mount_point(&mount_point).map_err(invalid_input_io_error)?;
    host.mount(&mount_manager_path).map_err(|err| {
        std::io::Error::other(format!(
            "failed to mount '{mount_point}' via WinFSP Mount Manager path '{mount_manager_path}': {err}. \
this usually means Administrator privileges are required, or WinFSP must be configured with MountUseMountmgrFromFSD=1"
        ))
    })?;

    // Register the kernel cache invalidator with the VFS so metadata updates
    // can proactively enqueue inodes for WinFSP notification.
    let invalidator = std::sync::Arc::new(WinfspKernelInvalidator::new(invalidation_queue));
    vfs.set_kernel_invalidator(invalidator);

    tracing::info!("mounted StudioFS at {mount_point}, press Ctrl+C to unmount");
    host.start_with_threads(options.dispatcher_threads)
        .map_err(|err| std::io::Error::other(format!("failed to start WinFSP host: {err}")))?;

    // Block until Ctrl+C or external shutdown event
    let (tx, rx) = std::sync::mpsc::channel::<()>();

    // If an event name is provided, create a named event and watch for it in
    // a background thread.  When the event is signalled (`studiofs umount`),
    // the watcher sends on `tx` which wakes `rx.recv()` below — identical to
    // the Ctrl+C path.
    if let Some(event_name) = shutdown_event_name {
        let tx_event = tx.clone();
        let wide: Vec<u16> = event_name
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();

        unsafe extern "system" {
            fn CreateEventW(
                lpEventAttributes: *mut core::ffi::c_void,
                bManualReset: i32,
                bInitialState: i32,
                lpName: *const u16,
            ) -> *mut core::ffi::c_void;
            fn WaitForSingleObject(hHandle: *mut core::ffi::c_void, dwMilliseconds: u32) -> u32;
            fn ResetEvent(hEvent: *mut core::ffi::c_void) -> i32;
            fn CloseHandle(hObject: *mut core::ffi::c_void) -> i32;
        }

        // SAFETY: `wide` is a NUL-terminated UTF-16 buffer derived from
        // a stable Rust string; `CreateEventW` is a plain FFI call that returns
        // either a valid event handle or null on failure.
        let event = unsafe {
            CreateEventW(
                std::ptr::null_mut(),
                1, // bManualReset = TRUE
                0, // bInitialState = FALSE
                wide.as_ptr(),
            )
        };
        if !event.is_null() {
            // A previous force-terminated mount can leave the mount-point
            // event object signaled while the kernel process object is still
            // visible. Re-arm the event so this fresh mount does not
            // immediately consume a stale shutdown signal.
            unsafe {
                ResetEvent(event);
            }
            // SAFETY: `event` is a valid opaque Win32 handle; we move it
            // across thread boundaries as an integer token and reconstruct the
            // raw handle inside the watcher thread before invoking Win32 APIs.
            let handle = event as usize;
            std::thread::Builder::new()
                .name("shutdown-event-watcher".into())
                // SAFETY: The watcher thread owns the copied event handle and
                // only calls Win32 APIs that are valid for that handle until it is
                // explicitly closed after signaling is complete.
                .spawn(move || unsafe {
                    let handle = handle as *mut core::ffi::c_void;
                    WaitForSingleObject(handle, 0xFFFF_FFFF); // INFINITE
                    let _ = tx_event.send(());
                    CloseHandle(handle);
                })
                .ok();
        } else {
            tracing::warn!("failed to create shutdown named event");
        }
    }

    let _shutdown_registration = register_shutdown_sender(mount_point.clone(), tx)?;
    // SAFETY: The handler function pointer is valid for the process lifetime. The
    // shutdown registry rejects a second active in-process WinFSP mount before this
    // handler can target the wrong mount sender.
    let _console_ctrl_handler = ConsoleCtrlHandlerRegistration::register();
    let _ = rx.recv(); // blocks

    tracing::info!("unmounting StudioFS from {mount_point}");
    // Best-effort graceful shutdown: first drain VFS background work so
    // metadata engines (pg/redb) can stop their threads and be closed cleanly
    // before the kernel unmounts the filesystem. Use the same env var as the
    // mount session to control upload drain timeout.
    let upload_secs = std::env::var("STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let upload_drain_timeout = std::time::Duration::from_secs(upload_secs);
    // Stop WinFSP before the VFS shutdown drain so late callbacks cannot race
    // after the local metadata/cache checkpoint has been flushed.
    stop_host_before_vfs_shutdown_and_process_exit(host, || {
        runtime_handle.block_on(vfs.shutdown(upload_drain_timeout));
    });

    Ok(())
}

#[must_use = "if unused, the WinFSP console control handler will be immediately unregistered"]
struct ConsoleCtrlHandlerRegistration {
    registered: bool,
}

impl ConsoleCtrlHandlerRegistration {
    fn register() -> Self {
        // SAFETY: `ctrl_handler` has the documented Win32 callback ABI and does
        // not capture state. Registering it with `TRUE` installs it for the
        // current process until the matching unregister call in `Drop`.
        let registered = unsafe { SetConsoleCtrlHandler(Some(ctrl_handler), WIN32_TRUE) != 0 };
        if !registered {
            tracing::warn!("failed to register console control handler for WinFSP shutdown");
        }
        Self { registered }
    }
}

impl Drop for ConsoleCtrlHandlerRegistration {
    fn drop(&mut self) {
        if self.registered {
            // SAFETY: This unregisters the same process-lifetime callback
            // registered by `ConsoleCtrlHandlerRegistration::register`.
            let unregistered =
                unsafe { SetConsoleCtrlHandler(Some(ctrl_handler), WIN32_FALSE) != 0 };
            if !unregistered {
                tracing::warn!("failed to unregister console control handler for WinFSP shutdown");
            }
        }
    }
}

struct ShutdownRegistry {
    next_id: u64,
    active: Option<ActiveShutdownSender>,
}

struct ActiveShutdownSender {
    id: u64,
    mount_point: String,
    sender: std::sync::mpsc::Sender<()>,
}

#[derive(Debug)]
struct ShutdownRegistration {
    id: u64,
}

impl Drop for ShutdownRegistration {
    fn drop(&mut self) {
        let mut registry = shutdown_registry().lock();
        if registry
            .active
            .as_ref()
            .is_some_and(|active| active.id == self.id)
        {
            registry.active = None;
        }
    }
}

fn shutdown_registry() -> &'static parking_lot::Mutex<ShutdownRegistry> {
    static SHUTDOWN_REGISTRY: parking_lot::Mutex<ShutdownRegistry> =
        parking_lot::Mutex::new(ShutdownRegistry {
            next_id: 1,
            active: None,
        });
    &SHUTDOWN_REGISTRY
}

fn register_shutdown_sender(
    mount_point: String,
    sender: std::sync::mpsc::Sender<()>,
) -> std::io::Result<ShutdownRegistration> {
    let mut registry = shutdown_registry().lock();
    if let Some(active) = registry.active.as_ref() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "WinFSP mount '{}' is already registered for process shutdown",
                active.mount_point
            ),
        ));
    }

    let id = registry.next_id;
    registry.next_id = registry.next_id.wrapping_add(1).max(1);
    registry.active = Some(ActiveShutdownSender {
        id,
        mount_point,
        sender,
    });
    Ok(ShutdownRegistration { id })
}

#[cfg(test)]
fn clear_shutdown_registry_for_test() {
    shutdown_registry().lock().active = None;
}

pub(crate) fn request_shutdown() {
    let tx = shutdown_registry()
        .lock()
        .active
        .as_ref()
        .map(|active| active.sender.clone());
    if let Some(tx) = tx {
        let _ = tx.send(());
    }
}

// SAFETY: This is a Win32 API with the documented `"system"` ABI; declaration
// matches the platform function signature expected by the linker.
unsafe extern "system" {
    fn SetConsoleCtrlHandler(
        handler: Option<unsafe extern "system" fn(u32) -> i32>,
        add: i32,
    ) -> i32;
}

// SAFETY: This callback is invoked by Win32 with process lifecycle event codes only.
// We synchronously record shutdown intent and return `TRUE` to acknowledge handling.
unsafe extern "system" fn ctrl_handler(_ctrl_type: u32) -> i32 {
    request_shutdown();
    WIN32_TRUE
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ALLOCATION_UNIT_SIZE, DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE,
        DEFAULT_DIR_INFO_TIMEOUT_MS, DEFAULT_FILE_INFO_TIMEOUT_MS,
        DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP, DEFAULT_PERSISTENT_ACLS, DEFAULT_SECTOR_SIZE,
        DEFAULT_SECTORS_PER_ALLOCATION_UNIT, DEFAULT_VOLUME_INFO_TIMEOUT_MS, MountOptionError,
        WinfspShutdownHost, canonical_drive_mount_point, clear_shutdown_registry_for_test,
        default_dispatcher_threads, is_drive_root_mount_point, mount_manager_mount_point,
        parse_mount_options, register_shutdown_sender, remove_test_mount_option_env,
        request_shutdown, set_test_mount_option_env, stop_host_before_vfs_shutdown,
        stop_host_before_vfs_shutdown_and_process_exit, winfsp_dll_stem_for_arch,
    };
    #[test]
    fn parse_mount_options_rejects_invalid_labels() {
        assert_eq!(
            parse_mount_options("   ").unwrap_err(),
            MountOptionError::EmptyVolumeLabel
        );
        assert_eq!(
            parse_mount_options("bad\0label").unwrap_err(),
            MountOptionError::VolumeLabelContainsNul
        );
    }

    #[test]
    fn parse_mount_options_normalizes_and_sets_defaults() {
        remove_test_mount_option_env("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS");
        remove_test_mount_option_env("STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS");
        remove_test_mount_option_env("STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS");
        remove_test_mount_option_env("STUDIOFS_WINFSP_SECTOR_SIZE");
        remove_test_mount_option_env("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE");
        remove_test_mount_option_env("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE");
        remove_test_mount_option_env("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP");

        let opts = parse_mount_options("  StudioFS  ").expect("options");
        assert_eq!(opts.volume_label, "StudioFS");
        assert_eq!(opts.file_info_timeout_ms, DEFAULT_FILE_INFO_TIMEOUT_MS);
        assert_eq!(opts.dir_info_timeout_ms, DEFAULT_DIR_INFO_TIMEOUT_MS);
        assert_eq!(opts.volume_info_timeout_ms, DEFAULT_VOLUME_INFO_TIMEOUT_MS);
        assert_eq!(opts.sector_size, DEFAULT_SECTOR_SIZE);
        assert_eq!(
            opts.sectors_per_allocation_unit,
            DEFAULT_SECTORS_PER_ALLOCATION_UNIT
        );
        assert_eq!(opts.dispatcher_threads, default_dispatcher_threads());
        assert_eq!(
            opts.allow_open_in_kernel_mode,
            DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE
        );
        assert_eq!(
            opts.flush_and_purge_on_cleanup,
            DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP
        );
    }

    #[test]
    fn parse_mount_options_defaults_to_non_persistent_acls() {
        remove_test_mount_option_env("STUDIOFS_WINFSP_PERSISTENT_ACLS");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.persistent_acls, DEFAULT_PERSISTENT_ACLS);
        assert!(!opts.persistent_acls);
    }

    #[test]
    fn parse_mount_options_allows_persistent_acls_override() {
        set_test_mount_option_env("STUDIOFS_WINFSP_PERSISTENT_ACLS", "1");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert!(
            opts.persistent_acls,
            "explicit WinFSP boolean override should be able to advertise persistent ACLs"
        );

        remove_test_mount_option_env("STUDIOFS_WINFSP_PERSISTENT_ACLS");
    }

    #[test]
    fn parse_mount_options_defaults_to_winfsp_cache_manager_file_info_timeout() {
        remove_test_mount_option_env("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.file_info_timeout_ms, u32::MAX);
    }

    #[test]
    fn parse_mount_options_allows_env_timeout_overrides() {
        set_test_mount_option_env("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS", "240000");
        set_test_mount_option_env("STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS", "-1");
        set_test_mount_option_env("STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS", "480000");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.file_info_timeout_ms, 240_000);
        assert_eq!(opts.dir_info_timeout_ms, u32::MAX);
        assert_eq!(opts.volume_info_timeout_ms, 480_000);

        remove_test_mount_option_env("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS");
        remove_test_mount_option_env("STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS");
        remove_test_mount_option_env("STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS");
    }

    #[test]
    fn parse_mount_options_invalid_env_value_falls_back_to_default() {
        set_test_mount_option_env("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS", "abc");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.file_info_timeout_ms, DEFAULT_FILE_INFO_TIMEOUT_MS);

        remove_test_mount_option_env("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS");
    }

    #[test]
    fn parse_mount_options_allows_active_bool_overrides() {
        set_test_mount_option_env("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE", "0");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert!(
            !opts.allow_open_in_kernel_mode,
            "active WinFSP boolean overrides should still be honored"
        );

        remove_test_mount_option_env("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE");
    }

    #[test]
    fn parse_mount_options_allows_flush_and_purge_on_cleanup_override() {
        set_test_mount_option_env("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP", "1");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert!(
            opts.flush_and_purge_on_cleanup,
            "WinFSP cleanup flush/purge override should be honored"
        );

        remove_test_mount_option_env("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP");
    }

    #[test]
    fn parse_mount_options_invalid_bool_falls_back_to_default() {
        set_test_mount_option_env("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE", "maybe");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(
            opts.allow_open_in_kernel_mode,
            DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE
        );

        remove_test_mount_option_env("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE");
    }

    #[test]
    fn parse_mount_options_allows_geometry_overrides() {
        set_test_mount_option_env("STUDIOFS_WINFSP_SECTOR_SIZE", "4096");
        set_test_mount_option_env("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE", "4194304");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.sector_size, 4096);
        assert_eq!(opts.sectors_per_allocation_unit, 1024);

        remove_test_mount_option_env("STUDIOFS_WINFSP_SECTOR_SIZE");
        remove_test_mount_option_env("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE");
    }

    #[test]
    fn parse_mount_options_invalid_geometry_falls_back_to_defaults() {
        set_test_mount_option_env("STUDIOFS_WINFSP_SECTOR_SIZE", "3000");
        set_test_mount_option_env("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE", "1048576");

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.sector_size, DEFAULT_SECTOR_SIZE);
        assert_eq!(
            opts.sectors_per_allocation_unit,
            DEFAULT_SECTORS_PER_ALLOCATION_UNIT
        );

        remove_test_mount_option_env("STUDIOFS_WINFSP_SECTOR_SIZE");
        remove_test_mount_option_env("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE");
    }

    #[test]
    fn winfsp_drive_mount_point_validation() {
        assert!(is_drive_root_mount_point("Z:"));
        assert!(is_drive_root_mount_point("Z:\\"));
        assert!(!is_drive_root_mount_point(r"C:\mounts\studiofs"));
        assert!(!is_drive_root_mount_point(r"\\server\share\studiofs"));
    }

    #[test]
    fn canonical_drive_mount_point_normalizes_case_and_separator() {
        assert_eq!(canonical_drive_mount_point("z:").unwrap(), "Z:");
        assert_eq!(canonical_drive_mount_point("z:\\").unwrap(), "Z:");
        assert!(canonical_drive_mount_point(r"C:\mounts\studiofs").is_err());
    }

    #[test]
    fn mount_manager_mount_point_builds_device_path() {
        assert_eq!(mount_manager_mount_point("z:").unwrap(), r"\\.\Z:");
        assert_eq!(mount_manager_mount_point("z:\\").unwrap(), r"\\.\Z:");
    }

    #[test]
    fn winfsp_geometry_defaults_are_explicit_and_non_zero() {
        assert_eq!(DEFAULT_SECTORS_PER_ALLOCATION_UNIT, 256);
        assert_eq!(
            u32::from(DEFAULT_SECTORS_PER_ALLOCATION_UNIT) * u32::from(DEFAULT_SECTOR_SIZE),
            DEFAULT_ALLOCATION_UNIT_SIZE
        );
    }

    #[test]
    fn winfsp_dll_stem_matches_supported_architectures() {
        assert_eq!(winfsp_dll_stem_for_arch("x86_64"), Some("winfsp-x64"));
        assert_eq!(winfsp_dll_stem_for_arch("x86"), Some("winfsp-x86"));
        assert_eq!(winfsp_dll_stem_for_arch("aarch64"), Some("winfsp-a64"));
        assert_eq!(winfsp_dll_stem_for_arch("arm"), None);
    }

    struct FakeShutdownHost {
        events: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
    }

    impl WinfspShutdownHost for FakeShutdownHost {
        fn stop_for_shutdown(&mut self) {
            self.events.borrow_mut().push("host.stop");
        }

        fn unmount_after_shutdown(&mut self) {
            self.events.borrow_mut().push("host.unmount");
        }
    }

    impl Drop for FakeShutdownHost {
        fn drop(&mut self) {
            self.events.borrow_mut().push("host.drop");
        }
    }

    struct ShutdownRegistryTestGuard {
        _guard: parking_lot::MutexGuard<'static, ()>,
    }

    impl ShutdownRegistryTestGuard {
        fn new() -> Self {
            static TEST_MUTEX: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
            let guard = TEST_MUTEX.lock();
            clear_shutdown_registry_for_test();
            Self { _guard: guard }
        }
    }

    impl Drop for ShutdownRegistryTestGuard {
        fn drop(&mut self) {
            clear_shutdown_registry_for_test();
        }
    }

    #[test]
    fn graceful_shutdown_stops_winfsp_host_before_vfs_shutdown() {
        let events = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut host = FakeShutdownHost {
            events: events.clone(),
        };

        stop_host_before_vfs_shutdown(&mut host, || {
            events.borrow_mut().push("vfs.shutdown");
        });

        assert_eq!(
            events.borrow().as_slice(),
            ["host.stop", "vfs.shutdown", "host.unmount"],
            "WinFSP callbacks must be stopped before the VFS shutdown checkpoint"
        );
        drop(host);
        assert_eq!(
            events.borrow().as_slice(),
            ["host.stop", "vfs.shutdown", "host.unmount", "host.drop"],
            "borrowed shutdown helper must leave ordinary host Drop behavior intact"
        );
    }

    #[test]
    fn graceful_shutdown_for_process_exit_drops_winfsp_host_after_vfs_shutdown() {
        let events = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let host = FakeShutdownHost {
            events: events.clone(),
        };

        stop_host_before_vfs_shutdown_and_process_exit(host, || {
            events.borrow_mut().push("vfs.shutdown");
        });

        assert_eq!(
            events.borrow().as_slice(),
            ["host.stop", "vfs.shutdown", "host.drop"],
            "the foreground mount process must release the WinFSP host context after VFS shutdown"
        );
    }

    #[test]
    fn shutdown_registration_rejects_second_active_mount_without_replacing_sender() {
        let _guard = ShutdownRegistryTestGuard::new();
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let first_registration =
            register_shutdown_sender("Z:".to_string(), first_tx).expect("first mount registers");

        let err = register_shutdown_sender("Y:".to_string(), second_tx)
            .expect_err("second active mount must be rejected");

        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        request_shutdown();
        assert_eq!(
            first_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Ok(())
        );
        assert!(second_rx.try_recv().is_err());
        drop(first_registration);
    }

    #[test]
    fn shutdown_registration_cleanup_only_removes_its_own_sender() {
        let _guard = ShutdownRegistryTestGuard::new();
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let first_registration =
            register_shutdown_sender("Z:".to_string(), first_tx).expect("first mount registers");
        drop(first_registration);
        assert_eq!(
            first_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        );

        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let second_registration =
            register_shutdown_sender("Y:".to_string(), second_tx).expect("slot cleaned up");

        request_shutdown();
        assert_eq!(
            second_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Ok(())
        );
        drop(second_registration);
    }

    #[test]
    fn graceful_shutdown_unmounts_winfsp_host_when_vfs_shutdown_panics() {
        let events = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut host = FakeShutdownHost {
            events: events.clone(),
        };

        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            stop_host_before_vfs_shutdown(&mut host, || {
                events.borrow_mut().push("vfs.shutdown");
                panic!("simulated VFS shutdown panic");
            });
        }));

        assert!(
            panic_result.is_err(),
            "the original VFS shutdown panic should still propagate"
        );
        assert_eq!(
            events.borrow().as_slice(),
            ["host.stop", "vfs.shutdown", "host.unmount"],
            "WinFSP unmount must still be attempted while unwinding"
        );
    }
}
