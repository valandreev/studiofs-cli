#![cfg(windows)]
//! StudioFS WinFSP backend for Windows.
//!
//! Implements `winfsp::filesystem::FileSystemContext` to expose the VFS
//! as a Windows drive letter via the WinFSP (Windows File System Proxy) driver.

mod context;

pub use context::{StudioFsContext, WinfspKernelInvalidator};
use thiserror::Error;

const DEFAULT_FILE_INFO_TIMEOUT_MS: u32 = 1000;
const DEFAULT_DIR_INFO_TIMEOUT_MS: u32 = 1000;
const DEFAULT_VOLUME_INFO_TIMEOUT_MS: u32 = 1000;
const DEFAULT_SECTOR_SIZE: u16 = 4096;
const DEFAULT_ALLOCATION_UNIT_SIZE: u32 = 1024 * 1024;
const DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE: bool = true;
const DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP: bool = false;
const DEFAULT_SECTORS_PER_ALLOCATION_UNIT: u16 =
    (DEFAULT_ALLOCATION_UNIT_SIZE / DEFAULT_SECTOR_SIZE as u32) as u16;
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
    allow_open_in_kernel_mode: bool,
    flush_and_purge_on_cleanup: bool,
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

pub struct WinfspMountRequest {
    pub vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    pub runtime_handle: tokio::runtime::Handle,
    pub mount_point: String,
    pub volume_label: String,
    pub read_only: bool,
    pub shutdown_event_name: Option<String>,
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
    let allow_open_in_kernel_mode = parse_bool_env(
        "STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE",
        DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE,
    );
    let flush_and_purge_on_cleanup = parse_bool_env(
        "STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP",
        DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP,
    );

    Ok(WinfspMountOptions {
        volume_label: trimmed.to_string(),
        file_info_timeout_ms,
        dir_info_timeout_ms,
        volume_info_timeout_ms,
        sector_size,
        sectors_per_allocation_unit,
        allow_open_in_kernel_mode,
        flush_and_purge_on_cleanup,
    })
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

fn parse_positive_u32_env(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(raw) => {
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
        Err(_) => default,
    }
}

fn parse_timeout_env(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(raw) => {
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
        Err(_) => default,
    }
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(raw) => {
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
        Err(_) => default,
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

/// Start the WinFSP filesystem, mount, and block until unmounted.
///
/// `shutdown_event_name` — if `Some`, a Win32 named event is created so that an
/// external process (`studiofs umount`) can signal a graceful shutdown.
pub fn mount_and_serve(request: WinfspMountRequest) -> std::io::Result<()> {
    use studiofs_meta::Ino;
    use winfsp::host::{FileSystemHost, FileSystemParams, VolumeParams};

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
    volume_params.persistent_acls(true);
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
    );

    // Use `new_with_timer` so the WinFSP threadpool timer periodically calls
    // our `NotifyingFileSystemContext` impl, which sends `FspFileSystemNotify`
    // for metadata-driven invalidations. The const generic (250) is the
    // timer interval in milliseconds — short enough for snappy Explorer
    // refreshes, long enough to batch rapid sequences.
    let mut host = FileSystemHost::new_with_timer::<Vec<Ino>, 250>(
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
    host.start()
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

    *shutdown_tx().lock() = Some(tx);
    // SAFETY: The handler function pointer is valid for the process lifetime and the
    // process state is single-instance during mount, so passing it to WinFSP is valid.
    unsafe {
        SetConsoleCtrlHandler(Some(ctrl_handler), 1);
    }
    let _ = rx.recv(); // blocks

    tracing::info!("unmounting StudioFS from {mount_point}");
    // Stop WinFSP first so no new callbacks race with the shutdown drain.
    host.stop();
    host.unmount();

    Ok(())
}

fn shutdown_tx() -> &'static parking_lot::Mutex<Option<std::sync::mpsc::Sender<()>>> {
    static SHUTDOWN_TX: std::sync::OnceLock<
        parking_lot::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    > = std::sync::OnceLock::new();
    SHUTDOWN_TX.get_or_init(|| parking_lot::Mutex::new(None))
}

pub(crate) fn request_shutdown() {
    let guard = shutdown_tx().lock();
    if let Some(tx) = guard.as_ref() {
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
    1 // TRUE — handled
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        DEFAULT_ALLOCATION_UNIT_SIZE, DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE,
        DEFAULT_DIR_INFO_TIMEOUT_MS, DEFAULT_FILE_INFO_TIMEOUT_MS,
        DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP, DEFAULT_SECTOR_SIZE,
        DEFAULT_SECTORS_PER_ALLOCATION_UNIT, DEFAULT_VOLUME_INFO_TIMEOUT_MS, MountOptionError,
        canonical_drive_mount_point, is_drive_root_mount_point, mount_manager_mount_point,
        parse_mount_options,
    };
    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_SECTOR_SIZE");
            std::env::remove_var("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE");
        }
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
    fn parse_mount_options_allows_env_timeout_overrides() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::set_var("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS", "240000");
            std::env::set_var("STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS", "-1");
            std::env::set_var("STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS", "480000");
        }

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.file_info_timeout_ms, 240_000);
        assert_eq!(opts.dir_info_timeout_ms, u32::MAX);
        assert_eq!(opts.volume_info_timeout_ms, 480_000);

        // SAFETY: Tests are single-threaded via `_guard` and restore environment
        // variables that were written earlier in the test.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS");
            std::env::remove_var("STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS");
            std::env::remove_var("STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS");
        }
    }

    #[test]
    fn parse_mount_options_invalid_env_value_falls_back_to_default() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::set_var("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS", "abc");
        }

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.file_info_timeout_ms, DEFAULT_FILE_INFO_TIMEOUT_MS);

        // SAFETY: Tests are single-threaded via `_guard` and restore environment
        // variables that were written earlier in the test.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS");
        }
    }

    #[test]
    fn parse_mount_options_allows_bool_overrides() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::set_var("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE", "0");
            std::env::set_var("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP", "true");
        }

        let opts = parse_mount_options("StudioFS").expect("options");
        assert!(!opts.allow_open_in_kernel_mode);
        assert!(opts.flush_and_purge_on_cleanup);

        // SAFETY: Tests are single-threaded via `_guard` and restore environment
        // variables that were written earlier in the test.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE");
            std::env::remove_var("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP");
        }
    }

    #[test]
    fn parse_mount_options_invalid_bool_falls_back_to_default() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::set_var("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE", "maybe");
            std::env::set_var("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP", "maybe");
        }

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(
            opts.allow_open_in_kernel_mode,
            DEFAULT_ALLOW_OPEN_IN_KERNEL_MODE
        );
        assert_eq!(
            opts.flush_and_purge_on_cleanup,
            DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP
        );

        // SAFETY: Tests are single-threaded via `_guard` and restore environment
        // variables that were written earlier in the test.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE");
            std::env::remove_var("STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP");
        }
    }

    #[test]
    fn parse_mount_options_allows_geometry_overrides() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::set_var("STUDIOFS_WINFSP_SECTOR_SIZE", "4096");
            std::env::set_var("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE", "4194304");
        }

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.sector_size, 4096);
        assert_eq!(opts.sectors_per_allocation_unit, 1024);

        // SAFETY: Tests are single-threaded via `_guard` and restore environment
        // variables that were written earlier in the test.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_SECTOR_SIZE");
            std::env::remove_var("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE");
        }
    }

    #[test]
    fn parse_mount_options_invalid_geometry_falls_back_to_defaults() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: Tests are single-threaded via `_guard` and temporarily mutate
        // process environment variables for deterministic parsing behavior.
        unsafe {
            std::env::set_var("STUDIOFS_WINFSP_SECTOR_SIZE", "3000");
            std::env::set_var("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE", "1048576");
        }

        let opts = parse_mount_options("StudioFS").expect("options");
        assert_eq!(opts.sector_size, DEFAULT_SECTOR_SIZE);
        assert_eq!(
            opts.sectors_per_allocation_unit,
            DEFAULT_SECTORS_PER_ALLOCATION_UNIT
        );

        // SAFETY: Tests are single-threaded via `_guard` and restore environment
        // variables that were written earlier in the test.
        unsafe {
            std::env::remove_var("STUDIOFS_WINFSP_SECTOR_SIZE");
            std::env::remove_var("STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE");
        }
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
}
