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

#[derive(Debug, Clone, PartialEq, Eq)]
struct WinfspMountOptions {
    volume_label: String,
    file_info_timeout_ms: u32,
    dir_info_timeout_ms: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum MountOptionError {
    #[error("volume label cannot be empty")]
    EmptyVolumeLabel,
    #[error("volume label cannot contain NUL")]
    VolumeLabelContainsNul,
}

fn parse_mount_options(volume_label: &str) -> Result<WinfspMountOptions, MountOptionError> {
    let trimmed = volume_label.trim();
    if trimmed.is_empty() {
        return Err(MountOptionError::EmptyVolumeLabel);
    }
    if trimmed.contains('\0') {
        return Err(MountOptionError::VolumeLabelContainsNul);
    }

    Ok(WinfspMountOptions {
        volume_label: trimmed.to_string(),
        file_info_timeout_ms: DEFAULT_FILE_INFO_TIMEOUT_MS,
        dir_info_timeout_ms: DEFAULT_DIR_INFO_TIMEOUT_MS,
    })
}

fn apply_volume_params(
    volume_params: &mut winfsp::host::VolumeParams,
    options: &WinfspMountOptions,
    mount_point: &str,
) {
    volume_params.filesystem_name("StudioFS");
    // `Prefix` switches WinFSP to network-device mode (WinFsp.Net). When
    // mounting into a directory, we need that mode to avoid object namespace
    // collisions with the default local-disk device name.
    if !is_drive_root_mount_point(mount_point) {
        volume_params.prefix("studiofs");
    }
    volume_params.file_info_timeout(options.file_info_timeout_ms);
    volume_params.dir_info_timeout(options.dir_info_timeout_ms);
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

fn shutdown_upload_drain_timeout() -> std::time::Duration {
    let secs = std::env::var("STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    std::time::Duration::from_secs(secs)
}

/// Start the WinFSP filesystem, mount, and block until unmounted.
///
/// `shutdown_event_name` — if `Some`, a Win32 named event is created so that an
/// external process (`studiofs umount`) can signal a graceful shutdown.
#[allow(clippy::too_many_arguments)]
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
    mount_point: &str,
    volume_label: &str,
    _foreground: bool,
    read_only: bool,
    shutdown_event_name: Option<&str>,
    profiling: Option<Box<dyn FnMut()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    use studiofs_meta::Ino;
    use winfsp::host::{FileSystemHost, FileSystemParams, VolumeParams};

    let options = parse_mount_options(volume_label)?;
    let mut volume_params = VolumeParams::new();
    volume_params.case_sensitive_search(true);
    volume_params.case_preserved_names(true);
    volume_params.unicode_on_disk(true);
    volume_params.persistent_acls(true);
    volume_params.reparse_points(true);
    volume_params.named_streams(false);
    volume_params.read_only_volume(read_only);
    volume_params.post_cleanup_when_modified_only(true);
    // Allow kernel-mode opens — required for shell extensions (7-zip, etc.)
    volume_params.allow_open_in_kernel_mode(true);
    volume_params.sector_size(512);
    volume_params.sectors_per_allocation_unit(1);
    volume_params.max_component_length(255);
    volume_params.volume_serial_number(0x5354_4644); // "STFD"
    // WinFSP layer tuning:
    // - file_info_timeout and dir_info_timeout tuned for fast metadata refresh.
    apply_volume_params(&mut volume_params, &options, mount_point);

    // Keep a reference for shutdown() (vfs is moved into context).
    let vfs_ref = vfs.clone();

    // Shared invalidation queue: the `WinfspKernelInvalidator` pushes inode
    // IDs from metadata refresh events; the WinFSP timer drains and sends OS notifications.
    let invalidation_queue = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<Ino>::new()));

    let context = StudioFsContext::new(vfs.clone(), rt.clone(), invalidation_queue.clone());

    // Use `new_with_timer` so the WinFSP threadpool timer periodically calls
    // our `NotifyingFileSystemContext` impl, which sends `FspFileSystemNotify`
    // for metadata-driven invalidations. The const generic (250) is the
    // timer interval in milliseconds — short enough for snappy Explorer
    // refreshes, long enough to batch rapid sequences.
    let mut host = FileSystemHost::new_with_timer::<Vec<Ino>, 250>(
        FileSystemParams::default_params(volume_params),
        context,
    )?;
    host.mount(mount_point)?;

    // Register the kernel cache invalidator with the VFS so metadata updates
    // can proactively enqueue inodes for WinFSP notification.
    let invalidator = std::sync::Arc::new(WinfspKernelInvalidator::new(invalidation_queue));
    vfs.set_kernel_invalidator(invalidator);

    tracing::info!("mounted StudioFS at {mount_point}, press Ctrl+C to unmount");
    host.start()?;

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

        let event = unsafe {
            CreateEventW(
                std::ptr::null_mut(),
                1, // bManualReset = TRUE
                0, // bInitialState = FALSE
                wide.as_ptr(),
            )
        };
        if !event.is_null() {
            // SAFETY: `event` is a valid handle; the thread takes ownership and
            // closes it after WaitForSingleObject returns.  We round-trip through
            // usize so the captured value is `Send`.
            let handle = event as usize;
            std::thread::Builder::new()
                .name("shutdown-event-watcher".into())
                .spawn(move || unsafe {
                    let h = handle as *mut core::ffi::c_void;
                    WaitForSingleObject(h, 0xFFFF_FFFF); // INFINITE
                    let _ = tx_event.send(());
                    CloseHandle(h);
                })
                .ok();
        } else {
            tracing::warn!("failed to create shutdown named event");
        }
    }

    *SHUTDOWN_TX.lock().unwrap() = Some(tx);
    unsafe {
        SetConsoleCtrlHandler(Some(ctrl_handler), 1);
    }
    let _ = rx.recv(); // blocks

    tracing::info!("unmounting StudioFS from {mount_point}");
    // Stop WinFSP first so no new callbacks race with the shutdown drain.
    host.stop();
    host.unmount();
    // By default do not block process exit on remote uploads; pending journal
    // entries are resumed on next mount.
    rt.block_on(vfs_ref.shutdown(shutdown_upload_drain_timeout()));
    if let Some(mut profiling) = profiling {
        profiling();
    }

    Ok(())
}

static SHUTDOWN_TX: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>> =
    std::sync::Mutex::new(None);

unsafe extern "system" {
    fn SetConsoleCtrlHandler(
        handler: Option<unsafe extern "system" fn(u32) -> i32>,
        add: i32,
    ) -> i32;
}

unsafe extern "system" fn ctrl_handler(_ctrl_type: u32) -> i32 {
    if let Ok(guard) = SHUTDOWN_TX.lock()
        && let Some(tx) = guard.as_ref()
    {
        let _ = tx.send(());
    }
    1 // TRUE — handled
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_DIR_INFO_TIMEOUT_MS, DEFAULT_FILE_INFO_TIMEOUT_MS, MountOptionError,
        parse_mount_options,
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
        let opts = parse_mount_options("  StudioFS  ").expect("options");
        assert_eq!(opts.volume_label, "StudioFS");
        assert_eq!(opts.file_info_timeout_ms, DEFAULT_FILE_INFO_TIMEOUT_MS);
        assert_eq!(opts.dir_info_timeout_ms, DEFAULT_DIR_INFO_TIMEOUT_MS);
    }
}
