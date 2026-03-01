#![cfg(windows)]
//! StudioFS WinFSP backend for Windows.
//!
//! Implements `winfsp::filesystem::FileSystemContext` to expose the VFS
//! as a Windows drive letter via the WinFSP (Windows File System Proxy) driver.

mod context;

pub use context::{StudioFsContext, WinfspKernelInvalidator};

/// Start the WinFSP filesystem, mount, and block until unmounted.
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
    mount_point: &str,
    _volume_label: &str,
    _foreground: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use studiofs_meta::Ino;
    use winfsp::host::{FileSystemHost, FileSystemParams, VolumeParams};

    let mut volume_params = VolumeParams::new();
    volume_params.case_sensitive_search(true);
    volume_params.case_preserved_names(true);
    volume_params.unicode_on_disk(true);
    volume_params.persistent_acls(true);
    volume_params.reparse_points(true);
    volume_params.named_streams(false);
    volume_params.read_only_volume(false);
    volume_params.post_cleanup_when_modified_only(true);
    // Allow kernel-mode opens — required for shell extensions (7-zip, etc.)
    volume_params.allow_open_in_kernel_mode(true);
    volume_params.sector_size(512);
    volume_params.sectors_per_allocation_unit(1);
    volume_params.max_component_length(255);
    volume_params.volume_serial_number(0x5354_4644); // "STFD"
    volume_params.filesystem_name("StudioFS");
    // WinFSP layer tuning:
    // - Shorter file_info_timeout for quick invalidation on overwrites (P1.2)
    // - Longer dir_info_timeout since directory entries change less frequently
    volume_params.file_info_timeout(1000);
    volume_params.dir_info_timeout(1000);

    // Keep a reference for shutdown() (vfs is moved into context).
    let vfs_ref = vfs.clone();

    // Shared invalidation queue: the `WinfspKernelInvalidator` pushes inode
    // IDs from NATS events; the WinFSP timer drains and sends OS notifications.
    let invalidation_queue = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<Ino>::new()));

    let context = StudioFsContext::new(vfs.clone(), rt.clone(), invalidation_queue.clone());

    // Use `new_with_timer` so the WinFSP threadpool timer periodically calls
    // our `NotifyingFileSystemContext` impl, which sends `FspFileSystemNotify`
    // for NATS-driven metadata invalidations.  The const generic (250) is the
    // timer interval in milliseconds — short enough for snappy Explorer
    // refreshes, long enough to batch rapid sequences.
    let mut host =
        FileSystemHost::new_with_timer::<Vec<Ino>, 250>(
            FileSystemParams::default_params(volume_params),
            context,
        )?;
    host.mount(mount_point)?;

    // Register the kernel cache invalidator with the VFS so that NATS events
    // can proactively enqueue inodes for WinFSP notification.
    let invalidator = std::sync::Arc::new(WinfspKernelInvalidator::new(invalidation_queue));
    vfs.set_kernel_invalidator(invalidator);

    tracing::info!("mounted StudioFS at {mount_point}, press Ctrl+C to unmount");
    host.start()?;

    // Block until Ctrl+C
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    *SHUTDOWN_TX.lock().unwrap() = Some(tx);
    unsafe {
        SetConsoleCtrlHandler(Some(ctrl_handler), 1);
    }
    let _ = rx.recv(); // blocks

    tracing::info!("unmounting StudioFS from {mount_point}");
    // Stop WinFSP first so no new callbacks race with the shutdown drain.
    host.stop();
    host.unmount();
    // Drain uploads and persist cache checkpoint after WinFSP callbacks are quiesced.
    rt.block_on(vfs_ref.shutdown(std::time::Duration::from_secs(30)));

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
