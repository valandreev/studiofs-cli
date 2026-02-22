#![cfg(windows)]
//! StudioFS WinFSP backend for Windows.
//!
//! Implements `winfsp::filesystem::FileSystemContext` to expose the VFS
//! as a Windows drive letter via the WinFSP (Windows File System Proxy) driver.

mod context;

pub use context::StudioFsContext;

/// Start the WinFSP filesystem, mount, and block until unmounted.
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
    mount_point: &str,
    _volume_label: &str,
    _foreground: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use winfsp::host::{FileSystemHost, VolumeParams};

    let mut volume_params = VolumeParams::new();
    // Case-insensitive search is required for proper Windows compatibility:
    // shell extensions (7-zip), TeraCopy, and most Win32 apps expect it.
    // resolve_path does case-insensitive fallback; open/create call set_normalized_name.
    volume_params.case_sensitive_search(false);
    volume_params.case_preserved_names(true);
    volume_params.unicode_on_disk(true);
    // Persistent ACLs enabled: we provide a full-access SD so all checks pass,
    // and Windows apps (shell, security tab) see proper ACL support.
    volume_params.persistent_acls(true);
    volume_params.reparse_points(true);
    volume_params.named_streams(false);
    volume_params.read_only_volume(false);
    volume_params.post_cleanup_when_modified_only(true);
    volume_params.flush_and_purge_on_cleanup(true);
    // Allow kernel-mode opens — required for shell extensions (7-zip, etc.)
    // and antivirus/indexing that access files from kernel context.
    volume_params.allow_open_in_kernel_mode(true);
    volume_params.pass_query_directory_pattern(true);
    volume_params.supports_posix_unlink_rename(true);
    volume_params.post_disposition_only_when_necessary(true);
    volume_params.sector_size(512);
    volume_params.sectors_per_allocation_unit(1);
    volume_params.max_component_length(255);
    volume_params.volume_serial_number(0x5354_4644); // "STFD"
    volume_params.filesystem_name("StudioFS");
    // WinFSP cache tuning:
    // - file_info_timeout enables the Windows Cache Manager for read perf
    // - dir_info_timeout can be longer since dir entries change less often
    volume_params.file_info_timeout(1000); // 1s: enables Cache Manager
    volume_params.dir_info_timeout(2000); // 2s: dir entries change less frequently

    // Keep a reference for persist_cache() on shutdown (vfs is moved into context).
    let vfs_ref = vfs.clone();
    let context = StudioFsContext::new(vfs, rt.clone());

    let mut host = FileSystemHost::new(volume_params, context)?;
    host.mount(mount_point)?;

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
    // Drain writeback state after WinFSP callbacks are quiesced.
    rt.block_on(vfs_ref.shutdown(std::time::Duration::from_secs(10)));

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
