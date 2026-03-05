#![cfg(unix)]

//! StudioFS FUSE backend for Linux and macOS using `fuser`.
//!
//! Implements `fuser::Filesystem` to expose StudioVfs via FUSE.

mod fs;

use std::sync::atomic::{AtomicBool, Ordering};

pub use fs::{FuseKernelInvalidator, StudioFuseFs};

fn shutdown_upload_drain_timeout() -> std::time::Duration {
    let secs = std::env::var("STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    std::time::Duration::from_secs(secs)
}

// ─── Signal Handling ─────────────────────────────────────────────────────────

/// Flag set by the SIGINT/SIGTERM handler.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

/// Spawn a background thread that watches for SIGINT/SIGTERM and triggers
/// `umount` on the mount point so that `fuser::mount2` returns cleanly and
/// the VFS shutdown/drain logic can execute.
fn spawn_signal_watcher(mount_point: &str) {
    let mp = mount_point.to_string();
    // Install signal handlers (replaces the default "terminate" action).
    unsafe {
        libc::signal(
            libc::SIGINT,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
    }
    std::thread::Builder::new()
        .name("signal-watcher".into())
        .spawn(move || {
            // Poll the atomic flag until a signal arrives.
            while !SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            tracing::info!("received shutdown signal, unmounting {mp} …");
            // Restore default signal handlers immediately so a second Ctrl+C
            // force-kills the process in case unmount or shutdown stalls.
            unsafe {
                libc::signal(libc::SIGINT, libc::SIG_DFL);
                libc::signal(libc::SIGTERM, libc::SIG_DFL);
            }
            // Trigger a clean unmount so fuser::mount2() returns.
            #[cfg(target_os = "macos")]
            {
                let _ = std::process::Command::new("umount").arg(&mp).status();
            }
            #[cfg(target_os = "linux")]
            {
                // Use lazy unmount (-z) so the command succeeds even when the
                // FUSE daemon process itself is keeping the mount busy.  The
                // mount point is detached from the filesystem hierarchy
                // immediately, which severs the FUSE connection and lets
                // fuser::mount2() return.  Actual cleanup happens once no
                // process references the mount any more (i.e. right after we
                // exit).
                let _ = std::process::Command::new("fusermount3")
                    .args(["-uz", &mp])
                    .status()
                    .or_else(|_| {
                        std::process::Command::new("fusermount")
                            .args(["-uz", &mp])
                            .status()
                    })
                    .or_else(|_| {
                        std::process::Command::new("umount")
                            .args(["-l", &mp])
                            .status()
                    });
            }
        })
        .expect("failed to spawn signal-watcher thread");
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Mount and serve the FUSE filesystem (blocks until unmounted).
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
    mount_point: &str,
    volume_name: &str,
    foreground: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Keep a reference for shutdown() after the FUSE session ends
    // (vfs is moved into StudioFuseFs below).
    let vfs_ref = vfs.clone();
    let fs = StudioFuseFs::new(vfs.clone(), rt.clone());
    let mut options = vec![
        fuser::MountOption::FSName(volume_name.into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::AllowOther,
        // Do NOT add DefaultPermissions here: it would make the kernel enforce
        // POSIX mode/uid/gid from getattr() in-kernel.  Old volumes have the
        // root inode stored with uid=0/gid=0 (historic default), so the kernel
        // would see the mounting user as "other" (r-x only) and block writes.
        // Instead we implement access() → ok() so the FUSE daemon decides.
    ];
    // macOS-specific options:
    //  noappledouble – suppress creation of ._  AppleDouble resource files
    //  volname       – display name in Finder
    #[cfg(target_os = "macos")]
    {
        options.push(fuser::MountOption::CUSTOM("noappledouble".into()));
        options.push(fuser::MountOption::CUSTOM(format!("volname={volume_name}")));
    }
    if foreground {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }

    // Watch for Ctrl+C / SIGTERM and trigger a clean unmount.
    spawn_signal_watcher(mount_point);

    tracing::info!("mounting StudioFS at {mount_point}, press Ctrl+C to unmount");

    // Use Session::new + run() instead of fuser::mount2 so we can obtain a
    // Notifier. The notifier allows VFS metadata refresh paths to
    // proactively clear the kernel FUSE dentry/attribute cache, making remote
    // creates/deletes visible immediately without waiting for TTL expiry.
    let mut session = fuser::Session::new(fs, mount_point.as_ref() as &std::path::Path, &options)?;
    let notifier = session.notifier();

    // Register the kernel cache invalidator with the VFS.
    let invalidator = std::sync::Arc::new(FuseKernelInvalidator::new(notifier));
    vfs.set_kernel_invalidator(invalidator);

    session.run()?;

    // Session::run returns when the filesystem is unmounted.
    // Drain uploads and persist cache checkpoint before process exit.
    tracing::info!("unmounted, draining uploads …");
    rt.block_on(vfs_ref.shutdown(shutdown_upload_drain_timeout()));
    // Use process::exit to avoid blocking on tokio Runtime drop, which would
    // wait for all background tasks (janitor, GC etc.) to complete — causing
    // an indefinite hang after unmount on Linux/WSL.
    tracing::info!("shutdown complete, exiting");
    std::process::exit(0);
}
