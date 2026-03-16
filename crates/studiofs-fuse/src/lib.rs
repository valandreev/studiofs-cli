#![cfg(unix)]

//! StudioFS FUSE backend for Linux and macOS using `fuser`.
//!
//! Implements `fuser::Filesystem` to expose StudioVfs via FUSE.

mod fs;

use std::sync::atomic::{AtomicBool, Ordering};

pub use fs::{FuseKernelInvalidator, StudioFuseFs};
use thiserror::Error;

fn shutdown_upload_drain_timeout() -> std::time::Duration {
    let secs = std::env::var("STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    std::time::Duration::from_secs(secs)
}

#[derive(Debug, Error, PartialEq, Eq)]
enum MountOptionError {
    #[error("volume name cannot be empty")]
    EmptyVolumeName,
    #[error("volume name cannot contain NUL")]
    VolumeNameContainsNul,
}

fn validate_volume_name(volume_name: &str) -> Result<(), MountOptionError> {
    if volume_name.trim().is_empty() {
        return Err(MountOptionError::EmptyVolumeName);
    }
    if volume_name.contains('\0') {
        return Err(MountOptionError::VolumeNameContainsNul);
    }
    Ok(())
}

fn build_mount_options(
    volume_name: &str,
    #[allow(unused_variables)] foreground: bool,
    read_only: bool,
) -> Result<Vec<fuser::MountOption>, MountOptionError> {
    validate_volume_name(volume_name)?;
    let mut options = vec![
        fuser::MountOption::FSName(volume_name.into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::AllowOther,
    ];
    #[cfg(target_os = "macos")]
    {
        options.push(fuser::MountOption::CUSTOM(format!("volname={volume_name}")));
    }
    #[cfg(target_os = "macos")]
    if foreground {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }
    if read_only {
        options.push(fuser::MountOption::RO);
    }
    Ok(options)
}

// ─── Signal Handling ─────────────────────────────────────────────────────────

/// Flag set by the SIGINT/SIGTERM handler.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

/// Spawn a background thread that watches for SIGINT/SIGTERM and triggers
/// an in-process unmount so `fuser::Session::run` returns cleanly and
/// the VFS shutdown/drain logic can execute.
fn spawn_signal_watcher(mount_point: String, mut unmount: fuser::SessionUnmounter) {
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
            let _ = unmount.unmount();
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
    read_only: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Keep a reference for shutdown() after the FUSE session ends
    // (vfs is moved into StudioFuseFs below).
    let vfs_ref = vfs.clone();
    let fs = StudioFuseFs::new(vfs.clone(), rt.clone());
    // Do NOT add DefaultPermissions here: it would make the kernel enforce
    // POSIX mode/uid/gid from getattr() in-kernel.  Old volumes have the
    // root inode stored with uid=0/gid=0 (historic default), so the kernel
    // would see the mounting user as "other" (r-x only) and block writes.
    // Instead we implement access() -> ok() so the FUSE daemon decides.
    let options = build_mount_options(volume_name, foreground, read_only)?;
    // macOS-specific options:
    //  volname – display name in Finder
    //
    // Do not use `noappledouble` or `noapplexattr`: StudioFS accepts native
    // macOS xattrs (including resource forks) via FUSE xattr callbacks, and
    // these options would interfere with expected metadata behavior.
    tracing::info!("mounting StudioFS at {mount_point}, press Ctrl+C to unmount");

    // Use Session::new + run() instead of fuser::mount2 so we can obtain a
    // Notifier. The notifier allows VFS metadata refresh paths to
    // proactively clear the kernel FUSE dentry/attribute cache, making remote
    // creates/deletes visible immediately without waiting for TTL expiry.
    let mut session = fuser::Session::new(fs, mount_point.as_ref() as &std::path::Path, &options)?;
    let unmount = session.unmount_callable();
    // Watch for Ctrl+C / SIGTERM and trigger in-process unmount.
    spawn_signal_watcher(mount_point.to_string(), unmount);
    let notifier = session.notifier();

    // Register the kernel cache invalidator with the VFS.
    let invalidator = std::sync::Arc::new(FuseKernelInvalidator::new(notifier));
    vfs.set_kernel_invalidator(invalidator);

    session.run()?;

    // Session::run returns when the filesystem is unmounted.
    // Drain uploads and persist cache checkpoint before process exit.
    vfs_ref.clear_kernel_invalidator();
    tracing::info!("unmounted, draining uploads …");
    rt.block_on(vfs_ref.shutdown(shutdown_upload_drain_timeout()));
    // Explicitly drop the FUSE session to trigger fuser/OS cleanup before hard exit.
    // Without this, libfuse resources can remain partially alive and abort on process shutdown.
    drop(session);
    // Use process::exit to avoid blocking on tokio Runtime drop, which would
    // wait for all background tasks (janitor, GC etc.) to complete — causing
    // an indefinite hang after unmount on Linux/WSL.
    tracing::info!("shutdown complete, exiting");
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::{MountOptionError, build_mount_options, validate_volume_name};

    #[test]
    fn volume_name_validation_rejects_empty_or_nul() {
        assert_eq!(
            validate_volume_name("   ").unwrap_err(),
            MountOptionError::EmptyVolumeName
        );
        assert_eq!(
            validate_volume_name("has\0nul").unwrap_err(),
            MountOptionError::VolumeNameContainsNul
        );
        validate_volume_name("studiofs").expect("valid volume name");
    }

    #[test]
    fn build_mount_options_sets_core_fuse_flags() {
        let options = build_mount_options("studiofs", false, false).expect("options");
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::FSName(name) if name == "studiofs"))
        );
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::AllowOther))
        );
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::AutoUnmount))
        );
        assert!(!options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_mount_options_includes_foreground_when_requested() {
        let options = build_mount_options("studiofs", true, false).expect("options");
        assert!(options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn build_mount_options_foreground_not_added_on_linux() {
        // On Linux, fusermount3 does not accept 'foreground' as a mount option.
        // fuser keeps the process in the foreground via session.run() itself.
        let options = build_mount_options("studiofs", true, false).expect("options");
        assert!(!options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_mount_options_sets_macos_volume_name() {
        let options = build_mount_options("studiofs-mac", false, false).expect("options");
        assert!(options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "volname=studiofs-mac")
        ));
    }
}
