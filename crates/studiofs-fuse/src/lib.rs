//! StudioFS FUSE backend for Linux and macOS using `fuser`.
//!
//! Implements `fuser::Filesystem` to expose `studiofs_vfs::StudioVfs` via FUSE.
//!
//! # Mount options
//!
//! The `STUDIOFS_FUSE_ALLOW_OTHER=1` environment variable enables the
//! `AllowOther` mount option on Linux. On macOS, `AllowOther` is enabled
//! unconditionally (per the macFUSE behavior expected by StudioFS).

#![cfg(unix)]

mod fs;

use std::sync::atomic::{AtomicBool, Ordering};

/// A `fuser::Notifier`-backed cache invalidator used by `studiofs_vfs` to
/// proactively clear kernel FUSE dentry/attribute caches.
pub use fs::FuseKernelInvalidator;
/// The `fuser::Filesystem` implementation that bridges StudioFS VFS operations
/// into FUSE callbacks.
pub use fs::StudioFuseFs;
use thiserror::Error;

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
    #[cfg_attr(not(target_os = "macos"), allow(unused_variables))] foreground: bool,
    read_only: bool,
) -> Result<Vec<fuser::MountOption>, MountOptionError> {
    build_mount_options_with_allow_other(
        volume_name,
        foreground,
        read_only,
        std::env::var("STUDIOFS_FUSE_ALLOW_OTHER").as_deref() == Ok("1"),
    )
}

fn build_mount_options_with_allow_other(
    volume_name: &str,
    #[cfg_attr(not(target_os = "macos"), allow(unused_variables))] foreground: bool,
    read_only: bool,
    allow_other_env: bool,
) -> Result<Vec<fuser::MountOption>, MountOptionError> {
    validate_volume_name(volume_name)?;
    let mut options = vec![
        fuser::MountOption::FSName(volume_name.into()),
        fuser::MountOption::AutoUnmount,
    ];
    if allow_other_env || cfg!(target_os = "macos") {
        options.push(fuser::MountOption::AllowOther);
    }
    #[cfg(target_os = "macos")]
    {
        options.push(fuser::MountOption::CUSTOM(format!("volname={volume_name}")));
        // Tell macFUSE to treat this as a local drive, which enables native Trash.
        options.push(fuser::MountOption::CUSTOM("local".into()));
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
fn spawn_signal_watcher(
    mount_point: String,
    mut unmount: fuser::SessionUnmounter,
) -> std::io::Result<()> {
    let mp = mount_point.to_string();
    // SAFETY: `libc::signal` mutates process-global signal disposition. We register a
    // process-lifetime signal handler for shutdown before entering the mount loop.
    unsafe {
        let previous = libc::signal(
            libc::SIGINT,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
        if previous == libc::SIG_ERR {
            return Err(std::io::Error::last_os_error());
        }
        let previous = libc::signal(
            libc::SIGTERM,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
        if previous == libc::SIG_ERR {
            return Err(std::io::Error::last_os_error());
        }
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
            // SAFETY: we restore the process-global default handlers with libc constants.
            unsafe {
                libc::signal(libc::SIGINT, libc::SIG_DFL);
                libc::signal(libc::SIGTERM, libc::SIG_DFL);
            }
            let _ = unmount.unmount();
        })
        .map(|_| ())?;
    Ok(())
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Mount and serve the FUSE filesystem (blocks until unmounted).
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    runtime_handle: tokio::runtime::Handle,
    mount_point: &str,
    volume_name: &str,
    foreground: bool,
    read_only: bool,
) -> std::io::Result<()> {
    let fs = StudioFuseFs::new(vfs.clone(), runtime_handle);
    // Do NOT add DefaultPermissions here: it would make the kernel enforce
    // POSIX mode/uid/gid from getattr() in-kernel.  Old volumes have the
    // root inode stored with uid=0/gid=0 (historic default), so the kernel
    // would see the mounting user as "other" (r-x only) and block writes.
    // Instead we implement access() -> ok() so the FUSE daemon decides.
    let options = build_mount_options(volume_name, foreground, read_only)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
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
    spawn_signal_watcher(mount_point.to_string(), unmount)?;
    let notifier = session.notifier();

    // Register the kernel cache invalidator with the VFS.
    let invalidator = std::sync::Arc::new(FuseKernelInvalidator::new(notifier));
    vfs.set_kernel_invalidator(invalidator);

    session.run()?;

    // Session::run returns when the filesystem is unmounted.
    vfs.clear_kernel_invalidator();
    tracing::info!("unmounted, finalizing FUSE session cleanup …");
    // If we arrived here without SIGINT/SIGTERM, the mount point was most
    // likely detached by Finder/umount. In that case skip dropping the session
    // object to avoid a second unmount attempt during Drop (EINVAL on macOS).
    let requested_shutdown = SHUTDOWN_REQUESTED.load(Ordering::Acquire);
    if requested_shutdown {
        // Explicitly drop the FUSE session to trigger fuser/OS cleanup before hard
        // exit. Without this, libfuse resources can remain partially alive and
        // abort on process shutdown.
        drop(session);
    } else {
        std::mem::forget(session);
    }
    tracing::info!("shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MountOptionError, build_mount_options, build_mount_options_with_allow_other,
        validate_volume_name,
    };

    fn mount_options_include_allow_other(options: &[fuser::MountOption]) -> bool {
        options
            .iter()
            .any(|opt| matches!(opt, fuser::MountOption::AllowOther))
    }

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
        let options =
            build_mount_options_with_allow_other("studiofs", false, false, false).expect("options");
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::FSName(name) if name == "studiofs"))
        );
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::AutoUnmount))
        );
        #[cfg(target_os = "macos")]
        assert!(mount_options_include_allow_other(&options));
        #[cfg(target_os = "linux")]
        assert!(!mount_options_include_allow_other(&options));
        assert!(!options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn build_mount_options_adds_allow_other_when_opted_in_on_linux() {
        let options =
            build_mount_options_with_allow_other("studiofs", false, false, true).expect("options");
        assert!(mount_options_include_allow_other(&options));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_mount_options_includes_foreground_when_requested() {
        let options =
            build_mount_options_with_allow_other("studiofs", true, false, false).expect("options");
        assert!(options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn build_mount_options_foreground_not_added_on_linux() {
        // On Linux, fusermount3 does not accept 'foreground' as a mount option.
        // fuser keeps the process in the foreground via session.run() itself.
        let options =
            build_mount_options_with_allow_other("studiofs", true, false, false).expect("options");
        assert!(!options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_mount_options_sets_macos_volume_name() {
        let options = build_mount_options_with_allow_other("studiofs-mac", false, false, false)
            .expect("options");
        assert!(options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "volname=studiofs-mac")
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_mount_options_sets_macos_local_flag() {
        let options = build_mount_options_with_allow_other("studiofs-mac", false, false, false)
            .expect("options");
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "local"))
        );
    }
}
