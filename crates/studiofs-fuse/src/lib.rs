//! StudioFS FUSE backend for Linux using `fuser`.
//!
//! Implements `fuser::Filesystem` to expose `studiofs_vfs::StudioVfs` via FUSE.
//!
//! # Mount options
//!
//! The `STUDIOFS_FUSE_ALLOW_OTHER=1` environment variable enables the
//! `AllowOther` mount option on Linux.

#![cfg(target_os = "linux")]

mod fs;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

/// A `fuser::Notifier`-backed cache invalidator used by `studiofs_vfs` to
/// proactively clear kernel FUSE dentry/attribute caches.
pub use fs::FuseKernelInvalidator;
/// The `fuser::Filesystem` implementation that bridges StudioFS VFS operations
/// into FUSE callbacks.
pub use fs::{StudioFuseAsyncWriteAdmissionLimits, StudioFuseFs, StudioFuseRuntimeOptions};
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
    read_only: bool,
) -> Result<Vec<fuser::MountOption>, MountOptionError> {
    build_mount_options_with_allow_other(
        volume_name,
        read_only,
        std::env::var("STUDIOFS_FUSE_ALLOW_OTHER").as_deref() == Ok("1"),
    )
}

fn build_mount_options_with_allow_other(
    volume_name: &str,
    read_only: bool,
    allow_other_env: bool,
) -> Result<Vec<fuser::MountOption>, MountOptionError> {
    build_mount_options_inner(volume_name, read_only, allow_other_env)
}

fn build_mount_options_inner(
    volume_name: &str,
    read_only: bool,
    allow_other_env: bool,
) -> Result<Vec<fuser::MountOption>, MountOptionError> {
    validate_volume_name(volume_name)?;
    let mut options = vec![fuser::MountOption::FSName(volume_name.into())];
    // On Linux, fuser's auto_unmount helper can leave CLI SIGTERM shutdowns
    // attached until the external watchdog sends SIGKILL. Let Mount::drop use
    // the explicit lazy fusermount/umount path instead.
    if !cfg!(target_os = "linux") {
        options.push(fuser::MountOption::AutoUnmount);
    }
    if allow_other_env {
        options.push(fuser::MountOption::AllowOther);
    }
    if read_only {
        options.push(fuser::MountOption::RO);
    }
    Ok(options)
}

// ─── Signal Handling ─────────────────────────────────────────────────────────

/// Flag set by the SIGINT/SIGTERM handler.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static CLI_SIGNAL_HANDLERS_INSTALLED: AtomicBool = AtomicBool::new(false);

extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

/// Controls whether StudioFS owns process-global FUSE shutdown signals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SignalHandling {
    /// Do not install SIGINT/SIGTERM handlers. Embedders remain responsible for
    /// unmounting through their own process lifecycle.
    #[default]
    Disabled,
    /// Install temporary SIGINT/SIGTERM handlers for the StudioFS CLI.
    ///
    /// This mode mutates process-global Unix signal dispositions and only one
    /// active mount in the current process may use it at a time.
    InstallForCli,
}

/// Mount request for the FUSE bridge.
pub struct FuseMountRequest {
    /// Shared VFS instance exposed through FUSE callbacks.
    pub vfs: Arc<studiofs_vfs::StudioVfs>,
    /// Tokio runtime handle used to execute VFS async operations from FUSE callbacks.
    pub runtime_handle: tokio::runtime::Handle,
    /// Filesystem mount point path.
    pub mount_point: String,
    /// Volume name passed to the FUSE mount options.
    pub volume_name: String,
    /// Whether the mounted filesystem should reject mutating operations.
    pub read_only: bool,
    /// Process signal ownership policy for this mount.
    pub signal_handling: SignalHandling,
    /// Optional caller-owned signal used to request a graceful in-process unmount.
    pub unmount_signal: Option<FuseUnmountSignal>,
}

impl FuseMountRequest {
    /// Create a FUSE mount request with embeddable defaults.
    pub fn new(
        vfs: Arc<studiofs_vfs::StudioVfs>,
        runtime_handle: tokio::runtime::Handle,
        mount_point: impl Into<String>,
        volume_name: impl Into<String>,
    ) -> Self {
        Self {
            vfs,
            runtime_handle,
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            signal_handling: SignalHandling::default(),
            unmount_signal: None,
        }
    }
}

/// Caller-owned handle for requesting graceful FUSE unmount without process signals.
#[derive(Clone, Debug, Default)]
pub struct FuseUnmountSignal {
    requested: Arc<AtomicBool>,
}

impl FuseUnmountSignal {
    /// Create a new unmount signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request the mount session to unmount.
    pub fn request_unmount(&self) {
        self.requested.store(true, Ordering::Release);
    }

    /// Return whether unmount has been requested.
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

struct CliSignalHandlers {
    previous_sigint: libc::sighandler_t,
    previous_sigterm: libc::sighandler_t,
}

impl CliSignalHandlers {
    fn install() -> std::io::Result<Self> {
        CLI_SIGNAL_HANDLERS_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "FUSE CLI signal handlers are already installed",
                )
            })?;
        // SAFETY: `libc::signal` mutates process-global signal disposition.
        // This path is explicit CLI policy, and the previous handlers are kept
        // so cleanup restores the host disposition instead of SIG_DFL.
        unsafe {
            let previous_sigint = libc::signal(
                libc::SIGINT,
                shutdown_signal_handler as *const () as libc::sighandler_t,
            );
            if previous_sigint == libc::SIG_ERR {
                CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
                return Err(std::io::Error::last_os_error());
            }
            let previous_sigterm = libc::signal(
                libc::SIGTERM,
                shutdown_signal_handler as *const () as libc::sighandler_t,
            );
            if previous_sigterm == libc::SIG_ERR {
                let _ = libc::signal(libc::SIGINT, previous_sigint);
                CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                previous_sigint,
                previous_sigterm,
            })
        }
    }
}

impl Drop for CliSignalHandlers {
    fn drop(&mut self) {
        // SAFETY: these values came from successful `libc::signal` calls in
        // `install`, so they are valid signal dispositions to restore.
        unsafe {
            libc::signal(libc::SIGINT, self.previous_sigint);
            libc::signal(libc::SIGTERM, self.previous_sigterm);
        }
        CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
    }
}

fn signal_handlers_for_policy(
    signal_handling: SignalHandling,
) -> std::io::Result<Option<CliSignalHandlers>> {
    match signal_handling {
        SignalHandling::Disabled => Ok(None),
        SignalHandling::InstallForCli => CliSignalHandlers::install().map(Some),
    }
}

struct SignalWatcher {
    stop: Arc<AtomicBool>,
    handlers: Arc<Mutex<Option<CliSignalHandlers>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SignalWatcher {
    fn restore_handlers(&self) {
        let _ = self.handlers.lock().expect("signal handler lock").take();
    }
}

impl Drop for SignalWatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.restore_handlers();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Spawn a background thread that watches for SIGINT/SIGTERM and triggers
/// an in-process unmount so `fuser::Session::run` returns cleanly and
/// the VFS shutdown/drain logic can execute.
fn spawn_signal_watcher(
    mount_point: String,
    mut unmount: fuser::SessionUnmounter,
    signal_handling: SignalHandling,
    unmount_signal: Option<FuseUnmountSignal>,
) -> std::io::Result<SignalWatcher> {
    let mp = mount_point.to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let handlers = Arc::new(Mutex::new(signal_handlers_for_policy(signal_handling)?));
    let watcher_stop = Arc::clone(&stop);
    let watcher_handlers = Arc::clone(&handlers);
    let thread = std::thread::Builder::new()
        .name("signal-watcher".into())
        .spawn(move || {
            // Poll the atomic flag until a signal arrives.
            while !watcher_stop.load(Ordering::Acquire)
                && !SHUTDOWN_REQUESTED.load(Ordering::Acquire)
                && !unmount_signal
                    .as_ref()
                    .is_some_and(FuseUnmountSignal::is_requested)
            {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            if watcher_stop.load(Ordering::Acquire) {
                return;
            }
            SHUTDOWN_REQUESTED.store(true, Ordering::Release);
            tracing::info!("received shutdown request, unmounting {mp} …");
            let _ = watcher_handlers.lock().expect("signal handler lock").take();
            let _ = unmount.unmount();
        })?;
    Ok(SignalWatcher {
        stop,
        handlers,
        thread: Some(thread),
    })
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Mount and serve the FUSE filesystem (blocks until unmounted).
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    runtime_handle: tokio::runtime::Handle,
    mount_point: &str,
    volume_name: &str,
    read_only: bool,
) -> std::io::Result<()> {
    let mut request = FuseMountRequest::new(vfs, runtime_handle, mount_point, volume_name);
    request.read_only = read_only;
    mount_with_request(request)
}

/// Mount and serve a FUSE filesystem with explicit library/CLI ownership policy.
pub fn mount_with_request(request: FuseMountRequest) -> std::io::Result<()> {
    let FuseMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        signal_handling,
        unmount_signal,
    } = request;
    SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    let fs = StudioFuseFs::new(vfs.clone(), runtime_handle);
    // Do NOT add DefaultPermissions here: it would make the kernel enforce
    // POSIX mode/uid/gid from getattr() in-kernel.  Old volumes have the
    // root inode stored with uid=0/gid=0 (historic default), so the kernel
    // would see the mounting user as "other" (r-x only) and block writes.
    // Instead we implement access() -> ok() so the FUSE daemon decides.
    let options = build_mount_options(&volume_name, read_only)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    let shutdown_hint = match signal_handling {
        SignalHandling::Disabled => "unmount through the host lifecycle",
        SignalHandling::InstallForCli => "press Ctrl+C to unmount",
    };
    tracing::info!("mounting StudioFS at {mount_point}, {shutdown_hint}");

    // Use Session::new + run() instead of fuser::mount2 so we can obtain a
    // Notifier. The notifier allows VFS metadata refresh paths to
    // proactively clear the kernel FUSE dentry/attribute cache, making remote
    // creates/deletes visible immediately without waiting for TTL expiry.
    let mut session = fuser::Session::new(fs, mount_point.as_ref() as &std::path::Path, &options)?;
    let unmount = session.unmount_callable();
    let signal_watcher = spawn_signal_watcher(
        mount_point.clone(),
        unmount,
        signal_handling,
        unmount_signal,
    )?;
    let notifier = session.notifier();

    // Register the kernel cache invalidator with the VFS.
    let invalidator = std::sync::Arc::new(FuseKernelInvalidator::new(notifier));
    vfs.set_kernel_invalidator(invalidator);

    session.run()?;
    drop(signal_watcher);

    // Session::run returns when the filesystem is unmounted.
    vfs.clear_kernel_invalidator();
    tracing::info!("unmounted, finalizing FUSE session cleanup …");
    // If we arrived here without SIGINT/SIGTERM, the mount point was likely
    // detached externally. Skip dropping the session object to avoid a second
    // unmount attempt during Drop.
    let requested_shutdown = SHUTDOWN_REQUESTED.load(Ordering::Acquire);
    if requested_shutdown {
        // Explicitly drop the FUSE session to trigger fuser/OS cleanup before hard exit.
        drop(session);
    } else {
        std::mem::forget(session);
    }
    tracing::info!("shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        FuseUnmountSignal, MountOptionError, SignalHandling, build_mount_options_with_allow_other,
        validate_volume_name,
    };
    static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct SignalDispositionGuard {
        sigint: libc::sighandler_t,
        sigterm: libc::sighandler_t,
    }

    impl SignalDispositionGuard {
        fn replace_with(handler: libc::sighandler_t) -> Self {
            // SAFETY: signal-disposition tests hold `SIGNAL_TEST_LOCK` and
            // restore the original dispositions through this guard's `Drop`.
            let (sigint, sigterm) = unsafe {
                (
                    libc::signal(libc::SIGINT, handler),
                    libc::signal(libc::SIGTERM, handler),
                )
            };
            Self { sigint, sigterm }
        }
    }

    impl Drop for SignalDispositionGuard {
        fn drop(&mut self) {
            // SAFETY: these dispositions were returned by `libc::signal` when
            // the guard was created.
            unsafe {
                libc::signal(libc::SIGINT, self.sigint);
                libc::signal(libc::SIGTERM, self.sigterm);
            }
        }
    }

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
            build_mount_options_with_allow_other("studiofs", false, false).expect("options");
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::FSName(name) if name == "studiofs"))
        );
        #[cfg(target_os = "linux")]
        assert!(
            !options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::AutoUnmount))
        );
        #[cfg(not(target_os = "linux"))]
        assert!(
            options
                .iter()
                .any(|opt| matches!(opt, fuser::MountOption::AutoUnmount))
        );
        #[cfg(target_os = "linux")]
        assert!(!mount_options_include_allow_other(&options));
        assert!(!options.iter().any(
            |opt| matches!(opt, fuser::MountOption::CUSTOM(custom) if custom == "foreground")
        ));
    }

    #[test]
    fn fuse_mount_request_defaults_to_library_owned_signal_policy() {
        assert_eq!(SignalHandling::default(), SignalHandling::Disabled);
    }

    #[test]
    fn disabled_signal_policy_does_not_install_process_handlers() {
        let _guard = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        extern "C" fn previous_handler(_sig: libc::c_int) {}

        let _signals = SignalDispositionGuard::replace_with(
            previous_handler as *const () as libc::sighandler_t,
        );

        let handlers = super::signal_handlers_for_policy(SignalHandling::Disabled)
            .expect("disabled signal policy");
        assert!(handlers.is_none());

        unsafe {
            assert_eq!(
                libc::signal(
                    libc::SIGINT,
                    previous_handler as *const () as libc::sighandler_t
                ),
                previous_handler as *const () as libc::sighandler_t
            );
            assert_eq!(
                libc::signal(
                    libc::SIGTERM,
                    previous_handler as *const () as libc::sighandler_t
                ),
                previous_handler as *const () as libc::sighandler_t
            );
        }
    }

    #[test]
    fn fuse_unmount_signal_records_programmatic_unmount_request() {
        let signal = FuseUnmountSignal::new();
        assert!(!signal.is_requested());

        let cloned = signal.clone();
        cloned.request_unmount();

        assert!(signal.is_requested());
    }

    #[test]
    fn cli_signal_handler_restores_previous_signal_dispositions() {
        let _guard = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        extern "C" fn previous_handler(_sig: libc::c_int) {}

        let _signals = SignalDispositionGuard::replace_with(
            previous_handler as *const () as libc::sighandler_t,
        );

        let previous = super::CliSignalHandlers::install().expect("install signal handlers");
        drop(previous);

        unsafe {
            assert_eq!(
                libc::signal(libc::SIGINT, libc::SIG_DFL),
                previous_handler as *const () as libc::sighandler_t
            );
            assert_eq!(
                libc::signal(libc::SIGTERM, libc::SIG_DFL),
                previous_handler as *const () as libc::sighandler_t
            );
        }
    }

    #[test]
    fn cli_signal_handler_rejects_concurrent_install() {
        let _guard = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let first = super::CliSignalHandlers::install().expect("first signal handler install");

        let err = match super::CliSignalHandlers::install() {
            Ok(_) => panic!("second install should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        drop(first);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn build_mount_options_adds_allow_other_when_opted_in_on_linux() {
        let options =
            build_mount_options_with_allow_other("studiofs", false, true).expect("options");
        assert!(mount_options_include_allow_other(&options));
    }
}
