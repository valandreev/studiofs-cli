//! StudioFS raw `/dev/fuse` backend for Linux.
//!
//! # Mount options
//!
//! The `STUDIOFS_FUSE_ALLOW_OTHER=1` environment variable enables unrestricted
//! FUSE session ACLs.

#![cfg(target_os = "linux")]

mod raw;

use std::sync::atomic::{AtomicBool, Ordering};

pub use raw::{RawFuseMountRequest, mount_raw_with_request};

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

pub(crate) fn signal_handlers_for_policy(
    signal_handling: SignalHandling,
) -> std::io::Result<Option<CliSignalHandlers>> {
    match signal_handling {
        SignalHandling::Disabled => Ok(None),
        SignalHandling::InstallForCli => CliSignalHandlers::install().map(Some),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::SignalHandling;
    use super::raw::{
        FUSE_ASYNC_READ, FUSE_AUTO_INVAL_DATA, FUSE_BIG_WRITES, FUSE_EXPLICIT_INVAL_DATA,
        FUSE_FILE_OPS, FUSE_MAX_PAGES, FUSE_PARALLEL_DIROPS, FUSE_READDIRPLUS,
        FUSE_READDIRPLUS_AUTO, FUSE_WRITEBACK_CACHE, InitIn, LinuxRawFuseProfile,
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

    #[test]
    fn raw_mount_request_defaults_to_library_owned_signal_policy() {
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

    #[test]
    fn raw_init_negotiation_uses_macfuse_like_linux_profile() {
        let profile = LinuxRawFuseProfile::macfuse_like();
        let input = InitIn {
            major: 7,
            minor: 38,
            max_readahead: 2 * 1024 * 1024,
            flags: FUSE_ASYNC_READ
                | FUSE_BIG_WRITES
                | FUSE_MAX_PAGES
                | FUSE_PARALLEL_DIROPS
                | FUSE_READDIRPLUS
                | FUSE_READDIRPLUS_AUTO
                | FUSE_FILE_OPS
                | FUSE_AUTO_INVAL_DATA
                | FUSE_EXPLICIT_INVAL_DATA
                | FUSE_WRITEBACK_CACHE,
        };

        let negotiated = profile.negotiate_init(input);

        assert_eq!(negotiated.out.max_write, profile.max_write);
        assert_eq!(negotiated.out.max_pages, profile.max_pages);
        assert_eq!(negotiated.out.max_background, profile.max_background);
        assert_eq!(
            negotiated.out.congestion_threshold,
            profile.congestion_threshold
        );
        assert_eq!(negotiated.out.max_readahead, profile.max_readahead);
        assert_eq!(
            negotiated.out.flags,
            FUSE_ASYNC_READ
                | FUSE_BIG_WRITES
                | FUSE_MAX_PAGES
                | FUSE_PARALLEL_DIROPS
                | FUSE_READDIRPLUS
                | FUSE_READDIRPLUS_AUTO
                | FUSE_FILE_OPS
                | FUSE_EXPLICIT_INVAL_DATA
        );
        assert_eq!(negotiated.out.flags & FUSE_READDIRPLUS, FUSE_READDIRPLUS);
        assert_eq!(
            negotiated.out.flags & FUSE_READDIRPLUS_AUTO,
            FUSE_READDIRPLUS_AUTO
        );
    }

    #[test]
    fn raw_init_negotiation_degrades_unadvertised_capabilities() {
        let profile = LinuxRawFuseProfile::macfuse_like();
        let input = InitIn {
            major: 7,
            minor: 23,
            max_readahead: 64 * 1024,
            flags: FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_AUTO_INVAL_DATA,
        };

        let negotiated = profile.negotiate_init(input);

        assert_eq!(negotiated.out.max_readahead, 64 * 1024);
        assert_eq!(negotiated.out.max_write, profile.max_write);
        assert_eq!(negotiated.out.max_pages, 0);
        assert_eq!(
            negotiated.out.flags,
            FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_AUTO_INVAL_DATA
        );
        assert!(negotiated.degraded.contains(&FUSE_MAX_PAGES));
        assert!(negotiated.degraded.contains(&FUSE_PARALLEL_DIROPS));
        assert!(negotiated.degraded.contains(&FUSE_READDIRPLUS));
        assert!(negotiated.degraded.contains(&FUSE_READDIRPLUS_AUTO));
        assert!(negotiated.degraded.contains(&FUSE_FILE_OPS));
    }
}
