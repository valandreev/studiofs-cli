#![doc = "Linux libfuse3 bridge boundary for StudioFS."]
#![cfg(target_os = "linux")]
#![deny(missing_docs)]

//! Linux libfuse3 bridge boundary for StudioFS.

use std::ffi::CString;
use std::io;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::Arc;
use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;
mod async_writes;
mod cache_policy;
mod directory_cache;
mod invalidation;
mod low_level;

/// Linux libfuse3 queue bounds and callback worker policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinuxFuseConnectionConfig {
    /// Maximum number of background kernel requests queued to the daemon.
    pub max_background: u32,
    /// Kernel congestion threshold for applying request backpressure.
    pub congestion_threshold: u32,
    /// Requested callback workers; `None` uses the queue-derived modern default.
    /// Positive values are capped at `max_background` and 128 workers.
    /// Explicit overrides require libfuse 3.12+; older builds keep CPU-based idle retention.
    pub worker_threads: Option<u32>,
}

impl Default for LinuxFuseConnectionConfig {
    fn default() -> Self {
        Self {
            max_background: 128,
            congestion_threshold: 96,
            worker_threads: None,
        }
    }
}

impl LinuxFuseConnectionConfig {
    fn validate(self, hard_worker_limit: bool) -> io::Result<Self> {
        if self.worker_threads == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Linux FUSE worker_threads must be greater than 0",
            ));
        }
        if self.worker_threads.is_some() && !hard_worker_limit {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Linux FUSE worker_threads requires libfuse 3.12+ for a hard worker limit",
            ));
        }
        if self.max_background == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Linux FUSE max_background must be greater than 0",
            ));
        }
        if self.congestion_threshold == 0 || self.congestion_threshold > self.max_background {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Linux FUSE congestion_threshold must be in 1..=max_background",
            ));
        }
        Ok(self)
    }

    pub(crate) fn worker_thread_count(self, hard_worker_limit: bool) -> u32 {
        if !hard_worker_limit {
            let parallelism = std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1);
            return u32::try_from(parallelism)
                .unwrap_or(u32::MAX)
                .saturating_mul(2)
                .clamp(1, self.max_background.max(1));
        }
        // More workers increased retained read-buffer RSS in the mounted 64-reader A/B gate.
        self.worker_threads
            .unwrap_or(16)
            .clamp(1, self.max_background.clamp(1, 128))
    }
}

fn parse_worker_threads(value: Option<std::ffi::OsString>) -> io::Result<Option<u32>> {
    value
        .map(|value| {
            value
                .to_str()
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|count| *count > 0)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "STUDIOFS_LINUX_FUSE_WORKER_THREADS must be a positive u32",
                    )
                })
        })
        .transpose()
}

/// Request used by the CLI backend router to launch the Linux libfuse3 bridge.
pub struct LinuxFuseMountRequest {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    connection_config: LinuxFuseConnectionConfig,
    mount_ready_callback: Option<Box<dyn FnOnce() + Send>>,
}

impl LinuxFuseMountRequest {
    /// Create a Linux libfuse3 mount request.
    pub fn new(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        mount_point: impl Into<PathBuf>,
        volume_name: impl Into<String>,
        read_only: bool,
    ) -> Self {
        Self {
            vfs,
            runtime_handle,
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only,
            connection_config: LinuxFuseConnectionConfig::default(),
            mount_ready_callback: None,
        }
    }

    /// Set a callback invoked after libfuse has attached the mount point.
    pub fn with_mount_ready_callback<F>(mut self, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        self.mount_ready_callback = Some(Box::new(callback));
        self
    }

    /// Override Linux libfuse3 connection-level queue bounds.
    pub fn with_connection_config(mut self, config: LinuxFuseConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }
}

/// Start serving the Linux libfuse3 bridge.
pub fn mount_with_request(request: LinuxFuseMountRequest) -> io::Result<()> {
    ensure_fusermount3_available()?;
    let LinuxFuseMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        connection_config,
        mount_ready_callback,
    } = request;
    let mut connection_config = connection_config;
    if connection_config.worker_threads.is_none() {
        connection_config.worker_threads =
            parse_worker_threads(std::env::var_os("STUDIOFS_LINUX_FUSE_WORKER_THREADS"))?;
    }
    let connection_config = connection_config.validate(low_level::has_worker_limit())?;
    let mut session = low_level::Session::mount(
        low_level::SessionState::new(vfs, runtime_handle, read_only, connection_config),
        mount_point,
        volume_name,
    )?;
    session.install_signal_handlers()?;
    if let Some(callback) = mount_ready_callback {
        callback();
    }
    session.run()
}

fn ensure_fusermount3_available() -> io::Result<()> {
    check_fusermount3_in_path(std::env::var_os("PATH"))
}

fn check_fusermount3_in_path(path_var: Option<std::ffi::OsString>) -> io::Result<()> {
    let path_var = path_var.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "fusermount3 is required for Linux StudioFS mounts; PATH is not set",
        )
    })?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join("fusermount3");
        if candidate.is_file() {
            return Ok(());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "fusermount3 is required for Linux StudioFS mounts; install the libfuse3 runtime package",
    ))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    #[test]
    fn fusermount3_check_reports_missing_path() {
        let err = super::check_fusermount3_in_path(None).expect_err("missing PATH should fail");

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(err.to_string().contains("fusermount3"));
    }

    #[test]
    fn fusermount3_check_reports_path_without_binary() {
        let err = super::check_fusermount3_in_path(Some(OsString::from("/definitely/missing")))
            .expect_err("missing fusermount3 should fail");

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(err.to_string().contains("fusermount3"));
    }

    #[test]
    fn connection_config_rejects_zero_max_background() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 0,
            congestion_threshold: 1,
            worker_threads: None,
        };

        assert_eq!(
            config.validate(true).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn connection_config_rejects_threshold_above_max_background() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 8,
            congestion_threshold: 9,
            worker_threads: None,
        };

        assert_eq!(
            config.validate(true).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn worker_threads_rejects_invalid_environment_values() {
        for value in ["", "0", "-1", "threads", "4294967296"] {
            assert_eq!(
                super::parse_worker_threads(Some(std::ffi::OsString::from(value)))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn worker_threads_accepts_positive_override_and_absent_default() {
        assert_eq!(super::parse_worker_threads(None).unwrap(), None);
        assert_eq!(
            super::parse_worker_threads(Some(std::ffi::OsString::from("48"))).unwrap(),
            Some(48)
        );
    }

    #[test]
    fn connection_config_rejects_zero_worker_threads() {
        let config = super::LinuxFuseConnectionConfig {
            worker_threads: Some(0),
            ..Default::default()
        };
        assert_eq!(
            config.validate(true).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn legacy_libfuse_rejects_explicit_worker_limit_but_accepts_default() {
        let config = super::LinuxFuseConnectionConfig {
            worker_threads: Some(16),
            ..Default::default()
        };
        assert_eq!(
            config.validate(false).unwrap_err().kind(),
            std::io::ErrorKind::Unsupported
        );
        assert!(
            super::LinuxFuseConnectionConfig::default()
                .validate(false)
                .is_ok()
        );
    }

    #[test]
    fn worker_threads_caps_oversized_configuration_and_preserves_small_override() {
        for (requested, expected) in [(1, 1), (48, 48), (u32::MAX, 128)] {
            let config = super::LinuxFuseConnectionConfig {
                max_background: 1024,
                worker_threads: Some(requested),
                ..Default::default()
            };
            assert_eq!(
                config.validate(true).unwrap().worker_thread_count(true),
                expected
            );
        }
    }

    #[test]
    fn modern_default_worker_count_stays_bounded_for_large_queues() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 1024,
            ..Default::default()
        };
        assert_eq!(config.worker_thread_count(true), 16);
    }
}
