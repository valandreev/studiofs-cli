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
mod low_level;

/// Linux libfuse3 connection-level queue bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinuxFuseConnectionConfig {
    /// Maximum number of background kernel requests queued to the daemon.
    pub max_background: u32,
    /// Kernel congestion threshold for applying request backpressure.
    pub congestion_threshold: u32,
}

impl Default for LinuxFuseConnectionConfig {
    fn default() -> Self {
        Self {
            max_background: 128,
            congestion_threshold: 96,
        }
    }
}

impl LinuxFuseConnectionConfig {
    fn validate(self) -> io::Result<Self> {
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
}

/// Request used by the CLI backend router to launch the Linux libfuse3 bridge.
pub struct LinuxFuseMountRequest {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    connection_config: LinuxFuseConnectionConfig,
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
        }
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
    } = request;
    let connection_config = connection_config.validate()?;
    let mut session = low_level::Session::mount(
        low_level::SessionState::new(vfs, runtime_handle, read_only, connection_config),
        mount_point,
        volume_name,
    )?;
    session.install_signal_handlers()?;
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
        };

        assert_eq!(
            config.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn connection_config_rejects_threshold_above_max_background() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 8,
            congestion_threshold: 9,
        };

        assert_eq!(
            config.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
