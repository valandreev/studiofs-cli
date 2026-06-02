#![cfg(target_os = "macos")]
#![deny(missing_docs)]

//! macOS native bridge boundary for StudioFS.
//!
//! This crate intentionally exposes only the mount request and serve API while
//! the low-level macFUSE session layer is still being built.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

/// Request used by the CLI backend router to launch the macOS bridge.
pub struct MacFuseMountRequest {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    run_in_foreground: bool,
}

impl MacFuseMountRequest {
    /// Create a macFUSE mount request with embeddable defaults.
    pub fn new(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        mount_point: impl Into<PathBuf>,
        volume_name: impl Into<String>,
    ) -> Self {
        Self {
            vfs,
            runtime_handle,
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            run_in_foreground: false,
        }
    }

    /// Set whether the bridge should reject write operations.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set whether the mount session should stay attached to the CLI process.
    pub fn with_run_in_foreground(mut self, run_in_foreground: bool) -> Self {
        self.run_in_foreground = run_in_foreground;
        self
    }
}

/// Start serving the macOS native bridge.
///
/// Real macFUSE callbacks are tracked by the follow-up macOS bridge issues.
pub fn mount_with_request(request: MacFuseMountRequest) -> io::Result<()> {
    let MacFuseMountRequest {
        vfs: _vfs,
        runtime_handle: _runtime_handle,
        mount_point: _mount_point,
        volume_name: _volume_name,
        read_only: _read_only,
        run_in_foreground: _run_in_foreground,
    } = request;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "studiofs-macfuse is an opt-in stub; native macFUSE callbacks are tracked by SFS-479/SFS-480",
    ))
}
