#![cfg(target_os = "macos")]
#![deny(missing_docs)]

//! macOS native bridge boundary for StudioFS.
//!
//! This crate intentionally exposes only the mount request and serve API while
//! the low-level macFUSE session layer is still being built.

use std::io;
use std::sync::Arc;

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

/// Request used by the CLI backend router to launch the macOS bridge.
pub struct MacFuseMountRequest {
    /// Shared StudioFS VFS instance served by the macOS bridge.
    pub vfs: Arc<StudioVfs>,
    /// Tokio runtime handle used by bridge callbacks to enter async VFS work.
    pub runtime_handle: Handle,
    /// Filesystem path where the volume should be mounted.
    pub mount_point: String,
    /// Human-readable volume name exposed by the mounted filesystem.
    pub volume_name: String,
    /// Whether the bridge should reject write operations.
    pub read_only: bool,
    /// Whether the bridge should keep the mount session attached to the CLI process.
    pub run_in_foreground: bool,
}

/// Start serving the macOS native bridge.
///
/// Real macFUSE callbacks are tracked by the follow-up macOS bridge issues.
pub fn mount_with_request(_request: MacFuseMountRequest) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "studiofs-macfuse is an opt-in stub; native macFUSE callbacks are tracked by SFS-479/SFS-480",
    ))
}
