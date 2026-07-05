#![doc = "Linux libfuse3 bridge boundary for StudioFS."]
#![cfg(target_os = "linux")]
#![deny(missing_docs)]

//! Linux libfuse3 bridge boundary for StudioFS.

use std::ffi::CString;
use std::io;
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;
use std::ptr::NonNull;
mod low_level;

/// Request used by the CLI backend router to launch the Linux libfuse3 bridge.
pub struct LinuxFuseMountRequest {
    mount_point: PathBuf,
    volume_name: String,
}

impl LinuxFuseMountRequest {
    /// Create a Linux libfuse3 mount request.
    pub fn new(mount_point: impl Into<PathBuf>, volume_name: impl Into<String>) -> Self {
        Self {
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
        }
    }
}

/// Start serving the Linux libfuse3 bridge.
pub fn mount_with_request(request: LinuxFuseMountRequest) -> io::Result<()> {
    ensure_fusermount3_available()?;
    let LinuxFuseMountRequest {
        mount_point,
        volume_name,
    } = request;
    let mut session = low_level::Session::mount(mount_point, volume_name)?;
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
}
