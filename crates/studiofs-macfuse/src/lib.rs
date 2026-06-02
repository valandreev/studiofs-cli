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

/// Low-level macFUSE repeated-read probe support.
pub mod probe;

/// Isolated low-level macFUSE session and reply boundary.
pub mod low_level;

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

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn callback_file_info_snapshot_is_independent_from_callback_pointer() {
        let mut raw = super::low_level::raw::FuseFileInfo::for_test(0x20, 7, true, false);

        let snapshot = super::low_level::FileInfoSnapshot::copy_from_callback(&raw);
        raw.fh = 99;
        raw.flags = 0x40;
        raw.set_cache_flags_for_test(false, true);

        assert_eq!(snapshot.file_handle(), 7);
        assert_eq!(snapshot.flags(), 0x20);
        assert!(snapshot.direct_io());
        assert!(!snapshot.keep_cache());
    }

    #[test]
    fn directory_entries_keep_names_and_offsets_in_owned_buffer() {
        let mut buffer = super::low_level::DirectoryEntryBuffer::new(512);

        buffer
            .push_for_test(2, OsStr::new("alpha"), 1)
            .expect("first entry fits");
        buffer
            .push_for_test(3, OsStr::from_bytes(b"beta.bin"), 2)
            .expect("second entry fits");

        assert_eq!(buffer.len(), buffer.as_slice().len());
        assert!(
            buffer
                .as_slice()
                .windows(b"alpha".len())
                .any(|w| w == b"alpha")
        );
        assert!(
            buffer
                .as_slice()
                .windows(b"beta.bin".len())
                .any(|w| w == b"beta.bin")
        );
    }

    #[test]
    fn session_config_builds_owned_mount_arguments() {
        let config = super::low_level::SessionConfig::new("/Volumes/Studio FS", "StudioFS Native")
            .with_read_only(true)
            .with_run_in_foreground(true);

        let args = config.argv_strings();

        assert_eq!(
            config.mount_point().as_os_str(),
            OsStr::new("/Volumes/Studio FS")
        );
        assert_eq!(config.volume_name(), "StudioFS Native");
        assert!(config.run_in_foreground());
        assert!(args.iter().any(|arg| arg == "studiofs-macfuse"));
        assert!(!args.iter().any(|arg| arg == "-f"));
        assert!(args.iter().any(|arg| arg == "-oro"));
        assert!(!args.iter().any(|arg| arg.contains("StudioFS Native")));
    }

    #[test]
    fn empty_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::empty();

        assert!(ops.size() > 0);
        assert!(!ops.as_ptr().is_null());
    }

    #[test]
    fn low_level_session_mount_unmount_smoke_is_gated() {
        if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
            return;
        }

        let mount_point = std::path::Path::new("target/studiofs-macfuse-mount-smoke");
        let _ = std::fs::remove_dir(mount_point);
        std::fs::create_dir_all(mount_point).expect("create macFUSE smoke mount point");
        let config = super::low_level::SessionConfig::new(mount_point, "StudioFS Smoke")
            .with_run_in_foreground(true);

        // SAFETY: The empty operation table is a static header-derived table
        // used only for a mount/unmount lifecycle smoke; no callbacks are run.
        let session = unsafe {
            super::low_level::Session::mount_with_ops(
                &config,
                super::low_level::LowLevelOperations::empty(),
                std::ptr::null_mut(),
            )
        }
        .expect("mount empty low-level macFUSE session");
        session.request_exit();
        drop(session);
        std::fs::remove_dir(mount_point).expect("remove macFUSE smoke mount point");
    }
}
