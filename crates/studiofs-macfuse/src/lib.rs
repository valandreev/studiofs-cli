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

mod adapter;

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
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        run_in_foreground,
    } = request;
    if !read_only {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "studiofs-macfuse currently supports only explicit read-only mounts",
        ));
    }

    let config = low_level::SessionConfig::new(mount_point, volume_name)
        .with_read_only(true)
        .with_run_in_foreground(run_in_foreground);
    let mut callback_state = Box::new(adapter::CallbackState::new(vfs, runtime_handle));
    let userdata =
        (&mut *callback_state) as *mut adapter::CallbackState as *mut std::os::raw::c_void;
    // SAFETY: `callback_state` remains boxed and live until after `session.run`
    // returns. The read-only callback table handles this exact userdata type.
    let session = unsafe {
        low_level::Session::mount_with_ops(
            &config,
            low_level::LowLevelOperations::read_only_studio_vfs(),
            userdata,
        )
    }?;
    let result = session.run();
    drop(callback_state);
    result
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
    fn session_config_builds_low_level_mount_arguments_without_kernel_read_only_option() {
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
        assert!(!args.iter().any(|arg| arg == "-oro"));
        assert!(!args.iter().any(|arg| arg == "ro"));
        assert!(!args.iter().any(|arg| arg.contains("StudioFS Native")));
    }

    #[test]
    fn empty_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::empty();

        assert!(ops.size() > 0);
        assert!(!ops.as_ptr().is_null());
    }

    #[test]
    fn read_only_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::read_only_studio_vfs();

        assert!(ops.size() > 0);
        assert!(!ops.as_ptr().is_null());
    }

    #[test]
    fn low_level_session_can_be_signaled_across_threads() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<super::low_level::Session>();
    }

    #[test]
    fn low_level_request_can_move_to_worker_thread() {
        fn assert_send<T: Send>() {}

        assert_send::<super::low_level::Request>();
    }

    #[test]
    fn directory_entry_buffer_storage_is_eight_byte_aligned() {
        let buffer = super::low_level::DirectoryEntryBuffer::new(512);

        assert_eq!(buffer.as_slice().as_ptr().align_offset(8), 0);
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

    #[test]
    fn read_only_adapter_reads_existing_vfs_payload_and_records_diagnostics() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-read-only-adapter-read",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"hello.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"hello from macFUSE"))
            .expect("seed payload");
        vfs.reset_diagnostics_observability();

        let adapter = super::adapter::ReadOnlyStudioVfsAdapter::new(
            vfs.clone(),
            fixture.runtime().handle().clone(),
        );
        let opened = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only");
        let bytes = adapter
            .read(attr.ino, opened.file_handle(), 6, 4)
            .expect("read payload through adapter");
        adapter
            .release(attr.ino, opened.file_handle())
            .expect("release read handle");

        assert_eq!(&bytes[..], b"from");
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.read.calls, 1);
        assert_eq!(metrics.fuse_callbacks.read.bytes, 4);
        assert_eq!(metrics.fuse_callbacks.read.errors, 0);
        assert_eq!(metrics.vfs_read_path.calls, 1);
    }

    #[test]
    fn read_only_adapter_rejects_write_or_truncate_open_flags() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-read-only-adapter-open-flags",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"hello.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::ReadOnlyStudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        assert_eq!(
            adapter.open(attr.ino, libc::O_WRONLY).unwrap_err(),
            libc::EROFS
        );
        assert_eq!(
            adapter
                .open(attr.ino, libc::O_RDONLY | libc::O_TRUNC)
                .unwrap_err(),
            libc::EROFS
        );
    }

    #[test]
    fn read_only_adapter_readdir_stops_successfully_when_reply_buffer_is_full() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-readdir-buffer-full")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        vfs.create(root, b"beta.txt", 501, 20, 0o644)
            .expect("create second file");
        let adapter = super::adapter::ReadOnlyStudioVfsAdapter::new(
            vfs.clone(),
            fixture.runtime().handle().clone(),
        );

        let buffer = adapter
            .readdir_for_test(root, 2, 32)
            .expect("full readdir buffer should return a partial success");

        assert!(!buffer.is_empty());
    }

    #[test]
    fn read_only_adapter_reads_symlink_targets() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-read-only-adapter-readlink",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .symlink(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"link.txt",
                b"target.txt",
                501,
                20,
            )
            .expect("create symlink");
        let adapter =
            super::adapter::ReadOnlyStudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        let target = adapter.readlink(attr.ino).expect("read symlink target");

        assert_eq!(target, b"target.txt");
    }
}
