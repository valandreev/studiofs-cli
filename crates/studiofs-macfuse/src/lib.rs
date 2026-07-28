#![doc = "macOS native bridge boundary for StudioFS."]
#![cfg(target_os = "macos")]
#![cfg_attr(target_os = "macos", deny(missing_docs))]

//! macOS native bridge boundary for StudioFS.
//!
//! This crate intentionally exposes only the mount request and serve API while
//! the low-level macFUSE session layer is still being built.

use std::io;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

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
    signal_handling: SignalHandling,
    unmount_signal: Option<MacFuseUnmountSignal>,
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
            signal_handling: SignalHandling::default(),
            unmount_signal: None,
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

    /// Set process signal ownership policy.
    pub fn with_signal_handling(mut self, signal_handling: SignalHandling) -> Self {
        self.signal_handling = signal_handling;
        self
    }
}

/// Controls whether StudioFS owns process-global macFUSE shutdown signals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SignalHandling {
    /// Do not install SIGINT/SIGTERM handlers.
    #[default]
    Disabled,
    /// Install temporary SIGINT/SIGTERM handlers for the StudioFS CLI.
    InstallForCli,
}

/// Caller-owned handle for requesting graceful macFUSE unmount.
#[derive(Clone, Debug, Default)]
pub struct MacFuseUnmountSignal {
    requested: Arc<AtomicBool>,
}

impl MacFuseUnmountSignal {
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

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static CLI_SIGNAL_HANDLERS_INSTALLED: AtomicBool = AtomicBool::new(false);

extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

struct CliSignalHandlers {
    previous_sigint: libc::sighandler_t,
    previous_sigterm: libc::sighandler_t,
}

impl CliSignalHandlers {
    fn install() -> io::Result<Self> {
        CLI_SIGNAL_HANDLERS_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "macFUSE CLI signal handlers are already installed",
                )
            })?;
        // SAFETY: this explicitly mutates process-global CLI signal disposition
        // and restores the previous handlers in Drop.
        unsafe {
            let previous_sigint = libc::signal(
                libc::SIGINT,
                shutdown_signal_handler as *const () as libc::sighandler_t,
            );
            if previous_sigint == libc::SIG_ERR {
                CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
                return Err(io::Error::last_os_error());
            }
            let previous_sigterm = libc::signal(
                libc::SIGTERM,
                shutdown_signal_handler as *const () as libc::sighandler_t,
            );
            if previous_sigterm == libc::SIG_ERR {
                let _ = libc::signal(libc::SIGINT, previous_sigint);
                CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
                return Err(io::Error::last_os_error());
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
        // SAFETY: these values came from successful `libc::signal` calls.
        unsafe {
            libc::signal(libc::SIGINT, self.previous_sigint);
            libc::signal(libc::SIGTERM, self.previous_sigterm);
        }
        CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
    }
}

fn signal_handlers_for_policy(
    signal_handling: SignalHandling,
) -> io::Result<Option<CliSignalHandlers>> {
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

impl Drop for SignalWatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(mut handlers) = self.handlers.lock() {
            let _ = handlers.take();
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn spawn_signal_watcher(
    exit_handle: low_level::SessionExitHandle,
    signal_handling: SignalHandling,
    unmount_signal: Option<MacFuseUnmountSignal>,
) -> io::Result<SignalWatcher> {
    let stop = Arc::new(AtomicBool::new(false));
    let handlers = Arc::new(Mutex::new(signal_handlers_for_policy(signal_handling)?));
    let watcher_stop = Arc::clone(&stop);
    let watcher_handlers = Arc::clone(&handlers);
    let thread = std::thread::Builder::new()
        .name("macfuse-signal-watcher".into())
        .spawn(move || {
            while !watcher_stop.load(Ordering::Acquire)
                && !SHUTDOWN_REQUESTED.load(Ordering::Acquire)
                && !unmount_signal
                    .as_ref()
                    .is_some_and(MacFuseUnmountSignal::is_requested)
            {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            if watcher_stop.load(Ordering::Acquire) {
                return;
            }
            if SHUTDOWN_REQUESTED.load(Ordering::Acquire)
                && let Ok(mut handlers) = watcher_handlers.lock()
            {
                let _ = handlers.take();
            }
            exit_handle.request_exit();
        })?;
    Ok(SignalWatcher {
        stop,
        handlers,
        thread: Some(thread),
    })
}

/// Start serving the macOS native bridge.
///
/// Async callback replies are drained before the macFUSE session is destroyed.
pub fn mount_with_request(request: MacFuseMountRequest) -> io::Result<()> {
    let MacFuseMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        run_in_foreground,
        signal_handling,
        unmount_signal,
    } = request;
    let config = low_level::SessionConfig::new(mount_point, volume_name)
        .with_read_only(read_only)
        .with_run_in_foreground(run_in_foreground);
    let mut callback_state = Box::new(adapter::CallbackState::new(
        Arc::clone(&vfs),
        runtime_handle,
        read_only,
    ));
    let userdata =
        (&mut *callback_state) as *mut adapter::CallbackState as *mut std::os::raw::c_void;
    // SAFETY: `callback_state` remains boxed and live until after the mounted
    // session is dropped. Both callback tables handle this exact userdata type.
    let mut session = unsafe {
        low_level::Session::mount_with_ops(
            &config,
            studio_vfs_ops_for_read_only(read_only),
            userdata,
        )
    }?;
    SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    let _signal_watcher =
        spawn_signal_watcher(session.exit_handle(), signal_handling, unmount_signal)?;
    vfs.set_kernel_invalidator(callback_state.kernel_invalidator(session.kernel_invalidator()));
    let result = session.run();
    vfs.clear_kernel_invalidator();
    // Async namespace callbacks retain raw macFUSE requests after returning
    // to the session loop. Drain them before Session::drop destroys the
    // session and request backing storage.
    callback_state.wait_for_reply_tasks();
    drop(session);
    drop(callback_state);
    result
}

fn studio_vfs_ops_for_read_only(read_only: bool) -> low_level::LowLevelOperations {
    if read_only {
        low_level::LowLevelOperations::read_only_studio_vfs()
    } else {
        low_level::LowLevelOperations::studio_vfs()
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use studiofs_meta::MetaFsOps;
    use studiofs_meta::engine::XATTR_REPLACE_FLAG;

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
    fn session_config_uses_buffered_data_cache_with_uncached_names() {
        let config = super::low_level::SessionConfig::new("/Volumes/Studio FS", "StudioFS Native")
            .with_read_only(true)
            .with_run_in_foreground(true);

        let args = config.argv_strings();
        let mount_args = config.mount_argv_strings();

        assert_eq!(
            config.mount_point().as_os_str(),
            OsStr::new("/Volumes/Studio FS")
        );
        assert_eq!(config.volume_name(), "StudioFS Native");
        assert!(config.run_in_foreground());
        assert!(args.iter().any(|arg| arg == "studiofs-macfuse"));
        assert!(
            mount_args
                .iter()
                .any(|arg| arg == "-ovolname=StudioFS Native")
        );
        assert!(mount_args.iter().any(|arg| arg == "-olocal"));
        assert!(!mount_args.iter().any(|arg| arg == "-onoappledouble"));
        assert!(mount_args.iter().any(|arg| arg == "-osparse"));
        assert!(!mount_args.iter().any(|arg| arg == "-odirect_io"));
        assert!(!mount_args.iter().any(|arg| arg == "-onolocalcaches"));
        assert!(!mount_args.iter().any(|arg| arg == "-oauto_cache"));
        let expected_geometry = super::low_level::MACOS_FUSE_IOSIZE_BYTES;
        assert!(
            mount_args
                .iter()
                .any(|arg| arg == &format!("-oiosize={expected_geometry}"))
        );
        assert!(
            mount_args
                .iter()
                .any(|arg| arg == &format!("-omax_read={expected_geometry}"))
        );
        assert!(mount_args.iter().any(|arg| arg == "-onovncache"));
        assert!(mount_args.iter().any(|arg| arg == "-odefer_permissions"));
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
    fn studio_vfs_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::studio_vfs();

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
    fn studio_vfs_low_level_ops_include_native_xattr_callbacks() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_xattr_callbacks());
        assert!(super::low_level::LowLevelOperations::read_only_studio_vfs().has_xattr_callbacks());
    }

    #[test]
    fn studio_vfs_low_level_ops_install_connection_init_callback() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_init_callback());
        assert!(super::low_level::LowLevelOperations::read_only_studio_vfs().has_init_callback());
    }

    #[test]
    fn studio_vfs_low_level_ops_install_forget_callback() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_forget_callback());
        assert!(super::low_level::LowLevelOperations::read_only_studio_vfs().has_forget_callback());
    }

    #[test]
    fn writable_studio_vfs_low_level_ops_install_rmdir_callback() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_rmdir_callback());
        assert!(!super::low_level::LowLevelOperations::read_only_studio_vfs().has_rmdir_callback());
    }

    #[test]
    fn writable_studio_vfs_low_level_ops_install_mknod_callback() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_mknod_callback());
        assert!(!super::low_level::LowLevelOperations::read_only_studio_vfs().has_mknod_callback());
    }

    #[test]
    fn read_only_mount_uses_restricted_low_level_ops() {
        let read_only_ops = super::studio_vfs_ops_for_read_only(true);
        let writable_ops = super::studio_vfs_ops_for_read_only(false);

        assert_eq!(
            read_only_ops.as_ptr(),
            super::low_level::LowLevelOperations::read_only_studio_vfs().as_ptr()
        );
        assert_eq!(
            writable_ops.as_ptr(),
            super::low_level::LowLevelOperations::studio_vfs().as_ptr()
        );
        assert_ne!(read_only_ops.as_ptr(), writable_ops.as_ptr());
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
    fn mounted_write_and_close_return_before_timeout() {
        if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
            return;
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-mounted-write-callback",
        )
        .expect("create VFS fixture");
        let ready_name = b"macfuse-mount-ready";
        fixture
            .vfs()
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                ready_name,
                501,
                20,
                0o644,
            )
            .expect("create mount readiness sentinel");
        let mount_point = std::path::PathBuf::from(format!(
            "target/studiofs-macfuse-write-smoke-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir(&mount_point);
        std::fs::create_dir_all(&mount_point).expect("create macFUSE write smoke mount point");
        let unmount_signal = super::MacFuseUnmountSignal::new();
        let request = super::MacFuseMountRequest {
            vfs: fixture.vfs(),
            runtime_handle: fixture.runtime().handle().clone(),
            mount_point: mount_point.clone(),
            volume_name: "StudioFS Write Smoke".into(),
            read_only: false,
            run_in_foreground: true,
            signal_handling: super::SignalHandling::Disabled,
            unmount_signal: Some(unmount_signal.clone()),
        };
        let mount = std::thread::spawn(move || super::mount_with_request(request));
        let ready_target = mount_point.join(std::str::from_utf8(ready_name).unwrap());
        let ready_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if std::fs::metadata(&ready_target).is_ok() {
                break;
            }
            if mount.is_finished() {
                let result = mount.join().expect("join failed mount thread");
                panic!("macFUSE write smoke mount exited before readiness: {result:?}");
            }
            if std::time::Instant::now() >= ready_deadline {
                unmount_signal.request_unmount();
                let _ = mount.join();
                panic!("macFUSE write smoke mount did not become ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let target = mount_point.join("callback-write.bin");
        let mut writer = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "dd if=/dev/zero of=\"$1\" bs=65536 count=1 status=none",
                "sh",
            ])
            .arg(&target)
            .spawn()
            .expect("start mounted write");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let status = loop {
            if let Some(status) = writer.try_wait().expect("poll mounted write") {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                writer.kill().expect("stop hung mounted write");
                writer.wait().expect("reap hung mounted write");
                break None;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };

        unmount_signal.request_unmount();
        mount
            .join()
            .expect("join macFUSE mount thread")
            .expect("unmount macFUSE write smoke");
        let cleanup_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            match std::fs::remove_file(&target) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::EBUSY | libc::ENXIO))
                        && std::time::Instant::now() < cleanup_deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => panic!("remove macFUSE write smoke file: {error}"),
            }
        }
        loop {
            match std::fs::remove_dir(&mount_point) {
                Ok(()) => break,
                Err(error)
                    if error.kind() == std::io::ErrorKind::ResourceBusy
                        && std::time::Instant::now() < cleanup_deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => panic!("remove macFUSE write smoke mount point: {error}"),
            }
        }

        assert!(
            status.is_some_and(|status| status.success()),
            "mounted macFUSE write did not complete before timeout"
        );
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

        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
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
        assert_eq!(
            metrics.fuse_async_read.admission_waits, 0,
            "uncontended async read admission should use the no-wait fast path"
        );
    }

    #[test]
    fn adapter_reports_iosize_backed_file_block_geometry() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-file-io-geometry",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"geometry.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let stat = adapter.getattr(attr.ino).expect("get file stat");

        assert_eq!(
            stat.st_blksize as u32,
            super::low_level::MACOS_FUSE_IOSIZE_BYTES,
            "macOS uses st_blksize as part of read clustering; keep it aligned with the mounted iosize"
        );
    }

    #[test]
    fn read_only_adapter_read_lets_vfs_handle_eof_without_pre_clamp_getattr() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-read-eof-no-preclamp")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"short.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"short"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let opened = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only");

        let bytes = adapter
            .read(attr.ino, opened.file_handle(), 1024, 4096)
            .expect("read past EOF through adapter");

        assert!(bytes.is_empty());
    }

    #[test]
    fn read_only_adapter_queues_read_without_waiting_for_vfs_completion() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-queues")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"queued.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"queued macFUSE read"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer to hold read gate");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 6)
            .expect("queue read without waiting for VFS completion");

        assert!(
            !queued.is_finished(),
            "queued read should still be pending while the writer gate is held"
        );

        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer gate");
        let bytes = queued.wait().expect("queued read should complete");

        assert_eq!(&bytes[..], b"queued");
    }

    #[test]
    fn read_only_adapter_delays_read_when_async_queue_is_full() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-full")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"full.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"bounded macFUSE read"))
            .expect("seed payload");
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_queue_byte_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            7,
        );
        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer to hold read gate");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        vfs.reset_diagnostics_observability();
        let held_capacity = adapter.hold_read_capacity_for_test(7);

        let delayed = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 7, 7)
            .expect("read should wait for bounded queue capacity");

        assert!(
            !delayed.is_finished(),
            "read should wait instead of returning EAGAIN while queue is full"
        );

        std::thread::sleep(std::time::Duration::from_millis(125));
        drop(held_capacity);
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer gate");
        let delayed_bytes = delayed.wait().expect("delayed read should complete");

        assert_eq!(&delayed_bytes[..], b" macFUS");
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.read.calls, 1);
        assert!(
            metrics.fuse_callbacks.read.max_ms >= 100.0,
            "read callback latency should include admission wait, got {:?}",
            metrics.fuse_callbacks.read
        );
    }

    #[test]
    fn read_only_adapter_queued_read_does_not_clone_shared_state_for_guard() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-shared")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"shared.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"shared macFUSE read"))
            .expect("seed payload");
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_queue_byte_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            7,
        );
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let held_capacity = adapter.hold_read_capacity_for_test(7);

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 7)
            .expect("read should wait for bounded queue capacity");
        fixture.runtime().block_on(async {
            let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    if adapter.pending_async_read_count_for_test() == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(
                observed.is_ok(),
                "queued read should publish pending diagnostics"
            );
        });

        assert_eq!(adapter.hot_path_shared_state_arc_count_for_test(), 2);

        drop(held_capacity);
        let bytes = queued.wait().expect("queued read should complete");
        assert_eq!(&bytes[..], b"shared ");
    }

    #[test]
    fn read_only_adapter_canceled_queued_read_skips_vfs_work() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-async-read-canceled-before-vfs",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"cancel.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"cancelable macFUSE read"))
            .expect("seed payload");
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_queue_byte_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            7,
        );
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        vfs.reset_diagnostics_observability();
        let held_capacity = adapter.hold_read_capacity_for_test(7);

        let canceled = adapter
            .queue_cancelable_read_for_test(attr.ino, reader.file_handle(), 0, 7)
            .expect("queue cancelable read");
        canceled.cancel();
        let errno = canceled
            .wait_timeout(std::time::Duration::from_secs(1))
            .expect("canceled read should finish while admission capacity is still held")
            .expect_err("canceled read should finish without VFS work");
        drop(held_capacity);

        assert_eq!(errno, libc::EINTR);
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.vfs_read_path.calls, 0);
        assert_eq!(metrics.fuse_callbacks.read.calls, 0);
        assert_eq!(metrics.fuse_async_read.pending_reads, 0);
        assert_eq!(metrics.fuse_async_read.peak_pending_reads, 1);
        assert_eq!(metrics.fuse_async_read.canceled_reads, 1);
        assert_eq!(metrics.fuse_async_read.completed_reads, 0);
    }

    #[test]
    fn read_only_adapter_rejects_negative_read_offset_before_queueing() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-async-read-negative-offset",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let rejected = adapter
            .queue_read_for_test(1, 1, -1, 1)
            .expect_err("negative offsets should fail before queueing");

        assert_eq!(rejected, libc::EINVAL);
    }

    #[test]
    fn read_only_adapter_records_actual_async_read_bytes_at_eof() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-eof-bytes")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"eof.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"short"))
            .expect("seed payload");
        vfs.reset_diagnostics_observability();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 4096)
            .expect("queue EOF-sized read");
        let bytes = queued.wait().expect("queued read should complete");

        assert_eq!(&bytes[..], b"short");
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.read.calls, 1);
        assert_eq!(metrics.fuse_callbacks.read.bytes, 5);
        assert_eq!(metrics.fuse_callbacks.read.errors, 0);
    }

    #[test]
    fn read_only_adapter_reuses_queued_read_buffer_after_reply_completion() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-reuse")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"reuse.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"reusable macFUSE buffer"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let first = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 8)
            .expect("queue first read");
        assert_eq!(
            &first.wait().expect("first read should complete")[..],
            b"reusable"
        );
        assert!(
            adapter.reusable_read_buffer_count_for_test() >= 1,
            "replied read buffers should remain reusable"
        );
        assert!(
            adapter
                .reusable_read_buffer_capacity_for_test()
                .is_some_and(|capacity| capacity >= 8)
        );

        let second = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 9, 7)
            .expect("queue second read");
        assert_eq!(
            &second.wait().expect("second read should complete")[..],
            b"macFUSE"
        );
        assert!(
            adapter.reusable_read_buffer_count_for_test() >= 1,
            "replied read buffers should remain reusable"
        );
        assert!(
            adapter
                .reusable_read_buffer_capacity_for_test()
                .is_some_and(|capacity| capacity >= 8)
        );
    }

    #[test]
    fn read_only_adapter_reuses_queued_read_buffer_for_larger_followup_read() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-grow")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"grow.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"small then larger read"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let first = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 5)
            .expect("queue first read");
        assert_eq!(
            &first.wait().expect("first read should complete")[..],
            b"small"
        );
        assert!(
            adapter.reusable_read_buffer_count_for_test() >= 1,
            "replied read buffers should remain reusable"
        );
        let first_capacity = adapter
            .reusable_read_buffer_capacity_for_test()
            .expect("first queued read should recycle one buffer");
        assert!(first_capacity >= 5);

        let second = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 22)
            .expect("queue larger followup read");
        assert_eq!(
            &second.wait().expect("second read should complete")[..],
            b"small then larger read"
        );
        assert!(
            adapter.reusable_read_buffer_count_for_test() >= 1,
            "replied read buffers should remain reusable"
        );
        let second_capacity = adapter
            .reusable_read_buffer_capacity_for_test()
            .expect("second queued read should recycle one buffer");
        assert!(second_capacity >= 22);
        assert!(second_capacity >= first_capacity);
    }

    #[test]
    fn buffered_adapter_read_executes_exact_kernel_requested_range() {
        const REQUEST_BYTES: usize = 4 * 1024;
        const PAYLOAD_BYTES: usize = 256 * 1024;

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-buffered-read-exact-range",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"buffered-exact-range.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let payload = vec![0xA5; PAYLOAD_BYTES];
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, &payload))
            .expect("seed payload");
        vfs.reset_diagnostics_observability();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open buffered reader through macFUSE adapter");
        assert!(
            !reader.direct_io(),
            "ordinary macFUSE data handles must use the kernel data cache"
        );

        let first = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, REQUEST_BYTES as u32)
            .expect("queue buffered read")
            .wait()
            .expect("buffered read should complete");
        assert_eq!(first, payload[..REQUEST_BYTES]);

        let second = adapter
            .queue_read_for_test(
                attr.ino,
                reader.file_handle(),
                REQUEST_BYTES as i64,
                REQUEST_BYTES as u32,
            )
            .expect("queue adjacent buffered read")
            .wait()
            .expect("adjacent buffered read should complete");
        assert_eq!(second, payload[REQUEST_BYTES..REQUEST_BYTES * 2]);

        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.read.calls, 2);
        assert_eq!(
            metrics.fuse_callbacks.read.bytes,
            (REQUEST_BYTES * 2) as u64
        );
        assert_eq!(
            metrics.vfs_read_path.calls, 2,
            "buffered adjacent callbacks must not reuse a daemon read window"
        );
        assert_eq!(
            metrics.vfs_read_path.requested_bytes,
            (REQUEST_BYTES * 2) as u64,
            "buffered reads must leave speculative readahead to the kernel/UBC"
        );
        assert_eq!(
            metrics.fuse_async_read.peak_pending_bytes, REQUEST_BYTES,
            "buffered read admission must account only the kernel-requested range"
        );
    }

    #[test]
    fn kernel_data_invalidation_clears_open_cache_snapshot() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data: AtomicBool,
            attrs: AtomicBool,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data.store(true, Ordering::Release);
            }

            fn inval_inode_attrs(&self, _ino: u64) {
                self.attrs.store(true, Ordering::Release);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-open-cache-invalidation",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-cache-invalidation.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let _reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());
        invalidator.inval_inode_attrs(attr.ino);

        invalidator.inval_inode(attr.ino);
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "data invalidation must force the next open to drop cached pages"
        );

        assert!(inner.data.load(Ordering::Acquire));
        assert!(inner.attrs.load(Ordering::Acquire));
    }

    #[test]
    fn kernel_invalidator_skips_inode_not_observed_by_macfuse_session() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
            attr_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }

            fn inval_inode_attrs(&self, _ino: u64) {
                self.attr_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-unobserved-inode-invalidation",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"never-observed.bin",
                501,
                20,
                0o644,
            )
            .expect("create file outside the macFUSE adapter");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());

        invalidator.inval_inode(attr.ino);
        invalidator.inval_inode_attrs(attr.ino);

        assert_eq!(inner.data_calls.load(Ordering::Acquire), 0);
        assert_eq!(inner.attr_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn kernel_data_invalidation_waits_until_async_reads_drain() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }

            fn inval_inode_attrs(&self, _ino: u64) {}
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-kernel-invalidation-read-drain",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"deferred-invalidation.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"0123456789abcdef"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let held_capacity = adapter.hold_read_capacity_for_test(8);
        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 8)
            .expect("queue read behind held admission capacity");
        fixture.runtime().block_on(async {
            let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    if adapter.pending_async_read_count_for_test() == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(
                observed.is_ok(),
                "queued read should become visible before invalidation"
            );
        });

        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());
        invalidator.inval_inode(attr.ino);

        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            0,
            "kernel FFI invalidation must not run while a macFUSE async read is active"
        );
        drop(held_capacity);
        assert_eq!(
            &queued.wait().expect("queued read should complete")[..],
            b"01234567"
        );
        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            1,
            "deferred kernel invalidation must flush after active reads drain"
        );
    }

    #[test]
    fn synchronous_adapter_read_does_not_take_buffer_from_queued_read_pool() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-sync-read-no-pool")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"sync.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"sync read payload"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 4)
            .expect("queue read to seed reusable pool");
        assert_eq!(
            &queued.wait().expect("queued read should complete")[..],
            b"sync"
        );
        assert_eq!(adapter.reusable_read_buffer_count_for_test(), 1);

        let bytes = adapter
            .read(attr.ino, reader.file_handle(), 0, 4)
            .expect("sync read should complete");

        assert_eq!(&bytes[..], b"sync");
        assert_eq!(adapter.reusable_read_buffer_count_for_test(), 1);
    }

    #[test]
    fn read_only_adapter_clamps_oversized_read_to_reusable_buffer_capacity() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-cap-drop")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"large-read.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"small payload"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let oversized_request = super::adapter::max_reusable_read_buffer_capacity_for_test() + 1;

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, oversized_request as u32)
            .expect("queue oversized read");

        assert_eq!(
            &queued.wait().expect("oversized read should complete")[..],
            b"small payload"
        );
        assert_eq!(adapter.reusable_read_buffer_count_for_test(), 1);
        let recycled_capacity = adapter
            .reusable_read_buffer_capacity_for_test()
            .expect("clamped read should recycle one buffer");
        assert!(recycled_capacity >= super::adapter::max_reusable_read_buffer_capacity_for_test());
        assert!(recycled_capacity <= super::adapter::max_retained_read_buffer_capacity_for_test());
    }

    #[test]
    fn read_only_adapter_zero_byte_read_uses_vfs_and_records_metrics() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-zero")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"zero.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"payload"))
            .expect("seed payload");
        vfs.reset_diagnostics_observability();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 0)
            .expect("queue zero-byte read");
        let bytes = queued.wait().expect("zero-byte read should complete");

        assert!(bytes.is_empty());
        // Zero-byte reads still exercise VFS/metrics, but their zero-capacity
        // buffers must not occupy reusable pool slots.
        assert_eq!(adapter.reusable_read_buffer_count_for_test(), 0);
        assert_eq!(adapter.reusable_read_buffer_capacity_for_test(), None);
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.read.calls, 1);
        assert_eq!(metrics.fuse_callbacks.read.bytes, 0);
        assert_eq!(metrics.fuse_callbacks.read.errors, 0);
        assert_eq!(metrics.vfs_read_path.calls, 1);
    }

    #[test]
    fn read_only_adapter_caps_oversized_read_to_queue_byte_limit() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-cap")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"cap.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"capped"))
            .expect("seed payload");
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_queue_byte_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            4,
        );
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 6)
            .expect("queue oversized read");
        let bytes = queued.wait().expect("oversized read should complete");

        assert_eq!(&bytes[..], b"capp");
    }

    #[test]
    fn read_only_adapter_read_queue_limit_is_clamped_for_tokio_semaphore() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-async-read-limit-clamp",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();

        let zero = super::adapter::StudioVfsAdapter::new_with_read_queue_byte_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            0,
        );
        let oversized = super::adapter::StudioVfsAdapter::new_with_read_queue_byte_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            usize::MAX,
        );

        assert_eq!(zero.read_queue_byte_limit_for_test(), 1);
        assert_eq!(
            oversized.read_queue_byte_limit_for_test() as usize,
            tokio::sync::Semaphore::MAX_PERMITS.min(u32::MAX as usize)
        );
    }

    #[test]
    fn adapter_write_keeps_accepted_bytes_visible_to_same_mount_reader() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-write-read-after-write",
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
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        vfs.reset_diagnostics_observability();

        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer through macFUSE adapter");
        let accepted = adapter
            .write(attr.ino, writer.file_handle(), 0, b"hello from macFUSE")
            .expect("write payload through macFUSE adapter");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let bytes = adapter
            .read(attr.ino, reader.file_handle(), 0, 18)
            .expect("read accepted local bytes through same adapter");

        assert_eq!(accepted, 18);
        assert_eq!(&bytes[..], b"hello from macFUSE");
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.write.calls, 1);
        assert_eq!(metrics.fuse_callbacks.write.bytes, 18);
        assert_eq!(metrics.fuse_callbacks.write.errors, 0);
    }

    #[test]
    fn adapter_write_schedules_deferred_materialization_for_opportunistic_callback() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-write-deferred-materialize",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"deferred.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer through macFUSE adapter");
        let payload = vec![0x5Au8; 5 * 1024 * 1024];

        adapter
            .write(attr.ino, writer.file_handle(), 0, &payload)
            .expect("write payload through macFUSE adapter");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let snapshot = vfs.write_path_observability_snapshot();
            if snapshot.vfs_write_path.materialize_completed.calls > 0 {
                break;
            }

            assert!(
                std::time::Instant::now() < deadline,
                "macFUSE adapter write did not schedule background materialization; pending_uploads={:?}, local_dirty={}, snapshot={:?}",
                vfs.pending_uploads_for_inode(attr.ino),
                vfs.inode_has_local_dirty_state(attr.ino),
                snapshot.vfs_write_path
            );
            fixture.runtime().block_on(async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            });
        }

        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer");
    }

    #[test]
    fn adapter_flush_fsync_release_and_truncate_preserve_local_first_bytes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-write-flush-fsync-release-truncate",
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
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open writable handle through macFUSE adapter");

        adapter
            .write(attr.ino, writer.file_handle(), 0, b"hello from macFUSE")
            .expect("write payload through macFUSE adapter");
        adapter
            .flush(attr.ino, writer.file_handle())
            .expect("flush writable handle through macFUSE adapter");
        adapter
            .fsync(Some(writer.file_handle()), attr.ino, false)
            .expect("fsync inode through macFUSE adapter");
        let truncated = adapter
            .set_len(Some(writer.file_handle()), attr.ino, 5)
            .expect("truncate through macFUSE adapter");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writable handle through macFUSE adapter");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("reopen reader through macFUSE adapter");
        let bytes = adapter
            .read(attr.ino, reader.file_handle(), 0, 32)
            .expect("read truncated accepted local bytes");

        assert_eq!(truncated.size, 5);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn adapter_handle_backed_fsync_survives_unlink_of_open_file() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-open-unlink-fsync",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-unlink.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open writable handle through macFUSE adapter");

        let initial = b"open handle payload";
        adapter
            .write(attr.ino, writer.file_handle(), 0, initial)
            .expect("write initial payload through macFUSE adapter");
        adapter
            .fsync(Some(writer.file_handle()), attr.ino, false)
            .expect("seed fsync should persist initial payload");
        fixture
            .meta()
            .unlink(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-unlink.txt",
            )
            .expect("unlink file while handle is open");

        adapter
            .write(
                attr.ino,
                writer.file_handle(),
                initial.len() as i64,
                b" after unlink",
            )
            .expect("write through open handle after foreign unlink");

        adapter
            .fsync(Some(writer.file_handle()), attr.ino, false)
            .expect("handle-backed fsync should not depend on removed namespace entry");
    }

    #[test]
    fn adapter_append_handle_fsync_survives_unlink_of_open_file() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-open-append-unlink-fsync",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-append-unlink.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_RDONLY | libc::O_APPEND)
            .expect("open append handle through macFUSE adapter");

        fixture
            .meta()
            .unlink(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-append-unlink.txt",
            )
            .expect("unlink file while append handle is open");

        adapter
            .write(attr.ino, writer.file_handle(), 0, b" after unlink")
            .expect("write through append handle after foreign unlink");
        adapter
            .fsync(Some(writer.file_handle()), attr.ino, true)
            .expect("append handle-backed fsync should not depend on removed namespace entry");
    }

    #[test]
    fn adapter_rmdir_removes_empty_directory_and_respects_read_only() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-adapter-rmdir")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let read_only_adapter = super::adapter::StudioVfsAdapter::new_with_read_only(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            true,
        );

        adapter
            .mkdir(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"empty-dir",
                0o755,
            )
            .expect("create directory through macFUSE adapter");
        assert_eq!(
            read_only_adapter
                .rmdir(
                    studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                    b"empty-dir",
                )
                .unwrap_err(),
            libc::EROFS
        );
        adapter
            .rmdir(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"empty-dir",
            )
            .expect("remove empty directory through macFUSE adapter");

        assert!(
            vfs.lookup(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"empty-dir",
            )
            .is_err()
        );
    }

    #[test]
    fn adapter_rmdir_retries_when_child_unlink_arrives_after_first_not_empty() {
        use std::thread;
        use std::time::Duration;

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-rmdir-child-unlink-arrives-late",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let parent = vfs
            .mkdir(root, b"late-delete-parent", 501, 20, 0o755)
            .expect("create parent directory");
        vfs.create(parent.ino, b"child.txt", 501, 20, 0o644)
            .expect("create child file");
        let adapter = Arc::new(super::adapter::StudioVfsAdapter::new(
            vfs.clone(),
            fixture.runtime().handle().clone(),
        ));

        let rmdir_adapter = adapter.clone();
        let rmdir = thread::spawn(move || rmdir_adapter.rmdir(root, b"late-delete-parent"));
        thread::sleep(Duration::from_millis(10));
        adapter
            .unlink(parent.ino, b"child.txt")
            .expect("late child unlink should succeed");

        rmdir
            .join()
            .expect("rmdir callback thread should finish")
            .expect("rmdir should retry after the late child unlink arrives");
    }

    #[test]
    fn adapter_rmdir_waits_for_child_unlink_beyond_short_retry_window() {
        use std::thread;
        use std::time::Duration;

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-rmdir-child-unlink-arrives-after-short-retry-window",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let parent = vfs
            .mkdir(root, b"delayed-delete-parent", 501, 20, 0o755)
            .expect("create parent directory");
        vfs.create(parent.ino, b"child.txt", 501, 20, 0o644)
            .expect("create child file");
        let adapter = Arc::new(super::adapter::StudioVfsAdapter::new(
            vfs.clone(),
            fixture.runtime().handle().clone(),
        ));

        let rmdir_adapter = adapter.clone();
        let rmdir = thread::spawn(move || rmdir_adapter.rmdir(root, b"delayed-delete-parent"));
        thread::sleep(Duration::from_millis(1_000));
        adapter
            .unlink(parent.ino, b"child.txt")
            .expect("delayed child unlink should succeed");

        rmdir
            .join()
            .expect("rmdir callback thread should finish")
            .expect("rmdir should wait beyond the short retry window");
    }

    #[test]
    fn read_only_adapter_still_rejects_write_or_truncate_open_flags() {
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
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_only(
            vfs,
            fixture.runtime().handle().clone(),
            true,
        );

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
    fn cache_policy_keeps_kernel_cache_only_for_read_only_handles() {
        let read_only = super::adapter::macfuse_open_cache_policy(libc::O_RDONLY, false);
        assert!(!read_only.direct_io);
        assert!(read_only.keep_cache);

        let write_only = super::adapter::macfuse_open_cache_policy(libc::O_WRONLY, false);
        assert!(!write_only.direct_io);
        assert!(!write_only.keep_cache);

        let read_write = super::adapter::macfuse_open_cache_policy(libc::O_RDWR, false);
        assert!(!read_write.direct_io);
        assert!(!read_write.keep_cache);

        let append =
            super::adapter::macfuse_open_cache_policy(libc::O_RDONLY | libc::O_APPEND, false);
        assert!(!append.direct_io);
        assert!(!append.keep_cache);

        let truncate = super::adapter::macfuse_open_cache_policy(libc::O_RDONLY, true);
        assert!(!truncate.direct_io);
        assert!(!truncate.keep_cache);
    }

    #[test]
    fn adapter_keeps_kernel_cache_only_for_unchanged_read_only_generation() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-generation",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"cached.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let first_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open first read-only handle");
        assert!(
            !first_open.keep_cache(),
            "first open must invalidate unknown kernel pages: {first_open:?}"
        );
        adapter
            .release(attr.ino, first_open.file_handle())
            .expect("release first handle");

        let repeated_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open repeated read-only handle");
        assert!(
            repeated_open.keep_cache(),
            "unchanged repeated open may keep kernel pages: {repeated_open:?}"
        );
        adapter
            .release(attr.ino, repeated_open.file_handle())
            .expect("release repeated handle");

        vfs.setattr(attr.ino, studiofs_meta::SetattrArgs::size_only(1))
            .expect("advance visible file generation");

        let changed_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open after visible generation changed");
        assert!(
            !changed_open.keep_cache(),
            "changed generation must invalidate kernel pages: {changed_open:?}"
        );
        let cache_decisions = vfs.write_path_observability_snapshot().macfuse_open_cache;
        assert_eq!(cache_decisions.read_only_keep, 1);
        assert_eq!(cache_decisions.read_only_drop, 2);
        assert_eq!(cache_decisions.read_only_generation_changed, 1);
    }

    #[test]
    fn adapter_clean_write_capable_release_makes_next_read_drop_kernel_cache() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-write-open",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"write-open.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let first_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open first read-only handle");
        adapter
            .release(attr.ino, first_open.file_handle())
            .expect("release first handle");
        let repeated_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open repeated read-only handle");
        assert!(
            repeated_open.keep_cache(),
            "unchanged repeated open should establish a cacheable observation: {repeated_open:?}"
        );
        adapter
            .release(attr.ino, repeated_open.file_handle())
            .expect("release repeated handle");

        let write_open = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open write-capable handle");
        assert!(
            !write_open.keep_cache(),
            "write-capable open must not keep cache: {write_open:?}"
        );
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "active write-capable open must clear previous cache observation"
        );
        adapter
            .release(attr.ino, write_open.file_handle())
            .expect("release write-capable handle");
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "clean final write release must leave the first read-only open to drop kernel pages"
        );

        let read_after_write_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle after write-capable open");
        assert!(
            !read_after_write_open.keep_cache(),
            "first read-only open after clean write release should drop cache: {read_after_write_open:?}"
        );
        let cache_decisions = vfs.write_path_observability_snapshot().macfuse_open_cache;
        assert_eq!(cache_decisions.clean_write_release_seeded, 0);
        assert_eq!(cache_decisions.read_only_generation_changed, 2);
        assert_eq!(cache_decisions.read_only_keep, 1);
    }

    #[test]
    fn adapter_read_only_handle_keeps_foreground_read_active_until_release() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-read-handle-foreground-lease",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"cached-media-read.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let opened = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle");
        assert_eq!(
            vfs.foreground_reads_inflight(),
            1,
            "read-only handle must keep background disk rereads paused while UBC serves pages"
        );

        adapter
            .release(attr.ino, opened.file_handle())
            .expect("release read-only handle");
        assert_eq!(vfs.foreground_reads_inflight(), 0);
    }

    #[test]
    fn adapter_clean_write_release_drops_cache_while_read_only_handle_remains_open() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-read-open-during-write-release",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"read-open-during-write-release.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let initial_read = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open initial read-only handle");
        adapter
            .release(attr.ino, initial_read.file_handle())
            .expect("release initial read-only handle");
        let held_read = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle that remains active");
        assert!(
            held_read.keep_cache(),
            "setup should establish a cacheable read-only handle: {held_read:?}"
        );

        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open clean write-capable handle");
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "active write-capable open must clear previous cache observation"
        );
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release clean write-capable handle while read-only handle remains open");

        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "closing the last write-capable handle must clear the cache observation"
        );
        let next_read = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle after clean write release");
        assert!(
            !next_read.keep_cache(),
            "first read-only open after last write-capable release should drop cache: {next_read:?}"
        );
    }

    #[test]
    fn adapter_clean_write_release_waits_for_all_write_capable_handles() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-multiple-write-release",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"multiple-write-release.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let first_writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open first write-capable handle");
        let second_writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open second write-capable handle");
        adapter
            .release(attr.ino, first_writer.file_handle())
            .expect("release first write-capable handle");
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "snapshot must remain cleared while another write-capable handle is active"
        );

        adapter
            .release(attr.ino, second_writer.file_handle())
            .expect("release second write-capable handle");
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "snapshot should remain cleared after the last write-capable handle closes"
        );
    }

    #[test]
    fn adapter_dirty_write_release_drops_cache_after_final_write_capable_handle() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-dirty-write-release",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"dirty-write-release.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let first_writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open first write-capable handle");
        let second_writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open second write-capable handle");
        adapter
            .write(attr.ino, first_writer.file_handle(), 0, b"dirty write")
            .expect("write payload through macFUSE adapter");
        adapter
            .release(attr.ino, first_writer.file_handle())
            .expect("release first write-capable handle");

        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "dirty release must not seed while another write-capable handle remains active"
        );
        let read_while_writer_active = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle while write-capable handle remains active");
        assert!(
            !read_while_writer_active.keep_cache(),
            "read-only open while a write-capable handle remains active must drop cache: {read_while_writer_active:?}"
        );
        adapter
            .release(attr.ino, read_while_writer_active.file_handle())
            .expect("release read-only handle");
        let cache_decisions = vfs.write_path_observability_snapshot().macfuse_open_cache;
        assert_eq!(cache_decisions.clean_write_release_seeded, 0);
        assert_eq!(cache_decisions.read_only_local_dirty_state, 1);

        adapter
            .release(attr.ino, second_writer.file_handle())
            .expect("release final write-capable handle");
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "final write-capable release should leave the next read-only open to drop cache"
        );
        let read_after_final_writer = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle after final write release");
        assert!(
            !read_after_final_writer.keep_cache(),
            "first read-only open after final dirty write release should drop cache: {read_after_final_writer:?}"
        );
    }

    #[test]
    fn adapter_clean_active_writer_records_active_writer_cache_drop() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-clean-active-writer",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"clean-active-writer.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open write-capable handle without writing");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle while clean writer remains active");

        assert!(
            !reader.keep_cache(),
            "read-only open while a clean writer remains active must drop cache: {reader:?}"
        );
        let cache_decisions = vfs.write_path_observability_snapshot().macfuse_open_cache;
        assert_eq!(cache_decisions.read_only_active_writer, 1);
        assert_eq!(cache_decisions.read_only_local_dirty_state, 0);

        adapter
            .release(attr.ino, reader.file_handle())
            .expect("release read-only handle");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release clean write-capable handle");
    }

    #[test]
    fn adapter_locally_materialized_write_release_drops_unpublished_kernel_cache_observation() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-local-stable-write-release",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"local-stable-write-release.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open write-capable handle");
        adapter
            .write(attr.ino, writer.file_handle(), 0, b"locally stable write")
            .expect("write payload through macFUSE adapter");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release write-capable handle");
        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "locally stable unpublished release should not seed a kernel cache observation"
        );
        fixture
            .runtime()
            .block_on(vfs.fsync(attr.ino, false))
            .expect("follow-up sync should wait for deferred release materialization");
        let local_attr = vfs
            .getattr(attr.ino)
            .expect("read locally materialized file attr");
        assert_ne!(local_attr.upload_state, studiofs_meta::UploadState::Ready);
        assert!(
            local_attr.content_epoch > local_attr.committed_content_epoch,
            "local release should advance visible epoch before remote publication: {local_attr:?}"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(attr.ino),
            "final release should drain dirty local state before a read-only cache observation"
        );

        let read_after_local_release = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only handle after local-stable write release");
        assert!(
            !read_after_local_release.keep_cache(),
            "first read-only open after local-stable release should drop cache: {read_after_local_release:?}"
        );
        let cache_decisions = vfs.write_path_observability_snapshot().macfuse_open_cache;
        assert_eq!(cache_decisions.clean_write_release_seeded, 0);
        assert_eq!(cache_decisions.local_stable_write_release_seeded, 0);
        assert_eq!(cache_decisions.read_only_keep, 0);
        assert_eq!(cache_decisions.read_only_local_stable_keep, 0);
        assert_eq!(cache_decisions.read_only_generation_changed, 1);
        assert_eq!(cache_decisions.read_only_local_stable_generation_changed, 1);
    }

    #[test]
    fn adapter_unlink_clears_kernel_cache_observation_for_removed_inode() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-adapter-cache-unlink")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let attr = vfs
            .create(root, b"unlink-cache.txt", 501, 20, 0o644)
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());

        let first_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open first read-only handle");
        adapter
            .release(attr.ino, first_open.file_handle())
            .expect("release first handle");
        let repeated_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open repeated read-only handle");
        assert!(repeated_open.keep_cache());
        adapter
            .release(attr.ino, repeated_open.file_handle())
            .expect("release repeated handle");
        assert!(adapter.has_open_cache_snapshot_for_test(attr.ino));

        adapter
            .unlink(root, b"unlink-cache.txt")
            .expect("unlink through macFUSE adapter");

        assert!(!adapter.has_open_cache_snapshot_for_test(attr.ino));
        let calls_before_late_completion = inner.data_calls.load(Ordering::Acquire);
        invalidator.inval_inode(attr.ino);
        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            calls_before_late_completion,
            "late background completion must not notify a vnode closed by final unlink"
        );
    }

    #[test]
    fn adapter_unlink_clears_file_flag_overlay_for_removed_inode() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-file-flags-unlink",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let (attr, open) = adapter
            .create(root, b".DS_Store", 0o644, libc::O_RDWR)
            .expect("create hidden Finder metadata file");
        assert!(adapter.file_flags_contains_for_test(attr.ino));
        adapter
            .release(attr.ino, open.file_handle())
            .expect("release created file handle");

        adapter
            .unlink(root, b".DS_Store")
            .expect("unlink hidden Finder metadata file");

        assert!(!adapter.file_flags_contains_for_test(attr.ino));
    }

    #[test]
    fn final_unlink_blocks_kernel_notification_from_late_write_completion() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-late-write-completion-after-unlink",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());
        let (attr, writer) = adapter
            .create(root, b"late-completion.bin", 0o644, libc::O_WRONLY)
            .expect("create file through macFUSE adapter");
        adapter
            .write(attr.ino, writer.file_handle(), 0, b"pending upload")
            .expect("accept local write");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer while background work may remain");
        adapter
            .unlink(root, b"late-completion.bin")
            .expect("unlink final hardlink");

        let calls_before_late_completion = inner.data_calls.load(Ordering::Acquire);
        invalidator.inval_inode(attr.ino);

        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            calls_before_late_completion,
            "late upload or GC completion must not notify the removed vnode"
        );
    }

    #[test]
    fn adapter_unlink_keeps_kernel_cache_observation_for_remaining_hardlink() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-hardlink",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let attr = vfs
            .create(root, b"hardlink-source.txt", 501, 20, 0o644)
            .expect("create file");
        vfs.link(attr.ino, root, b"hardlink-remaining.txt")
            .expect("create hardlink");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());

        let first_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open first read-only handle");
        adapter
            .release(attr.ino, first_open.file_handle())
            .expect("release first handle");
        let repeated_open = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open repeated read-only handle");
        assert!(repeated_open.keep_cache());
        adapter
            .release(attr.ino, repeated_open.file_handle())
            .expect("release repeated handle");
        assert!(adapter.has_open_cache_snapshot_for_test(attr.ino));

        adapter
            .unlink(root, b"hardlink-source.txt")
            .expect("unlink one hardlink through macFUSE adapter");

        assert!(adapter.has_open_cache_snapshot_for_test(attr.ino));
        assert_eq!(
            vfs.lookup(root, b"hardlink-remaining.txt")
                .expect("remaining hardlink should still resolve")
                .ino,
            attr.ino
        );
        let calls_before_invalidation = inner.data_calls.load(Ordering::Acquire);
        invalidator.inval_inode(attr.ino);
        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            calls_before_invalidation + 1,
            "unlinking one hardlink must keep kernel notification live for the remaining name"
        );
    }

    #[test]
    fn adapter_rename_closes_liveness_for_overwritten_final_link() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-rename-final-link-liveness",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let source = vfs
            .create(root, b"rename-source.txt", 501, 20, 0o644)
            .expect("create source file");
        let overwritten = vfs
            .create(root, b"rename-destination.txt", 501, 20, 0o644)
            .expect("create destination file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());

        adapter
            .lookup(root, b"rename-destination.txt")
            .expect("observe overwritten destination inode");
        adapter
            .rename(root, b"rename-source.txt", root, b"rename-destination.txt")
            .expect("rename over final destination link");

        assert_eq!(
            vfs.lookup(root, b"rename-destination.txt")
                .expect("renamed source should occupy destination")
                .ino,
            source.ino
        );
        invalidator.inval_inode(overwritten.ino);
        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            0,
            "overwritten final inode must stop receiving late kernel invalidations"
        );
    }

    #[test]
    fn adapter_rename_same_path_keeps_liveness_observed() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-rename-same-path-liveness",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let file = vfs
            .create(root, b"rename-same-path.txt", 501, 20, 0o644)
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());

        adapter
            .lookup(root, b"rename-same-path.txt")
            .expect("observe file");
        adapter
            .rename(root, b"rename-same-path.txt", root, b"rename-same-path.txt")
            .expect("same-path rename is a no-op");

        invalidator.inval_inode(file.ino);
        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            1,
            "same-path rename must not close the inode liveness gate"
        );
    }

    #[test]
    fn adapter_rename_keeps_liveness_for_overwritten_hardlink() {
        #[derive(Default)]
        struct RecordingInvalidator {
            data_calls: AtomicUsize,
        }

        impl studiofs_vfs::KernelInvalidator for RecordingInvalidator {
            fn inval_inode(&self, _ino: u64) {
                self.data_calls.fetch_add(1, Ordering::AcqRel);
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-rename-hardlink-liveness",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let _source = vfs
            .create(root, b"rename-hardlink-source.txt", 501, 20, 0o644)
            .expect("create source file");
        let overwritten = vfs
            .create(root, b"rename-hardlink-destination.txt", 501, 20, 0o644)
            .expect("create destination file");
        vfs.link(overwritten.ino, root, b"rename-hardlink-remaining.txt")
            .expect("create destination hardlink");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let inner = Arc::new(RecordingInvalidator::default());
        let invalidator = adapter.kernel_invalidator_for_test(inner.clone());

        adapter
            .lookup(root, b"rename-hardlink-destination.txt")
            .expect("observe overwritten hardlink inode");
        adapter
            .rename(
                root,
                b"rename-hardlink-source.txt",
                root,
                b"rename-hardlink-destination.txt",
            )
            .expect("rename over one hardlink name");

        assert_eq!(
            vfs.lookup(root, b"rename-hardlink-remaining.txt")
                .expect("remaining hardlink should resolve")
                .ino,
            overwritten.ino
        );
        invalidator.inval_inode(overwritten.ino);
        assert_eq!(
            inner.data_calls.load(Ordering::Acquire),
            1,
            "remaining hardlink must keep kernel invalidations live"
        );
    }

    #[test]
    fn adapter_open_cache_observations_are_bounded() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-cache-bounded",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let limit = adapter.open_cache_snapshot_limit_for_test();

        for index in 0..(limit + 3) {
            let name = format!("bounded-{index}.txt");
            let attr = vfs
                .create(root, name.as_bytes(), 501, 20, 0o644)
                .expect("create file");
            let first_open = adapter
                .open(attr.ino, libc::O_RDONLY)
                .expect("open first read-only handle");
            adapter
                .release(attr.ino, first_open.file_handle())
                .expect("release first handle");
            let repeated_open = adapter
                .open(attr.ino, libc::O_RDONLY)
                .expect("open repeated read-only handle");
            assert!(repeated_open.keep_cache());
            adapter
                .release(attr.ino, repeated_open.file_handle())
                .expect("release repeated handle");
        }

        assert!(adapter.open_cache_snapshot_count_for_test() <= limit);
    }

    #[test]
    fn adapter_roundtrips_native_resource_fork_xattr_bytes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-resource-fork-xattr",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"forked.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let fork_name = b"com.apple.ResourceFork";
        let fork_bytes = b"native resource fork bytes";

        adapter
            .set_xattr(attr.ino, fork_name, fork_bytes, 0)
            .expect("set resource fork through macFUSE adapter");

        assert_eq!(
            adapter
                .get_xattr(attr.ino, fork_name)
                .expect("get resource fork through macFUSE adapter"),
            Some(fork_bytes.to_vec())
        );
        assert_eq!(
            adapter
                .list_xattrs(attr.ino)
                .expect("list xattrs through macFUSE adapter"),
            vec![fork_name.to_vec()]
        );
        adapter
            .remove_xattr(attr.ino, fork_name)
            .expect("remove resource fork through macFUSE adapter");
        assert_eq!(
            adapter
                .get_xattr(attr.ino, fork_name)
                .expect("get removed resource fork through macFUSE adapter"),
            None
        );
    }

    #[test]
    fn adapter_maps_missing_xattr_replace_to_macos_no_attribute_errno() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-missing-xattr-replace",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"replace-missing-xattr.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let error = adapter
            .set_xattr(
                attr.ino,
                b"com.apple.FinderInfo",
                b"finder-info",
                XATTR_REPLACE_FLAG,
            )
            .expect_err("replace missing xattr should fail");

        assert_eq!(error, libc::ENOATTR);
    }

    #[test]
    fn adapter_maps_missing_xattr_remove_to_macos_no_attribute_errno() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-missing-xattr-remove",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"remove-missing-xattr.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let error = adapter
            .remove_xattr(attr.ino, b"com.apple.FinderInfo")
            .expect_err("remove missing xattr should fail");

        assert_eq!(error, libc::ENOATTR);
    }

    #[test]
    fn adapter_reports_native_resource_fork_xattr_decoded_size() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-adapter-xattr-size")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"fork-size.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let fork_name = b"com.apple.ResourceFork";
        let fork_bytes = vec![0x66; 64 * 1024];

        adapter
            .set_xattr(attr.ino, fork_name, &fork_bytes, 0)
            .expect("set resource fork through macFUSE adapter");

        assert_eq!(
            adapter
                .get_xattr_size(attr.ino, fork_name)
                .expect("get resource fork size through macFUSE adapter"),
            Some(fork_bytes.len())
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
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let buffer = adapter
            .readdir_for_test(root, 2, 32)
            .expect("full readdir buffer should return a partial success");

        assert!(!buffer.is_empty());
    }

    #[test]
    fn read_only_adapter_readdir_reuses_cached_entries_for_chunked_offsets() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-readdir-entry-cache")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        vfs.create(root, b"beta.txt", 501, 20, 0o644)
            .expect("create second file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        adapter
            .readdir_for_test(root, 2, 32)
            .expect("first readdir chunk");
        adapter
            .readdir_for_test(root, 3, 32)
            .expect("second readdir chunk");

        assert_eq!(adapter.readdir_cache_entry_count_for_test(), 1);
    }

    #[test]
    fn read_only_adapter_readdir_prunes_expired_cache_entries_on_refresh() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-readdir-cache-prune")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        adapter.fill_expired_readdir_cache_entries_for_test();

        adapter
            .readdir_for_test(root, 2, 32)
            .expect("refresh readdir cache");

        assert_eq!(adapter.readdir_cache_entry_count_for_test(), 1);
    }

    #[test]
    fn read_only_adapter_readdir_rate_limits_fresh_cache_pruning() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-readdir-cache-prune-rate-limit",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let first_dir = vfs
            .mkdir(root, b"first", 501, 20, 0o755)
            .expect("create first directory");
        let second_dir = vfs
            .mkdir(root, b"second", 501, 20, 0o755)
            .expect("create second directory");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        adapter.fill_fresh_readdir_cache_entries_for_test();

        adapter
            .readdir_for_test(first_dir.ino, 2, 32)
            .expect("populate first readdir cache");
        let first_miss_count = adapter.readdir_cache_entry_count_for_test();
        adapter
            .readdir_for_test(second_dir.ino, 2, 32)
            .expect("populate second missing readdir cache");

        assert_eq!(first_miss_count, 1025);
        assert_eq!(adapter.readdir_cache_entry_count_for_test(), 1026);
    }

    #[test]
    fn read_only_adapter_readdir_refresh_records_fresh_cache_timestamp() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-readdir-cache-fresh-timestamp",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        adapter
            .readdir_for_test(root, 2, 32)
            .expect("populate readdir cache");
        let expired = std::time::Instant::now() - std::time::Duration::from_secs(30);
        adapter.set_ready_readdir_cache_loaded_at_for_test(root, expired);

        adapter
            .readdir_for_test(root, 2, 32)
            .expect("refresh expired readdir cache");

        let refreshed = adapter
            .readdir_cache_loaded_at_for_test(root)
            .expect("ready readdir cache entry should exist");
        assert!(refreshed > expired);
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
            super::adapter::StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        let target = adapter.readlink(attr.ino).expect("read symlink target");

        assert_eq!(target, b"target.txt");
    }

    #[test]
    fn symlink_waits_for_deferred_parent_metadata() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-symlink-deferred-parent",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let parent = vfs
            .mkdir_with_deferred_metadata_admission(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"parent",
                501,
                20,
                0o755,
                fixture.runtime().handle(),
            )
            .expect("admit deferred parent directory");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        adapter
            .symlink(parent.ino, b"skills", b"../.agents/skills")
            .expect("symlink should wait for its deferred parent");
    }
}
