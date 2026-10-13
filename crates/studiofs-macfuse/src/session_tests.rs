//! Native regressions for the legacy macFUSE session loop.

#![cfg(target_os = "macos")]

use std::ffi::{CStr, c_void};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};

use super::{LowLevelOperations, Session, SessionConfig, raw};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(15);
// Detached Disk Arbitration unmount may outlast the foreground operation budget.
const UNMOUNT_TIMEOUT: Duration = Duration::from_secs(15);
const MOUNT_POINT_REMOVAL_TIMEOUT: Duration = Duration::from_secs(3);
const TEARDOWN_WAIT: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

type WriteCallback = unsafe extern "C" fn(
    *mut raw::FuseReq,
    raw::FuseIno,
    *const std::os::raw::c_char,
    usize,
    libc::off_t,
    *mut raw::FuseFileInfo,
);

unsafe extern "C" {
    fn studiofs_macfuse_test_ops_with_write(write: WriteCallback) -> *const raw::FuseLowLevelOps;
}

struct CallbackGate {
    blocked_inode: u64,
    entered: mpsc::Sender<()>,
    finished: mpsc::Sender<()>,
    // The receiver is mutex-protected because mpsc has one consumer. Dropping
    // the final sender is the broadcast event: every waiter observes disconnect
    // without a token-counting race or a Condvar predicate loop.
    released: Mutex<mpsc::Receiver<()>>,
}

static CALLBACK_GATE: Mutex<Option<Arc<CallbackGate>>> = Mutex::new(None);
static NATIVE_TEST_LOCK: Mutex<()> = Mutex::new(());

struct GateReset(Option<mpsc::Sender<()>>);

impl Drop for GateReset {
    fn drop(&mut self) {
        drop(self.0.take());
        *CALLBACK_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

unsafe extern "C" fn blocked_write(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    buf: *const std::os::raw::c_char,
    size: usize,
    offset: libc::off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let gate = CALLBACK_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .cloned();
    let gate = gate.filter(|gate| gate.blocked_inode == ino);
    if let Some(gate) = &gate {
        let _ = gate.entered.send(());
        // Disconnecting the channel releases every matching callback.
        let _ = gate
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
    }

    // SAFETY: macFUSE supplied a live request and callback-owned arguments; the
    // production callback owns the request reply exactly as it does in a mount.
    unsafe { crate::adapter::studiofs_macfuse_write(req, ino, buf, size, offset, fi) };
    if let Some(gate) = gate {
        let _ = gate.finished.send(());
    }
}

fn receive_before_teardown<T>(receiver: &mpsc::Receiver<T>, operation: &str) -> T {
    receiver
        .recv_timeout(CALLBACK_TIMEOUT)
        .unwrap_or_else(|error| {
            // Unwinding would drop callback userdata that a stuck native worker may
            // still access. End this explicitly selected native test without freeing it.
            eprintln!("native macFUSE {operation} timed out: {error}");
            // SAFETY: Terminate the selected native-test process without running
            // destructors or starting a core dump while native requests are live.
            unsafe { libc::_exit(101) };
        })
}

struct TempMountDir {
    path: PathBuf,
}

impl TempMountDir {
    fn create(path: PathBuf) -> std::io::Result<Self> {
        fs::create_dir(&path)?;
        let mut mount_point = Self { path };
        // Match the kernel's resolved mount name before mounting. Resolving a
        // stopped FUSE mount during teardown could block on a native request.
        mount_point.path = fs::canonicalize(&mount_point.path)?;
        Ok(mount_point)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempMountDir {
    fn drop(&mut self) {
        // A normal teardown removes the directory first. During unwinding a
        // live mount may still return EBUSY; do not delay panic reporting here.
        let _ = fs::remove_dir(&self.path);
    }
}

fn mount_point_is_active(path: &Path) -> std::io::Result<bool> {
    // MNT_NOWAIT reads the kernel's mount table without asking a stopped FUSE
    // session to service statfs. Use caller-owned storage rather than getmntinfo's
    // shared buffer, since macFUSE also has background threads.
    // SAFETY: A null buffer requests only the current mount count.
    let count = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
    if count < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut capacity = count as usize + 1;
    loop {
        let buffer_size = capacity
            .checked_mul(std::mem::size_of::<libc::statfs>())
            .and_then(|size| size.try_into().ok())
            .ok_or_else(|| std::io::Error::other("mount table is too large"))?;
        let mut mounts = Vec::<libc::statfs>::with_capacity(capacity);
        // SAFETY: The buffer is aligned and writable for buffer_size bytes.
        // getfsstat initializes each of the returned statfs entries.
        let count = unsafe { libc::getfsstat(mounts.as_mut_ptr(), buffer_size, libc::MNT_NOWAIT) };
        if count < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if count as usize == capacity {
            // A full buffer may omit the mount after concurrent table growth.
            capacity *= 2;
            continue;
        }
        // SAFETY: getfsstat initialized count entries, fewer than capacity.
        unsafe { mounts.set_len(count as usize) };
        return Ok(mounts.iter().any(|mount| {
            // SAFETY: The kernel returns a NUL-terminated mount point name.
            unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes()
                == path.as_os_str().as_bytes()
        }));
    }
}

fn remove_mount_point(path: &Path) -> std::io::Result<()> {
    remove_mount_point_with(
        path,
        std::time::Instant::now() + UNMOUNT_TIMEOUT,
        MOUNT_POINT_REMOVAL_TIMEOUT,
        |path| fs::remove_dir(path),
    )
}

fn remove_mount_point_with(
    path: &Path,
    unmount_deadline: std::time::Instant,
    removal_timeout: Duration,
    mut remove_dir: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    while mount_point_is_active(path)? {
        if std::time::Instant::now() >= unmount_deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "native macFUSE unmount did not complete: {}",
                    path.display()
                ),
            ));
        }
        // fuse_unmount starts a detached macFUSE worker. Session destruction
        // does not acknowledge that worker's Disk Arbitration completion.
        thread::sleep(POLL_INTERVAL);
    }
    let removal_deadline = std::time::Instant::now() + removal_timeout;
    loop {
        match remove_dir(path) {
            Err(error)
                if error.raw_os_error() == Some(libc::EBUSY)
                    && std::time::Instant::now() < removal_deadline =>
            {
                // The mount table can clear before the mounted vnode is released.
                thread::sleep(POLL_INTERVAL);
            }
            result => return result,
        }
    }
}

#[test]
fn remove_mount_point_retries_busy_directory_after_unmount_budget_is_exhausted() {
    let mount_point = TempMountDir::create(studiofs_test_support::workspace_target_temp_path(
        "macfuse-busy-dir-",
    ))
    .expect("create unmounted test directory");
    let mut attempts = 0;
    remove_mount_point_with(
        mount_point.path(),
        std::time::Instant::now(),
        MOUNT_POINT_REMOVAL_TIMEOUT,
        |path| {
            attempts += 1;
            if attempts < 3 {
                Err(std::io::Error::from_raw_os_error(libc::EBUSY))
            } else {
                fs::remove_dir(path)
            }
        },
    )
    .expect("remove directory after transient EBUSY");
    assert_eq!(attempts, 3);
    assert!(!mount_point.path().exists());
}

#[test]
fn remove_mount_point_returns_terminal_removal_errors() {
    let mount_point = TempMountDir::create(studiofs_test_support::workspace_target_temp_path(
        "macfuse-terminal-dir-",
    ))
    .expect("create unmounted test directory");
    for (errno, budget) in [
        (libc::EBUSY, Duration::ZERO),
        (libc::EPERM, MOUNT_POINT_REMOVAL_TIMEOUT),
    ] {
        let mut attempts = 0;
        let error = remove_mount_point_with(
            mount_point.path(),
            std::time::Instant::now() + UNMOUNT_TIMEOUT,
            budget,
            |_| {
                attempts += 1;
                Err(std::io::Error::from_raw_os_error(errno))
            },
        )
        .expect_err("return expired EBUSY or non-retryable error");
        assert_eq!(error.raw_os_error(), Some(errno));
        assert_eq!(attempts, 1);
    }
}

#[test]
fn mount_table_reports_unmounted_directory_as_inactive() {
    assert!(mount_point_is_active(Path::new("/")).expect("read root mount"));
    let mount_point = TempMountDir::create(studiofs_test_support::workspace_target_temp_path(
        "macfuse-mount-table-",
    ))
    .expect("create unmounted test directory");
    assert!(!mount_point_is_active(mount_point.path()).expect("read mount table"));
    remove_mount_point(mount_point.path()).expect("remove unmounted directory");
}

#[test]
fn temp_mount_dir_resolves_symlinked_prefix_before_mount() {
    let scratch = studiofs_test_support::workspace_target_tempdir("macfuse-dir-alias-");
    let canonical_root = fs::canonicalize(scratch.path()).expect("resolve test directory");
    let alias = scratch.path().join("alias");
    std::os::unix::fs::symlink(&canonical_root, &alias).expect("create directory alias");
    let mount_point =
        TempMountDir::create(alias.join("mount")).expect("create aliased mount point");
    assert_eq!(mount_point.path(), canonical_root.join("mount"));
}

#[test]
fn temp_mount_dir_removes_unmounted_directory() {
    let path = studiofs_test_support::workspace_target_temp_path("macfuse-session-dir-");
    {
        let mount_point = TempMountDir::create(path.clone()).expect("create test directory");
        assert!(mount_point.path().is_dir());
    }
    assert!(!path.exists());
}

#[test]
#[ignore = "requires an installed legacy macFUSE filesystem and permission to mount"]
fn legacy_novncache_namespace_changes_do_not_need_entry_notifications() {
    const UNSEEN_PARENT: u64 = 9_876_543_210;
    const CHILD_NAME: &[u8] = b"observed-child";
    const ROOT_MISSING_NAME: &[u8] = b"missing-root-entry";
    const CHILD_MISSING_NAME: &[u8] = b"missing-child-entry";
    const CHILD_FILE_NAME: &[u8] = b"child-file";
    const INITIAL_NAME: &[u8] = b"initial-entry";
    const NEGATIVE_NAME: &[u8] = b"negative-then-created";
    const RECREATE_NAME: &[u8] = b"recreate-entry";
    const RENAME_SOURCE_NAME: &[u8] = b"rename-source";
    const RENAME_TARGET_NAME: &[u8] = b"rename-target";
    const MOUNT_READY_NAME: &[u8] = b"mount-ready";

    fn notify_entry(channel: super::SessionChannel, parent: u64, name: &CStr) -> i32 {
        // SAFETY: Callers keep the captured channel attached to the live
        // session, and `name` provides a NUL-terminated buffer for libfuse.
        unsafe {
            super::raw::fuse_lowlevel_notify_inval_entry(
                channel.0.as_ptr(),
                parent,
                name.as_ptr(),
                name.to_bytes().len(),
            )
        }
    }

    let _serial = NATIVE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let fixture =
        ContractVfsFixture::new("macfuse-novncache-namespace-convergence").expect("create fixture");
    let vfs = fixture.vfs();
    // SAFETY: getuid/getgid are process-wide identity queries with no preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let child = vfs
        .mkdir(CONTRACT_ROOT_INO, CHILD_NAME, uid, gid, 0o755)
        .expect("create child directory");
    let child_file = vfs
        .create(child.ino, CHILD_FILE_NAME, uid, gid, 0o644)
        .expect("create child directory entry");
    let initial = vfs
        .create(CONTRACT_ROOT_INO, INITIAL_NAME, uid, gid, 0o644)
        .expect("create initial entry");
    let recreate_before = vfs
        .create(CONTRACT_ROOT_INO, RECREATE_NAME, uid, gid, 0o644)
        .expect("create entry to unlink and recreate");
    let rename_source = vfs
        .create(CONTRACT_ROOT_INO, RENAME_SOURCE_NAME, uid, gid, 0o644)
        .expect("create rename source");
    let rename_target = vfs
        .create(CONTRACT_ROOT_INO, RENAME_TARGET_NAME, uid, gid, 0o644)
        .expect("create rename destination");
    vfs.create(CONTRACT_ROOT_INO, MOUNT_READY_NAME, uid, gid, 0o644)
        .expect("create mount readiness entry");

    let callback_state = Box::new(crate::adapter::CallbackState::new(
        vfs.clone(),
        fixture.runtime().handle().clone(),
        false,
    ));
    let mount_point = TempMountDir::create(studiofs_test_support::workspace_target_temp_path(
        "macfuse-novncache-namespace-",
    ))
    .expect("create mount point");
    let config = SessionConfig::new(mount_point.path(), "StudioFS novncache probe")
        .with_run_in_foreground(true);
    let userdata = (&*callback_state as *const crate::adapter::CallbackState)
        .cast_mut()
        .cast::<c_void>();
    // SAFETY: The native-test lock serializes access to C's header-derived
    // static table, and libfuse copies it during session creation.
    let ops = unsafe { studiofs_macfuse_test_ops_with_write(blocked_write) };
    // SAFETY: The C helper returns the size of the installed macFUSE ops table.
    let ops_size = unsafe { super::studiofs_macfuse_low_level_ops_size() };
    // SAFETY: The operation table is static and callback_state stays boxed until
    // after the native session is destroyed.
    let mut session = unsafe {
        Session::mount_with_ops(
            &config,
            LowLevelOperations {
                raw: ops,
                size: ops_size,
            },
            userdata,
        )
    }
    .expect("mount native legacy macFUSE session");
    let channel = super::SessionChannel(session.channel);
    let invalidator = callback_state.kernel_invalidator(session.kernel_invalidator());
    vfs.set_kernel_invalidator(invalidator);
    let exit = session.exit_handle();
    let probe_mount_path = mount_point.path().to_path_buf();
    let probe_vfs = vfs.clone();
    let expected_child_ino = child.ino;
    let expected_child_file_ino = child_file.ino;
    let expected_initial_ino = initial.ino;
    let expected_recreate_before_ino = recreate_before.ino;
    let expected_rename_source_ino = rename_source.ino;
    let expected_rename_target_ino = rename_target.ino;
    let (session_run_tx, session_run_rx) = mpsc::channel();
    let (destroy_session_tx, destroy_session_rx) = mpsc::channel();
    let (destroy_done_tx, destroy_done_rx) = mpsc::channel();
    let session_worker = thread::spawn(move || {
        let result = session.run();
        let _ = session_run_tx.send(result);
        let _ = destroy_session_rx.recv();
        callback_state.wait_for_reply_tasks();
        drop(session);
        drop(callback_state);
        let _ = destroy_done_tx.send(());
    });

    let (native_probe_tx, native_probe_rx) = mpsc::channel();
    let native_probe_worker = thread::spawn(move || {
        // Defer assertion panics until the caller has destroyed the session
        // and callback userdata, just as it defers ordinary probe errors.
        let native_probe = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let readiness_deadline = std::time::Instant::now() + OPERATION_TIMEOUT;
            let ready_path = probe_mount_path.join(std::ffi::OsStr::from_bytes(MOUNT_READY_NAME));
            loop {
                match fs::metadata(&ready_path) {
                    Ok(_) => break,
                    Err(error)
                        if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENXIO))
                            && std::time::Instant::now() < readiness_deadline =>
                    {
                        thread::sleep(POLL_INTERVAL);
                    }
                    Err(error) => return Err(error),
                }
            }

            let root_entries = fs::read_dir(&probe_mount_path)?.collect::<Result<Vec<_>, _>>()?;
            let root_path = probe_mount_path;
            let observed_root_ino = fs::metadata(&root_path)?.ino();
            let child_path = root_path.join(std::ffi::OsStr::from_bytes(CHILD_NAME));
            let observed_child_ino = fs::metadata(&child_path)?.ino();
            let root_child_ino = root_entries
                .iter()
                .find(|entry| entry.file_name().as_bytes() == CHILD_NAME)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "root readdir did not return the expected child",
                    )
                })?
                .metadata()?
                .ino();
            let child_entries = fs::read_dir(&child_path)?.collect::<Result<Vec<_>, _>>()?;
            let child_file_path = child_path.join(std::ffi::OsStr::from_bytes(CHILD_FILE_NAME));
            let observed_child_file_ino = fs::metadata(&child_file_path)?.ino();
            let initial_path = root_path.join(std::ffi::OsStr::from_bytes(INITIAL_NAME));
            let observed_initial_ino = fs::metadata(&initial_path)?.ino();

            let root_missing_name =
                std::ffi::CString::new(ROOT_MISSING_NAME).expect("NUL-free name");
            let child_missing_name =
                std::ffi::CString::new(CHILD_MISSING_NAME).expect("NUL-free name");
            let root_child_name = std::ffi::CString::new(CHILD_NAME).expect("NUL-free name");
            let child_file_name = std::ffi::CString::new(CHILD_FILE_NAME).expect("NUL-free name");
            let unseen_parent_result = notify_entry(channel, UNSEEN_PARENT, &root_missing_name);
            let root_missing_name_result =
                notify_entry(channel, observed_root_ino, &root_missing_name);
            let root_known_name_result = notify_entry(channel, observed_root_ino, &root_child_name);
            let child_missing_name_result =
                notify_entry(channel, observed_child_ino, &child_missing_name);
            let child_known_name_result =
                notify_entry(channel, observed_child_ino, &child_file_name);
            // SAFETY: The channel remains alive and the inode identifier came
            // from a successful metadata lookup in this mounted session.
            let observed_root_inode_result = unsafe {
                super::raw::fuse_lowlevel_notify_inval_inode(
                    channel.0.as_ptr(),
                    observed_root_ino,
                    -1,
                    0,
                )
            };

            let negative_path = root_path.join(std::ffi::OsStr::from_bytes(NEGATIVE_NAME));
            let initial_negative_lookup = fs::metadata(&negative_path);
            if !matches!(initial_negative_lookup, Err(ref error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                return Err(std::io::Error::other(format!(
                    "expected initial negative lookup, got {initial_negative_lookup:?}"
                )));
            }
            let created_after_negative = probe_vfs
                .create(CONTRACT_ROOT_INO, NEGATIVE_NAME, uid, gid, 0o644)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let observed_created_after_negative = fs::metadata(&negative_path)?.ino();

            let recreate_path = root_path.join(std::ffi::OsStr::from_bytes(RECREATE_NAME));
            let observed_recreate_before = fs::metadata(&recreate_path)?.ino();
            probe_vfs
                .unlink(CONTRACT_ROOT_INO, RECREATE_NAME)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let missing_after_unlink = fs::metadata(&recreate_path);
            if !matches!(missing_after_unlink, Err(ref error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                return Err(std::io::Error::other(format!(
                    "expected pathname to disappear after unlink, got {missing_after_unlink:?}"
                )));
            }
            let recreated = probe_vfs
                .create(CONTRACT_ROOT_INO, RECREATE_NAME, uid, gid, 0o644)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let observed_recreated = fs::metadata(&recreate_path)?.ino();

            let rename_source_path =
                root_path.join(std::ffi::OsStr::from_bytes(RENAME_SOURCE_NAME));
            let rename_target_path =
                root_path.join(std::ffi::OsStr::from_bytes(RENAME_TARGET_NAME));
            let observed_rename_source = fs::metadata(&rename_source_path)?.ino();
            let observed_rename_target_before = fs::metadata(&rename_target_path)?.ino();
            probe_vfs
                .rename(
                    CONTRACT_ROOT_INO,
                    RENAME_SOURCE_NAME,
                    CONTRACT_ROOT_INO,
                    RENAME_TARGET_NAME,
                    true,
                )
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let missing_rename_source = fs::metadata(&rename_source_path);
            if !matches!(missing_rename_source, Err(ref error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                return Err(std::io::Error::other(format!(
                    "expected rename source to disappear, got {missing_rename_source:?}"
                )));
            }
            let observed_rename_target_after = fs::metadata(&rename_target_path)?.ino();

            // Release the collected entries, which may retain directory
            // resources, before enumerating the namespace after mutations.
            drop(root_entries);
            let reopened_entries = fs::read_dir(&root_path)?.collect::<Result<Vec<_>, _>>()?;
            let reopened_names = reopened_entries
                .iter()
                .map(|entry| entry.file_name())
                .collect::<std::collections::BTreeSet<_>>();

            assert_eq!(observed_root_ino, CONTRACT_ROOT_INO);
            assert_eq!(observed_child_ino, expected_child_ino);
            assert_eq!(root_child_ino, expected_child_ino);
            assert_eq!(observed_child_file_ino, expected_child_file_ino);
            assert_eq!(child_entries.len(), 1);
            assert_eq!(observed_initial_ino, expected_initial_ino);
            assert_eq!(unseen_parent_result, -libc::ENOENT);
            assert_eq!(root_missing_name_result, -libc::ENOENT);
            assert_eq!(root_known_name_result, -libc::ENOENT);
            assert_eq!(child_missing_name_result, -libc::ENOENT);
            assert_eq!(child_known_name_result, -libc::ENOENT);
            assert_eq!(observed_root_inode_result, 0);
            assert_eq!(observed_created_after_negative, created_after_negative.ino);
            assert_ne!(observed_recreate_before, observed_recreated);
            assert_eq!(observed_recreate_before, expected_recreate_before_ino);
            assert_eq!(observed_recreated, recreated.ino);
            assert_eq!(observed_rename_source, expected_rename_source_ino);
            assert_eq!(observed_rename_target_before, expected_rename_target_ino);
            assert_ne!(observed_rename_target_before, observed_rename_target_after);
            assert_eq!(observed_rename_target_after, expected_rename_source_ino);
            assert!(reopened_names.contains(std::ffi::OsStr::from_bytes(NEGATIVE_NAME)));
            assert!(reopened_names.contains(std::ffi::OsStr::from_bytes(RECREATE_NAME)));
            assert!(reopened_names.contains(std::ffi::OsStr::from_bytes(RENAME_TARGET_NAME)));
            assert!(!reopened_names.contains(std::ffi::OsStr::from_bytes(RENAME_SOURCE_NAME)));
            Ok::<_, std::io::Error>(())
        }));
        let _ = native_probe_tx.send(native_probe);
    });

    let native_probe_received = native_probe_rx.recv_timeout(OPERATION_TIMEOUT.saturating_mul(2));
    let probe_timed_out = native_probe_received.is_err();
    if probe_timed_out {
        exit.request_exit();
    }
    let native_probe = match native_probe_received {
        Ok(result) => result,
        Err(_) => receive_before_teardown(&native_probe_rx, "native novncache namespace probe"),
    };
    native_probe_worker
        .join()
        .expect("join native novncache namespace probe");

    vfs.clear_kernel_invalidator();
    exit.request_exit();
    let session_result = receive_before_teardown(&session_run_rx, "session loop shutdown");
    let _ = destroy_session_tx.send(());
    receive_before_teardown(&destroy_done_rx, "session destruction");
    session_worker.join().expect("join session worker");
    let mount_cleanup = remove_mount_point(mount_point.path());
    let fixture_shutdown = fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO));

    session_result.expect("native session loop failed");
    mount_cleanup.expect("remove native mount point");
    fixture_shutdown.expect("fixture VFS shutdown should succeed");
    assert!(
        !probe_timed_out,
        "native namespace probe exceeded its operation deadline"
    );
    match native_probe {
        Ok(result) => result.expect("native directory read or namespace mutation failed"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[test]
#[ignore = "requires an installed legacy macFUSE filesystem and permission to mount"]
fn session_serves_peer_request_and_drains_blocked_callback_on_exit() {
    let _serial = NATIVE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let fixture =
        ContractVfsFixture::new("macfuse-session-concurrency").expect("create VFS fixture");
    let vfs = fixture.vfs();
    // SAFETY: getuid/getgid are process-wide identity queries with no preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    vfs.create(CONTRACT_ROOT_INO, b"blocked-callback", uid, gid, 0o644)
        .expect("create callback that will block");
    vfs.create(CONTRACT_ROOT_INO, b"mount-ready", uid, gid, 0o644)
        .expect("create mount readiness file");
    vfs.create(CONTRACT_ROOT_INO, b"unrelated-request", uid, gid, 0o644)
        .expect("create unrelated request file");

    let mount_point = TempMountDir::create(studiofs_test_support::workspace_target_temp_path(
        "macfuse-session-concurrency-",
    ))
    .expect("create target mount point");
    let config = SessionConfig::new(mount_point.path(), "StudioFS Session Concurrency")
        .with_run_in_foreground(true);
    let callback_state = Box::new(crate::adapter::CallbackState::new(
        vfs,
        fixture.runtime().handle().clone(),
        false,
    ));
    let userdata = (&*callback_state as *const crate::adapter::CallbackState)
        .cast_mut()
        .cast::<c_void>();
    // SAFETY: The native-test lock serializes access to C's header-derived
    // static table, and libfuse copies it during session creation.
    let ops = unsafe { studiofs_macfuse_test_ops_with_write(blocked_write) };
    // SAFETY: The C shim returns the size of the installed macFUSE header's ops structure.
    let ops_size = unsafe { super::studiofs_macfuse_low_level_ops_size() };
    // SAFETY: The C helper supplies a valid table until this call returns, and the
    // boxed callback state remains live until after the session is destroyed.
    let mut session = unsafe {
        Session::mount_with_ops(
            &config,
            LowLevelOperations {
                raw: ops,
                size: ops_size,
            },
            userdata,
        )
    }
    .expect("mount native legacy macFUSE session");
    let exit = session.exit_handle();
    let (session_run_tx, session_run_rx) = mpsc::channel();
    let (destroy_session_tx, destroy_session_rx) = mpsc::channel();
    let (destroy_done_tx, destroy_done_rx) = mpsc::channel();
    let session_worker = thread::spawn(move || {
        let result = session.run();
        let _ = session_run_tx.send(result);
        // Preserve userdata if the test detects an early loop return. Destroy
        // it only after the caller has collected the outstanding callback replies.
        let _ = destroy_session_rx.recv();
        callback_state.wait_for_reply_tasks();
        drop(session);
        drop(callback_state);
        let _ = destroy_done_tx.send(());
    });

    let ready_path = mount_point.path().join("mount-ready");
    let deadline = std::time::Instant::now() + OPERATION_TIMEOUT;
    let (ready_tx, ready_rx) = mpsc::channel();
    let ready_worker = thread::spawn(move || {
        while std::time::Instant::now() < deadline {
            if fs::metadata(&ready_path).is_ok() {
                let _ = ready_tx.send(true);
                return;
            }
            thread::sleep(POLL_INTERVAL);
        }
        let _ = ready_tx.send(false);
    });
    // Allow the worker's final 10 ms polling slice to send its terminal result.
    let ready_result = ready_rx.recv_timeout(OPERATION_TIMEOUT + Duration::from_millis(500));
    // Keep both write handles open across unmount so worker-thread drops cannot
    // enqueue release callbacks while the session is shutting down.
    let setup = if matches!(ready_result, Ok(true)) {
        (|| {
            let blocked_file = Arc::new(
                OpenOptions::new()
                    .write(true)
                    .open(mount_point.path().join("blocked-callback"))?,
            );
            let unrelated_file = Arc::new(
                OpenOptions::new()
                    .write(true)
                    .open(mount_point.path().join("unrelated-request"))?,
            );
            let blocked_inode = blocked_file.metadata()?.ino();
            Ok::<_, std::io::Error>((blocked_file, unrelated_file, blocked_inode))
        })()
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "mount readiness request did not complete",
        ))
    };
    let (blocked_file, unrelated_file, blocked_inode) = match setup {
        Ok(handles) => handles,
        Err(error) => {
            exit.request_exit();
            let _ = receive_before_teardown(&session_run_rx, "setup shutdown");
            let _ = destroy_session_tx.send(());
            receive_before_teardown(&destroy_done_rx, "setup session destruction");
            session_worker.join().expect("join failed session worker");
            // Unmount releases a readiness syscall that missed the initial deadline.
            // Keep its worker and the fixture owned until completion is confirmed.
            if ready_result.is_err() {
                receive_before_teardown(&ready_rx, "readiness syscall completion");
            }
            ready_worker.join().expect("join failed readiness worker");
            remove_mount_point(mount_point.path()).expect("remove failed native mount point");
            panic!("native macFUSE setup failed: {error}");
        }
    };
    ready_worker.join().expect("join readiness worker");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let gate = Arc::new(CallbackGate {
        blocked_inode,
        entered: entered_tx,
        finished: finished_tx,
        released: Mutex::new(release_rx),
    });
    *CALLBACK_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&gate));
    let gate_reset = GateReset(Some(release_tx));

    let blocked_write_file = Arc::clone(&blocked_file);
    let (blocked_done_tx, blocked_done_rx) = mpsc::channel();
    let blocked_worker = thread::spawn(move || {
        let mut file = &*blocked_write_file;
        let result = file.write_all(b"a");
        let _ = blocked_done_tx.send(result);
    });
    let entered = entered_rx.recv_timeout(OPERATION_TIMEOUT).is_ok();

    let unrelated_write_file = Arc::clone(&unrelated_file);
    let (unrelated_done_tx, unrelated_done_rx) = mpsc::channel();
    let unrelated_worker = thread::spawn(move || {
        let mut file = &*unrelated_write_file;
        let result = file.write_all(b"b");
        let _ = unrelated_done_tx.send(result);
    });
    let unrelated_early = unrelated_done_rx.recv_timeout(OPERATION_TIMEOUT);
    let unrelated_returned_while_blocked = matches!(&unrelated_early, Ok(Ok(())));

    // On the red path the peer write is still queued in the kernel. Let the
    // single-threaded loop reply to it before requesting exit, otherwise native
    // unmount can wait forever for that unreplied request.
    let exit_while_blocked = unrelated_returned_while_blocked.then(|| exit.request_exit());
    let session_run_early = if exit_while_blocked.is_some() {
        session_run_rx.recv_timeout(TEARDOWN_WAIT)
    } else {
        Err(mpsc::RecvTimeoutError::Timeout)
    };
    let run_returned_while_blocked = session_run_early.is_ok();

    drop(gate_reset);
    if entered {
        receive_before_teardown(&finished_rx, "blocked callback completion");
    }
    let blocked_result = receive_before_teardown(&blocked_done_rx, "blocked syscall reply");
    let unrelated_result = match unrelated_early {
        Ok(result) => result,
        Err(_) => receive_before_teardown(&unrelated_done_rx, "unrelated syscall reply"),
    };
    let exit_requested = exit_while_blocked.unwrap_or_else(|| exit.request_exit());
    let session_result = match session_run_early {
        Ok(result) => result,
        Err(_) => receive_before_teardown(&session_run_rx, "session loop shutdown"),
    };
    let _ = destroy_session_tx.send(());
    receive_before_teardown(&destroy_done_rx, "session destruction");
    blocked_worker.join().expect("join blocked request worker");
    unrelated_worker
        .join()
        .expect("join unrelated request worker");
    session_worker.join().expect("join session worker");
    drop(blocked_file);
    drop(unrelated_file);
    let mount_cleanup = remove_mount_point(mount_point.path());
    let fixture_shutdown = fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO));

    assert!(entered, "blocked write callback did not start");
    assert!(
        unrelated_returned_while_blocked,
        "unrelated native request did not return while write was blocked"
    );
    assert!(exit_requested, "session exit was rejected before teardown");
    assert!(
        !run_returned_while_blocked,
        "session loop returned while a callback was still active"
    );
    blocked_result.expect("blocked native write failed");
    unrelated_result.expect("unrelated native write failed");
    session_result.expect("native session loop failed");
    mount_cleanup.expect("remove native mount point");
    fixture_shutdown.expect("fixture VFS shutdown should succeed");
}
