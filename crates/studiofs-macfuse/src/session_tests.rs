//! Native regressions for the legacy macFUSE session loop.

#![cfg(target_os = "macos")]

use std::ffi::c_void;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};

use super::{LowLevelOperations, Session, SessionConfig, raw};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(15);
const TEARDOWN_WAIT: Duration = Duration::from_secs(1);

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
    crate::adapter::studiofs_macfuse_write(req, ino, buf, size, offset, fi);
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

fn stat_completes_before(path: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        let mut probe = Command::new("/usr/bin/stat")
            .args(["-f", "%i"])
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start macFUSE readiness probe");
        loop {
            if let Some(status) = probe.try_wait().expect("poll macFUSE readiness probe") {
                if status.success() {
                    return true;
                }
                break;
            }
            if std::time::Instant::now() >= deadline {
                probe
                    .kill()
                    .expect("stop timed-out macFUSE readiness probe");
                probe
                    .wait()
                    .expect("reap timed-out macFUSE readiness probe");
                return false;
            }
            thread::yield_now();
        }
    }
}

fn remove_mount_point(path: &Path) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + OPERATION_TIMEOUT;
    loop {
        match fs::remove_dir(path) {
            Err(error)
                if error.raw_os_error() == Some(libc::EBUSY)
                    && std::time::Instant::now() < deadline =>
            {
                // macFUSE may release the mounted vnode after fuse_unmount returns.
                thread::yield_now();
            }
            result => return result,
        }
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

    let mount_point =
        studiofs_test_support::workspace_target_temp_path("macfuse-session-concurrency-");
    fs::create_dir(&mount_point).expect("create target mount point");
    let config = SessionConfig::new(&mount_point, "StudioFS Session Concurrency")
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
    let session_worker = thread::spawn(move || {
        let result = session.run();
        let _ = session_run_tx.send(result);
        // Preserve userdata if the test detects an early loop return. Destroy
        // it only after the caller has collected the outstanding callback replies.
        let _ = destroy_session_rx.recv();
        callback_state.wait_for_reply_tasks();
        drop(session);
        drop(callback_state);
        // The result channel also bounds reply draining and native destruction.
        let _ = session_run_tx.send(Ok(()));
    });

    let ready_path = mount_point.join("mount-ready");
    let ready = stat_completes_before(&ready_path, OPERATION_TIMEOUT);
    // Keep both write handles open across unmount so worker-thread drops cannot
    // enqueue release callbacks while the session is shutting down.
    let setup = if ready {
        (|| {
            let blocked_file = Arc::new(
                OpenOptions::new()
                    .write(true)
                    .open(mount_point.join("blocked-callback"))?,
            );
            let unrelated_file = Arc::new(
                OpenOptions::new()
                    .write(true)
                    .open(mount_point.join("unrelated-request"))?,
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
            receive_before_teardown(&session_run_rx, "setup session destruction")
                .expect("setup session destruction failed");
            session_worker.join().expect("join failed session worker");
            remove_mount_point(&mount_point).expect("remove failed native mount point");
            panic!("native macFUSE setup failed: {error}");
        }
    };
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
    let mut gate_reset = GateReset(Some(release_tx));

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

    drop(gate_reset.0.take());
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
    receive_before_teardown(&session_run_rx, "session destruction")
        .expect("session destruction failed");
    blocked_worker.join().expect("join blocked request worker");
    unrelated_worker
        .join()
        .expect("join unrelated request worker");
    session_worker.join().expect("join session worker");
    drop(blocked_file);
    drop(unrelated_file);
    let mount_cleanup = remove_mount_point(&mount_point);
    let fixture_shutdown = fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO));

    assert!(ready, "mount readiness request did not complete");
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
