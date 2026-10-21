//! `studiofs umount` — unmount a previously mounted filesystem volume.

use anyhow::Result;
use clap::Args;

#[cfg(unix)]
use anyhow::anyhow;

type ReadPidFn = fn(&str) -> Result<(u32, String, String)>;
type RemovePidFn = fn(&str) -> Result<()>;
type IsRunningFn = fn(u32) -> bool;
type IsMountActiveFn = fn(&str) -> Result<bool>;

#[derive(Debug, Args)]
#[command(about = "Unmount a previously mounted filesystem volume")]
#[command(after_help = "\
EXAMPLES:
  studiofs umount Z:
  studiofs umount /mnt/studio
")]
pub(crate) struct UmountArgs {
    /// Mount point to unmount (e.g. Z: on Windows, /mnt/studio on Linux/macOS)
    mount_point: String,
}

impl UmountArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        run(&self.mount_point)
    }
}

fn run_with(
    mount_point: &str,
    read_pid_file: ReadPidFn,
    remove_pid_file: RemovePidFn,
    is_process_running: IsRunningFn,
    is_mount_active: IsMountActiveFn,
) -> Result<()> {
    let (pid, mp, volume) = read_pid_file(mount_point)?;

    if !is_mount_active(mount_point)? {
        remove_pid_file(mount_point)?;
        println!(
            "Mount for '{}' has no active lifecycle lock. Cleaned up stale state (recorded PID {}).",
            mp, pid
        );
        return Ok(());
    }

    if !is_process_running(pid) {
        if !is_mount_active(mount_point)? {
            remove_pid_file(mount_point)?;
            println!("Mount for '{mp}' exited during unmount preflight. Cleaned up stale state.");
            return Ok(());
        }
        anyhow::bail!(
            "Mount for '{}' is still lock-held, but the recorded PID {} is not running or cannot be accessed. Refusing to terminate an unrelated process. Check process permissions; retry from an elevated terminal if the mount was started elevated.",
            mp,
            pid
        );
    }

    println!(
        "Unmounting volume '{}' from '{}' (PID: {}) …",
        volume, mp, pid
    );

    #[cfg(windows)]
    unmount_windows(mount_point, pid, is_mount_active)?;
    #[cfg(unix)]
    unmount_unix(mount_point, pid, is_mount_active)?;

    #[cfg(windows)]
    if !wait_for_mount_inactive(mount_point, is_mount_active)? {
        anyhow::bail!(
            "mount process {pid} exited but the lifecycle lock is still held; leaving mount record intact"
        );
    }

    // Clean up the persisted mount record.
    remove_pid_file(mount_point)?;

    println!("✓ Successfully unmounted '{}'", mount_point);
    Ok(())
}

pub fn run(mount_point: &str) -> Result<()> {
    let mount_point = super::utils::normalize_mount_point(mount_point);
    run_with(
        &mount_point,
        super::utils::read_pid_file,
        super::utils::remove_inactive_pid_file,
        super::utils::is_process_running,
        super::utils::mount_is_active,
    )
}

// ─── Windows ─────────────────────────────────────────────────────────────────

#[cfg(windows)]
fn unmount_windows(mount_point: &str, pid: u32, is_mount_active: IsMountActiveFn) -> Result<()> {
    unmount_windows_with_timeout(mount_point, pid, 15_000, is_mount_active)
}

/// Windows releases a dead process's file locks asynchronously, so poll briefly after exit.
#[cfg(windows)]
fn wait_for_mount_inactive(mount_point: &str, is_mount_active: IsMountActiveFn) -> Result<bool> {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if !is_mount_active(mount_point)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(windows)]
fn unmount_windows_with_timeout(
    mount_point: &str,
    pid: u32,
    exit_timeout_ms: u32,
    is_mount_active: IsMountActiveFn,
) -> Result<()> {
    use anyhow::Context;
    use std::os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle};

    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut core::ffi::c_void;
        fn OpenEventW(access: u32, inherit: i32, name: *const u16) -> *mut core::ffi::c_void;
        fn SetEvent(event: *mut core::ffi::c_void) -> i32;
        fn WaitForSingleObject(handle: *mut core::ffi::c_void, timeout: u32) -> u32;
    }
    const SYNCHRONIZE: u32 = 0x00100000;
    const PROCESS_TERMINATE: u32 = 0x0001;
    const EVENT_MODIFY_STATE: u32 = 0x0002;
    const ERROR_INVALID_PARAMETER: i32 = 87;
    const ERROR_FILE_NOT_FOUND: i32 = 2;
    const WAIT_OBJECT_0: u32 = 0;
    const WAIT_TIMEOUT: u32 = 258;

    // SAFETY: OpenProcess accepts the access mask and PID; null is checked before ownership.
    let process = unsafe { OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
    if process.is_null() {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER) {
            return Ok(());
        }
        return Err(error).with_context(|| format!("could not open mount process {pid}; check process permissions and retry from an elevated terminal if the mount was started elevated"));
    }
    // SAFETY: OpenProcess returned a valid, uniquely owned kernel handle.
    let process = unsafe { OwnedHandle::from_raw_handle(process) };
    // The open handle pins the PID; if the lock is already free, the mount exited and the PID
    // may belong to an unrelated process, so never signal or terminate it.
    if !is_mount_active(mount_point)? {
        return Ok(());
    }
    let wide: Vec<u16> = super::utils::shutdown_event_name(mount_point)
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: The NUL-terminated UTF-16 name remains alive for the call.
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
    if event.is_null() {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND) {
            return Err(error).context("could not open shutdown event");
        }
        tracing::warn!("could not open shutdown event — will attempt process termination");
    } else {
        // SAFETY: OpenEventW returned a valid, uniquely owned kernel handle.
        let event = unsafe { OwnedHandle::from_raw_handle(event) };
        // SAFETY: The owned event handle is open and has modify access.
        if unsafe { SetEvent(event.as_raw_handle()) } == 0 {
            return Err(std::io::Error::last_os_error())
                .context("could not signal mount shutdown event");
        }
    }

    let wait_for_exit = || -> Result<bool> {
        // SAFETY: The owned process handle has SYNCHRONIZE access and outlives the wait.
        match unsafe { WaitForSingleObject(process.as_raw_handle(), exit_timeout_ms) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            u32::MAX => {
                Err(std::io::Error::last_os_error()).context("could not wait for mount process")
            }
            result => anyhow::bail!("unexpected process wait result: {result:#x}"),
        }
    };
    if wait_for_exit()? {
        return Ok(());
    }
    tracing::warn!(
        exit_timeout_ms,
        "mount process did not exit before timeout — force-killing"
    );
    terminate_windows_process(process.as_handle()).context("could not terminate mount process")?;
    if !wait_for_exit().context("could not confirm terminated mount process exit")? {
        anyhow::bail!(
            "mount process {pid} did not exit within {exit_timeout_ms} ms after termination; leaving mount record intact"
        );
    }
    Ok(())
}

#[cfg(windows)]
fn terminate_windows_process(
    process: std::os::windows::io::BorrowedHandle<'_>,
) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    unsafe extern "system" {
        fn TerminateProcess(process: *mut core::ffi::c_void, exit_code: u32) -> i32;
        fn WaitForSingleObject(handle: *mut core::ffi::c_void, timeout: u32) -> u32;
    }
    // SAFETY: The borrowed handle remains open; Windows validates the requested access.
    if unsafe { TerminateProcess(process.as_raw_handle(), 1) } == 0 {
        let error = std::io::Error::last_os_error();
        // Exit can win the race after the graceful wait times out.
        // SAFETY: The borrowed process handle remains open for this nonblocking wait.
        if unsafe { WaitForSingleObject(process.as_raw_handle(), 0) } != 0 {
            return Err(error);
        }
    }
    Ok(())
}

// ─── Unix ────────────────────────────────────────────────────────────────────

#[cfg(unix)]
type UnixSignalFn = fn(u32, libc::c_int) -> std::io::Result<()>;
#[cfg(unix)]
type UnixDetachFn = fn(&str, std::time::Duration) -> Result<()>;

#[cfg(unix)]
#[derive(Clone, Copy)]
struct UnixUnmountTiming {
    detach_timeout: std::time::Duration,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
}

#[cfg(unix)]
fn send_unix_signal(pid: u32, signal: libc::c_int) -> std::io::Result<()> {
    // SAFETY: `kill` is called with a concrete PID and signal supplied by the
    // caller. Signal 0 is the standard existence probe and has no side effects.
    if unsafe { libc::kill(pid as i32, signal) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
const CHILD_REAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[cfg(unix)]
fn reap_child_with_timeout(child: &mut std::process::Child, timeout: std::time::Duration) -> bool {
    use std::time::Instant;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) if Instant::now() >= deadline => return false,
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(25)),
            Err(_) => return false,
        }
    }
}

#[cfg(unix)]
fn unmount_unix(mount_point: &str, pid: u32, is_mount_active: IsMountActiveFn) -> Result<()> {
    use std::time::Duration;

    unmount_unix_with_detach(
        mount_point,
        pid,
        is_mount_active,
        send_unix_signal,
        detach_unix_mount,
        UnixUnmountTiming {
            detach_timeout: Duration::from_secs(10),
            timeout: Duration::from_secs(15),
            poll_interval: Duration::from_millis(200),
        },
    )
}

#[cfg(target_os = "linux")]
fn detach_unix_mount(mount_point: &str, timeout: std::time::Duration) -> Result<()> {
    use anyhow::Context;
    use std::ffi::CString;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let c_mount_point = CString::new(mount_point)
        .map_err(|_| anyhow!("mount point contains an embedded NUL: {mount_point:?}"))?;
    // SAFETY: `c_mount_point` is a valid NUL-terminated path string.
    if unsafe { libc::umount2(c_mount_point.as_ptr(), libc::MNT_DETACH) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
        return Ok(());
    }

    for bin in ["fusermount3", "fusermount"] {
        let mut child = match Command::new(bin)
            .arg("-u")
            .arg(mount_point)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("start {bin} -u")),
        };
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("wait for {bin} -u"))?
            {
                return if status.success() {
                    Ok(())
                } else {
                    Err(anyhow!("{bin} -u exited with status {status}"))
                };
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = reap_child_with_timeout(&mut child, CHILD_REAP_TIMEOUT);
                anyhow::bail!("{bin} -u did not finish within {}s", timeout.as_secs());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    Err(err).context("detach FUSE mount with umount2; fusermount not found")
}

#[cfg(target_os = "macos")]
fn detach_unix_mount(mount_point: &str, timeout: std::time::Duration) -> Result<()> {
    detach_unix_mount_with(mount_point, timeout, run_macos_unmount_command)
}

#[cfg(target_os = "macos")]
fn detach_unix_mount_with(
    mount_point: &str,
    timeout: std::time::Duration,
    run: impl FnOnce(&str, &str, &[&str], std::time::Duration) -> Result<()>,
) -> Result<()> {
    run(mount_point, "/sbin/umount", &["-f"], timeout)
}

#[cfg(target_os = "macos")]
fn run_macos_unmount_command(
    mount_point: &str,
    program: &str,
    args: &[&str],
    timeout: std::time::Duration,
) -> Result<()> {
    use anyhow::Context;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let mut child = Command::new(program)
        .args(args)
        .arg(mount_point)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("start {program} {}", args.join(" ")))?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("wait for {program} {}", args.join(" ")))?
        {
            if status.success() {
                return Ok(());
            }
            anyhow::bail!(
                "{program} {} {mount_point} exited with status {status}",
                args.join(" ")
            );
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = reap_child_with_timeout(&mut child, CHILD_REAP_TIMEOUT);
            anyhow::bail!(
                "{program} {} {mount_point} did not finish within {} ms",
                args.join(" "),
                timeout.as_millis()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
fn detach_unix_mount(_mount_point: &str, _timeout: std::time::Duration) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn unmount_unix_with_detach(
    mount_point: &str,
    pid: u32,
    is_mount_active: IsMountActiveFn,
    signal: UnixSignalFn,
    detach: UnixDetachFn,
    timing: UnixUnmountTiming,
) -> Result<()> {
    use std::time::Instant;

    detach(mount_point, timing.detach_timeout)?;

    // Send SIGTERM after detaching the kernel mount so the FUSE background
    // thread can return naturally instead of unmounting from inside shutdown.
    if let Err(err) = signal(pid, libc::SIGTERM) {
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(()); // already gone
        }
        return Err(anyhow!("failed to send SIGTERM to PID {}: {}", pid, err));
    }

    // A successfully exited child can remain visible to kill(pid, 0) as a
    // zombie until its parent reaps it. The mountpoint state is the contract
    // this command owns, so stop once it is no longer active.
    let deadline = Instant::now() + timing.timeout;
    loop {
        std::thread::sleep(timing.poll_interval);
        if !is_mount_active(mount_point)? {
            break;
        }
        if let Err(err) = signal(pid, 0) {
            if err.raw_os_error() == Some(libc::ESRCH) {
                break; // gone
            }
            break; // gone
        }
        if Instant::now() > deadline {
            tracing::warn!("mount process did not exit within 15 s — sending SIGKILL");
            let _ = signal(pid, libc::SIGKILL);
            std::thread::sleep(timing.poll_interval);
            if is_mount_active(mount_point)? {
                anyhow::bail!(
                    "mount process {pid} did not release lifecycle lock after SIGKILL; leaving mount record intact"
                );
            }
            break;
        }
    }

    Ok(())
}

#[cfg(unix)]
#[cfg(test)]
fn unmount_unix_with(
    mount_point: &str,
    pid: u32,
    is_mount_active: IsMountActiveFn,
    signal: UnixSignalFn,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
) -> Result<()> {
    unmount_unix_with_detach(
        mount_point,
        pid,
        is_mount_active,
        signal,
        |_mount_point, _timeout| Ok(()),
        UnixUnmountTiming {
            detach_timeout: std::time::Duration::ZERO,
            timeout,
            poll_interval,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::sync::{Mutex, OnceLock};

    #[derive(Default)]
    struct TestPidState {
        value: Option<(u32, String, String)>,
        removed: bool,
    }

    static TEST_PID_STATE: OnceLock<Mutex<TestPidState>> = OnceLock::new();
    static TEST_PID_LOCK: Mutex<()> = Mutex::new(());
    #[cfg(unix)]
    static TEST_UNIX_SIGNALS: OnceLock<Mutex<Vec<libc::c_int>>> = OnceLock::new();
    #[cfg(unix)]
    static TEST_UNIX_EVENTS: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();
    #[cfg(unix)]
    static TEST_UNIX_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn pid_state() -> std::sync::MutexGuard<'static, TestPidState> {
        TEST_PID_STATE
            .get_or_init(|| Mutex::new(TestPidState::default()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn pid_test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_PID_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn test_read_pid_file(mount_point: &str) -> Result<(u32, String, String)> {
        let state = pid_state();
        state
            .value
            .clone()
            .ok_or_else(|| anyhow!("No mount found for '{mount_point}'. Is it mounted?"))
    }

    fn test_remove_pid_file(_mount_point: &str) -> Result<()> {
        let mut state = pid_state();
        state.value = None;
        state.removed = true;
        Ok(())
    }

    fn test_process_running(_pid: u32) -> bool {
        true
    }

    fn test_mount_not_active(_mount_point: &str) -> Result<bool> {
        Ok(false)
    }

    fn test_mount_active_once(_mount_point: &str) -> Result<bool> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CHECKS: AtomicUsize = AtomicUsize::new(0);
        Ok(CHECKS.fetch_add(1, Ordering::SeqCst) == 0)
    }

    #[test]
    fn process_exit_during_preflight_rechecks_the_lifecycle_lock() {
        let _guard = pid_test_lock();
        let mp = "__test_preflight_exit__";
        *pid_state() = TestPidState {
            value: Some((u32::MAX, mp.into(), "test".into())),
            removed: false,
        };
        let result = run_with(
            mp,
            test_read_pid_file,
            test_remove_pid_file,
            |_| false,
            test_mount_active_once,
        );
        assert!(result.is_ok(), "{result:?}");
        assert!(pid_state().removed);
    }

    #[test]
    fn record_removal_errors_are_reported() {
        let _guard = pid_test_lock();
        let mp = "__test_remove_failure__";
        *pid_state() = TestPidState {
            value: Some((u32::MAX, mp.into(), "test".into())),
            removed: false,
        };
        let result = run_with(
            mp,
            test_read_pid_file,
            |_| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()),
            test_process_running,
            test_mount_not_active,
        );
        assert!(result.is_err());
        assert!(pid_state().value.is_some());
    }

    #[cfg(any(unix, windows))]
    fn test_mount_active(_mount_point: &str) -> Result<bool> {
        Ok(true)
    }

    #[cfg(unix)]
    fn unix_signal_log() -> &'static Mutex<Vec<libc::c_int>> {
        TEST_UNIX_SIGNALS.get_or_init(|| Mutex::new(Vec::new()))
    }

    #[cfg(unix)]
    fn unix_test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_UNIX_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(unix)]
    fn record_unix_signal(_pid: u32, signal: libc::c_int) -> std::io::Result<()> {
        unix_signal_log().lock().unwrap().push(signal);
        unix_event_log().lock().unwrap().push("signal");
        Ok(())
    }

    #[cfg(unix)]
    fn unix_event_log() -> &'static Mutex<Vec<&'static str>> {
        TEST_UNIX_EVENTS.get_or_init(|| Mutex::new(Vec::new()))
    }

    #[cfg(unix)]
    fn record_unix_detach(_mount_point: &str, _timeout: std::time::Duration) -> Result<()> {
        unix_event_log().lock().unwrap().push("detach");
        Ok(())
    }

    /// `umount` on a mount point with no persisted mount record should return an error.
    #[test]
    fn umount_no_pid_file() {
        // Use a mount point that is almost certainly not mounted.
        let result = run("__nonexistent_test_mount_point_42__");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("No mount found") || msg.contains("Is it mounted?"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn umount_parses_mount_point() {
        let cli = crate::cli::parse_for_test(&["umount", "Z:"]).unwrap();
        match cli.command {
            crate::cli::Commands::Umount(UmountArgs { mount_point }) => {
                assert_eq!(mount_point, "Z:");
            }
            other => panic!("expected Umount, got {other:?}"),
        }
    }

    #[test]
    fn umount_unix_path() {
        let cli = crate::cli::parse_for_test(&["umount", "/mnt/studio"]).unwrap();
        match cli.command {
            crate::cli::Commands::Umount(UmountArgs { mount_point }) => {
                assert_eq!(mount_point, "/mnt/studio");
            }
            other => panic!("expected Umount, got {other:?}"),
        }
    }

    #[test]
    fn umount_requires_mount_point() {
        let result = crate::cli::parse_for_test(&["umount"]);
        assert!(result.is_err());
    }

    /// If a mount record exists but no process still holds the lifecycle lock, `umount` should
    /// clean it up without trusting the recorded PID.
    #[test]
    fn umount_stale_pid_file_cleanup() {
        let _guard = pid_test_lock();
        let mp = "__test_stale_umount__";
        {
            let mut state = pid_state();
            state.value = Some((0xFFFF_FFFE, mp.to_string(), "fakevol".to_string()));
            state.removed = false;
        }

        // Should succeed (prints "no longer running" and cleans up).
        let result = run_with(
            mp,
            test_read_pid_file,
            test_remove_pid_file,
            test_process_running,
            test_mount_not_active,
        );
        assert!(
            result.is_ok(),
            "umount of stale PID should succeed: {result:?}"
        );

        let state = pid_state();
        assert!(state.removed, "PID file should have been removed");
        assert!(state.value.is_none(), "PID state should be cleared");
    }

    #[cfg(unix)]
    #[test]
    fn unix_unmount_returns_once_mountpoint_is_inactive() {
        use std::time::Duration;

        let _guard = unix_test_lock();
        unix_signal_log().lock().unwrap().clear();

        let result = unmount_unix_with(
            "/mnt/studiofs-test",
            12345,
            test_mount_not_active,
            record_unix_signal,
            Duration::ZERO,
            Duration::ZERO,
        );

        assert!(
            result.is_ok(),
            "inactive mountpoint should finish graceful unix unmount: {result:?}"
        );
        assert_eq!(
            *unix_signal_log().lock().unwrap(),
            vec![libc::SIGTERM],
            "inactive mountpoint should not wait for kill(pid, 0) or send SIGKILL"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_unmount_reports_timeout_when_lifecycle_lock_stays_active() {
        use std::time::Duration;

        let _guard = unix_test_lock();
        unix_signal_log().lock().unwrap().clear();

        let result = unmount_unix_with(
            "/mnt/studiofs-test",
            12345,
            test_mount_active,
            record_unix_signal,
            Duration::ZERO,
            Duration::ZERO,
        );

        assert!(
            result.is_err(),
            "active lifecycle lock after SIGKILL should be reported as failed unmount"
        );
        assert_eq!(
            *unix_signal_log().lock().unwrap(),
            vec![libc::SIGTERM, 0, libc::SIGKILL],
            "timeout path should probe once, send SIGKILL, then return an error"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_unmount_detaches_mount_before_signaling_mount_process() {
        use std::time::Duration;

        let _guard = unix_test_lock();
        unix_event_log().lock().unwrap().clear();
        unix_signal_log().lock().unwrap().clear();

        let result = unmount_unix_with_detach(
            "/mnt/studiofs-test",
            12345,
            test_mount_not_active,
            record_unix_signal,
            record_unix_detach,
            UnixUnmountTiming {
                detach_timeout: Duration::ZERO,
                timeout: Duration::ZERO,
                poll_interval: Duration::ZERO,
            },
        );

        assert!(
            result.is_ok(),
            "external detach before SIGTERM should complete graceful unmount: {result:?}"
        );
        assert_eq!(
            *unix_event_log().lock().unwrap(),
            vec!["detach", "signal"],
            "FUSE mount must be detached before the mount process receives SIGTERM"
        );
    }

    #[cfg(unix)]
    #[test]
    fn child_reap_timeout_is_bounded() {
        use std::process::Command;
        use std::time::{Duration, Instant};

        let mut child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("start long-lived child");
        let started = Instant::now();

        let reaped = reap_child_with_timeout(&mut child, Duration::ZERO);

        assert!(
            !reaped,
            "running child should not be reaped with zero timeout"
        );
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "bounded reap exceeded its zero-timeout budget"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_unmount_detach_requests_a_forced_native_unmount() {
        use std::sync::Mutex;
        use std::time::Duration;

        type UnmountCall = (String, String, Vec<String>, Duration);

        let call: Mutex<Option<UnmountCall>> = Mutex::new(None);
        let result = detach_unix_mount_with(
            "/Volumes/studiofs/test-volume",
            Duration::from_secs(7),
            |mount_point, program, args, timeout| {
                *call.lock().unwrap() = Some((
                    mount_point.to_owned(),
                    program.to_owned(),
                    args.iter().map(|arg| (*arg).to_owned()).collect(),
                    timeout,
                ));
                Ok(())
            },
        );

        assert!(
            result.is_ok(),
            "forced native unmount should be requested: {result:?}"
        );
        assert_eq!(
            *call.lock().unwrap(),
            Some((
                "/Volumes/studiofs/test-volume".to_owned(),
                "/sbin/umount".to_owned(),
                vec!["-f".to_owned()],
                Duration::from_secs(7),
            ))
        );
    }

    #[cfg(windows)]
    mod windows_tests {
        use super::*;
        use std::os::windows::{
            io::{AsHandle, FromRawHandle, OwnedHandle},
            process::CommandExt,
        };
        use std::process::{Child, Command, Stdio};

        struct TestChild(Child);

        impl Drop for TestChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        #[test]
        fn termination_accepts_a_process_that_exited_at_the_timeout_boundary() {
            let mut child = TestChild(
                Command::new("cmd.exe")
                    .args(["/C", "exit", "0"])
                    .creation_flags(0x08000000)
                    .spawn()
                    .unwrap(),
            );
            assert!(child.0.wait().unwrap().success());
            assert!(terminate_windows_process(child.0.as_handle()).is_ok());
        }

        #[test]
        fn termination_denial_for_a_live_process_is_reported() {
            unsafe extern "system" {
                fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut core::ffi::c_void;
            }
            let mp = format!("umount-{}", uuid::Uuid::new_v4());
            let (mut child, _event) = waiting_child(&mp);
            // SAFETY: Open the live child with SYNCHRONIZE access only; check before ownership.
            let handle = unsafe { OpenProcess(0x00100000, 0, child.0.id()) };
            assert!(!handle.is_null(), "{}", std::io::Error::last_os_error());
            // SAFETY: OpenProcess returned a valid, uniquely owned kernel handle.
            let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
            let error = terminate_windows_process(handle.as_handle()).unwrap_err();
            assert_eq!(error.raw_os_error(), Some(5));
            assert!(child.0.try_wait().unwrap().is_none());
        }

        #[test]
        fn exited_pid_does_not_remove_a_record_whose_lock_is_still_held() {
            let _guard = pid_test_lock();
            let mp = "__test_held_lock_after_exit__";
            *pid_state() = TestPidState {
                value: Some((u32::MAX, mp.into(), "test".into())),
                removed: false,
            };
            let result = run_with(
                mp,
                test_read_pid_file,
                test_remove_pid_file,
                test_process_running,
                test_mount_active,
            );
            assert!(result.is_err());
            assert!(!pid_state().removed);
        }

        fn kernel_object(mount_point: &str, mutex: bool) -> OwnedHandle {
            unsafe extern "system" {
                fn CreateEventW(
                    attrs: *const core::ffi::c_void,
                    manual: i32,
                    initial: i32,
                    name: *const u16,
                ) -> *mut core::ffi::c_void;
                fn CreateMutexW(
                    attrs: *const core::ffi::c_void,
                    owner: i32,
                    name: *const u16,
                ) -> *mut core::ffi::c_void;
            }
            let wide: Vec<u16> = crate::cli::utils::shutdown_event_name(mount_point)
                .encode_utf16()
                .chain(Some(0))
                .collect();
            // SAFETY: The name is NUL-terminated and null attributes select default security.
            let handle = unsafe {
                if mutex {
                    CreateMutexW(std::ptr::null(), 0, wide.as_ptr())
                } else {
                    CreateEventW(std::ptr::null(), 1, 0, wide.as_ptr())
                }
            };
            assert!(!handle.is_null(), "{}", std::io::Error::last_os_error());
            // SAFETY: Creation succeeded and this is the sole owner of the returned handle.
            unsafe { OwnedHandle::from_raw_handle(handle) }
        }

        fn waiting_child(mount_point: &str) -> (TestChild, OwnedHandle) {
            let event = kernel_object(mount_point, false);
            let child = Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command",
                    "$ErrorActionPreference = 'Stop'; try { $event = [System.Threading.EventWaitHandle]::OpenExisting($env:STUDIOFS_TEST_SHUTDOWN_EVENT); if (-not $event.WaitOne(30000)) { exit 2 } } catch { exit 3 }"])
                .env("STUDIOFS_TEST_SHUTDOWN_EVENT", crate::cli::utils::shutdown_event_name(mount_point))
                .creation_flags(0x08000000) // CREATE_NO_WINDOW
                .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
                .spawn().map(TestChild).unwrap();
            (child, event)
        }

        #[test]
        fn graceful_exit_removes_record_after_child_exits() {
            let _guard = pid_test_lock();
            let mp = format!("umount-{}", uuid::Uuid::new_v4());
            let (mut child, _event) = waiting_child(&mp);
            *pid_state() = TestPidState {
                value: Some((child.0.id(), mp.clone(), "test".into())),
                removed: false,
            };
            let result = run_with(
                &mp,
                test_read_pid_file,
                test_remove_pid_file,
                test_process_running,
                |_| {
                    Ok(crate::cli::utils::is_process_running(
                        pid_state().value.as_ref().unwrap().0,
                    ))
                },
            );
            let exited = child.0.try_wait().unwrap();
            assert!(result.is_ok(), "{result:?}");
            assert!(
                exited.unwrap().success(),
                "child must exit through the shutdown event"
            );
            assert!(pid_state().removed);
        }

        #[test]
        fn native_event_error_keeps_record_and_child_running() {
            let _guard = pid_test_lock();
            let mp = format!("umount-{}", uuid::Uuid::new_v4());
            let (mut child, _event) = waiting_child(&format!("{mp}-child"));
            // A different kernel object under the shutdown name makes OpenEventW fail.
            let _mutex = kernel_object(&mp, true);
            *pid_state() = TestPidState {
                value: Some((child.0.id(), mp.clone(), "test".into())),
                removed: false,
            };
            let result = run_with(
                &mp,
                test_read_pid_file,
                test_remove_pid_file,
                test_process_running,
                test_mount_active,
            );
            let still_running = child.0.try_wait().unwrap().is_none();
            let error = result.unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .raw_os_error(),
                Some(6)
            );
            assert!(still_running);
            assert!(!pid_state().removed);
        }

        #[test]
        fn missing_event_forces_termination_and_confirms_exit() {
            let mp = format!("umount-{}", uuid::Uuid::new_v4());
            let (mut child, _event) = waiting_child(&format!("{mp}-child"));
            let result = unmount_windows_with_timeout(&mp, child.0.id(), 100, test_mount_active);
            let exited = child.0.try_wait().unwrap();
            assert!(result.is_ok(), "{result:?}");
            assert_eq!(exited.unwrap().code(), Some(1));
        }

        #[test]
        fn nonexistent_process_is_already_gone() {
            assert!(unmount_windows("__test_gone_umount__", u32::MAX, test_mount_active).is_ok());
        }

        #[test]
        fn released_lock_after_open_leaves_reused_pid_running() {
            let mp = format!("umount-{}", uuid::Uuid::new_v4());
            let (mut child, _event) = waiting_child(&mp);
            let result =
                unmount_windows_with_timeout(&mp, child.0.id(), 100, test_mount_not_active);
            assert!(result.is_ok(), "{result:?}");
            assert!(child.0.try_wait().unwrap().is_none());
        }

        fn test_mount_releases_after_exit(_mount_point: &str) -> Result<bool> {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static CHECKS: AtomicUsize = AtomicUsize::new(0);
            // Held for preflight and the first post-exit check, then released.
            Ok(CHECKS.fetch_add(1, Ordering::SeqCst) < 2)
        }

        #[test]
        fn delayed_lock_release_after_exit_still_removes_record() {
            let _guard = pid_test_lock();
            let mp = "__test_delayed_lock_release__";
            *pid_state() = TestPidState {
                value: Some((u32::MAX, mp.into(), "test".into())),
                removed: false,
            };
            let result = run_with(
                mp,
                test_read_pid_file,
                test_remove_pid_file,
                test_process_running,
                test_mount_releases_after_exit,
            );
            assert!(result.is_ok(), "{result:?}");
            assert!(pid_state().removed);
        }
    }
}
