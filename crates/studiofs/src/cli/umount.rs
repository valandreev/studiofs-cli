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
        let _ = remove_pid_file(mount_point);
        println!(
            "Mount for '{}' has no active lifecycle lock. Cleaned up stale state (recorded PID {}).",
            mp, pid
        );
        return Ok(());
    }

    if !is_process_running(pid) {
        anyhow::bail!(
            "Mount for '{}' is still lock-held, but the recorded PID {} is not running. Refusing to terminate an unrelated process.",
            mp,
            pid
        );
    }

    println!(
        "Unmounting volume '{}' from '{}' (PID: {}) …",
        volume, mp, pid
    );

    #[cfg(windows)]
    unmount_windows(mount_point, pid)?;

    #[cfg(unix)]
    unmount_unix(mount_point, pid, is_mount_active)?;

    // Clean up the persisted mount record.
    let _ = remove_pid_file(mount_point);

    println!("✓ Successfully unmounted '{}'", mount_point);
    Ok(())
}

pub fn run(mount_point: &str) -> Result<()> {
    let mount_point = super::utils::normalize_mount_point(mount_point);
    run_with(
        &mount_point,
        super::utils::read_pid_file,
        super::utils::remove_pid_file,
        super::utils::is_process_running,
        super::utils::mount_is_active,
    )
}

// ─── Windows ─────────────────────────────────────────────────────────────────

#[cfg(windows)]
fn unmount_windows(mount_point: &str, pid: u32) -> Result<()> {
    unsafe extern "system" {
        fn OpenEventW(
            dwDesiredAccess: u32,
            bInheritHandle: i32,
            lpName: *const u16,
        ) -> *mut core::ffi::c_void;
        fn SetEvent(hEvent: *mut core::ffi::c_void) -> i32;
        fn OpenProcess(
            dwDesiredAccess: u32,
            bInheritHandle: i32,
            dwProcessId: u32,
        ) -> *mut core::ffi::c_void;
        fn WaitForSingleObject(hHandle: *mut core::ffi::c_void, dwMilliseconds: u32) -> u32;
        fn TerminateProcess(hProcess: *mut core::ffi::c_void, uExitCode: u32) -> i32;
        fn CloseHandle(hObject: *mut core::ffi::c_void) -> i32;
    }

    const EVENT_MODIFY_STATE: u32 = 0x0002;
    const SYNCHRONIZE: u32 = 0x00100000;
    const PROCESS_TERMINATE: u32 = 0x0001;
    const WAIT_TIMEOUT: u32 = 0x00000102;

    // 1. Signal the named event for graceful shutdown
    let event_name = super::utils::shutdown_event_name(mount_point);
    let wide: Vec<u16> = event_name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();

    // SAFETY: The wide name comes from UTF-16 encoding of a Rust `String` and remains valid for the
    // duration of this call.
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
    if event.is_null() {
        tracing::warn!("could not open shutdown event — will attempt process termination");
    } else {
        // SAFETY: We only call `SetEvent` and `CloseHandle` on a non-null handle returned by
        // `OpenEventW`; `CloseHandle` is required to avoid leaking the handle.
        unsafe {
            SetEvent(event);
            CloseHandle(event);
        }
    }

    // 2. Wait for the process to exit (up to 15 s)
    // SAFETY: Opening a process by PID is an OS operation; null handle is handled explicitly.
    let process = unsafe { OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
    if !process.is_null() {
        // SAFETY: `process` is valid by the null check above and remains valid for this blocking call.
        let wait_result = unsafe { WaitForSingleObject(process, 15_000) };
        if wait_result == WAIT_TIMEOUT {
            tracing::warn!("mount process did not exit within 15 s — force-killing");
            // SAFETY: `process` is valid and still live until we close it after this block.
            unsafe {
                TerminateProcess(process, 1);
            }
        }
        // SAFETY: `process` was returned by `OpenProcess`; closing it is the required cleanup action.
        unsafe { CloseHandle(process) };
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
    #[cfg(unix)]
    static TEST_UNIX_SIGNALS: OnceLock<Mutex<Vec<libc::c_int>>> = OnceLock::new();
    #[cfg(unix)]
    static TEST_UNIX_EVENTS: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();
    #[cfg(unix)]
    static TEST_UNIX_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn pid_state() -> &'static Mutex<TestPidState> {
        TEST_PID_STATE.get_or_init(|| Mutex::new(TestPidState::default()))
    }

    fn test_read_pid_file(mount_point: &str) -> Result<(u32, String, String)> {
        let state = pid_state().lock().unwrap();
        state
            .value
            .clone()
            .ok_or_else(|| anyhow!("No mount found for '{mount_point}'. Is it mounted?"))
    }

    fn test_remove_pid_file(_mount_point: &str) -> Result<()> {
        let mut state = pid_state().lock().unwrap();
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

    #[cfg(unix)]
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
        let mp = "__test_stale_umount__";
        {
            let mut state = pid_state().lock().unwrap();
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

        let state = pid_state().lock().unwrap();
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
}
