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
    unmount: impl FnOnce(&str, u32, IsMountActiveFn) -> Result<()>,
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
            "Mount for '{}' is still lock-held, but the recorded PID {} is not running or cannot be accessed. Refusing to terminate an unrelated process. Check process permissions; retry from an elevated terminal if the mount was started elevated.",
            mp,
            pid
        );
    }

    println!(
        "Unmounting volume '{}' from '{}' (PID: {}) …",
        volume, mp, pid
    );

    unmount(mount_point, pid, is_mount_active)?;

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
        unmount_platform,
    )
}

// ─── Windows ─────────────────────────────────────────────────────────────────

#[cfg(windows)]
trait WindowsUnmountApi {
    type Handle;

    fn open_event(&mut self, name: &[u16]) -> std::io::Result<Self::Handle>;
    fn set_event(&mut self, event: &Self::Handle) -> std::io::Result<()>;
    fn open_process(&mut self, pid: u32) -> std::io::Result<Self::Handle>;
    fn wait_for_exit(
        &mut self,
        process: &Self::Handle,
        timeout: std::time::Duration,
    ) -> std::io::Result<bool>;
    fn terminate(&mut self, process: &Self::Handle) -> std::io::Result<()>;
}

#[cfg(windows)]
fn unmount_windows(mount_point: &str, pid: u32) -> Result<()> {
    unmount_windows_with(mount_point, pid, &mut NativeWindowsUnmountApi)
}

#[cfg(windows)]
fn unmount_windows_with(
    mount_point: &str,
    pid: u32,
    api: &mut impl WindowsUnmountApi,
) -> Result<()> {
    use anyhow::Context;
    use std::time::Duration;

    const GRACEFUL_TIMEOUT: Duration = Duration::from_secs(15);
    const ERROR_INVALID_PARAMETER: i32 = 87;
    const ERROR_FILE_NOT_FOUND: i32 = 2;
    let process = match api.open_process(pid) {
        Ok(process) => process,
        Err(error) if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER) => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("could not open mount process {pid}; check process permissions and retry from an elevated terminal if the mount was started elevated")),
    };

    let event_name = super::utils::shutdown_event_name(mount_point);
    let wide: Vec<u16> = event_name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();

    match api.open_event(&wide) {
        Ok(event) => api
            .set_event(&event)
            .context("could not signal mount shutdown event")?,
        Err(error) if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND) => {
            tracing::warn!("could not open shutdown event — will attempt process termination");
        }
        Err(error) => return Err(error).context("could not open shutdown event"),
    }

    if api
        .wait_for_exit(&process, GRACEFUL_TIMEOUT)
        .context("could not wait for mount process")?
    {
        return Ok(());
    }

    tracing::warn!("mount process did not exit within 15 s — force-killing");
    api.terminate(&process)
        .context("could not terminate mount process")?;
    if api
        .wait_for_exit(&process, GRACEFUL_TIMEOUT)
        .context("could not confirm terminated mount process exit")?
    {
        Ok(())
    } else {
        anyhow::bail!(
            "mount process {pid} did not exit within 15 s after termination; leaving mount record intact"
        )
    }
}

#[cfg(windows)]
struct NativeWindowsUnmountApi;

#[cfg(windows)]
impl WindowsUnmountApi for NativeWindowsUnmountApi {
    type Handle = std::os::windows::io::OwnedHandle;

    fn open_event(&mut self, name: &[u16]) -> std::io::Result<Self::Handle> {
        use std::os::windows::io::FromRawHandle;
        const EVENT_MODIFY_STATE: u32 = 0x0002;
        unsafe extern "system" {
            fn OpenEventW(access: u32, inherit: i32, name: *const u16) -> *mut core::ffi::c_void;
        }
        // SAFETY: `name` is NUL-terminated UTF-16 and remains alive for this call.
        let handle = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, name.as_ptr()) };
        if handle.is_null() {
            Err(std::io::Error::last_os_error())
        } else {
            // SAFETY: A successful OpenEventW returns a valid owned kernel handle.
            Ok(unsafe { Self::Handle::from_raw_handle(handle) })
        }
    }

    fn set_event(&mut self, event: &Self::Handle) -> std::io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        unsafe extern "system" {
            fn SetEvent(event: *mut core::ffi::c_void) -> i32;
        }
        // SAFETY: The borrowed handle is open and has event modify access.
        if unsafe { SetEvent(event.as_raw_handle().cast()) } != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn open_process(&mut self, pid: u32) -> std::io::Result<Self::Handle> {
        use std::os::windows::io::FromRawHandle;
        const SYNCHRONIZE: u32 = 0x00100000;
        const PROCESS_TERMINATE: u32 = 0x0001;
        unsafe extern "system" {
            fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut core::ffi::c_void;
        }
        // SAFETY: OpenProcess accepts the requested access mask and process ID.
        let handle = unsafe { OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
        if handle.is_null() {
            Err(std::io::Error::last_os_error())
        } else {
            // SAFETY: A successful OpenProcess returns a valid owned kernel handle.
            Ok(unsafe { Self::Handle::from_raw_handle(handle) })
        }
    }

    fn wait_for_exit(
        &mut self,
        process: &Self::Handle,
        timeout: std::time::Duration,
    ) -> std::io::Result<bool> {
        use std::os::windows::io::AsRawHandle;
        const WAIT_OBJECT_0: u32 = 0;
        const WAIT_TIMEOUT: u32 = 0x00000102;
        unsafe extern "system" {
            fn WaitForSingleObject(handle: *mut core::ffi::c_void, timeout: u32) -> u32;
        }
        let timeout_ms = timeout.as_millis().min(u32::MAX as u128) as u32;
        // SAFETY: The borrowed process handle remains open for the wait.
        match unsafe { WaitForSingleObject(process.as_raw_handle().cast(), timeout_ms) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            u32::MAX => Err(std::io::Error::last_os_error()),
            _ => Err(std::io::Error::other("unexpected process wait result")),
        }
    }

    fn terminate(&mut self, process: &Self::Handle) -> std::io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        unsafe extern "system" {
            fn TerminateProcess(process: *mut core::ffi::c_void, exit_code: u32) -> i32;
        }
        // SAFETY: The borrowed process handle remains open and has terminate access.
        if unsafe { TerminateProcess(process.as_raw_handle().cast(), 1) } != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

#[cfg(windows)]
fn unmount_platform(mount_point: &str, pid: u32, _is_mount_active: IsMountActiveFn) -> Result<()> {
    unmount_windows(mount_point, pid)
}

#[cfg(unix)]
fn unmount_platform(mount_point: &str, pid: u32, is_mount_active: IsMountActiveFn) -> Result<()> {
    unmount_unix(mount_point, pid, is_mount_active)
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
    static TEST_PID_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
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

    fn test_unmount_ok(
        _mount_point: &str,
        _pid: u32,
        _is_mount_active: IsMountActiveFn,
    ) -> Result<()> {
        Ok(())
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
        let _guard = TEST_PID_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            test_unmount_ok,
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

    #[cfg(windows)]
    mod windows_tests {
        use super::*;
        use std::collections::VecDeque;
        use std::io;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        struct FakeHandle {
            name: &'static str,
            dropped: Arc<Mutex<Vec<&'static str>>>,
        }

        impl Drop for FakeHandle {
            fn drop(&mut self) {
                self.dropped.lock().unwrap().push(self.name);
            }
        }

        struct FakeApi {
            open_event: io::Result<()>,
            set_event: io::Result<()>,
            open_process: io::Result<()>,
            waits: VecDeque<io::Result<bool>>,
            terminate: io::Result<()>,
            calls: Vec<&'static str>,
            wait_timeouts: Vec<Duration>,
            dropped: Arc<Mutex<Vec<&'static str>>>,
        }

        impl FakeApi {
            fn new(waits: impl IntoIterator<Item = io::Result<bool>>) -> Self {
                Self {
                    open_event: Ok(()),
                    set_event: Ok(()),
                    open_process: Ok(()),
                    waits: waits.into_iter().collect(),
                    terminate: Ok(()),
                    calls: Vec::new(),
                    wait_timeouts: Vec::new(),
                    dropped: Arc::default(),
                }
            }

            fn handle(&self, name: &'static str) -> FakeHandle {
                FakeHandle {
                    name,
                    dropped: Arc::clone(&self.dropped),
                }
            }
        }

        impl WindowsUnmountApi for FakeApi {
            type Handle = FakeHandle;

            fn open_event(&mut self, _name: &[u16]) -> io::Result<Self::Handle> {
                self.calls.push("open_event");
                self.open_event
                    .as_ref()
                    .map(|()| self.handle("event"))
                    .map_err(|error| {
                        io::Error::from_raw_os_error(error.raw_os_error().unwrap_or(1))
                    })
            }

            fn set_event(&mut self, _event: &Self::Handle) -> io::Result<()> {
                self.calls.push("set_event");
                self.set_event.as_ref().copied().map_err(|error| {
                    io::Error::from_raw_os_error(error.raw_os_error().unwrap_or(1))
                })
            }

            fn open_process(&mut self, _pid: u32) -> io::Result<Self::Handle> {
                self.calls.push("open_process");
                self.open_process
                    .as_ref()
                    .map(|()| self.handle("process"))
                    .map_err(|error| {
                        io::Error::from_raw_os_error(error.raw_os_error().unwrap_or(1))
                    })
            }

            fn wait_for_exit(
                &mut self,
                _process: &Self::Handle,
                timeout: Duration,
            ) -> io::Result<bool> {
                self.calls.push("wait");
                self.wait_timeouts.push(timeout);
                self.waits.pop_front().expect("scripted wait result")
            }

            fn terminate(&mut self, _process: &Self::Handle) -> io::Result<()> {
                self.calls.push("terminate");
                self.terminate.as_ref().copied().map_err(|error| {
                    io::Error::from_raw_os_error(error.raw_os_error().unwrap_or(1))
                })
            }
        }

        #[test]
        fn graceful_exit_is_confirmed_and_handles_are_closed() {
            let mut api = FakeApi::new([Ok(true)]);
            assert!(unmount_windows_with("Z:", 123, &mut api).is_ok());
            assert_eq!(
                api.calls,
                ["open_process", "open_event", "set_event", "wait"]
            );
            assert_eq!(api.wait_timeouts, [Duration::from_secs(15)]);
            assert_eq!(*api.dropped.lock().unwrap(), ["event", "process"]);
        }

        #[test]
        fn missing_event_falls_back_to_termination_and_waits_for_exit() {
            let mut api = FakeApi::new([Ok(false), Ok(true)]);
            api.open_event = Err(io::Error::from_raw_os_error(2));
            assert!(unmount_windows_with("Z:", 123, &mut api).is_ok());
            assert_eq!(
                api.calls,
                ["open_process", "open_event", "wait", "terminate", "wait"]
            );
            assert_eq!(api.wait_timeouts, [Duration::from_secs(15); 2]);
        }

        #[test]
        fn process_open_error_87_means_already_gone_but_access_denied_is_error() {
            let mut gone = FakeApi::new([]);
            gone.open_process = Err(io::Error::from_raw_os_error(87));
            assert!(unmount_windows_with("Z:", 123, &mut gone).is_ok());
            assert_eq!(gone.calls, ["open_process"]);

            let mut denied = FakeApi::new([]);
            denied.open_process = Err(io::Error::from_raw_os_error(5));
            assert!(unmount_windows_with("Z:", 123, &mut denied).is_err());
        }

        #[test]
        fn native_signal_wait_and_termination_errors_are_reported() {
            let mut event_denied = FakeApi::new([]);
            event_denied.open_event = Err(io::Error::from_raw_os_error(5));
            assert!(unmount_windows_with("Z:", 123, &mut event_denied).is_err());

            let mut signal_failed = FakeApi::new([]);
            signal_failed.set_event = Err(io::Error::from_raw_os_error(5));
            assert!(unmount_windows_with("Z:", 123, &mut signal_failed).is_err());

            let mut wait_failed = FakeApi::new([Err(io::Error::from_raw_os_error(6))]);
            assert!(unmount_windows_with("Z:", 123, &mut wait_failed).is_err());

            let mut terminate_failed = FakeApi::new([Ok(false)]);
            terminate_failed.terminate = Err(io::Error::from_raw_os_error(5));
            assert!(unmount_windows_with("Z:", 123, &mut terminate_failed).is_err());

            let mut exit_wait_failed =
                FakeApi::new([Ok(false), Err(io::Error::from_raw_os_error(6))]);
            assert!(unmount_windows_with("Z:", 123, &mut exit_wait_failed).is_err());
        }

        #[test]
        fn timeout_after_termination_is_failure_and_retains_pid_record() {
            let mut api = FakeApi::new([Ok(false), Ok(false)]);
            assert!(unmount_windows_with("Z:", 123, &mut api).is_err());
            assert_eq!(
                api.calls,
                [
                    "open_process",
                    "open_event",
                    "set_event",
                    "wait",
                    "terminate",
                    "wait"
                ]
            );
            assert_eq!(*api.dropped.lock().unwrap(), ["event", "process"]);

            let _guard = TEST_PID_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mp = "__test_windows_failed_umount__";
            let mut denied = FakeApi::new([]);
            denied.open_process = Err(io::Error::from_raw_os_error(5));
            let mut event_denied = FakeApi::new([]);
            event_denied.open_event = Err(io::Error::from_raw_os_error(5));
            let mut signal_failed = FakeApi::new([]);
            signal_failed.set_event = Err(io::Error::from_raw_os_error(5));
            let mut terminate_failed = FakeApi::new([Ok(false)]);
            terminate_failed.terminate = Err(io::Error::from_raw_os_error(5));
            for mut api in [
                denied,
                event_denied,
                signal_failed,
                FakeApi::new([Err(io::Error::from_raw_os_error(6))]),
                terminate_failed,
                FakeApi::new([Ok(false), Err(io::Error::from_raw_os_error(6))]),
                FakeApi::new([Ok(false), Ok(false)]),
            ] {
                {
                    let mut state = pid_state().lock().unwrap();
                    state.value = Some((123, mp.to_owned(), "fakevol".to_owned()));
                    state.removed = false;
                }
                let result = run_with(
                    mp,
                    test_read_pid_file,
                    test_remove_pid_file,
                    test_process_running,
                    test_mount_active,
                    |mp, pid, _| unmount_windows_with(mp, pid, &mut api),
                );
                assert!(result.is_err(), "unexpected success after {:?}", api.calls);
                let state = pid_state().lock().unwrap();
                assert!(!state.removed);
                assert!(state.value.is_some());
            }
        }

        #[test]
        fn confirmed_unmount_removes_pid_record_through_run_with() {
            let _guard = TEST_PID_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mp = "__test_windows_successful_umount__";
            let mut gone = FakeApi::new([]);
            gone.open_process = Err(io::Error::from_raw_os_error(87));
            for mut api in [
                FakeApi::new([Ok(true)]),
                FakeApi::new([Ok(false), Ok(true)]),
                gone,
            ] {
                {
                    let mut state = pid_state().lock().unwrap();
                    state.value = Some((123, mp.to_owned(), "fakevol".to_owned()));
                    state.removed = false;
                }
                let result = run_with(
                    mp,
                    test_read_pid_file,
                    test_remove_pid_file,
                    test_process_running,
                    test_mount_active,
                    |mp, pid, _| unmount_windows_with(mp, pid, &mut api),
                );
                assert!(result.is_ok(), "unmount failed: {result:?}");
                let state = pid_state().lock().unwrap();
                assert!(state.removed);
                assert!(state.value.is_none());
            }
        }
    }
}
