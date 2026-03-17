//! `studiofs umount` — unmount a previously mounted filesystem volume.

use anyhow::Result;

#[cfg(unix)]
use anyhow::anyhow;

type ReadPidFn = fn(&str) -> Result<(u32, String, String)>;
type RemovePidFn = fn(&str) -> Result<()>;
type IsRunningFn = fn(u32) -> bool;

fn run_with(
    mount_point: &str,
    read_pid_file: ReadPidFn,
    remove_pid_file: RemovePidFn,
    is_process_running: IsRunningFn,
) -> Result<()> {
    let (pid, mp, volume) = read_pid_file(mount_point)?;

    // Check the process is actually alive
    if !is_process_running(pid) {
        // Stale PID file — just clean up
        let _ = remove_pid_file(mount_point);
        println!(
            "Mount for '{}' (PID {}) is no longer running. Cleaned up stale state.",
            mp, pid
        );
        return Ok(());
    }

    println!(
        "Unmounting volume '{}' from '{}' (PID: {}) …",
        volume, mp, pid
    );

    #[cfg(windows)]
    unmount_windows(mount_point, pid)?;

    #[cfg(unix)]
    unmount_unix(pid)?;

    // Clean up PID file
    let _ = remove_pid_file(mount_point);
    let _ = std::fs::remove_dir(mount_point);

    println!("✓ Successfully unmounted '{}'", mount_point);
    Ok(())
}

pub fn run(mount_point: &str) -> Result<()> {
    run_with(
        mount_point,
        super::utils::read_pid_file,
        super::utils::remove_pid_file,
        super::utils::is_process_running,
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

    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
    if event.is_null() {
        tracing::warn!("could not open shutdown event — will attempt process termination");
    } else {
        unsafe {
            SetEvent(event);
            CloseHandle(event);
        }
    }

    // 2. Wait for the process to exit (up to 15 s)
    let process = unsafe { OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
    if !process.is_null() {
        let wait_result = unsafe { WaitForSingleObject(process, 15_000) };
        if wait_result == WAIT_TIMEOUT {
            tracing::warn!("mount process did not exit within 15 s — force-killing");
            unsafe {
                TerminateProcess(process, 1);
            }
        }
        unsafe { CloseHandle(process) };
    }

    Ok(())
}

// ─── Unix ────────────────────────────────────────────────────────────────────

#[cfg(unix)]
fn unmount_unix(pid: u32) -> Result<()> {
    use std::time::{Duration, Instant};

    // Send SIGTERM (the FUSE signal-watcher thread will handle it)
    let ret = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(()); // already gone
        }
        return Err(anyhow!("failed to send SIGTERM to PID {}: {}", pid, err));
    }

    // Poll until the process exits, hard-kill after 15 s
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        std::thread::sleep(Duration::from_millis(200));
        if unsafe { libc::kill(pid as i32, 0) } != 0 {
            break; // gone
        }
        if Instant::now() > deadline {
            tracing::warn!("mount process did not exit within 15 s — sending SIGKILL");
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
            break;
        }
    }

    Ok(())
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

    fn test_process_not_running(_pid: u32) -> bool {
        false
    }

    /// `umount` on a mount point with no PID file should return an error.
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

    /// If a PID file exists but the process is dead, `umount` should clean it up.
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
            test_process_not_running,
        );
        assert!(
            result.is_ok(),
            "umount of stale PID should succeed: {result:?}"
        );

        let state = pid_state().lock().unwrap();
        assert!(state.removed, "PID file should have been removed");
        assert!(state.value.is_none(), "PID state should be cleared");
    }
}
