//! `studiofs umount` — unmount a previously mounted filesystem volume.

use anyhow::Result;

#[cfg(unix)]
use anyhow::anyhow;

pub fn run(mount_point: &str) -> Result<()> {
    let (pid, mp, volume) = super::utils::read_pid_file(mount_point)?;

    // Check the process is actually alive
    if !super::utils::is_process_running(pid) {
        // Stale PID file — just clean up
        let _ = super::utils::remove_pid_file(mount_point);
        println!("Mount for '{}' (PID {}) is no longer running. Cleaned up stale state.", mp, pid);
        return Ok(());
    }

    println!("Unmounting volume '{}' from '{}' (PID: {}) …", volume, mp, pid);

    #[cfg(windows)]
    unmount_windows(mount_point, pid)?;

    #[cfg(unix)]
    unmount_unix(pid)?;

    // Clean up PID file
    let _ = super::utils::remove_pid_file(mount_point);

    println!("✓ Successfully unmounted '{}'", mount_point);
    Ok(())
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
    let wide: Vec<u16> = event_name.encode_utf16().chain(std::iter::once(0)).collect();

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
    let process =
        unsafe { OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
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
        // Write a PID file for a PID that is almost certainly dead.
        let mp = "__test_stale_umount__";
        let _ = super::super::utils::write_pid_file(mp, "fakevol", 0xFFFF_FFFE);

        // Should succeed (prints "no longer running" and cleans up).
        let result = run(mp);
        assert!(result.is_ok(), "umount of stale PID should succeed: {result:?}");

        // PID file should be gone.
        assert!(
            super::super::utils::read_pid_file(mp).is_err(),
            "PID file should have been removed"
        );
    }
}