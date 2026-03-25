use anyhow::{Result, anyhow};
use std::path::PathBuf;

pub fn get_default_meta_dir(volume_name: &str) -> Result<PathBuf> {
    let home = home::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    Ok(home.join(".studiofs").join("meta").join(volume_name))
}

// ─── Mount / PID management ─────────────────────────────────────────────────

/// Turn a mount-point path into a filesystem-safe identifier.
/// `Z:` → `z`, `/mnt/studio` → `mnt_studio`.
pub fn sanitize_mount_point(mp: &str) -> String {
    let s = mp
        .replace(['\\', '/', ':'], "_")
        .trim_matches('_')
        .to_lowercase();
    if s.is_empty() { "root".to_string() } else { s }
}

#[cfg(windows)]
pub fn normalize_mount_point(mp: &str) -> String {
    const PREFIX: &str = r"\\?\";
    let without_prefix = mp.strip_prefix(PREFIX).unwrap_or(mp);
    if let Some(unc_tail) = without_prefix.strip_prefix("UNC\\") {
        return format!(r"\\{unc_tail}");
    }
    without_prefix.to_string()
}

#[cfg(not(windows))]
pub fn normalize_mount_point(mp: &str) -> String {
    mp.to_string()
}

/// `~/.studiofs/mounts/`
pub fn get_mounts_dir() -> Result<PathBuf> {
    let home = home::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    let dir = home.join(".studiofs").join("mounts");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// `~/.studiofs/logs/`
pub fn get_logs_dir() -> Result<PathBuf> {
    let home = home::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    let dir = home.join(".studiofs").join("logs");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Full path to the PID file for a given mount point.
pub fn pid_file_path(mount_point: &str) -> Result<PathBuf> {
    let dir = get_mounts_dir()?;
    let sanitized = sanitize_mount_point(&normalize_mount_point(mount_point));
    Ok(dir.join(format!("{sanitized}.json")))
}

/// Write a JSON PID file for a mount.
pub fn write_pid_file(mount_point: &str, volume: &str, pid: u32) -> Result<()> {
    let path = pid_file_path(mount_point)?;
    let info = serde_json::json!({
        "pid": pid,
        "mount_point": mount_point,
        "volume": volume,
    });
    std::fs::write(&path, serde_json::to_string_pretty(&info)?)?;
    Ok(())
}

/// Read PID file → `(pid, mount_point, volume)`.
pub fn read_pid_file(mount_point: &str) -> Result<(u32, String, String)> {
    let normalized = normalize_mount_point(mount_point);
    let path = pid_file_path(&normalized)?;
    let data = std::fs::read_to_string(&path)
        .map_err(|_| anyhow!("No mount found for '{mount_point}'. Is it mounted?"))?;
    let v: serde_json::Value = serde_json::from_str(&data)?;
    let pid = v["pid"]
        .as_u64()
        .ok_or_else(|| anyhow!("corrupt PID file"))? as u32;
    let mp = v["mount_point"].as_str().unwrap_or(&normalized).to_string();
    let vol = v["volume"].as_str().unwrap_or("unknown").to_string();
    Ok((pid, mp, vol))
}

/// Remove PID file if it exists.
pub fn remove_pid_file(mount_point: &str) -> Result<()> {
    let path = pid_file_path(&normalize_mount_point(mount_point))?;
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

#[cfg(windows)]
pub fn is_drive_root(path: &str) -> bool {
    let bytes = path.as_bytes();
    if bytes.len() < 2 {
        return false;
    }

    if !(bytes[0].is_ascii_alphabetic() && bytes[1] == b':') {
        return false;
    }

    if bytes.len() == 2 {
        return true;
    }

    if bytes.len() == 3 && (bytes[2] == b'\\' || bytes[2] == b'/') {
        return true;
    }

    false
}

/// Named-event identifier used on Windows for graceful external shutdown.
#[cfg(any(windows, test))]
pub fn shutdown_event_name(mount_point: &str) -> String {
    let sanitized = sanitize_mount_point(&normalize_mount_point(mount_point));
    format!("Local\\StudioFS_{sanitized}")
}

/// Check whether a process with the given PID is still alive.
pub fn is_process_running(pid: u32) -> bool {
    #[cfg(windows)]
    {
        unsafe extern "system" {
            fn OpenProcess(
                dwDesiredAccess: u32,
                bInheritHandle: i32,
                dwProcessId: u32,
            ) -> *mut core::ffi::c_void;
            fn CloseHandle(hObject: *mut core::ffi::c_void) -> i32;
        }
        const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            false
        } else {
            unsafe { CloseHandle(handle) };
            true
        }
    }

    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
}

// ─── PID-file helpers that accept an explicit directory ──────────────────────
// These variants allow unit tests to use a temp directory instead of ~/.studiofs.

/// Write a PID file into the given directory.
#[cfg(test)]
pub fn write_pid_file_in(
    dir: &std::path::Path,
    mount_point: &str,
    volume: &str,
    pid: u32,
) -> Result<()> {
    let sanitized = sanitize_mount_point(mount_point);
    let path = dir.join(format!("{sanitized}.json"));
    let info = serde_json::json!({
        "pid": pid,
        "mount_point": mount_point,
        "volume": volume,
    });
    std::fs::write(&path, serde_json::to_string_pretty(&info)?)?;
    Ok(())
}

/// Read a PID file from the given directory.
#[cfg(test)]
pub fn read_pid_file_in(dir: &std::path::Path, mount_point: &str) -> Result<(u32, String, String)> {
    let sanitized = sanitize_mount_point(&normalize_mount_point(mount_point));
    let path = dir.join(format!("{sanitized}.json"));
    let data = std::fs::read_to_string(&path)
        .map_err(|_| anyhow!("No mount found for '{mount_point}'."))?;
    let v: serde_json::Value = serde_json::from_str(&data)?;
    let pid = v["pid"]
        .as_u64()
        .ok_or_else(|| anyhow!("corrupt PID file"))? as u32;
    let mp = v["mount_point"].as_str().unwrap_or(mount_point).to_string();
    let vol = v["volume"].as_str().unwrap_or("unknown").to_string();
    Ok((pid, mp, vol))
}

/// Remove PID file from the given directory.
#[cfg(test)]
pub fn remove_pid_file_in(dir: &std::path::Path, mount_point: &str) -> Result<()> {
    let sanitized = sanitize_mount_point(mount_point);
    let path = dir.join(format!("{sanitized}.json"));
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

/// Format a suggested cleanup command for the user based on the current OS.
pub fn format_cleanup_command(name: &str) -> String {
    #[cfg(windows)]
    {
        format!("studiofs.exe destroy {name} --local")
    }
    #[cfg(not(windows))]
    {
        format!("studiofs destroy {name} --local")
    }
}

// ─── Byte formatting utilities ───────────────────────────────────────────────

const BINARY_UNIT_DIVISOR: f64 = 1024.0;

/// Format a binary value (bytes) with appropriate unit (B, KiB, MiB, GiB, etc.)
fn format_binary_value(mut value: f64) -> (f64, &'static str) {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut unit = units[0];
    for next_unit in units.iter().skip(1) {
        if value < BINARY_UNIT_DIVISOR {
            break;
        }
        value /= BINARY_UNIT_DIVISOR;
        unit = next_unit;
    }
    (value, unit)
}

/// Format bytes as a human-readable string without space between number and unit.
/// Examples: "512B", "1.5KiB", "2.0MiB", "1.0GiB"
pub fn format_bytes(bytes: u64) -> String {
    let (value, unit) = format_binary_value(bytes as f64);
    if unit == "B" {
        format!("{value:.0}{unit}")
    } else {
        format!("{value:.1}{unit}")
    }
}

/// Format throughput (bytes per second) as a human-readable string.
/// Examples: "512B/s", "1.5KiB/s", "2.0MiB/s"
pub fn format_throughput(bytes_per_sec: f64) -> String {
    let (value, unit) = format_binary_value(bytes_per_sec.max(0.0));
    if unit == "B" {
        format!("{value:.0}{unit}/s")
    } else {
        format!("{value:.2}{unit}/s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── sanitize_mount_point ─────────────────────────────────────────────

    #[test]
    fn sanitize_windows_drive_letter() {
        assert_eq!(sanitize_mount_point("Z:"), "z");
    }

    #[test]
    fn sanitize_windows_drive_letter_with_backslash() {
        assert_eq!(sanitize_mount_point("Z:\\"), "z");
    }

    #[test]
    fn sanitize_unix_absolute_path() {
        assert_eq!(sanitize_mount_point("/mnt/studio"), "mnt_studio");
    }

    #[test]
    fn sanitize_unix_nested_path() {
        assert_eq!(
            sanitize_mount_point("/home/user/mount/data"),
            "home_user_mount_data"
        );
    }

    #[test]
    fn sanitize_empty_string() {
        assert_eq!(sanitize_mount_point(""), "root");
    }

    #[test]
    fn sanitize_just_separators() {
        assert_eq!(sanitize_mount_point("///"), "root");
    }

    #[cfg(windows)]
    #[test]
    fn normalize_mount_point_removes_nt_extended_prefix() {
        assert_eq!(
            normalize_mount_point(r"\\?\C:\Users\valen\studiofs\test"),
            r"C:\Users\valen\studiofs\test"
        );
    }

    #[cfg(windows)]
    #[test]
    fn normalize_mount_point_unc_extended_prefix() {
        assert_eq!(
            normalize_mount_point(r"\\?\UNC\server\share\vol"),
            r"\\server\share\vol"
        );
    }

    // ── shutdown_event_name ──────────────────────────────────────────────

    #[test]
    fn event_name_for_drive_letter() {
        let name = shutdown_event_name("Z:");
        assert_eq!(name, "Local\\StudioFS_z");
    }

    #[test]
    fn event_name_for_unix_path() {
        let name = shutdown_event_name("/mnt/studio");
        assert_eq!(name, "Local\\StudioFS_mnt_studio");
    }

    // ── PID file round-trip (using temp dir) ─────────────────────────────

    #[test]
    fn pid_file_write_read_remove() {
        let tmp = tempfile::tempdir().unwrap();
        let mp = "T:";
        write_pid_file_in(tmp.path(), mp, "testvol", 12345).unwrap();

        let (pid, read_mp, vol) = read_pid_file_in(tmp.path(), mp).unwrap();
        assert_eq!(pid, 12345);
        assert_eq!(read_mp, "T:");
        assert_eq!(vol, "testvol");

        remove_pid_file_in(tmp.path(), mp).unwrap();
        assert!(read_pid_file_in(tmp.path(), mp).is_err());
    }

    #[test]
    fn pid_file_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let mp = "/mnt/x";
        write_pid_file_in(tmp.path(), mp, "vol1", 100).unwrap();
        write_pid_file_in(tmp.path(), mp, "vol2", 200).unwrap();

        let (pid, _, vol) = read_pid_file_in(tmp.path(), mp).unwrap();
        assert_eq!(pid, 200);
        assert_eq!(vol, "vol2");
    }

    #[test]
    fn read_missing_pid_file_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let result = read_pid_file_in(tmp.path(), "X:");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("No mount found"), "unexpected error: {msg}");
    }

    #[test]
    fn remove_nonexistent_pid_file_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        // Should not error even though no file exists
        remove_pid_file_in(tmp.path(), "Q:").unwrap();
    }

    // ── is_process_running ───────────────────────────────────────────────

    #[test]
    fn current_process_is_running() {
        assert!(is_process_running(std::process::id()));
    }

    #[test]
    fn bogus_pid_is_not_running() {
        // PID 0xFFFF_FFFE is extremely unlikely to be a real process.
        assert!(!is_process_running(0xFFFF_FFFE));
    }
}
