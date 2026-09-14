use anyhow::{Context, Result, anyhow};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

pub(crate) const CLIENT_ROOT_ENV: &str = "STUDIOFS_CLIENT_ROOT";

pub(crate) fn get_client_root() -> Result<PathBuf> {
    get_client_root_with(|key| std::env::var_os(key))
}

fn get_client_root_with<F>(read_var: F) -> Result<PathBuf>
where
    F: FnOnce(&str) -> Option<OsString>,
{
    get_client_root_with_home(read_var, home::home_dir)
}

fn get_client_root_with_home<F, H>(read_var: F, home_dir: H) -> Result<PathBuf>
where
    F: FnOnce(&str) -> Option<OsString>,
    H: FnOnce() -> Option<PathBuf>,
{
    if let Some(root) = read_var(CLIENT_ROOT_ENV) {
        return normalize_client_root(PathBuf::from(root));
    }

    let home = home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    let root = home.join(".studiofs");
    std::path::absolute(&root)
        .with_context(|| format!("resolve default client root '{}'", root.display()))
}

fn normalize_client_root(root: PathBuf) -> Result<PathBuf> {
    if root.as_os_str().is_empty() {
        return Err(anyhow!("{CLIENT_ROOT_ENV} must not be empty when set"));
    }

    std::path::absolute(&root)
        .with_context(|| format!("resolve {CLIENT_ROOT_ENV} '{}'", root.display()))
}

pub(crate) fn default_meta_dir_in_client_root(client_root: &Path, volume_name: &str) -> PathBuf {
    client_root.join("meta").join(volume_name)
}

fn mounts_dir_in_client_root(client_root: &Path) -> PathBuf {
    client_root.join("mounts")
}

fn logs_dir_in_client_root(client_root: &Path) -> PathBuf {
    client_root.join("logs")
}

fn mount_record_path_in_mounts_dir(
    mounts_dir: &Path,
    mount_point: &str,
    extension: &str,
) -> PathBuf {
    let sanitized = sanitize_mount_point(&normalize_mount_point(mount_point));
    mounts_dir.join(format!("{sanitized}.{extension}"))
}

pub fn get_default_meta_dir(volume_name: &str) -> Result<PathBuf> {
    Ok(default_meta_dir_in_client_root(
        &get_client_root()?,
        volume_name,
    ))
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

/// `<client-root>/mounts/`, defaulting to `~/.studiofs/mounts/`.
pub fn get_mounts_dir() -> Result<PathBuf> {
    let dir = mounts_dir_in_client_root(&get_client_root()?);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// `<client-root>/logs/`, defaulting to `~/.studiofs/logs/`.
pub fn get_logs_dir() -> Result<PathBuf> {
    let dir = logs_dir_in_client_root(&get_client_root()?);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

const MOUNT_RECORD_VERSION: u32 = 2;

fn default_mount_record_version() -> u32 {
    MOUNT_RECORD_VERSION
}

/// Lifecycle state persisted in a mount record.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MountRecordStatus {
    /// The child owns the mount lock but native attach has not completed.
    #[default]
    Starting,
    /// The native mount is attached while metadata synchronization may continue.
    Mounted,
    /// The native mount and metadata synchronization are live.
    Live,
    /// The native mount remains usable with degraded synchronization.
    Degraded,
    /// Mount preparation or serving failed.
    Failed,
}

impl std::fmt::Display for MountRecordStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Starting => "starting",
            Self::Mounted => "mounted",
            Self::Live => "live",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
        })
    }
}

/// JSON state shared by a foreground mount child and detached parent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MountRecordInfo {
    #[serde(default = "default_mount_record_version")]
    pub record_version: u32,
    pub pid: u32,
    #[serde(default)]
    pub mount_point: String,
    #[serde(default)]
    pub volume: String,
    #[serde(default)]
    pub cache_path: Option<String>,
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Local/native lifecycle state used by the detached handshake.
    #[serde(default)]
    pub status: MountRecordStatus,
    /// PostgreSQL synchronization lifecycle, when known.
    #[serde(default)]
    pub sync_lifecycle: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MountRecordUpdateError {
    #[error("mount record instance changed for '{mount_point}'")]
    InstanceChanged { mount_point: String },
    #[error("mount record not found for '{mount_point}'")]
    NotFound { mount_point: String },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub(crate) struct MountLockGuard {
    lock_file: File,
    instance_id: String,
}

impl MountLockGuard {
    pub(crate) fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub(crate) fn persist_record_state(
        &self,
        mount_point: &str,
        volume: &str,
        pid: u32,
        cache_path: Option<&Path>,
        status: MountRecordStatus,
        sync_lifecycle: Option<&str>,
    ) -> Result<()> {
        write_mount_record_with_cache_and_instance(
            mount_point,
            volume,
            pid,
            cache_path,
            Some(self.instance_id()),
            status,
            sync_lifecycle,
        )
    }
}

impl Drop for MountLockGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
    }
}

/// Full path to the JSON mount record for a given mount point.
pub fn pid_file_path(mount_point: &str) -> Result<PathBuf> {
    let dir = get_mounts_dir()?;
    Ok(mount_record_path_in_mounts_dir(&dir, mount_point, "json"))
}

/// Full path to the lifecycle lock file for a given mount point.
pub(crate) fn mount_lock_file_path(mount_point: &str) -> Result<PathBuf> {
    let dir = get_mounts_dir()?;
    Ok(mount_record_path_in_mounts_dir(&dir, mount_point, "lock"))
}

fn open_mount_lock_file(mount_point: &str) -> Result<File> {
    let path = mount_lock_file_path(mount_point)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(Into::into)
}

fn try_acquire_mount_lock_file(lock_file: File) -> Result<Option<File>> {
    match lock_file.try_lock_exclusive() {
        Ok(()) => Ok(Some(lock_file)),
        Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn try_acquire_mount_lock(mount_point: &str) -> Result<Option<MountLockGuard>> {
    let lock_file = open_mount_lock_file(mount_point)?;
    Ok(
        try_acquire_mount_lock_file(lock_file)?.map(|lock_file| MountLockGuard {
            lock_file,
            instance_id: Uuid::new_v4().to_string(),
        }),
    )
}

pub(crate) fn mount_is_active(mount_point: &str) -> Result<bool> {
    let Some(lock_guard) = try_acquire_mount_lock(mount_point)? else {
        return Ok(true);
    };
    drop(lock_guard);
    Ok(false)
}

fn write_mount_record_with_cache_and_instance(
    mount_point: &str,
    volume: &str,
    pid: u32,
    cache_path: Option<&Path>,
    instance_id: Option<&str>,
    status: MountRecordStatus,
    sync_lifecycle: Option<&str>,
) -> Result<()> {
    let path = pid_file_path(mount_point)?;
    let record = MountRecordInfo {
        record_version: MOUNT_RECORD_VERSION,
        pid,
        mount_point: mount_point.to_string(),
        volume: volume.to_string(),
        cache_path: cache_path.map(|path| path.to_string_lossy().into_owned()),
        instance_id: instance_id.map(str::to_string),
        status,
        sync_lifecycle: sync_lifecycle.map(str::to_string),
    };
    write_mount_record_file(&path, &record)
}

fn write_mount_record_file(path: &Path, record: &MountRecordInfo) -> Result<()> {
    let parent = path
        .parent()
        .context("mount record path has no parent directory")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary mount record near '{}'", path.display()))?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), record)?;
    temporary.as_file_mut().flush()?;
    temporary.as_file().sync_all()?;
    let mut temporary = Some(temporary);
    for attempt in 0..5 {
        let file = temporary
            .take()
            .ok_or_else(|| anyhow!("mount record replacement file was lost"))?;
        match file.persist(path) {
            Ok(_) => return Ok(()),
            Err(error)
                if attempt + 1 < 5 && is_retryable_mount_record_replace_error(&error.error) =>
            {
                temporary = Some(error.file);
                let delay_ms = [10, 25, 50, 100][attempt];
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            Err(error) => {
                return Err(anyhow!(
                    "replace mount record '{}': {}",
                    path.display(),
                    error.error
                ));
            }
        }
    }
    Err(anyhow!("mount record replacement attempts exhausted"))
}

fn is_retryable_mount_record_replace_error(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        matches!(
            error.kind(),
            io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
        ) || matches!(error.raw_os_error(), Some(5 | 32))
    }
    #[cfg(not(windows))]
    {
        let _ = error;
        false
    }
}

pub(crate) fn update_mount_record_status(
    mount_point: &str,
    expected_instance_id: &str,
    status: MountRecordStatus,
    sync_lifecycle: Option<&str>,
) -> std::result::Result<(), MountRecordUpdateError> {
    let path = pid_file_path(mount_point).map_err(MountRecordUpdateError::Other)?;
    update_mount_record_status_at(
        &path,
        mount_point,
        expected_instance_id,
        status,
        sync_lifecycle,
    )
}

fn update_mount_record_status_at(
    path: &Path,
    mount_point: &str,
    expected_instance_id: &str,
    status: MountRecordStatus,
    sync_lifecycle: Option<&str>,
) -> std::result::Result<(), MountRecordUpdateError> {
    let mut record = read_mount_record_for_update(path, mount_point)?;
    if record.instance_id.as_deref() != Some(expected_instance_id) {
        return Err(MountRecordUpdateError::InstanceChanged {
            mount_point: mount_point.to_string(),
        });
    }
    record.status = status;
    record.sync_lifecycle = sync_lifecycle.map(str::to_string);
    write_mount_record_file(path, &record).map_err(MountRecordUpdateError::Other)
}

pub(crate) fn active_mount_record_for_volume(volume: &str) -> Result<Option<MountRecordInfo>> {
    let mounts_dir = get_mounts_dir()?;
    for entry in std::fs::read_dir(mounts_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let data = match std::fs::read(entry.path()) {
            Ok(data) => data,
            Err(_) => continue,
        };
        let record = match serde_json::from_slice::<MountRecordInfo>(&data) {
            Ok(record) if record.volume == volume => record,
            _ => continue,
        };
        if mount_record_is_active_candidate(&record) && mount_is_active(&record.mount_point)? {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn mount_record_is_active_candidate(record: &MountRecordInfo) -> bool {
    matches!(
        record.status,
        MountRecordStatus::Mounted | MountRecordStatus::Live | MountRecordStatus::Degraded
    ) || (record.instance_id.is_none() && record.status == MountRecordStatus::Starting)
}

/// Read PID file → `(pid, mount_point, volume)`.
pub fn read_pid_file(mount_point: &str) -> Result<(u32, String, String)> {
    let record = read_mount_record(mount_point)?;
    Ok((record.pid, record.mount_point, record.volume))
}

pub(crate) fn read_mount_record(mount_point: &str) -> Result<MountRecordInfo> {
    let normalized = normalize_mount_point(mount_point);
    let path = pid_file_path(&normalized)?;
    let data = std::fs::read_to_string(&path)
        .map_err(|_| anyhow!("No mount found for '{mount_point}'. Is it mounted?"))?;
    parse_mount_record(&data, &normalized)
}

fn read_mount_record_for_update(
    path: &Path,
    mount_point: &str,
) -> std::result::Result<MountRecordInfo, MountRecordUpdateError> {
    let data = std::fs::read_to_string(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            MountRecordUpdateError::NotFound {
                mount_point: mount_point.to_string(),
            }
        } else {
            MountRecordUpdateError::Other(
                anyhow::Error::new(error)
                    .context(format!("read mount record '{}'", path.display())),
            )
        }
    })?;
    parse_mount_record(&data, &normalize_mount_point(mount_point))
        .map_err(MountRecordUpdateError::Other)
}

fn parse_mount_record(data: &str, normalized_mount_point: &str) -> Result<MountRecordInfo> {
    let mut record: MountRecordInfo =
        serde_json::from_str(data).context("corrupt mount record PID file")?;
    if record.mount_point.is_empty() {
        record.mount_point = normalized_mount_point.to_string();
    }
    if record.volume.is_empty() {
        record.volume = "unknown".to_string();
    }
    Ok(record)
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
        // SAFETY: `OpenProcess` returns a nullable handle for the PID; null means process unavailable.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            false
        } else {
            // SAFETY: `handle` is non-null and must be closed to release OS resources.
            unsafe { CloseHandle(handle) };
            true
        }
    }

    #[cfg(unix)]
    {
        // SAFETY: signal 0 is a portable process existence probe with no delivery side effects.
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
}

// ─── PID-file helpers that accept an explicit directory ──────────────────────
// These variants allow unit tests to use a temp directory instead of the client root.

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
    let record = MountRecordInfo {
        record_version: MOUNT_RECORD_VERSION,
        pid,
        mount_point: mount_point.to_string(),
        volume: volume.to_string(),
        cache_path: None,
        instance_id: None,
        status: MountRecordStatus::Starting,
        sync_lifecycle: None,
    };
    write_mount_record_file(&path, &record)
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
    use std::ffi::OsString;

    #[test]
    fn client_root_absent_preserves_home_studiofs_metadata_layout() {
        let home = home::home_dir().expect("home dir");
        let expected_root =
            std::path::absolute(home.join(".studiofs")).expect("absolute default client root");

        let root = get_client_root_with(|_| None).expect("default client root");
        let meta_dir = default_meta_dir_in_client_root(&root, "projectfs");

        assert_eq!(root, expected_root);
        assert_eq!(meta_dir, expected_root.join("meta").join("projectfs"));
    }

    #[test]
    fn client_root_absent_normalizes_default_home_to_absolute_path() {
        let relative_home = std::path::PathBuf::from("relative-home");
        let root = get_client_root_with_home(|_| None, || Some(relative_home.clone()))
            .expect("default client root");

        assert!(root.is_absolute());
        assert_eq!(
            root,
            std::path::absolute(relative_home.join(".studiofs")).expect("absolute path")
        );
    }

    #[test]
    fn client_root_env_scopes_metadata_mounts_and_logs() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let configured_root = temp.path().join("client-a");
        let root = get_client_root_with(|_| Some(configured_root.clone().into_os_string()))
            .expect("client root");
        let mounts_dir = mounts_dir_in_client_root(&root);

        assert_eq!(
            default_meta_dir_in_client_root(&root, "projectfs"),
            root.join("meta").join("projectfs")
        );
        assert_eq!(mounts_dir, root.join("mounts"));
        assert_eq!(logs_dir_in_client_root(&root), root.join("logs"));
        assert_eq!(
            mount_record_path_in_mounts_dir(&mounts_dir, "Z:", "json"),
            root.join("mounts").join("z.json")
        );
        assert_eq!(
            mount_record_path_in_mounts_dir(&mounts_dir, "Z:", "lock"),
            root.join("mounts").join("z.lock")
        );
    }

    #[test]
    fn client_root_env_normalizes_relative_value_to_absolute_path() {
        let root = get_client_root_with(|_| Some(OsString::from("relative-client-root")))
            .expect("client root");

        assert!(root.is_absolute());
        assert_eq!(
            root,
            std::path::absolute("relative-client-root").expect("absolute path")
        );
    }

    #[test]
    fn client_root_env_rejects_empty_value() {
        let error = get_client_root_with(|_| Some(OsString::new()))
            .expect_err("empty client root rejected");

        assert!(error.to_string().contains(CLIENT_ROOT_ENV));
    }

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
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let mp = "/mnt/x";
        write_pid_file_in(tmp.path(), mp, "vol1", 100).unwrap();
        write_pid_file_in(tmp.path(), mp, "vol2", 200).unwrap();

        let (pid, _, vol) = read_pid_file_in(tmp.path(), mp).unwrap();
        assert_eq!(pid, 200);
        assert_eq!(vol, "vol2");
    }

    #[test]
    fn mount_record_lifecycle_fields_are_backward_compatible() {
        let record: MountRecordInfo = serde_json::from_str(
            r#"{
                "record_version": 2,
                "pid": 123,
                "mount_point": "T:",
                "volume": "demo",
                "cache_path": null,
                "instance_id": null
            }"#,
        )
        .expect("legacy mount record should deserialize");

        assert_eq!(record.status, MountRecordStatus::Starting);
        assert_eq!(record.sync_lifecycle, None);
    }

    #[test]
    fn mount_record_status_update_reports_instance_change_without_rereading() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let path = tmp.path().join("mount.json");
        write_mount_record_file(
            &path,
            &MountRecordInfo {
                record_version: MOUNT_RECORD_VERSION,
                pid: 123,
                mount_point: "T:".to_string(),
                volume: "demo".to_string(),
                cache_path: None,
                instance_id: Some("current-instance".to_string()),
                status: MountRecordStatus::Starting,
                sync_lifecycle: None,
            },
        )
        .expect("write mount record");

        let error = update_mount_record_status_at(
            &path,
            "T:",
            "replaced-instance",
            MountRecordStatus::Mounted,
            Some("local_ready"),
        )
        .expect_err("replaced instance should stop publication");

        assert!(matches!(
            error,
            MountRecordUpdateError::InstanceChanged { .. }
        ));
    }

    #[test]
    fn mount_record_status_update_reports_removed_record() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let error = update_mount_record_status_at(
            &tmp.path().join("missing.json"),
            "T:",
            "current-instance",
            MountRecordStatus::Mounted,
            Some("local_ready"),
        )
        .expect_err("removed mount record should stop publication");

        assert!(matches!(error, MountRecordUpdateError::NotFound { .. }));
    }

    #[test]
    fn active_mount_record_candidate_preserves_legacy_records_only() {
        let mut record = MountRecordInfo {
            record_version: MOUNT_RECORD_VERSION,
            pid: 123,
            mount_point: "T:".to_string(),
            volume: "demo".to_string(),
            cache_path: None,
            instance_id: None,
            status: MountRecordStatus::Starting,
            sync_lifecycle: None,
        };

        assert!(mount_record_is_active_candidate(&record));

        record.instance_id = Some("current-instance".to_string());
        assert!(!mount_record_is_active_candidate(&record));

        record.instance_id = None;
        record.status = MountRecordStatus::Failed;
        assert!(!mount_record_is_active_candidate(&record));
    }

    #[test]
    fn read_missing_pid_file_errors() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let result = read_pid_file_in(tmp.path(), "X:");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("No mount found"), "unexpected error: {msg}");
    }

    #[test]
    fn remove_nonexistent_pid_file_is_ok() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        // Should not error even though no file exists
        remove_pid_file_in(tmp.path(), "Q:").unwrap();
    }

    #[test]
    fn try_acquire_mount_lock_file_reports_contention() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let lock_path = tmp.path().join("mount.lock");
        let first = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        let second = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();

        let guard = try_acquire_mount_lock_file(first)
            .unwrap()
            .expect("first lock acquisition");
        assert!(
            try_acquire_mount_lock_file(second).unwrap().is_none(),
            "second handle should observe the lock as contended"
        );
        drop(guard);

        let third = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        assert!(
            try_acquire_mount_lock_file(third).unwrap().is_some(),
            "lock should be acquirable again after release"
        );
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
