use std::path::{Path, PathBuf};
use std::process;

use windows::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
use windows::Win32::System::Threading::{
    GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};
use winreg::RegKey;
use winreg::enums::{HKEY_CURRENT_USER, KEY_ALL_ACCESS};

const DRIVE_ICON_ICO: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/branding/studiofs-logo-final/win/studiofs-drive.ico"
));
const DRIVE_ICON_NAME: &str = "studiofs-drive.ico";
const DRIVE_ICON_KEY_ROOT: &str =
    "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons";
const DRIVE_ICON_METADATA_SOURCE: &str = "Source";
const DRIVE_ICON_METADATA_PID: &str = "ProcessId";
const DRIVE_ICON_METADATA_MARKER: &str = "studiofs-winfsp-drive-icon";

pub(crate) struct MountedDriveIcon {
    letter: char,
    previous: Option<String>,
}

impl Drop for MountedDriveIcon {
    fn drop(&mut self) {
        if let Err(error) = restore_drive_icon(self.letter, self.previous.clone()) {
            tracing::warn!(
                drive_letter = %self.letter,
                error = %error,
                "failed to restore drive icon value"
            );
        }
    }
}

pub(crate) fn mount_point_drive_icon(mount_point: &str) -> std::io::Result<MountedDriveIcon> {
    let letter = drive_letter(mount_point).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "only Windows drive-letter mount points support drive icon configuration",
        )
    })?;

    let icon_path = ensure_embedded_drive_icon_path()?;
    cleanup_orphaned_drive_icons(&icon_path);
    set_drive_icon(letter, &icon_path).map_err(std::io::Error::other)
}

fn set_drive_icon(letter: char, icon_path: &Path) -> std::io::Result<MountedDriveIcon> {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let key_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");
    let previous = hkcu
        .open_subkey(&key_path)
        .ok()
        .and_then(|key| key.get_value::<String, _>("").ok());
    let (icon_key, _) = hkcu.create_subkey(&key_path)?;
    icon_key.set_value("", &icon_path.to_string_lossy().to_string())?;
    icon_key.set_value(DRIVE_ICON_METADATA_SOURCE, &DRIVE_ICON_METADATA_MARKER)?;
    icon_key.set_value(DRIVE_ICON_METADATA_PID, &process::id())?;
    Ok(MountedDriveIcon { letter, previous })
}

fn restore_drive_icon(letter: char, previous: Option<String>) -> std::io::Result<()> {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let key_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");
    let icon_key = hkcu.open_subkey_with_flags(&key_path, KEY_ALL_ACCESS)?;

    match previous {
        Some(value) => {
            icon_key.set_value("", &value)?;
            let _ = icon_key.delete_value(DRIVE_ICON_METADATA_SOURCE);
            let _ = icon_key.delete_value(DRIVE_ICON_METADATA_PID);
        }
        None => {
            let _ = hkcu.delete_subkey_all(&key_path);
        }
    }

    Ok(())
}

fn ensure_embedded_drive_icon_path() -> std::io::Result<PathBuf> {
    let mut icon_dir = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("APPDATA").map(PathBuf::from))
        .unwrap_or_else(std::env::temp_dir);
    icon_dir.push("studiofs");
    icon_dir.push("icons");
    std::fs::create_dir_all(&icon_dir)?;

    let icon_path = icon_dir.join(DRIVE_ICON_NAME);
    if !std::fs::metadata(&icon_path).is_ok_and(|meta| meta.len() == DRIVE_ICON_ICO.len() as u64)
        || std::fs::read(&icon_path).ok() != Some(DRIVE_ICON_ICO.to_vec())
    {
        std::fs::write(&icon_path, DRIVE_ICON_ICO)?;
    }

    Ok(icon_path)
}

fn cleanup_orphaned_drive_icons(icon_path: &Path) {
    let expected_icon_path = icon_path.to_string_lossy().to_string();
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let self_pid = process::id();

    for letter in b'A'..=b'Z' {
        let key_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");
        let key = match hkcu.open_subkey(&key_path) {
            Ok(key) => key,
            Err(_) => continue,
        };

        let source = match key.get_value::<String, _>(DRIVE_ICON_METADATA_SOURCE).ok() {
            Some(value) => value,
            None => continue,
        };
        let current_value = match key.get_value::<String, _>("").ok() {
            Some(value) => value,
            None => continue,
        };
        let owning_pid = key.get_value::<u32, _>(DRIVE_ICON_METADATA_PID).ok();

        if source != DRIVE_ICON_METADATA_MARKER || current_value != expected_icon_path {
            continue;
        }

        if owning_pid.is_none_or(|pid| !is_process_alive(pid, self_pid)) {
            let _ = hkcu.delete_subkey_all(&key_path);
        }
    }
}

fn is_process_alive(pid: u32, self_pid: u32) -> bool {
    if pid == self_pid {
        return true;
    }
    if pid == 0 {
        return false;
    }

    let process_handle =
        match unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid).ok() } {
            Some(handle) => handle,
            None => return false,
        };

    let mut exit_code = 0;
    let active = unsafe { GetExitCodeProcess(process_handle, &mut exit_code) }.is_ok()
        && exit_code == STILL_ACTIVE.0 as u32;
    let _ = unsafe { CloseHandle(process_handle) };

    active
}

fn drive_letter(mount_point: &str) -> Option<char> {
    let bytes = mount_point.as_bytes();
    if bytes.len() < 2 || !bytes[0].is_ascii_alphabetic() || bytes[1] != b':' {
        return None;
    }
    Some(bytes[0].to_ascii_uppercase() as char)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_point_parses_drive_letter() {
        assert_eq!(drive_letter("Z:"), Some('Z'));
        assert_eq!(drive_letter("z"), None);
    }
}
