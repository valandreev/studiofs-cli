use std::path::{Path, PathBuf};
use std::process;

use windows::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
use windows::Win32::System::Threading::{
    GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};
use windows::Win32::UI::Shell::{
    SHCNE_ASSOCCHANGED, SHCNE_UPDATEDIR, SHCNF_IDLIST, SHCNF_PATHW, SHChangeNotify,
};
use winreg::RegKey;
use winreg::enums::{HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE, KEY_ALL_ACCESS};

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
    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    let key_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");
    let previous = hklm
        .open_subkey(&key_path)
        .ok()
        .and_then(|key| key.get_value::<String, _>("").ok());
    let (icon_key, _) = hklm.create_subkey(&key_path)?;
    let icon_value = format!("{},0", icon_path.to_string_lossy());
    icon_key.set_value("", &icon_value)?;
    icon_key.set_value(DRIVE_ICON_METADATA_PID, &process::id())?;
    icon_key.set_value(DRIVE_ICON_METADATA_SOURCE, &DRIVE_ICON_METADATA_MARKER)?;
    notify_shell_drive_icon_changed(letter);
    Ok(MountedDriveIcon { letter, previous })
}

fn restore_drive_icon(letter: char, previous: Option<String>) -> std::io::Result<()> {
    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    let key_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");

    match previous {
        Some(value) => {
            let icon_key = hklm.open_subkey_with_flags(&key_path, KEY_ALL_ACCESS)?;
            icon_key.set_value("", &value)?;
            let _ = icon_key.delete_value(DRIVE_ICON_METADATA_SOURCE);
            let _ = icon_key.delete_value(DRIVE_ICON_METADATA_PID);
        }
        None => {
            delete_default_icon_and_empty_parent(&hklm, letter);
        }
    }

    notify_shell_drive_icon_changed(letter);
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
    if !std::fs::read(&icon_path).is_ok_and(|bytes| bytes == DRIVE_ICON_ICO) {
        std::fs::write(&icon_path, DRIVE_ICON_ICO)?;
    }

    Ok(icon_path)
}

fn cleanup_orphaned_drive_icons(icon_path: &Path) {
    let expected_icon_path = icon_path.to_string_lossy().to_string();
    let expected_icon_value = format!("{expected_icon_path},0");
    let self_pid = process::id();

    cleanup_orphaned_drive_icons_in_root(
        &RegKey::predef(HKEY_LOCAL_MACHINE),
        &expected_icon_path,
        &expected_icon_value,
        self_pid,
    );
    cleanup_orphaned_drive_icons_in_root(
        &RegKey::predef(HKEY_CURRENT_USER),
        &expected_icon_path,
        &expected_icon_value,
        self_pid,
    );
}

fn cleanup_orphaned_drive_icons_in_root(
    root: &RegKey,
    expected_icon_path: &str,
    expected_icon_value: &str,
    self_pid: u32,
) {
    for letter in 'A'..='Z' {
        let key_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");
        let key = match root.open_subkey(&key_path) {
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

        if source != DRIVE_ICON_METADATA_MARKER
            || (current_value != expected_icon_path && current_value != expected_icon_value)
        {
            continue;
        }

        if owning_pid.is_none_or(|pid| !is_process_alive(pid, self_pid)) {
            drop(key);
            delete_default_icon_and_empty_parent(root, letter);
        }
    }
}

fn delete_default_icon_and_empty_parent(root: &RegKey, letter: char) {
    let default_icon_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}\DefaultIcon");
    let parent_path = format!(r"{DRIVE_ICON_KEY_ROOT}\{letter}");

    let _ = root.delete_subkey_all(&default_icon_path);
    let _ = root.delete_subkey(&parent_path);
}

fn notify_shell_drive_icon_changed(letter: char) {
    let drive_root = format!("{letter}:\\");
    let wide_drive_root = drive_root
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();

    unsafe {
        SHChangeNotify(
            SHCNE_UPDATEDIR,
            SHCNF_PATHW,
            Some(wide_drive_root.as_ptr().cast()),
            None,
        );
        SHChangeNotify(SHCNE_ASSOCCHANGED, SHCNF_IDLIST, None, None);
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
