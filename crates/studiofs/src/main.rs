//! StudioFS CLI — FUSE mount tool for S3-backed virtual filesystems.

use chrono::{Duration, NaiveDate, Utc};
use std::fs;
use std::path::{Path, PathBuf};
use tracing_appender::non_blocking::WorkerGuard;

mod cli;
mod diagnostics;
mod runtime_bridge;

const DAEMON_LOG_RETENTION_DAYS: i64 = 30;

fn prune_old_daemon_logs(logs_dir: &Path, stem: &str, today: NaiveDate) -> std::io::Result<()> {
    let cutoff = today - Duration::days(DAEMON_LOG_RETENTION_DAYS);
    for entry in fs::read_dir(logs_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(file_date) = parse_daemon_log_date(file_name, stem) else {
            continue;
        };
        if file_date < cutoff {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn parse_daemon_log_date(file_name: &str, stem: &str) -> Option<NaiveDate> {
    let prefix = format!("{stem}.log.");
    let date_part = file_name.strip_prefix(&prefix)?;
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
}

fn init_tracing(log_level: &str) -> anyhow::Result<Option<WorkerGuard>> {
    let filter = tracing_subscriber::EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if let (Ok(stem), Ok(logs_dir)) = (
        std::env::var("STUDIOFS_DAEMON_LOG_STEM"),
        std::env::var("STUDIOFS_DAEMON_LOG_DIR"),
    ) {
        let logs_dir = PathBuf::from(logs_dir);
        fs::create_dir_all(&logs_dir)?;
        let today = Utc::now().date_naive();
        let prune_error = prune_old_daemon_logs(&logs_dir, &stem, today).err();

        let file_appender = tracing_appender::rolling::daily(&logs_dir, format!("{stem}.log"));
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_ansi(false)
            .with_writer(non_blocking)
            .init();

        if let Some(err) = prune_error {
            tracing::warn!(
                log_dir = %logs_dir.display(),
                error = %err,
                "failed to prune old daemon logs"
            );
        }

        Ok(Some(guard))
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
        Ok(None)
    }
}

/// On Windows, locate WinFSP via the registry and pre-load `winfsp-x64.dll`
/// so that the delay-load resolver can find it.
#[cfg(windows)]
fn init_winfsp_dll_path() {
    use std::os::windows::ffi::OsStrExt;

    // SAFETY: This declaration uses the platform ABI for WinFSP's
    // delay-load entry point; it is only invoked with a trusted native
    // symbol signature and checked null-terminated UTF-16 arguments.
    unsafe extern "system" {
        fn LoadLibraryW(lpLibFileName: *const u16) -> *mut core::ffi::c_void;
    }

    // Read SxsDir from HKLM\SOFTWARE\WOW6432Node\WinFsp (or HKLM\SOFTWARE\WinFsp)
    let dll_path = (|| -> Option<PathBuf> {
        let hklm = winreg::RegKey::predef(winreg::enums::HKEY_LOCAL_MACHINE);

        let key = hklm
            .open_subkey("SOFTWARE\\WOW6432Node\\WinFsp")
            .or_else(|_| hklm.open_subkey("SOFTWARE\\WinFsp"))
            .ok()?;

        // Try SxsDir first (side-by-side installation)
        if let Ok(sxs_dir) = key.get_value::<String, _>("SxsDir") {
            let dll = PathBuf::from(&sxs_dir).join("bin").join("winfsp-x64.dll");
            if dll.exists() {
                return Some(dll);
            }
        }

        // Fallback: InstallDir
        if let Ok(install_dir) = key.get_value::<String, _>("InstallDir") {
            let dll = PathBuf::from(&install_dir)
                .join("bin")
                .join("winfsp-x64.dll");
            if dll.exists() {
                return Some(dll);
            }
        }

        None
    })();

    if let Some(path) = dll_path {
        // Convert to null-terminated UTF-16 for LoadLibraryW
        let wide: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // SAFETY: LoadLibraryW with a valid, null-terminated UTF-16 path.
        // Pre-loading the DLL means the delay-load resolver will find it
        // already in memory without needing to search PATH.
        // SAFETY: `wide` is a null-terminated UTF-16 buffer that lives across
        // the FFI call, and `LoadLibraryW` is invoked with the documented Win32 ABI.
        let handle = unsafe { LoadLibraryW(wide.as_ptr()) };
        if handle.is_null() {
            eprintln!("warning: failed to load winfsp-x64.dll from {:?}", path);
        }
    } else {
        eprintln!(
            "warning: WinFSP not found in registry. \
             Install WinFSP from https://winfsp.dev/ or add winfsp-x64.dll to PATH."
        );
    }
}

fn main() -> anyhow::Result<()> {
    use clap::Parser;

    #[cfg(windows)]
    init_winfsp_dll_path();

    let cli = cli::Cli::parse();
    let _daemon_log_guard = init_tracing(&cli.log_level)?;
    cli.dispatch()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_daily_daemon_log_file_names() {
        let stem = "volume-1234";
        assert_eq!(
            parse_daemon_log_date("volume-1234.log.2026-03-13", stem),
            Some(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap())
        );
        assert_eq!(
            parse_daemon_log_date("volume-1234-2026-03-13.log", stem),
            None
        );
    }
}
