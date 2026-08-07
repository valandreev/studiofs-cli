//! StudioFS CLI — FUSE mount tool for S3-backed virtual filesystems.

mod allocator_telemetry;

#[global_allocator]
static GLOBAL_ALLOCATOR: allocator_telemetry::TrackingMiMalloc =
    allocator_telemetry::TrackingMiMalloc;

use chrono::{Duration, NaiveDate, Utc};
#[cfg(test)]
use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::prelude::*;

mod cli;
mod diagnostics;
mod investigation;
mod runtime_bridge;
mod tray;

const DAEMON_LOG_RETENTION_DAYS: i64 = 30;
type DiagnosticsLogFilterHandle =
    tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;
static DIAGNOSTICS_LOG_FILTER_HANDLE: OnceLock<DiagnosticsLogFilterHandle> = OnceLock::new();

#[cfg(test)]
static TEST_DIAGNOSTICS_LOG_LEVEL: OnceLock<Mutex<Option<String>>> = OnceLock::new();

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
    let (filter_layer, reload_handle) = tracing_subscriber::reload::Layer::new(filter);
    let _ = DIAGNOSTICS_LOG_FILTER_HANDLE.set(reload_handle);

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

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_ansi(false)
                    .with_writer(non_blocking),
            )
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
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(tracing_subscriber::fmt::layer().with_target(false))
            .init();
        Ok(None)
    }
}

pub(crate) fn set_diagnostics_log_verbosity(
    level: crate::diagnostics::SafeLogVerbosity,
) -> anyhow::Result<()> {
    #[cfg(test)]
    if let Some(test_level) = TEST_DIAGNOSTICS_LOG_LEVEL.get() {
        *test_level.lock() = Some(level.as_directive().to_string());
        return Ok(());
    }

    let handle = DIAGNOSTICS_LOG_FILTER_HANDLE
        .get()
        .ok_or_else(|| anyhow::anyhow!("diagnostics log filter reload handle is unavailable"))?;
    handle.reload(tracing_subscriber::EnvFilter::new(level.as_directive()))?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn ensure_test_diagnostics_log_filter() {
    let _ = TEST_DIAGNOSTICS_LOG_LEVEL.get_or_init(|| Mutex::new(Some("info".to_string())));
}

fn main() -> anyhow::Result<()> {
    use clap::Parser;

    #[cfg(windows)]
    if let Err(error) = studiofs_winfsp::ensure_winfsp_runtime_loaded() {
        eprintln!("warning: {error}");
    }

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
