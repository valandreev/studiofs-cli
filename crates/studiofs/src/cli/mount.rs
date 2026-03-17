//! `studiofs mount` — mount a filesystem volume.

use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use studiofs_data::DataStoreConfig;
use studiofs_meta::redb_engine::RedbSnapshot;
use studiofs_meta::{MetaEngine, MetadataBackend, RedbMetaEngine, open_meta_engine};
use studiofs_vfs::StudioVfs;
use tracing::{debug, info};

/// Parse a human-readable size string like "100GB", "512MB", "1TB".
fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_uppercase();
    let (num_str, multiplier) = if s.ends_with("TB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
    } else if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else {
        (s.as_str(), 1u64)
    };
    let num: u64 = num_str.trim().parse()?;
    Ok(num * multiplier)
}

fn resolve_mount_path(name: &str, mount_point: &str) -> PathBuf {
    let mount_path = Path::new(mount_point);
    if mount_path.ends_with(name) {
        mount_path.to_path_buf()
    } else {
        mount_path.join(name)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    mount_point: &str,
    meta_dir: &Path,
    cache_dir: Option<&Path>,
    cache_size: &str,
    memory_limit: &str,
    principal: Option<&str>,
    foreground: bool,
    detach: bool,
    snapshot_id: Option<&str>,
) -> anyhow::Result<()> {
    let actual_mount_path = resolve_mount_path(name, mount_point);
    let actual_mount_path = actual_mount_path.to_string_lossy().into_owned();

    if let Err(err) = std::fs::create_dir_all(&actual_mount_path) {
        #[cfg(target_os = "macos")]
        if err.kind() == std::io::ErrorKind::PermissionDenied
            && actual_mount_path.starts_with("/Volumes")
        {
            anyhow::bail!(
                "Failed to create mount point '{}'. You can grant mount access with:\n  sudo mkdir -p '{}'\n  sudo chown $(id -un):$(id -gn) '{}'",
                actual_mount_path,
                actual_mount_path,
                actual_mount_path
            );
        }

        return Err(err.into());
    }

    #[cfg(windows)]
    {
        let mut entries = std::fs::read_dir(&actual_mount_path)?;
        if entries.next().is_some() {
            anyhow::bail!(
                "Mount point '{}' must be empty to mount a volume.",
                actual_mount_path
            );
        }
    }

    // ── Daemon mode: re-spawn in background and exit ────────────────────────
    if detach {
        return spawn_daemon(name, &actual_mount_path, meta_dir);
    }

    let run_in_foreground = foreground || !detach;

    if let Some(principal) = principal {
        // SAFETY: this happens before Tokio runtime creation, so no runtime
        // worker threads are alive to race on process environment access.
        unsafe {
            std::env::set_var("STUDIOFS_PRINCIPAL", principal);
        }
    }

    // ── Guard against double-mount ──────────────────────────────────────────
    // Note: when launched via `mount -d`, the parent pre-writes the PID file
    // with the child's PID before the child starts.  The child must therefore
    // ignore a PID file that points to itself.
    if let Ok((pid, _, _)) = super::utils::read_pid_file(&actual_mount_path) {
        if pid != std::process::id() && super::utils::is_process_running(pid) {
            anyhow::bail!(
                "Already mounted at '{}' (PID {}). Run `studiofs umount \"{}\"` first.",
                actual_mount_path,
                pid,
                actual_mount_path
            );
        }
        // Stale PID file (or the pre-written entry from the parent) — clean up
        let _ = super::utils::remove_pid_file(&actual_mount_path);
    }

    let db_path = meta_dir.join(format!("{name}.db"));
    debug!("opening volume {name:?} from {db_path:?}");
    let live_meta = open_meta_engine(&db_path)?;
    let mut format_info = live_meta.get_format_info()?;
    let (meta, read_only_mount) = if let Some(snapshot_id) = snapshot_id {
        let snapshot = live_meta
            .get_snapshot(snapshot_id)?
            .ok_or_else(|| anyhow::anyhow!("snapshot '{snapshot_id}' not found"))?;
        if snapshot.snapshot_payload.is_empty() {
            anyhow::bail!("snapshot '{snapshot_id}' payload is empty");
        }
        let snapshot_state: RedbSnapshot = serde_json::from_slice(&snapshot.snapshot_payload)
            .map_err(|e| anyhow::anyhow!("invalid snapshot payload for '{snapshot_id}': {e}"))?;

        let snapshot_dir = meta_dir.join("snapshots");
        std::fs::create_dir_all(&snapshot_dir)?;
        let snapshot_db_path = snapshot_dir.join(format!("{name}.{snapshot_id}.db"));
        if snapshot_db_path.exists() {
            let _ = std::fs::remove_file(&snapshot_db_path);
        }
        let snapshot_engine = RedbMetaEngine::open(&snapshot_db_path)?;
        format_info.metadata.backend = MetadataBackend::RedbLocal;
        format_info.metadata.postgres = None;
        snapshot_engine.set_format_info(&format_info)?;
        snapshot_engine.import_snapshot(&snapshot_state)?;

        (
            Arc::new(snapshot_engine) as Arc<dyn studiofs_meta::MetaEngine>,
            true,
        )
    } else {
        (live_meta, false)
    };

    info!(
        "mounting volume '{}' (storage: endpoint={}, bucket={})",
        format_info.name, format_info.endpoint, format_info.bucket
    );

    // Set up data store config (StudioVfs creates Read + Write connections directly)
    let data_config = DataStoreConfig::from(&format_info);

    // Set up caches
    let mem_limit = parse_size(memory_limit)?;

    let cache_size_bytes = parse_size(cache_size)?;
    let base_cache_path = cache_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| meta_dir.join("cache"));

    // BlockCache will create blocks/ and upload/ subdirectories inside this UUID dir.
    let cache_path = base_cache_path.join(&format_info.uuid);

    let cache_path_str = cache_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid cache path"))?;

    // Create a tokio runtime for async ops in FUSE callbacks and background tasks
    let rt = Arc::new(
        tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("failed to create tokio runtime: {e}"))?,
    );
    let _guard = rt.enter();

    // Create VFS.
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_path_str,
        cache_size_bytes,
        mem_limit,
    )
    .map_err(|e| anyhow::anyhow!("failed to init vfs: {e}"))?;

    println!("✓ Volume '{}' ready to mount at '{}'", name, actual_mount_path);
    println!("  Memory limit: {}", memory_limit);
    println!("  Cache size:   {}", cache_size);
    println!(
        "  Storage:      {} / {}",
        format_info.endpoint, format_info.bucket
    );
    println!("  Invalidation: PostgreSQL sync");
    if let Some(snapshot_id) = snapshot_id {
        println!("  Snapshot:     {} (read-only)", snapshot_id);
    }

    // Background tasks (uploader + GC) are spawned internally by StudioVfs::new

    // Write PID file so `studiofs umount` can find us
    let _ = super::utils::write_pid_file(&actual_mount_path, name, std::process::id());

    // Start platform-specific FUSE mount
    #[cfg(windows)]
    {
        // Named event for external graceful shutdown (used by `studiofs umount`).
        let event_name = super::utils::shutdown_event_name(&actual_mount_path);
        info!("starting WinFSP mount at {actual_mount_path}");
        studiofs_winfsp::mount_and_serve(
            vfs,
            rt,
            &actual_mount_path,
            &format_info.name,
            run_in_foreground,
            read_only_mount,
            Some(&event_name),
        )
        .map_err(|e| anyhow::anyhow!("WinFSP error: {e}"))?;

        // WinFSP returns normally — clean up PID file
        let _ = super::utils::remove_pid_file(&actual_mount_path);
    }

    #[cfg(unix)]
    {
        info!("starting FUSE mount at {actual_mount_path}");
        studiofs_fuse::mount_and_serve(
            vfs,
            rt,
            &actual_mount_path,
            &format_info.name,
            run_in_foreground,
            read_only_mount,
        )
        .map_err(|e| anyhow::anyhow!("FUSE error: {e}"))?;
        // Note: FUSE mount_and_serve calls process::exit(0), so this is unreachable.
        // PID file is cleaned up by the umount command or overwritten on next mount.
    }

    Ok(())
}

// ─── Daemon spawn ────────────────────────────────────────────────────────────

/// Re-execute the current process without `-d`/`--detach`, detached from the
/// terminal. The child enables daily-rotated tracing logs under `~/.studiofs/logs/`.
fn spawn_daemon(name: &str, mount_point: &str, meta_dir: &Path) -> anyhow::Result<()> {
    // Reconstruct args, stripping the detach flag
    let mut args: Vec<String> = std::env::args()
        .skip(1) // skip program name
        .filter(|a| a != "-d" && a != "--detach")
        .map(|arg| arg.to_string())
        .collect();

    if let Some(mount_arg_idx) = find_daemon_mount_point_arg_index(&args, name) {
        if let Some(arg) = args.get_mut(mount_arg_idx) {
        *arg = mount_point.to_string();
    }
    }
    let logs_dir = super::utils::get_logs_dir()?;
    let log_stem = daemon_log_stem(name, meta_dir)?;
    let today = Utc::now().format("%Y-%m-%d");
    let log_path = logs_dir.join(format!("{log_stem}.log.{today}"));

    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(&exe);
    cmd.args(&args)
        .env("STUDIOFS_DAEMON_LOG_STEM", &log_stem)
        .env("STUDIOFS_DAEMON_LOG_DIR", &logs_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Create a new session so the child is fully detached from the terminal.
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    let child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn background mount process: {e}"))?;
    let pid = child.id();

    // Write PID file (the child will overwrite with identical data once it starts)
    super::utils::write_pid_file(mount_point, name, pid)?;

    println!(
        "✓ Volume '{}' mounting at '{}' in background (PID: {})",
        name, mount_point, pid
    );
    println!("  Log file: {}", log_path.display());
    println!("  To unmount: studiofs umount \"{}\"", mount_point);

    Ok(())
}

fn daemon_log_stem(name: &str, meta_dir: &Path) -> anyhow::Result<String> {
    let db_path = meta_dir.join(format!("{name}.db"));
    let meta = open_meta_engine(&db_path)?;
    let format_info = meta.get_format_info()?;
    Ok(format!(
        "{}-{}",
        sanitize_log_component(name),
        format_info.uuid
    ))
}

fn find_daemon_mount_point_arg_index(args: &[String], volume_name: &str) -> Option<usize> {
    let mount_cmd_idx = args.iter().position(|arg| arg == "mount")?;
    let mut i = mount_cmd_idx + 1;

    while i < args.len() {
        if args[i] == volume_name {
            i += 1;
            while i < args.len() {
                let arg = &args[i];
                if arg == "-d" || arg == "--detach" || arg == "--foreground" {
                    i += 1;
                    continue;
                }
                if arg.starts_with("--cache-dir=")
                    || arg.starts_with("--cache-size=")
                    || arg.starts_with("--memory-limit=")
                    || arg.starts_with("--principal=")
                    || arg.starts_with("--snapshot-id=")
                {
                    i += 1;
                    continue;
                }
                if arg == "--cache-dir"
                    || arg == "--cache-size"
                    || arg == "--memory-limit"
                    || arg == "--principal"
                    || arg == "--snapshot-id"
                {
                    i += 2;
                    continue;
                }
                if arg.starts_with('-') {
                    i += 1;
                    continue;
                }

                return Some(i);
            }
            return None;
        }
        i += 1;
    }

    None
}

fn sanitize_log_component(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "volume".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("100GB").unwrap(), 100 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("1TB").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn test_resolve_mount_path() {
        assert_eq!(
            resolve_mount_path("projectfs", "/mnt"),
            Path::new("/mnt").join("projectfs")
        );
        assert_eq!(
            resolve_mount_path("projectfs", "/mnt/projectfs"),
            Path::new("/mnt/projectfs")
        );
    }

    #[test]
    fn test_find_daemon_mount_point_arg_with_flags_between_name_and_mount_point() {
        let args = vec![
            "mount".to_string(),
            "projectfs".to_string(),
            "--foreground".to_string(),
            "/mnt/studio".to_string(),
            "-d".to_string(),
        ];
        let mount_arg_idx = find_daemon_mount_point_arg_index(&args, "projectfs").unwrap();
        assert_eq!(mount_arg_idx, 3);
        assert_eq!(args[mount_arg_idx], "/mnt/studio");
    }

    #[test]
    fn test_find_daemon_mount_point_arg_with_value_flags() {
        let args = vec![
            "mount".to_string(),
            "projectfs".to_string(),
            "--cache-dir".to_string(),
            "/tmp/studio-cache".to_string(),
            "/mnt/studio".to_string(),
        ];
        let mount_arg_idx = find_daemon_mount_point_arg_index(&args, "projectfs").unwrap();
        assert_eq!(mount_arg_idx, 4);
        assert_eq!(args[mount_arg_idx], "/mnt/studio");
    }
}
