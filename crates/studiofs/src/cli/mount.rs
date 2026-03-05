//! `studiofs mount` — mount a filesystem volume.

use std::path::Path;
use std::sync::Arc;
use studiofs_data::DataStoreConfig;
use studiofs_meta::open_meta_engine;
use studiofs_vfs::StudioVfs;
use tracing::info;

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

pub fn run(
    name: &str,
    mount_point: &str,
    meta_dir: &Path,
    cache_dir: Option<&Path>,
    cache_size: &str,
    memory_limit: &str,
    foreground: bool,
    detach: bool,
) -> anyhow::Result<()> {
    // ── Daemon mode: re-spawn in background and exit ────────────────────────
    if detach {
        return spawn_daemon(name, mount_point);
    }

    // ── Guard against double-mount ──────────────────────────────────────────
    // Note: when launched via `mount -d`, the parent pre-writes the PID file
    // with the child's PID before the child starts.  The child must therefore
    // ignore a PID file that points to itself.
    if let Ok((pid, _, _)) = super::utils::read_pid_file(mount_point) {
        if pid != std::process::id() && super::utils::is_process_running(pid) {
            anyhow::bail!(
                "Already mounted at '{}' (PID {}). Run `studiofs umount \"{}\"` first.",
                mount_point,
                pid,
                mount_point
            );
        }
        // Stale PID file (or the pre-written entry from the parent) — clean up
        let _ = super::utils::remove_pid_file(mount_point);
    }

    let db_path = meta_dir.join(format!("{name}.db"));
    info!("opening volume {name:?} from {db_path:?}");

    // Open metadata engine configured for this volume.
    let meta = open_meta_engine(&db_path)?;
    let format_info = meta.get_format_info()?;

    info!(
        "volume: {} ({}), endpoint: {}, bucket: {}",
        format_info.name, format_info.uuid, format_info.endpoint, format_info.bucket
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

    println!("✓ Volume '{}' ready to mount at '{}'", name, mount_point);
    println!("  Memory limit: {}", memory_limit);
    println!("  Cache size:   {}", cache_size);
    println!("  Invalidation: PostgreSQL sync");

    // Background tasks (uploader + GC) are spawned internally by StudioVfs::new

    // Write PID file so `studiofs umount` can find us
    let _ = super::utils::write_pid_file(mount_point, name, std::process::id());

    // Named event for external graceful shutdown (used by `studiofs umount` on Windows)
    let event_name = super::utils::shutdown_event_name(mount_point);

    // Start platform-specific FUSE mount
    #[cfg(windows)]
    {
        info!("starting WinFSP mount at {mount_point}");
        studiofs_winfsp::mount_and_serve(
            vfs,
            rt,
            mount_point,
            &format_info.name,
            foreground,
            Some(&event_name),
        )
        .map_err(|e| anyhow::anyhow!("WinFSP error: {e}"))?;

        // WinFSP returns normally — clean up PID file
        let _ = super::utils::remove_pid_file(mount_point);
    }

    #[cfg(unix)]
    {
        info!("starting FUSE mount at {mount_point}");
        studiofs_fuse::mount_and_serve(vfs, rt, mount_point, &format_info.name, foreground)
            .map_err(|e| anyhow::anyhow!("FUSE error: {e}"))?;
        // Note: FUSE mount_and_serve calls process::exit(0), so this is unreachable.
        // PID file is cleaned up by the umount command or overwritten on next mount.
    }

    Ok(())
}

// ─── Daemon spawn ────────────────────────────────────────────────────────────

/// Re-execute the current process without `-d`/`--detach`, detached from the
/// terminal.  Stdout/stderr are redirected to a log file under `~/.studiofs/logs/`.
fn spawn_daemon(name: &str, mount_point: &str) -> anyhow::Result<()> {
    // Reconstruct args, stripping the detach flag
    let args: Vec<String> = std::env::args()
        .skip(1) // skip program name
        .filter(|a| a != "-d" && a != "--detach")
        .collect();

    // Prepare log file
    let logs_dir = super::utils::get_logs_dir()?;
    let log_path = logs_dir.join(format!(
        "{}.log",
        super::utils::sanitize_mount_point(mount_point)
    ));
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let log_file_err = log_file.try_clone()?;

    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(&exe);
    cmd.args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(log_file)
        .stderr(log_file_err);

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
}
