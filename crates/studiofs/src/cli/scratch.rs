//! `studiofs scratch` — scratch-layer inspection, cleanup, promotion, and orphan restoration.

use crate::diagnostics::{
    LocalDiagnosticsClient, LocalDiagnosticsDiscovery, discover_local_sessions,
};
use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use studiofs_cache::open_local_payload_backend;
use studiofs_cache::runtime::{CacheIndex, LocalPayloadBackendKind, detect_cache_backends};
use studiofs_meta::{MetaOpenOptions, open_meta_engine_with_options};
use studiofs_vfs::{
    ScratchCleanOutcome, ScratchListEntry, ScratchOrphanSummary, ScratchPromotePathOutcome,
    ScratchRestoreOutcome,
};

use super::utils::format_bytes;

#[derive(Debug, Args)]
#[command(about = "Manage local-only scratch-layer entries")]
#[command(after_help = "\
EXAMPLES:
  studiofs scratch list --volume myvolume
  studiofs scratch list --volume myvolume --orphaned
  studiofs scratch clean --volume myvolume --older-than 7d
  studiofs scratch promote /mnt/studio/renders/tmp
  studiofs scratch promote --volume myvolume renders/tmp
  studiofs scratch restore --volume myvolume 1700000000000/renders/autosave.tmp
")]
pub(crate) struct ScratchArgs {
    #[command(subcommand)]
    command: ScratchCommand,
}

#[derive(Debug, Subcommand)]
enum ScratchCommand {
    /// List scratch entries or relocated orphaned scratch subtrees
    List(ListArgs),
    /// Remove scratch entries (skips entries with open handles on mounted volumes)
    Clean(CleanArgs),
    /// Promote a scratch file or directory into tracked storage
    Promote(PromoteArgs),
    /// Restore an orphaned scratch entry or subtree back into the scratch layer
    Restore(RestoreArgs),
}

#[derive(Debug, Args)]
struct ListArgs {
    /// Optional mounted path or volume-relative path filter
    path: Option<String>,

    /// List orphaned scratch subtrees relocated after foreign parent removal
    #[arg(long)]
    orphaned: bool,

    /// Target volume name (optional for mounted paths or when one volume is mounted)
    #[arg(long, value_name = "VOLUME_NAME")]
    volume: Option<String>,

    /// Override local cache base directory for offline mode (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct CleanArgs {
    /// Only remove entries with mtime older than this duration (e.g. `30m`, `24h`, `7d`)
    #[arg(long, value_name = "DURATION")]
    older_than: Option<String>,

    /// Optional mounted path or volume-relative path filter
    path: Option<String>,

    /// Target volume name (optional for mounted paths or when one volume is mounted)
    #[arg(long, value_name = "VOLUME_NAME")]
    volume: Option<String>,

    /// Override local cache base directory for offline mode (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PromoteArgs {
    /// Absolute mounted path or volume-relative path to promote
    path: String,

    /// Target volume name (optional for absolute mounted paths or when one volume is mounted)
    #[arg(long, value_name = "VOLUME_NAME")]
    volume: Option<String>,

    /// Override local cache base directory for offline mode (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RestoreArgs {
    /// Orphan identifier (`<timestamp>/<old_path>`) or unambiguous original path
    orphan: String,

    /// Optional volume-relative destination path (defaults to the original path)
    #[arg(long, value_name = "PATH")]
    to: Option<String>,

    /// Target volume name (optional when one volume is mounted)
    #[arg(long, value_name = "VOLUME_NAME")]
    volume: Option<String>,

    /// Override local cache base directory for offline mode (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
enum ScratchTarget {
    Mounted {
        discovery: LocalDiagnosticsDiscovery,
        relative_path: Option<String>,
    },
    Offline {
        meta_dir: PathBuf,
        db_path: PathBuf,
        relative_path: Option<String>,
    },
}

impl ScratchArgs {
    pub(crate) fn dispatch(self) -> Result<()> {
        match self.command {
            ScratchCommand::List(args) => run_list(args),
            ScratchCommand::Clean(args) => run_clean(args),
            ScratchCommand::Promote(args) => run_promote(args),
            ScratchCommand::Restore(args) => run_restore(args),
        }
    }
}

fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .unwrap_or(0)
}

fn local_cache_path(meta_dir: &Path, cache_dir: Option<&Path>, volume_uuid: &str) -> PathBuf {
    let base = cache_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| meta_dir.join("cache"));
    base.join(volume_uuid)
}

pub(crate) fn parse_duration_secs(raw: &str) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("duration must not be empty");
    }
    let split = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (num_str, unit) = trimmed.split_at(split);
    if num_str.is_empty() {
        bail!("invalid duration `{raw}`; expected e.g. `30s`, `15m`, `24h`, `7d`");
    }
    let value: u64 = num_str
        .parse()
        .with_context(|| format!("invalid duration `{raw}`"))?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "" | "s" => 1u64,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => bail!("invalid duration unit in `{raw}`; use `s`, `m`, `h`, `d`, or `w`"),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("duration `{raw}` overflows u64 seconds"))
}

fn open_offline_meta(db_path: &Path) -> Result<std::sync::Arc<dyn studiofs_meta::MetaEngine>> {
    open_meta_engine_with_options(
        db_path,
        MetaOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .map_err(anyhow::Error::from)
}

fn run_list(args: ListArgs) -> Result<()> {
    let sessions = discover_local_sessions()?;
    let cwd = std::env::current_dir().ok();
    let target = resolve_scratch_target(
        &sessions,
        args.path.as_deref(),
        args.volume.as_deref(),
        cwd.as_deref(),
        false,
        super::utils::get_default_meta_dir,
    )?;
    if args.orphaned {
        let orphans = execute_list_orphans(&target)?;
        println!("{}", format_orphan_list(&orphans));
    } else {
        let entries = execute_list(&target)?;
        println!("{}", format_scratch_list(&entries));
    }
    Ok(())
}

fn run_clean(args: CleanArgs) -> Result<()> {
    let older_than_secs = args
        .older_than
        .as_deref()
        .map(parse_duration_secs)
        .transpose()?;
    let sessions = discover_local_sessions()?;
    let cwd = std::env::current_dir().ok();
    let target = resolve_scratch_target(
        &sessions,
        args.path.as_deref(),
        args.volume.as_deref(),
        cwd.as_deref(),
        false,
        super::utils::get_default_meta_dir,
    )?;
    let outcome = execute_clean(&target, args.cache_dir.as_deref(), older_than_secs)?;
    println!("{}", format_clean_outcome(&outcome));
    Ok(())
}

fn run_promote(args: PromoteArgs) -> Result<()> {
    let sessions = discover_local_sessions()?;
    let cwd = std::env::current_dir().ok();
    let target = resolve_scratch_target(
        &sessions,
        Some(&args.path),
        args.volume.as_deref(),
        cwd.as_deref(),
        true,
        super::utils::get_default_meta_dir,
    )?;
    let outcome = execute_promote(&target, args.cache_dir.as_deref())?;
    println!("{}", format_promote_outcome(&outcome));
    Ok(())
}

fn run_restore(args: RestoreArgs) -> Result<()> {
    let sessions = discover_local_sessions()?;
    let cwd = std::env::current_dir().ok();
    let target = resolve_scratch_target(
        &sessions,
        None,
        args.volume.as_deref(),
        cwd.as_deref(),
        false,
        super::utils::get_default_meta_dir,
    )?;
    let outcome = execute_restore(
        &target,
        args.cache_dir.as_deref(),
        &args.orphan,
        args.to.as_deref(),
    )?;
    println!("{}", format_restore_outcome(&outcome));
    Ok(())
}

fn execute_list(target: &ScratchTarget) -> Result<Vec<ScratchListEntry>> {
    match target {
        ScratchTarget::Mounted {
            discovery,
            relative_path,
        } => {
            let client = LocalDiagnosticsClient::new(discovery.clone());
            let rel = relative_path.clone();
            crate::runtime_bridge::block_on(async move {
                client.list_scratch_entries(rel.as_deref()).await
            })
        }
        ScratchTarget::Offline {
            db_path,
            relative_path,
            ..
        } => {
            let meta = open_offline_meta(db_path)?;
            studiofs_vfs::list_scratch_entries_offline(meta.as_ref(), relative_path.as_deref())
                .map_err(anyhow::Error::from)
        }
    }
}

fn execute_list_orphans(target: &ScratchTarget) -> Result<Vec<ScratchOrphanSummary>> {
    match target {
        ScratchTarget::Mounted {
            discovery,
            relative_path,
        } => {
            let client = LocalDiagnosticsClient::new(discovery.clone());
            let rel = relative_path.clone();
            crate::runtime_bridge::block_on(async move {
                client.list_scratch_orphans(rel.as_deref()).await
            })
        }
        ScratchTarget::Offline {
            db_path,
            relative_path,
            ..
        } => {
            let meta = open_offline_meta(db_path)?;
            studiofs_vfs::list_scratch_orphans_offline(meta.as_ref(), relative_path.as_deref())
                .map_err(anyhow::Error::from)
        }
    }
}

fn execute_clean(
    target: &ScratchTarget,
    cache_dir: Option<&Path>,
    older_than_secs: Option<u64>,
) -> Result<ScratchCleanOutcome> {
    match target {
        ScratchTarget::Mounted {
            discovery,
            relative_path,
        } => {
            let client = LocalDiagnosticsClient::new(discovery.clone());
            let rel = relative_path.clone();
            crate::runtime_bridge::block_on(async move {
                client
                    .clean_scratch_entries(rel.as_deref(), older_than_secs)
                    .await
            })
        }
        ScratchTarget::Offline {
            meta_dir,
            db_path,
            relative_path,
        } => {
            let meta = open_offline_meta(db_path)?;
            let format_info = meta.get_format_info()?;
            let outcome = studiofs_vfs::clean_scratch_offline(
                meta.as_ref(),
                relative_path.as_deref(),
                older_than_secs,
                current_unix_secs(),
            )?;
            if !outcome.freed_payload_object_ids.is_empty() {
                let cache_path = local_cache_path(meta_dir, cache_dir, &format_info.uuid);
                let detection = detect_cache_backends(&cache_path).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to inspect local cache at '{}': {error}",
                        cache_path.display()
                    )
                })?;
                if detection.runtime_backend() == Some(LocalPayloadBackendKind::Current) {
                    let opened = open_local_payload_backend(&cache_path, &format_info.uuid)
                        .map_err(|error| {
                            anyhow::anyhow!(
                                "failed to open local cache backend at '{}': {error}",
                                cache_path.display()
                            )
                        })?;
                    opened
                        .backend
                        .remove_batch(&outcome.freed_payload_object_ids)
                        .map_err(|error| {
                            anyhow::anyhow!("failed to remove cleaned scratch payloads: {error}")
                        })?;
                }
            }
            Ok(outcome)
        }
    }
}

fn execute_promote(
    target: &ScratchTarget,
    cache_dir: Option<&Path>,
) -> Result<ScratchPromotePathOutcome> {
    match target {
        ScratchTarget::Mounted {
            discovery,
            relative_path,
        } => {
            let rel = relative_path.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "scratch promote path must name a file or directory inside the volume"
                )
            })?;
            let client = LocalDiagnosticsClient::new(discovery.clone());
            crate::runtime_bridge::block_on(async move { client.promote_scratch_path(&rel).await })
        }
        ScratchTarget::Offline {
            meta_dir,
            db_path,
            relative_path,
        } => {
            let rel = relative_path.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "scratch promote path must name a file or directory inside the volume"
                )
            })?;
            let meta = open_offline_meta(db_path)?;
            let format_info = meta.get_format_info()?;
            let (outcome, payload_objects) =
                studiofs_vfs::promote_scratch_path_offline(meta.as_ref(), rel)?;
            if !payload_objects.is_empty() {
                let cache_path = local_cache_path(meta_dir, cache_dir, &format_info.uuid);
                let detection = detect_cache_backends(&cache_path).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to inspect local cache at '{}': {error}",
                        cache_path.display()
                    )
                })?;
                if detection.runtime_backend() == Some(LocalPayloadBackendKind::Current)
                    && detection.index_exists
                {
                    let index = CacheIndex::open(&detection.layout.redb_index_path)?;
                    for object_id in payload_objects {
                        // `false` = object absent from the index; startup replay reconciles it.
                        index.mark_scratch_dirty(object_id)?;
                    }
                }
            }
            Ok(outcome)
        }
    }
}

fn execute_restore(
    target: &ScratchTarget,
    cache_dir: Option<&Path>,
    orphan: &str,
    to: Option<&str>,
) -> Result<ScratchRestoreOutcome> {
    match target {
        ScratchTarget::Mounted { discovery, .. } => {
            let client = LocalDiagnosticsClient::new(discovery.clone());
            let orphan = orphan.to_string();
            let to = to.map(str::to_string);
            crate::runtime_bridge::block_on(async move {
                client.restore_scratch_orphan(&orphan, to.as_deref()).await
            })
        }
        ScratchTarget::Offline {
            meta_dir, db_path, ..
        } => {
            let meta = open_offline_meta(db_path)?;
            if let Ok(format_info) = meta.get_format_info() {
                let cache_path = local_cache_path(meta_dir, cache_dir, &format_info.uuid);
                let _ = meta.set_scratch_cache_dir(cache_path);
            }
            studiofs_vfs::restore_scratch_orphan_offline(meta.as_ref(), orphan, to)
                .map_err(anyhow::Error::from)
        }
    }
}

fn format_scratch_list(entries: &[ScratchListEntry]) -> String {
    if entries.is_empty() {
        return "No scratch entries.".to_string();
    }
    let mut lines = Vec::with_capacity(entries.len());
    for entry in entries {
        let mut flags = Vec::new();
        if entry.is_conflict {
            flags.push("conflict");
        }
        if entry.no_longer_matches {
            flags.push("no-longer-matches");
        }
        if entry.has_open_handles {
            flags.push("open");
        }
        let suffix = if flags.is_empty() {
            String::new()
        } else {
            format!(" [{}]", flags.join(", "))
        };
        lines.push(format!(
            "{:<7} {:>10}  {}{}",
            entry.kind,
            format_bytes(entry.size),
            entry.relative_path,
            suffix
        ));
    }
    lines.join("\n")
}

fn format_orphan_list(orphans: &[ScratchOrphanSummary]) -> String {
    if orphans.is_empty() {
        return "No orphaned scratch entries.".to_string();
    }
    let mut lines = Vec::with_capacity(orphans.len());
    for orphan in orphans {
        let state = if orphan.materialized {
            "materialized"
        } else {
            "pending"
        };
        lines.push(format!(
            "{}  ({} entries, {}, {})",
            orphan.id,
            orphan.entry_count,
            format_bytes(orphan.total_bytes),
            state
        ));
    }
    lines.join("\n")
}

fn format_clean_outcome(outcome: &ScratchCleanOutcome) -> String {
    let mut summary = format!(
        "Cleaned {} scratch entries ({})",
        outcome.removed_entries,
        format_bytes(outcome.freed_bytes)
    );
    let mut skipped = Vec::new();
    if outcome.skipped_open_handles > 0 {
        skipped.push(format!("{} open", outcome.skipped_open_handles));
    }
    if outcome.skipped_newer > 0 {
        skipped.push(format!("{} newer", outcome.skipped_newer));
    }
    if !skipped.is_empty() {
        summary.push_str(&format!("; skipped {}", skipped.join(", ")));
    }
    summary
}

fn format_promote_outcome(outcome: &ScratchPromotePathOutcome) -> String {
    format!(
        "Promoted scratch path `{}` ({} inodes)",
        outcome.relative_path, outcome.promoted_inodes
    )
}

fn format_restore_outcome(outcome: &ScratchRestoreOutcome) -> String {
    format!(
        "Restored scratch orphan `{}` to `{}` (ino {})",
        outcome.orphan_id, outcome.restored_path, outcome.ino
    )
}

fn resolve_scratch_target(
    sessions: &[LocalDiagnosticsDiscovery],
    raw_path: Option<&str>,
    volume: Option<&str>,
    cwd: Option<&Path>,
    require_non_empty_path: bool,
    meta_dir_lookup: impl Fn(&str) -> Result<PathBuf>,
) -> Result<ScratchTarget> {
    let all_sessions: Vec<&LocalDiagnosticsDiscovery> = sessions.iter().collect();
    let rooted = raw_path.is_some_and(|raw| {
        let p = Path::new(raw);
        p.has_root() || matches!(p.components().next(), Some(Component::Prefix(_)))
    });

    if let Some(volume_name) = volume {
        let matching: Vec<&LocalDiagnosticsDiscovery> = sessions
            .iter()
            .filter(|session| session.volume == volume_name)
            .collect();
        if !matching.is_empty() {
            if let Some(raw) = raw_path {
                let path = Path::new(raw);
                if rooted {
                    let (session, relative) =
                        match_mounted_path(&all_sessions, path, None, require_non_empty_path)?
                            .filter(|(session, _)| session.volume == volume_name)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "path `{raw}` is outside the mounts for volume '{volume_name}'"
                                )
                            })?;
                    return Ok(ScratchTarget::Mounted {
                        discovery: session.clone(),
                        relative_path: relative,
                    });
                }
                if matching.len() == 1 {
                    let rel = path_to_volume_relative(path, require_non_empty_path)?;
                    return Ok(ScratchTarget::Mounted {
                        discovery: matching[0].clone(),
                        relative_path: rel,
                    });
                }
                bail!(
                    "volume '{volume_name}' is mounted at multiple mount points; pass a full path inside the target mount point"
                );
            }
            if matching.len() == 1 {
                return Ok(ScratchTarget::Mounted {
                    discovery: matching[0].clone(),
                    relative_path: None,
                });
            }
            bail!(
                "volume '{volume_name}' is mounted at multiple mount points; pass a full path inside the target mount point"
            );
        }

        // Offline fallback against redb when the volume is not currently mounted.
        if rooted {
            let raw = raw_path.unwrap_or_default();
            bail!(
                "volume '{volume_name}' is not mounted; pass a volume-relative path instead of `{raw}` for offline mode"
            );
        }
        let meta_dir = meta_dir_lookup(volume_name)?;
        let db_path = meta_dir.join(format!("{volume_name}.db"));
        if !db_path.exists() {
            bail!(
                "volume '{volume_name}' is not mounted and no local metadata database was found at '{}'",
                db_path.display()
            );
        }
        let relative_path = match raw_path {
            Some(raw) => path_to_volume_relative(Path::new(raw), require_non_empty_path)?,
            None => {
                if require_non_empty_path {
                    bail!("scratch promote path must name a file or directory inside the volume");
                }
                None
            }
        };
        return Ok(ScratchTarget::Offline {
            meta_dir,
            db_path,
            relative_path,
        });
    }

    if let Some(raw) = raw_path {
        let path = Path::new(raw);
        if let Some((session, rel_path)) =
            match_mounted_path(&all_sessions, path, cwd, require_non_empty_path)?
        {
            return Ok(ScratchTarget::Mounted {
                discovery: session.clone(),
                relative_path: rel_path,
            });
        }
        if rooted {
            bail!("path `{raw}` is outside the active StudioFS mounts");
        }
        match sessions {
            [] => bail!(
                "no active mounted StudioFS volume found for '{raw}'; pass `--volume <VOLUME_NAME>` for offline mode"
            ),
            [only] => Ok(ScratchTarget::Mounted {
                discovery: only.clone(),
                relative_path: path_to_volume_relative(path, require_non_empty_path)?,
            }),
            many => {
                let names: Vec<&str> = many.iter().map(|s| s.volume.as_str()).collect();
                bail!(
                    "multiple mounted StudioFS volumes are active ({}); pass `--volume <VOLUME_NAME>` or a full path inside a mount point",
                    names.join(", ")
                );
            }
        }
    } else {
        if require_non_empty_path {
            bail!("scratch promote path must name a file or directory inside the volume");
        }
        match sessions {
            [] => bail!(
                "no active mounted StudioFS volume found; pass `--volume <VOLUME_NAME>` for offline mode"
            ),
            [only] => Ok(ScratchTarget::Mounted {
                discovery: only.clone(),
                relative_path: None,
            }),
            many => {
                let names: Vec<&str> = many.iter().map(|s| s.volume.as_str()).collect();
                bail!(
                    "multiple mounted StudioFS volumes are active ({}); pass `--volume <VOLUME_NAME>`",
                    names.join(", ")
                );
            }
        }
    }
}

fn match_mounted_path<'a>(
    sessions: &[&'a LocalDiagnosticsDiscovery],
    raw_path: &Path,
    cwd: Option<&Path>,
    require_non_empty: bool,
) -> Result<Option<(&'a LocalDiagnosticsDiscovery, Option<String>)>> {
    let candidate = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else if let Some(dir) = cwd {
        dir.join(raw_path)
    } else {
        raw_path.to_path_buf()
    };

    let mut best = None;
    for session in sessions {
        let mount_path = Path::new(&session.mount_point);
        let drive_root;
        let mount_path = if cfg!(windows) && !mount_path.has_root() {
            drive_root = PathBuf::from(format!("{}\\", session.mount_point));
            drive_root.as_path()
        } else {
            mount_path
        };
        if let Some(rel) = strip_mount_prefix(mount_path, &candidate) {
            let score = mount_path.components().count();
            if best
                .as_ref()
                .is_none_or(|(_, best_score, _)| score > *best_score)
            {
                best = Some((*session, score, rel));
            }
        }
    }
    best.map(|(session, _, rel)| Ok((session, path_to_volume_relative(&rel, require_non_empty)?)))
        .transpose()
}

fn strip_mount_prefix(mount_path: &Path, candidate: &Path) -> Option<PathBuf> {
    if let Ok(stripped) = candidate.strip_prefix(mount_path) {
        return Some(stripped.to_path_buf());
    }
    let canon_mount = std::fs::canonicalize(mount_path).ok()?;
    // Resolve aliases in the parent without following the target entry itself.
    let canon_parent = std::fs::canonicalize(candidate.parent()?).ok()?;
    let canon_candidate = canon_parent.join(candidate.file_name()?);
    Some(
        canon_candidate
            .strip_prefix(canon_mount)
            .ok()?
            .to_path_buf(),
    )
}

fn path_to_volume_relative(stripped: &Path, require_non_empty: bool) -> Result<Option<String>> {
    let mut parts = Vec::new();
    for component in stripped.components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| anyhow::anyhow!("scratch path must be valid UTF-8"))?,
            ),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("invalid scratch path `{}`", stripped.display());
            }
        }
    }
    if parts.is_empty() {
        if require_non_empty {
            bail!("scratch promote path must name a file or directory inside the volume");
        }
        return Ok(None);
    }
    Ok(Some(parts.join("/")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::{
        DIAGNOSTICS_PROTOCOL_VERSION, LiveMountDiagnosticsSource, LiveMountRuntimeFingerprint,
        LocalDiagnosticsEndpoint, LocalDiagnosticsHost,
    };
    use std::sync::Arc;
    use studiofs_cache::local::LocalPayloadPut;
    use studiofs_meta::{
        ChunkId, FormatInfo, FormatInfoDataStore, InodeAttr, MetaEngineCore, MetaFormat, MetaFsOps,
        PayloadRef, ROOT_INO, RedbMetaEngine, SliceId, SliceOrder, SliceRange, SliceRecord,
    };
    use studiofs_vfs::OpenFlags;
    use studiofs_vfs::contract_harness::ContractVfsFixture;

    const MOUNT_A: &str = if cfg!(windows) { "Q:" } else { "/mnt/a" };
    const MOUNT_B: &str = if cfg!(windows) { "R:" } else { "/mnt/b" };
    const ROOT_A: &str = if cfg!(windows) { "Q:/" } else { "/mnt/a" };

    fn test_session(volume: &str, mount_point: &str) -> LocalDiagnosticsDiscovery {
        #[cfg(unix)]
        let endpoint = LocalDiagnosticsEndpoint::UnixSocket {
            path: format!("/tmp/{volume}.sock"),
        };
        #[cfg(windows)]
        let endpoint = LocalDiagnosticsEndpoint::WindowsNamedPipe {
            name: format!(r"\\.\pipe\{volume}"),
        };
        LocalDiagnosticsDiscovery {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            session_id: format!("sess-{volume}"),
            volume: volume.to_string(),
            volume_uuid: format!("uuid-{volume}"),
            bridge: "test".to_string(),
            mount_point: mount_point.to_string(),
            pid: 42,
            cache_path: "/tmp/cache".to_string(),
            read_only: false,
            started_unix_ms: 0,
            endpoint,
            auth_token: "token".to_string(),
        }
    }

    fn resolve_promote_target(
        sessions: &[LocalDiagnosticsDiscovery],
        raw_path: &str,
        volume: Option<&str>,
        cwd: Option<&Path>,
    ) -> Result<(LocalDiagnosticsDiscovery, String)> {
        let missing_dir = |_: &str| Ok(PathBuf::from("/nonexistent-studiofs-meta-dir"));
        match resolve_scratch_target(sessions, Some(raw_path), volume, cwd, true, missing_dir)? {
            ScratchTarget::Mounted {
                discovery,
                relative_path,
            } => Ok((discovery, relative_path.unwrap())),
            ScratchTarget::Offline { .. } => bail!("expected mounted target"),
        }
    }

    #[test]
    fn resolve_promote_target_matches_full_mount_path() {
        let sessions = vec![
            test_session("vol-a", MOUNT_A),
            test_session("vol-b", MOUNT_B),
        ];
        let path = format!("{MOUNT_B}/renders/tmp");
        let (session, rel) = resolve_promote_target(&sessions, &path, None, None).unwrap();
        assert_eq!(session.volume, "vol-b");
        assert_eq!(rel, "renders/tmp");
    }

    #[test]
    fn resolve_promote_target_uses_explicit_volume_with_relative_path() {
        let sessions = vec![
            test_session("vol-a", MOUNT_A),
            test_session("vol-b", MOUNT_B),
        ];
        let (session, rel) =
            resolve_promote_target(&sessions, "renders/tmp", Some("vol-a"), None).unwrap();
        assert_eq!(session.volume, "vol-a");
        assert_eq!(rel, "renders/tmp");
    }

    #[test]
    fn resolve_promote_target_uses_single_mounted_volume_for_relative_path() {
        let sessions = vec![test_session("vol-a", MOUNT_A)];
        let (session, rel) = resolve_promote_target(&sessions, "renders/tmp", None, None).unwrap();
        assert_eq!(session.volume, "vol-a");
        assert_eq!(rel, "renders/tmp");
    }

    #[test]
    fn resolve_promote_target_rejects_ambiguous_relative_path_with_multiple_mounts() {
        let sessions = vec![
            test_session("vol-a", MOUNT_A),
            test_session("vol-b", MOUNT_B),
        ];
        let error = resolve_promote_target(&sessions, "renders/tmp", None, None).unwrap_err();
        assert!(error.to_string().contains("--volume"));
    }

    #[test]
    fn resolve_promote_target_preserves_volume_relative_path_inside_mount() {
        let sessions = vec![test_session("vol-a", MOUNT_A)];
        let (_, relative) = resolve_promote_target(
            &sessions,
            "renders/tmp ",
            Some("vol-a"),
            Some(&Path::new(ROOT_A).join("nested")),
        )
        .unwrap();
        assert_eq!(relative, "renders/tmp ");
    }

    #[test]
    fn resolve_promote_target_rejects_an_absolute_path_outside_the_mount() {
        let sessions = vec![test_session("vol-a", MOUNT_A)];
        let path = if cfg!(windows) {
            "R:/elsewhere/tmp"
        } else {
            "/elsewhere/tmp"
        };
        assert!(resolve_promote_target(&sessions, path, Some("vol-a"), None).is_err());
        assert!(resolve_promote_target(&sessions, path, None, None).is_err());
    }

    #[test]
    fn resolve_promote_target_rejects_mount_root() {
        let sessions = vec![test_session("vol-a", MOUNT_A)];
        let error = resolve_promote_target(&sessions, ROOT_A, None, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must name a file or directory inside the volume")
        );
    }

    #[test]
    fn parse_duration_secs_handles_units_and_rejects_invalid() {
        assert_eq!(parse_duration_secs("30s").unwrap(), 30);
        assert_eq!(parse_duration_secs("45").unwrap(), 45);
        assert_eq!(parse_duration_secs("15m").unwrap(), 900);
        assert_eq!(parse_duration_secs("24h").unwrap(), 86_400);
        assert_eq!(parse_duration_secs("7d").unwrap(), 604_800);
        assert_eq!(parse_duration_secs("2w").unwrap(), 1_209_600);
        assert!(parse_duration_secs("").is_err());
        assert!(parse_duration_secs("10y").is_err());
        assert!(parse_duration_secs("abc").is_err());
    }

    #[test]
    fn scratch_cli_parses_all_subcommands() {
        let list = crate::cli::parse_for_test(&[
            "scratch",
            "list",
            "renders",
            "--orphaned",
            "--volume",
            "vol",
        ])
        .unwrap();
        assert!(matches!(
            list.command,
            crate::cli::Commands::Scratch(ScratchArgs {
                command: ScratchCommand::List(ListArgs {
                    path: Some(_),
                    orphaned: true,
                    volume: Some(_),
                    ..
                })
            })
        ));

        let clean = crate::cli::parse_for_test(&[
            "scratch",
            "clean",
            "--older-than",
            "7d",
            "renders",
            "--volume",
            "vol",
        ])
        .unwrap();
        assert!(matches!(
            clean.command,
            crate::cli::Commands::Scratch(ScratchArgs {
                command: ScratchCommand::Clean(CleanArgs {
                    older_than: Some(_),
                    path: Some(_),
                    volume: Some(_),
                    ..
                })
            })
        ));

        let promote =
            crate::cli::parse_for_test(&["scratch", "promote", "--volume", "vol", "renders/a.tmp"])
                .unwrap();
        assert!(matches!(
            promote.command,
            crate::cli::Commands::Scratch(ScratchArgs {
                command: ScratchCommand::Promote(PromoteArgs { .. })
            })
        ));

        let restore = crate::cli::parse_for_test(&[
            "scratch",
            "restore",
            "--volume",
            "vol",
            "--to",
            "recovered/a.tmp",
            "1700000000000/old/a.tmp",
        ])
        .unwrap();
        assert!(matches!(
            restore.command,
            crate::cli::Commands::Scratch(ScratchArgs {
                command: ScratchCommand::Restore(RestoreArgs { to: Some(_), .. })
            })
        ));
    }

    #[test]
    fn offline_scratch_commands_work_end_to_end() {
        let tmp = studiofs_test_support::workspace_target_tempdir("scratch-cli-offline-");
        let meta_dir = tmp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).unwrap();
        let db_path = meta_dir.join("offline-vol.db");
        let format_info = FormatInfo::new(
            "offline-vol".to_string(),
            uuid::Uuid::new_v4().to_string(),
            FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "bucket".to_string(),
                access_key: "key".to_string(),
                secret_key: "secret".to_string(),
                region: "us-east-1".to_string(),
            },
            4,
        )
        .unwrap();
        let cache_base = tmp.path().join("cache");
        let cache_path = cache_base.join(&format_info.uuid);

        // Seed offline metadata + local scratch payload objects + one orphan.
        {
            let meta = RedbMetaEngine::open(&db_path).unwrap();
            meta.init_root(0, 0).unwrap();
            meta.set_format_info(&format_info).unwrap();
            meta.set_scratch_cache_dir(cache_path.clone()).unwrap();

            let opened = open_local_payload_backend(&cache_path, &format_info.uuid).unwrap();
            opened
                .backend
                .put_scratch_batch(&[
                    LocalPayloadPut {
                        object_id: 301,
                        data: bytes::Bytes::from_static(b"clean-me-data"),
                        evictable: false,
                    },
                    LocalPayloadPut {
                        object_id: 302,
                        data: bytes::Bytes::from_static(b"promote-me-data"),
                        evictable: false,
                    },
                ])
                .unwrap();

            let mut old_file = InodeAttr::new_file(100, ROOT_INO, 0, 0, 0o644);
            old_file.size = 13;
            old_file.mtime = 100;
            meta.create_scratch_entry(ROOT_INO, b"._old.tmp", &old_file)
                .unwrap();
            meta.commit_scratch_overlay(
                old_file.ino,
                &[(
                    ChunkId(0),
                    vec![SliceRecord::new(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange::new(0, 13).unwrap(),
                        PayloadRef::new_partial(301, 13),
                    )],
                )],
            )
            .unwrap();
            // Restore old mtime after commit_scratch_overlay touched it.
            meta.set_scratch_attr(&old_file).unwrap();

            let mut promote_file = InodeAttr::new_file(101, ROOT_INO, 0, 0, 0o644);
            promote_file.size = 15;
            promote_file.mtime = current_unix_secs();
            meta.create_scratch_entry(ROOT_INO, b"keep.txt", &promote_file)
                .unwrap();
            meta.commit_scratch_overlay(
                promote_file.ino,
                &[(
                    ChunkId(0),
                    vec![SliceRecord::new(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange::new(0, 15).unwrap(),
                        PayloadRef::new_partial(302, 15),
                    )],
                )],
            )
            .unwrap();

            // Create a tracked parent with a scratch child, then remove the parent via snapshot import
            // to produce a real orphan record.
            let dir = meta.create_dir(ROOT_INO, b"vanished", 0, 0, 0o755).unwrap();
            let mut snap_without_dir = meta.export_snapshot().unwrap();
            snap_without_dir.inodes.retain(|(ino, _)| *ino != dir.ino);
            snap_without_dir.dentries.clear();
            let mut orphan_child = InodeAttr::new_file(102, dir.ino, 0, 0, 0o644);
            orphan_child.size = 4;
            meta.create_scratch_entry(dir.ino, b"lost.tmp", &orphan_child)
                .unwrap();
            meta.import_snapshot(&snap_without_dir).unwrap();
        }

        let target_root = ScratchTarget::Offline {
            meta_dir: meta_dir.clone(),
            db_path: db_path.clone(),
            relative_path: None,
        };

        // 1. Offline `list`
        let entries = execute_list(&target_root).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|e| e.relative_path == "._old.tmp" && !e.no_longer_matches)
        );
        assert!(
            entries
                .iter()
                .any(|e| e.relative_path == "keep.txt" && e.no_longer_matches)
        );

        // 2. Offline `list --orphaned`
        let orphans = execute_list_orphans(&target_root).unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].root_old_path, "vanished/lost.tmp");

        // 3. Offline `clean --older-than 1h` removes `old.tmp` and purges payload 301, keeps `keep.txt`.
        let cleaned = execute_clean(&target_root, Some(&cache_base), Some(3600)).unwrap();
        assert_eq!(cleaned.removed_entries, 1);
        assert_eq!(cleaned.skipped_newer, 1);
        let detection = detect_cache_backends(&cache_path).unwrap();
        let index = CacheIndex::open(&detection.layout.redb_index_path).unwrap();
        assert!(!index.scratch_object_ids().unwrap().contains(&301));
        drop(index);

        // 4. Offline `promote keep.txt` promotes to tracked and transitions payload 302 to dirty.
        let promote_target = ScratchTarget::Offline {
            meta_dir: meta_dir.clone(),
            db_path: db_path.clone(),
            relative_path: Some("keep.txt".to_string()),
        };
        let promoted = execute_promote(&promote_target, Some(&cache_base)).unwrap();
        assert_eq!(promoted.promoted_inodes, 1);
        let index = CacheIndex::open(&detection.layout.redb_index_path).unwrap();
        assert!(!index.scratch_object_ids().unwrap().contains(&302));
        assert!(index.stats().unwrap().dirty_bytes >= 15);
        drop(index);

        // 5. Offline `restore` restores the orphan back into scratch.
        let restored =
            execute_restore(&target_root, Some(&cache_base), &orphans[0].id, None).unwrap();
        assert_eq!(restored.restored_path, "vanished/lost.tmp");
        assert!(execute_list_orphans(&target_root).unwrap().is_empty());
        let after_list = execute_list(&target_root).unwrap();
        assert!(
            after_list
                .iter()
                .any(|e| e.relative_path == "vanished/lost.tmp")
        );
    }

    #[test]
    fn mounted_scratch_commands_work_over_control_api() {
        let fixture = ContractVfsFixture::new("scratch-cli-mounted").unwrap();
        let vfs = fixture.vfs();
        let meta = fixture.core().meta();
        let tmp = studiofs_test_support::workspace_target_tempdir("scratch-cli-mounted-diag-");

        let open_file = vfs
            .store_scratch_entry(b"open.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        let closed_file = vfs
            .store_scratch_entry(b"closed.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        let promote_file = vfs
            .store_scratch_entry(
                b"promote.tmp",
                InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644),
            )
            .unwrap();
        let fh = vfs
            .open_file(open_file.ino, OpenFlags::RDWR, false)
            .unwrap();

        // Create an orphan via snapshot import.
        let dir = meta.create_dir(ROOT_INO, b"gone", 0, 0, 0o755).unwrap();
        let mut snap_without_dir = meta.export_snapshot().unwrap();
        snap_without_dir.inodes.retain(|(ino, _)| *ino != dir.ino);
        snap_without_dir.dentries.clear();
        let orphan_child = InodeAttr::new_file(500, dir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(dir.ino, b"orph.tmp", &orphan_child)
            .unwrap();
        meta.import_snapshot(&snap_without_dir).unwrap();

        let source = Arc::new(LiveMountDiagnosticsSource::new(
            fixture.core().format_info(),
            MOUNT_A,
            fixture.cache_dir(),
            std::process::id(),
            false,
            LiveMountRuntimeFingerprint {
                bridge: "test".to_string(),
                cache_size: None,
                endpoint_override: None,
                snapshot_id: None,
                vfs_config: serde_json::json!({}),
            },
            vfs.clone(),
        ));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (host, discovery) = LocalDiagnosticsHost::start_for_test(
            rt.handle(),
            tmp.path(),
            "scratch-cli-mounted",
            source,
        )
        .expect("start diagnostics host");
        let mounted_root = ScratchTarget::Mounted {
            discovery: discovery.clone(),
            relative_path: None,
        };

        // 1. Mounted `list`
        let listed = execute_list(&mounted_root).unwrap();
        assert_eq!(listed.len(), 3);
        assert!(
            listed
                .iter()
                .any(|e| e.relative_path == "open.tmp" && e.has_open_handles)
        );

        // 2. Mounted `list --orphaned`
        let orphans = execute_list_orphans(&mounted_root).unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].root_old_path, "gone/orph.tmp");

        // 3. Mounted `promote`
        let promote_target = ScratchTarget::Mounted {
            discovery: discovery.clone(),
            relative_path: Some("promote.tmp".to_string()),
        };
        let promoted = execute_promote(&promote_target, None).unwrap();
        assert_eq!(promoted.promoted_inodes, 1);
        assert!(!vfs.is_scratch_inode(promote_file.ino).unwrap());

        // 4. Mounted `clean` skips open handle (`open.tmp`), removes `closed.tmp`.
        let cleaned = execute_clean(&mounted_root, None, None).unwrap();
        assert_eq!(cleaned.removed_entries, 1);
        assert_eq!(cleaned.skipped_open_handles, 1);
        assert!(vfs.is_scratch_inode(open_file.ino).unwrap());
        assert!(!vfs.is_scratch_inode(closed_file.ino).unwrap());

        // 5. Mounted `restore`
        let restored = execute_restore(&mounted_root, None, &orphans[0].id, None).unwrap();
        assert_eq!(restored.restored_path, "gone/orph.tmp");
        assert!(execute_list_orphans(&mounted_root).unwrap().is_empty());

        rt.block_on(async {
            let _ = vfs.release_fh(fh).await;
            host.stop().await;
        });
    }
}
