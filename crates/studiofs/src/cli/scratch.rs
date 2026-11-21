//! `studiofs scratch` — scratch-layer inspection and promotion commands.

use crate::diagnostics::{
    LocalDiagnosticsClient, LocalDiagnosticsDiscovery, discover_local_sessions,
};
use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use std::path::{Component, Path, PathBuf};
use studiofs_vfs::ScratchPromotePathOutcome;

#[derive(Debug, Args)]
#[command(about = "Manage local-only scratch-layer entries")]
#[command(after_help = "\
EXAMPLES:
  studiofs scratch promote /mnt/studio/renders/tmp
  studiofs scratch promote --volume myvolume renders/tmp
")]
pub(crate) struct ScratchArgs {
    #[command(subcommand)]
    command: ScratchCommand,
}

#[derive(Debug, Subcommand)]
enum ScratchCommand {
    /// Promote a scratch file or directory into tracked storage through the mount control API
    Promote(PromoteArgs),
}

#[derive(Debug, Args)]
struct PromoteArgs {
    /// Mounted filesystem path or volume-relative path to promote
    path: String,

    /// Target mounted volume name (optional when <PATH> is under a mount point or one volume is mounted)
    #[arg(long, value_name = "VOLUME_NAME")]
    volume: Option<String>,
}

impl ScratchArgs {
    pub(crate) fn dispatch(self) -> Result<()> {
        match self.command {
            ScratchCommand::Promote(args) => run_promote(args),
        }
    }
}

fn run_promote(args: PromoteArgs) -> Result<()> {
    let sessions = discover_local_sessions()?;
    let cwd = std::env::current_dir().ok();
    let (discovery, relative_path) = resolve_promote_target(
        &sessions,
        &args.path,
        args.volume.as_deref(),
        cwd.as_deref(),
    )?;
    let client = LocalDiagnosticsClient::new(discovery);
    let outcome = crate::runtime_bridge::block_on(async move {
        client.promote_scratch_path(&relative_path).await
    })?;
    println!("{}", format_promote_outcome(&outcome));
    Ok(())
}

fn format_promote_outcome(outcome: &ScratchPromotePathOutcome) -> String {
    let mut parts = vec![format!(
        "Promoted scratch path `{}` ({} promoted",
        outcome.relative_path, outcome.promoted_inodes
    )];
    if outcome.merged_directories > 0 {
        parts.push(format!(", {} merged", outcome.merged_directories));
    }
    if outcome.conflict_renames > 0 {
        parts.push(format!(", {} conflict-renamed", outcome.conflict_renames));
    }
    parts.push(")".to_string());
    parts.concat()
}

fn resolve_promote_target(
    sessions: &[LocalDiagnosticsDiscovery],
    raw_path: &str,
    volume: Option<&str>,
    cwd: Option<&Path>,
) -> Result<(LocalDiagnosticsDiscovery, String)> {
    if let Some(volume_name) = volume {
        let matching: Vec<&LocalDiagnosticsDiscovery> = sessions
            .iter()
            .filter(|session| session.volume == volume_name)
            .collect();
        if matching.is_empty() {
            bail!(
                "volume '{volume_name}' is not currently mounted; mount it before running `studiofs scratch promote`"
            );
        }
        if let Some((session, rel_path)) = match_mounted_path(&matching, Path::new(raw_path), cwd)?
        {
            return Ok((session.clone(), rel_path));
        }
        if matching.len() == 1 {
            return Ok((matching[0].clone(), normalize_relative_path(raw_path)?));
        }
        bail!(
            "volume '{volume_name}' is mounted at multiple mount points; pass a full path inside the target mount point"
        );
    }

    let all_sessions: Vec<&LocalDiagnosticsDiscovery> = sessions.iter().collect();
    if let Some((session, rel_path)) = match_mounted_path(&all_sessions, Path::new(raw_path), cwd)?
    {
        return Ok((session.clone(), rel_path));
    }

    match sessions {
        [] => bail!("no active mounted StudioFS volume found for '{raw_path}'"),
        [only] => Ok((only.clone(), normalize_relative_path(raw_path)?)),
        many => {
            let names: Vec<&str> = many.iter().map(|s| s.volume.as_str()).collect();
            bail!(
                "multiple mounted StudioFS volumes are active ({}); pass `--volume <VOLUME_NAME>` or a full path inside a mount point",
                names.join(", ")
            );
        }
    }
}

fn match_mounted_path<'a>(
    sessions: &[&'a LocalDiagnosticsDiscovery],
    raw_path: &Path,
    cwd: Option<&Path>,
) -> Result<Option<(&'a LocalDiagnosticsDiscovery, String)>> {
    let candidate = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else if let Some(dir) = cwd {
        dir.join(raw_path)
    } else {
        raw_path.to_path_buf()
    };

    let mut best: Option<(&'a LocalDiagnosticsDiscovery, usize, String)> = None;
    for session in sessions {
        let mount_path = Path::new(&session.mount_point);
        if let Some(rel) = strip_mount_prefix(mount_path, &candidate)? {
            let score = session.mount_point.len();
            if best
                .as_ref()
                .is_none_or(|(_, best_score, _)| score > *best_score)
            {
                best = Some((*session, score, rel));
            }
        }
    }
    Ok(best.map(|(session, _, rel)| (session, rel)))
}

fn strip_mount_prefix(mount_path: &Path, candidate: &Path) -> Result<Option<String>> {
    if let Ok(stripped) = candidate.strip_prefix(mount_path) {
        return Ok(Some(path_to_volume_relative(stripped)?));
    }
    if let (Ok(canon_mount), Some(canon_candidate)) = (
        std::fs::canonicalize(mount_path),
        canonicalize_existing_or_parent(candidate),
    ) && let Ok(stripped) = canon_candidate.strip_prefix(&canon_mount)
    {
        return Ok(Some(path_to_volume_relative(stripped)?));
    }
    Ok(None)
}

fn canonicalize_existing_or_parent(path: &Path) -> Option<PathBuf> {
    if let Ok(canon) = std::fs::canonicalize(path) {
        return Some(canon);
    }
    let file_name = path.file_name()?;
    let parent = path.parent()?;
    let canon_parent = std::fs::canonicalize(parent).ok()?;
    Some(canon_parent.join(file_name))
}

fn path_to_volume_relative(stripped: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in stripped.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("invalid scratch promote path `{}`", stripped.display());
            }
        }
    }
    if parts.is_empty() {
        bail!("scratch promote path must name a file or directory inside the volume");
    }
    Ok(parts.join("/"))
}

fn normalize_relative_path(raw: &str) -> Result<String> {
    let normalized_sep = if cfg!(windows) {
        raw.replace('\\', "/")
    } else {
        raw.to_string()
    };
    let trimmed = normalized_sep.trim().trim_matches('/');
    if trimmed.is_empty() {
        bail!("scratch promote path must name a file or directory inside the volume");
    }
    let mut parts = Vec::new();
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            bail!("invalid scratch promote path `{raw}`");
        }
        parts.push(segment);
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::{DIAGNOSTICS_PROTOCOL_VERSION, LocalDiagnosticsEndpoint};

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

    #[test]
    fn resolve_promote_target_matches_full_mount_path() {
        let sessions = vec![
            test_session("vol-a", "/mnt/a"),
            test_session("vol-b", "/mnt/b"),
        ];
        let (session, rel) =
            resolve_promote_target(&sessions, "/mnt/b/renders/tmp", None, None).unwrap();
        assert_eq!(session.volume, "vol-b");
        assert_eq!(rel, "renders/tmp");
    }

    #[test]
    fn resolve_promote_target_uses_explicit_volume_with_relative_path() {
        let sessions = vec![
            test_session("vol-a", "/mnt/a"),
            test_session("vol-b", "/mnt/b"),
        ];
        let (session, rel) =
            resolve_promote_target(&sessions, "renders/tmp", Some("vol-a"), None).unwrap();
        assert_eq!(session.volume, "vol-a");
        assert_eq!(rel, "renders/tmp");
    }

    #[test]
    fn resolve_promote_target_uses_single_mounted_volume_for_relative_path() {
        let sessions = vec![test_session("vol-a", "/mnt/a")];
        let (session, rel) = resolve_promote_target(&sessions, "renders/tmp", None, None).unwrap();
        assert_eq!(session.volume, "vol-a");
        assert_eq!(rel, "renders/tmp");
    }

    #[test]
    fn resolve_promote_target_rejects_ambiguous_relative_path_with_multiple_mounts() {
        let sessions = vec![
            test_session("vol-a", "/mnt/a"),
            test_session("vol-b", "/mnt/b"),
        ];
        let error = resolve_promote_target(&sessions, "renders/tmp", None, None).unwrap_err();
        assert!(error.to_string().contains("--volume"));
    }

    #[test]
    fn resolve_promote_target_rejects_mount_root() {
        let sessions = vec![test_session("vol-a", "/mnt/a")];
        let error = resolve_promote_target(&sessions, "/mnt/a", None, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must name a file or directory inside the volume")
        );
    }
}
