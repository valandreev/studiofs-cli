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
    /// Absolute mounted path or volume-relative path to promote
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
    format!(
        "Promoted scratch path `{}` ({} inodes)",
        outcome.relative_path, outcome.promoted_inodes
    )
}

fn resolve_promote_target(
    sessions: &[LocalDiagnosticsDiscovery],
    raw_path: &str,
    volume: Option<&str>,
    cwd: Option<&Path>,
) -> Result<(LocalDiagnosticsDiscovery, String)> {
    let path = Path::new(raw_path);
    let rooted = path.has_root() || matches!(path.components().next(), Some(Component::Prefix(_)));
    let all_sessions: Vec<&LocalDiagnosticsDiscovery> = sessions.iter().collect();
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
        if rooted {
            return match_mounted_path(&all_sessions, path, None)?
                .filter(|(session, _)| session.volume == volume_name)
                .map(|(session, relative)| (session.clone(), relative))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "path `{raw_path}` is outside the mounts for volume '{volume_name}'"
                    )
                });
        }
        if matching.len() == 1 {
            return Ok((matching[0].clone(), path_to_volume_relative(path)?));
        }
        bail!(
            "volume '{volume_name}' is mounted at multiple mount points; pass a full path inside the target mount point"
        );
    }

    if let Some((session, rel_path)) = match_mounted_path(&all_sessions, path, cwd)? {
        return Ok((session.clone(), rel_path));
    }
    if rooted {
        bail!("path `{raw_path}` is outside the active StudioFS mounts");
    }

    match sessions {
        [] => bail!("no active mounted StudioFS volume found for '{raw_path}'"),
        [only] => Ok((only.clone(), path_to_volume_relative(path)?)),
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
    best.map(|(session, _, rel)| Ok((session, path_to_volume_relative(&rel)?)))
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

fn path_to_volume_relative(stripped: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in stripped.components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| anyhow::anyhow!("scratch promote path must be valid UTF-8"))?,
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::{DIAGNOSTICS_PROTOCOL_VERSION, LocalDiagnosticsEndpoint};

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
}
