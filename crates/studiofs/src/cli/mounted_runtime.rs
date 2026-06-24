use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
struct ActiveMount {
    pid: u32,
    mount_point: PathBuf,
    volume: String,
    cache_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub(crate) struct MountedRuntimeInfo {
    pub(crate) pid: u32,
    pub(crate) volume: String,
    pub(crate) mount_point: PathBuf,
    pub(crate) cache_path: Option<PathBuf>,
}

fn normalize_path_for_compare(path: &Path) -> Option<String> {
    let raw = path.to_str()?;
    #[cfg(windows)]
    {
        Some(
            super::utils::normalize_mount_point(raw)
                .replace('/', "\\")
                .trim_end_matches('\\')
                .to_lowercase(),
        )
    }
    #[cfg(not(windows))]
    {
        Some(raw.trim_end_matches('/').to_string())
    }
}

fn cleanup_stale_mounts() -> Result<()> {
    let mounts_dir = super::utils::get_mounts_dir()?;
    cleanup_stale_mounts_in(
        &mounts_dir,
        super::utils::mount_is_active,
        super::utils::remove_pid_file,
    )
}

fn cleanup_stale_mounts_in<F, R>(
    mounts_dir: &Path,
    is_mount_active: F,
    mut remove_pid_file: R,
) -> Result<()>
where
    F: Fn(&str) -> Result<bool>,
    R: FnMut(&str) -> Result<()>,
{
    for entry in std::fs::read_dir(mounts_dir)
        .with_context(|| format!("read mounts dir {}", mounts_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_json::Value =
            serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
        let Some(_pid) = parsed.get("pid").and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let Some(mount_point) = parsed
            .get("mount_point")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if !is_mount_active(mount_point)? {
            let _ = remove_pid_file(mount_point);
        }
    }
    Ok(())
}

fn is_path_under_mount(target: &Path, mount_point: &Path) -> bool {
    let Some(target_norm) = normalize_path_for_compare(target) else {
        return false;
    };
    let Some(mount_norm) = normalize_path_for_compare(mount_point) else {
        return false;
    };
    target_norm == mount_norm
        || target_norm
            .strip_prefix(&mount_norm)
            .is_some_and(|tail| tail.starts_with('\\') || tail.starts_with('/'))
}

fn active_mounts() -> Result<Vec<ActiveMount>> {
    let mounts_dir = super::utils::get_mounts_dir()?;
    active_mounts_in(&mounts_dir, super::utils::mount_is_active)
}

fn active_mounts_in<F>(mounts_dir: &Path, is_mount_active: F) -> Result<Vec<ActiveMount>>
where
    F: Fn(&str) -> Result<bool>,
{
    let mut mounts = Vec::new();
    for entry in std::fs::read_dir(mounts_dir)
        .with_context(|| format!("read mounts dir {}", mounts_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_json::Value =
            serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
        let Some(pid) = parsed.get("pid").and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let Some(mount_point) = parsed
            .get("mount_point")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let volume = parsed
            .get("volume")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let cache_path = parsed
            .get("cache_path")
            .and_then(serde_json::Value::as_str)
            .map(PathBuf::from);
        if !is_mount_active(mount_point)? {
            continue;
        }
        mounts.push(ActiveMount {
            pid: pid as u32,
            mount_point: PathBuf::from(mount_point),
            volume: volume.to_string(),
            cache_path,
        });
    }
    Ok(mounts)
}

pub(crate) fn resolve_mounted_runtime(target: &Path) -> Result<Option<MountedRuntimeInfo>> {
    cleanup_stale_mounts()?;
    let mut best: Option<ActiveMount> = None;
    for mount in active_mounts()? {
        if is_path_under_mount(target, &mount.mount_point) {
            let keep = best.as_ref().is_none_or(|existing| {
                mount.mount_point.components().count() > existing.mount_point.components().count()
            });
            if keep {
                best = Some(mount);
            }
        }
    }

    Ok(best.map(|active| MountedRuntimeInfo {
        pid: active.pid,
        volume: active.volume,
        mount_point: active.mount_point,
        cache_path: active.cache_path,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_mount_pid_json(
        dir: &Path,
        file_name: &str,
        pid: u64,
        mount_point: &str,
        volume: &str,
        cache_path: Option<&str>,
    ) {
        let mut payload = serde_json::json!({
            "pid": pid,
            "mount_point": mount_point,
            "volume": volume,
        });
        if let Some(cache_path) = cache_path {
            payload["cache_path"] = serde_json::Value::String(cache_path.to_string());
        }
        std::fs::write(
            dir.join(file_name),
            serde_json::to_string_pretty(&payload).expect("serialize pid json"),
        )
        .expect("write pid json");
    }

    #[test]
    fn mount_path_detection_handles_nested_paths() {
        let mount = PathBuf::from("/mnt/studiofs");
        let target = PathBuf::from("/mnt/studiofs/cache-bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[test]
    fn mount_path_detection_rejects_sibling_paths() {
        let mount = PathBuf::from("/mnt/studiofs");
        let target = PathBuf::from("/mnt/studiofs-other/cache-bench");
        assert!(!is_path_under_mount(&target, &mount));
    }

    #[cfg(windows)]
    #[test]
    fn mount_path_detection_normalizes_extended_drive_prefix() {
        let mount = PathBuf::from(r"C:\StudioFS");
        let target = PathBuf::from(r"\\?\C:\StudioFS\bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[cfg(windows)]
    #[test]
    fn mount_path_detection_normalizes_unc_prefix() {
        let mount = PathBuf::from(r"\\server\share\studiofs");
        let target = PathBuf::from(r"\\?\UNC\server\share\studiofs\bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[test]
    fn cleanup_stale_mounts_removes_only_dead_process_entries() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        write_mount_pid_json(
            temp.path(),
            "dead.json",
            41,
            "/mnt/stale",
            "stale-vol",
            None,
        );
        write_mount_pid_json(temp.path(), "live.json", 42, "/mnt/live", "live-vol", None);
        std::fs::write(
            temp.path().join("missing-pid.json"),
            serde_json::json!({
                "mount_point": "/mnt/ignored",
                "volume": "ignored-vol",
            })
            .to_string(),
        )
        .unwrap();

        let removed = std::cell::RefCell::new(Vec::new());
        cleanup_stale_mounts_in(
            temp.path(),
            |mount_point| Ok(mount_point == "/mnt/live"),
            |mount_point| {
                removed.borrow_mut().push(mount_point.to_string());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(removed.into_inner(), vec!["/mnt/stale".to_string()]);
    }

    #[test]
    fn active_mounts_filters_dead_processes_and_preserves_runtime_metadata() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        write_mount_pid_json(
            temp.path(),
            "live.json",
            77,
            "/mnt/live",
            "live-vol",
            Some("/cache/live"),
        );
        write_mount_pid_json(
            temp.path(),
            "dead.json",
            78,
            "/mnt/dead",
            "dead-vol",
            Some("/cache/dead"),
        );

        let mounts =
            active_mounts_in(temp.path(), |mount_point| Ok(mount_point == "/mnt/live")).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].pid, 77);
        assert_eq!(mounts[0].mount_point, PathBuf::from("/mnt/live"));
        assert_eq!(mounts[0].volume, "live-vol");
        assert_eq!(mounts[0].cache_path, Some(PathBuf::from("/cache/live")));
    }
}
