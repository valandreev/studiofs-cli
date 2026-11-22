//! Mounted and offline scratch-layer tooling operations: status inspection,
//! listing, cleanup, offline promotion, and orphan restoration.

use super::file_session::FileSessionOperation;
use super::metrics::{
    ScratchCleanOutcome, ScratchConflictSummary, ScratchListEntry, ScratchOrphanSummary,
    ScratchPromoteOverLimitEntry, ScratchPromotePathOutcome, ScratchRestoreOutcome,
    ScratchRuleFileWarning, VfsScratchStatusSnapshot,
};
use super::scratch_auto_promote::{
    ScratchPromoteRevisit, ScratchPromoteSubtreeWalk, collect_scratch_promote_subtree,
    find_over_limit_scratch_directories, format_relative_path, join_relative_path,
    load_compiled_scratch_rules_from_meta, parse_volume_relative_components,
};
use super::{StudioVfs, VfsError, VfsResult};
use crate::scratch_rules::{ScratchClass, ScratchRules};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use studiofs_meta::schema::ROOT_INO;
use studiofs_meta::{
    FileType, Ino, InodeAttr, MetaEngine, MetaEngineCore, MetaError, MetaNamespace,
};

const MAX_STATUS_SAMPLE_PATHS: usize = 25;

#[derive(Debug, Clone)]
struct WalkedScratchEntry {
    parent: Ino,
    name: Vec<u8>,
    attr: InodeAttr,
    rel_path: Vec<u8>,
    tracked_parent: bool,
}

fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .unwrap_or(0)
}

fn normalize_filter_path(filter_path: Option<&str>) -> VfsResult<Option<String>> {
    let Some(raw) = filter_path else {
        return Ok(None);
    };
    let trimmed = raw.trim_matches('/');
    if trimmed.is_empty() || trimmed == "." {
        return Ok(None);
    }
    let components = parse_volume_relative_components(trimmed)?;
    Ok(Some(components.join("/")))
}

fn path_matches_filter(rel_path: &str, filter: Option<&str>) -> bool {
    let Some(prefix) = filter else {
        return true;
    };
    rel_path == prefix
        || (rel_path.len() > prefix.len()
            && rel_path.starts_with(prefix)
            && rel_path.as_bytes()[prefix.len()] == b'/')
}

fn file_type_label(kind: FileType) -> &'static str {
    match kind {
        FileType::RegularFile => "file",
        FileType::Directory => "dir",
        FileType::Symlink => "symlink",
    }
}

fn format_orphan_summary(record: &studiofs_meta::ScratchOrphanRecord) -> ScratchOrphanSummary {
    let root_old_path = format_relative_path(&record.root_old_path);
    let total_bytes = record
        .entries
        .iter()
        .filter(|entry| entry.attr.kind == FileType::RegularFile)
        .map(|entry| entry.attr.size)
        .sum();
    ScratchOrphanSummary {
        id: format!("{}/{}", record.timestamp_unix_ms, root_old_path),
        timestamp_unix_ms: record.timestamp_unix_ms,
        root_old_path,
        entry_count: record.entries.len(),
        total_bytes,
        materialized: record.materialized,
    }
}

/// Walk all reachable scratch entries attached to tracked parents, returning
/// them in pre-order (parents before children).
fn walk_all_scratch_entries<M: MetaEngineCore + MetaNamespace + ?Sized>(
    meta: &M,
) -> VfsResult<Vec<WalkedScratchEntry>> {
    let roots = meta.list_tracked_parent_scratch_entries()?;
    let mut parent_paths: BTreeMap<Ino, Option<Vec<u8>>> = BTreeMap::new();
    let mut entries = Vec::new();
    let mut visited = HashSet::new();

    for (parent, name, attr) in roots {
        let parent_path = if let Some(cached) = parent_paths.get(&parent) {
            cached.clone()
        } else {
            let resolved = super::ignore_rules::resolve_tracked_directory_path_with(
                meta,
                parent,
                true,
                |_| None,
            )?;
            parent_paths.insert(parent, resolved.clone());
            resolved
        };
        let Some(parent_path) = parent_path else {
            continue;
        };
        let rel_path = join_relative_path(&parent_path, &name);
        let mut stack = vec![WalkedScratchEntry {
            parent,
            name,
            attr,
            rel_path,
            tracked_parent: true,
        }];
        while let Some(current) = stack.pop() {
            if !visited.insert(current.attr.ino) {
                continue;
            }
            let current_ino = current.attr.ino;
            let current_is_dir = current.attr.kind == FileType::Directory;
            let current_rel = current.rel_path.clone();
            entries.push(current);
            if current_is_dir {
                let mut children = meta.readdir_scratch(current_ino)?;
                children.sort_by(|a, b| b.0.cmp(&a.0));
                for (child_name, child_attr) in children {
                    let child_rel = join_relative_path(&current_rel, &child_name);
                    stack.push(WalkedScratchEntry {
                        parent: current_ino,
                        name: child_name,
                        attr: child_attr,
                        rel_path: child_rel,
                        tracked_parent: false,
                    });
                }
            }
        }
    }
    Ok(entries)
}

fn collect_tracked_matching_rule_entries<M: MetaEngineCore + MetaNamespace + ?Sized>(
    meta: &M,
    rules: &ScratchRules,
) -> VfsResult<(usize, Vec<String>)> {
    let mut count = 0usize;
    let mut samples = Vec::new();
    let mut stack: Vec<(Ino, Vec<u8>)> = vec![(ROOT_INO, Vec::new())];
    let mut visited_dirs = HashSet::new();

    while let Some((dir_ino, dir_rel)) = stack.pop() {
        if !visited_dirs.insert(dir_ino) {
            continue;
        }
        let mut children = match meta.readdir(dir_ino) {
            Ok(children) => children,
            Err(MetaError::InodeNotFound(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        children.sort_by(|a, b| b.name.cmp(&a.name));
        for child in children {
            let child_rel = join_relative_path(&dir_rel, &child.name);
            let is_dir = child.kind == FileType::Directory;
            if rules.classify(&child_rel, is_dir) == ScratchClass::Scratch {
                count += 1;
                if samples.len() < MAX_STATUS_SAMPLE_PATHS {
                    samples.push(format_relative_path(&child_rel));
                }
            }
            if is_dir {
                stack.push((child.ino, child_rel));
            }
        }
    }
    samples.sort();
    Ok((count, samples))
}

fn inspect_scratch_status_with_rules<M: MetaEngineCore + MetaNamespace + ?Sized>(
    meta: &M,
    rules: &ScratchRules,
    over_limit_directories: Vec<ScratchPromoteOverLimitEntry>,
) -> VfsResult<VfsScratchStatusSnapshot> {
    let walked = walk_all_scratch_entries(meta)?;
    let mut entry_count = 0usize;
    let mut total_bytes = 0u64;
    let mut no_longer_matching_count = 0usize;
    let mut no_longer_matching_paths = Vec::new();
    let mut path_by_ino: HashMap<Ino, String> = HashMap::new();

    for item in &walked {
        entry_count += 1;
        if item.attr.kind == FileType::RegularFile {
            total_bytes = total_bytes.saturating_add(item.attr.size);
        }
        let rel_str = format_relative_path(&item.rel_path);
        path_by_ino.insert(item.attr.ino, rel_str.clone());
        if item.tracked_parent {
            let is_dir = item.attr.kind == FileType::Directory;
            if rules.classify(&item.rel_path, is_dir) == ScratchClass::Tracked {
                no_longer_matching_count += 1;
                if no_longer_matching_paths.len() < MAX_STATUS_SAMPLE_PATHS {
                    no_longer_matching_paths.push(rel_str);
                }
            }
        }
    }
    no_longer_matching_paths.sort();

    let mut conflicts: Vec<ScratchConflictSummary> = meta
        .list_scratch_conflicts()?
        .into_iter()
        .map(|record| ScratchConflictSummary {
            ino: record.ino,
            original_name: format_relative_path(&record.original_name),
            conflict_name: format_relative_path(&record.conflict_name),
            relative_path: path_by_ino.get(&record.ino).cloned(),
        })
        .collect();
    conflicts.sort_by(|a, b| {
        a.relative_path
            .cmp(&b.relative_path)
            .then_with(|| a.conflict_name.cmp(&b.conflict_name))
    });

    let mut orphans: Vec<ScratchOrphanSummary> = meta
        .list_scratch_orphans()?
        .iter()
        .map(format_orphan_summary)
        .collect();
    orphans.sort_by(|a, b| a.id.cmp(&b.id));

    let (tracked_matching_rule_count, tracked_matching_rule_paths) =
        collect_tracked_matching_rule_entries(meta, rules)?;

    let rule_file_warnings: Vec<ScratchRuleFileWarning> = rules
        .rule_file_diagnostics()
        .into_iter()
        .map(|(dir, diags)| {
            let rule_file_path = if dir.is_empty() {
                ".studiofsignore".to_string()
            } else {
                format!("{}/.studiofsignore", format_relative_path(&dir))
            };
            let mut lines: Vec<usize> = diags.into_iter().map(|d| d.line).collect();
            lines.sort_unstable();
            lines.dedup();
            ScratchRuleFileWarning {
                rule_file_path,
                lines,
            }
        })
        .collect();

    Ok(VfsScratchStatusSnapshot {
        entry_count,
        total_bytes,
        no_longer_matching_count,
        no_longer_matching_paths,
        over_limit_directories,
        conflicts,
        orphans,
        tracked_matching_rule_count,
        tracked_matching_rule_paths,
        rule_file_warnings,
    })
}

/// Inspect scratch-layer status offline directly from redb metadata.
pub fn inspect_scratch_status(
    meta: &dyn MetaEngine,
    entry_limit: usize,
) -> VfsResult<VfsScratchStatusSnapshot> {
    let rules = load_compiled_scratch_rules_from_meta(meta)?;
    let over_limit = find_over_limit_scratch_directories(meta, entry_limit)?;
    inspect_scratch_status_with_rules(meta, &rules, over_limit)
}

fn list_scratch_entries_with_rules<M: MetaEngineCore + MetaNamespace + ?Sized>(
    meta: &M,
    rules: &ScratchRules,
    filter_path: Option<&str>,
    open_check: impl Fn(Ino) -> bool,
) -> VfsResult<Vec<ScratchListEntry>> {
    let filter = normalize_filter_path(filter_path)?;
    let conflicts: HashSet<Ino> = meta
        .list_scratch_conflicts()?
        .into_iter()
        .map(|c| c.ino)
        .collect();
    let walked = walk_all_scratch_entries(meta)?;
    let mut out = Vec::new();
    for item in walked {
        let rel_str = format_relative_path(&item.rel_path);
        if !path_matches_filter(&rel_str, filter.as_deref()) {
            continue;
        }
        let is_dir = item.attr.kind == FileType::Directory;
        let no_longer_matches =
            item.tracked_parent && rules.classify(&item.rel_path, is_dir) == ScratchClass::Tracked;
        out.push(ScratchListEntry {
            relative_path: rel_str,
            kind: file_type_label(item.attr.kind).to_string(),
            size: item.attr.size,
            mtime_secs: item.attr.mtime,
            is_conflict: conflicts.contains(&item.attr.ino),
            no_longer_matches,
            has_open_handles: open_check(item.attr.ino),
        });
    }
    out.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(out)
}

/// List scratch entries offline against redb metadata.
pub fn list_scratch_entries_offline(
    meta: &dyn MetaEngine,
    filter_path: Option<&str>,
) -> VfsResult<Vec<ScratchListEntry>> {
    let rules = load_compiled_scratch_rules_from_meta(meta)?;
    list_scratch_entries_with_rules(meta, &rules, filter_path, |_| false)
}

fn list_scratch_orphans_with_meta<M: MetaEngineCore + ?Sized>(
    meta: &M,
    filter_path: Option<&str>,
) -> VfsResult<Vec<ScratchOrphanSummary>> {
    let filter = normalize_filter_path(filter_path)?;
    let mut out: Vec<ScratchOrphanSummary> = meta
        .list_scratch_orphans()?
        .iter()
        .map(format_orphan_summary)
        .filter(|summary| path_matches_filter(&summary.root_old_path, filter.as_deref()))
        .collect();
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

/// List orphaned scratch subtrees offline (or on any `MetaEngine`).
pub fn list_scratch_orphans_offline(
    meta: &dyn MetaEngine,
    filter_path: Option<&str>,
) -> VfsResult<Vec<ScratchOrphanSummary>> {
    list_scratch_orphans_with_meta(meta, filter_path)
}

/// Clean scratch entries offline against redb metadata.
///
/// Removes matching files/symlinks and empty scratch directories bottom-up and
/// returns freed local payload object IDs for the caller to purge from the
/// local cache store.
pub fn clean_scratch_offline(
    meta: &dyn MetaEngine,
    filter_path: Option<&str>,
    older_than_secs: Option<u64>,
    now_unix_secs: i64,
) -> VfsResult<ScratchCleanOutcome> {
    let filter = normalize_filter_path(filter_path)?;
    let cutoff =
        older_than_secs.map(|d| now_unix_secs.saturating_sub(i64::try_from(d).unwrap_or(i64::MAX)));
    let walked = walk_all_scratch_entries(meta)?;
    let mut outcome = ScratchCleanOutcome::default();
    let mut freed_objects: HashSet<u64> = HashSet::new();
    let mut kept_dirs: HashSet<Ino> = HashSet::new();

    // Reverse pre-order gives bottom-up order (children before parents).
    for item in walked.into_iter().rev() {
        let rel_str = format_relative_path(&item.rel_path);
        if !path_matches_filter(&rel_str, filter.as_deref()) {
            kept_dirs.insert(item.parent);
            continue;
        }
        if let Some(cutoff_ts) = cutoff
            && item.attr.mtime > cutoff_ts
        {
            outcome.skipped_newer += 1;
            kept_dirs.insert(item.parent);
            if item.attr.kind == FileType::Directory {
                kept_dirs.insert(item.attr.ino);
            }
            continue;
        }
        if item.attr.kind == FileType::Directory {
            if kept_dirs.contains(&item.attr.ino) {
                kept_dirs.insert(item.parent);
                continue;
            }
            match meta.rmdir_scratch_entry(item.parent, &item.name) {
                Ok(_) => {
                    outcome.removed_entries += 1;
                }
                Err(MetaError::DirNotEmpty(_)) => {
                    kept_dirs.insert(item.parent);
                }
                Err(MetaError::EntryNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        } else {
            match meta.unlink_scratch_entry(item.parent, &item.name, false) {
                Ok((attr, freed)) => {
                    outcome.removed_entries += 1;
                    if attr.kind == FileType::RegularFile {
                        outcome.freed_bytes = outcome.freed_bytes.saturating_add(attr.size);
                    }
                    freed_objects.extend(freed);
                }
                Err(MetaError::EntryNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    let mut freed_vec: Vec<u64> = freed_objects.into_iter().collect();
    freed_vec.sort_unstable();
    outcome.freed_payload_object_ids = freed_vec;
    Ok(outcome)
}

/// Explicitly promote a scratch file or directory offline against redb.
///
/// Returns `(outcome, promoted_payload_object_ids)` so the caller can mark
/// those local payload objects as dirty/pending upload in the local cache index.
pub fn promote_scratch_path_offline(
    meta: &dyn MetaEngine,
    raw_path: &str,
) -> VfsResult<(ScratchPromotePathOutcome, Vec<u64>)> {
    let components = parse_volume_relative_components(raw_path)?;
    let rules = load_compiled_scratch_rules_from_meta(meta)?;

    let mut parent = ROOT_INO;
    for (idx, segment) in components[..components.len() - 1].iter().enumerate() {
        match meta.lookup(parent, segment.as_bytes()) {
            Ok(attr) => {
                if attr.kind != FileType::Directory {
                    return Err(MetaError::NotADirectory(attr.ino).into());
                }
                parent = attr.ino;
            }
            Err(MetaError::EntryNotFound { .. }) => {
                if meta
                    .lookup_scratch_entry(parent, segment.as_bytes())?
                    .is_some()
                {
                    let prefix = components[..=idx].join("/");
                    return Err(VfsError::InvalidName(format!(
                        "parent directory `{prefix}` is still in scratch; promote it first"
                    )));
                }
                return Err(MetaError::EntryNotFound {
                    parent,
                    name: (*segment).to_string(),
                }
                .into());
            }
            Err(error) => return Err(error.into()),
        }
    }

    let target_name = components[components.len() - 1].as_bytes();
    let Some(root_ino) = meta.lookup_scratch_entry(parent, target_name)? else {
        return match meta.lookup(parent, target_name) {
            Ok(_) => Err(VfsError::InvalidName(format!(
                "path `{raw_path}` is already tracked, not in scratch"
            ))),
            Err(error) => Err(error.into()),
        };
    };

    let root = meta.get_scratch_attr(root_ino)?;
    let root_is_dir = root.kind == FileType::Directory;
    let force_subtree = rules.classify(raw_path.as_bytes(), root_is_dir) != ScratchClass::Tracked;
    let subtree = match collect_scratch_promote_subtree(
        meta,
        root,
        raw_path.as_bytes(),
        usize::MAX,
        ScratchPromoteRevisit::TargetChanged,
        |child_path, child_is_dir| {
            !force_subtree && rules.classify(child_path, child_is_dir) != ScratchClass::Tracked
        },
    )? {
        ScratchPromoteSubtreeWalk::Ready(subtree) => subtree,
        ScratchPromoteSubtreeWalk::OverLimit { .. } => return Err(VfsError::CrossDevice),
    };

    let promoted = meta.promote_scratch_in_place(
        parent,
        target_name,
        root_ino,
        &subtree.frozen,
        &subtree.kept_scratch,
        None,
    )?;

    let mut payload_objects: HashSet<u64> = HashSet::new();
    for attr in &promoted.attrs {
        if attr.kind == FileType::RegularFile {
            for (object_id, _) in meta.get_regular_file_graph_object_usage(attr.ino)? {
                payload_objects.insert(object_id);
            }
        }
    }
    let mut payload_object_ids: Vec<u64> = payload_objects.into_iter().collect();
    payload_object_ids.sort_unstable();

    Ok((
        ScratchPromotePathOutcome {
            relative_path: raw_path.to_owned(),
            promoted_inodes: promoted.attrs.len(),
        },
        payload_object_ids,
    ))
}

fn resolve_orphan_spec<M: MetaEngineCore + ?Sized>(
    meta: &M,
    orphan_spec: &str,
) -> VfsResult<studiofs_meta::ScratchOrphanRecord> {
    let spec = orphan_spec.trim().trim_start_matches('/');
    if spec.is_empty() {
        return Err(VfsError::InvalidName(
            "orphan identifier must not be empty".to_string(),
        ));
    }
    let orphans = meta.list_scratch_orphans()?;
    // Exact `<timestamp>/<old_path>` match first.
    if let Some((ts_str, path_part)) = spec.split_once('/')
        && let Ok(ts) = ts_str.parse::<u64>()
        && let Some(found) = orphans
            .iter()
            .find(|r| r.timestamp_unix_ms == ts && r.root_old_path == path_part.as_bytes())
    {
        return Ok(found.clone());
    }

    // Fallback: match by `root_old_path` if unambiguous.
    let matching: Vec<&studiofs_meta::ScratchOrphanRecord> = orphans
        .iter()
        .filter(|r| r.root_old_path == spec.as_bytes())
        .collect();
    match matching.as_slice() {
        [only] => Ok((*only).clone()),
        [] => Err(MetaError::EntryNotFound {
            parent: ROOT_INO,
            name: spec.to_string(),
        }
        .into()),
        many => {
            let ids: Vec<String> = many.iter().map(|r| format_orphan_summary(r).id).collect();
            Err(VfsError::InvalidName(format!(
                "multiple orphans match `{spec}`; specify the full `<timestamp>/<path>` id: {}",
                ids.join(", ")
            )))
        }
    }
}

fn resolve_restore_target<M: MetaEngineCore + MetaNamespace + ?Sized>(
    meta: &M,
    target_path: &str,
) -> VfsResult<(Ino, Vec<u8>, String)> {
    let components = parse_volume_relative_components(target_path)?;
    let normalized = components.join("/");
    let mut parent = ROOT_INO;
    let mut parent_is_scratch = false;
    for segment in &components[..components.len() - 1] {
        if !parent_is_scratch {
            match meta.lookup(parent, segment.as_bytes()) {
                Ok(attr) => {
                    if attr.kind != FileType::Directory {
                        return Err(MetaError::NotADirectory(attr.ino).into());
                    }
                    parent = attr.ino;
                    continue;
                }
                Err(MetaError::EntryNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(scratch_ino) = meta.lookup_scratch_entry(parent, segment.as_bytes())? {
            let attr = meta.get_scratch_attr(scratch_ino)?;
            if attr.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(scratch_ino).into());
            }
            parent = scratch_ino;
            parent_is_scratch = true;
            continue;
        }
        return Err(MetaError::EntryNotFound {
            parent,
            name: (*segment).to_string(),
        }
        .into());
    }
    let name = components[components.len() - 1].as_bytes().to_vec();
    Ok((parent, name, normalized))
}

fn restore_scratch_orphan_with_meta<M: MetaEngineCore + MetaNamespace + ?Sized>(
    meta: &M,
    orphan_spec: &str,
    target_path: Option<&str>,
) -> VfsResult<ScratchRestoreOutcome> {
    let record = resolve_orphan_spec(meta, orphan_spec)?;
    let orphan_id = format_orphan_summary(&record).id;
    let (target_parent, target_name, restored_path) = match target_path {
        Some(path) => {
            let (parent, name, normalized) = resolve_restore_target(meta, path)?;
            (Some(parent), Some(name), normalized)
        }
        None => (None, None, format_relative_path(&record.root_old_path)),
    };
    let restored = meta.restore_scratch_orphan(
        record.timestamp_unix_ms,
        &record.root_old_path,
        target_parent,
        target_name.as_deref(),
    )?;
    Ok(ScratchRestoreOutcome {
        orphan_id,
        restored_path,
        ino: restored.ino,
    })
}

/// Restore an orphaned scratch entry or subtree offline against redb.
pub fn restore_scratch_orphan_offline(
    meta: &dyn MetaEngine,
    orphan_spec: &str,
    target_path: Option<&str>,
) -> VfsResult<ScratchRestoreOutcome> {
    restore_scratch_orphan_with_meta(meta, orphan_spec, target_path)
}

impl StudioVfs {
    /// Returns a comprehensive scratch-layer status snapshot using live rules
    /// and live auto-promote over-limit state.
    pub fn scratch_status_snapshot(&self) -> VfsResult<VfsScratchStatusSnapshot> {
        let meta = self.sync_meta();
        let rules = self.ignore_rules.current_rules();
        let over_limit = self.scratch_auto_promote_snapshot().over_limit_directories;
        inspect_scratch_status_with_rules(meta.as_ref(), rules.as_ref(), over_limit)
    }

    /// List scratch entries on a live mount, annotating entries with open handles.
    pub fn list_scratch_entries(
        &self,
        filter_path: Option<&str>,
    ) -> VfsResult<Vec<ScratchListEntry>> {
        let meta = self.sync_meta();
        let rules = self.ignore_rules.current_rules();
        list_scratch_entries_with_rules(meta.as_ref(), rules.as_ref(), filter_path, |ino| {
            self.handle_table.open_count(ino) > 0
        })
    }

    /// List orphaned scratch subtrees on a live mount.
    pub fn list_scratch_orphan_entries(
        &self,
        filter_path: Option<&str>,
    ) -> VfsResult<Vec<ScratchOrphanSummary>> {
        let meta = self.sync_meta();
        list_scratch_orphans_with_meta(meta.as_ref(), filter_path)
    }

    /// Clean scratch entries on a live mount, skipping entries with open handles
    /// and entries newer than `older_than_secs`.
    pub async fn clean_scratch_entries(
        &self,
        filter_path: Option<&str>,
        older_than_secs: Option<u64>,
    ) -> VfsResult<ScratchCleanOutcome> {
        let filter = normalize_filter_path(filter_path)?;
        let Ok(_op_guard) = self.state.shutdown.mutating_operation_gate.try_read() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "volume is shutting down",
            )
            .into());
        };
        let now_unix_secs = current_unix_secs();
        let cutoff = older_than_secs
            .map(|d| now_unix_secs.saturating_sub(i64::try_from(d).unwrap_or(i64::MAX)));
        let meta = self.sync_meta();
        let walked = walk_all_scratch_entries(meta.as_ref())?;
        let mut outcome = ScratchCleanOutcome::default();
        let mut kept_dirs: HashSet<Ino> = HashSet::new();
        for item in walked.into_iter().rev() {
            let rel_str = format_relative_path(&item.rel_path);
            if !path_matches_filter(&rel_str, filter.as_deref()) {
                kept_dirs.insert(item.parent);
                continue;
            }
            if cutoff.is_some_and(|cutoff| item.attr.mtime > cutoff) {
                outcome.skipped_newer += 1;
                kept_dirs.insert(item.parent);
                if item.attr.kind == FileType::Directory {
                    kept_dirs.insert(item.attr.ino);
                }
                continue;
            }
            if item.attr.kind == FileType::Directory && kept_dirs.contains(&item.attr.ino) {
                kept_dirs.insert(item.parent);
                continue;
            }

            if item.parent == item.attr.ino {
                return Err(VfsError::InvalidName("scratch directory cycle".into()));
            }
            let _parent_session = self.file_session_cleanup(item.parent).with_permit(
                self.state
                    .file_session
                    .enter_op(item.parent, FileSessionOperation::Delete)
                    .await,
            );
            self.wait_for_deferred_materialization_inflight(item.attr.ino)
                .await;
            let target_operation = if item.attr.kind == FileType::Directory {
                FileSessionOperation::Read
            } else {
                FileSessionOperation::Delete
            };
            let _inode_session = self.file_session_cleanup(item.attr.ino).with_permit(
                self.state
                    .file_session
                    .enter_op(item.attr.ino, target_operation)
                    .await,
            );
            let namespace_mutations = self.namespace_mutations();
            let removed = {
                let mut admission = self.handle_table.scratch_cleanup_epoch.write();
                if item.attr.kind != FileType::Directory
                    && let Some(cutoff) = cutoff
                    && self
                        .peek_dirty_attr(item.attr.ino)
                        .is_some_and(|attr| attr.mtime > cutoff)
                {
                    outcome.skipped_newer += 1;
                    kept_dirs.insert(item.parent);
                    continue;
                }
                if self.handle_table.cleanup_open_count(item.attr.ino) > 0 {
                    outcome.skipped_open_handles += 1;
                    kept_dirs.insert(item.parent);
                    if item.attr.kind == FileType::Directory {
                        kept_dirs.insert(item.attr.ino);
                    }
                    continue;
                }
                // Child removal changes directory mtime; age was checked before the walk.
                let current_cutoff = if item.attr.kind == FileType::Directory {
                    None
                } else {
                    cutoff
                };
                let removed = meta.clean_scratch_entry(
                    item.parent,
                    &item.name,
                    item.attr.ino,
                    item.attr.kind,
                    current_cutoff,
                );
                if let Ok((attr, _)) = &removed {
                    *admission = admission.wrapping_add(1);
                    if attr.nlink == 0 {
                        self.state.namespace.dirty_attrs.lock().remove(&attr.ino);
                    }
                    namespace_mutations.record_scratch_removal(item.parent, &item.name, [attr.ino]);
                }
                removed
            };
            match removed {
                Ok((attr, freed_objects)) => {
                    self.invalidate_kernel_entries([(item.parent, item.name.clone())]);
                    self.invalidate_kernel_inode_attrs([item.parent]);
                    self.prune_over_limit_directories(|ino| ino == attr.ino);
                    if attr.kind == FileType::Directory {
                        self.bump_directory_generation(attr.ino);
                    }
                    outcome.removed_entries += 1;
                    if attr.kind == FileType::RegularFile {
                        outcome.freed_bytes = outcome.freed_bytes.saturating_add(attr.size);
                    }
                    if attr.nlink == 0 {
                        self.cleanup_scratch_inode_runtime_and_payloads(attr.ino, freed_objects)
                            .await;
                    }
                }
                Err(MetaError::DirNotEmpty(_) | MetaError::EntryNotFound { .. }) => {
                    kept_dirs.insert(item.parent);
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(outcome)
    }

    /// Restore an orphaned scratch entry or subtree on a live mount.
    pub fn restore_scratch_orphan_by_spec(
        &self,
        orphan_spec: &str,
        target_path: Option<&str>,
    ) -> VfsResult<ScratchRestoreOutcome> {
        let Ok(_op_guard) = self.state.shutdown.mutating_operation_gate.try_read() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "volume is shutting down",
            )
            .into());
        };
        let meta = self.sync_meta();
        let outcome = restore_scratch_orphan_with_meta(meta.as_ref(), orphan_spec, target_path)?;
        let parent = meta.get_scratch_attr(outcome.ino)?.parent;
        if let Some(name) = outcome.restored_path.rsplit('/').next() {
            self.invalidate_kernel_entries([(parent, name.as_bytes().to_vec())]);
        }
        self.invalidate_kernel_inode_attrs([parent]);
        self.invalidate_attr_read_cache([parent, outcome.ino]);
        self.dentry_cache_invalidate_parent(parent);
        if parent != ROOT_INO {
            self.dentry_cache_invalidate_parent(ROOT_INO);
        }
        self.ignore_rules.bump_dir_rename_epoch();
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::file_session::OpenFlags;
    use studiofs_meta::{MetaFsOps, MetaNamespace, RedbMetaEngine};

    #[test]
    fn offline_status_list_clean_promote_and_restore_work_end_to_end() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-tooling-offline-");
        let meta = RedbMetaEngine::open(temp.path().join("meta.db")).unwrap();
        meta.init_root(0, 0).unwrap();

        // Install cached ignore rules including a pattern matching `.studiofsignore`.
        let mut sources: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        sources.insert(Vec::new(), b"*.tmp\n.*ignore\ntracked-dir/\n".to_vec());
        meta.store_ignore_rules_cache(&postcard::to_allocvec(&sources).unwrap())
            .unwrap();

        // Tracked entry matching a rule (`tracked.tmp`).
        meta.create_file(ROOT_INO, b"tracked.tmp", 0, 0, 0o644)
            .unwrap();
        let tracked_dir = meta
            .create_dir(ROOT_INO, b"tracked-dir", 0, 0, 0o755)
            .unwrap();
        meta.create_file(tracked_dir.ino, b"child", 0, 0, 0o644)
            .unwrap();

        // Scratch entries: one matching (`a.tmp`), one no longer matching (`stale.txt`).
        let mut matching = InodeAttr::new_file(100, ROOT_INO, 0, 0, 0o644);
        matching.size = 42;
        matching.mtime = 1000;
        meta.create_scratch_entry(ROOT_INO, b"a.tmp", &matching)
            .unwrap();

        let mut stale = InodeAttr::new_file(101, ROOT_INO, 0, 0, 0o644);
        stale.size = 10;
        stale.mtime = 2000;
        meta.create_scratch_entry(ROOT_INO, b"stale.txt", &stale)
            .unwrap();

        let status = inspect_scratch_status(&meta, 100_000).unwrap();
        assert_eq!(status.entry_count, 2);
        assert_eq!(status.total_bytes, 52);
        assert_eq!(status.no_longer_matching_count, 1);
        assert_eq!(status.no_longer_matching_paths, vec!["stale.txt"]);
        assert_eq!(status.tracked_matching_rule_count, 3);
        assert_eq!(
            status.tracked_matching_rule_paths,
            vec!["tracked-dir", "tracked-dir/child", "tracked.tmp"]
        );
        assert_eq!(status.rule_file_warnings.len(), 1);
        assert_eq!(
            status.rule_file_warnings[0].rule_file_path,
            ".studiofsignore"
        );
        assert_eq!(status.rule_file_warnings[0].lines, vec![2]);

        let listed = list_scratch_entries_offline(&meta, None).unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].relative_path, "a.tmp");
        assert!(!listed[0].no_longer_matches);
        assert_eq!(listed[1].relative_path, "stale.txt");
        assert!(listed[1].no_longer_matches);

        // Promote `stale.txt` offline.
        let (promoted, _) = promote_scratch_path_offline(&meta, "stale.txt").unwrap();
        assert_eq!(promoted.promoted_inodes, 1);
        assert_eq!(meta.lookup(ROOT_INO, b"stale.txt").unwrap().ino, 101);
        assert!(
            meta.lookup_scratch_entry(ROOT_INO, b"stale.txt")
                .unwrap()
                .is_none()
        );

        // Clean with `--older-than` skips newer entry, then removes older entry.
        let skipped = clean_scratch_offline(&meta, None, Some(500), 1200).unwrap();
        assert_eq!(skipped.removed_entries, 0);
        assert_eq!(skipped.skipped_newer, 1);

        let cleaned = clean_scratch_offline(&meta, None, Some(100), 1200).unwrap();
        assert_eq!(cleaned.removed_entries, 1);
        assert_eq!(cleaned.freed_bytes, 42);
        assert!(
            list_scratch_entries_offline(&meta, None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn mounted_clean_skips_entries_with_open_handles() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-tooling-mounted-");
        let (rt, vfs, _meta) = super::super::tests::make_unstarted_test_vfs(&temp, "clean-open");
        let _guard = rt.enter();
        rt.block_on(async {
            let open_attr = vfs
                .store_scratch_entry(b"open.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            let closed_attr = vfs
                .store_scratch_entry(b"closed.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            let fh = vfs
                .handle_table
                .open(
                    open_attr.ino,
                    OpenFlags::RDWR,
                    Some(open_attr.clone()),
                    false,
                )
                .fh;

            let listed = vfs.list_scratch_entries(None).unwrap();
            assert_eq!(listed.len(), 2);
            assert!(
                listed
                    .iter()
                    .any(|e| e.relative_path == "open.tmp" && e.has_open_handles)
            );
            assert!(
                listed
                    .iter()
                    .any(|e| e.relative_path == "closed.tmp" && !e.has_open_handles)
            );

            let outcome = vfs.clean_scratch_entries(None, None).await.unwrap();
            assert_eq!(outcome.removed_entries, 1);
            assert_eq!(outcome.skipped_open_handles, 1);
            assert!(vfs.is_scratch_inode(open_attr.ino).unwrap());
            assert!(!vfs.is_scratch_inode(closed_attr.ino).unwrap());

            let _ = vfs.handle_table.close(fh);
            let outcome2 = vfs.clean_scratch_entries(None, None).await.unwrap();
            assert_eq!(outcome2.removed_entries, 1);
            assert_eq!(outcome2.skipped_open_handles, 0);
        });
    }

    #[test]
    fn mounted_clean_skips_pending_truncate_open() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-clean-truncate-open-");
        let (rt, vfs, _meta) = super::super::tests::make_unstarted_test_vfs(&temp, "clean-trunc");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .store_scratch_entry(
                    b"truncate.tmp",
                    InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644),
                )
                .unwrap();
            let pending = vfs.handle_table.admit_pending_truncate_open(attr.ino);

            let outcome = vfs.clean_scratch_entries(None, None).await.unwrap();
            assert_eq!(outcome.removed_entries, 0);
            assert_eq!(outcome.skipped_open_handles, 1);
            assert!(vfs.is_scratch_inode(attr.ino).unwrap());

            drop(pending);
            let outcome = vfs.clean_scratch_entries(None, None).await.unwrap();
            assert_eq!(outcome.removed_entries, 1);
            assert!(!vfs.is_scratch_inode(attr.ino).unwrap());
        });
    }
    #[test]
    fn mounted_clean_rechecks_handles_after_waiting_for_writer() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-clean-writer-");
        let (rt, vfs, _meta) = super::super::tests::make_unstarted_test_vfs(&temp, "clean-writer");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .store_scratch_entry(b"busy.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            let writer = vfs
                .state
                .file_session
                .enter_op(
                    attr.ino,
                    super::super::file_session::FileSessionOperation::Write,
                )
                .await;
            let clean = vfs.clean_scratch_entries(None, None);
            tokio::pin!(clean);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(clean.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            let fh = vfs.open_file(attr.ino, OpenFlags::RDONLY, false).unwrap();
            drop(writer);
            let outcome = clean.await.unwrap();
            assert_eq!(outcome.removed_entries, 0);
            assert_eq!(outcome.skipped_open_handles, 1);
            assert!(vfs.is_scratch_inode(attr.ino).unwrap());
            let _ = vfs.handle_table.close(fh);
        });
    }

    #[test]
    fn mounted_clean_does_not_remove_replacement_after_waiting() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-clean-replacement-");
        let (rt, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&temp, "clean-replacement");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .store_scratch_entry(b"old.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            let writer = vfs
                .state
                .file_session
                .enter_op(attr.ino, FileSessionOperation::Write)
                .await;
            let clean = vfs.clean_scratch_entries(None, None);
            tokio::pin!(clean);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(clean.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            meta.rename_scratch_entry(ROOT_INO, b"old.tmp", ROOT_INO, b"moved.tmp", false, false)
                .unwrap();
            let replacement = vfs
                .store_scratch_entry(b"old.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            drop(writer);
            let outcome = clean.await.unwrap();
            assert_eq!(outcome.removed_entries, 0);
            assert_eq!(
                meta.lookup_scratch_entry(ROOT_INO, b"old.tmp").unwrap(),
                Some(replacement.ino)
            );
            assert_eq!(
                meta.lookup_scratch_entry(ROOT_INO, b"moved.tmp").unwrap(),
                Some(attr.ino)
            );
        });
    }
}
