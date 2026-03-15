//! `studiofs fsck` — inspect and repair metadata inconsistencies.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;

use studiofs_meta::engine::UndoLogKind;
use studiofs_meta::{
    FileType, MetaEngine, MetaError, MetaOpenOptions, MetadataBackend, ROOT_INO,
    open_meta_engine_with_options,
};

#[derive(Debug, Clone)]
struct DanglingDentry {
    parent: u64,
    ino: u64,
    name: Vec<u8>,
}

#[derive(Debug, Clone)]
struct OrphanInode {
    ino: u64,
    kind: FileType,
    nlink: u32,
}

#[derive(Debug, Clone)]
struct NlinkMismatch {
    ino: u64,
    kind: FileType,
    expected_nlink: u32,
    actual_links: u32,
}

#[derive(Debug, Default)]
struct NamespaceScan {
    dangling: Vec<DanglingDentry>,
    reachable_inodes: HashSet<u64>,
    incoming_links: HashMap<u64, u32>,
    child_dir_counts: HashMap<u64, u32>,
}

#[derive(Debug, Default)]
struct FsckReport {
    dangling: Vec<DanglingDentry>,
    orphan_inodes: Vec<OrphanInode>,
    nlink_mismatches: Vec<NlinkMismatch>,
    pending_creates: usize,
    deferred_deletes: usize,
    undo_pending_create: usize,
    undo_deferred_delete: usize,
}

impl FsckReport {
    fn has_findings(&self) -> bool {
        !self.dangling.is_empty()
            || !self.orphan_inodes.is_empty()
            || !self.nlink_mismatches.is_empty()
            || self.pending_creates > 0
            || self.deferred_deletes > 0
            || self.undo_pending_create > 0
            || self.undo_deferred_delete > 0
    }
}

fn scan_namespace(meta: &dyn MetaEngine) -> anyhow::Result<NamespaceScan> {
    let mut scan = NamespaceScan::default();
    let mut stack = vec![ROOT_INO];
    let mut visited_dirs = HashSet::new();
    scan.reachable_inodes.insert(ROOT_INO);

    while let Some(parent) = stack.pop() {
        if !visited_dirs.insert(parent) {
            continue;
        }

        let entries = match meta.readdir(parent) {
            Ok(entries) => entries,
            Err(MetaError::InodeNotFound(_)) => continue,
            Err(err) => return Err(anyhow::anyhow!("readdir failed for parent {parent}: {err}")),
        };

        for entry in entries {
            match meta.get_attr(entry.ino) {
                Ok(attr) => {
                    scan.reachable_inodes.insert(attr.ino);
                    *scan.incoming_links.entry(attr.ino).or_insert(0) += 1;
                    if attr.kind == FileType::Directory {
                        *scan.child_dir_counts.entry(parent).or_insert(0) += 1;
                        stack.push(attr.ino);
                    }
                }
                Err(MetaError::InodeNotFound(_)) => scan.dangling.push(DanglingDentry {
                    parent,
                    ino: entry.ino,
                    name: entry.name,
                }),
                Err(err) => {
                    return Err(anyhow::anyhow!(
                        "get_attr failed for ino {} under parent {}: {}",
                        entry.ino,
                        parent,
                        err
                    ));
                }
            }
        }
    }

    Ok(scan)
}

fn collect_report_with_progress(meta: &dyn MetaEngine, progress: bool) -> anyhow::Result<FsckReport> {
    if progress {
        println!("Collecting fsck inputs...");
        println!("  reading pending create records...");
    }
    let pending_creates = meta.list_pending_creates()?;
    if progress {
        println!("  reading deferred delete records...");
    }
    let deferred_deletes = meta.list_deferred_deletes()?;
    if progress {
        println!("  reading undo log records...");
    }
    let undo_logs = meta.list_undo_logs()?;
    if progress {
        println!("  enumerating inode attributes...");
    }
    let all_inodes = meta.list_inode_attrs()?;
    if progress {
        println!("  scanning reachable namespace from root...");
    }
    let scan = scan_namespace(meta)?;
    if progress {
        println!(
            "  namespace scan complete: reachable_inodes={} dangling_dentries={}",
            scan.reachable_inodes.len(),
            scan.dangling.len()
        );
        println!("  reconciling nlink and orphan state...");
    }

    let mut report = FsckReport {
        dangling: scan.dangling,
        pending_creates: pending_creates.len(),
        deferred_deletes: deferred_deletes.len(),
        ..FsckReport::default()
    };

    for attr in &all_inodes {
        if attr.ino != ROOT_INO && !scan.reachable_inodes.contains(&attr.ino) {
            report.orphan_inodes.push(OrphanInode {
                ino: attr.ino,
                kind: attr.kind,
                nlink: attr.nlink,
            });
        }

        let actual_nlink = if attr.kind == FileType::Directory {
            2 + scan.child_dir_counts.get(&attr.ino).copied().unwrap_or(0)
        } else {
            scan.incoming_links.get(&attr.ino).copied().unwrap_or(0)
        };
        if attr.nlink != actual_nlink {
            report.nlink_mismatches.push(NlinkMismatch {
                ino: attr.ino,
                kind: attr.kind,
                expected_nlink: attr.nlink,
                actual_links: actual_nlink,
            });
        }
    }

    for row in undo_logs {
        match row.kind {
            UndoLogKind::PendingCreate => report.undo_pending_create += 1,
            UndoLogKind::DeferredDelete => report.undo_deferred_delete += 1,
        }
    }

    Ok(report)
}

fn collect_report(meta: &dyn MetaEngine) -> anyhow::Result<FsckReport> {
    collect_report_with_progress(meta, false)
}

fn print_report(name: &str, report: &FsckReport) {
    println!("FSCK report for volume '{}'", name);
    println!("────────────────────────────");
    println!("  dangling dentries:    {}", report.dangling.len());
    println!("  orphan inodes:        {}", report.orphan_inodes.len());
    println!("  nlink mismatches:     {}", report.nlink_mismatches.len());
    println!("  pending creates:      {}", report.pending_creates);
    println!("  deferred deletes:     {}", report.deferred_deletes);
    println!("  undo pending_create:  {}", report.undo_pending_create);
    println!("  undo deferred_delete: {}", report.undo_deferred_delete);

    if !report.dangling.is_empty() {
        println!();
        println!("Dangling dentries (up to 25):");
        for d in report.dangling.iter().take(25) {
            let name = String::from_utf8_lossy(&d.name);
            println!("  parent={} name='{}' ino={}", d.parent, name, d.ino);
        }
        if report.dangling.len() > 25 {
            println!("  ... and {} more", report.dangling.len() - 25);
        }
    }

    if !report.orphan_inodes.is_empty() {
        println!();
        println!("Orphan inodes (up to 25):");
        for o in report.orphan_inodes.iter().take(25) {
            println!("  ino={} kind={:?} nlink={}", o.ino, o.kind, o.nlink);
        }
        if report.orphan_inodes.len() > 25 {
            println!("  ... and {} more", report.orphan_inodes.len() - 25);
        }
    }

    if !report.nlink_mismatches.is_empty() {
        println!();
        println!("nlink mismatches (up to 25):");
        for m in report.nlink_mismatches.iter().take(25) {
            println!(
                "  ino={} kind={:?} expected_nlink={} actual_links={}",
                m.ino, m.kind, m.expected_nlink, m.actual_links
            );
        }
        if report.nlink_mismatches.len() > 25 {
            println!("  ... and {} more", report.nlink_mismatches.len() - 25);
        }
    }
}

fn ask_should_repair() -> anyhow::Result<bool> {
    print!("Repair all detected issues now? [y/N]: ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn remove_orphan_file(
    meta: &dyn MetaEngine,
    ino: u64,
    orphan_blocks_pending_gc: &mut usize,
    removed_orphans: &mut usize,
) -> anyhow::Result<()> {
    let removed_ids = match meta.remove_file_blocks(ino) {
        Ok(ids) => ids,
        Err(MetaError::InodeNotFound(_)) => Vec::new(),
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to remove orphan blocks for ino {}: {}",
                ino,
                err
            ));
        }
    };

    if !removed_ids.is_empty() {
        let ts = now_unix_secs();
        for object_id in &removed_ids {
            meta.add_pending_deletion(*object_id, ts)?;
        }
        *orphan_blocks_pending_gc += removed_ids.len();
    }

    match meta.remove_attr(ino) {
        Ok(()) | Err(MetaError::InodeNotFound(_)) => *removed_orphans += 1,
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to remove orphan inode {}: {}",
                ino,
                err
            ));
        }
    }

    Ok(())
}

fn repair_orphan_directory(
    meta: &dyn MetaEngine,
    ino: u64,
    removed_orphans: &mut usize,
    orphan_blocks_pending_gc: &mut usize,
    skipped_hardlink_orphans: &mut usize,
    processed: &mut HashSet<u64>,
) -> anyhow::Result<()> {
    if !processed.insert(ino) {
        return Ok(());
    }

    let entries = match meta.readdir(ino) {
        Ok(entries) => entries,
        Err(MetaError::InodeNotFound(_)) => return Ok(()),
        Err(err) => return Err(anyhow::anyhow!("readdir failed for orphan dir {}: {}", ino, err)),
    };

    for entry in entries {
        match entry.kind {
            FileType::Directory => {
                repair_orphan_directory(
                    meta,
                    entry.ino,
                    removed_orphans,
                    orphan_blocks_pending_gc,
                    skipped_hardlink_orphans,
                    processed,
                )?;
                match meta.rmdir(ino, &entry.name) {
                    Ok(()) | Err(MetaError::EntryNotFound { .. }) => {
                        *removed_orphans += 1;
                    }
                    Err(MetaError::InodeNotFound(_)) => {
                        *removed_orphans += 1;
                    }
                    Err(MetaError::DirNotEmpty(_)) => {}
                    Err(err) => {
                        return Err(anyhow::anyhow!(
                            "failed to remove orphan dir entry parent={} child={}: {}",
                            ino,
                            entry.ino,
                            err
                        ));
                    }
                }
            }
            _ => {
                let attr = match meta.get_attr(entry.ino) {
                    Ok(attr) => Some(attr),
                    Err(MetaError::InodeNotFound(_)) => None,
                    Err(err) => {
                        return Err(anyhow::anyhow!(
                            "get_attr failed for orphan child ino {} under parent {}: {}",
                            entry.ino,
                            ino,
                            err
                        ));
                    }
                };
                if attr.as_ref().is_some_and(|attr| attr.nlink > 1) {
                    *skipped_hardlink_orphans += 1;
                    continue;
                }

                match meta.remove_entry(ino, &entry.name) {
                    Ok(_) | Err(MetaError::EntryNotFound { .. }) => {}
                    Err(err) => {
                        return Err(anyhow::anyhow!(
                            "failed to remove orphan child entry parent={} child={}: {}",
                            ino,
                            entry.ino,
                            err
                        ));
                    }
                }

                if attr.is_some() {
                    remove_orphan_file(
                        meta,
                        entry.ino,
                        orphan_blocks_pending_gc,
                        removed_orphans,
                    )?;
                }
                processed.insert(entry.ino);
            }
        }
    }

    match meta.remove_attr(ino) {
        Ok(()) | Err(MetaError::InodeNotFound(_)) => *removed_orphans += 1,
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to remove orphan directory inode {}: {}",
                ino,
                err
            ));
        }
    }
    Ok(())
}

fn repair(meta: &dyn MetaEngine, report: &FsckReport) -> anyhow::Result<()> {
    let replayed = meta.recover_undo_logs()?;
    if !replayed.is_empty() {
        println!("Replayed undo logs, touched {} parent dirs.", replayed.len());
    }

    let mut removed_dangling = 0usize;
    let mut dangling_already_gone = 0usize;
    for d in &report.dangling {
        match meta.remove_entry(d.parent, &d.name) {
            Ok(_) => removed_dangling += 1,
            Err(MetaError::EntryNotFound { .. }) => dangling_already_gone += 1,
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to remove dangling dentry parent={} ino={}: {}",
                    d.parent,
                    d.ino,
                    err
                ));
            }
        }
    }

    let mut removed_orphans = 0usize;
    let skipped_dir_orphans = 0usize;
    let mut skipped_hardlink_orphans = 0usize;
    let mut orphan_blocks_pending_gc = 0usize;
    let mut processed_orphans = HashSet::new();

    for orphan in &report.orphan_inodes {
        if processed_orphans.contains(&orphan.ino) {
            continue;
        }
        if orphan.kind == FileType::Directory {
            repair_orphan_directory(
                meta,
                orphan.ino,
                &mut removed_orphans,
                &mut orphan_blocks_pending_gc,
                &mut skipped_hardlink_orphans,
                &mut processed_orphans,
            )?;
            continue;
        }
        if orphan.nlink > 1 {
            skipped_hardlink_orphans += 1;
            continue;
        }

        remove_orphan_file(
            meta,
            orphan.ino,
            &mut orphan_blocks_pending_gc,
            &mut removed_orphans,
        )?;
        processed_orphans.insert(orphan.ino);
    }

    let mut repaired_nlinks = 0usize;
    let post_cleanup = collect_report(meta)?;
    for mismatch in &post_cleanup.nlink_mismatches {
        let mut attr = match meta.get_attr(mismatch.ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => continue,
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to read inode {} before nlink repair: {}",
                    mismatch.ino,
                    err
                ));
            }
        };
        attr.nlink = mismatch.actual_links;
        attr.touch_ctime();
        meta.set_attr(&attr)?;
        repaired_nlinks += 1;
    }

    println!("Repair summary:");
    println!("  replayed undo parents: {}", replayed.len());
    println!("  dangling entries removed: {}", removed_dangling);
    println!("  dangling already gone: {}", dangling_already_gone);
    println!("  orphan inodes removed: {}", removed_orphans);
    println!("  orphan blocks pending gc: {}", orphan_blocks_pending_gc);
    println!("  orphan dirs skipped: {}", skipped_dir_orphans);
    println!("  orphan hardlinks skipped: {}", skipped_hardlink_orphans);
    println!("  nlink mismatches repaired: {}", repaired_nlinks);
    Ok(())
}

pub fn run(name: &str, meta_dir: &Path, auto_repair: bool) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        anyhow::bail!("Volume '{}' not found at {}", name, db_path.display());
    }

    let meta = open_meta_engine_with_options(
        &db_path,
        MetaOpenOptions {
            background_poll: false,
        },
    )?;
    let format_info = meta.get_format_info()?;

    if format_info.metadata.backend == MetadataBackend::Postgres {
        println!("Syncing local metadata from PostgreSQL before fsck...");
        meta.sync_metadata()?;
    }

    println!("Collecting fsck report...");
    let report = collect_report_with_progress(meta.as_ref(), true)?;
    print_report(name, &report);

    if !report.has_findings() {
        println!();
        println!("No issues found.");
        return Ok(());
    }

    let should_repair = if auto_repair {
        true
    } else {
        println!();
        ask_should_repair()?
    };
    if !should_repair {
        println!("No repairs applied.");
        return Ok(());
    }

    println!("Running repair...");
    repair(meta.as_ref(), &report)?;

    println!();
    println!("Rescanning...");
    let post = collect_report_with_progress(meta.as_ref(), true)?;
    print_report(name, &post);
    if post.has_findings() {
        println!("Some issues remain; rerun fsck or inspect logs.");
    } else {
        println!("Repair completed cleanly.");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::RedbMetaEngine;

    fn temp_engine() -> RedbMetaEngine {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fsck-test.db");
        let engine = RedbMetaEngine::open(&path).expect("open redb");
        engine.init_root(1000, 1000).expect("init root");
        std::mem::forget(dir);
        engine
    }

    #[test]
    fn repair_removes_orphan_directory_subtree() {
        let engine = temp_engine();
        let orphan_dir = engine
            .create_dir(ROOT_INO, b"orphan-root", 1000, 1000, 0o755)
            .expect("create orphan root");
        let nested_dir = engine
            .create_dir(orphan_dir.ino, b"nested", 1000, 1000, 0o755)
            .expect("create nested dir");
        engine
            .create_file(nested_dir.ino, b"payload.txt", 1000, 1000, 0o644)
            .expect("create payload");

        engine
            .remove_entry(ROOT_INO, b"orphan-root")
            .expect("detach orphan subtree");

        let report = collect_report(&engine).expect("collect pre-repair report");
        assert_eq!(report.orphan_inodes.len(), 3);

        repair(&engine, &report).expect("repair orphan subtree");

        let post = collect_report(&engine).expect("collect post-repair report");
        assert!(post.orphan_inodes.is_empty());
        assert!(post.nlink_mismatches.is_empty());
    }
}
