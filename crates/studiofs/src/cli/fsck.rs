//! `studiofs fsck` — inspect and repair metadata inconsistencies.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;

use clap::Args;
use studiofs_meta::engine::UndoLogKind;
use studiofs_meta::{
    DirEntry, FileType, MetaEngine, MetaError, MetaOpenOptions, MetadataBackend, ROOT_INO,
    open_meta_engine_with_options,
};
use thiserror::Error;

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

#[derive(Debug, Clone, Copy)]
pub enum FsckRepairMode {
    Ask,
    Auto,
}

impl From<bool> for FsckRepairMode {
    fn from(auto_repair: bool) -> Self {
        if auto_repair {
            FsckRepairMode::Auto
        } else {
            FsckRepairMode::Ask
        }
    }
}

#[derive(Debug, Error)]
pub enum FsckError {
    #[error("volume not found at {path}")]
    LocalDbNotFound { path: String },

    #[error("metadata operation failed: {0}")]
    Meta(#[from] MetaError),

    #[error("input/output error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Message(String),
}

type FsckResult<T> = std::result::Result<T, FsckError>;

#[derive(Debug, Args)]
#[command(about = "Check and optionally repair metadata inconsistencies")]
#[command(after_help = "\
EXAMPLES:
  studiofs fsck myvolume
  studiofs fsck myvolume --repair
")]
pub(crate) struct FsckArgs {
    /// Volume name
    name: String,

    /// Repair findings immediately without interactive confirmation
    #[arg(long)]
    repair: bool,
}

impl FsckArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        run(&self.name, &meta_dir, self.repair)?;
        Ok(())
    }
}

fn scan_namespace(meta: &dyn MetaEngine) -> FsckResult<NamespaceScan> {
    let mut scan = NamespaceScan::default();
    let reachable_inodes = &mut scan.reachable_inodes;
    let incoming_links = &mut scan.incoming_links;
    let child_dir_counts = &mut scan.child_dir_counts;
    let dangling = &mut scan.dangling;
    let mut stack = vec![ROOT_INO];
    let mut visited_dirs = HashSet::new();
    reachable_inodes.insert(ROOT_INO);

    while let Some(parent) = stack.pop() {
        if !visited_dirs.insert(parent) {
            continue;
        }

        let entries = match meta.readdir(parent) {
            Ok(entries) => entries,
            Err(MetaError::InodeNotFound(_)) => continue,
            Err(err) => {
                return Err(FsckError::Message(format!(
                    "readdir failed for parent {parent}: {err}"
                )));
            }
        };

        for entry in entries {
            let entry_ino = entry.ino;
            let entry_name = entry.name;
            match meta.get_attr(entry_ino) {
                Ok(attr) => {
                    let ino = attr.ino;
                    let kind = attr.kind;
                    reachable_inodes.insert(ino);
                    *incoming_links.entry(ino).or_insert(0) += 1;
                    if kind == FileType::Directory {
                        *child_dir_counts.entry(parent).or_insert(0) += 1;
                        stack.push(ino);
                    }
                }
                Err(MetaError::InodeNotFound(_)) => dangling.push(DanglingDentry {
                    parent,
                    ino: entry_ino,
                    name: entry_name,
                }),
                Err(err) => {
                    return Err(FsckError::Message(format!(
                        "get_attr failed for ino {entry_ino} under parent {parent}: {err}"
                    )));
                }
            }
        }
    }

    Ok(scan)
}

fn collect_report_with_progress(meta: &dyn MetaEngine, progress: bool) -> FsckResult<FsckReport> {
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
    let reachable_inodes = &scan.reachable_inodes;
    let incoming_links = &scan.incoming_links;
    let child_dir_counts = &scan.child_dir_counts;
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
        let ino = attr.ino;
        let kind = attr.kind;
        let nlink = attr.nlink;

        if ino != ROOT_INO && !reachable_inodes.contains(&ino) {
            report.orphan_inodes.push(OrphanInode { ino, kind, nlink });
        }

        let actual_nlink = if kind == FileType::Directory {
            2 + child_dir_counts.get(&ino).copied().unwrap_or(0)
        } else {
            incoming_links.get(&ino).copied().unwrap_or(0)
        };
        if nlink != actual_nlink {
            report.nlink_mismatches.push(NlinkMismatch {
                ino,
                kind,
                expected_nlink: nlink,
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

fn collect_report(meta: &dyn MetaEngine) -> FsckResult<FsckReport> {
    collect_report_with_progress(meta, false)
}

fn print_report(name: &str, report: &FsckReport) {
    println!("FSCK report for volume '{name}'");
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
            println!("  parent={} name={:?} ino={}", d.parent, d.name, d.ino);
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

fn ask_should_repair() -> FsckResult<bool> {
    print!("Repair all detected issues now? [y/N]: ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn remove_orphan_file(
    meta: &dyn MetaEngine,
    ino: u64,
    orphan_blocks_pending_gc: &mut usize,
    removed_orphans: &mut usize,
) -> FsckResult<()> {
    let removed_ids = match meta.remove_regular_file_graph(ino) {
        Ok(ids) => ids,
        Err(MetaError::InodeNotFound(_)) => Vec::new(),
        Err(err) => {
            return Err(FsckError::Message(format!(
                "failed to remove orphan regular-file graph for ino {ino}: {err}"
            )));
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
            return Err(FsckError::Message(format!(
                "failed to remove orphan inode {ino}: {err}"
            )));
        }
    }

    Ok(())
}

fn orphan_directory_entries(
    ino: u64,
    entries: Result<Vec<DirEntry>, MetaError>,
) -> FsckResult<Option<Vec<DirEntry>>> {
    match entries {
        Ok(entries) => Ok(Some(entries)),
        Err(MetaError::InodeNotFound(_)) => Ok(None),
        Err(err) => Err(FsckError::Message(format!(
            "readdir failed for orphan dir {ino}: {err}"
        ))),
    }
}

fn repair_orphan_directory(
    meta: &dyn MetaEngine,
    ino: u64,
    removed_orphans: &mut usize,
    orphan_blocks_pending_gc: &mut usize,
    skipped_hardlink_orphans: &mut usize,
    skipped_dir_orphans: &mut usize,
    processed: &mut HashSet<u64>,
) -> FsckResult<()> {
    if !processed.insert(ino) {
        return Ok(());
    }

    let Some(entries) = orphan_directory_entries(ino, meta.readdir(ino))? else {
        return Ok(());
    };
    let mut has_unremoved_entries = false;
    for entry in entries {
        let entry_ino = entry.ino;
        let entry_name = entry.name;
        match entry.kind {
            FileType::Directory => {
                repair_orphan_directory(
                    meta,
                    entry_ino,
                    removed_orphans,
                    orphan_blocks_pending_gc,
                    skipped_hardlink_orphans,
                    skipped_dir_orphans,
                    processed,
                )?;
                match meta.rmdir(ino, &entry_name) {
                    Ok(()) | Err(MetaError::EntryNotFound { .. }) => {
                        *removed_orphans += 1;
                    }
                    Err(MetaError::InodeNotFound(_)) => {
                        *removed_orphans += 1;
                    }
                    Err(MetaError::DirNotEmpty(_)) => has_unremoved_entries = true,
                    Err(err) => {
                        return Err(FsckError::Message(format!(
                            "failed to remove orphan dir entry parent={ino} child={entry_ino}: {err}"
                        )));
                    }
                }
            }
            _ => {
                let attr = match meta.get_attr(entry_ino) {
                    Ok(attr) => Some(attr),
                    Err(MetaError::InodeNotFound(_)) => None,
                    Err(err) => {
                        return Err(FsckError::Message(format!(
                            "get_attr failed for orphan child ino {entry_ino} under parent {ino}: {err}"
                        )));
                    }
                };
                if attr.as_ref().is_some_and(|attr| attr.nlink > 1) {
                    *skipped_hardlink_orphans += 1;
                    has_unremoved_entries = true;
                    continue;
                }

                let removed = meta
                    .remove_entry_if_matches(ino, &entry_name, entry_ino)
                    .map_err(|err| {
                        FsckError::Message(format!(
                            "failed to remove orphan child entry parent={ino} child={entry_ino}: {err}"
                        ))
                    })?;
                if removed.is_none() {
                    has_unremoved_entries = true;
                    processed.insert(entry_ino);
                    continue;
                }

                if attr.is_some() {
                    remove_orphan_file(meta, entry_ino, orphan_blocks_pending_gc, removed_orphans)?;
                }
                processed.insert(entry_ino);
            }
        }
    }

    if has_unremoved_entries {
        let Some(entries) = orphan_directory_entries(ino, meta.readdir(ino))? else {
            return Ok(());
        };
        if !entries.is_empty() {
            *skipped_dir_orphans += 1;
            return Ok(());
        }
    }

    match meta.remove_attr(ino) {
        Ok(()) | Err(MetaError::InodeNotFound(_)) => *removed_orphans += 1,
        Err(err) => {
            return Err(FsckError::Message(format!(
                "failed to remove orphan directory inode {ino}: {err}"
            )));
        }
    }
    Ok(())
}

fn repair(meta: &dyn MetaEngine, report: &FsckReport) -> FsckResult<()> {
    let replayed = meta.recover_undo_logs()?;
    if !replayed.is_empty() {
        println!(
            "Replayed undo logs, touched {} parent dirs.",
            replayed.len()
        );
    }

    let mut removed_dangling = 0usize;
    let mut dangling_changed_or_gone = 0usize;
    let dangling = &report.dangling;
    let orphan_inodes = &report.orphan_inodes;

    for d in dangling {
        match meta.remove_entry_if_matches(d.parent, &d.name, d.ino) {
            Ok(Some(_)) => removed_dangling += 1,
            Ok(None) => dangling_changed_or_gone += 1,
            Err(err) => {
                return Err(FsckError::Message(format!(
                    "failed to remove dangling dentry parent={} ino={}: {err}",
                    d.parent, d.ino
                )));
            }
        }
    }

    let mut removed_orphans = 0usize;
    let mut skipped_dir_orphans = 0usize;
    let mut skipped_hardlink_orphans = 0usize;
    let mut orphan_blocks_pending_gc = 0usize;
    let mut processed_orphans = HashSet::new();

    for orphan in orphan_inodes {
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
                &mut skipped_dir_orphans,
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
    let nlink_mismatches = &post_cleanup.nlink_mismatches;
    for mismatch in nlink_mismatches {
        let mut attr = match meta.get_attr(mismatch.ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => continue,
            Err(err) => {
                return Err(FsckError::Message(format!(
                    "failed to read inode {} before nlink repair: {err}",
                    mismatch.ino
                )));
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
    println!(
        "  dangling entries changed or gone: {}",
        dangling_changed_or_gone
    );
    println!("  orphan inodes removed: {}", removed_orphans);
    println!("  orphan blocks pending gc: {}", orphan_blocks_pending_gc);
    println!("  orphan dirs skipped: {}", skipped_dir_orphans);
    println!("  orphan hardlinks skipped: {}", skipped_hardlink_orphans);
    println!("  nlink mismatches repaired: {}", repaired_nlinks);
    if skipped_dir_orphans > 0 {
        println!("  skipped orphan dirs: {skipped_dir_orphans}");
    }
    Ok(())
}

pub fn run<AutoRepair>(name: &str, meta_dir: &Path, auto_repair: AutoRepair) -> FsckResult<()>
where
    AutoRepair: Into<FsckRepairMode>,
{
    let auto_repair = auto_repair.into();
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(FsckError::LocalDbNotFound {
            path: db_path.display().to_string(),
        });
    }

    let meta = open_meta_engine_with_options(
        &db_path,
        MetaOpenOptions {
            background_poll: false,
            ..Default::default()
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

    let should_repair = match auto_repair {
        FsckRepairMode::Auto => true,
        FsckRepairMode::Ask => {
            println!();
            ask_should_repair()?
        }
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
    use std::ops::Deref;
    use studiofs_meta::engine::{MetaFsOps, MetaInodes};
    use studiofs_meta::{MetaNamespace, RedbMetaEngine};

    struct TestEngine {
        engine: RedbMetaEngine,
        _dir: tempfile::TempDir,
    }

    impl Deref for TestEngine {
        type Target = RedbMetaEngine;

        fn deref(&self) -> &Self::Target {
            &self.engine
        }
    }

    fn temp_engine() -> TestEngine {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let path = dir.path().join("fsck-test.db");
        let engine = RedbMetaEngine::open(&path).expect("open redb");
        engine.init_root(1000, 1000).expect("init root");
        TestEngine { engine, _dir: dir }
    }

    #[test]
    fn orphan_directory_readdir_error_keeps_inode_context() {
        let error =
            match orphan_directory_entries(42, Err(MetaError::Storage("read failed".into()))) {
                Err(FsckError::Message(error)) => error,
                other => panic!("expected contextual fsck error, got {other:?}"),
            };

        assert_eq!(
            error,
            "readdir failed for orphan dir 42: storage error: read failed"
        );
        assert!(
            orphan_directory_entries(42, Err(MetaError::InodeNotFound(42)))
                .expect("missing orphan directory is benign")
                .is_none()
        );
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

        let report = collect_report(&*engine).expect("collect pre-repair report");
        assert_eq!(report.orphan_inodes.len(), 3);

        repair(&*engine, &report).expect("repair orphan subtree");

        let post = collect_report(&*engine).expect("collect post-repair report");
        assert!(post.orphan_inodes.is_empty());
        assert!(post.nlink_mismatches.is_empty());
    }

    #[test]
    fn repair_preserves_replacement_for_stale_dangling_report() {
        let engine = temp_engine();
        let dangling = engine
            .create_file(ROOT_INO, b"entry", 1000, 1000, 0o644)
            .expect("create original entry");
        engine
            .remove_attr(dangling.ino)
            .expect("remove original inode");

        let report = collect_report(&*engine).expect("collect dangling report");
        assert_eq!(report.dangling.len(), 1);
        assert_eq!(report.dangling[0].ino, dangling.ino);

        engine
            .remove_entry(ROOT_INO, b"entry")
            .expect("remove stale binding");
        let replacement = engine
            .create_file(ROOT_INO, b"entry", 1000, 1000, 0o644)
            .expect("create healthy replacement");

        repair(&*engine, &report).expect("repair stale report");

        assert_eq!(
            engine
                .lookup(ROOT_INO, b"entry")
                .expect("replacement binding")
                .ino,
            replacement.ino
        );
    }

    #[test]
    fn repair_counts_and_preserves_orphan_directory_with_skipped_hardlink_child() {
        let engine = temp_engine();
        let orphan_dir = engine
            .create_dir(ROOT_INO, b"orphan", 1000, 1000, 0o755)
            .expect("create orphan directory");
        let child = engine
            .create_file(orphan_dir.ino, b"shared", 1000, 1000, 0o644)
            .expect("create child file");
        engine
            .link(child.ino, ROOT_INO, b"live-link")
            .expect("create live hardlink");
        engine
            .remove_entry(ROOT_INO, b"orphan")
            .expect("detach orphan directory");

        let mut removed_orphans = 0;
        let mut orphan_blocks_pending_gc = 0;
        let mut skipped_hardlink_orphans = 0;
        let mut skipped_dir_orphans = 0;
        let mut processed = HashSet::new();
        repair_orphan_directory(
            &*engine,
            orphan_dir.ino,
            &mut removed_orphans,
            &mut orphan_blocks_pending_gc,
            &mut skipped_hardlink_orphans,
            &mut skipped_dir_orphans,
            &mut processed,
        )
        .expect("repair orphan directory");

        assert_eq!(skipped_dir_orphans, 1);
        assert_eq!(skipped_hardlink_orphans, 1);
        assert_eq!(removed_orphans, 0);
        assert_eq!(orphan_blocks_pending_gc, 0);

        let report = collect_report(&*engine).expect("collect retained orphan report");
        repair(&*engine, &report).expect("repair retained orphan directory");
        assert!(engine.get_attr(orphan_dir.ino).is_ok());
        assert_eq!(
            engine
                .lookup(orphan_dir.ino, b"shared")
                .expect("preserved child binding")
                .ino,
            child.ino
        );
        assert_eq!(
            engine
                .lookup(ROOT_INO, b"live-link")
                .expect("live hardlink")
                .ino,
            child.ino
        );
    }

    #[test]
    fn fsck_parses_defaults() {
        let cli = crate::cli::parse_for_test(&["fsck", "vol"]).unwrap();
        match cli.command {
            crate::cli::Commands::Fsck(FsckArgs { name, repair }) => {
                assert_eq!(name, "vol");
                assert!(!repair);
            }
            other => panic!("expected Fsck, got {other:?}"),
        }
    }

    #[test]
    fn fsck_parses_repair_flag() {
        let cli = crate::cli::parse_for_test(&["fsck", "vol", "--repair"]).unwrap();
        match cli.command {
            crate::cli::Commands::Fsck(FsckArgs { repair, .. }) => assert!(repair),
            other => panic!("expected Fsck, got {other:?}"),
        }
    }
}
