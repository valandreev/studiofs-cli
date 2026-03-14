//! `studiofs fsck` — inspect and repair metadata inconsistencies.

use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use studiofs_meta::{MetaEngine, MetaError, MetadataBackend, ROOT_INO, open_meta_engine};
use studiofs_meta::engine::UndoLogKind;

#[derive(Debug, Clone)]
struct DanglingDentry {
    parent: u64,
    ino: u64,
    name: Vec<u8>,
}

#[derive(Debug, Default)]
struct FsckReport {
    dangling: Vec<DanglingDentry>,
    pending_creates: usize,
    deferred_deletes: usize,
    undo_pending_create: usize,
    undo_deferred_delete: usize,
}

impl FsckReport {
    fn has_findings(&self) -> bool {
        !self.dangling.is_empty()
            || self.pending_creates > 0
            || self.deferred_deletes > 0
            || self.undo_pending_create > 0
            || self.undo_deferred_delete > 0
    }
}

fn scan_dangling_dentries(meta: &dyn MetaEngine) -> anyhow::Result<Vec<DanglingDentry>> {
    let mut dangling = Vec::new();
    let mut stack = vec![ROOT_INO];
    let mut visited = HashSet::new();

    while let Some(parent) = stack.pop() {
        if !visited.insert(parent) {
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
                    if attr.kind == studiofs_meta::FileType::Directory {
                        stack.push(attr.ino);
                    }
                }
                Err(MetaError::InodeNotFound(_)) => dangling.push(DanglingDentry {
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

    Ok(dangling)
}

fn collect_report(meta: &dyn MetaEngine) -> anyhow::Result<FsckReport> {
    let pending_creates = meta.list_pending_creates()?;
    let deferred_deletes = meta.list_deferred_deletes()?;
    let undo_logs = meta.list_undo_logs()?;

    let mut report = FsckReport {
        dangling: scan_dangling_dentries(meta)?,
        pending_creates: pending_creates.len(),
        deferred_deletes: deferred_deletes.len(),
        ..FsckReport::default()
    };

    for row in undo_logs {
        match row.kind {
            UndoLogKind::PendingCreate => report.undo_pending_create += 1,
            UndoLogKind::DeferredDelete => report.undo_deferred_delete += 1,
        }
    }

    Ok(report)
}

fn print_report(name: &str, report: &FsckReport) {
    println!("FSCK report for volume '{}'", name);
    println!("────────────────────────────");
    println!("  dangling dentries:   {}", report.dangling.len());
    println!("  pending creates:     {}", report.pending_creates);
    println!("  deferred deletes:    {}", report.deferred_deletes);
    println!("  undo pending_create: {}", report.undo_pending_create);
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
}

fn ask_should_repair() -> anyhow::Result<bool> {
    print!("Repair all detected issues now? [y/N]: ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

fn repair(meta: &dyn MetaEngine, report: &FsckReport) -> anyhow::Result<()> {
    let replayed = meta.recover_undo_logs()?;
    if !replayed.is_empty() {
        println!("Replayed undo logs, touched {} parent dirs.", replayed.len());
    }

    let mut removed = 0usize;
    let mut already_gone = 0usize;
    for d in &report.dangling {
        match meta.remove_entry(d.parent, &d.name) {
            Ok(_) => removed += 1,
            Err(MetaError::EntryNotFound { .. }) => already_gone += 1,
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

    println!("Repair summary:");
    println!("  replayed undo parents: {}", replayed.len());
    println!("  dangling entries removed: {}", removed);
    println!("  dangling already gone: {}", already_gone);
    Ok(())
}

pub fn run(name: &str, meta_dir: &Path, auto_repair: bool) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        anyhow::bail!("Volume '{}' not found at {}", name, db_path.display());
    }

    let meta = open_meta_engine(&db_path)?;
    let format_info = meta.get_format_info()?;

    if format_info.metadata.backend == MetadataBackend::Postgres {
        println!("Syncing local metadata from PostgreSQL before fsck...");
        meta.sync_metadata()?;
    }

    let report = collect_report(meta.as_ref())?;
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
    let post = collect_report(meta.as_ref())?;
    print_report(name, &post);
    if post.has_findings() {
        println!("Some issues remain; rerun fsck or inspect logs.");
    } else {
        println!("Repair completed cleanly.");
    }

    Ok(())
}
