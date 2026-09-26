use redb::ReadableTableMetadata;

const LOCAL_CHURN_CYCLES: usize = 4;
const LOCAL_CHURN_DEFAULT_FILES: usize = 32;
const LOCAL_CHURN_LIVE_FILES: usize = 4;

#[derive(Debug)]
struct LocalChurnTableRows {
    overlay_inodes: u64,
    base_inodes: u64,
    inode_tombstones: u64,
    inode_lifecycle: u64,
    overlay_dentries: u64,
    base_dentries: u64,
    overlay_dentry_index: u64,
    base_dentry_index: u64,
    dentry_tombstones: u64,
    overlay_chunks: u64,
    base_chunks: u64,
    pending_compaction: u64,
    overlay_xattrs: u64,
    base_xattrs: u64,
    xattr_tombstones: u64,
    overlay_acl_rules: u64,
    base_acl_rules: u64,
    acl_tombstones: u64,
    event_log: u64,
}

impl LocalChurnTableRows {
    fn total_rows(&self) -> u64 {
        self.overlay_inodes
            + self.base_inodes
            + self.inode_tombstones
            + self.inode_lifecycle
            + self.overlay_dentries
            + self.base_dentries
            + self.overlay_dentry_index
            + self.base_dentry_index
            + self.dentry_tombstones
            + self.overlay_chunks
            + self.base_chunks
            + self.pending_compaction
            + self.overlay_xattrs
            + self.base_xattrs
            + self.xattr_tombstones
            + self.overlay_acl_rules
            + self.base_acl_rules
            + self.acl_tombstones
            + self.event_log
    }

    fn metadata_rows(&self) -> u64 {
        self.total_rows() - self.event_log
    }
}

fn local_churn_table_rows(engine: &RedbMetaEngine) -> LocalChurnTableRows {
    let transaction = engine.begin_read_txn().expect("read churn table counts");
    LocalChurnTableRows {
        overlay_inodes: transaction
            .open_table(INODES_TABLE)
            .expect("open overlay inode table")
            .len()
            .expect("count overlay inode rows"),
        base_inodes: transaction
            .open_table(BASE_INODES_TABLE)
            .expect("open base inode table")
            .len()
            .expect("count base inode rows"),
        inode_tombstones: transaction
            .open_table(INODE_TOMBSTONES_TABLE)
            .expect("open inode tombstone table")
            .len()
            .expect("count inode tombstone rows"),
        inode_lifecycle: transaction
            .open_table(INODE_LIFECYCLE_TABLE)
            .expect("open inode lifecycle table")
            .len()
            .expect("count inode lifecycle rows"),
        overlay_dentries: transaction
            .open_table(DENTRIES_TABLE)
            .expect("open overlay dentry table")
            .len()
            .expect("count overlay dentry rows"),
        base_dentries: transaction
            .open_table(BASE_DENTRIES_TABLE)
            .expect("open base dentry table")
            .len()
            .expect("count base dentry rows"),
        overlay_dentry_index: transaction
            .open_table(DENTRIES_BY_INODE_TABLE)
            .expect("open overlay dentry index")
            .len()
            .expect("count overlay dentry index rows"),
        base_dentry_index: transaction
            .open_table(BASE_DENTRIES_BY_INODE_TABLE)
            .expect("open base dentry index")
            .len()
            .expect("count base dentry index rows"),
        dentry_tombstones: transaction
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .expect("open dentry tombstone table")
            .len()
            .expect("count dentry tombstone rows"),
        overlay_chunks: transaction
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .expect("open regular-file overlay table")
            .len()
            .expect("count regular-file overlay rows"),
        base_chunks: transaction
            .open_table(REGULAR_FILE_BASE_TABLE)
            .expect("open regular-file base table")
            .len()
            .expect("count regular-file base rows"),
        pending_compaction: transaction
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .expect("open pending regular-file compaction table")
            .len()
            .expect("count pending regular-file compaction rows"),
        overlay_xattrs: transaction
            .open_table(XATTRS_TABLE)
            .expect("open overlay xattr table")
            .len()
            .expect("count overlay xattr rows"),
        base_xattrs: transaction
            .open_table(BASE_XATTRS_TABLE)
            .expect("open base xattr table")
            .len()
            .expect("count base xattr rows"),
        xattr_tombstones: transaction
            .open_table(XATTR_TOMBSTONES_TABLE)
            .expect("open xattr tombstone table")
            .len()
            .expect("count xattr tombstone rows"),
        overlay_acl_rules: transaction
            .open_table(ACL_RULES_TABLE)
            .expect("open overlay ACL table")
            .len()
            .expect("count overlay ACL rows"),
        base_acl_rules: transaction
            .open_table(BASE_ACL_RULES_TABLE)
            .expect("open base ACL table")
            .len()
            .expect("count base ACL rows"),
        acl_tombstones: transaction
            .open_table(ACL_RULE_TOMBSTONES_TABLE)
            .expect("open ACL tombstone table")
            .len()
            .expect("count ACL tombstone rows"),
        event_log: transaction
            .open_table(EVENT_LOG_TABLE)
            .expect("open local event log")
            .len()
            .expect("count local event rows"),
    }
}

fn report_local_churn_checkpoint(
    engine: &RedbMetaEngine,
    checkpoint: &str,
    files_per_cycle: usize,
    live_dir: Ino,
    left_dir: Ino,
    right_dir: Ino,
) -> LocalStorageDiagnostics {
    invalidate_local_storage_page_stats(engine);
    let diagnostics = engine
        .local_storage_diagnostics()
        .expect("read local redb diagnostics")
        .expect("local redb diagnostics are available");
    assert!(diagnostics.physical_file_bytes.is_some_and(|bytes| bytes > 0));

    let visible_live_entries = engine.readdir(live_dir).expect("read live entries").len();
    assert_eq!(visible_live_entries, LOCAL_CHURN_LIVE_FILES);
    for slot in 0..LOCAL_CHURN_LIVE_FILES {
        let name = format!("survivor-{slot:04}");
        let entry = engine
            .lookup(live_dir, name.as_bytes())
            .expect("survivor dentry exists");
        assert_eq!(entry.kind, FileType::RegularFile);
        assert_eq!(
            engine
                .get_attr(entry.ino)
                .expect("survivor inode exists")
                .kind,
            FileType::RegularFile
        );
    }
    let visible_left_entries = engine.readdir(left_dir).expect("read left entries").len();
    let visible_right_entries = engine.readdir(right_dir).expect("read right entries").len();
    assert_eq!(
        visible_left_entries, 0,
        "left churn directory must be empty at checkpoint {checkpoint}"
    );
    assert_eq!(
        visible_right_entries, 0,
        "right churn directory must be empty at checkpoint {checkpoint}"
    );
    let table_rows = local_churn_table_rows(engine);
    assert_eq!(
        table_rows.overlay_dentries, table_rows.overlay_dentry_index,
        "overlay dentry index should match stored dentry rows"
    );
    assert_eq!(
        table_rows.base_dentries, table_rows.base_dentry_index,
        "base dentry index should match stored dentry rows"
    );
    assert_eq!(
        table_rows.pending_compaction, 0,
        "temporary-file compaction markers should be cleared at checkpoint {checkpoint}"
    );
    let tracked_table_row_total = table_rows.total_rows();
    let tracked_metadata_table_rows = table_rows.metadata_rows();

    println!(
        "SFS-733 checkpoint={checkpoint} files_per_cycle={files_per_cycle} \
         visible_live_entries={visible_live_entries} visible_left_entries={visible_left_entries} \
         visible_right_entries={visible_right_entries} \
         tracked_metadata_table_rows={tracked_metadata_table_rows} \
         tracked_table_row_total={tracked_table_row_total} table_rows={table_rows:?} \
         redb={diagnostics:?}"
    );
    diagnostics
}

#[test]
#[ignore = "manual local redb churn measurement; see docs/perf/local-redb-growth-workload.md"]
fn local_metadata_churn_redb_growth_workload() {
    let files_per_cycle = std::env::var("STUDIOFS_REDB_CHURN_FILES")
        .map_or(LOCAL_CHURN_DEFAULT_FILES, |value| {
            value
                .parse::<usize>()
                .expect("STUDIOFS_REDB_CHURN_FILES must be a positive integer")
        });
    assert!(files_per_cycle > 0, "STUDIOFS_REDB_CHURN_FILES must be positive");
    let compact_at_end = std::env::var("STUDIOFS_REDB_CHURN_COMPACT")
        .is_ok_and(|value| value == "1");

    let engine = temp_engine();
    let left = engine
        .create_dir(ROOT_INO, b"churn-left", 1000, 1000, 0o755)
        .expect("create left churn directory");
    let right = engine
        .create_dir(ROOT_INO, b"churn-right", 1000, 1000, 0o755)
        .expect("create right churn directory");
    let live = engine
        .create_dir(ROOT_INO, b"live", 1000, 1000, 0o755)
        .expect("create bounded live directory");
    for slot in 0..LOCAL_CHURN_LIVE_FILES {
        let name = format!("survivor-{slot:04}");
        engine
            .create_file(live.ino, name.as_bytes(), 1000, 1000, 0o644)
            .expect("create bounded live file");
    }

    report_local_churn_checkpoint(
        &engine,
        "initial",
        files_per_cycle,
        live.ino,
        left.ino,
        right.ino,
    );

    for cycle in 0..LOCAL_CHURN_CYCLES {
        for slot in 0..files_per_cycle {
            let directory_name = format!("dir-{cycle:04}-{slot:06}");
            let temporary_dir = engine
                .create_dir(left.ino, directory_name.as_bytes(), 1000, 1000, 0o755)
                .expect("create churn directory");
            let source_name = format!("source-{slot:06}");
            let file = engine
                .create_file(
                    temporary_dir.ino,
                    source_name.as_bytes(),
                    1000,
                    1000,
                    0o644,
                )
                .expect("create churn file");

            let xattr_name = b"user.sfs733-churn";
            let acl_principal = "sfs733-user";
            let published_rows = [
                MetaEvent::SetAttr { attr: file.clone() },
                MetaEvent::AddEntry {
                    parent: temporary_dir.ino,
                    name: source_name.as_bytes().to_vec(),
                    entry: DirEntryValue {
                        ino: file.ino,
                        kind: FileType::RegularFile,
                    },
                },
                MetaEvent::SetXattr {
                    ino: file.ino,
                    name: xattr_name.to_vec(),
                    value: b"published".to_vec(),
                    flags: 0,
                },
                MetaEvent::SetAclRule {
                    ino: file.ino,
                    principal: acl_principal.to_string(),
                    access_level: AccessLevel::ReadOnly,
                },
            ];
            engine
                .apply_replica_events_to_base(&published_rows)
                .expect("seed regular-file base rows");

            let mut chunk = RegularFileChunkState::default();
            chunk.insert_slice(SliceRecord::new(
                SliceId(1),
                SliceOrder(1),
                SliceRange::new(0, 512).expect("valid local metadata slice"),
                PayloadRef::new_partial(file.ino + 1, 512),
            ));
            set_regular_file_chunk_rows(
                &engine,
                file.ino,
                &[(ChunkId(0), chunk.clone())],
                &[(ChunkId(0), chunk)],
            );
            set_regular_file_pending_compaction_rows(&engine, file.ino, &[ChunkId(0)]);

            let mut attr = engine.get_attr(file.ino).expect("read churn file attr");
            attr.size = (cycle * files_per_cycle + slot + 1) as u64;
            engine.set_attr(&attr).expect("update churn file attr");

            engine
                .set_xattr(file.ino, xattr_name, b"before", 0)
                .expect("create churn xattr");
            let xattr_value = format!("cycle={cycle};slot={slot}");
            engine
                .set_xattr(file.ino, xattr_name, xattr_value.as_bytes(), 0)
                .expect("update churn xattr");
            engine
                .remove_xattr(file.ino, xattr_name)
                .expect("remove churn xattr");

            engine
                .set_acl_rule(file.ino, acl_principal, AccessLevel::ReadOnly)
                .expect("create churn ACL row");
            engine
                .set_acl_rule(file.ino, acl_principal, AccessLevel::ReadWrite)
                .expect("update churn ACL row");
            engine
                .remove_acl_rule(file.ino, acl_principal)
                .expect("remove churn ACL row");

            let renamed_name = format!("renamed-{slot:06}");
            engine
                .rename_atomic(
                    temporary_dir.ino,
                    source_name.as_bytes(),
                    temporary_dir.ino,
                    renamed_name.as_bytes(),
                )
                .expect("rename within churn directory");
            let moved_name = format!("moved-{cycle:04}-{slot:06}");
            engine
                .rename_atomic(
                    temporary_dir.ino,
                    renamed_name.as_bytes(),
                    right.ino,
                    moved_name.as_bytes(),
                )
                .expect("rename across churn directories");
            engine
                .unlink(right.ino, moved_name.as_bytes())
                .expect("unlink churn file");

            let retired_rows = [
                MetaEvent::RemoveEntry {
                    parent: temporary_dir.ino,
                    name: source_name.as_bytes().to_vec(),
                },
                MetaEvent::RemoveRegularFileGraph { ino: file.ino },
                MetaEvent::RemoveAttr { ino: file.ino },
                MetaEvent::RemoveXattr {
                    ino: file.ino,
                    name: xattr_name.to_vec(),
                },
                MetaEvent::RemoveAclRule {
                    ino: file.ino,
                    principal: acl_principal.to_string(),
                },
            ];
            engine
                .apply_replica_events_to_base(&retired_rows)
                .expect("retire regular-file base rows");
            engine
                .apply_replica_serving_state(&retired_rows)
                .expect("retire regular-file serving rows");
            engine
                .rmdir(left.ino, directory_name.as_bytes())
                .expect("remove empty churn directory");
        }

        let cycle_diagnostics = report_local_churn_checkpoint(
            &engine,
            &format!("cycle-{cycle:04}"),
            files_per_cycle,
            live.ino,
            left.ino,
            right.ino,
        );
        assert!(cycle_diagnostics.next_local_event_seq.is_some());
        assert!(cycle_diagnostics.retained_local_event_span.is_some());
    }

    if compact_at_end {
        let attempts_before = engine
            .local_storage_diagnostics()
            .expect("read pre-compaction redb diagnostics")
            .expect("pre-compaction redb diagnostics are available")
            .compaction_attempts;
        engine
            .compact_local_storage()
            .expect("compact local metadata database");
        let after = report_local_churn_checkpoint(
            &engine,
            "after-compaction",
            files_per_cycle,
            live.ino,
            left.ino,
            right.ino,
        );
        assert_eq!(after.compaction_attempts, attempts_before + 1);
        assert!(after.last_compaction_duration_ns.is_some());
    }
}
