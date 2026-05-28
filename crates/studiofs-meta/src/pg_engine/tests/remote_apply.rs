use super::*;
use crate::engine::MetaEngine;
use crate::redb_engine::layout::encode_dentry_key;

#[test]
fn remote_apply_coordinator_does_not_capture_pg_engine_state() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("pg_engine")
            .join("remote_apply.rs"),
    )
    .expect("read remote_apply source");
    let code_without_line_comments = source
        .lines()
        .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        !code_without_line_comments.contains("PgMetaEngine")
            && !code_without_line_comments.contains("engine: &")
            && !code_without_line_comments.contains("self.engine"),
        "RemoteApplyCoordinator must not retain or reach through broad PgMetaEngine state"
    );
}

#[test]
fn project_remote_events_filters_unpublished_overlay_slices() {
    let events = vec![
        MetaEvent::SetRegularFileOverlaySlices {
            ino: 42,
            chunk_slices: vec![(
                ChunkId(0),
                vec![SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 4096).unwrap(),
                    payload: Some(PayloadRef::new_partial(9001, 4096)),
                    payload_state: PayloadRefState::LocalOnly,
                }],
            )],
        },
        MetaEvent::RemoveRegularFileGraph { ino: 42 },
        MetaEvent::PublishRegularFileBaseSlices {
            ino: 42,
            chunk_slices: vec![(
                ChunkId(0),
                vec![SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 4096).unwrap(),
                    payload: Some(PayloadRef::new_partial(9002, 4096)),
                    payload_state: PayloadRefState::Published,
                }],
            )],
        },
    ];

    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    assert_eq!(projected.len(), 1);
    assert!(matches!(
        projected.first(),
        Some(MetaEvent::PublishRegularFileBaseSlices { ino: 42, .. })
    ));
}

#[test]
fn project_local_batch_hides_uncommitted_regular_file_namespace() {
    let mut draft = InodeAttr::new_file(ROOT_INO + 10, ROOT_INO, 0, 0, 0o644);
    draft.size = 4096;
    draft.content_epoch = 1;
    draft.committed_size = 0;
    draft.committed_content_epoch = 0;
    draft.upload_state = UploadState::Uploading;

    let projected = RemoteApplyCoordinator::project_local_batch(&[
        MetaEvent::SetAttr {
            attr: draft.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"draft.bin".to_vec(),
            entry: DirEntryValue {
                ino: draft.ino,
                kind: FileType::RegularFile,
            },
        },
        MetaEvent::SetRegularFileOverlaySlices {
            ino: draft.ino,
            chunk_slices: vec![(
                ChunkId(0),
                vec![SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 4096).unwrap(),
                    payload: Some(PayloadRef::new_partial(9001, 4096)),
                    payload_state: PayloadRefState::LocalOnly,
                }],
            )],
        },
    ]);

    assert!(
        !projected.events.iter().any(
            |event| matches!(event, MetaEvent::AddEntry { entry, .. } if entry.ino == draft.ino)
                || matches!(event, MetaEvent::SetRegularFileOverlaySlices { ino, .. } if *ino == draft.ino)
        ),
        "local draft namespace and payload state must not be projected to PG"
    );
    assert!(
        projected
            .events
            .iter()
            .any(|event| matches!(event, MetaEvent::SetAttr { attr } if attr.ino == draft.ino)),
        "uploading attr metadata remains batchable while the namespace stays hidden"
    );
}

#[test]
fn project_local_batch_hides_uncommitted_regular_file_namespace_from_local_log() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let draft = pg
        .create_file(ROOT_INO, b"log-draft.bin", 0, 0, 0o644)
        .expect("create draft");
    set_file_state(&pg, draft.ino, 4096, 1);
    pg.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(9003, 4096))
        .expect("stage local-only payload");
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let events = pg.local.read_events_range(start, end).expect("read events");
    let projected = RemoteApplyCoordinator::project_local_batch(&events);

    assert!(
        !projected.events.iter().any(
            |event| matches!(event, MetaEvent::AddEntry { entry, .. } if entry.ino == draft.ino)
        ),
        "local event log projection must hide the draft dentry"
    );

    let batch = pg
        .claim_pending_batch(true)
        .expect("claim pending draft batch");
    let claimed_events = pg
        .local
        .read_events_range(batch.event_start, batch.event_end)
        .expect("read claimed events");
    let claimed_projected = RemoteApplyCoordinator::project_local_batch(&claimed_events);
    assert!(
        !claimed_projected.events.iter().any(
            |event| matches!(event, MetaEvent::AddEntry { entry, .. } if entry.ino == draft.ino)
        ),
        "claimed local batch projection must hide the draft dentry"
    );
}

#[test]
fn project_local_batch_hides_brand_new_empty_regular_file_namespace() {
    let draft = InodeAttr::new_file(ROOT_INO + 11, ROOT_INO, 0, 0, 0o644);

    let projected = RemoteApplyCoordinator::project_local_batch(&[
        MetaEvent::SetAttr {
            attr: draft.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"empty-draft.bin".to_vec(),
            entry: DirEntryValue {
                ino: draft.ino,
                kind: FileType::RegularFile,
            },
        },
    ]);

    assert!(
        !projected.events.iter().any(
            |event| matches!(event, MetaEvent::AddEntry { entry, .. } if entry.ino == draft.ino)
        ),
        "brand-new empty local files are not remote-visible until they publish data"
    );
}

#[test]
fn remote_snapshot_hides_uncommitted_regular_file_namespace() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let draft = pg
        .create_file(ROOT_INO, b"snapshot-draft.bin", 0, 0, 0o644)
        .expect("create draft");
    set_file_state(&pg, draft.ino, 4096, 1);
    pg.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(9002, 4096))
        .expect("stage local-only payload");

    let snapshot = pg
        .local
        .export_remote_snapshot()
        .expect("export remote snapshot");

    assert!(
        !snapshot.inodes.iter().any(|(ino, _)| *ino == draft.ino),
        "remote snapshot must hide the uncommitted regular-file inode"
    );
    assert!(
        !snapshot.dentries.iter().any(|(_, raw)| {
            postcard::from_bytes::<DirEntryValue>(raw)
                .map(|entry| entry.ino == draft.ino)
                .unwrap_or(false)
        }),
        "remote snapshot must hide dentries pointing at the uncommitted inode"
    );
}

#[test]
fn remote_snapshot_hides_brand_new_empty_regular_file_namespace() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let draft = pg
        .create_file(ROOT_INO, b"snapshot-empty-draft.bin", 0, 0, 0o644)
        .expect("create empty draft");

    let snapshot = pg
        .local
        .export_remote_snapshot()
        .expect("export remote snapshot");

    assert!(
        !snapshot.inodes.iter().any(|(ino, _)| *ino == draft.ino),
        "remote snapshot must hide a brand-new empty local file"
    );
    assert!(
        !snapshot.dentries.iter().any(|(_, raw)| {
            postcard::from_bytes::<DirEntryValue>(raw)
                .map(|entry| entry.ino == draft.ino)
                .unwrap_or(false)
        }),
        "remote snapshot must hide dentries pointing at a brand-new empty local file"
    );
}

#[test]
fn project_local_batch_publishes_explicit_empty_regular_file_namespace() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let start = pg.local.get_counter("nextEventSeq").expect("start seq");
    let file = pg
        .create_file(ROOT_INO, b"published-empty.bin", 0, 0, 0o644)
        .expect("create empty file");
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 0)
            .expect("publish empty file"),
        "empty file publish should succeed"
    );
    let end = pg.local.get_counter("nextEventSeq").expect("end seq");

    let events = pg
        .local
        .read_events_range(start, end)
        .expect("read empty publish events");
    let projected = RemoteApplyCoordinator::project_local_batch(&events);

    assert!(
        projected.events.iter().any(
            |event| matches!(event, MetaEvent::AddEntry { entry, .. } if entry.ino == file.ino)
        ),
        "explicit empty-file publish must make the dentry remote-visible"
    );
    assert!(
        projected.events.iter().any(
            |event| matches!(event, MetaEvent::SetAttr { attr } if attr.ino == file.ino
                && attr.size == 0
                && attr.committed_size == 0
                && attr.content_epoch == 1
                && attr.committed_content_epoch == 1)
        ),
        "explicit empty-file publish must advance the committed generation"
    );

    apply_foreign_batch(
        &remote,
        &projected.events,
        &OverlayConflictSets::default(),
        &std::collections::HashSet::new(),
    )
    .expect("apply empty publish to remote peer");
    let remote_entry = remote
        .lookup(ROOT_INO, b"published-empty.bin")
        .expect("remote peer should see published empty file");
    assert_eq!(remote_entry.ino, file.ino);
    let remote_attr = remote
        .get_attr(file.ino)
        .expect("remote peer should have published empty attr");
    assert_eq!(remote_attr.size, 0);
    assert_eq!(remote_attr.committed_size, 0);
    assert_eq!(remote_attr.content_epoch, 1);
    assert_eq!(remote_attr.committed_content_epoch, 1);
}

#[test]
fn self_batch_acknowledges_projected_prefix_with_filtered_overlay_events() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"projected-self-ack.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");

    let payload = PayloadRef::new_partial(77_004, 4096);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base graph"),
        "base graph publish must succeed"
    );
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    assert!(
        events.len() > projected.len(),
        "setup should include local-only overlay events before the remote-visible publish"
    );

    pg.flushed_event_seq.store(start, Ordering::SeqCst);
    acknowledge_local_prefix_from_self_batch(&pg, &projected).expect("ack projected self batch");

    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        end,
        "self-batch ack must advance over the raw local events that produced the projected batch"
    );
}

#[test]
fn self_batch_namespace_mutation_advances_namespace_version_token() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let attr = InodeAttr::new_file(ROOT_INO + 10, ROOT_INO, 0, 0, 0o644);
    let batch_version = 17;

    apply_decoded_foreign_batches_locked(
        &pg,
        vec![DecodedReplayBatch {
            batch_version,
            events: vec![
                MetaEvent::SetAttr { attr },
                MetaEvent::AddEntry {
                    parent: ROOT_INO,
                    name: b"self-visible-namespace.txt".to_vec(),
                    entry: DirEntryValue {
                        ino: ROOT_INO + 10,
                        kind: FileType::RegularFile,
                    },
                },
            ],
            is_self_batch: true,
        }],
        None,
        0,
    )
    .expect("apply visible self namespace batch");

    assert_eq!(
        pg.local_version.load(Ordering::SeqCst),
        batch_version,
        "self-visible replay must still advance the metadata token"
    );
    assert_eq!(
        pg.namespace_version.load(Ordering::SeqCst),
        batch_version,
        "self-visible namespace replay must advance the namespace token for VFS invalidation"
    );
}

#[test]
fn foreign_unlink_after_local_rename_removes_destination_alias() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"file.txt", 77_010);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    pg.rename(ROOT_INO, b"file.txt", ROOT_INO, b"renamed.txt")
        .expect("local rename");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_ok());

    apply_foreign_batch(
        &pg,
        &[
            MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: b"file.txt".to_vec(),
            },
            MetaEvent::RemoveAttr { ino: file.ino },
        ],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply foreign unlink");

    assert!(pg.lookup(ROOT_INO, b"file.txt").is_err());
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_err());
    assert!(pg.get_attr(file.ino).is_err());
}

#[test]
fn foreign_unlink_during_local_append_removes_serving_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"append-target.txt", 77_011);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, file.ino, 64 + u64::from(DEFAULT_BLOCK_SIZE), 2);
    let local_tail = PayloadRef::new_partial(77_012, DEFAULT_BLOCK_SIZE);
    pg.stage_payload_slice(file.ino, 1, &local_tail)
        .expect("stage local append overlay");
    let local_append_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"append-target.txt").is_ok());
    let batch = [
        MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"append-target.txt".to_vec(),
        },
        MetaEvent::RemoveAttr { ino: file.ino },
    ];
    let current_regular_file_conflicts = RemoteApplyCoordinator::new(&pg.local)
        .current_regular_file_overlay_conflicts_for_batch(&batch)
        .expect("collect current regular-file conflicts");
    assert!(
        current_regular_file_conflicts
            .block_inodes
            .contains(&file.ino)
    );
    assert!(
        current_regular_file_conflicts.dentry_keys.is_empty(),
        "regular-file overlay protection must not mask the foreign unlink path"
    );

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"append-target.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "foreign unlink must remove namespace visibility even when local append state is preserved"
    );
    assert!(
        pg.get_attr(file.ino).is_ok(),
        "accepted local payload state may remain observable by inode"
    );

    mark_overlay_objects_uploaded(&pg, file.ino, 2, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish local append"),
        "local append publish should still succeed by inode"
    );
    let local_append_end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(local_append_start, local_append_end)
        .expect("read local append events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local append self batch");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"append-target.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "self-ack of the local append must not resurrect the deleted path"
    );
}

#[test]
fn foreign_unlink_during_active_open_handle_preserves_inode_binding() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"open-target.txt", 77_210);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");
    pg.note_active_open_inode(file.ino);

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.remove_attr_inodes.contains(&file.ino));
    assert!(!overlay.attr_inodes.contains(&file.ino));
    assert!(!overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"open-target.txt").is_ok());

    let batch = [
        MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"open-target.txt".to_vec(),
        },
        MetaEvent::RemoveAttr { ino: file.ino },
    ];

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"open-target.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "foreign unlink must remove namespace visibility for new opens"
    );
    assert!(
        pg.get_attr(file.ino).is_ok(),
        "active open handles must keep their inode binding alive by ino"
    );

    pg.note_inactive_open_inode(file.ino);
    let overlay = pg.local_overlay_conflicts().expect("overlay after close");
    assert!(!overlay.remove_attr_inodes.contains(&file.ino));
    assert!(!overlay.attr_inodes.contains(&file.ino));
    assert!(!overlay.block_inodes.contains(&file.ino));
}

#[test]
fn active_open_handle_can_flush_dirty_attr_after_foreign_unlink() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"open-target.txt", 77_212);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");
    pg.note_active_open_inode(file.ino);

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let batch = [
        MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"open-target.txt".to_vec(),
        },
        MetaEvent::RemoveAttr { ino: file.ino },
    ];
    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    let mut dirty_attr = pg
        .get_attr(file.ino)
        .expect("active open handle keeps attr visible");
    dirty_attr.size += 7;
    dirty_attr.committed_size = dirty_attr.size;
    dirty_attr.touch_mtime();

    pg.flush_regular_file_overlay_slices_with_attr(file.ino, &[], Some(&dirty_attr))
        .expect("dirty attr flush after active foreign unlink");

    assert_eq!(
        pg.get_attr(file.ino).expect("attr after dirty flush").size,
        dirty_attr.size
    );
}

#[test]
fn active_open_handle_does_not_hide_foreign_content_publish() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"publish-target.txt", 77_211);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");
    pg.note_active_open_inode(file.ino);

    let mut remote_attr = pg.get_attr(file.ino).expect("load local attr");
    remote_attr.size = 512;
    remote_attr.committed_size = 512;
    remote_attr.content_epoch = remote_attr.content_epoch.saturating_add(1);
    remote_attr.committed_content_epoch = remote_attr.content_epoch;
    remote_attr.mtime = remote_attr.mtime.saturating_add(1);
    remote_attr.ctime = remote_attr.ctime.saturating_add(1);

    let remote_payload = PayloadRef::new_partial(77_212, 512);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(0, 512).expect("remote publish range"),
                payload: Some(remote_payload),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: remote_attr }, remote_publish],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply foreign publish");

    let applied = pg.get_attr(file.ino).expect("load applied attr");
    assert_eq!(applied.size, 512);
    assert_eq!(applied.committed_size, 512);
}

#[test]
fn foreign_rename_replace_during_local_append_removes_source_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let source = create_published_small_file(&pg, ROOT_INO, b"slot-3.txt", 77_112);
    let target = create_published_small_file(&pg, ROOT_INO, b"slot-3-renamed.txt", 77_113);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, source.ino, 227, 2);
    let local_tail = PayloadRef::new_partial(77_114, 163);
    pg.stage_payload_slice(source.ino, 1, &local_tail)
        .expect("stage local append overlay");
    let local_append_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&source.ino));
    assert!(pg.lookup(ROOT_INO, b"slot-3.txt").is_ok());

    let rename_replace = MetaEvent::RenameEntry {
        parent: ROOT_INO,
        name: b"slot-3.txt".to_vec(),
        new_parent: ROOT_INO,
        new_name: b"slot-3-renamed.txt".to_vec(),
        entry: DirEntryValue {
            ino: source.ino,
            kind: FileType::RegularFile,
        },
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: Some(target.ino),
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    };

    apply_foreign_batch(&pg, &[rename_replace], &overlay, &HashSet::new())
        .expect("apply foreign rename replace");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"slot-3.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "foreign rename/replace must remove the old source path even with local append state"
    );
    let renamed = pg
        .lookup(ROOT_INO, b"slot-3-renamed.txt")
        .expect("lookup renamed target");
    assert_eq!(renamed.ino, source.ino);
    assert!(pg.get_attr(target.ino).is_err());

    mark_overlay_objects_uploaded(&pg, source.ino, 2, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(source.ino, 2)
            .expect("publish local append"),
        "local append publish should still succeed by inode after source rename"
    );
    let local_append_end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(local_append_start, local_append_end)
        .expect("read local append events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local append self batch");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"slot-3.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "self-ack of the local append must not resurrect the renamed source path"
    );
    let renamed = pg
        .lookup(ROOT_INO, b"slot-3-renamed.txt")
        .expect("lookup renamed target after self-ack");
    assert_eq!(renamed.ino, source.ino);
}

#[test]
fn foreign_unlink_during_local_uploading_preserves_writer_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"uploading-target.txt", 77_014);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, file.ino, 64, 2);
    let local_payload = PayloadRef::new_partial(77_015, 64);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage active local upload overlay");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"uploading-target.txt").is_ok());

    let batch = [
        MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"uploading-target.txt".to_vec(),
        },
        MetaEvent::RemoveAttr { ino: file.ino },
    ];
    let current_regular_file_conflicts = RemoteApplyCoordinator::new(&pg.local)
        .current_regular_file_overlay_conflicts_for_batch(&batch)
        .expect("collect current regular-file conflicts");
    assert!(
        current_regular_file_conflicts
            .dentry_keys
            .contains(&encode_dentry_key(ROOT_INO, b"uploading-target.txt")),
        "active writer-local uploads must keep the serving dentry private"
    );

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        pg.lookup(ROOT_INO, b"uploading-target.txt").is_ok(),
        "foreign unlink must not remove an active writer-local uploading path"
    );
    let local_attr = pg.get_attr(file.ino).expect("local attr after unlink");
    assert_eq!(local_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_first_object_id(&pg, file.ino)
            .expect("lookup local payload")
            .expect("local payload remains visible"),
        local_payload.object_id
    );
}

#[test]
fn foreign_unlink_during_unpublished_local_uploading_preserves_writer_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"new-uploading-target.txt", 0, 0, 0o644)
        .expect("create unpublished local file");
    set_file_state(&pg, file.ino, 64, 1);
    let local_payload = PayloadRef::new_partial(77_016, 64);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage unpublished local upload overlay");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"new-uploading-target.txt").is_ok());

    let batch = [MetaEvent::RemoveEntry {
        parent: ROOT_INO,
        name: b"new-uploading-target.txt".to_vec(),
    }];
    let current_regular_file_conflicts = RemoteApplyCoordinator::new(&pg.local)
        .current_regular_file_overlay_conflicts_for_batch(&batch)
        .expect("collect current regular-file conflicts");
    assert!(
        current_regular_file_conflicts
            .dentry_keys
            .contains(&encode_dentry_key(ROOT_INO, b"new-uploading-target.txt")),
        "unpublished writer-local uploads must keep the serving dentry private"
    );

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        pg.lookup(ROOT_INO, b"new-uploading-target.txt").is_ok(),
        "foreign unlink must not remove an unpublished writer-local uploading path"
    );
}

#[test]
fn recent_flush_dentry_reconcile_applies_deferred_foreign_base_entry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let local_file = pg
        .create_file(ROOT_INO, b"slot.txt", 0, 0, 0o644)
        .expect("create local slot");
    let local_end = pg.local.get_counter("nextEventSeq").expect("local end");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let slot_key = encode_dentry_key(ROOT_INO, b"slot.txt");
    assert!(
        overlay.dentry_keys.contains(&slot_key),
        "local create should protect the serving dentry key"
    );

    let remote_ino = 77_017;
    let mut remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 0, 0, 0o644);
    remote_attr.size = 1;
    remote_attr.committed_size = 1;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    let remote_entry = DirEntryValue {
        ino: remote_ino,
        kind: FileType::RegularFile,
    };
    let remote_batch = [
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"slot.txt".to_vec(),
            entry: remote_entry,
        },
    ];

    apply_foreign_batch(&pg, &remote_batch, &overlay, &HashSet::new())
        .expect("apply deferred foreign dentry");
    assert_eq!(
        pg.lookup(ROOT_INO, b"slot.txt")
            .expect("local slot still visible")
            .ino,
        local_file.ino,
        "conflicting foreign AddEntry must not replace the local serving key yet"
    );

    pg.persist_flushed_event_seq(local_end)
        .expect("ack local create");
    let overlay_after_ack = pg.local_overlay_conflicts().expect("overlay after ack");
    assert!(
        !overlay_after_ack.dentry_keys.contains(&slot_key),
        "the local dentry key should be safe to reconcile after self-ack"
    );

    assert!(
        RemoteApplyCoordinator::new(&pg.local)
            .reconcile_serving_dentries_from_base_with_overlay(
                &HashSet::from([slot_key]),
                &overlay_after_ack,
            )
            .expect("reconcile deferred dentry"),
        "deferred base dentry should be replayed into serving"
    );
    assert_eq!(
        pg.lookup(ROOT_INO, b"slot.txt")
            .expect("remote slot visible after reconcile")
            .ino,
        remote_ino
    );
}

#[test]
fn foreign_unready_add_entry_does_not_hide_visible_local_regular_file() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let local_file = create_published_small_file(&pg, ROOT_INO, b"slot.txt", 77_018);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    let remote_ino = 77_019;
    let mut remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 0, 0, 0o644);
    remote_attr.size = 227;
    remote_attr.content_epoch = 1;
    remote_attr.upload_state = UploadState::Updating;
    let remote_entry = DirEntryValue {
        ino: remote_ino,
        kind: FileType::RegularFile,
    };
    let remote_batch = [
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"slot.txt".to_vec(),
            entry: remote_entry,
        },
    ];

    let (_changed, protected_overlay) = apply_foreign_batch(
        &pg,
        &remote_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply unready foreign add entry");
    assert!(
        protected_overlay
            .dentry_keys
            .contains(&encode_dentry_key(ROOT_INO, b"slot.txt")),
        "unready foreign regular-file dentry must be deferred until publish"
    );
    assert_eq!(
        pg.lookup(ROOT_INO, b"slot.txt")
            .expect("local slot remains visible")
            .ino,
        local_file.ino,
        "an unready foreign AddEntry must not hide the current serving source path"
    );
}

#[test]
fn foreign_unready_add_entry_without_local_slot_becomes_visible_for_zero_convergence() {
    let remote_ino = 77_020;
    let mut remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 0, 0, 0o644);
    remote_attr.upload_state = UploadState::Updating;
    let remote_entry = DirEntryValue {
        ino: remote_ino,
        kind: FileType::RegularFile,
    };
    let remote_batch = [
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"remote-draft.txt".to_vec(),
            entry: remote_entry,
        },
    ];

    let (_overlay_dir, overlay_pg) = temp_pg_engine_for_unit();
    let (_changed, protected_overlay) = apply_foreign_batch(
        &overlay_pg,
        &remote_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply unready foreign add entry without local slot");
    assert!(
        !protected_overlay
            .dentry_keys
            .contains(&encode_dentry_key(ROOT_INO, b"remote-draft.txt")),
        "new unready remote dentry should not be deferred when no local serving path exists"
    );

    let (_dir, pg) = temp_pg_engine_for_unit();
    apply_decoded_foreign_batches_locked(
        &pg,
        vec![DecodedReplayBatch {
            batch_version: 9,
            events: remote_batch.to_vec(),
            is_self_batch: false,
        }],
        None,
        0,
    )
    .expect("apply full decoded replay");
    assert_eq!(
        pg.lookup(ROOT_INO, b"remote-draft.txt")
            .expect("remote draft should be visible")
            .ino,
        remote_ino
    );
}

#[test]
fn foreign_base_publish_preserves_unflushed_local_overlay_payload() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"payload.txt", 77_020);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, file.ino, 64, 2);
    let local_payload = PayloadRef::new_partial(77_021, 64);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));

    let remote_payload = PayloadRef::new_partial(77_022, 64);
    let mut remote_slice = SliceRecord::new(
        SliceId(0),
        SliceOrder(0),
        SliceRange::new(0, 64).expect("remote slice range"),
        remote_payload,
    );
    remote_slice.payload_state = PayloadRefState::Published;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: vec![(ChunkId(0), vec![remote_slice])],
        }],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply foreign publish");

    let object_ids: Vec<_> = pg
        .lookup_regular_file_slices(file.ino, 0, 64)
        .expect("lookup local payload")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![local_payload.object_id],
        "unflushed local payload must remain visible over a conflicting remote publish"
    );
}

#[test]
fn decoded_replay_coalesces_regular_file_serving_reconciliation() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let first = create_published_small_file(&pg, ROOT_INO, b"first.bin", 77_023);
    let second = create_published_small_file(&pg, ROOT_INO, b"second.bin", 77_024);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    let mut first_remote = pg.get_attr(first.ino).expect("first attr");
    first_remote.size = 128;
    first_remote.committed_size = 128;
    first_remote.content_epoch = 2;
    first_remote.committed_content_epoch = 2;
    first_remote.mtime = first_remote.mtime.saturating_add(1);
    first_remote.ctime = first_remote.ctime.saturating_add(1);

    let mut second_remote = pg.get_attr(second.ino).expect("second attr");
    second_remote.size = 256;
    second_remote.committed_size = 256;
    second_remote.content_epoch = 2;
    second_remote.committed_content_epoch = 2;
    second_remote.mtime = second_remote.mtime.saturating_add(1);
    second_remote.ctime = second_remote.ctime.saturating_add(1);

    RedbMetaEngine::reset_regular_file_serving_reconcile_call_count_for_test();
    let changed = apply_decoded_foreign_batches_locked(
        &pg,
        vec![
            DecodedReplayBatch {
                batch_version: 1,
                events: vec![MetaEvent::SetAttr { attr: first_remote }],
                is_self_batch: false,
            },
            DecodedReplayBatch {
                batch_version: 2,
                events: vec![MetaEvent::SetAttr {
                    attr: second_remote,
                }],
                is_self_batch: false,
            },
        ],
        None,
        0,
    )
    .expect("apply decoded replay batches");

    assert!(changed, "replay should report applied metadata changes");
    assert_eq!(
        RedbMetaEngine::regular_file_serving_reconcile_call_count_for_test(),
        1,
        "regular-file serving reconciliation should be coalesced for the replay pass"
    );
    assert_eq!(pg.local_version.load(Ordering::SeqCst), 2);
    assert_eq!(pg.get_attr(first.ino).expect("first after").size, 128);
    assert_eq!(pg.get_attr(second.ino).expect("second after").size, 256);
}

#[test]
fn protected_attr_merge_accepts_newer_remote_shrink_epoch() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-shrink.txt", 77_030);
    let mut current = pg.get_attr(file.ino).expect("load local attr");
    current.size = 2 * 1024 * 1024;
    current.committed_size = current.size;
    current.content_epoch = 5;
    current.committed_content_epoch = 5;
    current.upload_state = UploadState::Ready;
    pg.set_attr(&current).expect("seed local protected attr");

    let mut remote = current.clone();
    remote.size = 1024 * 1024;
    remote.committed_size = remote.size;
    remote.content_epoch = 6;
    remote.committed_content_epoch = 6;
    remote.mtime = remote.mtime.saturating_add(1);
    remote.ctime = remote.ctime.saturating_add(1);

    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: remote }],
        &OverlayConflictSets::default(),
        &HashSet::from([file.ino]),
    )
    .expect("apply newer remote shrink");

    let applied = pg.get_attr(file.ino).expect("load applied attr");
    assert_eq!(
        applied.size,
        1024 * 1024,
        "newer remote content epoch must be allowed to shrink a protected inode"
    );
    assert_eq!(applied.content_epoch, 6);
    assert_eq!(applied.committed_content_epoch, 6);
}

#[test]
fn protected_attr_merge_preserves_larger_size_for_newer_remote_payload_publish() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-newer-write.txt", 77_035);
    let mut current = pg.get_attr(file.ino).expect("load local attr");
    current.size = 3_146_061;
    current.committed_size = current.size;
    current.content_epoch = 5;
    current.committed_content_epoch = 5;
    current.upload_state = UploadState::Ready;
    pg.set_attr(&current).expect("seed local protected attr");

    let mut remote = current.clone();
    remote.size = 3_145_926;
    remote.committed_size = remote.size;
    remote.content_epoch = 6;
    remote.committed_content_epoch = 6;
    remote.mtime = remote.mtime.saturating_add(1);
    remote.ctime = remote.ctime.saturating_add(1);

    let remote_tail = PayloadRef::new_partial(77_036, 26);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(
                    3_145_900 % DEFAULT_CHUNK_SIZE,
                    (3_145_900 % DEFAULT_CHUNK_SIZE) + 26,
                )
                .expect("remote tail range"),
                payload: Some(remote_tail),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };

    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: remote }, remote_publish],
        &OverlayConflictSets::default(),
        &HashSet::from([file.ino]),
    )
    .expect("apply newer remote write publish");

    let applied = pg.get_attr(file.ino).expect("load applied attr");
    assert_eq!(
        applied.size, current.size,
        "a newer remote write publish must not shrink a protected larger logical EOF"
    );
    assert_eq!(applied.content_epoch, 6);
    assert_eq!(applied.committed_content_epoch, 6);
}

#[test]
fn protected_foreign_publish_does_not_get_replaced_by_aux_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-aux-publish.txt", 77_040);
    let mut attr = pg.get_attr(file.ino).expect("load local attr");
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = (3 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    pg.set_attr(&attr).expect("seed local protected attr");

    let local_tail = PayloadRef::new_partial(77_041, 4096);
    pg.stage_payload_slice(file.ino, 3, &local_tail)
        .expect("stage local tail");
    mark_overlay_objects_uploaded(&pg, file.ino, 3, &[local_tail.object_id]);
    assert!(
        pg.local
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .expect("publish local tail")
    );

    let remote_middle = PayloadRef::new_partial(77_042, 4096);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(2, Some(&remote_middle), PayloadRefState::Published)
            .expect("remote middle slices"),
    };
    apply_foreign_batch(
        &pg,
        &[remote_publish],
        &OverlayConflictSets::default(),
        &HashSet::from([file.ino]),
    )
    .expect("apply protected foreign publish");

    let object_ids: Vec<_> = pg
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .expect("lookup merged protected base graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![77_040, remote_middle.object_id, local_tail.object_id],
        "protected base merge must not be overwritten by aux replacement in the same foreign batch"
    );
}

#[test]
fn protected_foreign_publish_can_clear_committed_serving_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-serving-overlay.txt", 77_050);
    let mut attr = pg.get_attr(file.ino).expect("load local attr");
    attr.size = 2 * u64::from(DEFAULT_BLOCK_SIZE);
    attr.committed_size = attr.size;
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.upload_state = UploadState::Ready;
    pg.set_attr(&attr).expect("seed committed attr");
    pg.stage_payload_tombstone(file.ino, 1)
        .expect("stage stale serving tombstone");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));

    let remote_tail = PayloadRef::new_partial(77_051, DEFAULT_BLOCK_SIZE);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(1, Some(&remote_tail), PayloadRefState::Published)
            .expect("remote tail slices"),
    };
    let (changed, _) =
        apply_foreign_batch(&pg, &[remote_publish], &overlay, &HashSet::from([file.ino]))
            .expect("apply protected foreign publish");
    assert!(
        changed,
        "protected publish-only batches that update serving metadata must wake VFS invalidation"
    );

    let object_ids: Vec<_> = pg
        .lookup_regular_file_slices(file.ino, 0, 2 * u64::from(DEFAULT_BLOCK_SIZE))
        .expect("lookup merged protected graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![77_050, remote_tail.object_id],
        "a committed serving overlay must not keep hiding a protected base payload merge"
    );
}

#[test]
fn attr_only_foreign_batch_must_not_regress_published_file_content_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"attr-only-regression.txt", 77_052);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local seed publish");
    let mut winner_attr = pg.get_attr(file.ino).expect("load winner attr");
    winner_attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.content_epoch = 3;
    winner_attr.committed_content_epoch = 3;
    winner_attr.upload_state = UploadState::Ready;
    let winner_payload = PayloadRef::new_full(77_053, DEFAULT_BLOCK_SIZE);
    let winner_batch = vec![
        MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: fixture_payload_slices(
                0,
                Some(&winner_payload),
                PayloadRefState::Published,
            )
            .expect("winner slices"),
        },
        MetaEvent::SetAttr {
            attr: winner_attr.clone(),
        },
    ];
    apply_foreign_batch(
        &pg,
        &winner_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply winner batch");
    assert_eq!(
        lookup_object_ids(&pg, file.ino, winner_attr.size).expect("lookup winner object ids"),
        vec![winner_payload.object_id],
        "setup should expose the winner payload before the attr-only batch"
    );

    let mut stale_attr = winner_attr.clone();
    stale_attr.size = 0;
    stale_attr.committed_size = 0;
    stale_attr.content_epoch = 1;
    stale_attr.committed_content_epoch = 1;
    stale_attr.mtime = winner_attr.mtime.saturating_add(1);
    stale_attr.ctime = stale_attr.mtime;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: stale_attr }],
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply attr-only stale batch");

    let attr = pg.get_attr(file.ino).expect("load final attr");
    assert_eq!(attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.committed_size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.content_epoch, 3);
    assert_eq!(attr.committed_content_epoch, 3);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, attr.size).expect("lookup final object ids"),
        vec![winner_payload.object_id],
        "an attr-only batch must not hide the latest published content graph"
    );
}

#[test]
fn attr_only_updating_foreign_batch_must_not_hide_published_file_content_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file =
        create_published_small_file(&pg, ROOT_INO, b"attr-only-updating-regression.txt", 77_054);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local seed publish");

    let mut winner_attr = pg.get_attr(file.ino).expect("load winner attr");
    winner_attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.content_epoch = 5;
    winner_attr.committed_content_epoch = 5;
    winner_attr.upload_state = UploadState::Ready;
    let winner_payload = PayloadRef::new_full(77_055, DEFAULT_BLOCK_SIZE);
    let winner_batch = vec![
        MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: fixture_payload_slices(
                0,
                Some(&winner_payload),
                PayloadRefState::Published,
            )
            .expect("winner slices"),
        },
        MetaEvent::SetAttr {
            attr: winner_attr.clone(),
        },
    ];
    apply_foreign_batch(
        &pg,
        &winner_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply winner batch");
    assert_eq!(
        lookup_object_ids(&pg, file.ino, winner_attr.size).expect("lookup winner object ids"),
        vec![winner_payload.object_id],
        "setup should expose the winner payload before the attr-only Updating batch"
    );
    let base_attr = pg
        .local
        .get_base_attr(file.ino)
        .expect("load base attr")
        .expect("base attr exists");
    assert_eq!(base_attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(base_attr.upload_state, UploadState::Ready);

    let mut stale_updating_attr = winner_attr.clone();
    stale_updating_attr.size = 0;
    stale_updating_attr.committed_size = 0;
    stale_updating_attr.content_epoch = 3;
    stale_updating_attr.committed_content_epoch = 2;
    stale_updating_attr.upload_state = UploadState::Updating;
    stale_updating_attr.mtime = winner_attr.mtime.saturating_add(1);
    stale_updating_attr.ctime = stale_updating_attr.mtime;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr {
            attr: stale_updating_attr,
        }],
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply attr-only updating batch");

    let attr = pg.get_attr(file.ino).expect("load final attr");
    assert_eq!(attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.committed_size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.content_epoch, 5);
    assert_eq!(attr.committed_content_epoch, 5);
    assert_eq!(attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, attr.size).expect("lookup final object ids"),
        vec![winner_payload.object_id],
        "a late attr-only Updating batch must not hide the latest published content graph"
    );
}

#[test]
fn newer_attr_only_updating_foreign_batch_preserves_uploading_marker_and_old_content() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"newer-attr-only-updating.txt", 77_060);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local seed publish");

    let mut updating_attr = pg.get_attr(file.ino).expect("load attr");
    updating_attr.content_epoch = updating_attr.content_epoch.saturating_add(1);
    updating_attr.upload_state = UploadState::Updating;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr {
            attr: updating_attr.clone(),
        }],
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply newer attr-only updating batch");

    let attr = pg.get_attr(file.ino).expect("load final attr");
    assert_eq!(attr.upload_state, UploadState::Updating);
    assert_eq!(attr.content_epoch, updating_attr.content_epoch);
    assert_eq!(
        attr.committed_content_epoch, updating_attr.committed_content_epoch,
        "the committed epoch must remain pinned to the old published content"
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, attr.committed_size).expect("lookup object ids"),
        vec![77_060],
        "attr-only Updating must keep serving the previous committed payload"
    );
}

#[test]
fn lifecycle_publish_ready_without_overlay_still_protects_foreign_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(
            ROOT_INO,
            b"publish-ready-lifecycle-protect.txt",
            0,
            0,
            0o644,
        )
        .expect("create file");
    let mut attr = pg.get_attr(file.ino).expect("load attr");
    attr.size = 4096;
    attr.content_epoch = attr.content_epoch.saturating_add(1);
    pg.set_attr(&attr)
        .expect("stage blockless publish-ready attr");

    assert!(
        !pg.local
            .has_local_regular_file_overlay(file.ino)
            .expect("probe regular-file overlay"),
        "setup should rely on lifecycle state, not chunk overlay rows"
    );
    let lifecycle = pg
        .local
        .get_inode_lifecycle(file.ino)
        .expect("load lifecycle")
        .expect("lifecycle should exist");
    assert_eq!(lifecycle.state, InodeLifecycleState::PublishReady);

    assert!(
        RemoteApplyCoordinator::new(&pg.local)
            .local_regular_file_state_needs_foreign_protection(file.ino)
            .expect("probe protection"),
        "publish-ready lifecycle rows represent local publication work even without chunk overlay rows"
    );
}

#[test]
fn stale_local_only_lifecycle_without_overlay_does_not_protect_committed_base_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"stale-lifecycle-ready.txt", 87);
    let published_attr = pg.get_attr(file.ino).expect("load published attr");

    let mut local_attr = published_attr.clone();
    local_attr.size = local_attr.size.saturating_add(128);
    local_attr.content_epoch = local_attr.content_epoch.saturating_add(1);
    local_attr.upload_state = UploadState::Updating;
    pg.set_attr(&local_attr)
        .expect("stage local attr lifecycle");

    assert_ne!(
        pg.local
            .get_inode_lifecycle(file.ino)
            .expect("load local lifecycle")
            .expect("local lifecycle")
            .state,
        InodeLifecycleState::Published
    );

    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: published_attr.clone(),
        }])
        .expect("restore committed base attr without refreshing stale lifecycle");
    pg.local
        .apply_replica_serving_state(&[MetaEvent::SetAttr {
            attr: published_attr.clone(),
        }])
        .expect("restore visible attr without refreshing stale lifecycle");

    assert!(
        !pg.local
            .has_local_regular_file_overlay(file.ino)
            .expect("probe overlay"),
        "setup must not have local overlay rows"
    );
    assert_ne!(
        pg.local
            .get_inode_lifecycle(file.ino)
            .expect("load stale lifecycle")
            .expect("stale lifecycle")
            .state,
        InodeLifecycleState::Published,
        "setup should retain a stale unpublished lifecycle row like mounted replay"
    );
    let current_attr = pg.local.get_attr(file.ino).expect("load current attr");
    assert_eq!(current_attr.upload_state, UploadState::Ready);
    assert_eq!(current_attr.size, current_attr.committed_size);
    assert_eq!(
        current_attr.content_epoch,
        current_attr.committed_content_epoch
    );
    assert!(
        !RemoteApplyCoordinator::new(&pg.local)
            .local_regular_file_state_needs_foreign_protection(file.ino)
            .expect("probe protection"),
        "a stale LocalOnly lifecycle row must not protect a Ready committed base attr without overlay"
    );
}

#[test]
fn attr_only_lifecycle_overlay_does_not_block_committed_base_payload_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"attr-only-block-replay.txt", 87);

    let mut local_attr = pg.get_attr(file.ino).expect("load local attr");
    local_attr.size = 8192;
    pg.set_attr(&local_attr).expect("set local attr overlay");

    assert!(
        RemoteApplyCoordinator::new(&pg.local)
            .local_regular_file_state_needs_foreign_protection(file.ino)
            .expect("probe attr protection"),
        "local attr lifecycle must still protect the local attr overlay"
    );
    assert!(
        !pg.local
            .has_local_regular_file_overlay(file.ino)
            .expect("probe block overlay"),
        "setup must not have local chunk overlay rows"
    );

    let overlay = RemoteApplyCoordinator::new(&pg.local)
        .current_regular_file_overlay_conflicts_for_batch(&[MetaEvent::SetAttr {
            attr: local_attr.clone(),
        }])
        .expect("collect overlay conflicts");
    assert!(overlay.attr_inodes.contains(&file.ino));
    assert!(
        !overlay.block_inodes.contains(&file.ino),
        "attr-only lifecycle protection must not protect committed base ranges"
    );

    let remote_payload = PayloadRef::new_full(88, DEFAULT_BLOCK_SIZE);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(0, Some(&remote_payload), PayloadRefState::Published)
            .expect("remote payload slices"),
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("load remote attr");
    remote_attr.size = 4096;
    remote_attr.committed_size = 4096;
    remote_attr.content_epoch = remote_attr.content_epoch.saturating_add(1);
    remote_attr.committed_content_epoch = remote_attr.content_epoch;
    remote_attr.upload_state = UploadState::Ready;

    apply_foreign_batch(
        &pg,
        &[
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            remote_publish,
        ],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply committed payload under local attr overlay");

    assert_eq!(
        lookup_first_object_id(&pg, file.ino).expect("lookup committed payload"),
        Some(remote_payload.object_id),
        "committed payload replay must replace the base graph despite local attr overlay"
    );
    assert_eq!(
        pg.get_attr(file.ino)
            .expect("local attr should remain")
            .size,
        8192,
        "local attr overlay should still mask remote size"
    );
}

#[test]
fn newer_ready_foreign_publish_replaces_protected_committed_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-replace.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 233, 2);
    let local_head = PayloadRef::new_partial(77_060, 233);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 2, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        overlay.block_inodes.contains(&file.ino),
        "unacknowledged local publish should protect the inode before the remote winner arrives"
    );

    let remote_head = PayloadRef::new_partial(77_061, 229);
    let remote_tail = PayloadRef::new_partial(77_062, 229);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![
                SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 229).expect("head range"),
                    payload: Some(remote_head.clone()),
                    payload_state: PayloadRefState::Published,
                },
                SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(1),
                    range: SliceRange::new(229, 458).expect("tail range"),
                    payload: Some(remote_tail.clone()),
                    payload_state: PayloadRefState::Published,
                },
            ],
        )],
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("local attr");
    remote_attr.size = 458;
    remote_attr.committed_size = 458;
    remote_attr.content_epoch = 2;
    remote_attr.committed_content_epoch = 2;
    remote_attr.upload_state = UploadState::Ready;
    remote_attr.mtime = remote_attr.mtime.saturating_add(1);
    remote_attr.ctime = remote_attr.ctime.saturating_add(1);

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply newer ready foreign publish");

    let spans = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged graph");
    let object_ids = spans
        .iter()
        .filter_map(|span| span.payload.as_ref().map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_head.object_id, remote_tail.object_id],
        "newer ready remote publish should replace, not splice into, a protected committed base graph"
    );
}

#[test]
fn growing_ready_foreign_publish_replaces_higher_epoch_protected_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-grow.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 235, 1);
    let local_head = PayloadRef::new_partial(77_070, 235);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        overlay.block_inodes.contains(&file.ino),
        "unacknowledged local publish should protect the inode before the remote growth arrives"
    );

    let remote_head = PayloadRef::new_partial(77_071, 231);
    let remote_tail = PayloadRef::new_partial(77_072, 231);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![
                SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 231).expect("head range"),
                    payload: Some(remote_head.clone()),
                    payload_state: PayloadRefState::Published,
                },
                SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(1),
                    range: SliceRange::new(231, 462).expect("tail range"),
                    payload: Some(remote_tail.clone()),
                    payload_state: PayloadRefState::Published,
                },
            ],
        )],
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("local attr");
    remote_attr.size = 462;
    remote_attr.committed_size = 462;
    remote_attr.content_epoch = 0;
    remote_attr.committed_content_epoch = 0;
    remote_attr.upload_state = UploadState::Ready;

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply growing ready foreign publish");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_head.object_id, remote_tail.object_id],
        "growing ready remote publish should replace, not splice into, a protected committed base graph"
    );
    assert_eq!(
        pg.get_attr(file.ino).expect("load applied attr").size,
        remote_attr.size,
        "serving attr must grow with the authoritative ready foreign publish"
    );
}

#[test]
fn ready_foreign_overwrite_replaces_larger_same_epoch_protected_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-overwrite.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 223, 0);
    let local_head = PayloadRef::new_partial(77_090, 223);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 0, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 0)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    set_file_state(&pg, file.ino, 446, 1);
    let local_tail = PayloadRef::new_partial(77_091, 223);
    pg.stage_payload_slice(file.ino, 223, &local_tail)
        .expect("stage local tail");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local tail"),
        "local append publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        overlay.block_inodes.contains(&file.ino),
        "unacknowledged local append should protect the inode before the remote overwrite arrives"
    );

    let remote_payload = PayloadRef::new_partial(77_092, 227);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(0, 227).expect("remote overwrite range"),
                payload: Some(remote_payload.clone()),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("local attr");
    remote_attr.size = 227;
    remote_attr.committed_size = 227;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    remote_attr.upload_state = UploadState::Ready;
    remote_attr.mtime = remote_attr.mtime.saturating_sub(1);
    remote_attr.ctime = remote_attr.ctime.saturating_sub(1);

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply ready foreign overwrite");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_payload.object_id],
        "ready remote overwrite should replace, not splice into, a larger same-epoch protected graph"
    );
}

#[test]
fn publish_without_ready_attr_preserves_protected_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-publish-only.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 235, 1);
    let local_head = PayloadRef::new_partial(77_080, 235);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let remote_head = PayloadRef::new_partial(77_081, 231);
    let remote_tail = PayloadRef::new_partial(77_082, 231);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![
                SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 231).expect("head range"),
                    payload: Some(remote_head),
                    payload_state: PayloadRefState::Published,
                },
                SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(1),
                    range: SliceRange::new(231, 462).expect("tail range"),
                    payload: Some(remote_tail.clone()),
                    payload_state: PayloadRefState::Published,
                },
            ],
        )],
    };

    apply_foreign_batch(&pg, &[remote_publish], &overlay, &HashSet::from([file.ino]))
        .expect("apply publish-only foreign batch");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, 462)
        .expect("lookup merged graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![local_head.object_id, remote_tail.object_id],
        "publish-only foreign batches are not authoritative enough to replace protected base ranges"
    );
}

#[test]
fn recent_flush_reconcile_resets_stale_regular_file_serving_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"recent-flush-stale-serving.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 227, 3);
    let local_payload = PayloadRef::new_partial(91_011, 227);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 3, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .expect("publish local payload"),
        "local publish should succeed"
    );
    let events_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let events_end = pg.local.get_counter("nextEventSeq").expect("events end");
    let local_events = pg
        .local
        .read_events_range(events_start, events_end)
        .expect("read local events");
    pg.persist_flushed_event_seq(events_end)
        .expect("mark local publish flushed");

    let mut remote_attr = pg.get_attr(file.ino).expect("load published attr");
    remote_attr.size = 446;
    remote_attr.committed_size = 446;
    remote_attr.content_epoch = 4;
    remote_attr.committed_content_epoch = 4;
    remote_attr.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .expect("apply authoritative base attr");
    pg.persist_local_version(7).expect("catch up local version");

    assert_eq!(
        pg.get_attr(file.ino).expect("load stale serving attr").size,
        227,
        "flushed local serving attr can remain stale after base advances"
    );

    let recent_flush = RecentFlush {
        batch_version: 7,
        overlay: RemoteApplyCoordinator::overlay_conflicts_for_events(
            &RemoteApplyCoordinator::project_remote_events(&local_events),
        ),
    };
    reconcile_recent_flush_regular_files(&pg, Some(&recent_flush)).expect("reconcile recent flush");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "recent-flush reconciliation must reset stale regular-file serving attrs from base"
    );
}

#[test]
fn unit_remote_projection_preserves_nested_base_graph_publish() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let start = writer.local.get_counter("nextEventSeq").expect("start seq");
    let dir = writer
        .create_dir(ROOT_INO, b"nested", 0, 0, 0o755)
        .expect("create dir");
    let file = writer
        .create_file(dir.ino, b"leaf.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&writer, file.ino, 4096, 1);
    let block = PayloadRef::new_partial(701, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &block)
        .expect("set payload slice");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[block.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base graph"),
        "publish must succeed"
    );
    let end = writer.local.get_counter("nextEventSeq").expect("end seq");

    apply_projected_remote_range(&writer, &remote, start, end);

    let remote_dir = remote.lookup(ROOT_INO, b"nested").expect("lookup dir");
    let remote_file = remote
        .lookup(remote_dir.ino, b"leaf.bin")
        .expect("lookup file");
    assert_eq!(remote_file.ino, file.ino);
    let remote_attr = remote.get_attr(remote_file.ino).expect("get remote attr");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_file.ino, remote_attr.size).expect("remote object ids"),
        vec![block.object_id]
    );
}

#[test]
fn unit_remote_projection_base_graph_publish_replaces_base_graph_on_shrink() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let batch1_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 start");
    let file = writer
        .create_file(ROOT_INO, b"shrink.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&writer, file.ino, block_size + 4096, 1);
    let first = PayloadRef::new_full(1001, DEFAULT_BLOCK_SIZE);
    let second = PayloadRef::new_partial(block_size + 2, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &first)
        .expect("set first block");
    writer
        .stage_payload_slice(file.ino, 1, &second)
        .expect("set second block");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[first.object_id, second.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish initial base graph"),
        "initial base graph publish must succeed"
    );
    let batch1_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 end");
    apply_projected_remote_range(&writer, &remote, batch1_start, batch1_end);

    assert_eq!(
        lookup_object_ids(&remote, file.ino, block_size + 4096).expect("remote initial object ids"),
        vec![first.object_id, second.object_id]
    );

    let batch2_start = batch1_end;
    set_file_state(&writer, file.ino, 4096, 2);
    writer
        .stage_payload_tombstone(file.ino, 1)
        .expect("remove second block for shrink");
    let replacement = PayloadRef::new_partial(block_size + 3, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &replacement)
        .expect("set replacement payload slice");
    mark_overlay_objects_uploaded(&writer, file.ino, 2, &[replacement.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish shrink base graph"),
        "shrink base graph publish must succeed"
    );
    let batch2_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch2 end");
    apply_projected_remote_range(&writer, &remote, batch2_start, batch2_end);

    let remote_attr = remote.get_attr(file.ino).expect("remote attr after shrink");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 2);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, file.ino, remote_attr.size)
            .expect("remote object ids after shrink"),
        vec![replacement.object_id],
        "base graph publish must replace the full remote base graph and drop stale trailing blocks"
    );
}

#[test]
fn unit_remote_projection_updates_nested_file_after_temp_rename_then_publish() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let batch1_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 start");
    let dir = writer
        .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
        .expect("create dir");
    let file = writer
        .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
        .expect("create temp file");
    writer
        .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
        .expect("rename temp to final");
    let batch1_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 end");

    apply_projected_remote_range(&writer, &remote, batch1_start, batch1_end);

    let remote_dir = remote
        .lookup(ROOT_INO, b"drafts")
        .expect("lookup dir after rename");
    assert!(matches!(
        remote.lookup(remote_dir.ino, b"final.bin"),
        Err(MetaError::EntryNotFound { .. })
    ));

    let batch2_start = batch1_end;
    set_file_state(&writer, file.ino, block_size + 4096, 1);
    let block0 = PayloadRef::new_full(801, DEFAULT_BLOCK_SIZE);
    let block1 = PayloadRef::new_partial(802, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &block0)
        .expect("set payload slice 0");
    writer
        .stage_payload_slice(file.ino, 1, &block1)
        .expect("set payload slice 1");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[block0.object_id, block1.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish renamed base graph"),
        "publish after rename must succeed"
    );
    let batch2_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch2 end");

    apply_projected_remote_range(&writer, &remote, batch2_start, batch2_end);

    let remote_file_after = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup final after publish");
    assert_eq!(remote_file_after.ino, file.ino);
    let remote_attr_after = remote
        .get_attr(remote_file_after.ino)
        .expect("attr after publish");
    assert_eq!(remote_attr_after.size, block_size + 4096);
    assert_eq!(remote_attr_after.committed_size, block_size + 4096);
    assert_eq!(remote_attr_after.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_file_after.ino, remote_attr_after.size)
            .expect("remote object ids after publish"),
        vec![block0.object_id, block1.object_id]
    );
}

#[test]
fn unit_remote_projection_keeps_published_file_visible_when_unpublished_epoch_is_deleted() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let seed_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("seed start");
    let dir = writer
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create dir");
    let file = writer
        .create_file(dir.ino, b"final.bin", 0, 0, 0o644)
        .expect("create final");
    set_file_state(&writer, file.ino, 4096, 1);
    let published = PayloadRef::new_partial(904, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &published)
        .expect("set published payload slice");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[published.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish seed base graph"),
        "seed publish must succeed"
    );
    let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");

    apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

    let remote_dir = remote.lookup(ROOT_INO, b"shared").expect("lookup dir");
    let remote_file = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup published file");
    assert_eq!(remote_file.ino, file.ino);

    let upload_start = seed_end;
    set_file_state(&writer, file.ino, 4096, 2);
    let uploaded_overlay = PayloadRef::new_partial(905, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &uploaded_overlay)
        .expect("set unpublished overlay");
    assert!(
        writer
            .mark_regular_file_overlay_object_uploaded_if_matches(
                file.ino,
                2,
                uploaded_overlay.object_id,
            )
            .expect("mark overlay uploaded"),
        "overlay upload marker must succeed"
    );
    let upload_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("upload end");

    apply_projected_remote_range(&writer, &remote, upload_start, upload_end);

    writer
        .unlink(dir.ino, b"final.bin")
        .expect("unlink unpublished replacement");
    let delete_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("delete end");

    apply_projected_remote_range(&writer, &remote, upload_end, delete_end);

    let remote_after = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("remote must retain last published entry");
    assert_eq!(remote_after.ino, file.ino);
    let remote_attr = remote.get_attr(file.ino).expect("remote attr");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.content_epoch, 1);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(
        lookup_object_ids(&remote, file.ino, remote_attr.size).expect("remote object ids"),
        vec![published.object_id]
    );

    assert!(
        writer.get_attr(file.ino).is_err(),
        "local unlink removes inode attr"
    );
    let lifecycle = writer
        .get_inode_lifecycle(file.ino)
        .expect("local lifecycle")
        .expect("deleted-before-publish lifecycle");
    assert_eq!(lifecycle.state, InodeLifecycleState::DeletedBeforePublish);
    assert!(lifecycle.has_published_base);
    assert_eq!(lifecycle.deferred_cleanup_object_ids, vec![905]);
    let pending_deletions = writer.get_pending_deletions().expect("pending deletions");
    assert!(
        pending_deletions
            .iter()
            .any(|(object_id, _timestamp)| *object_id == 905),
        "uploaded unpublished object should be queued for GC"
    );
    assert!(
        pending_deletions
            .iter()
            .all(|(object_id, _timestamp)| *object_id != 904),
        "last published object must remain referenced"
    );
}

#[test]
fn unit_remote_projection_preserves_overlay_publish_path() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let start = writer.local.get_counter("nextEventSeq").expect("start seq");
    let file = writer
        .create_file(ROOT_INO, b"overlay.bin", 0, 0, 0o644)
        .expect("create overlay file");
    set_file_state(&writer, file.ino, 4096, 1);
    let block = PayloadRef::new_partial(906, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &block)
        .expect("set overlay block");
    assert!(
        writer
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, block.object_id)
            .expect("mark overlay uploaded"),
        "uploaded marker must succeed"
    );
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish overlay to base"),
        "overlay publish must succeed"
    );
    let end = writer.local.get_counter("nextEventSeq").expect("end seq");

    apply_projected_remote_range(&writer, &remote, start, end);

    let remote_file = remote
        .lookup(ROOT_INO, b"overlay.bin")
        .expect("lookup overlay file");
    let remote_attr = remote.get_attr(remote_file.ino).expect("remote attr");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_file.ino, remote_attr.size)
            .expect("remote overlay object ids"),
        vec![block.object_id]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-committed-block-read");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"remote.bin", 50_301);
    set_file_state(&a, file.ino, 4096, 2);
    let block = PayloadRef::new_partial(77, 4096);
    a.stage_payload_slice(file.ino, 0, &block)
        .expect("set staged payload slice");
    mark_overlay_objects_uploaded(&a, file.ino, 2, &[block.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish committed base graph")
    );
    a.flush_metadata().expect("flush a");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.bin",
        Duration::from_secs(10)
    ));
    let remote = b.lookup(ROOT_INO, b"remote.bin").expect("lookup remote");
    assert_eq!(
        lookup_first_object_id(&b, remote.ino)
            .expect("get remote payload slice")
            .expect("remote committed payload slice"),
        77
    );

    assert_eq!(
        lookup_object_ids(&b, remote.ino, 4096).expect("get remote object ids"),
        vec![77]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_uploading_marker_replication() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-remote-uploading-marker");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"uploading.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 4096, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(501, 4096))
        .expect("set staged payload slice");
    a.flush_metadata().expect("flush uploading marker");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        matches!(
            b.lookup(ROOT_INO, b"uploading.bin"),
            Err(MetaError::EntryNotFound { .. })
        )
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_nested_base_graph_publish_visible_on_remote_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-nested-base-graph-publish");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir = a
        .create_dir(ROOT_INO, b"nested", 0, 0, 0o755)
        .expect("create nested dir");
    a.flush_metadata().expect("flush nested dir");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"nested",
        Duration::from_secs(10)
    ));

    let file = a
        .create_file(dir.ino, b"leaf.bin", 0, 0, 0o644)
        .expect("create nested file");
    set_file_state(&a, file.ino, 4096, 1);
    let block = PayloadRef::new_partial(511, 4096);
    a.stage_payload_slice(file.ino, 0, &block)
        .expect("set nested block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[block.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish nested base graph")
    );
    a.flush_metadata().expect("flush nested publish");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"nested") {
            Ok(remote_dir) => match b.lookup(remote_dir.ino, b"leaf.bin") {
                Ok(remote_file) => b
                    .get_attr(remote_file.ino)
                    .map(|attr| {
                        attr.size == 4096
                            && attr.committed_size == 4096
                            && attr.upload_state == UploadState::Ready
                    })
                    .unwrap_or(false),
                Err(_) => false,
            },
            Err(_) => false,
        }
    }));

    let remote_dir = b.lookup(ROOT_INO, b"nested").expect("lookup remote dir");
    let remote_file = b
        .lookup(remote_dir.ino, b"leaf.bin")
        .expect("lookup remote nested file");
    assert_eq!(
        lookup_object_ids(&b, remote_file.ino, 4096).expect("get remote nested object ids"),
        vec![block.object_id]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_nested_temp_rename_then_publish_visible_on_remote_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-nested-temp-rename-publish");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir = a
        .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
        .expect("create drafts dir");
    a.flush_metadata().expect("flush drafts dir");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"drafts",
        Duration::from_secs(10)
    ));

    let file = a
        .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
        .expect("create temp file");
    a.rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
        .expect("rename temp to final");
    a.flush_metadata().expect("flush rename");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"drafts") {
            Ok(remote_dir) => matches!(
                b.lookup(remote_dir.ino, b"final.bin"),
                Err(MetaError::EntryNotFound { .. })
            ),
            Err(_) => false,
        }
    }));

    let block_size = DEFAULT_BLOCK_SIZE as u64;
    set_file_state(&a, file.ino, block_size + 4096, 1);
    let block0 = PayloadRef::new_full(611, DEFAULT_BLOCK_SIZE);
    let block1 = PayloadRef::new_partial(612, 4096);
    a.stage_payload_slice(file.ino, 0, &block0)
        .expect("set payload slice 0");
    a.stage_payload_slice(file.ino, 1, &block1)
        .expect("set payload slice 1");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[block0.object_id, block1.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish renamed nested base graph")
    );
    a.flush_metadata().expect("flush renamed nested base graph");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"drafts") {
            Ok(remote_dir) => match b.lookup(remote_dir.ino, b"final.bin") {
                Ok(remote_file) => {
                    let attr_ready = b
                        .get_attr(remote_file.ino)
                        .map(|attr| {
                            attr.size == block_size + 4096
                                && attr.committed_size == block_size + 4096
                                && attr.upload_state == UploadState::Ready
                        })
                        .unwrap_or(false);
                    let blocks_ready = lookup_object_ids(&b, remote_file.ino, block_size + 4096)
                        .map(|object_ids| object_ids == vec![block0.object_id, block1.object_id])
                        .unwrap_or(false);
                    attr_ready && blocks_ready
                }
                Err(_) => false,
            },
            Err(_) => false,
        }
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_update_keeps_old_committed_version_until_switch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-old-committed-until-switch");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"committed.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 4096, 1);
    let baseline = PayloadRef::new_partial(701, 4096);
    a.stage_payload_slice(file.ino, 0, &baseline)
        .expect("set baseline block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[baseline.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush baseline");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"committed.bin",
        Duration::from_secs(10)
    ));
    let remote = b.lookup(ROOT_INO, b"committed.bin").expect("lookup remote");
    assert_eq!(
        lookup_first_object_id(&b, remote.ino)
            .expect("get baseline remote block")
            .expect("baseline remote block"),
        701
    );

    set_file_state(&a, file.ino, 4096, 2);
    let replacement = PayloadRef::new_partial(702, 4096);
    a.stage_payload_slice(file.ino, 0, &replacement)
        .expect("set replacement payload slice");
    a.flush_metadata().expect("flush updating marker");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.get_attr(remote.ino)
            .map(|attr| attr.upload_state == UploadState::Updating)
            .unwrap_or(false)
    }));

    let remote_attr = b
        .get_attr(remote.ino)
        .expect("get remote attr after update");
    assert_eq!(remote_attr.upload_state, UploadState::Updating);
    assert_eq!(
        lookup_first_object_id(&b, remote.ino)
            .expect("get remote block after update")
            .expect("remote block after update"),
        701,
        "remote peer must keep serving the previous committed version until publish"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_publish_switch_is_atomic_across_peers() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-atomic-publish-switch");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"atomic.bin", 0, 0, 0o644)
        .expect("create file");
    let block_size = DEFAULT_BLOCK_SIZE as u64;
    set_file_state(&a, file.ino, block_size + 4096, 1);
    let baseline0 = PayloadRef::new_full(801, DEFAULT_BLOCK_SIZE);
    let baseline1 = PayloadRef::new_partial(802, 4096);
    a.stage_payload_slice(file.ino, 0, &baseline0)
        .expect("set baseline block 0");
    a.stage_payload_slice(file.ino, 1, &baseline1)
        .expect("set baseline block 1");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[baseline0.object_id, baseline1.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush baseline base graph");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"atomic.bin",
        Duration::from_secs(10)
    ));
    let remote = b.lookup(ROOT_INO, b"atomic.bin").expect("lookup remote");

    set_file_state(&a, file.ino, block_size + 4096, 2);
    let replacement0 = PayloadRef::new_full(901, DEFAULT_BLOCK_SIZE);
    let replacement1 = PayloadRef::new_partial(902, 4096);
    a.stage_payload_slice(file.ino, 0, &replacement0)
        .expect("set replacement payload slice 0");
    a.stage_payload_slice(file.ino, 1, &replacement1)
        .expect("set replacement payload slice 1");
    a.flush_metadata().expect("flush updating state");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        let object_ids = lookup_object_ids(&b, remote.ino, block_size + 4096).unwrap_or_default();
        object_ids == vec![801, 802]
            && b.get_attr(remote.ino)
                .map(|attr| attr.upload_state == UploadState::Updating)
                .unwrap_or(false)
    }));

    mark_overlay_objects_uploaded(
        &a,
        file.ino,
        2,
        &[replacement0.object_id, replacement1.object_id],
    );
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish replacement base graph")
    );
    a.flush_metadata().expect("flush replacement base graph");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        let object_ids = lookup_object_ids(&b, remote.ino, block_size + 4096).unwrap_or_default();
        object_ids == vec![901, 902]
            && b.get_attr(remote.ino)
                .map(|attr| attr.upload_state == UploadState::Ready)
                .unwrap_or(false)
    }));

    assert_eq!(
        lookup_object_ids(&b, remote.ino, block_size + 4096).expect("get remote object ids"),
        vec![901, 902]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_foreign_events_do_not_block_local_accepted_reads() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let dir_c = tempdir().expect("tempdir c");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let db_c = dir_c.path().join("meta-c.redb");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-does-not-flush-local");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);
    seed_local(&db_c, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    let c = PgMetaEngine::open(&db_c).expect("open c");

    let draft = b
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create local draft");
    set_file_state(&b, draft.ino, 4096, 1);
    b.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(1001, 4096))
        .expect("set local draft block");

    a.create_dir(ROOT_INO, b"foreign", 0, 0, 0o755)
        .expect("create foreign dir");
    a.flush_metadata().expect("flush foreign dir");

    assert!(wait_until(Duration::from_secs(10), || {
        if let Ok(mut state) = b.pending_state.lock() {
            state.force_flush = false;
            state.last_mutation_at = Some(Instant::now());
        }
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"foreign").is_ok()
    }));

    let local_attr = b.get_attr(draft.ino).expect("get local draft attr");
    assert_eq!(local_attr.size, 4096);
    assert_eq!(local_attr.committed_size, 0);
    assert_eq!(local_attr.upload_state, UploadState::Uploading);
    assert_eq!(
        lookup_first_object_id(&b, draft.ino)
            .expect("get local draft block")
            .expect("local draft block"),
        1001
    );

    if let Ok(mut state) = b.pending_state.lock() {
        state.force_flush = false;
        state.last_mutation_at = Some(Instant::now());
    }
    let _ = c.sync_metadata();
    assert!(
        c.lookup(ROOT_INO, b"draft.bin").is_err(),
        "syncing foreign events on b must not flush its local accepted draft into PG"
    );
    assert!(
        c.lookup(ROOT_INO, b"foreign").is_ok(),
        "foreign namespace convergence should still work while b has local-only accepted state"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_overlay_allows_unrelated_remote_batches_to_apply() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-overlay-prefix");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let draft = a
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create draft");
    let mut draft_attr = a.get_attr(draft.ino).expect("get draft attr");
    draft_attr.size = 4096;
    a.set_attr(&draft_attr).expect("set draft size");
    a.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(501, 4096))
        .expect("set draft staged payload slice");
    a.flush_metadata().expect("flush draft namespace");

    b.create_dir(ROOT_INO, b"foreign.txt", 0, 0, 0o755)
        .expect("create foreign");
    b.flush_metadata().expect("flush foreign");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"foreign.txt").is_ok()
    }));

    let draft_after = a.get_attr(draft.ino).expect("get local draft");
    assert_eq!(draft_after.size, 4096);
    assert_eq!(draft_after.committed_size, 0);
    assert_eq!(
        lookup_first_object_id(&a, draft.ino)
            .expect("get staged draft block")
            .expect("staged draft block exists"),
        501
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_committed_block_applies_despite_local_attr_overlay() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-committed-block-vs-attr-overlay");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"remote.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 4096, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(87, 4096))
        .expect("set baseline remote block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[87]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.bin",
        Duration::from_secs(10)
    ));

    let b_file = b.lookup(ROOT_INO, b"remote.bin").expect("lookup on b");
    let mut local_attr = b.get_attr(b_file.ino).expect("get local attr");
    local_attr.size = 8192;
    b.set_attr(&local_attr).expect("set local attr overlay");

    set_file_state(&a, file.ino, 4096, 2);
    let block = PayloadRef::new_partial(88, 4096);
    a.stage_payload_slice(file.ino, 0, &block)
        .expect("set staged remote slice");
    mark_overlay_objects_uploaded(&a, file.ino, 2, &[block.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish remote slice graph")
    );
    a.flush_metadata().expect("flush remote slice graph");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        lookup_first_object_id(&b, b_file.ino)
            .map(|object_id| object_id == Some(88))
            .unwrap_or(false)
    }));

    assert_eq!(
        lookup_first_object_id(&b, b_file.ino)
            .expect("get committed payload slice on b")
            .expect("committed payload slice visible on b"),
        88
    );

    let attr_after = b.get_attr(b_file.ino).expect("get local attr after sync");
    assert_eq!(
        attr_after.size, 8192,
        "local attr overlay should still mask remote size"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_conflicting_batch_does_not_block_later_foreign_batch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-conflict-gap");
    let info = volume.format_info();
    seed_local(&db, &info);

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let draft = pg
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create draft");
    let mut draft_attr = pg.get_attr(draft.ino).expect("get draft attr");
    draft_attr.size = 4096;
    pg.set_attr(&draft_attr).expect("set staged size");
    pg.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(601, 4096))
        .expect("set draft block");

    let mut conflicted_attr = draft_attr.clone();
    conflicted_attr.size = 8192;

    let remote_ino = 1_234_567_890u64;
    let remote_attr = InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755);

    let conflict_event = MetaEvent::SetAttr {
        attr: conflicted_attr,
    };
    let add_event = MetaEvent::SetAttr {
        attr: remote_attr.clone(),
    };
    let dentry_event = MetaEvent::AddEntry {
        parent: ROOT_INO,
        name: b"unrelated.txt".to_vec(),
        entry: DirEntryValue {
            ino: remote_ino,
            kind: FileType::Directory,
        },
    };

    let conflict_payload = postcard::to_stdvec(&conflict_event).expect("encode conflict event");
    let add_payload = postcard::to_stdvec(&add_event).expect("encode add attr event");
    let dentry_payload = postcard::to_stdvec(&dentry_event).expect("encode unrelated dentry event");

    run_async(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 1, $2, $3, 'manual-foreign')",
                &[&volume.volume_id, &conflict_event.kind(), &conflict_payload],
            )
            .await
            .map_err(|e| MetaError::Storage(format!("insert conflict event failed: {e}")))?;

        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 2, $2, $3, 'manual-foreign')",
                &[&volume.volume_id, &add_event.kind(), &add_payload],
            )
            .await
            .map_err(|e| MetaError::Storage(format!("insert remote attr event failed: {e}")))?;
        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 2, $2, $3, 'manual-foreign')",
                &[&volume.volume_id, &dentry_event.kind(), &dentry_payload],
            )
            .await
            .map_err(|e| MetaError::Storage(format!("insert remote dentry event failed: {e}")))?;
        conn_task.abort();
        Ok(())
    })
    .expect("write synthetic remote batches");

    pg.sync_metadata().expect("sync remote batches");
    let remote = pg
        .lookup(ROOT_INO, b"unrelated.txt")
        .expect("lookup unrelated entry");
    assert_eq!(remote.ino, remote_ino);

    let local = pg
        .get_attr(draft.ino)
        .expect("lookup local draft attr after sync");
    assert_eq!(
        local.size, 4096,
        "conflicting foreign batch should not clobber local overlay"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_excludes_uncommitted_local_payloads() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-projection");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let file = a
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create draft");

    let mut attr = a.get_attr(file.ino).expect("get attr");
    attr.size = 4096;
    attr.mtime = InodeAttr::now_nanos();
    attr.ctime = attr.mtime;
    a.set_attr(&attr).expect("set staged size");
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(42, 4096))
        .expect("set staged payload slice");
    a.flush_metadata().expect("flush a");

    let b = PgMetaEngine::open(&db_b).expect("open b");
    assert!(
        wait_hidden(&b, ROOT_INO, b"draft.bin", Duration::from_secs(10)),
        "checkpoint must keep a brand-new unpublished file hidden from peers"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_unlink_removes_file_from_namespace_immediately() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-remote-unlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    create_published_small_file(&a, ROOT_INO, b"remote-delete.txt", 50_302);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote-delete.txt",
        Duration::from_secs(10)
    ));

    a.unlink(ROOT_INO, b"remote-delete.txt")
        .expect("unlink on a");
    a.flush_metadata().expect("flush unlink");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"remote-delete.txt").is_err()
    }));
    assert!(b.lookup(ROOT_INO, b"remote-delete.txt").is_err());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_unlink_foreign_visible_uploading_entry() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-unlink-uploading");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"uploading.bin", 0, 0, 0o644)
        .expect("create uploading file");
    set_file_state(&a, file.ino, 4096, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(776, 4096))
        .expect("set baseline uploading block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[776]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush baseline");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"uploading.bin",
        Duration::from_secs(10)
    ));

    set_file_state(&a, file.ino, 4096, 2);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(777, 4096))
        .expect("set local uploading block");
    a.flush_metadata().expect("flush uploading namespace");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"uploading.bin")
            .map(|entry| {
                b.get_attr(entry.ino)
                    .map(|attr| attr.upload_state == UploadState::Updating)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    }));

    b.unlink(ROOT_INO, b"uploading.bin")
        .expect("unlink uploading file on peer");
    b.flush_metadata().expect("flush peer unlink");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        a.lookup(ROOT_INO, b"uploading.bin").is_ok()
            && b.lookup(ROOT_INO, b"uploading.bin").is_err()
    }));
    let local_attr = a
        .get_attr(file.ino)
        .expect("get local attr after peer unlink");
    assert_eq!(local_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_first_object_id(&a, file.ino)
            .expect("get local payload after peer unlink")
            .expect("writer keeps local pending payload after peer unlink"),
        777
    );

    drop(a);
    let reopened = reopen_pg_engine_with_retry(&db_a);
    reopened.sync_metadata().expect("sync reopened a");
    assert!(reopened.lookup(ROOT_INO, b"uploading.bin").is_ok());
    assert_eq!(
        lookup_first_object_id(&reopened, file.ino)
            .expect("get reopened local payload")
            .expect("reopened local payload"),
        777
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_rename_foreign_visible_base_entry() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-rename-base");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_306);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(10)
    ));

    b.rename(ROOT_INO, b"remote.txt", ROOT_INO, b"renamed-by-b.txt")
        .expect("rename on b");
    b.flush_metadata().expect("flush rename on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"remote.txt").is_err()
            && a.lookup(ROOT_INO, b"renamed-by-b.txt")
                .map(|entry| entry.ino == file.ino)
                .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_unlink_foreign_visible_base_entry() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-unlink-base");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_307);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(10)
    ));

    b.unlink(ROOT_INO, b"remote.txt").expect("unlink on b");
    b.flush_metadata().expect("flush unlink on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"remote.txt").is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_peer_unlink_base_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-peer-unlink-base");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_308);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(10)
    ));

    b.unlink(ROOT_INO, b"remote.txt").expect("unlink on b");
    b.flush_metadata().expect("flush unlink on b");
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"remote.txt").is_err()
    }));

    drop(a);
    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        reopened_a.lookup(ROOT_INO, b"remote.txt").is_err()
            && reopened_a.get_attr(file.ino).is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_own_flush_does_not_skip_earlier_foreign_batch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-own-flush-gap");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"from_a.txt", 0, 0, 0o755)
        .expect("create on a");
    a.flush_metadata().expect("flush a");

    b.create_dir(ROOT_INO, b"from_b.txt", 0, 0, 0o755)
        .expect("create on b");
    b.flush_metadata().expect("flush b");

    assert!(
        wait_visible(&b, ROOT_INO, b"from_a.txt", Duration::from_secs(12)),
        "client B must not skip an earlier foreign batch when it publishes its own batch"
    );
    assert!(
        wait_visible(&a, ROOT_INO, b"from_b.txt", Duration::from_secs(12)),
        "client A should observe client B's batch too"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_earlier_foreign_batch_gap_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-gap");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"from_a.txt", 0, 0, 0o755)
        .expect("create on a");
    a.flush_metadata().expect("flush a");

    b.create_dir(ROOT_INO, b"from_b.txt", 0, 0, 0o755)
        .expect("create on b");
    b.flush_metadata().expect("flush b");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"from_a.txt",
        Duration::from_secs(12)
    ));
    drop(b);

    let reopened_b = reopen_pg_engine_with_retry(&db_b);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_b.sync_metadata();
        reopened_b.lookup(ROOT_INO, b"from_a.txt").is_ok()
            && reopened_b.lookup(ROOT_INO, b"from_b.txt").is_ok()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_two_clients_different_dirs_no_loss() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-dirs");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir_a_attr = a
        .create_dir(ROOT_INO, b"client_a", 0, 0, 0o755)
        .expect("a create dir");
    let dir_b_attr = b
        .create_dir(ROOT_INO, b"client_b", 0, 0, 0o755)
        .expect("b create dir");

    for i in 0..20 {
        let name = format!("a_{i:02}.txt");
        a.create_dir(dir_a_attr.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("a create dir");
    }
    for i in 0..20 {
        let name = format!("b_{i:02}.txt");
        b.create_dir(dir_b_attr.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("b create dir");
    }

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    let converged = wait_until(Duration::from_secs(20), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        a.lookup(ROOT_INO, b"client_a").is_ok()
            && a.lookup(ROOT_INO, b"client_b").is_ok()
            && b.lookup(ROOT_INO, b"client_a").is_ok()
            && b.lookup(ROOT_INO, b"client_b").is_ok()
    });
    assert!(converged);

    let a_client_a = a.lookup(ROOT_INO, b"client_a").expect("a lookup client_a");
    let a_client_b = a.lookup(ROOT_INO, b"client_b").expect("a lookup client_b");
    let b_client_a = b.lookup(ROOT_INO, b"client_a").expect("b lookup client_a");
    let b_client_b = b.lookup(ROOT_INO, b"client_b").expect("b lookup client_b");

    let a_files_in_a = a.readdir(a_client_a.ino).expect("a readdir client_a");
    let a_files_in_b = a.readdir(a_client_b.ino).expect("a readdir client_b");
    let b_files_in_a = b.readdir(b_client_a.ino).expect("b readdir client_a");
    let b_files_in_b = b.readdir(b_client_b.ino).expect("b readdir client_b");

    assert_eq!(
        a_files_in_a.len(),
        20,
        "client A should see all files from A"
    );
    assert_eq!(
        a_files_in_b.len(),
        20,
        "client A should see all files from B"
    );
    assert_eq!(
        b_files_in_a.len(),
        20,
        "client B should see all files from A"
    );
    assert_eq!(
        b_files_in_b.len(),
        20,
        "client B should see all files from B"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_same_dir_concurrent_writes_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-samedir");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let shared = a
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create shared dir");
    a.flush_metadata().expect("flush shared");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"shared",
        Duration::from_secs(10)
    ));

    for i in 0..20 {
        let name = format!("a_{i:02}.dat");
        a.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("a create dir");
    }
    for i in 0..20 {
        let name = format!("b_{i:02}.dat");
        b.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("b create dir");
    }

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    assert!(wait_until(Duration::from_secs(20), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        a.readdir(shared.ino)
            .map(|v| v.len() == 40)
            .unwrap_or(false)
            && b.readdir(shared.ino)
                .map(|v| v.len() == 40)
                .unwrap_or(false)
    }));

    drop(a);
    drop(b);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    let reopened_b = reopen_pg_engine_with_retry(&db_b);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        let _ = reopened_b.sync_metadata();
        reopened_a
            .lookup(ROOT_INO, b"shared")
            .ok()
            .and_then(|dir| reopened_a.readdir(dir.ino).ok())
            .map(|v| v.len() == 40)
            .unwrap_or(false)
            && reopened_b
                .lookup(ROOT_INO, b"shared")
                .ok()
                .and_then(|dir| reopened_b.readdir(dir.ino).ok())
                .map(|v| v.len() == 40)
                .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_same_dir_different_files_no_loss() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-samedir");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let shared = a
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create shared dir");
    a.flush_metadata().expect("flush shared");
    assert!(
        wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
        "shared dir should become visible on b"
    );

    for i in 0..20 {
        let name = format!("a_{i:02}.dat");
        a.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("a create dir");
    }
    for i in 0..20 {
        let name = format!("b_{i:02}.dat");
        b.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("b create dir");
    }

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    let converged = wait_until(Duration::from_secs(20), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        let a_ok = a
            .readdir(shared.ino)
            .map(|v| v.len() >= 40)
            .unwrap_or(false);
        let b_ok = b
            .readdir(shared.ino)
            .map(|v| v.len() >= 40)
            .unwrap_or(false);
        a_ok && b_ok
    });
    assert!(converged);

    let a_entries = a.readdir(shared.ino).expect("a readdir");
    let b_entries = b.readdir(shared.ino).expect("b readdir");
    assert_eq!(a_entries.len(), 40, "A should see all files in shared dir");
    assert_eq!(b_entries.len(), 40, "B should see all files in shared dir");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_concurrent_create_same_name_single_winner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-concurrent-create");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.create_dir(ROOT_INO, b"race.txt", 0, 0, 0o755);
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.create_dir(ROOT_INO, b"race.txt", 0, 0, 0o755);
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out
    });

    let r1 = t1.join().expect("thread a");
    let r2 = t2.join().expect("thread b");

    let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
    assert!(
        ok_count >= 1,
        "at least one client should create race.txt in async sync model"
    );

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    assert!(wait_visible(
        &a,
        ROOT_INO,
        b"race.txt",
        Duration::from_secs(12)
    ));
    let entry = a.lookup(ROOT_INO, b"race.txt").expect("lookup race");
    assert!(entry.ino > 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_concurrent_file_vs_dir_same_name_single_winner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-vs-dir-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.create_file(ROOT_INO, b"victim", 0, 0, 0o644);
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out.map(|attr| (attr.ino, FileType::RegularFile))
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.create_dir(ROOT_INO, b"victim", 0, 0, 0o755);
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out.map(|attr| (attr.ino, FileType::Directory))
    });

    let r1 = t1.join().expect("thread a");
    let r2 = t2.join().expect("thread b");
    assert!(
        r1.is_ok() || r2.is_ok(),
        "at least one side must win the namespace race"
    );

    if let Ok(entry) = a.lookup(ROOT_INO, b"victim") {
        if a.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&a, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_320, 64);
            a.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on a");
            mark_overlay_objects_uploaded(&a, entry.ino, 1, &[block.object_id]);
            assert!(
                a.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on a")
            );
            a.flush_metadata().expect("flush winner on a");
        }
    } else if let Ok(entry) = b.lookup(ROOT_INO, b"victim") {
        if b.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&b, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_321, 64);
            b.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on b");
            mark_overlay_objects_uploaded(&b, entry.ino, 1, &[block.object_id]);
            assert!(
                b.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on b")
            );
            b.flush_metadata().expect("flush winner on b");
        }
    }

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        match (a.lookup(ROOT_INO, b"victim"), b.lookup(ROOT_INO, b"victim")) {
            (Ok(ae), Ok(be)) => match (a.get_attr(ae.ino), b.get_attr(be.ino)) {
                (Ok(a_attr), Ok(b_attr)) => ae.ino == be.ino && a_attr.kind == b_attr.kind,
                _ => false,
            },
            _ => false,
        }
    }));

    let a_entry = a.lookup(ROOT_INO, b"victim").expect("a lookup victim");
    let b_entry = b.lookup(ROOT_INO, b"victim").expect("b lookup victim");
    let a_attr = a.get_attr(a_entry.ino).expect("a attr victim");
    let b_attr = b.get_attr(b_entry.ino).expect("b attr victim");
    assert_eq!(a_entry.ino, b_entry.ino);
    assert_eq!(a_attr.kind, b_attr.kind);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_file_dir_race_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-file-dir-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.create_file(ROOT_INO, b"victim", 0, 0, 0o644);
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.create_dir(ROOT_INO, b"victim", 0, 0, 0o755);
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out
    });

    let _ = t1.join().expect("thread a");
    let _ = t2.join().expect("thread b");

    if let Ok(entry) = a.lookup(ROOT_INO, b"victim") {
        if a.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&a, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_322, 64);
            a.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on a");
            mark_overlay_objects_uploaded(&a, entry.ino, 1, &[block.object_id]);
            assert!(
                a.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on a")
            );
            a.flush_metadata().expect("flush winner on a");
        }
    } else if let Ok(entry) = b.lookup(ROOT_INO, b"victim") {
        if b.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&b, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_323, 64);
            b.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on b");
            mark_overlay_objects_uploaded(&b, entry.ino, 1, &[block.object_id]);
            assert!(
                b.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on b")
            );
            b.flush_metadata().expect("flush winner on b");
        }
    }

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        match (a.lookup(ROOT_INO, b"victim"), b.lookup(ROOT_INO, b"victim")) {
            (Ok(ae), Ok(be)) => match (a.get_attr(ae.ino), b.get_attr(be.ino)) {
                (Ok(a_attr), Ok(b_attr)) => ae.ino == be.ino && a_attr.kind == b_attr.kind,
                _ => false,
            },
            _ => false,
        }
    }));

    let expected = a.lookup(ROOT_INO, b"victim").expect("winner on a");
    let expected_attr = a.get_attr(expected.ino).expect("winner attr on a");

    drop(a);
    drop(b);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    let reopened_b = reopen_pg_engine_with_retry(&db_b);

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        let _ = reopened_b.sync_metadata();
        match (
            reopened_a.lookup(ROOT_INO, b"victim"),
            reopened_b.lookup(ROOT_INO, b"victim"),
        ) {
            (Ok(ae), Ok(be)) => match (reopened_a.get_attr(ae.ino), reopened_b.get_attr(be.ino)) {
                (Ok(a_attr), Ok(b_attr)) => {
                    ae.ino == expected.ino
                        && be.ino == expected.ino
                        && a_attr.kind == expected_attr.kind
                        && b_attr.kind == expected_attr.kind
                }
                _ => false,
            },
            _ => false,
        }
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_concurrent_rename_single_winner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-concurrent-rename");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    a.create_dir(ROOT_INO, b"source.txt", 0, 0, 0o755)
        .expect("seed source");
    a.flush_metadata().expect("flush source");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"source.txt",
        Duration::from_secs(10)
    ));

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_a.txt");
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_b.txt");
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out
    });

    let r1 = t1.join().expect("thread a");
    let r2 = t2.join().expect("thread b");
    let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
    assert!(ok_count >= 1, "at least one rename should apply");
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"source.txt").is_err()
    }));
    let a_exists = a.lookup(ROOT_INO, b"moved_by_a.txt").is_ok();
    let b_exists = a.lookup(ROOT_INO, b"moved_by_b.txt").is_ok();
    assert_ne!(a_exists, b_exists, "only one destination name should exist");
    assert!(
        a.lookup(ROOT_INO, b"source.txt").is_err(),
        "source must be gone"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_remote_rename_overwrite_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-rename-overwrite");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let src = create_published_small_file(&a, ROOT_INO, b"src.txt", 50_310);
    let dst = create_published_small_file(&a, ROOT_INO, b"dst.txt", 50_311);
    a.flush_metadata().expect("flush seed files");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"src.txt",
        Duration::from_secs(10)
    ));
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"dst.txt",
        Duration::from_secs(10)
    ));

    a.rename(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
        .expect("rename overwrite on a");
    a.flush_metadata().expect("flush rename overwrite");
    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"src.txt").is_err()
            && b.lookup(ROOT_INO, b"dst.txt")
                .map(|entry| entry.ino == src.ino)
                .unwrap_or(false)
    }));

    drop(b);
    let reopened_b = reopen_pg_engine_with_retry(&db_b);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_b.sync_metadata();
        reopened_b.lookup(ROOT_INO, b"src.txt").is_err()
            && reopened_b
                .lookup(ROOT_INO, b"dst.txt")
                .map(|entry| entry.ino == src.ino)
                .unwrap_or(false)
            && reopened_b.get_attr(dst.ino).is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_publish_before_local_persist_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-crash-after-publish");
    let info = volume.format_info();
    seed_local(&db, &info);

    let a = PgMetaEngine::open(&db).expect("open a");
    a.create_dir(ROOT_INO, b"draft", 0, 0, 0o755)
        .expect("create local directory");

    let start = a.flushed_event_seq.load(Ordering::SeqCst);
    let end = a.local.get_counter("nextEventSeq").expect("nextEventSeq");
    let events = a.local.read_events_range(start, end).expect("local events");
    let _version = a
        .push_event_log_batch_locked(&events)
        .expect("push batch without local persist");

    drop(a);

    let reopened = reopen_pg_engine_with_retry(&db);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened.sync_metadata();
        reopened.lookup(ROOT_INO, b"draft").is_ok()
            && reopened.flushed_event_seq.load(Ordering::SeqCst)
                == reopened.local.get_counter("nextEventSeq").unwrap_or(0)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_publish_then_remote_unlink_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-crash-publish-then-unlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_dir(ROOT_INO, b"remote.txt", 0, 0, 0o755)
        .expect("create local entry");
    let start = a.flushed_event_seq.load(Ordering::SeqCst);
    let end = a.local.get_counter("nextEventSeq").expect("nextEventSeq");
    let events = a.local.read_events_range(start, end).expect("local events");
    let _version = a
        .push_event_log_batch_locked(&events)
        .expect("push batch without local persist");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(12)
    ));
    b.rmdir(ROOT_INO, b"remote.txt").expect("peer remove");
    b.flush_metadata().expect("flush peer remove");

    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        reopened_a.lookup(ROOT_INO, b"remote.txt").is_err()
            && reopened_a.get_attr(file.ino).is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_unpublished_local_create_flushes_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-unpublished-create");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"draft.txt", 0, 0, 0o755)
        .expect("create local draft");
    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(
        reopened_a.replication_backlog().pending_ops > 0,
        "reopen must rebuild pending state for unpublished local events"
    );

    reopened_a
        .flush_metadata()
        .expect("flush restored unpublished events");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"draft.txt",
        Duration::from_secs(12)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_claimed_batch_crash_flushes_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-claimed-batch-crash");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"draft.txt", 0, 0, 0o755)
        .expect("create local draft");
    let claimed = a.claim_pending_batch(true).expect("claim pending batch");
    assert!(claimed.event_end > claimed.event_start);
    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(
        reopened_a.replication_backlog().pending_ops > 0,
        "reopen must recover a claimed-but-unpublished batch after crash"
    );

    reopened_a.flush_metadata().expect("flush recovered batch");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"draft.txt",
        Duration::from_secs(12)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_claimed_multi_event_batch_preserves_sequence_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-claimed-multievent");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir = a
        .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
        .expect("create drafts dir");
    let file = a
        .create_dir(dir.ino, b"temp.txt", 0, 0, 0o755)
        .expect("create temp dir");
    a.rename(dir.ino, b"temp.txt", dir.ino, b"final.txt")
        .expect("rename temp to final");
    let claimed = a.claim_pending_batch(true).expect("claim pending batch");
    assert!(claimed.event_end > claimed.event_start);
    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    reopened_a
        .flush_metadata()
        .expect("flush recovered multi-event batch");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"drafts") {
            Ok(dir_entry) => {
                b.lookup(dir_entry.ino, b"temp.txt").is_err()
                    && b.lookup(dir_entry.ino, b"final.txt")
                        .map(|entry| entry.ino == file.ino)
                        .unwrap_or(false)
            }
            Err(_) => false,
        }
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_delete_and_recreate() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-delete-recreate");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let first = a
        .create_dir(ROOT_INO, b"recreate.txt", 0, 0, 0o755)
        .expect("create first");
    a.rmdir(ROOT_INO, b"recreate.txt").expect("remove first");
    let second = a
        .create_dir(ROOT_INO, b"recreate.txt", 0, 0, 0o755)
        .expect("create second");
    assert_ne!(
        first.ino, second.ino,
        "recreated entry should have a new inode in current model"
    );

    a.flush_metadata().expect("flush a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"recreate.txt",
        Duration::from_secs(10)
    ));
    let on_b = b.lookup(ROOT_INO, b"recreate.txt").expect("lookup on b");
    assert_eq!(on_b.ino, second.ino);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_rapid_create_delete() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-rapid-cd");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    for i in 0..200 {
        let name = format!("rapid_{i:03}.tmp");
        pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
            .expect("create");
        pg.unlink(ROOT_INO, name.as_bytes()).expect("unlink");
    }

    let entries = pg.readdir(ROOT_INO).expect("readdir root");
    assert!(
        entries.is_empty(),
        "rapid create/delete should leave root empty"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_hardlinks_cross_client() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-hardlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"orig.txt", 50_312);
    a.link(file.ino, ROOT_INO, b"link.txt").expect("hardlink");

    a.flush_metadata().expect("flush a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"orig.txt",
        Duration::from_secs(10)
    ));
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"link.txt",
        Duration::from_secs(10)
    ));
    let orig = b.lookup(ROOT_INO, b"orig.txt").expect("lookup orig");
    let link = b.lookup(ROOT_INO, b"link.txt").expect("lookup link");
    assert_eq!(orig.ino, link.ino, "hardlink should point to same inode");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_symlinks_cross_client() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-symlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let sym = a
        .create_symlink(ROOT_INO, b"my-link", b"target/path.txt", 0, 0)
        .expect("create symlink");
    a.flush_metadata().expect("flush a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"my-link",
        Duration::from_secs(10)
    ));
    let link = b.lookup(ROOT_INO, b"my-link").expect("lookup symlink");
    assert_eq!(link.ino, sym.ino);
    let target = b.get_symlink_target(link.ino).expect("readlink");
    assert_eq!(target, b"target/path.txt");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_update_foreign_visible_symlink_target() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-symlink-update");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let sym = a
        .create_symlink(ROOT_INO, b"shared-link", b"target/a", 0, 0)
        .expect("create symlink on a");
    a.flush_metadata().expect("flush create on a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"shared-link",
        Duration::from_secs(10)
    ));
    assert_eq!(b.get_symlink_target(sym.ino).unwrap(), b"target/a");

    b.set_symlink_target(sym.ino, b"target/b")
        .expect("update symlink on b");
    b.flush_metadata().expect("flush update on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.get_symlink_target(sym.ino)
            .map(|target| target == b"target/b")
            .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_local_xattr_overlay_masks_foreign_update_until_flush() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-xattr-overlay-precedence");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"shared.txt", 50_313);
    a.set_xattr(file.ino, b"user.color", b"red", 0)
        .expect("set red xattr on a");
    a.flush_metadata().expect("flush red on a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"shared.txt",
        Duration::from_secs(10)
    ));
    assert_eq!(
        b.get_xattr(file.ino, b"user.color").unwrap(),
        Some(b"red".to_vec())
    );

    b.set_xattr(file.ino, b"user.color", b"blue", 0)
        .expect("set blue xattr on b");

    a.set_xattr(file.ino, b"user.color", b"green", 0)
        .expect("set green xattr on a");
    a.flush_metadata().expect("flush green on a");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        b.get_xattr(file.ino, b"user.color")
            .map(|value| value == Some(b"blue".to_vec()))
            .unwrap_or(false)
    }));
    assert_eq!(
        b.get_xattr(file.ino, b"user.color").unwrap(),
        Some(b"blue".to_vec()),
        "local xattr overlay should mask foreign base update"
    );

    b.flush_metadata().expect("flush blue on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.get_xattr(file.ino, b"user.color")
            .map(|value| value == Some(b"blue".to_vec()))
            .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_basic_crud() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-basic-crud");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let file = pg
        .create_file(ROOT_INO, b"hello.txt", 0, 0, 0o644)
        .expect("create");
    let looked = pg.lookup(ROOT_INO, b"hello.txt").expect("lookup");
    assert_eq!(file.ino, looked.ino);

    pg.rename(ROOT_INO, b"hello.txt", ROOT_INO, b"renamed.txt")
        .expect("rename");
    assert!(pg.lookup(ROOT_INO, b"hello.txt").is_err());
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_ok());

    pg.unlink(ROOT_INO, b"renamed.txt").expect("unlink");
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_err());

    pg.create_dir(ROOT_INO, b"subdir", 0, 0, 0o755)
        .expect("mkdir");
    pg.rmdir(ROOT_INO, b"subdir").expect("rmdir");
    assert!(pg.lookup(ROOT_INO, b"subdir").is_err());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_deep_path() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-deep-path");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let mut parent = ROOT_INO;
    for n in [b"a", b"b", b"c", b"d", b"e"] {
        let dir = pg.create_dir(parent, n, 0, 0, 0o755).expect("mkdir");
        parent = dir.ino;
    }
    let leaf = pg
        .create_file(parent, b"leaf.txt", 0, 0, 0o644)
        .expect("create leaf");
    let looked = pg.lookup(parent, b"leaf.txt").expect("lookup leaf");
    assert_eq!(leaf.ino, looked.ino);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_large_directory() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-large-dir");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    for i in 0..1000 {
        let name = format!("file_{i:04}.dat");
        pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
            .expect("create");
    }
    let entries = pg.readdir(ROOT_INO).expect("readdir");
    assert_eq!(entries.len(), 1000);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_special_filenames() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-special-names");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let names = vec![
        b"file with spaces.txt".to_vec(),
        "файл_тест_日本語.txt".as_bytes().to_vec(),
        format!("{}.txt", "a".repeat(200)).as_bytes().to_vec(),
        b"file...multiple...dots.txt".to_vec(),
        b"-dashstart.txt".to_vec(),
    ];
    for n in &names {
        pg.create_file(ROOT_INO, n, 0, 0, 0o644).expect("create");
    }
    for n in &names {
        assert!(
            pg.lookup(ROOT_INO, n).is_ok(),
            "name should resolve: {:?}",
            n
        );
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_readdir_during_modifications() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-readdir-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let writer = std::thread::spawn({
        let a = Arc::clone(&a);
        move || {
            for i in 0..120 {
                let name = format!("w_{i:03}.dat");
                let _ = a.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644);
                if i % 3 == 0 {
                    let _ = a.unlink(ROOT_INO, name.as_bytes());
                }
            }
        }
    });

    for _ in 0..80 {
        let _ = b.sync_metadata();
        let _ = b.readdir(ROOT_INO).expect("readdir should not fail");
        std::thread::sleep(Duration::from_millis(10));
    }
    writer.join().expect("writer join");
    b.sync_metadata().expect("final sync");
    let _ = b.readdir(ROOT_INO).expect("final readdir");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_visibility_timing() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-vis-timing");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let mut delays = Vec::new();
    for i in 0..5 {
        let name = format!("vis_{i:02}.dat");
        a.create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
            .expect("create");
        a.flush_metadata().expect("flush a");
        let start = std::time::Instant::now();
        let mut seen = false;
        for _ in 0..50 {
            b.sync_metadata().expect("sync b");
            if b.lookup(ROOT_INO, name.as_bytes()).is_ok() {
                seen = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(seen, "entry should become visible on peer");
        delays.push(start.elapsed().as_millis() as u64);
    }

    let avg = delays.iter().sum::<u64>() / delays.len() as u64;
    assert!(avg < 5000, "avg visibility delay should stay bounded");
}

#[test]
fn sparse_ready_foreign_publish_preserves_disjoint_base_ranges_when_not_complete() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"sparse-ready-merge.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096, 3);
    let local_tail = PayloadRef::new_full(77_055, DEFAULT_BLOCK_SIZE);
    pg.stage_payload_slice(file.ino, 4, &local_tail)
        .expect("stage local tail");
    mark_overlay_objects_uploaded(&pg, file.ino, 3, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .expect("publish local tail"),
        "local sparse tail publish should succeed"
    );
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local sparse publish");
    assert!(
        pg.local_overlay_conflicts()
            .expect("overlay conflicts")
            .block_inodes
            .is_empty(),
        "this reproduces an unprotected peer that already acked its own sparse publish"
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
            .expect("lookup local sparse graph"),
        vec![local_tail.object_id],
        "setup should have a committed sparse tail before the remote peer publishes"
    );

    let remote_middle = PayloadRef::new_full(77_056, DEFAULT_BLOCK_SIZE);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(2, Some(&remote_middle), PayloadRefState::Published)
            .expect("remote middle slices"),
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("load attr");
    remote_attr.size = (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    remote_attr.committed_size = remote_attr.size;
    remote_attr.content_epoch = 4;
    remote_attr.committed_content_epoch = 4;
    remote_attr.upload_state = UploadState::Ready;
    let remote_batch = vec![
        remote_publish,
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
    ];
    assert!(
        RemoteApplyCoordinator::partial_regular_file_publish_inodes(&remote_batch)
            .contains(&file.ino),
        "sparse ready publish payloads do not cover committed EOF"
    );

    apply_foreign_batch(
        &pg,
        &remote_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply sparse ready foreign publish");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged sparse graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_middle.object_id, local_tail.object_id],
        "a sparse ready publish that does not cover committed EOF must not erase disjoint published ranges"
    );
}

#[test]
fn sparse_ready_self_batch_preserves_imported_disjoint_base_ranges_when_not_complete() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"sparse-ready-self-merge.txt", 0, 0, 0o644)
        .expect("create file");
    let logical_size = (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    set_file_state(&pg, file.ino, logical_size, 3);

    let imported_tail = PayloadRef::new_full(77_057, DEFAULT_BLOCK_SIZE);
    let imported_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(4, Some(&imported_tail), PayloadRefState::Published)
            .expect("imported tail slices"),
    };
    pg.local
        .apply_replica_events_to_base_preserving_protected_base_ranges(
            std::slice::from_ref(&imported_publish),
            &HashSet::from([file.ino]),
        )
        .expect("seed imported sparse tail");
    assert_eq!(
        lookup_object_ids(&pg, file.ino, logical_size).expect("lookup imported graph"),
        vec![imported_tail.object_id],
        "setup should have an imported sparse tail before self replay"
    );

    let local_middle = PayloadRef::new_full(77_058, DEFAULT_BLOCK_SIZE);
    let local_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(2, Some(&local_middle), PayloadRefState::Published)
            .expect("local middle slices"),
    };
    let mut local_attr = pg.get_attr(file.ino).expect("load attr");
    local_attr.size = logical_size;
    local_attr.committed_size = local_attr.size;
    local_attr.content_epoch = 4;
    local_attr.committed_content_epoch = 4;
    local_attr.upload_state = UploadState::Ready;
    let self_batch = vec![
        local_publish,
        MetaEvent::SetAttr {
            attr: local_attr.clone(),
        },
    ];
    assert!(
        RemoteApplyCoordinator::partial_regular_file_publish_inodes(&self_batch)
            .contains(&file.ino),
        "self sparse publish payloads do not cover committed EOF"
    );

    apply_visible_self_batch(&pg, &self_batch).expect("apply visible self sparse publish");

    let object_ids =
        lookup_object_ids(&pg, file.ino, logical_size).expect("lookup merged sparse graph");
    assert_eq!(
        object_ids,
        vec![local_middle.object_id, imported_tail.object_id],
        "a sparse self publish that does not cover committed EOF must not erase imported disjoint ranges"
    );
}

#[test]
fn sparse_ready_foreign_publish_sets_serving_size_when_payload_graph_is_partial() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let seed_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("seed start");
    let file = writer
        .create_file(ROOT_INO, b"sparse-partial-serving-size.bin", 0, 0, 0o644)
        .expect("create sparse file");
    set_file_state(&writer, file.ino, 233, 1);
    let head = PayloadRef::new_partial(77_070, 233);
    writer
        .stage_payload_slice(file.ino, 0, &head)
        .expect("stage sparse head");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[head.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish sparse head"),
        "initial sparse head publish should succeed"
    );
    let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");
    apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

    let remote_file = remote
        .lookup(ROOT_INO, b"sparse-partial-serving-size.bin")
        .expect("lookup remote sparse file");
    let seeded_attr = remote.get_attr(remote_file.ino).expect("load seeded attr");
    assert_eq!(seeded_attr.size, 233);

    let sparse_start = seed_end;
    let logical_size = (2 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    set_file_state(&writer, file.ino, logical_size, 2);
    let tail = PayloadRef::new_partial(77_071, 4096);
    writer
        .stage_payload_slice(file.ino, 2, &tail)
        .expect("stage sparse tail");
    mark_overlay_objects_uploaded(&writer, file.ino, 2, &[tail.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish sparse tail"),
        "sparse tail publish should succeed"
    );
    let sparse_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("sparse end");

    apply_projected_remote_range(&writer, &remote, sparse_start, sparse_end);

    let remote_attr = remote.get_attr(remote_file.ino).expect("load remote attr");
    assert_eq!(
        remote_attr.size, logical_size,
        "sparse Ready publish must advance peer serving size even when the publish graph has holes"
    );
    assert_eq!(remote_attr.committed_size, logical_size);
    assert_eq!(
        lookup_object_ids(&remote, remote_file.ino, logical_size)
            .expect("lookup remote sparse graph"),
        vec![head.object_id, tail.object_id]
    );
}

#[test]
fn metadata_only_sparse_extend_replay_advances_peer_logical_size() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = u64::from(DEFAULT_BLOCK_SIZE);

    let seed_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("seed start");
    let file = writer
        .create_file(ROOT_INO, b"sparse-metadata-only-extend.bin", 0, 0, 0o644)
        .expect("create sparse file");
    let old_size = block_size + 64;
    set_file_state(&writer, file.ino, old_size, 1);
    let head = PayloadRef::new_full(77_090, DEFAULT_BLOCK_SIZE);
    let tail = PayloadRef::new_partial(77_091, 64);
    writer
        .stage_payload_slice(file.ino, 0, &head)
        .expect("stage head");
    writer
        .stage_payload_slice(file.ino, 1, &tail)
        .expect("stage tail");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[head.object_id, tail.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish initial sparse file"),
        "initial sparse publish should succeed"
    );
    let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");
    apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

    let remote_file = remote
        .lookup(ROOT_INO, b"sparse-metadata-only-extend.bin")
        .expect("lookup seeded file");
    assert_eq!(
        remote.get_attr(remote_file.ino).expect("seeded attr").size,
        old_size
    );

    let extend_start = seed_end;
    let logical_size = (2 * block_size) + 123;
    set_file_state(&writer, file.ino, logical_size, 2);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish metadata-only sparse extend"),
        "metadata-only sparse extend should publish"
    );
    let extend_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("extend end");

    apply_projected_remote_range(&writer, &remote, extend_start, extend_end);

    let remote_attr = remote.get_attr(remote_file.ino).expect("extended attr");
    assert_eq!(
        remote_attr.size, logical_size,
        "metadata-only sparse extend must advance peer serving size"
    );
    assert_eq!(
        remote_attr.committed_size, logical_size,
        "metadata-only sparse extend must advance peer committed EOF"
    );
    assert_eq!(
        lookup_object_ids(&remote, remote_file.ino, logical_size)
            .expect("lookup sparse graph after metadata-only extend"),
        vec![head.object_id, tail.object_id]
    );
}

#[test]
fn ready_complete_publish_preserved_base_ranges_sets_remote_serving_size() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-preserved-size.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 446, 4);
    let local_payload = PayloadRef::new_partial(77_083, 446);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 4, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 4)
            .expect("publish local payload"),
        "local payload publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let remote_full = PayloadRef::new_partial(77_084, 227);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(0, 227).expect("remote full range"),
                payload: Some(remote_full),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    let mut ready_attr = pg.get_attr(file.ino).expect("load current attr");
    ready_attr.size = 227;
    ready_attr.committed_size = 227;
    ready_attr.content_epoch = 4;
    ready_attr.committed_content_epoch = 4;
    ready_attr.upload_state = UploadState::Ready;

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: ready_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply ready foreign publish");

    let served = pg.get_attr(file.ino).expect("load served attr");
    assert_eq!(
        served.size, ready_attr.size,
        "full Ready foreign publish must set remote serving size even when protected base ranges are preserved"
    );
}

#[test]
fn unit_remote_projection_converges_nested_rename_overwrite_to_source_inode() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let seed_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("seed start");
    let dir = writer
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create dir");
    let old = writer
        .create_file(dir.ino, b"final.bin", 0, 0, 0o644)
        .expect("create old final");
    set_file_state(&writer, old.ino, 4096, 1);
    let old_block = PayloadRef::new_partial(901, 4096);
    writer
        .stage_payload_slice(old.ino, 0, &old_block)
        .expect("set old block");
    mark_overlay_objects_uploaded(&writer, old.ino, 1, &[old_block.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(old.ino, 1)
            .expect("publish old base graph"),
        "old base graph publish must succeed"
    );
    let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");

    apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

    let remote_dir = remote.lookup(ROOT_INO, b"shared").expect("lookup dir");
    let remote_old = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup seeded final");
    assert_eq!(remote_old.ino, old.ino);

    let replace_start = seed_end;
    let temp = writer
        .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
        .expect("create replacement temp");
    set_file_state(&writer, temp.ino, block_size + 4096, 2);
    let new_block0 = PayloadRef::new_full(902, DEFAULT_BLOCK_SIZE);
    let new_block1 = PayloadRef::new_partial(903, 4096);
    writer
        .stage_payload_slice(temp.ino, 0, &new_block0)
        .expect("set replacement payload slice 0");
    writer
        .stage_payload_slice(temp.ino, 1, &new_block1)
        .expect("set replacement payload slice 1");
    mark_overlay_objects_uploaded(
        &writer,
        temp.ino,
        2,
        &[new_block0.object_id, new_block1.object_id],
    );
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(temp.ino, 2)
            .expect("publish replacement temp"),
        "replacement temp publish must succeed"
    );
    writer
        .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
        .expect("rename replacement over final");
    let replace_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("replace end");

    apply_projected_remote_range(&writer, &remote, replace_start, replace_end);

    let remote_final = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup replaced final");
    assert_eq!(remote_final.ino, temp.ino);
    assert!(
        remote.get_attr(old.ino).is_err(),
        "old inode must be removed"
    );
    let remote_attr = remote
        .get_attr(remote_final.ino)
        .expect("get replaced attr");
    assert_eq!(remote_attr.size, block_size + 4096);
    assert_eq!(remote_attr.committed_size, block_size + 4096);
    assert_eq!(remote_attr.committed_content_epoch, 2);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_final.ino, remote_attr.size)
            .expect("replacement object ids"),
        vec![new_block0.object_id, new_block1.object_id]
    );
}
