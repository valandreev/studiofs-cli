#[test]
fn test_create_file_and_lookup() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"test.txt", 1000, 1000, 0o644)
        .unwrap();
    assert_eq!(attr.kind, FileType::RegularFile);
    assert_eq!(attr.mode, 0o644);

    let entry = engine.lookup(ROOT_INO, b"test.txt").unwrap();
    assert_eq!(entry.ino, attr.ino);
    assert_eq!(entry.kind, FileType::RegularFile);
}

#[test]
fn namespace_create_spec_marks_directory_parent_update() {
    let spec = NamespaceCreateSpec::new_dir(41, ROOT_INO, b"subdir", 1000, 1000, 0o755);

    assert_eq!(spec.kind(), FileType::Directory);
    assert!(spec.updates_parent_attr());
    assert_eq!(spec.dir_entry_value().kind, FileType::Directory);

    let attr = spec.initial_attr();
    assert_eq!(attr.kind, FileType::Directory);
    assert_eq!(attr.parent, ROOT_INO);
    assert_eq!(attr.mode, 0o755);
}

#[test]
fn namespace_create_spec_builds_symlink_target_event() {
    let spec = NamespaceCreateSpec::new_symlink(52, ROOT_INO, b"link", b"/tmp/target", 7, 8);

    assert_eq!(spec.kind(), FileType::Symlink);
    assert!(!spec.updates_parent_attr());
    assert_eq!(spec.symlink_target(), Some(&b"/tmp/target"[..]));

    let attr = spec.initial_attr();
    assert_eq!(attr.kind, FileType::Symlink);
    assert_eq!(attr.uid, 7);
    assert_eq!(attr.gid, 8);
    assert_eq!(attr.size, b"/tmp/target".len() as u64);

    assert!(matches!(
        spec.extra_event(),
        Some(crate::events::MetaEvent::SetSymlinkTarget { ino: 52, target })
            if target == b"/tmp/target"
    ));
}

#[test]
fn namespace_delete_spec_open_handle_records_deferred_delete() {
    let spec = NamespaceDeleteSpec::new_unlink_open_handle(ROOT_INO, b"ghost.txt");

    assert!(spec.records_deferred_delete());
    assert!(!spec.updates_parent_nlink());
    assert!(!spec.allows_directory_target());
}

#[test]
fn namespace_delete_spec_rmdir_updates_parent_nlink() {
    let spec = NamespaceDeleteSpec::new_rmdir(ROOT_INO, b"empty");

    assert!(!spec.records_deferred_delete());
    assert!(spec.updates_parent_nlink());
    assert!(spec.allows_directory_target());
}

#[test]
fn test_create_dir_and_readdir() {
    let engine = temp_engine();
    let dir_attr = engine
        .create_dir(ROOT_INO, b"subdir", 1000, 1000, 0o755)
        .unwrap();
    assert_eq!(dir_attr.kind, FileType::Directory);

    // Parent nlink should be 3 (root=2, +1 for subdir)
    let root = engine.get_attr(ROOT_INO).unwrap();
    assert_eq!(root.nlink, 3);

    let entries = engine.readdir(ROOT_INO).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, b"subdir");
}

#[test]
fn test_unlink_file() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"to_delete.txt", 1000, 1000, 0o644)
        .unwrap();

    engine.unlink(ROOT_INO, b"to_delete.txt").unwrap();

    assert!(engine.lookup(ROOT_INO, b"to_delete.txt").is_err());
    assert!(engine.get_attr(attr.ino).is_err());
}

#[test]
fn test_unlink_open_handle_keeps_inode_and_decrements_nlink() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"open-unlink.txt", 1000, 1000, 0o644)
        .unwrap();

    let result = engine
        .unlink_open_handle(ROOT_INO, b"open-unlink.txt")
        .unwrap();

    assert_eq!(result.ino, attr.ino);
    assert_eq!(result.nlink, 0);
    assert!(engine.lookup(ROOT_INO, b"open-unlink.txt").is_err());
    let persisted = engine.get_attr(attr.ino).unwrap();
    assert_eq!(persisted.nlink, 0);
    let deferred = engine.list_deferred_deletes().unwrap();
    assert!(deferred.iter().any(|record| record.ino == attr.ino));
    engine.remove_deferred_delete(attr.ino).unwrap();
    let deferred_after = engine.list_deferred_deletes().unwrap();
    assert!(!deferred_after.iter().any(|record| record.ino == attr.ino));
}

#[test]
fn test_rmdir() {
    let engine = temp_engine();
    engine
        .create_dir(ROOT_INO, b"empty_dir", 1000, 1000, 0o755)
        .unwrap();
    engine.rmdir(ROOT_INO, b"empty_dir").unwrap();

    assert!(engine.lookup(ROOT_INO, b"empty_dir").is_err());
    let root = engine.get_attr(ROOT_INO).unwrap();
    assert_eq!(root.nlink, 2); // back to initial
}

#[test]
fn test_rmdir_notempty() {
    let engine = temp_engine();
    let dir = engine
        .create_dir(ROOT_INO, b"notempty", 1000, 1000, 0o755)
        .unwrap();
    engine
        .create_file(dir.ino, b"file.txt", 1000, 1000, 0o644)
        .unwrap();

    let result = engine.rmdir(ROOT_INO, b"notempty");
    assert!(result.is_err());
}

#[test]
fn test_rmdir_uses_inode_kind_for_legacy_directory_entry() {
    let engine = temp_engine();
    let dir = engine
        .create_dir(ROOT_INO, b"ajadisktest.dat", 1000, 1000, 0o755)
        .unwrap();

    let removed = engine.remove_entry(ROOT_INO, b"ajadisktest.dat").unwrap();
    assert_eq!(removed.ino, dir.ino);
    assert_eq!(removed.kind, FileType::Directory);
    engine
        .add_entry(
            ROOT_INO,
            b"ajadisktest.dat",
            &DirEntryValue {
                ino: dir.ino,
                kind: FileType::RegularFile,
            },
        )
        .unwrap();

    let lookup = engine.lookup(ROOT_INO, b"ajadisktest.dat").unwrap();
    assert_eq!(lookup.kind, FileType::RegularFile);
    assert_eq!(engine.get_attr(dir.ino).unwrap().kind, FileType::Directory);

    engine.rmdir(ROOT_INO, b"ajadisktest.dat").unwrap();

    assert!(engine.lookup(ROOT_INO, b"ajadisktest.dat").is_err());
    assert!(engine.get_attr(dir.ino).is_err());
    assert_eq!(engine.get_attr(ROOT_INO).unwrap().nlink, 2);
}

#[test]
fn test_unlink_rejects_legacy_directory_entry_with_stale_file_dentry_kind() {
    let engine = temp_engine();
    let dir = engine
        .create_dir(ROOT_INO, b"ajadisktest.dat", 1000, 1000, 0o755)
        .unwrap();

    engine.remove_entry(ROOT_INO, b"ajadisktest.dat").unwrap();
    engine
        .add_entry(
            ROOT_INO,
            b"ajadisktest.dat",
            &DirEntryValue {
                ino: dir.ino,
                kind: FileType::RegularFile,
            },
        )
        .unwrap();

    let result = engine.unlink(ROOT_INO, b"ajadisktest.dat");
    assert!(matches!(result, Err(MetaError::IsADirectory(ino)) if ino == dir.ino));
    assert_eq!(engine.lookup(ROOT_INO, b"ajadisktest.dat").unwrap().ino, dir.ino);
    assert_eq!(engine.get_attr(ROOT_INO).unwrap().nlink, 3);
}

#[test]
fn test_rename() {
    let engine = temp_engine();
    let src = engine
        .create_file(ROOT_INO, b"old.txt", 1000, 1000, 0o644)
        .unwrap();
    let displaced = engine
        .rename_atomic(ROOT_INO, b"old.txt", ROOT_INO, b"new.txt")
        .unwrap();
    assert_eq!(displaced.moved_ino, src.ino);
    assert!(displaced.displaced_ino.is_none());

    assert!(engine.lookup(ROOT_INO, b"old.txt").is_err());
    assert!(engine.lookup(ROOT_INO, b"new.txt").is_ok());
}

#[test]
fn test_rename_overwrite_file() {
    let engine = temp_engine();
    let src = engine
        .create_file(ROOT_INO, b"src.txt", 1000, 1000, 0o644)
        .unwrap();
    let dst = engine
        .create_file(ROOT_INO, b"dst.txt", 1000, 1000, 0o644)
        .unwrap();

    let displaced = engine
        .rename_atomic(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
        .unwrap();

    // Should return the displaced inode
    assert_eq!(displaced.moved_ino, src.ino);
    assert_eq!(displaced.displaced_ino, Some(dst.ino));
    // Source is gone, dest now points to src's inode
    assert!(engine.lookup(ROOT_INO, b"src.txt").is_err());
    let entry = engine.lookup(ROOT_INO, b"dst.txt").unwrap();
    assert_eq!(entry.ino, src.ino);
    // Displaced inode should be removed (nlink was 1 → 0)
    assert!(engine.get_attr(dst.ino).is_err());
}

#[test]
fn test_rename_overwrite_empty_dir() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"file.txt", 1000, 1000, 0o644)
        .unwrap();
    let dir = engine
        .create_dir(ROOT_INO, b"empty_dir", 1000, 1000, 0o755)
        .unwrap();

    let displaced = engine
        .rename_atomic(ROOT_INO, b"file.txt", ROOT_INO, b"empty_dir")
        .unwrap();

    assert_eq!(displaced.moved_ino, file.ino);
    assert_eq!(displaced.displaced_ino, Some(dir.ino));
    assert!(engine.get_attr(dir.ino).is_err());
    let entry = engine.lookup(ROOT_INO, b"empty_dir").unwrap();
    assert_eq!(entry.ino, file.ino);
}

#[test]
fn test_rename_overwrite_nonempty_dir_fails() {
    let engine = temp_engine();
    engine
        .create_file(ROOT_INO, b"file.txt", 1000, 1000, 0o644)
        .unwrap();
    let dir = engine
        .create_dir(ROOT_INO, b"notempty", 1000, 1000, 0o755)
        .unwrap();
    engine
        .create_file(dir.ino, b"child.txt", 1000, 1000, 0o644)
        .unwrap();

    let result = engine.rename_atomic(ROOT_INO, b"file.txt", ROOT_INO, b"notempty");
    assert!(result.is_err());
    // Source should still exist (rollback in same txn)
    assert!(engine.lookup(ROOT_INO, b"file.txt").is_ok());
    assert!(engine.lookup(ROOT_INO, b"notempty").is_ok());
}

#[test]
fn test_rename_move_dir_between_parents() {
    let engine = temp_engine();
    let dir_a = engine
        .create_dir(ROOT_INO, b"a", 1000, 1000, 0o755)
        .unwrap();
    let dir_b = engine
        .create_dir(ROOT_INO, b"b", 1000, 1000, 0o755)
        .unwrap();
    // root nlink = 2 + 2 subdirs = 4
    assert_eq!(engine.get_attr(ROOT_INO).unwrap().nlink, 4);

    // Create a subdir inside /a
    let sub = engine
        .create_dir(dir_a.ino, b"sub", 1000, 1000, 0o755)
        .unwrap();
    // /a nlink = 2 + 1 subdir = 3
    assert_eq!(engine.get_attr(dir_a.ino).unwrap().nlink, 3);
    assert_eq!(engine.get_attr(dir_b.ino).unwrap().nlink, 2);

    // Move /a/sub → /b/sub
    let displaced = engine
        .rename_atomic(dir_a.ino, b"sub", dir_b.ino, b"sub")
        .unwrap();
    assert_eq!(displaced.moved_ino, sub.ino);
    assert!(displaced.displaced_ino.is_none());

    // /a nlink = 2 (lost subdir)
    assert_eq!(engine.get_attr(dir_a.ino).unwrap().nlink, 2);
    // /b nlink = 3 (gained subdir)
    assert_eq!(engine.get_attr(dir_b.ino).unwrap().nlink, 3);
    // sub's parent should now be dir_b
    assert_eq!(engine.get_attr(sub.ino).unwrap().parent, dir_b.ino);
    // Dentry moved
    assert!(engine.lookup(dir_a.ino, b"sub").is_err());
    assert!(engine.lookup(dir_b.ino, b"sub").is_ok());
}

#[test]
fn test_rename_nonexistent_source() {
    let engine = temp_engine();
    let result = engine.rename_atomic(ROOT_INO, b"nope.txt", ROOT_INO, b"dst.txt");
    assert!(result.is_err());
}

#[test]
fn test_rename_concurrent_single_winner() {
    let engine = Arc::new(temp_engine());
    let src = engine
        .create_file(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(3));
    let a_engine = Arc::clone(&engine);
    let a_barrier = Arc::clone(&barrier);
    let t_a = std::thread::spawn(move || {
        a_barrier.wait();
        a_engine.rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_a.txt")
    });

    let b_engine = Arc::clone(&engine);
    let b_barrier = Arc::clone(&barrier);
    let t_b = std::thread::spawn(move || {
        b_barrier.wait();
        b_engine.rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_b.txt")
    });

    barrier.wait();

    let out_a = t_a.join().expect("rename thread a panicked");
    let out_b = t_b.join().expect("rename thread b panicked");
    let ok_count = [out_a.is_ok(), out_b.is_ok()]
        .into_iter()
        .filter(|ok| *ok)
        .count();
    assert_eq!(ok_count, 1, "exactly one concurrent rename should win");
    assert!(
        engine.lookup(ROOT_INO, b"source.txt").is_err(),
        "source entry must be removed after winning rename"
    );

    let a_entry = engine.lookup(ROOT_INO, b"moved_by_a.txt").ok();
    let b_entry = engine.lookup(ROOT_INO, b"moved_by_b.txt").ok();
    assert_ne!(
        a_entry.is_some(),
        b_entry.is_some(),
        "only one destination name should remain visible"
    );
    if let Some(entry) = a_entry {
        assert_eq!(entry.ino, src.ino);
    }
    if let Some(entry) = b_entry {
        assert_eq!(entry.ino, src.ino);
    }
}

#[test]
fn redb_rename_keeps_same_inode_and_incarnation_ids() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"rename-inc.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 7;
    engine.set_attr(&attr).unwrap();
    engine
        .set_file_block(file.ino, 0, &BlockRef::new_partial(401, 4096))
        .unwrap();
    assert!(engine.publish_file_manifest(file.ino, 7).unwrap());

    engine
        .rename_atomic(
            ROOT_INO,
            b"rename-inc.bin",
            ROOT_INO,
            b"rename-inc-next.bin",
        )
        .unwrap();

    let renamed = engine.lookup(ROOT_INO, b"rename-inc-next.bin").unwrap();
    assert_eq!(renamed.ino, file.ino);
    let renamed_attr = engine.get_attr(renamed.ino).unwrap();
    assert_eq!(renamed_attr.content_epoch, 7);
    assert_eq!(renamed_attr.committed_content_epoch, 7);
    assert_eq!(renamed_attr.upload_state, UploadState::Ready);
}

#[test]
fn test_link_basic() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"original.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    assert_eq!(attr.nlink, 1);

    // Create hardlink
    let linked = engine.link(ino, ROOT_INO, b"hardlink.txt").unwrap();
    assert_eq!(linked.ino, ino);
    assert_eq!(linked.nlink, 2);

    // Both names should resolve to the same inode
    let entry1 = engine.lookup(ROOT_INO, b"original.txt").unwrap();
    let entry2 = engine.lookup(ROOT_INO, b"hardlink.txt").unwrap();
    assert_eq!(entry1.ino, ino);
    assert_eq!(entry2.ino, ino);
}

#[test]
fn test_link_directory_fails() {
    let engine = temp_engine();

    let dir = engine.create_dir(ROOT_INO, b"mydir", 0, 0, 0o755).unwrap();

    let result = engine.link(dir.ino, ROOT_INO, b"dir_link");
    assert!(result.is_err()); // EPERM: can't hardlink directories
}

#[test]
fn test_link_duplicate_name_fails() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();
    engine
        .create_file(ROOT_INO, b"taken.txt", 0, 0, 0o644)
        .unwrap();

    let result = engine.link(attr.ino, ROOT_INO, b"taken.txt");
    assert!(result.is_err());
}

#[test]
fn test_link_unlink_nlink_tracking() {
    let engine = temp_engine();

    let attr = engine.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Create two hardlinks: nlink should be 3
    engine.link(ino, ROOT_INO, b"b.txt").unwrap();
    let linked = engine.link(ino, ROOT_INO, b"c.txt").unwrap();
    assert_eq!(linked.nlink, 3);

    // Unlink one: nlink=2, inode still exists
    let unlinked = engine.unlink(ROOT_INO, b"a.txt").unwrap();
    assert_eq!(unlinked.nlink, 2);
    assert!(engine.get_attr(ino).is_ok());

    // Unlink another: nlink=1, still exists
    let unlinked2 = engine.unlink(ROOT_INO, b"b.txt").unwrap();
    assert_eq!(unlinked2.nlink, 1);
    assert!(engine.get_attr(ino).is_ok());

    // Unlink last: nlink=0, inode removed
    let unlinked3 = engine.unlink(ROOT_INO, b"c.txt").unwrap();
    assert_eq!(unlinked3.nlink, 0);
    assert!(engine.get_attr(ino).is_err());
}

#[test]
fn test_unlink_last_link_cleans_blocks_for_inode_with_0xff_suffix() {
    let engine = temp_engine();
    engine.set_counter(counters::NEXT_INODE, 255).unwrap();

    let attr = engine
        .create_file(ROOT_INO, b"ff-suffix.bin", 0, 0, 0o644)
        .unwrap();
    assert_eq!(attr.ino, 255);

    let mut persisted = engine.get_attr(attr.ino).unwrap();
    persisted.size = 4096;
    engine.set_attr(&persisted).unwrap();
    engine
        .set_file_block(attr.ino, 0, &BlockRef::new_partial(4_255, 4096))
        .unwrap();

    let removed = engine.unlink(ROOT_INO, b"ff-suffix.bin").unwrap();
    assert_eq!(removed.nlink, 0);
    assert!(engine.get_attr(attr.ino).is_err());
    assert!(engine.get_file_blocks(attr.ino).unwrap().is_empty());
}

// === xattr Tests ===
