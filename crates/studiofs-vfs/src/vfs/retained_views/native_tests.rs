//! Tests for the synthesized native trash projection: root visibility, paired
//! `$R`/`$I` names, the `$I` v2 record, and local draft sidecars.

use super::super::tests::make_unstarted_test_vfs;
use super::super::{ROOT_INO, StudioVfs};
use super::{PRIVATE_STORE_NAME, TRASH_ROOT_NAME, trash_view_name};
use std::sync::Arc;
use studiofs_meta::trash::MetaVolumeSettings;
use studiofs_meta::{Ino, MetaFsOps, MetaInodes, MetaNamespace, RedbMetaEngine};
use tokio::runtime::Runtime;

fn retained_source(
    tmp: &tempfile::TempDir,
    tag: &str,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>, Ino, Ino) {
    let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, tag);
    let (trash, receipt) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let source = vfs.create(ROOT_INO, b"source", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"orig").await.unwrap();
        vfs.fsync(source.ino, false).await.unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        let receipt = meta.next_inode().unwrap();
        meta.retain_namespace_entry(ROOT_INO, b"source", store, receipt)
            .unwrap();
        (trash, receipt)
    });
    (rt, vfs, meta, trash, receipt)
}

#[test]
fn trash_root_is_visible_once_retention_exists_and_lists_matching_pairs() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-list-");
    let (_rt, vfs, _meta, trash, receipt) = retained_source(&tmp, "retained-native-list");

    let root_names: Vec<Vec<u8>> = vfs
        .readdir(ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(root_names.iter().any(|name| name == TRASH_ROOT_NAME));
    assert!(root_names.iter().all(|name| name != PRIVATE_STORE_NAME));
    assert!(vfs.lookup(ROOT_INO, TRASH_ROOT_NAME).is_ok());

    let suffix = format!("{receipt:06X}");
    let listed: Vec<String> = vfs
        .readdir(trash)
        .unwrap()
        .into_iter()
        .map(|entry| String::from_utf8(entry.name).unwrap())
        .collect();
    assert_eq!(
        listed,
        vec![format!("$R{suffix}"), format!("$I{suffix}")],
        "one $R/$I pair with a shared suffix"
    );
    assert_eq!(listed[0], trash_view_name(receipt));
}

#[test]
fn sidecar_is_v2_with_the_trusted_original_path_and_payload_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-sidecar-");
    let (rt, vfs, _meta, _trash, receipt) = retained_source(&tmp, "retained-native-sidecar");
    let sidecar = format!("$I{receipt:06X}");
    vfs.set_native_trash_path_prefix("W:\\");

    let attr = vfs
        .lookup(vfs.trash_root_ino().unwrap().unwrap(), sidecar.as_bytes())
        .unwrap();
    let bytes = rt
        .block_on(vfs.read(attr.ino, 0, attr.size as u32))
        .unwrap();

    assert_eq!(
        u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        2,
        "version"
    );
    assert_eq!(
        u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        4,
        "payload size"
    );
    let units = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let path: Vec<u16> = bytes[28..28 + units * 2]
        .chunks(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect();
    assert_eq!(path.last(), Some(&0), "null terminated");
    let text = String::from_utf16(&path[..path.len() - 1]).unwrap();
    assert_eq!(
        text, "W:\\source",
        "prefix from the local mount, path from the receipt"
    );
}

#[test]
fn draft_sidecar_writes_stay_local_and_publish_nothing() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-draft-");
    let (rt, vfs, meta, trash, _receipt) = retained_source(&tmp, "retained-native-draft");

    // Shell creates a draft `$I` before the rename; a foreign name must stay local.
    let draft = b"$I0000AB";
    vfs.create(trash, draft, 0, 0, 0o644).unwrap();
    rt.block_on(vfs.write(vfs.lookup(trash, draft).unwrap().ino, 0, b"client-path"))
        .unwrap();

    assert!(
        meta.lookup(trash, draft).is_err(),
        "draft must not be published to shared metadata"
    );
    assert!(vfs.lookup(trash, draft).is_ok(), "draft is visible locally");
}

/// A payload written but not yet flushed when Shell recycles it. The WinFSP
/// `close` that drains it never arrives, so the background drain must finish
/// on the retained inode alone, or restore stays refused.
#[test]
fn retained_payload_with_unflushed_writes_drains_in_the_background() {
    use super::super::LocalSyncPolicy;
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-unflushed-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-native-unflushed");
    let (payload, receipt) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let source = vfs.create(ROOT_INO, b"source", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"restore-me").await.unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        let receipt = meta.next_inode().unwrap();
        meta.retain_namespace_entry(ROOT_INO, b"source", store, receipt)
            .unwrap();
        (source.ino, receipt)
    });
    assert!(meta.retained_entry(receipt).unwrap().is_some());
    assert!(
        vfs.inode_has_pending_local_state(payload),
        "precondition: the write must still be pending"
    );
    rt.block_on(vfs.flush_local_dirty_state_background(payload, LocalSyncPolicy::Strict))
        .expect("background drain of a retained payload");

    assert!(!vfs.inode_has_pending_local_state(payload));
}

/// Reading a `$R` view returns the retained payload's bytes, never zeros.
#[test]
fn trash_view_read_returns_retained_payload_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-view-read-");
    let (rt, vfs, _meta, trash, receipt) = retained_source(&tmp, "retained-native-view-read");
    let view = trash_view_name(receipt);

    let attr = vfs.lookup(trash, view.as_bytes()).unwrap();
    assert_eq!(attr.size, 4);
    let bytes = rt.block_on(vfs.read(attr.ino, 0, 4)).unwrap();
    assert_eq!(&bytes[..], b"orig");

    let mut buffer = [0u8; 4];
    let read = rt
        .block_on(vfs.read_into(attr.ino, 0, &mut buffer))
        .unwrap();
    assert_eq!((read, &buffer), (4, b"orig"));
}

/// Shell reads `$R` by its listed size; a stale committed size of 0 reads nothing.
#[test]
fn trash_view_of_unflushed_payload_reports_and_reads_pending_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-view-unflushed-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-native-view-unflushed");
    let (trash, receipt) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let source = vfs.create(ROOT_INO, b"source", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"restore-me").await.unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        let receipt = meta.next_inode().unwrap();
        meta.retain_namespace_entry(ROOT_INO, b"source", store, receipt)
            .unwrap();
        (trash, receipt)
    });

    let attr = vfs
        .lookup(trash, trash_view_name(receipt).as_bytes())
        .unwrap();
    assert_eq!(attr.size, 10, "view size includes pending local bytes");
    let bytes = rt.block_on(vfs.read(attr.ino, 0, 10)).unwrap();
    assert_eq!(&bytes[..], b"restore-me");
}

/// Restore right after an unflushed write must drain the payload's own accepted
/// bytes first. Shell's Undelete runs at once, before the background drain.
#[test]
fn trash_view_restore_drains_unflushed_writes_first() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-restore-unflushed-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-native-restore-unflushed");
    let (trash, receipt) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let source = vfs.create(ROOT_INO, b"source", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"orig").await.unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        let receipt = meta.next_inode().unwrap();
        meta.retain_namespace_entry(ROOT_INO, b"source", store, receipt)
            .unwrap();
        (trash, receipt)
    });
    let view = trash_view_name(receipt);

    vfs.rename(trash, view.as_bytes(), ROOT_INO, b"restored", false)
        .expect("restore after an unflushed write");

    let restored = meta.lookup(ROOT_INO, b"restored").unwrap();
    let bytes = rt.block_on(vfs.read(restored.ino, 0, 4)).unwrap();
    assert_eq!(&bytes[..], b"orig");
}

#[test]
fn directory_trash_view_exposes_directory_kind_subtree_and_recursive_sidecar_size() {
    use super::super::FileType;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-dir-view-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-native-dir-view");
    let (trash, receipt) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = vfs.mkdir(ROOT_INO, b"dir", 0, 0, 0o755).unwrap();
        let a = vfs.create(dir.ino, b"a.txt", 0, 0, 0o644).unwrap();
        vfs.write(a.ino, 0, b"alpha-123").await.unwrap();
        vfs.fsync(a.ino, false).await.unwrap();
        let nested = vfs.mkdir(dir.ino, b"nested", 0, 0, 0o755).unwrap();
        let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
        // Leave b.txt unflushed so recursive sidecar size and subtree read test dirty bytes.
        vfs.write(b.ino, 0, b"beta-4567").await.unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(ROOT_INO, b"dir", trash, b"$RSHELL01", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let receipt = meta.readdir(store).unwrap()[0].ino;
        (trash, receipt)
    });

    let view = trash_view_name(receipt);
    let sidecar = format!("$I{receipt:06X}");
    let r_attr = vfs.lookup(trash, view.as_bytes()).unwrap();
    assert_eq!(r_attr.kind, FileType::Directory);

    let i_attr = vfs.lookup(trash, sidecar.as_bytes()).unwrap();
    let i_bytes = rt
        .block_on(vfs.read(i_attr.ino, 0, i_attr.size as u32))
        .unwrap();
    assert_eq!(
        u64::from_le_bytes(i_bytes[8..16].try_into().unwrap()),
        18,
        "directory $I v2 size must equal sum of nested regular files"
    );

    let root_children: Vec<_> = vfs
        .readdir_with_attrs(r_attr.ino)
        .unwrap()
        .into_iter()
        .map(|item| (item.entry.name, item.entry.kind, item.attr.unwrap().ino))
        .collect();
    assert_eq!(root_children.len(), 2, "children: {root_children:?}");

    let a_attr = vfs.lookup(r_attr.ino, b"a.txt").unwrap();
    assert_eq!(a_attr.kind, FileType::RegularFile);
    assert_eq!(a_attr.size, 9);
    assert_eq!(
        &rt.block_on(vfs.read(a_attr.ino, 0, 9)).unwrap()[..],
        b"alpha-123"
    );

    let nested_attr = vfs.lookup(r_attr.ino, b"nested").unwrap();
    assert_eq!(nested_attr.kind, FileType::Directory);
    let nested_children = vfs.readdir(nested_attr.ino).unwrap();
    assert_eq!(nested_children.len(), 1);
    assert_eq!(nested_children[0].name, b"b.txt");

    let b_attr = vfs.lookup(nested_attr.ino, b"b.txt").unwrap();
    assert_eq!(b_attr.kind, FileType::RegularFile);
    assert_eq!(b_attr.size, 9);
    assert_eq!(
        &rt.block_on(vfs.read(b_attr.ino, 0, 9)).unwrap()[..],
        b"beta-4567"
    );

    // Reading directory view itself must fail with IsDirectory, never return zeros.
    assert!(rt.block_on(vfs.read(r_attr.ino, 0, 16)).is_err());
    assert!(rt.block_on(vfs.read(nested_attr.ino, 0, 16)).is_err());
}

#[test]
fn directory_trash_view_collision_preserves_data_and_hardlinks_and_bottom_up_clear_purges() {
    use studiofs_meta::trash::{RetainedEntryState, RetainedPurgeReason};

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-native-dir-col-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-native-dir-col");
    let (trash, receipt, h1_orig_ino) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = vfs.mkdir(ROOT_INO, b"dir", 0, 0, 0o755).unwrap();
        let nested = vfs.mkdir(dir.ino, b"nested", 0, 0, 0o755).unwrap();
        let a = vfs.create(dir.ino, b"a.txt", 0, 0, 0o644).unwrap();
        vfs.write(a.ino, 0, b"alpha-123").await.unwrap();
        let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
        vfs.write(b.ino, 0, b"beta-4567").await.unwrap();
        let h1 = vfs.create(dir.ino, b"h1.txt", 0, 0, 0o644).unwrap();
        vfs.write(h1.ino, 0, b"hardlink-data").await.unwrap();
        vfs.fsync(h1.ino, false).await.unwrap();
        vfs.link(h1.ino, dir.ino, b"h2.txt").unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(ROOT_INO, b"dir", trash, b"$RSHELL02", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let receipt = meta.readdir(store).unwrap()[0].ino;
        (trash, receipt, h1.ino)
    });

    let view = trash_view_name(receipt);
    let r_attr = vfs.lookup(trash, view.as_bytes()).unwrap();
    let nested_attr = vfs.lookup(r_attr.ino, b"nested").unwrap();
    assert_eq!(nested_attr.kind, super::super::FileType::Directory);

    // Mutating operations inside the retained directory view must be refused.
    assert!(vfs.create(r_attr.ino, b"evil.txt", 0, 0, 0o644).is_err());
    assert!(vfs.mkdir(r_attr.ino, b"evil-dir", 0, 0, 0o755).is_err());
    assert!(
        vfs.symlink(r_attr.ino, b"evil-sym", b"target", 0, 0)
            .is_err()
    );
    assert!(
        vfs.rename(r_attr.ino, b"a.txt", ROOT_INO, b"stolen.txt", false)
            .is_err()
    );

    // Create a colliding destination at the original path; restore must never overwrite.
    let collider = vfs.mkdir(ROOT_INO, b"dir", 0, 0, 0o755).unwrap();
    let sentinel = vfs.create(collider.ino, b"keep.txt", 0, 0, 0o644).unwrap();
    rt.block_on(vfs.write(sentinel.ino, 0, b"KEEP-COLLIDING"))
        .unwrap();

    for replace in [false, true] {
        let err = vfs
            .rename(trash, view.as_bytes(), ROOT_INO, b"dir", replace)
            .unwrap_err();
        assert!(
            matches!(
                err,
                super::super::VfsError::Meta(super::super::MetaError::EntryExists { .. })
            ),
            "collision with replace={replace} must return EntryExists, got {err:?}"
        );
    }
    assert_eq!(
        meta.retained_entry(receipt).unwrap().unwrap().state,
        RetainedEntryState::Retained
    );
    assert_eq!(
        &rt.block_on(vfs.read(sentinel.ino, 0, 14)).unwrap()[..],
        b"KEEP-COLLIDING"
    );

    // Explicit alternate destination succeeds and preserves bytes + internal hardlinks.
    vfs.rename(trash, view.as_bytes(), ROOT_INO, b"dir-alt", false)
        .unwrap();
    assert_eq!(
        meta.retained_entry(receipt).unwrap().unwrap().state,
        RetainedEntryState::Restored {
            parent: ROOT_INO,
            name: b"dir-alt".to_vec(),
        }
    );
    let restored_dir = vfs.lookup(ROOT_INO, b"dir-alt").unwrap();
    let h1_restored = vfs.lookup(restored_dir.ino, b"h1.txt").unwrap();
    let h2_restored = vfs.lookup(restored_dir.ino, b"h2.txt").unwrap();
    assert_eq!(h1_restored.ino, h1_orig_ino);
    assert_eq!(h2_restored.ino, h1_orig_ino);
    assert_eq!(h1_restored.nlink, 2);
    let restored_nested = vfs.lookup(restored_dir.ino, b"nested").unwrap();
    let restored_b = vfs.lookup(restored_nested.ino, b"b.txt").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(restored_b.ino, 0, 9)).unwrap()[..],
        b"beta-4567"
    );

    // Recycle again and verify bottom-up Clear (`unlink`/`rmdir` inside `$R`, then `rmdir($R)`).
    vfs.rename(ROOT_INO, b"dir-alt", trash, b"$RSHELL03", false)
        .unwrap();
    let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
    let receipt2 = meta
        .readdir(store)
        .unwrap()
        .into_iter()
        .map(|e| e.ino)
        .max()
        .unwrap();
    let view2 = trash_view_name(receipt2);
    let r2 = vfs.lookup(trash, view2.as_bytes()).unwrap();
    let nested2 = vfs.lookup(r2.ino, b"nested").unwrap();

    // Unlinking one hardlink view must not hide the sibling hardlink name.
    vfs.unlink(r2.ino, b"h1.txt").unwrap();
    assert!(vfs.lookup(r2.ino, b"h1.txt").is_err());
    assert!(vfs.lookup(r2.ino, b"h2.txt").is_ok());

    // Non-empty subtree directory refuses rmdir until its child is unlinked.
    assert!(vfs.rmdir(r2.ino, b"nested").is_err());
    assert!(vfs.lookup(nested2.ino, b"b.txt").is_ok());

    vfs.unlink(nested2.ino, b"b.txt").unwrap();
    assert!(vfs.readdir(nested2.ino).unwrap().is_empty());
    vfs.rmdir(r2.ino, b"nested").unwrap();
    vfs.unlink(r2.ino, b"a.txt").unwrap();
    vfs.unlink(r2.ino, b"h2.txt").unwrap();
    assert!(vfs.readdir(r2.ino).unwrap().is_empty());
    vfs.rmdir(trash, view2.as_bytes()).unwrap();

    assert_eq!(
        meta.retained_entry(receipt2).unwrap().unwrap().state,
        RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        }
    );
    assert!(vfs.lookup(trash, view2.as_bytes()).is_err());
}

#[test]
fn linux_profile_projects_trash_root_files_and_info_containers_with_native_mode_and_uid() {
    use super::super::NativeTrashProfile;
    use super::super::{FileType, MetaError, VfsError};

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-containers-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-containers");
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    // Seed non-native root trash folders to confirm Linux profile hides them.
    meta.create_dir(ROOT_INO, b"$RECYCLE.BIN", 0, 0, 0o755)
        .unwrap();
    meta.create_dir(ROOT_INO, b".Trashes", 0, 0, 0o755).unwrap();

    let root_names: Vec<Vec<u8>> = vfs
        .readdir(ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(root_names.iter().any(|name| name == b".Trash-1000"));
    assert!(root_names.iter().all(|name| name != b"$RECYCLE.BIN"));
    assert!(root_names.iter().all(|name| name != b".Trashes"));

    let trash_root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    assert_eq!(trash_root.kind, FileType::Directory);
    assert_eq!(trash_root.mode & 0o777, 0o700);
    assert_eq!(trash_root.uid, 1000);

    let mut container_names: Vec<Vec<u8>> = vfs
        .readdir(trash_root.ino)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    container_names.sort();
    assert_eq!(
        container_names,
        vec![b"expunged".to_vec(), b"files".to_vec(), b"info".to_vec()]
    );

    let files_dir = vfs.lookup(trash_root.ino, b"files").unwrap();
    let info_dir = vfs.lookup(trash_root.ino, b"info").unwrap();
    assert_eq!(files_dir.kind, FileType::Directory);
    assert_eq!(files_dir.mode & 0o777, 0o700);
    assert_eq!(files_dir.uid, 1000);
    assert_eq!(info_dir.kind, FileType::Directory);
    assert_eq!(info_dir.mode & 0o777, 0o700);
    assert_eq!(info_dir.uid, 1000);

    for existing in [b"files".as_slice(), b"info", b"expunged"] {
        assert!(matches!(
            vfs.mkdir(trash_root.ino, existing, 1000, 1000, 0o700),
            Err(VfsError::Meta(MetaError::EntryExists { .. }))
        ));
    }
    assert!(vfs.should_bypass_bridge_entry_cache(trash_root.ino));
    assert!(vfs.should_bypass_bridge_entry_cache(files_dir.ino));
    assert!(vfs.should_bypass_bridge_entry_cache(info_dir.ino));
}

#[test]
fn linux_trash_names_leave_room_for_sidecar_and_collision_suffix() {
    use super::super::NativeTrashProfile;
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-long-name-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-long-name");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
    let name = format!("{}a", "ф".repeat(127));
    for (parent_name, target) in [(b"one", b"$RONE"), (b"two", b"$RTWO")] {
        let parent = vfs.mkdir(ROOT_INO, parent_name, 0, 0, 0o755).unwrap();
        vfs.create(parent.ino, name.as_bytes(), 0, 0, 0o644)
            .unwrap();
        vfs.rename(parent.ino, name.as_bytes(), trash, target, true)
            .unwrap();
    }
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    let root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files = vfs.lookup(root.ino, b"files").unwrap();
    let info = vfs.lookup(root.ino, b"info").unwrap();
    let names = vfs.readdir(files.ino).unwrap();
    assert_eq!(names.len(), 2);
    assert_ne!(names[0].name, names[1].name);
    for entry in names {
        assert!(entry.name.len() <= 245);
        assert!(std::str::from_utf8(&entry.name).is_ok());
        let mut sidecar = entry.name;
        sidecar.extend_from_slice(b".trashinfo");
        assert!(vfs.lookup(info.ino, &sidecar).is_ok());
    }
}

#[test]
fn linux_retained_directories_allow_owner_traversal_and_clear_but_reject_new_children() {
    use super::super::{NativeTrashProfile, VfsError};
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-clear-mode-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-clear-mode");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    let dir = vfs.mkdir(ROOT_INO, b"album", 1000, 1000, 0o755).unwrap();
    vfs.mkdir(dir.ino, b"raw", 1000, 1000, 0o755).unwrap();
    let root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files = vfs.lookup(root.ino, b"files").unwrap();
    vfs.rename(ROOT_INO, b"album", files.ino, b"album", true)
        .unwrap();
    let album = vfs.lookup(files.ino, b"album").unwrap();
    let raw = vfs.lookup(album.ino, b"raw").unwrap();
    for attr in [album.clone(), raw] {
        assert_eq!(attr.uid, 1000);
        assert_eq!(attr.mode & 0o777, 0o700);
    }
    assert!(matches!(
        vfs.create(album.ino, b"new", 1000, 1000, 0o600),
        Err(VfsError::OperationNotSupported)
    ));
}

#[test]
fn linux_gio_atomic_trashinfo_draft_stays_local_and_cannot_replace_generated_record() {
    use super::super::{MetaError, NativeTrashProfile, VfsError};
    use studiofs_meta::MetaReplication;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-gio-draft-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-gio-draft");
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files = vfs.lookup(root.ino, b"files").unwrap();
    let info = vfs.lookup(root.ino, b"info").unwrap();
    let version = meta.metadata_version_token();
    let temporary = b"note.txt.trashinfo.ABC123";
    let draft = vfs.create(info.ino, temporary, 1000, 1000, 0o600).unwrap();
    let fh = vfs
        .open_file(draft.ino, super::super::OpenFlags::WRONLY, false)
        .unwrap();
    rt.block_on(async {
        vfs.write_fh(fh, draft.ino, 0, b"[Trash Info]\nPath=note.txt\n")
            .await
            .unwrap();
        vfs.fsync_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
    assert!(
        vfs.lookup(info.ino, temporary).is_ok(),
        "closed GIO draft must survive until rename"
    );
    vfs.rename(info.ino, temporary, info.ino, b"note.txt.trashinfo", true)
        .unwrap();
    assert!(vfs.lookup(info.ino, temporary).is_err());
    assert!(vfs.lookup(info.ino, b"note.txt.trashinfo").is_ok());
    assert_eq!(meta.metadata_version_token(), version);

    vfs.create(ROOT_INO, b"note.txt", 1000, 1000, 0o644)
        .unwrap();
    vfs.rename(ROOT_INO, b"note.txt", files.ino, b"note.txt", true)
        .unwrap();
    let generated = vfs.lookup(info.ino, b"note.txt.trashinfo").unwrap();
    assert!(vfs.is_generated_trash_record_ino(generated.ino));
    vfs.create(info.ino, temporary, 1000, 1000, 0o600).unwrap();
    assert!(matches!(
        vfs.rename(info.ino, temporary, info.ino, b"note.txt.trashinfo", true),
        Err(VfsError::Meta(MetaError::EntryExists { .. }))
    ));
    assert!(vfs.lookup(info.ino, temporary).is_ok());
    assert_eq!(
        vfs.lookup(info.ino, b"note.txt.trashinfo").unwrap().ino,
        generated.ino
    );
}

#[test]
fn linux_profile_draft_trashinfo_collision_and_disambiguation_flow() {
    use super::super::NativeTrashProfile;
    use super::super::{MetaError, VfsError};

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-collision-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-collision");
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();

    let dir1 = vfs.mkdir(ROOT_INO, b"dir one", 1000, 1000, 0o755).unwrap();
    let dir2 = vfs.mkdir(ROOT_INO, b"dir2", 1000, 1000, 0o755).unwrap();
    let dir3 = vfs.mkdir(ROOT_INO, b"dir3", 1000, 1000, 0o755).unwrap();

    let f1 = vfs
        .create(dir1.ino, b"note #1.txt", 1000, 1000, 0o644)
        .unwrap();
    let f2 = vfs
        .create(dir2.ino, b"note #1.txt", 1000, 1000, 0o644)
        .unwrap();
    let f3 = vfs
        .create(dir3.ino, b"note #1.txt", 1000, 1000, 0o644)
        .unwrap();
    rt.block_on(async {
        vfs.write(f1.ino, 0, b"first").await.unwrap();
        vfs.write(f2.ino, 0, b"second").await.unwrap();
        vfs.write(f3.ino, 0, b"third").await.unwrap();
    });

    let trash_root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files_dir = vfs.lookup(trash_root.ino, b"files").unwrap();
    let info_dir = vfs.lookup(trash_root.ino, b"info").unwrap();

    // First gio-style delete: draft .trashinfo + rename into files/
    let draft1 = vfs
        .create(info_dir.ino, b"note #1.txt.trashinfo", 1000, 1000, 0o600)
        .unwrap();
    rt.block_on(vfs.write(draft1.ino, 0, b"[Trash Info]\nPath=client\n"))
        .unwrap();
    assert!(meta.lookup(trash_root.ino, b"info").is_err());
    vfs.rename(
        dir1.ino,
        b"note #1.txt",
        files_dir.ino,
        b"note #1.txt",
        true,
    )
    .unwrap();

    let sidecar1 = vfs.lookup(info_dir.ino, b"note #1.txt.trashinfo").unwrap();
    assert!(vfs.is_generated_trash_record_ino(sidecar1.ino));
    let sidecar1_bytes = rt
        .block_on(vfs.read(sidecar1.ino, 0, sidecar1.size as u32))
        .unwrap();
    let sidecar1_text = String::from_utf8(sidecar1_bytes.to_vec()).unwrap();
    assert!(
        sidecar1_text.starts_with("[Trash Info]\nPath=dir%20one/note%20%231.txt\nDeletionDate="),
        "unexpected .trashinfo text: {sidecar1_text:?}"
    );
    assert!(sidecar1_text.ends_with('\n'));
    let receipt = vfs
        .trash_view_receipt_for(files_dir.ino, b"note #1.txt")
        .unwrap()
        .unwrap();
    let entry = meta.retained_entry(receipt).unwrap().unwrap();
    let deleted_at = chrono::DateTime::from_timestamp(entry.deleted_at_unix_secs as i64, 0)
        .unwrap()
        .with_timezone(&chrono::Local);
    assert!(sidecar1_text.contains(&format!(
        "DeletionDate={}\n",
        deleted_at.format("%Y-%m-%dT%H:%M:%S")
    )));

    // Creating an active retained .trashinfo again must fail with EEXIST (gio O_CREAT|O_EXCL loop).
    assert!(matches!(
        vfs.create(info_dir.ino, b"note #1.txt.trashinfo", 1000, 1000, 0o600),
        Err(VfsError::Meta(MetaError::EntryExists { .. }))
    ));

    // Second and third same-name trashes receive .2 and .3 without overwriting.
    let draft2 = vfs
        .create(info_dir.ino, b"note #1.txt.2.trashinfo", 1000, 1000, 0o600)
        .unwrap();
    let _ = draft2;
    vfs.rename(
        dir2.ino,
        b"note #1.txt",
        files_dir.ino,
        b"note #1.txt.2",
        true,
    )
    .unwrap();
    vfs.rename(
        dir3.ino,
        b"note #1.txt",
        files_dir.ino,
        b"note #1.txt.3",
        true,
    )
    .unwrap();

    let mut listed_files: Vec<String> = vfs
        .readdir(files_dir.ino)
        .unwrap()
        .into_iter()
        .map(|e| String::from_utf8(e.name).unwrap())
        .collect();
    listed_files.sort();
    assert_eq!(
        listed_files,
        vec!["note #1.txt", "note #1.txt.2", "note #1.txt.3"]
    );

    let mut listed_info: Vec<String> = vfs
        .readdir(info_dir.ino)
        .unwrap()
        .into_iter()
        .map(|e| String::from_utf8(e.name).unwrap())
        .collect();
    listed_info.sort();
    assert_eq!(
        listed_info,
        vec![
            "note #1.txt.2.trashinfo",
            "note #1.txt.3.trashinfo",
            "note #1.txt.trashinfo",
        ]
    );

    assert_eq!(
        &rt.block_on(vfs.read(
            vfs.lookup(files_dir.ino, b"note #1.txt").unwrap().ino,
            0,
            16
        ))
        .unwrap()[..],
        b"first"
    );
    assert_eq!(
        &rt.block_on(vfs.read(
            vfs.lookup(files_dir.ino, b"note #1.txt.2").unwrap().ino,
            0,
            16
        ))
        .unwrap()[..],
        b"second"
    );
    assert_eq!(
        &rt.block_on(vfs.read(
            vfs.lookup(files_dir.ino, b"note #1.txt.3").unwrap().ino,
            0,
            16
        ))
        .unwrap()[..],
        b"third"
    );

    // Deleting the first entry keeps `.2` and `.3` stable for the survivors.
    vfs.unlink(info_dir.ino, b"note #1.txt.trashinfo").unwrap();
    assert!(vfs.lookup(files_dir.ino, b"note #1.txt").is_err());
    assert!(vfs.lookup(info_dir.ino, b"note #1.txt.trashinfo").is_err());
    assert!(vfs.lookup(files_dir.ino, b"note #1.txt.2").is_ok());
    assert!(vfs.lookup(info_dir.ino, b"note #1.txt.3.trashinfo").is_ok());
}

#[test]
fn linux_profile_subtree_restore_collision_clear_acl_and_ttl_zero() {
    use super::super::{AccessLevel, MetaError, NativeTrashProfile, VfsError};

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-ops-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-ops");
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();

    let ro_parent = vfs
        .mkdir(ROOT_INO, b"ro_parent", 1000, 1000, 0o755)
        .unwrap();
    let ro_file = vfs
        .create(ro_parent.ino, b"ro.txt", 1000, 1000, 0o644)
        .unwrap();
    let hidden_parent = vfs
        .mkdir(ROOT_INO, b"hidden_parent", 1000, 1000, 0o755)
        .unwrap();
    let hidden_file = vfs
        .create(hidden_parent.ino, b"secret.txt", 1000, 1000, 0o644)
        .unwrap();
    let folder = vfs.mkdir(ROOT_INO, b"folder", 1000, 1000, 0o755).unwrap();
    let leaf = vfs
        .create(folder.ino, b"leaf.bin", 1000, 1000, 0o644)
        .unwrap();
    rt.block_on(async {
        vfs.write(ro_file.ino, 0, b"readonly-data").await.unwrap();
        vfs.write(hidden_file.ino, 0, b"hidden-data").await.unwrap();
        vfs.write(leaf.ino, 0, b"subtree-payload").await.unwrap();
    });

    let files_dir = vfs
        .lookup(vfs.lookup(ROOT_INO, b".Trash-1000").unwrap().ino, b"files")
        .unwrap();
    let info_dir = vfs
        .lookup(vfs.lookup(ROOT_INO, b".Trash-1000").unwrap().ino, b"info")
        .unwrap();

    vfs.rename(ro_parent.ino, b"ro.txt", files_dir.ino, b"ro.txt", true)
        .unwrap();
    vfs.rename(
        hidden_parent.ino,
        b"secret.txt",
        files_dir.ino,
        b"secret.txt",
        true,
    )
    .unwrap();
    vfs.rename(ROOT_INO, b"folder", files_dir.ino, b"folder", true)
        .unwrap();

    // Current original_parent ACL filtering: ReadOnly allows read/list, blocks purge/restore; NoAccess hides.
    vfs.set_acl_rule(ro_parent.ino, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();
    vfs.set_acl_rule(hidden_parent.ino, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();

    assert!(vfs.lookup(files_dir.ino, b"secret.txt").is_err());
    assert!(vfs.lookup(info_dir.ino, b"secret.txt.trashinfo").is_err());
    assert!(vfs.lookup(files_dir.ino, b"ro.txt").is_ok());
    assert!(matches!(
        vfs.unlink(files_dir.ino, b"ro.txt"),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.unlink(info_dir.ino, b"ro.txt.trashinfo"),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.rename(files_dir.ino, b"ro.txt", ROOT_INO, b"stolen-ro.txt", true),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));

    // Directory subtree read & collision refusal on restore even when replace=true.
    let folder_view = vfs.lookup(files_dir.ino, b"folder").unwrap();
    let leaf_view = vfs.lookup(folder_view.ino, b"leaf.bin").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(leaf_view.ino, 0, 32)).unwrap()[..],
        b"subtree-payload"
    );
    let occupied = vfs.mkdir(ROOT_INO, b"folder", 1000, 1000, 0o755).unwrap();
    let _sentinel = vfs
        .create(occupied.ino, b"keep", 1000, 1000, 0o644)
        .unwrap();
    assert!(matches!(
        vfs.rename(files_dir.ino, b"folder", ROOT_INO, b"folder", true),
        Err(VfsError::Meta(MetaError::EntryExists { .. }))
    ));

    // Bottom-up Clear on retained directory removes both files/folder and info/folder.trashinfo.
    vfs.unlink(folder_view.ino, b"leaf.bin").unwrap();
    vfs.rmdir(files_dir.ino, b"folder").unwrap();
    assert!(vfs.lookup(files_dir.ino, b"folder").is_err());
    assert!(vfs.lookup(info_dir.ino, b"folder.trashinfo").is_err());

    // TTL = 0 disables new retention and removes source directly while cleaning draft .trashinfo.
    meta.compare_and_set_trash_days(1, 0).unwrap().unwrap();
    let direct = vfs
        .create(ROOT_INO, b"ttl0.txt", 1000, 1000, 0o644)
        .unwrap();
    vfs.create(info_dir.ino, b"ttl0.txt.trashinfo", 1000, 1000, 0o600)
        .unwrap();
    vfs.rename(ROOT_INO, b"ttl0.txt", files_dir.ino, b"ttl0.txt", true)
        .unwrap();
    assert!(vfs.lookup(ROOT_INO, b"ttl0.txt").is_err());
    assert!(vfs.lookup(files_dir.ino, b"ttl0.txt").is_err());
    assert!(vfs.lookup(info_dir.ino, b"ttl0.txt.trashinfo").is_err());
    assert!(!vfs.inode_is_in_private_store(direct.ino).unwrap());
}

#[test]
fn macos_profile_projects_trashes_uid_and_handles_collisions_restore_clear_ttl0() {
    use super::super::{FileType, MetaError, NativeTrashProfile, VfsError};

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-macos-profile-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-macos-profile");
    vfs.set_native_trash_profile(NativeTrashProfile::MacOs { uid: 501 });
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    meta.create_dir(ROOT_INO, b"$RECYCLE.BIN", 0, 0, 0o755)
        .unwrap();
    meta.create_dir(ROOT_INO, b".Trash-1000", 1000, 1000, 0o700)
        .unwrap();

    let root_names: Vec<Vec<u8>> = vfs
        .readdir(ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(root_names.iter().any(|n| n == b".Trashes"));
    assert!(root_names.iter().all(|n| n != b"$RECYCLE.BIN"));
    assert!(root_names.iter().all(|n| n != b".Trash-1000"));

    let trashes = vfs.lookup(ROOT_INO, b".Trashes").unwrap();
    assert_eq!(trashes.kind, FileType::Directory);
    let mut trashes_children: Vec<Vec<u8>> = vfs
        .readdir(trashes.ino)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    trashes_children.sort();
    assert_eq!(trashes_children, vec![b"501".to_vec()]);
    assert!(matches!(
        vfs.mkdir(trashes.ino, b"501", 501, 20, 0o700),
        Err(VfsError::Meta(MetaError::EntryExists { .. }))
    ));
    let user_trash = vfs.lookup(trashes.ino, b"501").unwrap();
    assert_eq!(user_trash.mode & 0o777, 0o700);
    assert_eq!(user_trash.uid, 501);

    // Finder .DS_Store inside .Trashes/501 stays local ignored.
    let ds = vfs
        .create(user_trash.ino, b".DS_Store", 501, 20, 0o644)
        .unwrap();
    assert!(vfs.is_synthesized_ignored_ino(ds.ino));

    let f1 = vfs.create(ROOT_INO, b"clip.mov", 501, 20, 0o644).unwrap();
    let sub = vfs.mkdir(ROOT_INO, b"take2", 501, 20, 0o755).unwrap();
    let f2 = vfs.create(sub.ino, b"clip.mov", 501, 20, 0o644).unwrap();
    rt.block_on(async {
        vfs.write(f1.ino, 0, b"clip-one").await.unwrap();
        vfs.write(f2.ino, 0, b"clip-two").await.unwrap();
    });

    vfs.rename(ROOT_INO, b"clip.mov", user_trash.ino, b"clip.mov", true)
        .unwrap();
    vfs.rename(sub.ino, b"clip.mov", user_trash.ino, b"clip.mov", true)
        .unwrap();

    let v1 = vfs.lookup(user_trash.ino, b"clip.mov").unwrap();
    let v2 = vfs.lookup(user_trash.ino, b"clip.mov.2").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(v1.ino, 0, 16)).unwrap()[..],
        b"clip-one"
    );
    assert_eq!(
        &rt.block_on(vfs.read(v2.ino, 0, 16)).unwrap()[..],
        b"clip-two"
    );

    // Restore second entry, Clear first entry, then verify TTL=0 direct delete.
    vfs.rename(user_trash.ino, b"clip.mov.2", sub.ino, b"clip.mov", true)
        .unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(vfs.lookup(sub.ino, b"clip.mov").unwrap().ino, 0, 16))
            .unwrap()[..],
        b"clip-two"
    );
    vfs.unlink(user_trash.ino, b"clip.mov").unwrap();
    assert!(vfs.lookup(user_trash.ino, b"clip.mov").is_err());

    meta.compare_and_set_trash_days(1, 0).unwrap().unwrap();
    vfs.rename(sub.ino, b"clip.mov", user_trash.ino, b"clip.mov", true)
        .unwrap();
    assert!(vfs.lookup(sub.ino, b"clip.mov").is_err());
    assert!(vfs.lookup(user_trash.ino, b"clip.mov").is_err());
}

#[test]
fn cross_os_recycle_on_windows_is_listed_restored_and_cleared_on_linux_and_macos() {
    use super::super::NativeTrashProfile;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-cross-os-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-cross-os");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    vfs.set_native_trash_profile(NativeTrashProfile::Windows);

    let docs = vfs.mkdir(ROOT_INO, b"docs", 0, 0, 0o755).unwrap();
    let report = vfs.create(docs.ino, b"report.txt", 0, 0, 0o644).unwrap();
    rt.block_on(async {
        vfs.write(report.ino, 0, b"cross-os-body").await.unwrap();
        vfs.fsync(report.ino, false).await.unwrap();
    });
    let trash_win = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
    vfs.rename(docs.ino, b"report.txt", trash_win, b"$RWIN001", false)
        .unwrap();

    // Linux view over the exact same shared metadata + read-only mount check:
    let data_config = studiofs_data::store::DataStoreConfig::from(
        &studiofs_meta::MetaFormat::get_format_info(&*meta).unwrap(),
    );
    let ro_cache = tmp.path().join("linux-ro-cache");
    let linux_ro = {
        let _guard = rt.enter();
        StudioVfs::new_read_only_with_snapshot_mount_fence(
            meta.clone(),
            &data_config,
            ro_cache.to_str().unwrap(),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            super::super::VfsConfig::default().try_into().unwrap(),
            Arc::new(|_| Ok(())),
        )
        .unwrap()
    };
    linux_ro.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    let version_before = linux_ro.meta().metadata_version_token();
    let linux_root = linux_ro.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let linux_files = linux_ro.lookup(linux_root.ino, b"files").unwrap();
    let linux_info = linux_ro.lookup(linux_root.ino, b"info").unwrap();
    let ro_view = linux_ro.lookup(linux_files.ino, b"report.txt").unwrap();
    assert_eq!(ro_view.size, b"cross-os-body".len() as u64);
    let ro_info_view = linux_ro
        .lookup(linux_info.ino, b"report.txt.trashinfo")
        .unwrap();
    let info_text = String::from_utf8(
        rt.block_on(linux_ro.read(ro_info_view.ino, 0, ro_info_view.size as u32))
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(info_text.starts_with("[Trash Info]\nPath=docs/report.txt\nDeletionDate="));
    assert_eq!(linux_ro.meta().metadata_version_token(), version_before);
    drop(linux_ro);

    // Switch same VFS instance to Linux profile and verify payload read, then macOS restore.
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    let rw_files = vfs
        .lookup(vfs.lookup(ROOT_INO, b".Trash-1000").unwrap().ino, b"files")
        .unwrap();
    let rw_view = vfs.lookup(rw_files.ino, b"report.txt").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(rw_view.ino, 0, 32)).unwrap()[..],
        b"cross-os-body"
    );

    vfs.set_native_trash_profile(NativeTrashProfile::MacOs { uid: 501 });
    let mac_user = vfs
        .lookup(vfs.lookup(ROOT_INO, b".Trashes").unwrap().ino, b"501")
        .unwrap();
    vfs.rename(mac_user.ino, b"report.txt", docs.ino, b"report.txt", true)
        .unwrap();
    let restored = vfs.lookup(docs.ino, b"report.txt").unwrap();
    assert_eq!(restored.ino, report.ino);
    assert_eq!(
        &rt.block_on(vfs.read(restored.ino, 0, 32)).unwrap()[..],
        b"cross-os-body"
    );
}

#[test]
fn trash_record_and_container_inodes_report_bypass_cache_flags() {
    use super::super::NativeTrashProfile;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-cache-bypass-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-cache-bypass");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });

    let regular = vfs
        .create(ROOT_INO, b"live.txt", 1000, 1000, 0o644)
        .unwrap();
    let dir = vfs.mkdir(ROOT_INO, b"dir", 1000, 1000, 0o755).unwrap();
    let f = vfs.create(dir.ino, b"item.txt", 1000, 1000, 0o644).unwrap();
    rt.block_on(vfs.write(f.ino, 0, b"payload")).unwrap();

    assert!(!vfs.should_bypass_bridge_entry_cache(regular.ino));
    assert!(!vfs.is_generated_trash_record_ino(regular.ino));

    let trash_root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files_dir = vfs.lookup(trash_root.ino, b"files").unwrap();
    let info_dir = vfs.lookup(trash_root.ino, b"info").unwrap();
    vfs.rename(dir.ino, b"item.txt", files_dir.ino, b"item.txt", true)
        .unwrap();

    let trashinfo = vfs.lookup(info_dir.ino, b"item.txt.trashinfo").unwrap();
    let retained = vfs.lookup(files_dir.ino, b"item.txt").unwrap();
    assert!(vfs.should_bypass_bridge_entry_cache(trash_root.ino));
    assert!(vfs.should_bypass_bridge_entry_cache(files_dir.ino));
    assert!(vfs.should_bypass_bridge_entry_cache(info_dir.ino));
    assert!(vfs.should_bypass_bridge_entry_cache(trashinfo.ino));
    assert!(vfs.should_bypass_bridge_entry_cache(retained.ino));
    assert!(vfs.is_generated_trash_record_ino(trashinfo.ino));
    assert!(!vfs.is_generated_trash_record_ino(retained.ino));
}

/// GVFS `trash:` delete/Empty Trash renames `files/<entry>` into
/// `.Trash-<uid>/expunged/<n>` (`gvfs` `trash_item_delete`), then best-effort
/// unlinks the sidecar and expunges `expunged/`. The rename must Clear the receipt.
#[test]
fn linux_gvfs_expunge_rename_clears_file_and_directory_receipts() {
    use super::super::namespace::DentryCacheValue;
    use super::super::{FileType, MetaError, NativeTrashProfile, VfsError};
    use studiofs_meta::trash::{RetainedEntryState, RetainedPurgeReason};
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-gvfs-expunge-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-gvfs-expunge");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    rt.block_on(async {
        let note = vfs.create(ROOT_INO, b"note", 1000, 1000, 0o644).unwrap();
        vfs.write(note.ino, 0, b"bytes").await.unwrap();
        vfs.fsync(note.ino, false).await.unwrap();
    });
    let album = vfs.mkdir(ROOT_INO, b"album", 1000, 1000, 0o755).unwrap();
    vfs.create(album.ino, b"child", 1000, 1000, 0o644).unwrap();
    vfs.create(ROOT_INO, b"live", 1000, 1000, 0o644).unwrap();
    let root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files = vfs.lookup(root.ino, b"files").unwrap();
    let info = vfs.lookup(root.ino, b"info").unwrap();
    for name in [b"note".as_slice(), b"album"] {
        vfs.rename(ROOT_INO, name, files.ino, name, true).unwrap();
    }

    // `g_file_make_directory_with_parents` sees an existing directory.
    assert!(matches!(
        vfs.mkdir(root.ino, b"expunged", 1000, 1000, 0o700),
        Err(VfsError::Meta(MetaError::EntryExists { .. }))
    ));
    let expunged = vfs.lookup(root.ino, b"expunged").unwrap();
    assert_eq!(expunged.kind, FileType::Directory);
    assert_eq!((expunged.uid, expunged.mode & 0o777), (1000, 0o700));

    for (index, name) in [b"note".as_slice(), b"album"].into_iter().enumerate() {
        let target = format!("{}", 4_000_000_000_u32 + index as u32);
        let receipt = vfs
            .trash_view_receipt_for(files.ino, name)
            .unwrap()
            .unwrap();
        vfs.rename(files.ino, name, expunged.ino, target.as_bytes(), true)
            .unwrap();
        assert_eq!(
            meta.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::User,
            }
        );
        // No stale positive dentry may survive for the expunge target name.
        assert!(matches!(
            vfs.dentry_cache_lookup(expunged.ino, target.as_bytes()),
            Some(DentryCacheValue::Negative)
        ));
        assert!(vfs.lookup(files.ino, name).is_err(), "receipt view cleared");
        let mut sidecar = name.to_vec();
        sidecar.extend_from_slice(b".trashinfo");
        assert!(vfs.lookup(info.ino, &sidecar).is_err(), "sidecar cleared");
        assert!(vfs.lookup(expunged.ino, target.as_bytes()).is_err());
    }
    assert!(vfs.readdir(files.ino).unwrap().is_empty());
    assert!(vfs.readdir(expunged.ino).unwrap().is_empty());

    // `expunged` is never a destructive sink for live data or new entries.
    assert!(matches!(
        vfs.rename(ROOT_INO, b"live", expunged.ino, b"1", true),
        Err(VfsError::OperationNotSupported)
    ));
    assert!(vfs.lookup(ROOT_INO, b"live").is_ok());
    assert!(matches!(
        vfs.create(expunged.ino, b"new", 1000, 1000, 0o600),
        Err(VfsError::OperationNotSupported)
    ));
    assert!(matches!(
        vfs.mkdir(expunged.ino, b"new", 1000, 1000, 0o700),
        Err(VfsError::OperationNotSupported)
    ));
}

/// Negative GVFS expunge paths: ACL, sidecar and subtree sources are refused,
/// a retried expunge is not-found, and `expunged` is Linux-only and immovable.
#[test]
fn linux_gvfs_expunge_refusals_keep_receipts_and_retry_is_not_found() {
    use super::super::{AccessLevel, MetaError, NativeTrashProfile, VfsError};
    use studiofs_meta::trash::RetainedEntryState;
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-linux-gvfs-refuse-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-linux-gvfs-refuse");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    vfs.set_native_trash_profile(NativeTrashProfile::Linux { uid: 1000 });
    let ro = vfs.mkdir(ROOT_INO, b"ro", 1000, 1000, 0o755).unwrap();
    vfs.create(ro.ino, b"locked", 1000, 1000, 0o644).unwrap();
    let album = vfs.mkdir(ROOT_INO, b"album", 1000, 1000, 0o755).unwrap();
    vfs.create(album.ino, b"child", 1000, 1000, 0o644).unwrap();
    vfs.create(ROOT_INO, b"note", 1000, 1000, 0o644).unwrap();
    let root = vfs.lookup(ROOT_INO, b".Trash-1000").unwrap();
    let files = vfs.lookup(root.ino, b"files").unwrap();
    let info = vfs.lookup(root.ino, b"info").unwrap();
    let expunged = vfs.lookup(root.ino, b"expunged").unwrap();
    vfs.rename(ro.ino, b"locked", files.ino, b"locked", true)
        .unwrap();
    for name in [b"album".as_slice(), b"note"] {
        vfs.rename(ROOT_INO, name, files.ino, name, true).unwrap();
    }

    // 1. No write access on the original parent: denied, receipt stays Retained.
    let locked = vfs
        .trash_view_receipt_for(files.ino, b"locked")
        .unwrap()
        .unwrap();
    let principal = vfs.acl_principal.clone();
    vfs.set_acl_rule(ro.ino, &principal, AccessLevel::ReadOnly)
        .unwrap();
    assert!(matches!(
        vfs.rename(files.ino, b"locked", expunged.ino, b"1", true),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert_eq!(
        meta.retained_entry(locked).unwrap().unwrap().state,
        RetainedEntryState::Retained
    );
    assert!(vfs.lookup(files.ino, b"locked").is_ok());

    // 2. A sidecar record is not a receipt view.
    assert!(matches!(
        vfs.rename(info.ino, b"note.trashinfo", expunged.ino, b"2", true),
        Err(VfsError::OperationNotSupported)
    ));
    assert!(vfs.lookup(info.ino, b"note.trashinfo").is_ok());

    // 3. A subtree child of a directory view is not a whole entry.
    let album_view = vfs.lookup(files.ino, b"album").unwrap();
    assert!(matches!(
        vfs.rename(album_view.ino, b"child", expunged.ino, b"3", true),
        Err(VfsError::OperationNotSupported)
    ));
    assert!(vfs.lookup(album_view.ino, b"child").is_ok());

    // 4. A retried expunge of an already purged entry is not-found, not ENOTSUP.
    vfs.rename(files.ino, b"note", expunged.ino, b"4", true)
        .unwrap();
    assert!(matches!(
        vfs.rename(files.ino, b"note", expunged.ino, b"5", true),
        Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
    ));

    // 5. `expunged` cannot be removed, moved, or used as a rename source.
    assert!(vfs.rmdir(root.ino, b"expunged").is_err());
    assert!(
        vfs.rename(root.ino, b"expunged", ROOT_INO, b"moved", true)
            .is_err()
    );
    assert!(
        vfs.rename(expunged.ino, b"4", ROOT_INO, b"stolen", true)
            .is_err()
    );
    assert!(vfs.lookup(root.ino, b"expunged").is_ok());
    assert!(vfs.lookup(ROOT_INO, b"moved").is_err());
    assert!(vfs.lookup(ROOT_INO, b"stolen").is_err());

    // 6. Windows and macOS profiles project no `expunged`.
    vfs.set_native_trash_profile(NativeTrashProfile::Windows);
    let recycle = vfs.lookup(ROOT_INO, TRASH_ROOT_NAME).unwrap();
    assert!(vfs.lookup(recycle.ino, b"expunged").is_err());
    vfs.set_native_trash_profile(NativeTrashProfile::MacOs { uid: 501 });
    let trashes = vfs.lookup(ROOT_INO, b".Trashes").unwrap();
    let user = vfs.lookup(trashes.ino, b"501").unwrap();
    assert!(vfs.lookup(trashes.ino, b"expunged").is_err());
    assert!(vfs.lookup(user.ino, b"expunged").is_err());
}
