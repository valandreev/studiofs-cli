//! Negative tests for the private retained store and its views.
//!
//! Each public entrypoint is exercised with the known store, receipt and payload
//! inodes, so a missing guard shows up as a successful call.

use super::super::tests::make_unstarted_test_vfs;
use super::super::{AccessLevel, MetaError, ROOT_INO, StudioVfs, VfsError};
use super::{PRIVATE_STORE_NAME, TRASH_ROOT_NAME, trash_view_name};
use std::sync::Arc;
use studiofs_meta::trash::{MetaVolumeSettings, RetainedEntryState};
use studiofs_meta::{Ino, MetaFsOps, MetaInodes, MetaNamespace, RedbMetaEngine};
use tokio::runtime::Runtime;

/// Name of the receipt xattr the store owns; ordinary clients must not reach it.
const RECEIPT_XATTR: &[u8] = b"system.studiofs.retained.v1";

struct Fixture {
    rt: Arc<Runtime>,
    vfs: Arc<StudioVfs>,
    meta: Arc<RedbMetaEngine>,
    store: Ino,
    receipt: Ino,
    data: Ino,
    trash: Ino,
    view: String,
}

/// Retains `source` (a payload-backed file) into the trash via the VFS rename path.
fn retained_fixture(tmp: &tempfile::TempDir, tag: &str) -> Fixture {
    let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, tag);
    let (store, receipt, data, trash) = rt.block_on(async {
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
        let data = meta.lookup(receipt, b"data").unwrap().ino;
        (store, receipt, data, trash)
    });
    let view = trash_view_name(receipt);
    Fixture {
        rt,
        vfs,
        meta,
        store,
        receipt,
        data,
        trash,
        view,
    }
}

#[test]
fn disabled_empty_trash_root_is_unlisted_but_shell_can_reopen_it() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-empty-root-hidden-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-empty-root-hidden");
    meta.create_dir(ROOT_INO, TRASH_ROOT_NAME, 0, 0, 0o755)
        .unwrap();

    let lookup_visible = vfs.lookup(ROOT_INO, b"$recycle.bin").is_ok();
    let readdir_visible = vfs
        .readdir(ROOT_INO)
        .unwrap()
        .iter()
        .any(|entry| entry.name == TRASH_ROOT_NAME);

    assert!(
        lookup_visible && !readdir_visible,
        "Shell needs exact-path lookup without listing an empty disabled root: lookup={lookup_visible}, readdir={readdir_visible}"
    );
}

#[test]
fn disabled_retention_keeps_readable_retained_receipts_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-disabled-visible-");
    let f = retained_fixture(&tmp, "retained-disabled-visible");
    let principal = f.vfs.acl_principal.clone();
    f.vfs
        .set_acl_rule(ROOT_INO, &principal, AccessLevel::ReadOnly)
        .unwrap();
    f.meta.compare_and_set_trash_days(1, 0).unwrap().unwrap();

    assert_eq!(
        f.vfs.lookup(ROOT_INO, b"$recycle.bin").unwrap().ino,
        f.trash
    );
    assert!(
        f.vfs
            .readdir(ROOT_INO)
            .unwrap()
            .iter()
            .any(|entry| entry.name == TRASH_ROOT_NAME)
    );
    let names: Vec<_> = f
        .vfs
        .readdir(f.trash)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(names.iter().any(|name| name == f.view.as_bytes()));
    assert!(
        names
            .iter()
            .any(|name| name == format!("$I{:06X}", f.receipt).as_bytes())
    );
}

#[test]
fn inaccessible_retained_receipt_does_not_keep_disabled_trash_root_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-disabled-acl-hidden-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-disabled-acl-hidden");
    meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
    let (parent, trash) = rt.block_on(async {
        let parent = vfs.mkdir(ROOT_INO, b"private", 0, 0, 0o755).unwrap();
        vfs.create(parent.ino, b"source", 0, 0, 0o644).unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(parent.ino, b"source", trash, b"$R000001", false)
            .unwrap();
        (parent.ino, trash)
    });
    let principal = vfs.acl_principal.clone();
    vfs.set_acl_rule(parent, &principal, AccessLevel::NoAccess)
        .unwrap();
    meta.compare_and_set_trash_days(1, 0).unwrap().unwrap();

    assert_eq!(vfs.lookup(ROOT_INO, TRASH_ROOT_NAME).unwrap().ino, trash);
    assert!(
        vfs.readdir(ROOT_INO)
            .unwrap()
            .iter()
            .all(|entry| entry.name != TRASH_ROOT_NAME)
    );
    assert!(vfs.readdir(trash).unwrap().is_empty());
}

fn is_refused<T>(result: super::super::VfsResult<T>) -> bool {
    result.is_err()
}

#[test]
fn private_store_inodes_refuse_every_public_entrypoint() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-entry-");
    let f = retained_fixture(&tmp, "retained-guard-entry");
    let vfs = &f.vfs;
    let (store, receipt, data) = (f.store, f.receipt, f.data);

    // A real child under the store, reached only through meta, to exercise
    // lookup and readdir of a nested name rather than an empty directory.
    f.meta.create_file(store, b"probe", 0, 0, 0o600).unwrap();
    vfs.create(ROOT_INO, b"outside", 0, 0, 0o644).unwrap();

    let refused = [
        ("getattr store", is_refused(vfs.getattr(store))),
        ("getattr receipt", is_refused(vfs.getattr(receipt))),
        ("getattr data", is_refused(vfs.getattr(data))),
        (
            "lookup store/probe",
            is_refused(vfs.lookup(store, b"probe")),
        ),
        (
            "lookup receipt/data",
            is_refused(vfs.lookup(receipt, b"data")),
        ),
        ("readdir store", is_refused(vfs.readdir(store))),
        ("readdir receipt", is_refused(vfs.readdir(receipt))),
        (
            "readdir_with_attrs store",
            is_refused(vfs.readdir_with_attrs(store)),
        ),
        ("readlink data", is_refused(vfs.readlink(data))),
        (
            "open_file data",
            is_refused(vfs.open_file(data, super::super::file_session::OpenFlags::RDONLY, false)),
        ),
        (
            "create in receipt",
            is_refused(vfs.create(receipt, b"n", 0, 0, 0o644)),
        ),
        (
            "mkdir in store",
            is_refused(vfs.mkdir(store, b"n", 0, 0, 0o755)),
        ),
        (
            "symlink in store",
            is_refused(vfs.symlink(store, b"s", b"t", 0, 0)),
        ),
        (
            "link data outward",
            is_refused(vfs.link(data, ROOT_INO, b"hl")),
        ),
        (
            "unlink receipt/data",
            is_refused(vfs.unlink(receipt, b"data")),
        ),
        ("rmdir in store", is_refused(vfs.rmdir(store, b"probe"))),
        (
            "rename receipt/data outward",
            is_refused(vfs.rename(receipt, b"data", ROOT_INO, b"stolen", false)),
        ),
        (
            "rename outside into store",
            is_refused(vfs.rename(ROOT_INO, b"outside", store, b"x", false)),
        ),
        (
            "set_xattr data",
            is_refused(vfs.set_xattr(data, b"user.x", b"v", 0)),
        ),
        (
            "set_xattr receipt record",
            is_refused(vfs.set_xattr(receipt, RECEIPT_XATTR, b"forged", 0)),
        ),
        (
            "remove_xattr receipt record",
            is_refused(vfs.remove_xattr(receipt, RECEIPT_XATTR)),
        ),
        (
            "flock data",
            is_refused(f.rt.block_on(vfs.flock(
                data,
                super::super::locking::LockKind::Exclusive,
                1,
                false,
            ))),
        ),
    ];
    let accepted: Vec<&str> = refused
        .iter()
        .filter(|(_, refused)| !refused)
        .map(|(label, _)| *label)
        .collect();
    assert!(accepted.is_empty(), "store paths accepted: {accepted:?}");

    // Async entrypoints, on the same known payload inode.
    f.rt.block_on(async {
        assert!(vfs.read(data, 0, 4).await.is_err(), "read accepted");
        assert!(vfs.write(data, 0, b"x").await.is_err(), "write accepted");
        assert!(vfs.fsync(data, false).await.is_err(), "fsync accepted");
    });

    // Control: the same kinds of operation still succeed outside the store.
    assert!(vfs.getattr(ROOT_INO).is_ok());
    assert!(vfs.readdir(ROOT_INO).is_ok());
    assert!(vfs.mkdir(ROOT_INO, b"ordinary", 0, 0, 0o755).is_ok());
    // The receipt record is still intact after the refused forgery.
    assert!(f.meta.retained_entry(receipt).unwrap().is_some());
}

#[test]
fn get_xattr_of_store_inodes_is_absent_for_ordinary_clients() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-xattr-");
    let f = retained_fixture(&tmp, "retained-guard-xattr");
    assert_eq!(f.vfs.get_xattr(f.receipt, RECEIPT_XATTR).unwrap(), None);
    assert_eq!(f.vfs.get_xattr(f.data, RECEIPT_XATTR).unwrap(), None);
}

#[test]
fn revoked_original_parent_write_refuses_purge_and_restore_but_keeps_listing() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-acl-");
    let f = retained_fixture(&tmp, "retained-guard-acl");
    let principal = f.vfs.acl_principal.clone();
    f.vfs
        .set_acl_rule(ROOT_INO, &principal, AccessLevel::ReadOnly)
        .unwrap();

    // Listing needs read access only: the view is still resolvable.
    assert_eq!(
        f.vfs
            .trash_view_receipt_for(f.trash, f.view.as_bytes())
            .unwrap(),
        Some(f.receipt)
    );

    let purge = f.vfs.unlink(f.trash, f.view.as_bytes());
    assert!(
        matches!(purge, Err(VfsError::Meta(MetaError::AccessDenied { .. }))),
        "purge without original-parent write: {purge:?}"
    );
    let restore = f
        .vfs
        .rename(f.trash, f.view.as_bytes(), ROOT_INO, b"back", false);
    assert!(restore.is_err(), "restore into read-only parent accepted");
    assert_eq!(
        f.meta.retained_entry(f.receipt).unwrap().unwrap().state,
        RetainedEntryState::Retained
    );
}

#[test]
fn canonical_hex_sidecar_draft_without_receipt_stays_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-sidecar-draft-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "retained-guard-sidecar-draft");
    let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
    let draft = vfs.create(trash, b"$IABCDEF", 0, 0, 0o600).unwrap();

    assert_eq!(
        vfs.trash_view_receipt_for(trash, b"$IABCDEF").unwrap(),
        None,
        "a canonical sidecar spelling without a retained receipt is a Shell draft"
    );
    assert_eq!(vfs.lookup(trash, b"$IABCDEF").unwrap().ino, draft.ino);
    assert!(vfs.ignored_entry(draft.ino).unwrap().view.is_none());
    assert_eq!(
        vfs.getattr(draft.ino).unwrap().size,
        0,
        "draft metadata remains accessible"
    );
}

#[test]
fn revoked_original_parent_hides_warmed_sidecar_from_lookup_and_reads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-warmed-sidecar-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-warmed-sidecar");
    let (parent, trash, view_name, sidecar_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let parent = vfs.mkdir(ROOT_INO, b"private", 0, 0, 0o755).unwrap().ino;
        let source = vfs.create(parent, b"secret.txt", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"private bytes").await.unwrap();
        vfs.fsync(source.ino, false).await.unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(parent, b"secret.txt", trash, b"$R000001", false)
            .unwrap();
        let entries = vfs.readdir(trash).unwrap();
        let sidecar = entries
            .iter()
            .find(|entry| entry.name.starts_with(b"$I"))
            .expect("retained listing includes sidecar");
        let view = entries
            .iter()
            .find(|entry| entry.name.starts_with(b"$R"))
            .expect("retained listing includes payload view");
        (parent, trash, view.name.clone(), sidecar.name.clone())
    });
    let view_attr = vfs.lookup(trash, &view_name).unwrap();
    let sidecar_attr = vfs.lookup(trash, &sidecar_name).unwrap();
    let sidecar_snapshot = vfs.ignored_entry(sidecar_attr.ino).unwrap();
    let sidecar_handle = vfs
        .open_file(
            sidecar_attr.ino,
            super::super::file_session::OpenFlags::RDONLY,
            false,
        )
        .unwrap();
    let sidecar_bytes = rt
        .block_on(vfs.read(sidecar_attr.ino, 0, sidecar_attr.size as u32))
        .unwrap();
    assert!(!sidecar_bytes.is_empty(), "warm the cached sidecar bytes");

    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();

    let read_before_lookup = rt.block_on(vfs.read(sidecar_attr.ino, 0, sidecar_bytes.len() as u32));
    let mut handle_buffer = vec![0; sidecar_bytes.len()];
    let handle_read_before_lookup =
        rt.block_on(vfs.read_into_fh(sidecar_handle, sidecar_attr.ino, 0, &mut handle_buffer));
    let lookup = vfs.lookup(trash, &sidecar_name);
    let read_after_lookup = rt.block_on(vfs.read(sidecar_attr.ino, 0, sidecar_bytes.len() as u32));
    let handle_read_after_lookup =
        rt.block_on(vfs.read_into_fh(sidecar_handle, sidecar_attr.ino, 0, &mut handle_buffer));
    let view_lookup = vfs.lookup(trash, &view_name);
    let getattr = vfs.getattr(sidecar_attr.ino);
    let view_getattr = vfs.getattr(view_attr.ino);
    let lookup_is_not_found =
        matches!(lookup, Err(VfsError::Meta(MetaError::EntryNotFound { .. })));
    let view_lookup_is_not_found = matches!(
        view_lookup,
        Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
    );
    assert!(
        lookup_is_not_found
            && view_lookup_is_not_found
            && read_before_lookup.is_err()
            && read_after_lookup.is_err()
            && handle_read_before_lookup.is_err()
            && handle_read_after_lookup.is_err()
            && getattr.is_err()
            && view_getattr.is_err(),
        "hidden retained views must refuse exact lookup and cached inode metadata/reads before and after detach: sidecar_lookup={lookup:?}, view_lookup={view_lookup:?}, read_before_refused={}, read_after_refused={}, handle_before_refused={}, handle_after_refused={}, sidecar_getattr_refused={}, view_getattr_refused={}",
        read_before_lookup.is_err(),
        read_after_lookup.is_err(),
        handle_read_before_lookup.is_err(),
        handle_read_after_lookup.is_err(),
        getattr.is_err(),
        view_getattr.is_err(),
    );
    rt.block_on(vfs.release_fh(sidecar_handle)).unwrap();
    assert!(
        vfs.ignored_entry(sidecar_attr.ino).is_none(),
        "close the detached cache entry"
    );
    assert!(
        vfs.ensure_ignored_entry_snapshot_visible(&sidecar_snapshot)
            .is_err(),
        "the bytes' saved snapshot remains denied after its inode cache entry is evicted"
    );
}

#[test]
fn trashed_original_parent_hides_its_retained_entries() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-trashed-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-trashed");
    let (trash, store, dir) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = vfs.mkdir(ROOT_INO, b"dirA", 0, 0, 0o755).unwrap();
        vfs.create(dir.ino, b"f", 0, 0, 0o644).unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        (trash, store, dir.ino)
    });
    let receipt = meta.next_inode().unwrap();
    meta.retain_namespace_entry(dir, b"f", store, receipt)
        .unwrap();
    let view = trash_view_name(receipt);
    assert_eq!(
        vfs.trash_view_receipt_for(trash, view.as_bytes()).unwrap(),
        Some(receipt),
        "child of a live directory is visible"
    );

    // Retaining the directory itself moves it under the store: its child hides.
    vfs.rename(ROOT_INO, b"dirA", trash, b"$R0DIRA", false)
        .unwrap();
    assert_eq!(
        vfs.trash_view_receipt_for(trash, view.as_bytes()).unwrap(),
        None,
        "child of a trashed directory is hidden"
    );
}

#[test]
fn missing_original_parent_hides_entry_and_recreated_path_grants_nothing() {
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-missing-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-missing");
    let (trash, store, dir) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let dir = vfs.mkdir(ROOT_INO, b"dirB", 0, 0, 0o755).unwrap();
        vfs.create(dir.ino, b"g", 0, 0, 0o644).unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        (trash, store, dir.ino)
    });
    let receipt = meta.next_inode().unwrap();
    meta.retain_namespace_entry(dir, b"g", store, receipt)
        .unwrap();
    let view = trash_view_name(receipt);

    // The original directory is now empty: remove it.
    vfs.rmdir(ROOT_INO, b"dirB").unwrap();
    assert_eq!(
        vfs.trash_view_receipt_for(trash, view.as_bytes()).unwrap(),
        None,
        "entry with a missing original parent is hidden"
    );

    // A new directory at the same path has a new identity and grants nothing.
    vfs.mkdir(ROOT_INO, b"dirB", 0, 0, 0o755).unwrap();
    assert_eq!(
        vfs.trash_view_receipt_for(trash, view.as_bytes()).unwrap(),
        None,
        "recreated path does not restore visibility"
    );
    assert_eq!(
        meta.retained_entry(receipt).unwrap().unwrap().state,
        RetainedEntryState::Retained,
        "the receipt itself is untouched"
    );
}

#[test]
fn warmed_generated_trash_after_nonroot_revoke_refuses_cached_attr_and_mutation_bypasses() {
    use super::super::SetattrArgs;
    use super::super::file_session::OpenFlags;
    use super::super::{LocalSyncPolicy, ReadAheadOwner};

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-f5-bypasses-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-f5-bypasses");
    let (parent, trash, view_name, sidecar_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let parent = vfs.mkdir(ROOT_INO, b"private", 0, 0, 0o755).unwrap().ino;
        let source = vfs.create(parent, b"secret.txt", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"private bytes").await.unwrap();
        vfs.fsync(source.ino, false).await.unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(parent, b"secret.txt", trash, b"$R000001", false)
            .unwrap();
        let entries = vfs.readdir(trash).unwrap();
        let sidecar = entries
            .iter()
            .find(|entry| entry.name.starts_with(b"$I"))
            .expect("retained listing includes sidecar");
        let view = entries
            .iter()
            .find(|entry| entry.name.starts_with(b"$R"))
            .expect("retained listing includes payload view");
        (parent, trash, view.name.clone(), sidecar.name.clone())
    });

    let view_attr = vfs.lookup(trash, &view_name).unwrap();
    let sidecar_attr = vfs.lookup(trash, &sidecar_name).unwrap();
    let sidecar_rd_fh = vfs
        .open_file(sidecar_attr.ino, OpenFlags::RDONLY, false)
        .unwrap();
    let sidecar_rw_fh = vfs
        .open_file(sidecar_attr.ino, OpenFlags::RDWR, false)
        .unwrap();
    let view_rd_fh = vfs
        .open_file(view_attr.ino, OpenFlags::RDONLY, false)
        .unwrap();
    let view_rw_fh = vfs
        .open_file(view_attr.ino, OpenFlags::RDWR, false)
        .unwrap();
    assert!(vfs.get_open_file_attr(sidecar_rd_fh).is_some());
    assert!(vfs.get_open_file_attr(view_rd_fh).is_some());

    // Warm effective_acl_cache before revoking access on the non-root original parent.
    assert_eq!(
        vfs.effective_access_level(sidecar_attr.ino).unwrap(),
        AccessLevel::ReadWrite
    );
    assert_eq!(
        vfs.effective_access_level(view_attr.ino).unwrap(),
        AccessLevel::ReadWrite
    );

    // Revoke access on the non-root original parent BEFORE any lookup.
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();

    // ACL enforcement must not follow $RECYCLE.BIN -> ROOT_INO.
    assert!(
        vfs.enforce_access(sidecar_attr.ino, AccessLevel::ReadOnly)
            .is_err(),
        "enforce_access on hidden sidecar must fail"
    );
    assert!(
        vfs.enforce_access(view_attr.ino, AccessLevel::ReadOnly)
            .is_err(),
        "enforce_access on hidden view must fail"
    );

    // setattr (deferred and handle-backed) must not mutate or return cached attrs.
    assert!(
        vfs.setattr_attr_only_deferred_metadata(
            sidecar_attr.ino,
            SetattrArgs {
                mode: Some(0o600),
                ..Default::default()
            },
            rt.handle(),
        )
        .is_err(),
        "setattr_attr_only_deferred_metadata on hidden sidecar must fail"
    );
    assert!(
        vfs.setattr_attr_only_deferred_metadata(
            view_attr.ino,
            SetattrArgs {
                mode: Some(0o600),
                ..Default::default()
            },
            rt.handle(),
        )
        .is_err(),
        "setattr_attr_only_deferred_metadata on hidden view must fail"
    );
    assert!(
        vfs.setattr_with_handle(
            Some(sidecar_rw_fh),
            sidecar_attr.ino,
            SetattrArgs::size_only(99)
        )
        .is_err(),
        "setattr_with_handle on hidden sidecar must fail"
    );
    assert!(
        vfs.setattr_with_handle(None, view_attr.ino, SetattrArgs::size_only(99))
            .is_err(),
        "setattr_with_handle on hidden view must fail"
    );
    assert_eq!(
        vfs.ignored_entry(sidecar_attr.ino).unwrap().attr.size,
        sidecar_attr.size
    );
    assert_eq!(
        vfs.ignored_entry(view_attr.ino).unwrap().attr.size,
        view_attr.size
    );

    // open_file variants and get_open_file_attr must refuse hidden generated entries.
    assert!(
        vfs.open_file(sidecar_attr.ino, OpenFlags::RDONLY, false)
            .is_err(),
        "open_file on hidden sidecar must fail"
    );
    assert!(
        vfs.open_file(view_attr.ino, OpenFlags::RDONLY, false)
            .is_err(),
        "open_file on hidden view must fail"
    );
    assert!(
        vfs.open_file_with_known_attr(&sidecar_attr, OpenFlags::RDONLY, false)
            .is_err(),
        "open_file_with_known_attr on hidden sidecar must fail"
    );
    assert!(
        vfs.open_created_file_with_known_attr_and_read_ahead_owner(
            &sidecar_attr,
            OpenFlags::RDONLY,
            false,
            ReadAheadOwner::Vfs,
        )
        .is_err(),
        "open_created_file_with_known_attr on hidden sidecar must fail"
    );
    assert_eq!(
        vfs.get_open_file_attr(sidecar_rd_fh),
        None,
        "get_open_file_attr must not expose hidden sidecar attrs"
    );
    assert_eq!(
        vfs.get_open_file_attr(view_rd_fh),
        None,
        "get_open_file_attr must not expose hidden view attrs"
    );

    // write and write_fh paths must be denied on hidden generated entries.
    rt.block_on(async {
        assert!(
            vfs.write(sidecar_attr.ino, 0, b"tamper").await.is_err(),
            "write on hidden sidecar must fail"
        );
        assert!(
            vfs.write_with_local_sync_policy(view_attr.ino, 0, b"tamper", LocalSyncPolicy::Strict,)
                .await
                .is_err(),
            "write_with_local_sync_policy on hidden view must fail"
        );
        assert!(
            vfs.write_fh(sidecar_rw_fh, sidecar_attr.ino, 0, b"tamper")
                .await
                .is_err(),
            "write_fh on hidden sidecar must fail"
        );
        assert!(
            vfs.write_fh_with_deferred_materialize(
                view_rw_fh,
                view_attr.ino,
                0,
                b"tamper",
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .is_err(),
            "write_fh_with_deferred_materialize on hidden view must fail"
        );

        let mut buf = vec![0u8; sidecar_attr.size.max(16) as usize];
        assert!(
            vfs.read_into_fh(sidecar_rd_fh, sidecar_attr.ino, 0, &mut buf)
                .await
                .is_err(),
            "held-handle read on hidden sidecar must fail"
        );
        assert!(
            vfs.read_into_fh(view_rd_fh, view_attr.ino, 0, &mut buf)
                .await
                .is_err(),
            "held-handle read on hidden view must fail"
        );

        // xattr mutations and fsync must fail on hidden entries; xattr reads report absent.
        assert!(
            vfs.set_xattr(sidecar_attr.ino, b"user.x", b"v", 0).is_err(),
            "set_xattr on hidden sidecar must fail"
        );
        assert!(
            vfs.remove_xattr(view_attr.ino, b"user.x").is_err(),
            "remove_xattr on hidden view must fail"
        );
        assert!(
            vfs.fsync(sidecar_attr.ino, false).await.is_err(),
            "fsync on hidden sidecar must fail"
        );
        assert!(
            vfs.fsync_fh(sidecar_rd_fh).await.is_err(),
            "fsync_fh on hidden sidecar must fail"
        );
        assert_eq!(vfs.get_xattr(sidecar_attr.ino, b"user.x").unwrap(), None);
        assert_eq!(
            vfs.get_xattr_size(sidecar_attr.ino, b"user.x").unwrap(),
            None
        );
        assert!(vfs.list_xattrs(sidecar_attr.ino).unwrap().is_empty());
    });

    // ACL restoration control: ReadOnly restores read/open/attr visibility while keeping writes denied.
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();
    assert_eq!(
        vfs.lookup(trash, &sidecar_name).unwrap().ino,
        sidecar_attr.ino
    );
    assert_eq!(vfs.lookup(trash, &view_name).unwrap().ino, view_attr.ino);
    assert!(vfs.get_open_file_attr(sidecar_rd_fh).is_some());
    assert!(vfs.get_open_file_attr(view_rd_fh).is_some());
    let mut restored_buf = vec![0u8; 13];
    let n = rt
        .block_on(vfs.read_into_fh(view_rd_fh, view_attr.ino, 0, &mut restored_buf))
        .unwrap();
    assert_eq!(&restored_buf[..n as usize], b"private bytes");
    assert!(
        vfs.setattr_with_handle(
            Some(sidecar_rw_fh),
            sidecar_attr.ino,
            SetattrArgs::size_only(99)
        )
        .is_err(),
        "ReadOnly original parent must still refuse ReadWrite setattr"
    );
    assert!(
        rt.block_on(vfs.write_fh(sidecar_rw_fh, sidecar_attr.ino, 0, b"x"))
            .is_err(),
        "ReadOnly original parent must still refuse ReadWrite write_fh"
    );

    // Revoke again, then verify unlink / rename / no-replace do not expose warmed sidecar.
    vfs.create(ROOT_INO, b"outside", 0, 0, 0o644).unwrap();
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();
    assert!(matches!(
        vfs.rename(ROOT_INO, b"outside", trash, &sidecar_name, false),
        Err(VfsError::OperationNotSupported)
    ));
    assert!(matches!(
        vfs.rename(trash, &sidecar_name, ROOT_INO, b"stolen", true),
        Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
    ));
    assert!(matches!(
        vfs.unlink(trash, &sidecar_name),
        Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
    ));

    // Re-warm sidecar while ReadOnly, revoke again, then verify create / create_deferred
    // do NOT return old generated metadata.
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();
    let rewarmed_sidecar = vfs.lookup(trash, &sidecar_name).unwrap();
    assert_eq!(rewarmed_sidecar.size, sidecar_attr.size);
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();
    let created = vfs.create(trash, &sidecar_name, 0, 0, 0o600).unwrap();
    assert_ne!(
        created.ino, rewarmed_sidecar.ino,
        "create on hidden generated $I name must not return old generated inode"
    );
    assert_eq!(
        created.size, 0,
        "create on hidden generated $I name must not return old generated size"
    );
    assert!(
        vfs.ignored_entry(created.ino).unwrap().view.is_none(),
        "new draft must have view None"
    );
    let created_deferred = vfs
        .create_with_deferred_metadata_admission(trash, &sidecar_name, 0, 0, 0o600, rt.handle())
        .unwrap();
    assert_eq!(created_deferred.ino, created.ino);
    assert_eq!(created_deferred.size, 0);

    // Cleanup open handles.
    rt.block_on(async {
        vfs.release_fh(sidecar_rd_fh).await.unwrap();
        vfs.release_fh(sidecar_rw_fh).await.unwrap();
        vfs.release_fh(view_rd_fh).await.unwrap();
        vfs.release_fh(view_rw_fh).await.unwrap();
    });
    assert_eq!(vfs.lookup(trash, &sidecar_name).unwrap().ino, created.ino);
}

#[test]
fn canonical_hex_sidecar_draft_create_write_read_and_handle_attr_remain_usable() {
    use super::super::SetattrArgs;
    use super::super::file_session::OpenFlags;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-draft-usable-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "retained-guard-draft-usable");
    let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
    let unrelated = vfs.mkdir(ROOT_INO, b"unrelated", 0, 0, 0o755).unwrap();

    // Canonical hex Shell draft without a receipt (view: None).
    let draft = vfs.create(trash, b"$IABCDEF", 0, 0, 0o600).unwrap();
    assert!(vfs.ignored_entry(draft.ino).unwrap().view.is_none());
    let fh = vfs
        .open_created_file_with_known_attr(&draft, OpenFlags::RDWR, false)
        .unwrap();
    assert_eq!(vfs.get_open_file_attr(fh).unwrap().ino, draft.ino);

    rt.block_on(async {
        assert_eq!(
            vfs.write_fh(fh, draft.ino, 0, b"shell-draft-1")
                .await
                .unwrap(),
            13
        );
        assert_eq!(vfs.write(draft.ino, 13, b"-more").await.unwrap(), 5);
        assert_eq!(vfs.getattr(draft.ino).unwrap().size, 18);

        // Bump acl_epoch to force cache miss in enforce_access_async for ignored inodes.
        vfs.set_acl_rule(unrelated.ino, &vfs.acl_principal, AccessLevel::ReadOnly)
            .unwrap();

        let mut buf = [0xAAu8; 32];
        let n = vfs.read_into_fh(fh, draft.ino, 0, &mut buf).await.unwrap();
        assert_eq!(n, 18);
        assert!(buf[..18].iter().all(|&b| b == 0));

        let bytes = vfs.read(draft.ino, 0, 32).await.unwrap();
        assert_eq!(bytes.len(), 18);
        assert!(bytes.iter().all(|&b| b == 0));

        let updated = vfs
            .setattr_with_handle(Some(fh), draft.ino, SetattrArgs::size_only(24))
            .unwrap();
        assert_eq!(updated.size, 24);
        vfs.fsync(draft.ino, false).await.unwrap();
        vfs.fsync_fh(fh).await.unwrap();
        vfs.set_xattr(draft.ino, b"user.draft", b"ok", 0).unwrap();
        assert_eq!(vfs.get_xattr(draft.ino, b"user.draft").unwrap(), None);

        vfs.release_fh(fh).await.unwrap();
    });

    // Plain ignored file control.
    let plain = vfs.create(ROOT_INO, b".fseventsd", 0, 0, 0o644).unwrap();
    let plain_fh = vfs.open_file(plain.ino, OpenFlags::RDWR, false).unwrap();
    rt.block_on(async {
        assert_eq!(
            vfs.write_fh(plain_fh, plain.ino, 0, b"plain")
                .await
                .unwrap(),
            5
        );
        let read = vfs.read(plain.ino, 0, 16).await.unwrap();
        assert_eq!(read.len(), 5);
        assert!(vfs.get_open_file_attr(plain_fh).is_some());
        vfs.release_fh(plain_fh).await.unwrap();
    });
}

#[test]
fn read_uses_single_snapshot_and_absent_ignored_entry_never_zero_fills() {
    use super::super::file_session::OpenFlags;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-read-snapshot-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-read-snapshot");
    let (parent, trash, sidecar_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let parent = vfs.mkdir(ROOT_INO, b"private", 0, 0, 0o755).unwrap().ino;
        let source = vfs.create(parent, b"secret.txt", 0, 0, 0o644).unwrap();
        vfs.write(source.ino, 0, b"private bytes").await.unwrap();
        vfs.fsync(source.ino, false).await.unwrap();
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(parent, b"secret.txt", trash, b"$R000001", false)
            .unwrap();
        let entries = vfs.readdir(trash).unwrap();
        let sidecar = entries
            .iter()
            .find(|entry| entry.name.starts_with(b"$I"))
            .expect("retained listing includes sidecar");
        (parent, trash, sidecar.name.clone())
    });

    let sidecar_attr = vfs.lookup(trash, &sidecar_name).unwrap();
    let sidecar_snapshot = vfs.ignored_entry(sidecar_attr.ino).unwrap();
    let sidecar_fh = vfs
        .open_file(sidecar_attr.ino, OpenFlags::RDONLY, false)
        .unwrap();

    // Evict sidecar from by_ino while keeping a stale handle, then revoke parent access.
    assert!(vfs.detach_ignored_entry_name(sidecar_attr.ino));
    let discard_dir = vfs
        .mkdir(ROOT_INO, b".Spotlight-V100", 0, 0, 0o755)
        .unwrap();
    for idx in 0..8 {
        let name = format!("evict-{idx}");
        let tmp_entry = vfs
            .create(discard_dir.ino, name.as_bytes(), 0, 0, 0o644)
            .unwrap();
        assert!(vfs.detach_ignored_entry_name(tmp_entry.ino));
    }
    assert!(vfs.ignored_entry(sidecar_attr.ino).is_none());

    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();

    rt.block_on(async {
        let mut buf = vec![0x5Au8; sidecar_attr.size as usize];
        assert!(
            vfs.read_into_fh(sidecar_fh, sidecar_attr.ino, 0, &mut buf)
                .await
                .is_err(),
            "absent evicted ignored inode must not zero-fill via held handle"
        );
        assert!(
            vfs.read(sidecar_attr.ino, 0, sidecar_attr.size as u32)
                .await
                .is_err(),
            "absent evicted ignored inode must not zero-fill via read"
        );
        assert!(
            vfs.read_into(sidecar_attr.ino, 0, &mut buf).await.is_err(),
            "absent evicted ignored inode must not zero-fill via read_into"
        );
        assert!(
            vfs.read_ignored_entry_snapshot_into(&sidecar_snapshot, 0, &mut buf)
                .await
                .is_err(),
            "hidden generated snapshot must not zero-fill via read_ignored_entry_snapshot_into"
        );
        vfs.release_fh(sidecar_fh).await.unwrap();
    });
}

#[test]
fn directory_trash_view_subtree_respects_original_parent_acl() {
    use super::super::file_session::OpenFlags;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-dir-acl-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-dir-acl");
    let (parent, trash, view_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let parent = vfs.mkdir(ROOT_INO, b"private", 0, 0, 0o755).unwrap().ino;
        let dir = vfs.mkdir(parent, b"folder", 0, 0, 0o755).unwrap();
        let a = vfs.create(dir.ino, b"a.txt", 0, 0, 0o644).unwrap();
        vfs.write(a.ino, 0, b"secret-a").await.unwrap();
        let nested = vfs.mkdir(dir.ino, b"nested", 0, 0, 0o755).unwrap();
        let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
        vfs.write(b.ino, 0, b"secret-b").await.unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(parent, b"folder", trash, b"$RSHELLDIR", false)
            .unwrap();
        let entries = vfs.readdir(trash).unwrap();
        let view = entries
            .iter()
            .find(|entry| entry.name.starts_with(b"$R"))
            .expect("retained listing includes directory view");
        (parent, trash, view.name.clone())
    });

    let r_dir = vfs.lookup(trash, &view_name).unwrap();
    let r_a = vfs.lookup(r_dir.ino, b"a.txt").unwrap();
    let r_nested = vfs.lookup(r_dir.ino, b"nested").unwrap();
    let r_b = vfs.lookup(r_nested.ino, b"b.txt").unwrap();
    let r_b_fh = vfs.open_file(r_b.ino, OpenFlags::RDONLY, false).unwrap();

    // Warm effective_acl_cache before downgrading original_parent.
    assert_eq!(
        vfs.effective_access_level(r_b.ino).unwrap(),
        AccessLevel::ReadWrite
    );

    // ReadOnly permits listing/reading nested views but refuses subtree/root clear or restore.
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(r_b.ino, 0, 8)).unwrap()[..],
        b"secret-b"
    );
    assert!(matches!(
        vfs.unlink(r_nested.ino, b"b.txt"),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.rmdir(r_dir.ino, b"nested"),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.rmdir(trash, &view_name),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(
        vfs.rename(trash, &view_name, ROOT_INO, b"stolen-dir", false)
            .is_err()
    );
    assert!(vfs.lookup(r_nested.ino, b"b.txt").is_ok());

    // NoAccess immediately hides all warmed subtree directories/files and open reads.
    vfs.set_acl_rule(parent, &vfs.acl_principal, AccessLevel::NoAccess)
        .unwrap();
    assert!(vfs.lookup(r_dir.ino, b"a.txt").is_err());
    assert!(vfs.lookup(r_nested.ino, b"b.txt").is_err());
    assert!(vfs.getattr(r_nested.ino).is_err());
    assert!(vfs.getattr(r_b.ino).is_err());
    assert!(vfs.readdir(r_nested.ino).is_err());
    let mut buf = [0u8; 8];
    assert!(
        rt.block_on(vfs.read_into_fh(r_b_fh, r_b.ino, 0, &mut buf))
            .is_err()
    );
    assert!(rt.block_on(vfs.read(r_a.ino, 0, 8)).is_err());

    rt.block_on(vfs.release_fh(r_b_fh)).unwrap();
}

#[test]
fn write_fh_with_deferred_materialize_refuses_writable_retained_views() {
    use super::super::LocalSyncPolicy;
    use super::super::file_session::OpenFlags;
    use bytes::Bytes;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-deferred-write-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-deferred-write");
    let (trash, dir_view, file_view) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let parent = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap().ino;

        let dir = vfs.mkdir(parent, b"folder", 0, 0, 0o755).unwrap();
        let child = vfs.create(dir.ino, b"a.txt", 0, 0, 0o644).unwrap();
        vfs.write(child.ino, 0, b"alpha").await.unwrap();
        vfs.fsync(child.ino, false).await.unwrap();

        let file = vfs.create(parent, b"solo.txt", 0, 0, 0o644).unwrap();
        vfs.write(file.ino, 0, b"solo-bytes").await.unwrap();
        vfs.fsync(file.ino, false).await.unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(parent, b"folder", trash, b"$RDIR01", false)
            .unwrap();
        vfs.rename(parent, b"solo.txt", trash, b"$RFIL01", false)
            .unwrap();

        let mut dir_view = None;
        let mut file_view = None;
        for entry in vfs.readdir(trash).unwrap() {
            if entry.name.starts_with(b"$R") {
                if entry.kind == super::super::FileType::Directory {
                    dir_view = Some(entry.name);
                } else {
                    file_view = Some(entry.name);
                }
            }
        }
        (
            trash,
            dir_view.expect("directory view"),
            file_view.expect("file view"),
        )
    });

    let sidecar_name = [b"$I".as_slice(), &file_view[2..]].concat();
    let i_attr = vfs.lookup(trash, &sidecar_name).unwrap();
    let r_file_attr = vfs.lookup(trash, &file_view).unwrap();
    let r_dir_attr = vfs.lookup(trash, &dir_view).unwrap();
    let r_sub_attr = vfs.lookup(r_dir_attr.ino, b"a.txt").unwrap();

    // Open writable handles under original_parent ReadWrite.
    let i_fh = vfs.open_file(i_attr.ino, OpenFlags::RDWR, false).unwrap();
    let r_file_fh = vfs
        .open_file(r_file_attr.ino, OpenFlags::RDWR, false)
        .unwrap();
    let r_sub_fh = vfs
        .open_file(r_sub_attr.ino, OpenFlags::RDWR, false)
        .unwrap();

    rt.block_on(async {
        for (label, fh, orig_attr) in [
            ("$I sidecar", i_fh, &i_attr),
            ("$R file", r_file_fh, &r_file_attr),
            ("Subtree file", r_sub_fh, &r_sub_attr),
        ] {
            assert!(
                matches!(
                    vfs.write_fh_with_deferred_materialize(
                        fh,
                        orig_attr.ino,
                        0,
                        b"tamper-longer-than-original-payload",
                        LocalSyncPolicy::Opportunistic,
                    )
                    .await,
                    Err(VfsError::OperationNotSupported)
                ),
                "{label} deferred write_fh must fail with OperationNotSupported under RW parent"
            );
            assert!(
                matches!(
                    vfs.write_fh_owned_with_deferred_materialize(
                        fh,
                        orig_attr.ino,
                        0,
                        Bytes::from_static(b"owned-tamper-longer-than-original"),
                        LocalSyncPolicy::Strict,
                    )
                    .await,
                    Err(VfsError::OperationNotSupported)
                ),
                "{label} owned deferred write_fh must fail with OperationNotSupported under RW parent"
            );
            let current = vfs.getattr(orig_attr.ino).unwrap();
            assert_eq!(current.size, orig_attr.size, "{label} size mutated");
            assert_eq!(current.mtime, orig_attr.mtime, "{label} mtime mutated");
        }

        // Control: plain ignored file allows deferred writes and updates synthetic size.
        let plain = vfs.create(ROOT_INO, b".fseventsd", 0, 0, 0o644).unwrap();
        let plain_fh = vfs.open_file(plain.ino, OpenFlags::RDWR, false).unwrap();
        assert_eq!(
            vfs.write_fh_with_deferred_materialize(
                plain_fh,
                plain.ino,
                0,
                b"plain",
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap(),
            5
        );
        assert_eq!(vfs.getattr(plain.ino).unwrap().size, 5);

        vfs.release_fh(plain_fh).await.unwrap();
        vfs.release_fh(i_fh).await.unwrap();
        vfs.release_fh(r_file_fh).await.unwrap();
        vfs.release_fh(r_sub_fh).await.unwrap();
    });
}

#[test]
fn root_read_only_with_writable_work_dir_allows_restore_and_clear_while_ro_original_denies() {
    use studiofs_meta::trash::RetainedPurgeReason;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-guard-root-ro-work-rw-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-guard-root-ro-work-rw");
    let (trash, work, locked, work_file_views, work_dir_views, locked_file_view, locked_dir_view) =
        rt.block_on(async {
            meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
            let work = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap().ino;
            vfs.set_acl_rule(work, &vfs.acl_principal, AccessLevel::ReadWrite)
                .unwrap();
            let locked = vfs.mkdir(ROOT_INO, b"locked", 0, 0, 0o755).unwrap().ino;
            vfs.set_acl_rule(locked, &vfs.acl_principal, AccessLevel::ReadWrite)
                .unwrap();
            let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();

            // Create two files + two dirs under /work, and one file + one dir under /locked.
            let wf1 = vfs.create(work, b"wf1.txt", 0, 0, 0o644).unwrap();
            vfs.write(wf1.ino, 0, b"work-file-1").await.unwrap();
            vfs.fsync(wf1.ino, false).await.unwrap();

            let wf2 = vfs.create(work, b"wf2.txt", 0, 0, 0o644).unwrap();
            vfs.write(wf2.ino, 0, b"work-file-2").await.unwrap();
            vfs.fsync(wf2.ino, false).await.unwrap();

            async fn seed_sample_dir(vfs: &StudioVfs, parent: Ino, name: &[u8]) {
                let d = vfs.mkdir(parent, name, 0, 0, 0o755).unwrap();
                let a = vfs.create(d.ino, b"a.txt", 0, 0, 0o644).unwrap();
                vfs.write(a.ino, 0, b"dir-a").await.unwrap();
                vfs.fsync(a.ino, false).await.unwrap();
                let nested = vfs.mkdir(d.ino, b"nested", 0, 0, 0o755).unwrap();
                let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
                vfs.write(b.ino, 0, b"dir-b").await.unwrap();
                vfs.fsync(b.ino, false).await.unwrap();
            }
            seed_sample_dir(&vfs, work, b"wd1").await;
            seed_sample_dir(&vfs, work, b"wd2").await;

            let lf = vfs.create(locked, b"lf.txt", 0, 0, 0o644).unwrap();
            vfs.write(lf.ino, 0, b"locked-file").await.unwrap();
            vfs.fsync(lf.ino, false).await.unwrap();
            seed_sample_dir(&vfs, locked, b"ld").await;

            vfs.rename(work, b"wf1.txt", trash, b"$RWFIL1", false)
                .unwrap();
            vfs.rename(work, b"wf2.txt", trash, b"$RWFIL2", false)
                .unwrap();
            vfs.rename(work, b"wd1", trash, b"$RWDIR1", false).unwrap();
            vfs.rename(work, b"wd2", trash, b"$RWDIR2", false).unwrap();
            vfs.rename(locked, b"lf.txt", trash, b"$RLFIL1", false)
                .unwrap();
            vfs.rename(locked, b"ld", trash, b"$RLDIR1", false).unwrap();

            let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
            let mut work_file_views = Vec::new();
            let mut work_dir_views = Vec::new();
            let mut locked_file_view = None;
            let mut locked_dir_view = None;
            for child in meta.readdir(store).unwrap() {
                let Ok(receipt) = std::str::from_utf8(&child.name).unwrap().parse::<Ino>() else {
                    continue;
                };
                let entry = meta.retained_entry(receipt).unwrap().unwrap();
                let view = trash_view_name(receipt).into_bytes();
                let kind = meta.get_attr(entry.inode).unwrap().kind;
                if entry.original_parent == work {
                    if kind == super::super::FileType::Directory {
                        work_dir_views.push((receipt, view));
                    } else {
                        work_file_views.push((receipt, view));
                    }
                } else if entry.original_parent == locked {
                    if kind == super::super::FileType::Directory {
                        locked_dir_view = Some((receipt, view));
                    } else {
                        locked_file_view = Some((receipt, view));
                    }
                }
            }
            (
                trash,
                work,
                locked,
                work_file_views,
                work_dir_views,
                locked_file_view.unwrap(),
                locked_dir_view.unwrap(),
            )
        });

    // Make ROOT_INO ReadOnly ($RECYCLE.BIN inherits ReadOnly), /locked ReadOnly,
    // while /work remains explicitly ReadWrite.
    vfs.set_acl_rule(ROOT_INO, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();
    vfs.set_acl_rule(locked, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();

    // Destination ROOT_INO (ReadOnly) must still be denied even for /work entry.
    assert!(matches!(
        vfs.rename(
            trash,
            &work_file_views[0].1,
            ROOT_INO,
            b"denied-root",
            false
        ),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));

    // /work file & directory restore MUST succeed despite $RECYCLE.BIN inheriting ROOT ReadOnly.
    vfs.rename(trash, &work_file_views[0].1, work, b"restored.txt", false)
        .expect("file restore into RW /work under RO root must succeed");
    let restored_file = vfs.lookup(work, b"restored.txt").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(restored_file.ino, 0, 16)).unwrap()[..],
        b"work-file-1"
    );

    vfs.rename(trash, &work_dir_views[0].1, work, b"restored-dir", false)
        .expect("directory restore into RW /work under RO root must succeed");
    let restored_dir = vfs.lookup(work, b"restored-dir").unwrap();
    let restored_nested = vfs.lookup(restored_dir.ino, b"nested").unwrap();
    let restored_b = vfs.lookup(restored_nested.ino, b"b.txt").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(restored_b.ino, 0, 16)).unwrap()[..],
        b"dir-b"
    );

    // /work file & directory bottom-up Clear MUST succeed despite $RECYCLE.BIN inheriting ROOT ReadOnly.
    vfs.unlink(trash, &work_file_views[1].1)
        .expect("file Clear for RW /work under RO root must succeed");
    assert_eq!(
        meta.retained_entry(work_file_views[1].0)
            .unwrap()
            .unwrap()
            .state,
        RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        }
    );

    let r_dir = vfs.lookup(trash, &work_dir_views[1].1).unwrap();
    let r_nested = vfs.lookup(r_dir.ino, b"nested").unwrap();
    vfs.unlink(r_nested.ino, b"b.txt").unwrap();
    vfs.rmdir(r_dir.ino, b"nested").unwrap();
    vfs.unlink(r_dir.ino, b"a.txt").unwrap();
    vfs.rmdir(trash, &work_dir_views[1].1)
        .expect("directory Clear for RW /work under RO root must succeed");
    assert_eq!(
        meta.retained_entry(work_dir_views[1].0)
            .unwrap()
            .unwrap()
            .state,
        RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        }
    );

    // /locked (ReadOnly original_parent) MUST still deny restore (even into RW /work) and Clear.
    assert!(matches!(
        vfs.rename(trash, &locked_file_view.1, work, b"stolen-lf", false),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.rename(trash, &locked_dir_view.1, work, b"stolen-ld", false),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.unlink(trash, &locked_file_view.1),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(matches!(
        vfs.rmdir(trash, &locked_dir_view.1),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    let locked_r_dir = vfs.lookup(trash, &locked_dir_view.1).unwrap();
    assert!(matches!(
        vfs.unlink(locked_r_dir.ino, b"a.txt"),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert_eq!(
        meta.retained_entry(locked_file_view.0)
            .unwrap()
            .unwrap()
            .state,
        RetainedEntryState::Retained
    );
    assert_eq!(
        meta.retained_entry(locked_dir_view.0)
            .unwrap()
            .unwrap()
            .state,
        RetainedEntryState::Retained
    );
}

#[test]
fn partial_directory_clear_persists_across_reopen_updates_sidecar_and_restores_remaining_tree() {
    use super::super::tests::reopen_existing_test_vfs;
    use super::trash_sidecar_name;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-partial-clear-reopen-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-partial-clear-reopen");
    let (work, trash, receipt, view_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let work = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap().ino;
        let dir = vfs.mkdir(work, b"folder", 0, 0, 0o755).unwrap();
        let a = vfs.create(dir.ino, b"a.txt", 0, 0, 0o644).unwrap();
        vfs.write(a.ino, 0, b"alpha-123").await.unwrap();
        vfs.fsync(a.ino, false).await.unwrap();
        let nested = vfs.mkdir(dir.ino, b"nested", 0, 0, 0o755).unwrap();
        let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
        vfs.write(b.ino, 0, b"beta-4567").await.unwrap();
        vfs.fsync(b.ino, false).await.unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(work, b"folder", trash, b"$RPART01", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let receipt = meta.readdir(store).unwrap()[0].ino;
        (work, trash, receipt, trash_view_name(receipt))
    });

    let sidecar = trash_sidecar_name(receipt);
    let r_dir = vfs.lookup(trash, view_name.as_bytes()).unwrap();
    let r_nested = vfs.lookup(r_dir.ino, b"nested").unwrap();
    vfs.unlink(r_nested.ino, b"b.txt").unwrap();
    vfs.rmdir(r_dir.ino, b"nested").unwrap();

    // Sidecar $I size must immediately reflect only the surviving tree (9 bytes for a.txt).
    let i_attr = vfs.lookup(trash, sidecar.as_bytes()).unwrap();
    let i_bytes = rt
        .block_on(vfs.read(i_attr.ino, 0, i_attr.size as u32))
        .unwrap();
    assert_eq!(
        u64::from_le_bytes(i_bytes[8..16].try_into().unwrap()),
        9,
        "updated $I logical size after deleting nested/b.txt must equal 9 bytes"
    );

    // Simulate process kill / unmount / remount mid-walk.
    drop(vfs);
    drop(meta);
    drop(rt);
    let (rt, vfs, meta) = reopen_existing_test_vfs(&tmp);

    let r_dir_reopened = vfs.lookup(trash, view_name.as_bytes()).unwrap();
    assert!(
        vfs.lookup(r_dir_reopened.ino, b"nested").is_err(),
        "deleted child directory must remain deleted across VFS reopen"
    );
    let remaining_a = vfs.lookup(r_dir_reopened.ino, b"a.txt").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(remaining_a.ino, 0, 16)).unwrap()[..],
        b"alpha-123"
    );
    let i_attr_reopened = vfs.lookup(trash, sidecar.as_bytes()).unwrap();
    let i_bytes_reopened = rt
        .block_on(vfs.read(i_attr_reopened.ino, 0, i_attr_reopened.size as u32))
        .unwrap();
    assert_eq!(
        u64::from_le_bytes(i_bytes_reopened[8..16].try_into().unwrap()),
        9
    );

    // Restoring the surviving tree must restore only a.txt with exact bytes.
    vfs.rename(trash, view_name.as_bytes(), work, b"restored", false)
        .expect("restore of surviving partial-Clear tree must succeed");
    let restored_dir = vfs.lookup(work, b"restored").unwrap();
    assert!(vfs.lookup(restored_dir.ino, b"nested").is_err());
    let restored_a = vfs.lookup(restored_dir.ino, b"a.txt").unwrap();
    assert_eq!(
        &rt.block_on(vfs.read(restored_a.ino, 0, 16)).unwrap()[..],
        b"alpha-123"
    );
    assert!(matches!(
        meta.retained_entry(receipt).unwrap().unwrap().state,
        RetainedEntryState::Restored { .. }
    ));
}

#[test]
fn partial_directory_clear_survives_unrelated_and_reverted_acl_changes() {
    use studiofs_meta::trash::RetainedPurgeReason;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-partial-acl-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-partial-acl");
    let (work, other, trash, receipt, view_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let work = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap().ino;
        let other = vfs.mkdir(ROOT_INO, b"other", 0, 0, 0o755).unwrap().ino;
        let dir = vfs.mkdir(work, b"folder", 0, 0, 0o755).unwrap();
        let a = vfs.create(dir.ino, b"a.txt", 0, 0, 0o644).unwrap();
        vfs.write(a.ino, 0, b"alpha-123").await.unwrap();
        let nested = vfs.mkdir(dir.ino, b"nested", 0, 0, 0o755).unwrap();
        let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
        vfs.write(b.ino, 0, b"beta-4567").await.unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(work, b"folder", trash, b"$RACL01", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let receipt = meta.readdir(store).unwrap()[0].ino;
        (work, other, trash, receipt, trash_view_name(receipt))
    });

    let r_dir = vfs.lookup(trash, view_name.as_bytes()).unwrap();
    let r_nested = vfs.lookup(r_dir.ino, b"nested").unwrap();
    vfs.unlink(r_nested.ino, b"b.txt").unwrap();
    vfs.rmdir(r_dir.ino, b"nested").unwrap();

    // Unrelated ACL mutation must never resurrect deleted children or abort Clear.
    vfs.set_acl_rule(other, "alice", AccessLevel::ReadOnly)
        .unwrap();
    assert!(
        vfs.lookup(r_dir.ino, b"nested").is_err(),
        "unrelated ACL change must not resurrect deleted child"
    );

    // Downgrading original_parent denies further deletes while keeping earlier deletes permanent.
    vfs.set_acl_rule(work, &vfs.acl_principal, AccessLevel::ReadOnly)
        .unwrap();
    assert!(matches!(
        vfs.unlink(r_dir.ino, b"a.txt"),
        Err(VfsError::Meta(MetaError::AccessDenied { .. }))
    ));
    assert!(vfs.lookup(r_dir.ino, b"nested").is_err());

    // Restoring ReadWrite allows completing Clear cleanly.
    vfs.set_acl_rule(work, &vfs.acl_principal, AccessLevel::ReadWrite)
        .unwrap();
    assert!(vfs.lookup(r_dir.ino, b"nested").is_err());
    vfs.unlink(r_dir.ino, b"a.txt").unwrap();
    vfs.rmdir(trash, view_name.as_bytes()).unwrap();
    assert_eq!(
        meta.retained_entry(receipt).unwrap().unwrap().state,
        RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        }
    );
}

#[test]
fn large_directory_clear_exceeding_legacy_tombstone_cap_completes_without_resurrection() {
    use studiofs_meta::trash::RetainedPurgeReason;

    const FILE_COUNT: usize = 80;
    let tmp = studiofs_test_support::workspace_target_tempdir("retained-large-clear-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-large-clear");
    let (trash, receipt, view_name) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let d = vfs.mkdir(ROOT_INO, b"big-dir", 0, 0, 0o755).unwrap();
        for i in 0..FILE_COUNT {
            let name = format!("f-{i:03}");
            vfs.create(d.ino, name.as_bytes(), 0, 0, 0o644).unwrap();
        }
        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(ROOT_INO, b"big-dir", trash, b"$RLARGE", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let receipt = meta.readdir(store).unwrap()[0].ino;
        (trash, receipt, trash_view_name(receipt))
    });

    let view_dir = vfs.lookup(trash, view_name.as_bytes()).unwrap();
    for i in 0..FILE_COUNT {
        let name = format!("f-{i:03}");
        vfs.unlink(view_dir.ino, name.as_bytes())
            .unwrap_or_else(|err| panic!("unlink {name} failed: {err:?}"));
    }
    assert!(
        vfs.readdir(view_dir.ino).unwrap().is_empty(),
        "no earlier children may resurrect after >64 deletes"
    );
    vfs.rmdir(trash, view_name.as_bytes())
        .expect("final rmdir after deleting all >64 children must succeed");
    assert_eq!(
        meta.retained_entry(receipt).unwrap().unwrap().state,
        RetainedEntryState::PurgeRequested {
            reason: RetainedPurgeReason::User,
        }
    );
}

#[test]
fn open_trunc_on_retained_views_returns_operation_not_supported() {
    use super::super::file_session::OpenFlags;
    use super::trash_sidecar_name;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-open-trunc-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-open-trunc");
    let (trash, file_receipt, dir_receipt) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let f = vfs.create(ROOT_INO, b"solo.txt", 0, 0, 0o644).unwrap();
        vfs.write(f.ino, 0, b"solo-data").await.unwrap();
        vfs.fsync(f.ino, false).await.unwrap();

        let d = vfs.mkdir(ROOT_INO, b"folder", 0, 0, 0o755).unwrap();
        let sub = vfs.create(d.ino, b"child.txt", 0, 0, 0o644).unwrap();
        vfs.write(sub.ino, 0, b"child-data").await.unwrap();
        vfs.fsync(sub.ino, false).await.unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(ROOT_INO, b"solo.txt", trash, b"$RF01", false)
            .unwrap();
        vfs.rename(ROOT_INO, b"folder", trash, b"$RD01", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let mut receipts: Vec<Ino> = meta
            .readdir(store)
            .unwrap()
            .into_iter()
            .map(|e| e.ino)
            .collect();
        receipts.sort_unstable();
        (trash, receipts[0], receipts[1])
    });

    let i_attr = vfs
        .lookup(trash, trash_sidecar_name(file_receipt).as_bytes())
        .unwrap();
    let r_file_attr = vfs
        .lookup(trash, trash_view_name(file_receipt).as_bytes())
        .unwrap();
    let r_dir_attr = vfs
        .lookup(trash, trash_view_name(dir_receipt).as_bytes())
        .unwrap();
    let r_sub_attr = vfs.lookup(r_dir_attr.ino, b"child.txt").unwrap();

    for (label, ino) in [
        ("$I sidecar", i_attr.ino),
        ("$R file", r_file_attr.ino),
        ("$R subtree child", r_sub_attr.ino),
    ] {
        let err = vfs
            .open_file(ino, OpenFlags::RDWR, true)
            .err()
            .unwrap_or_else(|| panic!("{label}: trunc=true must fail"));
        assert!(
            matches!(err, VfsError::OperationNotSupported),
            "{label}: expected OperationNotSupported for trunc=true, got {err:?}"
        );

        let fh = vfs
            .open_file(ino, OpenFlags::RDONLY, false)
            .unwrap_or_else(|_| panic!("{label}: non-trunc RDONLY open must succeed"));
        rt.block_on(vfs.release_fh(fh)).unwrap();
    }
}

#[test]
fn partial_directory_clear_with_outside_hardlink_and_open_handle_preserves_live_data() {
    use super::super::file_session::OpenFlags;

    let tmp = studiofs_test_support::workspace_target_tempdir("retained-clear-hardlink-open-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-clear-hardlink-open");
    let (trash, receipt, shared_ino, outside_ino, open_source_ino) = rt.block_on(async {
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let work = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap().ino;
        let folder = vfs.mkdir(work, b"folder", 0, 0, 0o755).unwrap();

        let shared = vfs.create(folder.ino, b"shared.txt", 0, 0, 0o644).unwrap();
        vfs.write(shared.ino, 0, b"shared-live-payload")
            .await
            .unwrap();
        vfs.fsync(shared.ino, false).await.unwrap();
        vfs.link(shared.ino, ROOT_INO, b"outside-live.txt").unwrap();

        let opened = vfs.create(folder.ino, b"opened.txt", 0, 0, 0o644).unwrap();
        vfs.write(opened.ino, 0, b"opened-orphan-payload")
            .await
            .unwrap();
        vfs.fsync(opened.ino, false).await.unwrap();
        vfs.link(opened.ino, ROOT_INO, b"temp-open.txt").unwrap();

        let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
        vfs.rename(work, b"folder", trash, b"$RHL01", false)
            .unwrap();
        let store = meta.lookup(ROOT_INO, PRIVATE_STORE_NAME).unwrap().ino;
        let receipt = meta.readdir(store).unwrap()[0].ino;
        (trash, receipt, shared.ino, shared.ino, opened.ino)
    });

    // Open a handle on `temp-open.txt` and then unlink `temp-open.txt` so `open_source_ino`
    // stays open (`nlink == 1` inside retained dir, 1 live handle).
    let open_fh = vfs
        .open_file(open_source_ino, OpenFlags::RDONLY, false)
        .unwrap();
    vfs.unlink(ROOT_INO, b"temp-open.txt").unwrap();

    let view_dir = vfs
        .lookup(trash, trash_view_name(receipt).as_bytes())
        .unwrap();
    // Unlink `shared.txt` from trash: outside hardlink must remain live with full bytes!
    vfs.unlink(view_dir.ino, b"shared.txt").unwrap();
    assert!(vfs.lookup(view_dir.ino, b"shared.txt").is_err());
    let outside_attr = vfs.lookup(ROOT_INO, b"outside-live.txt").unwrap();
    assert_eq!(outside_attr.ino, outside_ino);
    assert_eq!(outside_attr.nlink, 1);
    assert_eq!(
        &rt.block_on(vfs.read(shared_ino, 0, 32)).unwrap()[..],
        b"shared-live-payload"
    );

    // Unlink `opened.txt` while `open_fh` is open: open handle must keep reading orphaned payload!
    vfs.unlink(view_dir.ino, b"opened.txt").unwrap();
    assert!(vfs.lookup(view_dir.ino, b"opened.txt").is_err());
    // Per-call ACL hides private payloads from handle reads; the purge must still
    // defer inode destruction to the last close, like an ordinary open unlink.
    let orphan = meta.get_attr(open_source_ino).unwrap();
    assert_eq!(orphan.nlink, 0);
    // The purge is not final until metadata removes the inode: no upload tombstone yet.
    assert!(
        !vfs.state
            .upload
            .deleted_inodes
            .lock()
            .contains(&open_source_ino)
    );

    // Releasing the last open handle completes orphan cleanup without corrupting shared.txt.
    rt.block_on(vfs.release_fh(open_fh)).unwrap();
    assert!(meta.get_attr(open_source_ino).is_err());
    assert!(
        vfs.state
            .upload
            .deleted_inodes
            .lock()
            .contains(&open_source_ino),
        "a removed orphan cancels its uploads"
    );
    assert_eq!(
        &rt.block_on(vfs.read(shared_ino, 0, 32)).unwrap()[..],
        b"shared-live-payload"
    );
}
