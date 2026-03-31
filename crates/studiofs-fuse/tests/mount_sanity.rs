#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::StudioFuseFs;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("fuse-test-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("fuse-test-uuid-{volume_suffix}"),
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "studiofs-test".into(),
        access_key: "test".into(),
        secret_key: "test".into(),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        block_size: 4 * 1024 * 1024,
    }
}

fn make_test_vfs(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
) -> Arc<StudioVfs> {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");

    let format_info = test_format_info(volume_suffix);
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs background workers");
    vfs
}

#[test]
#[ignore = "requires /dev/fuse or macFUSE and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_session_mount_and_drop_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mount_dir = tempfile::tempdir().expect("mount tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let fs = StudioFuseFs::new(vfs, rt);
    let options = vec![
        fuser::MountOption::FSName("studiofs-test".into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::CUSTOM("foreground".into()),
    ];

    let session = fuser::Session::new(fs, mount_dir.path(), &options).expect("create FUSE session");
    drop(session);
}

#[cfg(target_os = "macos")]
#[test]
fn macos_resource_fork_roundtrip_uses_native_xattr_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let vfs = make_test_vfs(&tmp, "macos_resource_fork_xattr", &rt);

    let file = vfs
        .create(1, b"fork.bin", 0, 0, 0o644)
        .expect("create file for resource fork");
    let fork_name = b"com.apple.ResourceFork";
    let fork_value = vec![0, 1, 2, 3, b'R', b'S', b'R', b'C'];

    vfs.set_xattr(file.ino, fork_name, &fork_value, 0)
        .expect("set resource fork xattr");

    let got = vfs
        .get_xattr(file.ino, fork_name)
        .expect("get resource fork xattr");
    assert_eq!(got, Some(fork_value.clone()));

    let names = vfs.list_xattrs(file.ino).expect("list xattrs");
    assert!(
        names.iter().any(|name| name.as_slice() == fork_name),
        "resource fork key should be visible in native xattr listing"
    );

    vfs.remove_xattr(file.ino, fork_name)
        .expect("remove resource fork xattr");
    let after_remove = vfs
        .get_xattr(file.ino, fork_name)
        .expect("get xattr after remove");
    assert_eq!(after_remove, None);
}

/// Full lifecycle smoke test: mount → write → read → readdir → delete → unmount.
///
/// Uses the low-level `fuser::Session` API directly (not `mount_and_serve`)
/// because the latter calls `std::process::exit(0)` after unmount, which would
/// kill the test harness.
///
/// Mount options intentionally omit `AllowOther` to avoid requiring
/// `user_allow_other` in `/etc/fuse.conf`, making this runnable on CI hosted
/// runners that have `/dev/fuse` available.
#[test]
#[ignore = "requires /dev/fuse or macFUSE and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_mount_write_read_unmount_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mount_dir = tempfile::tempdir().expect("mount tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let fs = StudioFuseFs::new(vfs, rt);

    let options = vec![
        fuser::MountOption::FSName("studiofs-smoke".into()),
        fuser::MountOption::AutoUnmount,
    ];

    let mount_path = mount_dir.path().to_path_buf();
    let mut session = fuser::Session::new(fs, &mount_path, &options).expect("create FUSE session");
    let mut unmount = session.unmount_callable();

    // Run the FUSE event loop in a background thread.
    let session_handle = std::thread::spawn(move || {
        if let Err(e) = session.run() {
            eprintln!("FUSE session error (may be normal during unmount): {e}");
        }
    });

    // Poll until the mount becomes available (up to 10 s).
    // On slow CI runners a fixed sleep(2 s) is insufficient.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::metadata(&mount_path).is_ok() {
            // The mount root is stat-able — FUSE is up.
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "FUSE mount did not become available within 10 s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── Smoke: create + write ───────────────────────────────────────
    let test_file = mount_path.join("smoke.txt");
    let payload = b"StudioFS FUSE smoke-test payload";
    std::fs::write(&test_file, payload).expect("write through FUSE mount");

    // ── Smoke: read-back ────────────────────────────────────────────
    let readback = std::fs::read(&test_file).expect("read through FUSE mount");
    assert_eq!(readback, payload, "read-back must match written data");

    // ── Smoke: directory listing ────────────────────────────────────
    let entries: Vec<_> = std::fs::read_dir(&mount_path)
        .expect("readdir through FUSE mount")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    assert!(
        entries.iter().any(|n| n == "smoke.txt"),
        "directory listing must contain the created file, got: {entries:?}"
    );

    // ── Smoke: delete ───────────────────────────────────────────────
    std::fs::remove_file(&test_file).expect("delete through FUSE mount");

    // ── Clean unmount ───────────────────────────────────────────────
    let _ = unmount.unmount();
    session_handle
        .join()
        .expect("FUSE session thread should exit cleanly");
}
