#![cfg(windows)]

use std::sync::Arc;
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};
use studiofs_winfsp::mount_and_serve;

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("winfsp-mount-test-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("winfsp-mount-test-uuid-{volume_suffix}"),
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

fn signal_named_event(name: &str) {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Threading::{EVENT_MODIFY_STATE, OpenEventW, SetEvent};
    use windows::core::HSTRING;

    let wide = HSTRING::from(name);
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, false, &wide) };
    let Ok(event) = event else { return };
    let _ = unsafe { SetEvent(event) };
    let _ = unsafe { CloseHandle(event) };
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_unmount_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP mount smoke test: set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let event_name = format!("Local\\StudioFsMountSmoke-{}", std::process::id());
    let event_for_thread = event_name.clone();

    let handle = std::thread::spawn(move || {
        mount_and_serve(
            vfs,
            rt,
            &mount_point,
            "StudioFS-Test",
            true,
            false,
            Some(&event_for_thread),
            None,
        )
        .map_err(|e| e.to_string())
    });

    std::thread::sleep(Duration::from_secs(1));
    signal_named_event(&event_name);

    let result = handle.join().expect("mount thread panicked");
    result.expect("WinFSP mount/unmount should complete");
}

/// Full lifecycle smoke test: mount → write → read → readdir → delete → unmount.
///
/// Uses `mount_and_serve` in a background thread with a named Win32 event
/// for external shutdown. File I/O is performed through the drive letter
/// before signalling the event.
#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_write_read_unmount_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP write-read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let event_name = format!("Local\\StudioFsWriteReadSmoke-{}", std::process::id());
    let event_for_thread = event_name.clone();
    let mp_for_thread = mount_point.clone();

    let handle = std::thread::spawn(move || {
        mount_and_serve(
            vfs,
            rt,
            &mp_for_thread,
            "StudioFS-Test",
            true,
            false,
            Some(&event_for_thread),
            None,
        )
        .map_err(|e| e.to_string())
    });

    // Poll until the drive root becomes available (up to 10 s).
    // On slow CI runners a fixed sleep(2 s) is insufficient.
    let drive_root = if mount_point.ends_with('\\') || mount_point.ends_with('/') {
        mount_point.clone()
    } else {
        format!("{mount_point}\\")
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::metadata(&drive_root).is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "WinFSP mount at {drive_root} did not become available within 10 s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── Smoke: create + write ───────────────────────────────────────
    let test_file = std::path::PathBuf::from(&drive_root).join("smoke.txt");
    let payload = b"StudioFS WinFSP smoke-test payload";
    std::fs::write(&test_file, payload).expect("write through WinFSP mount");

    // ── Smoke: read-back ────────────────────────────────────────────
    let readback = std::fs::read(&test_file).expect("read through WinFSP mount");
    assert_eq!(readback, payload, "read-back must match written data");

    // ── Smoke: directory listing ────────────────────────────────────
    let entries: Vec<_> = std::fs::read_dir(&drive_root)
        .expect("readdir through WinFSP mount")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    assert!(
        entries.iter().any(|n| n == std::ffi::OsStr::new("smoke.txt")),
        "directory listing must contain the created file, got: {entries:?}"
    );

    // ── Smoke: delete ───────────────────────────────────────────────
    std::fs::remove_file(&test_file).expect("delete through WinFSP mount");

    // ── Clean unmount ───────────────────────────────────────────────
    signal_named_event(&event_name);
    let result = handle.join().expect("mount thread panicked");
    result.expect("WinFSP mount/unmount should complete");
}
