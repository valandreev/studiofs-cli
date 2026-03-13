#![cfg(windows)]

use std::sync::Arc;
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaEngine, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::StudioVfs;
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
    StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .expect("create vfs")
}

fn signal_named_event(name: &str) {
    unsafe extern "system" {
        fn OpenEventW(
            dwDesiredAccess: u32,
            bInheritHandle: i32,
            lpName: *const u16,
        ) -> *mut core::ffi::c_void;
        fn SetEvent(hEvent: *mut core::ffi::c_void) -> i32;
        fn CloseHandle(hObject: *mut core::ffi::c_void) -> i32;
    }

    const EVENT_MODIFY_STATE: u32 = 0x0002;
    let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
    if event.is_null() {
        return;
    }
    unsafe {
        let _ = SetEvent(event);
        let _ = CloseHandle(event);
    }
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_unmount_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mount_dir = tempfile::tempdir().expect("mountdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let event_name = format!("Local\\StudioFsMountSmoke-{}", std::process::id());
    let mount_point = mount_dir
        .path()
        .to_str()
        .expect("mount point utf8")
        .to_string();
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
        )
        .map_err(|e| e.to_string())
    });

    std::thread::sleep(Duration::from_secs(1));
    signal_named_event(&event_name);

    let result = handle.join().expect("mount thread panicked");
    result.expect("WinFSP mount/unmount should complete");
}
