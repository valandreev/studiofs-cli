#![cfg(unix)]

use std::sync::Arc;

use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::StudioFuseFs;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaEngine, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::StudioVfs;

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
    StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .expect("create vfs")
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
