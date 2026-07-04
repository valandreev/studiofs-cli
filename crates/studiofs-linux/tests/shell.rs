//! Smoke tests for the clean Linux bridge shell.

use std::sync::Arc;
use std::time::Duration;

use studiofs_data::DataStoreConfig;
use studiofs_linux::{LinuxMountRequest, LinuxMountShell};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, ROOT_INO, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig, VfsConfigValidated};
use tokio::runtime::Runtime;

fn make_format_info(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("linux-shell-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("linux-shell-uuid-{volume_suffix}"),
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "studiofs-test".into(),
        access_key: "test".into(),
        secret_key: "test".into(),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: studiofs_meta::EncryptionConfig::new(
            [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        ),
        block_size: 4 * 1024 * 1024,
    }
}

fn make_vfs(tmp: &tempfile::TempDir) -> (Runtime, Arc<StudioVfs>) {
    let runtime = Runtime::new().expect("runtime");
    let _guard = runtime.enter();
    let meta = Arc::new(RedbMetaEngine::open(tmp.path().join("meta.redb")).expect("open redb"));
    meta.init_root(0, 0).expect("init root");
    let format_info = make_format_info("smoke");
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir utf8"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfigValidated::try_from(VfsConfig::default()).expect("valid vfs config"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs");
    (runtime, vfs)
}

#[test]
fn shell_answers_root_lifecycle_callbacks() {
    let tmp = studiofs_test_support::workspace_target_tempdir("linux-shell-");
    let (runtime, vfs) = make_vfs(&tmp);
    let mut shell = LinuxMountShell::new(LinuxMountRequest { vfs: vfs.clone() });

    shell.init().expect("init");
    let root = shell.getattr(ROOT_INO).expect("root getattr");
    let entries = shell.readdir(ROOT_INO).expect("root readdir");
    let dot = shell.lookup(ROOT_INO, b".").expect("root lookup");
    shell.destroy().expect("destroy");

    assert_eq!(root.ino, ROOT_INO);
    assert_eq!(dot.ino, ROOT_INO);
    assert!(entries.is_empty());

    runtime.block_on(vfs.shutdown(Duration::ZERO));
}

#[test]
fn unsupported_requests_return_enosys() {
    let err = LinuxMountShell::unsupported().expect_err("read unsupported");

    assert_eq!(err.raw_os_error(), Some(libc::ENOSYS));
}
