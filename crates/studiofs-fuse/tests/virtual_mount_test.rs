//! Integration tests for `studiofs-fuse` virtual mount and VFS behavior.
//!
//! These tests exercise core filesystem paths directly through `StudioVfs` and
//! `StudioFuseFs` on Unix targets.

#![cfg(target_os = "linux")]

use std::sync::{Arc, OnceLock};
use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::StudioFuseFs;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};

/// Resolves the workspace root directory from the current crate manifest path.
fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
}

/// Computes the path to the repository `.env` file.
fn workspace_env_path() -> std::path::PathBuf {
    workspace_root().join(".env")
}

/// Loads environment variables from `.env` once for the current test process.
fn load_test_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    let _ = ONCE.get_or_init(|| {
        let env_path = workspace_env_path();
        if env_path.exists() {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }
    });
}

/// Returns cached repository-level `.env` variables loaded from disk.
fn workspace_dotenv_vars() -> &'static std::collections::BTreeMap<String, String> {
    static VARS: OnceLock<std::collections::BTreeMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = std::collections::BTreeMap::new();
        let env_path = workspace_env_path();
        if let Ok(iter) = dotenvy::from_path_iter(&env_path) {
            for item in iter.flatten() {
                vars.insert(item.0, item.1);
            }
        }
        vars
    })
}

/// Gets a cached `.env` variable by key.
fn workspace_dotenv_var(key: &str) -> Option<String> {
    workspace_dotenv_vars().get(key).cloned()
}

/// Resolves a test environment variable from process env, repository `.env`, and legacy key.
fn test_online_env_var(primary: &str, legacy: &str, default: &str) -> String {
    load_test_env();
    std::env::var(primary)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| workspace_dotenv_var(primary))
        .or_else(|| workspace_dotenv_var(legacy))
        .unwrap_or_else(|| default.to_string())
}

// NOTE: Testing StudioFuseFs directly is hard because fuser::Request is not public.
// We test the VFS logic directly because fuser::Request is not public.

/// Smoke test for virtual mount behavior through the VFS layer.
#[test]
fn test_virtual_mount_vfs_smoke() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    load_test_env();

    // 1. Setup VFS and the Bridge (StudioFuseFs)
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");

    let volume_name = "virtual-smoke-test";
    let format_info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.into(),
        uuid: "virtual-smoke-uuid".into(),
        endpoint: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
            "STUDIOFS_S3_ENDPOINT",
            "http://127.0.0.1:9000",
        ),
        bucket: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_BUCKET",
            "STUDIOFS_S3_BUCKET",
            "studiofs-test",
        ),
        access_key: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
            "STUDIOFS_S3_ACCESS_KEY",
            "studiofs",
        ),
        secret_key: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
            "STUDIOFS_S3_SECRET_KEY",
            "studiofs123",
        ),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: studiofs_meta::EncryptionConfig::new(
            [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        ),
        block_size: 4 * 1024 * 1024,
    };
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
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

    {
        let _guard = rt.enter();
        vfs.start().expect("start vfs background workers");
    }

    let _fs = StudioFuseFs::new(vfs.clone(), rt.handle().clone());

    let file = vfs
        .create(1, b"test.txt", 0, 0, 0o644)
        .expect("create file");
    let payload = b"studiofs virtual mount smoke";
    let fh = vfs
        .open_file(file.ino, studiofs_vfs::OpenFlags::WRONLY, false)
        .expect("open file for write");
    rt.block_on(vfs.write_fh(fh, file.ino, 0, payload))
        .expect("write payload");
    rt.block_on(vfs.release_fh(fh))
        .expect("release write handle");

    let readback = rt
        .block_on(vfs.read(file.ino, 0, payload.len() as u32))
        .expect("read payload");
    assert_eq!(readback.as_ref(), payload);
}
