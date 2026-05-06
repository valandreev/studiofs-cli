//! Crate documentation for lint completion.
#![cfg(unix)]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::StudioFuseFs;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};

const PLATFORM_BRIDGE_MOUNT_TESTS_ENV: &str = "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS";

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn workspace_env_path() -> std::path::PathBuf {
    workspace_root().join(".env")
}

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

fn platform_bridge_mount_tests_skip_message(test_name: &str, actual: Option<&str>) -> String {
    let actual = actual.unwrap_or("<unset>");
    format!(
        "studiofs_smoke_skip test={test_name:?} reason=\"env gate disabled\" env={PLATFORM_BRIDGE_MOUNT_TESTS_ENV:?} expected=\"1\" actual={actual:?}"
    )
}

fn platform_bridge_mount_tests_enabled(test_name: &str) -> bool {
    let actual = std::env::var(PLATFORM_BRIDGE_MOUNT_TESTS_ENV).ok();
    if actual.as_deref() == Some("1") {
        true
    } else {
        eprintln!(
            "{}",
            platform_bridge_mount_tests_skip_message(test_name, actual.as_deref())
        );
        false
    }
}

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

fn workspace_dotenv_var(key: &str) -> Option<String> {
    workspace_dotenv_vars().get(key).cloned()
}

fn test_online_env_var(primary: &str, legacy: &str, default: &str) -> String {
    load_test_env();
    std::env::var(primary)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| workspace_dotenv_var(primary))
        .or_else(|| workspace_dotenv_var(legacy))
        .unwrap_or_else(|| default.to_string())
}

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    load_test_env();
    let volume_name = format!("fuse-test-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("fuse-test-uuid-{volume_suffix}"),
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
    if !platform_bridge_mount_tests_enabled("fuse_session_mount_and_drop_smoke") {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mount_dir = tempfile::tempdir().expect("mount tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());
    let mut options = vec![fuser::MountOption::FSName("studiofs-test".into())];
    // On Linux, AutoUnmount in fuser v0.16 can implicitly add 'allow_other',
    // which fails on CI. Since we just drop the session here (and move to next test
    // or end), we either need it or we don't. For smoke tests, let's try to be minimal.
    // Use if cfg!() instead of #[cfg] to avoid unused_mut warnings on Linux.
    if !cfg!(target_os = "linux") {
        options.push(fuser::MountOption::AutoUnmount);
    }
    if cfg!(target_os = "macos") {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }

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
    if !platform_bridge_mount_tests_enabled("fuse_mount_write_read_unmount_smoke") {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mount_dir = tempfile::tempdir().expect("mount tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());

    let mut options = vec![fuser::MountOption::FSName("studiofs-smoke".into())];
    // Manually unmounting at the end of the test, so AutoUnmount is optional.
    // On Linux CI, AutoUnmount may trigger 'allow_other' checks in fusermount3.
    // Use if cfg!() instead of #[cfg] to avoid unused_mut warnings on Linux.
    if !cfg!(target_os = "linux") {
        options.push(fuser::MountOption::AutoUnmount);
    }
    if cfg!(target_os = "macos") {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }

    let mount_path = mount_dir.path().to_path_buf();

    // Capture the device ID before FUSE mounts over the directory.
    // When FUSE is active the kernel overlays a new device, so dev() changes.
    use std::os::unix::fs::MetadataExt;
    let pre_dev = std::fs::metadata(&mount_path)
        .expect("mount dir should exist before session creation")
        .dev();

    println!("Creating FUSE session at {:?}...", mount_path);
    let mut session = fuser::Session::new(fs, &mount_path, &options).expect("create FUSE session");
    let mut unmount = session.unmount_callable();

    // Run the FUSE event loop in a background thread.
    println!("Starting FUSE background loop...");
    let session_handle = std::thread::spawn(move || {
        if let Err(e) = session.run() {
            eprintln!("FUSE session error (may be normal during unmount): {e}");
        }
        println!("FUSE background loop exited.");
    });

    // Poll until the FUSE filesystem is overlaid (device ID diverges from pre-mount value).
    println!("Waiting for FUSE mount to become available...");
    // A plain is_ok() check would pass immediately because the temp dir already exists.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::metadata(&mount_path)
            .map(|m| m.dev() != pre_dev)
            .unwrap_or(false)
        {
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
        entries
            .iter()
            .any(|n| n == std::ffi::OsStr::new("smoke.txt")),
        "directory listing must contain the created file, got: {entries:?}"
    );

    // ── Smoke: delete ───────────────────────────────────────────────
    std::fs::remove_file(&test_file).expect("delete through FUSE mount");

    // ── Clean unmount ───────────────────────────────────────────────
    unmount.unmount().expect("FUSE unmount should succeed");
    session_handle
        .join()
        .expect("FUSE session thread should exit cleanly");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_bridge_mount_tests_skip_message_names_env_gate() {
        let message = platform_bridge_mount_tests_skip_message("smoke_case", None);

        assert!(message.contains("studiofs_smoke_skip"));
        assert!(message.contains("test=\"smoke_case\""));
        assert!(message.contains("reason=\"env gate disabled\""));
        assert!(message.contains("env=\"STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS\""));
        assert!(message.contains("expected=\"1\""));
        assert!(message.contains("actual=\"<unset>\""));
    }
}
