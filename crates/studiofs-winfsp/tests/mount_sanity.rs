#![cfg(windows)]

use std::net::ToSocketAddrs;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_meta::UploadState;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, InodeAttr, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};
use studiofs_winfsp::mount_and_serve;

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

fn workspace_dotenv_vars() -> &'static std::collections::HashMap<String, String> {
    static VARS: OnceLock<std::collections::HashMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = std::collections::HashMap::new();
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

fn test_online_endpoint() -> String {
    test_online_env_var(
        "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
        "STUDIOFS_S3_ENDPOINT",
        "http://127.0.0.1:9000",
    )
}

fn endpoint_authority(endpoint: &str) -> Option<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (rest, default_port) = if let Some(rest) = trimmed.strip_prefix("http://") {
        (rest, 80)
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        (rest, 443)
    } else {
        (trimmed, 80)
    };

    let authority = rest.split('/').next()?.trim();
    if authority.is_empty() {
        return None;
    }

    if authority.starts_with('[') {
        if authority.contains("]:") {
            return Some(authority.to_string());
        }
        return Some(format!("{authority}:{default_port}"));
    }

    if authority.contains(':') {
        return Some(authority.to_string());
    }

    Some(format!("{authority}:{default_port}"))
}

fn online_upload_backend_status() -> &'static (String, bool) {
    static STATUS: OnceLock<(String, bool)> = OnceLock::new();
    STATUS.get_or_init(|| {
        let endpoint = test_online_endpoint();
        let available = endpoint_authority(&endpoint)
            .and_then(|authority| {
                authority
                    .to_socket_addrs()
                    .ok()
                    .map(|addrs| addrs.take(4).collect::<Vec<_>>())
            })
            .is_some_and(|addrs| {
                addrs.iter().any(|addr| {
                    std::net::TcpStream::connect_timeout(addr, Duration::from_secs(1)).is_ok()
                })
            });
        (endpoint, available)
    })
}

fn require_online_upload_backend(test_name: &str) -> bool {
    let (endpoint, available) = online_upload_backend_status();
    if *available {
        return true;
    }

    eprintln!(
        "Skipping {test_name}: post-upload WinFSP smoke tests require a reachable S3 endpoint; resolved endpoint '{endpoint}' was not reachable."
    );
    false
}

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("winfsp-mount-test-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("winfsp-mount-test-uuid-{volume_suffix}"),
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

fn drive_root(mount_point: &str) -> String {
    if mount_point.ends_with('\\') || mount_point.ends_with('/') {
        mount_point.to_string()
    } else {
        format!("{mount_point}\\")
    }
}

fn wait_for_drive_root(drive_root: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::fs::metadata(drive_root).is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "WinFSP mount at {drive_root} did not become available within {} s",
            timeout.as_secs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn with_winfsp_mount<R, F>(
    vfs: Arc<StudioVfs>,
    rt: Arc<tokio::runtime::Runtime>,
    mount_point: String,
    event_name: String,
    body: F,
) -> R
where
    F: FnOnce(&str) -> R,
{
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

    let drive_root = drive_root(&mount_point);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for_drive_root(&drive_root, Duration::from_secs(10));
        body(&drive_root)
    }));

    signal_named_event(&event_name);
    let join_result = handle.join();

    match result {
        Ok(value) => {
            let mount_result = join_result.expect("mount thread panicked");
            mount_result.expect("WinFSP mount/unmount should complete");
            value
        }
        Err(payload) => {
            if let Ok(Err(error)) = join_result {
                eprintln!("WinFSP mount thread returned during cleanup: {error}");
            }
            std::panic::resume_unwind(payload)
        }
    }
}

fn wait_for_lookup(vfs: &StudioVfs, name: &[u8], label: &str, timeout: Duration) -> InodeAttr {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match vfs.lookup(1, name) {
            Ok(attr) => return attr,
            Err(error) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "lookup {label}: {error:?}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn wait_for_pending_upload_observable(vfs: &StudioVfs, ino: u64, label: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let current_attr = vfs.getattr(ino).expect(label);
        if !vfs.pending_uploads_for_inode(ino).is_empty()
            || current_attr.upload_state != UploadState::Ready
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label} reached Ready before pending upload state was observable"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_upload_ready(vfs: &StudioVfs, ino: u64, label: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let current_attr = vfs.getattr(ino).expect(label);
        if vfs.pending_uploads_for_inode(ino).is_empty()
            && current_attr.upload_state == UploadState::Ready
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label} did not reach published Ready state within {} s",
            timeout.as_secs()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
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
    with_winfsp_mount(vfs, rt, mount_point, event_name, |_| {});
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

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let test_file = std::path::PathBuf::from(drive_root).join("smoke.txt");
        let payload = b"StudioFS WinFSP smoke-test payload";
        std::fs::write(&test_file, payload).expect("write through WinFSP mount");

        let readback = std::fs::read(&test_file).expect("read through WinFSP mount");
        assert_eq!(readback, payload, "read-back must match written data");

        let entries: Vec<_> = std::fs::read_dir(drive_root)
            .expect("readdir through WinFSP mount")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(
            entries
                .iter()
                .any(|n| n == std::ffi::OsStr::new("smoke.txt")),
            "directory listing must contain the created file, got: {entries:?}"
        );

        std::fs::remove_file(&test_file).expect("delete through WinFSP mount");
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_callback_flush_and_cleanup_observability_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP callback observability smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "callback_observability_smoke", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsCallbackObservabilitySmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let flush_snapshot_before = vfs_observer.write_path_observability_snapshot();
        let repeated_flush_file = std::path::PathBuf::from(drive_root).join("repeated-flush.bin");
        let mut repeated = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&repeated_flush_file)
            .expect("open repeated-flush file through WinFSP mount");
        use std::io::Write;
        repeated
            .write_all(b"callback flush observability payload")
            .expect("write repeated-flush payload through WinFSP mount");
        repeated
            .sync_all()
            .expect("first sync_all through WinFSP mount");
        repeated
            .sync_all()
            .expect("second sync_all through WinFSP mount");
        drop(repeated);

        let flush_snapshot_after = vfs_observer.write_path_observability_snapshot();
        assert!(
            flush_snapshot_after.winfsp_callbacks.flush
                >= flush_snapshot_before.winfsp_callbacks.flush + 2,
            "two sync_all calls should surface at least two WinFSP flush callbacks"
        );
        assert!(
            flush_snapshot_after.winfsp_foreground_flushes.flush
                > flush_snapshot_before.winfsp_foreground_flushes.flush,
            "the first sync_all should attribute a foreground barrier to the flush callback path"
        );
        assert!(
            flush_snapshot_after
                .winfsp_foreground_flushes
                .flush_skipped_clean
                > flush_snapshot_before
                    .winfsp_foreground_flushes
                    .flush_skipped_clean,
            "the second sync_all should show a clean-skip flush callback instead of another barrier"
        );
        assert!(
            flush_snapshot_after.winfsp_callbacks.cleanup
                > flush_snapshot_before.winfsp_callbacks.cleanup,
            "closing the clean handle should still surface a cleanup callback"
        );
        assert!(
            flush_snapshot_after
                .winfsp_foreground_flushes
                .cleanup_skipped_clean
                > flush_snapshot_before
                    .winfsp_foreground_flushes
                    .cleanup_skipped_clean,
            "cleanup after explicit sync_all should be attributed as a clean skip"
        );
        assert!(
            flush_snapshot_after.winfsp_callbacks.close
                > flush_snapshot_before.winfsp_callbacks.close,
            "closing the repeated-flush handle should surface a close callback"
        );

        let close_snapshot_before = vfs_observer.write_path_observability_snapshot();
        let dirty_close_file = std::path::PathBuf::from(drive_root).join("dirty-close.bin");
        let mut dirty = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&dirty_close_file)
            .expect("open dirty-close file through WinFSP mount");
        dirty
            .write_all(b"dirty close payload")
            .expect("write dirty-close payload through WinFSP mount");
        drop(dirty);

        let close_snapshot_after = vfs_observer.write_path_observability_snapshot();
        assert!(
            close_snapshot_after.winfsp_callbacks.cleanup
                > close_snapshot_before.winfsp_callbacks.cleanup,
            "dropping a dirty writer should surface a cleanup callback"
        );
        assert!(
            close_snapshot_after.winfsp_foreground_flushes.cleanup
                > close_snapshot_before.winfsp_foreground_flushes.cleanup,
            "dirty-handle close should attribute its barrier to cleanup"
        );
        assert!(
            close_snapshot_after.winfsp_callbacks.close
                > close_snapshot_before.winfsp_callbacks.close,
            "dropping a dirty writer should still surface a close callback"
        );
        assert!(
            close_snapshot_after
                .winfsp_foreground_flushes
                .close_skipped_clean
                > close_snapshot_before
                    .winfsp_foreground_flushes
                    .close_skipped_clean,
            "close should see the inode as clean after cleanup already drained the dirty state"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_reopen_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend("winfsp_mount_reopen_read_after_upload_finalize_smoke") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP post-upload read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_read", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!("Local\\StudioFsPostUploadReadSmoke-{}", std::process::id());

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let test_file = std::path::PathBuf::from(drive_root).join("post-upload.bin");
        let payload = vec![0xA7u8; 12 * 1024 * 1024];
        std::fs::write(&test_file, &payload).expect("write through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"post-upload.bin",
            "written file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "WinFSP file",
            Duration::from_secs(30),
        );

        let reopened = std::fs::read(&test_file).expect("re-open read through WinFSP mount");
        assert_eq!(
            reopened, payload,
            "post-upload read-back must match written data"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_copy_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend("winfsp_mount_copy_read_after_upload_finalize_smoke") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP post-upload copy-read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let source_file = tmp.path().join("copy-source.bin");
    let source_payload: Vec<u8> = (0..(20 * 1024 * 1024))
        .map(|index| ((index as u64).wrapping_mul(11400714819323198485) >> 33) as u8)
        .collect();
    std::fs::write(&source_file, &source_payload).expect("write local source file");

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_copy_read", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsPostUploadCopyReadSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file = std::path::PathBuf::from(drive_root).join("copied-post-upload.bin");
        std::fs::copy(&source_file, &target_file).expect("copy through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"copied-post-upload.bin",
            "copied file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "copied WinFSP file",
            Duration::from_secs(45),
        );

        let reopened =
            std::fs::read(&target_file).expect("re-open read copied file through WinFSP mount");
        assert_eq!(
            reopened, source_payload,
            "copied file post-upload read-back must match source payload"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_set_len_chunked_write_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend(
        "winfsp_mount_set_len_chunked_write_read_after_upload_finalize_smoke",
    ) {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP set-len chunked-write smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let payload: Vec<u8> = (0..(17 * 1024 * 1024 + 37_777))
        .map(|index| (index as u8).wrapping_mul(19).wrapping_add(0x5D))
        .collect();
    let chunk_pattern = [65_553usize, 131_069, 33_791, 1_048_581, 524_311, 262_147];

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_chunked_write", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsPostUploadChunkedWriteSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file = std::path::PathBuf::from(drive_root).join("set-len-chunked.bin");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&target_file)
            .expect("open chunked target file through WinFSP mount");
        file.set_len(payload.len() as u64)
            .expect("pre-extend chunked target file through WinFSP mount");

        let mut offset = 0usize;
        let mut chunk_index = 0usize;
        while offset < payload.len() {
            let chunk_len =
                chunk_pattern[chunk_index % chunk_pattern.len()].min(payload.len() - offset);
            use std::io::{Seek, SeekFrom, Write};
            file.seek(SeekFrom::Start(offset as u64))
                .expect("seek chunked target file");
            file.write_all(&payload[offset..offset + chunk_len])
                .expect("write chunk through WinFSP mount");
            offset += chunk_len;
            chunk_index += 1;
        }
        file.sync_all()
            .expect("sync chunked target file through WinFSP mount");
        drop(file);

        let attr = wait_for_lookup(
            &vfs_observer,
            b"set-len-chunked.bin",
            "chunked target file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "chunked WinFSP file",
            Duration::from_secs(45),
        );

        let reopened = std::fs::read(&target_file)
            .expect("re-open read chunked file through WinFSP mount after publish");
        assert_eq!(
            reopened, payload,
            "chunked file post-upload read-back must match written payload"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_same_handle_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend("winfsp_mount_same_handle_read_after_upload_finalize_smoke") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP same-handle post-upload read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let payload: Vec<u8> = (0..(14 * 1024 * 1024 + 8191))
        .map(|index| (index as u8).wrapping_mul(7).wrapping_add(0x33))
        .collect();

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_same_handle", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsPostUploadSameHandleSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file = std::path::PathBuf::from(drive_root).join("same-handle.bin");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&target_file)
            .expect("open same-handle file through WinFSP mount");
        use std::io::{Read, Seek, SeekFrom, Write};
        file.write_all(&payload)
            .expect("write same-handle payload through WinFSP mount");
        file.sync_all()
            .expect("sync same-handle file through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"same-handle.bin",
            "same-handle file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "same-handle WinFSP file",
            Duration::from_secs(45),
        );

        file.seek(SeekFrom::Start(0))
            .expect("seek same-handle file back to start");
        let mut same_handle_read = Vec::new();
        file.read_to_end(&mut same_handle_read)
            .expect("read same-handle file after publish");
        assert_eq!(
            same_handle_read, payload,
            "same-handle post-upload read-back must match written payload"
        );
        drop(file);
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_reader_open_during_upload_stays_readable_after_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend(
        "winfsp_mount_reader_open_during_upload_stays_readable_after_finalize_smoke",
    ) {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP reader-open-during-upload smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let payload: Vec<u8> = (0..(24 * 1024 * 1024 + 123_457))
        .map(|index| (index as u8).wrapping_mul(23).wrapping_add(0x41))
        .collect();

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "reader_open_during_upload", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsReaderOpenDuringUploadSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file =
            std::path::PathBuf::from(drive_root).join("reader-open-during-upload.bin");
        let mut writer = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&target_file)
            .expect("open writer file through WinFSP mount");
        use std::io::{Read, Seek, SeekFrom, Write};
        writer
            .write_all(&payload)
            .expect("write payload through WinFSP mount");
        writer
            .sync_all()
            .expect("sync writer file through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"reader-open-during-upload.bin",
            "reader-open-during-upload file",
            Duration::from_secs(5),
        );
        wait_for_pending_upload_observable(
            &vfs_observer,
            attr.ino,
            "reader-open-during-upload file",
            Duration::from_secs(5),
        );

        let mut reader = std::fs::File::open(&target_file)
            .expect("open reader handle during pending upload through WinFSP mount");
        let mut prefix = vec![0u8; 256 * 1024];
        reader
            .read_exact(&mut prefix)
            .expect("read prefix through reader handle during pending upload");
        assert_eq!(
            prefix,
            payload[..prefix.len()],
            "reader handle must observe correct prefix while upload is still pending"
        );

        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "reader-open-during-upload file",
            Duration::from_secs(45),
        );

        reader
            .seek(SeekFrom::Start(0))
            .expect("seek reader handle back to start after publish");
        let mut reopened_from_existing_reader = Vec::new();
        reader
            .read_to_end(&mut reopened_from_existing_reader)
            .expect("read through reader handle after publish");
        assert_eq!(
            reopened_from_existing_reader, payload,
            "reader handle opened during pending upload must remain readable after publish"
        );

        drop(reader);
        drop(writer);
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_mmap_reader_during_upload_stays_readable_after_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend(
        "winfsp_mount_mmap_reader_during_upload_stays_readable_after_finalize_smoke",
    ) {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP mmap-reader-during-upload smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let payload: Vec<u8> = (0..(18 * 1024 * 1024 + 65_537))
        .map(|index| (index as u8).wrapping_mul(29).wrapping_add(0x17))
        .collect();

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mmap_reader_during_upload", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsMmapReaderDuringUploadSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file =
            std::path::PathBuf::from(drive_root).join("mmap-reader-during-upload.bin");
        std::fs::write(&target_file, &payload).expect("write mmap target through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"mmap-reader-during-upload.bin",
            "mmap-reader-during-upload file",
            Duration::from_secs(5),
        );
        wait_for_pending_upload_observable(
            &vfs_observer,
            attr.ino,
            "mmap-reader-during-upload file",
            Duration::from_secs(5),
        );

        use std::os::windows::io::AsRawHandle;
        use windows::Win32::Foundation::{CloseHandle, HANDLE};
        use windows::Win32::System::Memory::{
            CreateFileMappingW, FILE_MAP_READ, MapViewOfFile, PAGE_READONLY, UnmapViewOfFile,
        };

        let reader = std::fs::File::open(&target_file)
            .expect("open mmap reader handle during pending upload through WinFSP mount");
        let mapping = unsafe {
            CreateFileMappingW(
                HANDLE(reader.as_raw_handle()),
                None,
                PAGE_READONLY,
                0,
                0,
                None,
            )
        }
        .expect("create read-only file mapping through WinFSP mount");
        let view = unsafe { MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, payload.len()) };
        assert!(
            !view.Value.is_null(),
            "MapViewOfFile returned a null view for WinFSP mmap smoke"
        );

        let mapped = unsafe { std::slice::from_raw_parts(view.Value as *const u8, payload.len()) };
        assert_eq!(
            &mapped[..256 * 1024],
            &payload[..256 * 1024],
            "memory-mapped reader must observe correct prefix while upload is still pending"
        );

        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "mmap-reader-during-upload file",
            Duration::from_secs(45),
        );

        std::thread::sleep(Duration::from_millis(1500));

        assert_eq!(
            mapped, payload,
            "memory-mapped reader opened during pending upload must remain readable after publish"
        );

        unsafe {
            UnmapViewOfFile(view).expect("unmap WinFSP mmap view");
            CloseHandle(mapping).expect("close WinFSP mmap mapping handle");
        }
        drop(reader);
    });
}
