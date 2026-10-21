use super::*;

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::time::{Duration, Instant};

use crate::context::tests::make_test_context;
use studiofs_meta::ROOT_INO;
use studiofs_vfs::OpenFlags;
use windows::Win32::Storage::FileSystem::GetLogicalDrives;
use winfsp::host::{FileSystemHost, FileSystemParams, VolumeParams};

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_cached_handle_reloads_after_queue_overflow() {
    assert_eq!(
        std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref(),
        Ok("1"),
        "set STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 to run this native mount test"
    );
    let mount_point = std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT")
        .expect("set STUDIOFS_WINFSP_TEST_MOUNT_POINT to an unused drive, for example Z:");
    let bytes = mount_point.as_bytes();
    assert!(
        bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':',
        "mount point must be a drive letter"
    );
    // SAFETY: GetLogicalDrives takes no arguments and returns the assigned-drive mask.
    let assigned_drives = unsafe { GetLogicalDrives() };
    assert_ne!(assigned_drives, 0, "GetLogicalDrives failed");
    assert_eq!(
        assigned_drives & (1 << (bytes[0].to_ascii_uppercase() - b'A')),
        0,
        "requested WinFSP test drive is already assigned"
    );

    let (_tmp, vfs, context, queue) = make_test_context("sfs873-overflow-mounted");
    let runtime = context
        ._test_runtime
        .as_ref()
        .expect("test fixture retains its Tokio runtime")
        .clone();
    assert!(wait_until(Duration::from_secs(5), || vfs.coherence_allows_os_cache()));

    let file = vfs
        .create(ROOT_INO, b"overflow-cache.bin", 0, 0, 0o644)
        .expect("create test file");
    let original = b"old cached bytes";
    let updated = b"new cached bytes";
    let writer = vfs
        .open_file(file.ino, OpenFlags::RDWR, false)
        .expect("open initial VFS writer");
    runtime.block_on(async {
        assert_eq!(
            vfs.write_fh(writer, file.ino, 0, original)
                .await
                .expect("write initial bytes"),
            original.len() as u32
        );
        vfs.flush_fh(writer).await.expect("flush initial bytes");
        vfs.release_fh(writer)
            .await
            .expect("release initial writer");
    });

    let options =
        crate::parse_mount_options("StudioFS-SFS873-Overflow").expect("parse WinFSP mount options");
    let mut volume_params = VolumeParams::new();
    volume_params.case_sensitive_search(true);
    volume_params.case_preserved_names(true);
    volume_params.unicode_on_disk(true);
    volume_params.reparse_points(true);
    volume_params.named_streams(false);
    volume_params.read_only_volume(false);
    volume_params.post_cleanup_when_modified_only(true);
    volume_params.allow_open_in_kernel_mode(options.allow_open_in_kernel_mode);
    volume_params.max_component_length(255);
    volume_params.volume_serial_number(0x5354_4644);
    crate::apply_volume_params(&mut volume_params, &options, &mount_point)
        .expect("apply production WinFSP volume parameters");

    // Keep the fixture's invalidation queue but do not register a VFS invalidator:
    // the direct VFS overwrite below must model a lost external notification.
    let mut host = FileSystemHost::new_with_timer_async::<PreparedWinfspNotifications, 250>(
        FileSystemParams::default_params(volume_params),
        context,
    )
    .expect("create WinFSP host");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mount_manager_path = crate::mount_manager_mount_point(&mount_point)
            .expect("convert drive letter to Mount Manager path");
        host.mount(&mount_manager_path).expect("mount WinFSP host");
        host.start_with_threads(options.dispatcher_threads)
            .expect("start WinFSP host");

        let drive_root = format!("{}\\", mount_point);
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::fs::metadata(&drive_root).is_err() {
            assert!(
                Instant::now() < deadline,
                "WinFSP mount did not become ready"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        let callbacks_before_open = vfs.write_path_observability_snapshot().winfsp_callbacks;
        let path = format!("{drive_root}overflow-cache.bin");
        let mut cached_reader = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open read-only handle through WinFSP");
        let callbacks_after_open = vfs.write_path_observability_snapshot().winfsp_callbacks;
        assert_eq!(
            callbacks_after_open.os_cache_enabled_open,
            callbacks_before_open.os_cache_enabled_open + 1,
            "healthy read-only open should be admitted to the WinFSP file cache"
        );

        let mut read = Vec::new();
        cached_reader
            .read_to_end(&mut read)
            .expect("warm WinFSP cached file handle");
        assert_eq!(read, original);
        let callbacks_after_warm_read = vfs.write_path_observability_snapshot().winfsp_callbacks;
        assert!(callbacks_after_warm_read.os_cache_miss_read_callbacks > 0);
        cached_reader
            .seek(SeekFrom::Start(0))
            .expect("rewind warm handle");
        read.clear();
        cached_reader
            .read_to_end(&mut read)
            .expect("read cached bytes");
        assert_eq!(read, original);
        assert_eq!(
            vfs.write_path_observability_snapshot()
                .winfsp_callbacks
                .os_cache_miss_read_callbacks,
            callbacks_after_warm_read.os_cache_miss_read_callbacks,
            "the repeated read must be served by the Windows cache"
        );

        let writer = vfs
            .open_file(file.ino, OpenFlags::RDWR, false)
            .expect("open direct VFS overwrite handle");
        runtime.block_on(async {
            assert_eq!(
                vfs.write_fh(writer, file.ino, 0, updated)
                    .await
                    .expect("overwrite through VFS"),
                updated.len() as u32
            );
            vfs.flush_fh(writer).await.expect("flush VFS overwrite");
            vfs.release_fh(writer).await.expect("release VFS overwrite");
        });
        assert_eq!(
            runtime
                .block_on(vfs.read(file.ino, 0, updated.len() as u32))
                .expect("read current VFS bytes")
                .as_ref(),
            updated,
            "the current VFS view must contain the overwrite"
        );

        let failed_before = queue.delivery_snapshot().failed_notifications;
        let submitted_before = queue.delivery_snapshot().submitted_notifications;
        {
            let mut state = queue.state.lock();
            for ino in 10_000..10_000 + WINFSP_INVALIDATION_QUEUE_CAPACITY as u64 {
                assert!(queue.push_locked(&mut state, NotificationKey::Inode(ino), None));
            }
            assert!(
                !queue.push_locked(&mut state, NotificationKey::Inode(file.ino), None),
                "the target invalidation must be rejected from the full queue"
            );
        }
        assert_eq!(
            queue.delivery_snapshot().failed_notifications,
            failed_before + 1,
            "overflow should record the rejected target"
        );

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut observed = Vec::new();
        loop {
            cached_reader
                .seek(SeekFrom::Start(0))
                .expect("rewind the same cached file handle");
            observed.clear();
            cached_reader
                .read_to_end(&mut observed)
                .expect("read the same cached file handle after overflow");
            if observed == updated
                && queue.delivery_snapshot().submitted_notifications > submitted_before
                && vfs
                    .write_path_observability_snapshot()
                    .winfsp_callbacks
                    .os_cache_miss_read_callbacks
                    > callbacks_after_warm_read.os_cache_miss_read_callbacks
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cached handle did not reload the VFS overwrite after overflow; observed={observed:?}, delivery={:?}, read_callbacks_before={}, read_callbacks_after={}",
                queue.delivery_snapshot(),
                callbacks_after_warm_read.os_cache_miss_read_callbacks,
                vfs.write_path_observability_snapshot()
                    .winfsp_callbacks
                    .os_cache_miss_read_callbacks
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }));

    host.stop();
    host.unmount();
    drop(host);
    runtime
        .block_on(vfs.shutdown(Duration::from_secs(1)))
        .expect("shut down test VFS after unmount");
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}
