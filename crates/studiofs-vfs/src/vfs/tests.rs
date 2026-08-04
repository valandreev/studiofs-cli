use super::namespace_ops::NAMESPACE_INODE_BATCH;
use super::*;
use crate::contract_harness::{ContractVfsFixture, ContractVfsOp};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::thread;
use std::time::Duration;
use studiofs_cache::CachedStoreWriterConfig;
use studiofs_meta::engine::MetadataAuthorityStatus;
use studiofs_meta::engine::{MetaEngineCore, MetaInodes, MetaPayloadObjects};
use studiofs_meta::schema::{
    CURRENT_FORMAT_VERSION, ChunkId, DEFAULT_BLOCK_SIZE, FormatInfo, InodeLifecycleState,
    MetadataConfig, PayloadRefState, PayloadRetentionClass, ROOT_INO, SliceId, SliceOrder,
    SliceRange, SliceRecord, TimeSpec,
};
use studiofs_meta::{
    FileLockLease, HardLockKind, MetaEngine, MetaFormat, MetaFsOps, MetaNamespace, RedbMetaEngine,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

#[test]
fn os_cache_prepare_unknown_handle_is_conservative() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-unknown-handle");

    assert_eq!(
        vfs.prepare_os_cache_open(u64::MAX, OsCacheParticipation::Enabled),
        OsCacheOpenDecision::replace(OsCacheReason::UnknownHandle)
    );
}

#[test]
fn read_ahead_owner_unknown_handle_is_conservative() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "read-ahead-owner-unknown-handle");

    assert_eq!(vfs.read_ahead_owner(u64::MAX), None);
    assert!(!vfs.set_read_ahead_owner(u64::MAX, ReadAheadOwner::Kernel));
}

#[test]
fn read_ahead_owner_is_handle_scoped_for_same_inode() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "read-ahead-owner-handle-scoped");
    let attr = vfs
        .create(ROOT_INO, b"owners.txt", 1000, 1000, 0o644)
        .expect("create");

    let kernel_fh = vfs
        .open_file_with_read_ahead_owner(attr.ino, OpenFlags::RDONLY, false, ReadAheadOwner::Kernel)
        .expect("kernel-owned open");
    let vfs_fh = vfs
        .open_file(attr.ino, OpenFlags::RDONLY, false)
        .expect("VFS-owned open");

    assert_eq!(
        vfs.read_ahead_owner(kernel_fh),
        Some(ReadAheadOwner::Kernel)
    );
    assert_eq!(vfs.read_ahead_owner(vfs_fh), Some(ReadAheadOwner::Vfs));

    assert!(vfs.set_read_ahead_owner(vfs_fh, ReadAheadOwner::Kernel));
    assert_eq!(
        vfs.read_ahead_owner(kernel_fh),
        Some(ReadAheadOwner::Kernel)
    );
}

#[test]
fn prepared_kernel_owner_skips_eager_open_prefetch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "read-ahead-owner-eager-prefetch");
    let attr = vfs
        .create(ROOT_INO, b"prefetch.txt", 1000, 1000, 0o644)
        .expect("create");
    vfs.setattr(attr.ino, SetattrArgs::size_only(4096))
        .expect("give file non-zero size");
    let queued_prefetch = || {
        vfs.startup
            .lifecycle
            .lock()
            .channels
            .as_ref()
            .expect("unstarted VFS retains startup channels")
            .prefetch_rx
            .len()
    };
    let before = queued_prefetch();

    vfs.open_file_with_read_ahead_owner(attr.ino, OpenFlags::RDONLY, false, ReadAheadOwner::Kernel)
        .expect("kernel-owned open");
    assert_eq!(
        queued_prefetch(),
        before,
        "kernel-owned open must not schedule eager VFS prefetch"
    );

    vfs.open_file(attr.ino, OpenFlags::RDONLY, false)
        .expect("default VFS-owned open");
    assert_eq!(
        queued_prefetch(),
        before + 1,
        "existing open API must preserve eager VFS prefetch"
    );
}

#[test]
fn kernel_to_vfs_owner_transition_queues_eager_prefetch_exactly_once() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "read-ahead-owner-transition-prefetch");
    let attr = vfs
        .create(ROOT_INO, b"transition-prefetch.txt", 1000, 1000, 0o644)
        .expect("create");
    vfs.setattr(attr.ino, SetattrArgs::size_only(4096))
        .expect("give file non-zero size");
    let fh = vfs
        .open_file_with_read_ahead_owner(attr.ino, OpenFlags::RDONLY, false, ReadAheadOwner::Kernel)
        .expect("kernel-owned open");
    let queued_prefetch = || {
        vfs.startup
            .lifecycle
            .lock()
            .channels
            .as_ref()
            .expect("unstarted VFS retains startup channels")
            .prefetch_rx
            .len()
    };
    let before = queued_prefetch();

    assert!(vfs.set_read_ahead_owner(fh, ReadAheadOwner::Vfs));
    assert_eq!(queued_prefetch(), before + 1);

    assert!(vfs.set_read_ahead_owner(fh, ReadAheadOwner::Vfs));
    assert_eq!(
        queued_prefetch(),
        before + 1,
        "Vfs-to-Vfs owner assignment must not duplicate eager prefetch"
    );

    assert!(vfs.set_read_ahead_owner(fh, ReadAheadOwner::Kernel));
    assert_eq!(
        queued_prefetch(),
        before + 1,
        "switching back to kernel ownership must not queue VFS prefetch"
    );

    let mut lifecycle = vfs.startup.lifecycle.lock();
    let queued = lifecycle
        .channels
        .as_mut()
        .expect("unstarted VFS retains startup channels")
        .prefetch_rx
        .try_recv()
        .expect("Kernel-to-VFS transition queued one batch");
    let PrefetchWorkItem::Batch(batch) = queued else {
        panic!("eager transition must queue a batch");
    };
    assert!(
        batch.owner_token.is_some(),
        "transition prefetch must remain scoped to the live VFS owner generation"
    );
}

#[test]
fn runtime_stats_owner_transition_never_queues_eager_prefetch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) =
        make_unstarted_test_vfs(&tmp, "runtime-stats-owner-transition-prefetch");
    let queued_prefetch = || {
        vfs.startup
            .lifecycle
            .lock()
            .channels
            .as_ref()
            .expect("unstarted VFS retains startup channels")
            .prefetch_rx
            .len()
    };
    let before = queued_prefetch();
    let fh = vfs
        .open_file_with_read_ahead_owner(
            RUNTIME_STATS_FILE_INO,
            OpenFlags::RDONLY,
            false,
            ReadAheadOwner::Kernel,
        )
        .expect("open runtime stats with provisional owner");

    assert!(vfs.set_read_ahead_owner(fh, ReadAheadOwner::Vfs));
    assert_eq!(
        queued_prefetch(),
        before,
        "dynamic runtime stats must bypass generic eager prefetch"
    );

    vfs.open_file(RUNTIME_STATS_FILE_INO, OpenFlags::RDONLY, false)
        .expect("open runtime stats with VFS owner");
    assert_eq!(
        queued_prefetch(),
        before,
        "runtime stats must also bypass eager prefetch on an initial VFS owner"
    );
}

#[test]
fn os_cache_clean_read_first_replaces_then_reuses_after_release() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-clean-reuse");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"cache.txt", 1000, 1000, 0o644)
            .expect("create");

        let first_fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("first open");
        assert_eq!(
            vfs.prepare_os_cache_open(first_fh, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        assert_eq!(
            vfs.prepare_os_cache_open(first_fh, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation),
            "preparation must be idempotent for one handle"
        );
        vfs.release_fh(first_fh).await.expect("first release");

        let second_fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("second open");
        assert_eq!(
            vfs.prepare_os_cache_open(second_fh, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::reuse(OsCacheReason::StableGeneration)
        );
        vfs.release_fh(second_fh).await.expect("second release");
    });
}

#[test]
fn os_cache_writer_blocks_reads_until_every_writer_is_released() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-writer-lifecycle");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"writer.txt", 1000, 1000, 0o644)
            .expect("create");
        let seed_fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("seed open");
        assert_eq!(
            vfs.prepare_os_cache_open(seed_fh, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        vfs.release_fh(seed_fh).await.expect("seed release");

        let writer_a = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer A");
        let writer_b = vfs
            .open_file(attr.ino, OpenFlags::WRONLY, false)
            .expect("writer B");
        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader");
        assert_eq!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        vfs.release_fh(reader).await.expect("reader release");
        vfs.release_fh(writer_a).await.expect("writer A release");

        let still_blocked = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("still-blocked reader");
        assert_eq!(
            vfs.prepare_os_cache_open(still_blocked, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        vfs.release_fh(still_blocked)
            .await
            .expect("still-blocked reader release");
        vfs.release_fh(writer_b).await.expect("writer B release");

        let clean_reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("clean reader");
        assert_eq!(
            vfs.prepare_os_cache_open(clean_reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        vfs.release_fh(clean_reader)
            .await
            .expect("clean reader release");
    });
}

#[test]
fn os_cache_abort_rolls_back_generation_and_writer_lifecycle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-abort");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"abort.txt", 1000, 1000, 0o644)
            .expect("create");
        let rejected = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("rejected read open");
        assert_eq!(
            vfs.prepare_os_cache_open(rejected, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        vfs.abort_os_cache_open(rejected);
        vfs.release_fh(rejected)
            .await
            .expect("rejected read release");

        let retry = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("retry read open");
        assert_eq!(
            vfs.prepare_os_cache_open(retry, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        vfs.release_fh(retry).await.expect("retry read release");

        let rejected_writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("rejected writer open");
        assert_eq!(
            vfs.prepare_os_cache_open(rejected_writer, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::WriteCapable)
        );
        vfs.abort_os_cache_open(rejected_writer);

        let blocked_reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader while rejected writer awaits release");
        assert_eq!(
            vfs.prepare_os_cache_open(blocked_reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        vfs.release_fh(blocked_reader)
            .await
            .expect("blocked reader release");
        vfs.release_fh(rejected_writer)
            .await
            .expect("rejected writer release");

        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader after rejected writer");
        assert_ne!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled)
                .reason(),
            OsCacheReason::ActiveWriter
        );
        vfs.release_fh(reader).await.expect("reader release");
    });
}

#[test]
fn os_cache_last_writer_remains_active_through_release_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-release-barrier");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"barrier.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer");
        let gate = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Write)
            .await;
        let releasing_vfs = Arc::clone(&vfs);
        let release = tokio::spawn(async move { releasing_vfs.release_fh(writer).await });

        for _ in 0..1_000 {
            if vfs.get_open_file(writer).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            vfs.get_open_file(writer).is_none(),
            "release must close admission before waiting on the inode barrier"
        );

        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader during release barrier");
        assert_eq!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );

        drop(gate);
        release
            .await
            .expect("release task")
            .expect("writer release");
        vfs.release_fh(reader).await.expect("reader release");
    });
}

#[test]
fn os_cache_reports_write_non_regular_dirty_and_unknown_reasons() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-reasons");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"reasons.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer");
        assert_eq!(
            vfs.prepare_os_cache_open(writer, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::WriteCapable)
        );
        vfs.abort_os_cache_open(writer);
        vfs.release_fh(writer).await.expect("writer release");

        let dirty_reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("dirty reader");
        let mut dirty_attr = vfs.getattr(attr.ino).expect("load attr for dirty state");
        dirty_attr.mtime = InodeAttr::now_nanos();
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, dirty_attr);
        assert_eq!(
            vfs.prepare_os_cache_open(dirty_reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::LocalDirty)
        );
        vfs.release_fh(dirty_reader)
            .await
            .expect("dirty reader release");

        let unknown = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("unknown-state reader");
        *vfs.get_open_file(unknown)
            .expect("open handle")
            .open_attr
            .write() = None;
        assert_eq!(
            vfs.prepare_os_cache_open(unknown, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::UnknownState)
        );
        vfs.release_fh(unknown).await.expect("unknown release");

        let directory = vfs
            .mkdir(ROOT_INO, b"directory", 1000, 1000, 0o755)
            .expect("mkdir");
        let directory_fh = vfs
            .open_file(directory.ino, OpenFlags::RDONLY, false)
            .expect("open directory");
        assert_eq!(
            vfs.prepare_os_cache_open(directory_fh, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::NonRegular)
        );
        vfs.release_fh(directory_fh)
            .await
            .expect("directory release");
    });
}

#[test]
fn os_cache_writer_never_enrolls_as_foreground_read_handle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-writer-foreground");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"foreground-writer.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer");
        let foreground_read_handles = vfs.os_cache.lock().active_foreground_handle_count();
        assert!(
            !vfs.foreground_io_active(),
            "legacy adapters that do not call the seam must retain prior QoS behavior"
        );
        assert!(
            vfs.wait_for_foreground_reads_idle(Duration::from_millis(10))
                .await,
            "an unprepared writer is not a foreground read"
        );

        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader");
        assert_eq!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter),
            "unprepared writer must still block cache reuse"
        );
        vfs.release_fh(reader).await.expect("reader release");
        assert!(
            !vfs.foreground_io_active(),
            "the prepared reader lifetime ended at release"
        );

        assert_eq!(
            vfs.prepare_os_cache_open(writer, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::WriteCapable)
        );
        assert_eq!(
            vfs.os_cache.lock().active_foreground_handle_count(),
            foreground_read_handles,
            "prepared writers must not inflate the read-handle count"
        );
        assert!(
            vfs.wait_for_foreground_reads_idle(Duration::from_millis(10))
                .await,
            "a prepared writer is still not a foreground read"
        );
        vfs.write_fh(writer, attr.ino, 0, b"writer activity")
            .await
            .expect("writer activity");
        assert!(
            vfs.foreground_io_active(),
            "ordinary writer activity must retain the existing upload/QoS signal"
        );
        assert!(
            vfs.wait_for_foreground_reads_idle(Duration::from_millis(10))
                .await,
            "writer activity must not delay the read-only idle barrier"
        );
        vfs.release_fh(writer).await.expect("writer release");
        assert!(!vfs.foreground_io_active());
    });
}

#[test]
fn os_cache_final_unlink_retires_generation_with_and_without_open_handle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-final-unlink");
    rt.block_on(async move {
        for (name, keep_open) in [
            (b"closed.txt".as_slice(), false),
            (b"open.txt".as_slice(), true),
        ] {
            let attr = vfs
                .create(ROOT_INO, name, 1000, 1000, 0o644)
                .expect("create");
            let seed = vfs
                .open_file(attr.ino, OpenFlags::RDONLY, false)
                .expect("seed open");
            let _ = vfs.prepare_os_cache_open(seed, OsCacheParticipation::Enabled);
            vfs.release_fh(seed).await.expect("seed release");
            assert!(vfs.os_cache.lock().has_generation(attr.ino));

            let surviving_handle = keep_open.then(|| {
                vfs.open_file(attr.ino, OpenFlags::RDONLY, false)
                    .expect("surviving open handle")
            });
            vfs.unlink(ROOT_INO, name).expect("final unlink");
            assert!(
                !vfs.os_cache.lock().has_generation(attr.ino),
                "final unlink must synchronously retire the generation"
            );
            if let Some(fh) = surviving_handle {
                vfs.release_fh(fh).await.expect("surviving release");
            }
        }
    });
}

#[test]
fn os_cache_unlink_preserves_generation_when_hardlink_survives() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-hardlink-unlink");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"original.txt", 1000, 1000, 0o644)
            .expect("create");
        vfs.link(attr.ino, ROOT_INO, b"alias.txt")
            .expect("hardlink");
        let seed = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("seed open");
        let _ = vfs.prepare_os_cache_open(seed, OsCacheParticipation::Enabled);
        vfs.release_fh(seed).await.expect("seed release");

        vfs.unlink(ROOT_INO, b"original.txt")
            .expect("unlink one hardlink");

        assert!(vfs.os_cache.lock().has_generation(attr.ino));
    });
}

#[test]
fn hardlink_preserves_volatile_local_size_and_updates_link_count() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "hardlink-unflushed-local-size");
    let runtime = rt.handle().clone();
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"original.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::WRONLY, false)
            .expect("writer");
        vfs.write_fh(writer, attr.ino, 0, b"namespace")
            .await
            .expect("write");
        vfs.flush_volatile_writeback_with_outcome(attr.ino, writer)
            .await
            .expect("volatile flush");
        let gate = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Write)
            .await;
        let release = vfs.release_fh_with_deferred_last_handle_barrier(writer, &runtime);
        assert!(release.closed_last_handle);
        let visible_attr = vfs.getattr(attr.ino).expect("visible local attr");
        assert_eq!(visible_attr.size, b"namespace".len() as u64);
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);

        vfs.rename(ROOT_INO, b"original.txt", ROOT_INO, b"renamed.txt")
            .expect("rename");
        vfs.state.namespace.attr_read_cache.lock().put(
            attr.ino,
            visible_attr,
            ATTR_CACHE_TTL,
            Instant::now(),
        );
        assert_eq!(
            vfs.getattr(attr.ino).expect("visible local attr").size,
            b"namespace".len() as u64
        );

        let linking_vfs = Arc::clone(&vfs);
        let link =
            tokio::task::spawn_blocking(move || linking_vfs.link(attr.ino, ROOT_INO, b"alias.txt"));
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !link.is_finished(),
            "hardlink must serialize with the in-flight release mutation"
        );
        drop(gate);
        let linked = link
            .await
            .expect("hardlink task")
            .expect("hardlink mutation");
        assert_eq!(linked.size, b"namespace".len() as u64);
        assert_eq!(linked.nlink, 2);

        vfs.wait_for_deferred_release_inflight(attr.ino).await;
        assert_eq!(
            vfs.getattr(attr.ino).expect("settled linked attr").size,
            b"namespace".len() as u64
        );
    });
}

#[test]
fn concurrent_hardlinks_do_not_regress_visible_link_count() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "concurrent-hardlink-count");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
            .expect("create");
        let start = Arc::new(std::sync::Barrier::new(3));
        let first_vfs = Arc::clone(&vfs);
        let first_start = Arc::clone(&start);
        let first = std::thread::spawn(move || {
            first_start.wait();
            first_vfs.link(attr.ino, ROOT_INO, b"first.txt")
        });
        let second_vfs = Arc::clone(&vfs);
        let second_start = Arc::clone(&start);
        let second = std::thread::spawn(move || {
            second_start.wait();
            second_vfs.link(attr.ino, ROOT_INO, b"second.txt")
        });
        start.wait();

        let first = first.join().expect("first link task").expect("first link");
        let second = second
            .join()
            .expect("second link task")
            .expect("second link");
        let mut observed_counts = [first.nlink, second.nlink];
        observed_counts.sort_unstable();
        assert_eq!(observed_counts, [2, 3]);
        assert_eq!(vfs.getattr(attr.ino).expect("final attr").nlink, 3);
    });
}

#[test]
fn os_cache_rename_overwrite_retires_only_truly_displaced_inode() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-rename-overwrite");
    rt.block_on(async move {
        let retired = vfs
            .create(ROOT_INO, b"retired-target.txt", 1000, 1000, 0o644)
            .expect("retired target");
        let source = vfs
            .create(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
            .expect("source");
        let seed = vfs
            .open_file(retired.ino, OpenFlags::RDONLY, false)
            .expect("seed retired");
        let _ = vfs.prepare_os_cache_open(seed, OsCacheParticipation::Enabled);
        vfs.release_fh(seed).await.expect("seed release");

        vfs.rename(ROOT_INO, b"source.txt", ROOT_INO, b"retired-target.txt")
            .expect("rename overwrite");
        assert!(!vfs.os_cache.lock().has_generation(retired.ino));
        assert_eq!(
            vfs.lookup(ROOT_INO, b"retired-target.txt")
                .expect("moved source")
                .ino,
            source.ino
        );

        let open_retired = vfs
            .create(ROOT_INO, b"open-retired-target.txt", 1000, 1000, 0o644)
            .expect("open retired target");
        vfs.create(ROOT_INO, b"open-source.txt", 1000, 1000, 0o644)
            .expect("open target source");
        let open_target_fh = vfs
            .open_file(open_retired.ino, OpenFlags::RDONLY, false)
            .expect("open displaced target");
        let _ = vfs.prepare_os_cache_open(open_target_fh, OsCacheParticipation::Enabled);
        vfs.rename(
            ROOT_INO,
            b"open-source.txt",
            ROOT_INO,
            b"open-retired-target.txt",
        )
        .expect("rename over open final target");
        assert!(
            !vfs.os_cache.lock().has_generation(open_retired.ino),
            "a final displaced name retires generation even while a handle keeps the inode live"
        );
        let _ = vfs.release_fh(open_target_fh).await;

        let surviving = vfs
            .create(ROOT_INO, b"surviving-target.txt", 1000, 1000, 0o644)
            .expect("surviving target");
        vfs.link(surviving.ino, ROOT_INO, b"surviving-alias.txt")
            .expect("target hardlink");
        vfs.create(ROOT_INO, b"second-source.txt", 1000, 1000, 0o644)
            .expect("second source");
        let seed = vfs
            .open_file(surviving.ino, OpenFlags::RDONLY, false)
            .expect("seed surviving");
        let _ = vfs.prepare_os_cache_open(seed, OsCacheParticipation::Enabled);
        vfs.release_fh(seed).await.expect("seed release");

        vfs.rename(
            ROOT_INO,
            b"second-source.txt",
            ROOT_INO,
            b"surviving-target.txt",
        )
        .expect("rename over one hardlink");
        assert!(
            vfs.os_cache.lock().has_generation(surviving.ino),
            "displaced name must not retire an inode still reachable by another hardlink"
        );
    });
}

#[test]
fn os_cache_release_wakes_foreground_idle_waiter_promptly() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-idle-notify");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"idle.txt", 1000, 1000, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("open");
        let _ = vfs.prepare_os_cache_open(fh, OsCacheParticipation::Enabled);
        let waiting_vfs = Arc::clone(&vfs);
        let waiter = tokio::spawn(async move {
            waiting_vfs
                .wait_for_foreground_reads_idle(Duration::from_secs(5))
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !waiter.is_finished(),
            "a prepared read handle must keep the read-idle barrier waiting"
        );

        vfs.release_fh(fh).await.expect("release");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), waiter)
                .await
                .expect("OS-cache release should notify promptly")
                .expect("waiter task"),
        );
    });
}

#[test]
fn os_cache_unprepared_truncating_handle_blocks_reader_reuse() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-trunc-writer");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"truncate.txt", 1000, 1000, 0o644)
            .expect("create");
        let truncating = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, true)
            .expect("truncating open");
        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader");
        assert_eq!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        vfs.release_fh(reader).await.expect("reader release");
        vfs.release_fh(truncating)
            .await
            .expect("truncating release");
    });
}

#[test]
fn os_cache_disabled_read_tracks_foreground_lifetime_until_release() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-disabled-lifetime");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"disabled.txt", 1000, 1000, 0o644)
            .expect("create");
        let baseline = vfs.os_cache.lock().active_foreground_handle_count();
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("open");
        assert_eq!(
            vfs.prepare_os_cache_open(fh, OsCacheParticipation::Disabled),
            OsCacheOpenDecision::replace(OsCacheReason::Disabled)
        );
        assert_eq!(
            vfs.os_cache.lock().active_foreground_handle_count(),
            baseline + 1
        );
        vfs.release_fh(fh).await.expect("release");
        assert_eq!(
            vfs.os_cache.lock().active_foreground_handle_count(),
            baseline
        );
    });
}

#[test]
fn os_cache_deferred_release_retains_writer_until_background_barrier_finishes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-deferred-release");
    let runtime = rt.handle().clone();
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"deferred.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer");
        vfs.write_fh(writer, attr.ino, 0, b"dirty")
            .await
            .expect("write");
        let gate = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Write)
            .await;
        let outcome = vfs.release_fh_with_deferred_last_handle_barrier(writer, &runtime);
        assert!(outcome.closed_last_handle);

        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader during deferred barrier");
        assert_eq!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        drop(gate);
        vfs.wait_for_deferred_release_inflight(attr.ino).await;
        vfs.release_fh(reader).await.expect("reader release");
    });
}

#[test]
fn os_cache_cancelled_release_drops_writer_lifecycle_exactly_once() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-cancelled-release");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"cancelled.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer");
        let gate = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Write)
            .await;
        let releasing_vfs = Arc::clone(&vfs);
        let release = tokio::spawn(async move { releasing_vfs.release_fh(writer).await });
        for _ in 0..1_000 {
            if vfs.get_open_file(writer).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        release.abort();
        let _ = release.await;

        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader after cancellation");
        assert_ne!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled)
                .reason(),
            OsCacheReason::ActiveWriter
        );
        drop(gate);
        vfs.release_fh(reader).await.expect("reader release");
    });
}

#[test]
fn os_cache_release_error_still_drops_writer_lifecycle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "os-cache-release-error");
    rt.block_on(async move {
        let attr = vfs
            .create(ROOT_INO, b"error.txt", 1000, 1000, 0o644)
            .expect("create");
        let writer = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("writer");
        vfs.write_fh(writer, attr.ino, 0, b"dirty")
            .await
            .expect("write");
        vfs.mark_upload_sticky_error(
            attr.ino,
            &VfsError::Other("simulated release failure".into()),
        );
        vfs.release_fh(writer)
            .await
            .expect_err("sticky error must fail release");

        let reader = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("reader after release error");
        assert_ne!(
            vfs.prepare_os_cache_open(reader, OsCacheParticipation::Enabled)
                .reason(),
            OsCacheReason::ActiveWriter
        );
        vfs.clear_upload_sticky_error(attr.ino);
        vfs.release_fh(reader).await.expect("reader release");
    });
}

#[tokio::test]
async fn fetch_warmup_shutdown_cancels_in_flight_probe() {
    let shutdown = CancellationToken::new();
    let completed = Arc::new(AtomicBool::new(false));
    let completed_by_warmup = completed.clone();
    let task = tokio::spawn(run_fetch_warmup_until_shutdown(
        shutdown.clone(),
        async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            completed_by_warmup.store(true, AtomicOrdering::Release);
        },
    ));

    tokio::task::yield_now().await;
    shutdown.cancel();

    tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .expect("shutdown must cancel an in-flight warmup probe")
        .expect("warmup task must not panic");
    assert!(
        !completed.load(AtomicOrdering::Acquire),
        "cancellation must drop the in-flight warmup operation"
    );
}

#[test]
fn rmdir_waits_for_inflight_child_unlink_before_reporting_not_empty() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "rmdir-inflight-child-unlink");
    rt.block_on(async move {
        let parent = vfs
            .mkdir(ROOT_INO, b"parent", 1000, 1000, 0o755)
            .expect("create parent directory");
        let child = vfs
            .create(parent.ino, b"child.txt", 1000, 1000, 0o644)
            .expect("create child file");

        let child_delete_gate = vfs
            .state
            .file_session
            .enter_op(child.ino, FileSessionOperation::Delete)
            .await;
        let unlink_vfs = vfs.clone();
        let unlink =
            tokio::task::spawn_blocking(move || unlink_vfs.unlink(parent.ino, b"child.txt"));
        tokio::task::yield_now().await;

        let rmdir_vfs = vfs.clone();
        let rmdir = tokio::task::spawn_blocking(move || rmdir_vfs.rmdir(ROOT_INO, b"parent"));
        tokio::time::sleep(Duration::from_millis(25)).await;
        drop(child_delete_gate);

        unlink
            .await
            .expect("child unlink task should complete")
            .expect("child unlink should succeed");
        rmdir
            .await
            .expect("rmdir task should complete")
            .expect("rmdir should wait for the in-flight child unlink");
    });
}

#[test]
fn rmdir_waits_for_queued_child_unlink_on_parent_namespace() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "rmdir-queued-parent-unlink");
    rt.block_on(async move {
        let parent = vfs
            .mkdir(ROOT_INO, b"parent", 1000, 1000, 0o755)
            .expect("create parent directory");
        vfs.create(parent.ino, b"child.txt", 1000, 1000, 0o644)
            .expect("create child file");

        let parent_delete_gate = vfs
            .state
            .file_session
            .enter_op(parent.ino, FileSessionOperation::Delete)
            .await;

        let unlink_vfs = vfs.clone();
        let unlink =
            tokio::task::spawn_blocking(move || unlink_vfs.unlink(parent.ino, b"child.txt"));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if vfs
                    .state
                    .file_session
                    .debug_snapshot(parent.ino)
                    .is_some_and(|snapshot| snapshot.queued_ops > 0)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("child unlink should queue behind the parent delete gate");

        let rmdir_vfs = vfs.clone();
        let rmdir = tokio::task::spawn_blocking(move || rmdir_vfs.rmdir(ROOT_INO, b"parent"));
        drop(parent_delete_gate);

        unlink
            .await
            .expect("child unlink task should complete")
            .expect("child unlink should succeed");
        rmdir
            .await
            .expect("rmdir task should complete")
            .expect("rmdir should wait for the queued parent unlink");
    });
}

#[test]
fn cleanup_rmdir_retries_when_child_unlink_starts_after_directory_cleanup() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "rmdir-cleanup-late-child");
    rt.block_on(async move {
        let parent = vfs
            .mkdir(ROOT_INO, b"parent", 1000, 1000, 0o755)
            .expect("create parent directory");
        vfs.create(parent.ino, b"child.txt", 1000, 1000, 0o644)
            .expect("create child file");

        let rmdir_vfs = vfs.clone();
        let rmdir =
            tokio::task::spawn_blocking(move || rmdir_vfs.rmdir_for_cleanup(ROOT_INO, b"parent"));
        tokio::time::sleep(Duration::from_millis(25)).await;

        vfs.unlink(parent.ino, b"child.txt")
            .expect("late child unlink should succeed");
        rmdir
            .await
            .expect("cleanup rmdir task should complete")
            .expect("cleanup rmdir should retry after the child unlink");
    });
}

#[tokio::test]
async fn epoch_finalizer_shutdown_cancels_in_flight_work() {
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run_epoch_finalization_until_shutdown(
        shutdown.clone(),
        async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            7_u8
        },
    ));

    tokio::task::yield_now().await;
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .expect("shutdown must cancel in-flight epoch finalization")
        .expect("epoch finalizer task must not panic");
    assert!(
        result.is_none(),
        "cancelled finalization must not report a completed epoch"
    );
}

#[derive(Default)]
struct RecordingKernelInvalidator {
    data_seen: parking_lot::Mutex<Vec<Ino>>,
    attr_seen: parking_lot::Mutex<Vec<Ino>>,
}

impl RecordingKernelInvalidator {
    fn snapshot(&self) -> Vec<Ino> {
        self.data_seen.lock().clone()
    }

    fn attr_snapshot(&self) -> Vec<Ino> {
        self.attr_seen.lock().clone()
    }
}

impl KernelInvalidator for RecordingKernelInvalidator {
    fn inval_inode(&self, ino: Ino) {
        self.data_seen.lock().push(ino);
    }

    fn inval_inode_attrs(&self, ino: Ino) {
        self.attr_seen.lock().push(ino);
    }
}

#[test]
fn published_read_plan_cache_evicts_least_recently_used_inode_when_full() {
    let read = ReadState::new(1);
    let key = PublishedReadPlanCacheKey {
        size: 1,
        committed_size: 1,
        content_epoch: 1,
        committed_content_epoch: 1,
        upload_state: MetaUploadState::Ready,
    };

    for ino in 1..=PUBLISHED_READ_PLAN_CACHE_MAX_INODES as u64 {
        read.published_read_plan_cache_put(ino, key, 0, 1, Arc::from([]));
    }
    assert!(
        read.published_read_plan_cache_get(1, key, 0, 1).is_some(),
        "setup should mark inode 1 as recently used"
    );
    read.published_read_plan_cache_put(9_999, key, 0, 1, Arc::from([]));

    let cache = read.published_read_plan_cache.lock();
    assert_eq!(cache.len(), PUBLISHED_READ_PLAN_CACHE_MAX_INODES);
    assert!(cache.contains(&1));
    assert!(cache.contains(&9_999));
}

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("namespace-resolver-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("namespace-resolver-uuid-{volume_suffix}"),
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

fn make_unstarted_test_vfs_with_config(
    tmp: &TempDir,
    volume_suffix: &str,
    cfg: VfsConfig,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");

    let format_info = test_format_info(volume_suffix);
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let rt = Arc::new(Runtime::new().expect("runtime"));
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        cfg.try_into().expect("test VFS config should validate"),
    )
    .expect("create vfs");
    (rt, vfs, meta)
}

pub(super) fn make_unstarted_test_vfs(
    tmp: &TempDir,
    volume_suffix: &str,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    make_unstarted_test_vfs_with_config(tmp, volume_suffix, VfsConfig::default())
}

fn make_test_vfs(
    tmp: &TempDir,
    volume_suffix: &str,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, volume_suffix);
    let _guard = rt.enter();
    vfs.start().expect("start vfs background workers");
    (rt, vfs, meta)
}

fn make_test_vfs_with_config(
    tmp: &TempDir,
    volume_suffix: &str,
    cfg: VfsConfig,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let (rt, vfs, meta) = make_unstarted_test_vfs_with_config(tmp, volume_suffix, cfg);
    let _guard = rt.enter();
    vfs.start().expect("start vfs background workers");
    (rt, vfs, meta)
}

fn enqueue_deferred_namespace_create(vfs: &Arc<StudioVfs>, attr: &InodeAttr, name: &[u8]) {
    vfs.enqueue_deferred_namespace_op(
        Some(super::namespace::DeferredNamespaceKey::new(ROOT_INO, name)),
        attr.clone(),
        super::namespace::DeferredNamespaceOp::CreateFile {
            attr: attr.clone(),
            parent: ROOT_INO,
            name: name.to_vec(),
            uid: 0,
            gid: 0,
            mode: 0o644,
        },
    )
    .expect("admit deferred namespace create");
}

fn runtime_stats_test_tempdir() -> TempDir {
    studiofs_test_support::workspace_target_tempdir("runtime-stats-")
}

#[test]
fn volatile_flush_does_not_wait_for_deferred_namespace_metadata() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "volatile-flush-deferred-metadata");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(
                ROOT_INO,
                b"volatile-flush-deferred-metadata.txt",
                0,
                0,
                0o644,
            )
            .expect("create");
        vfs.state
            .namespace
            .deferred
            .inner
            .lock()
            .pending_inodes
            .insert(attr.ino, 1);

        tokio::time::timeout(
            Duration::from_millis(100),
            vfs.flush_volatile_writeback_with_outcome(attr.ino, 0),
        )
        .await
        .expect("volatile flush must not wait for deferred namespace commit")
        .expect("volatile flush should succeed");
    });
}

#[test]
fn read_into_fh_clamps_to_remaining_file_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "read-into-fh-clamps");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(ROOT_INO, b"small.txt", 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh(fh, attr.ino, 0, b"hello").await.unwrap();

        let mut out = [0xAA; 8];
        let read_len = vfs.read_into_fh(fh, attr.ino, 2, &mut out).await.unwrap();
        let read_len = read_len as usize;

        assert_eq!(read_len, 3);
        assert_eq!(&out[..read_len], b"llo");
        assert_eq!(&out[read_len..], &[0xAA; 5]);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn read_into_fh_reads_dirty_overlay_while_materialize_is_active() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "read-during-materialize");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"read-during-materialize.txt", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh(fh, attr.ino, 0, b"dirty-overlay")
            .await
            .unwrap();

        let _materialize = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Materialize)
            .await;
        let mut out = [0; 13];
        let read = tokio::time::timeout(Duration::from_millis(100), async {
            vfs.read_into_fh(fh, attr.ino, 0, &mut out).await
        })
        .await
        .expect("foreground read should not queue behind background materialization")
        .expect("dirty overlay read should succeed");

        assert_eq!(read as usize, out.len());
        assert_eq!(&out, b"dirty-overlay");
        drop(_materialize);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_write_does_not_wait_for_background_materialize_session() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "write-during-materialize");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"write-during-materialize.txt", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let materialize = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Materialize)
            .await;

        let written = tokio::time::timeout(Duration::from_millis(100), async {
            vfs.write_fh_with_deferred_materialize(
                fh,
                attr.ino,
                0,
                b"foreground",
                LocalSyncPolicy::Opportunistic,
            )
            .await
        })
        .await
        .expect("foreground write must not queue behind background materialization")
        .expect("foreground write should be accepted");

        assert_eq!(written, b"foreground".len() as u32);
        drop(materialize);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn durability_barrier_still_waits_for_background_materialize_session() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-during-materialize");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"flush-during-materialize.txt", 0, 0, 0o644)
            .unwrap();
        let materialize = vfs
            .state
            .file_session
            .enter_op(attr.ino, FileSessionOperation::Materialize)
            .await;
        let vfs_clone = vfs.clone();
        let mut barrier = Box::pin(tokio::spawn(async move {
            let _barrier = vfs_clone
                .state
                .file_session
                .enter_op(attr.ino, FileSessionOperation::Flush)
                .await;
        }));

        tokio::time::timeout(Duration::from_millis(50), &mut barrier)
            .await
            .expect_err("durability barrier must wait for background materialization");
        drop(materialize);
        tokio::time::timeout(Duration::from_millis(150), barrier)
            .await
            .expect("durability barrier should pass after materialization finishes")
            .unwrap();
    });
}

#[test]
fn runtime_stats_surface_is_root_local_parseable_and_not_persisted() {
    let tmp = runtime_stats_test_tempdir();
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "runtime-stats-surface");

    let attr = vfs
        .lookup(ROOT_INO, RUNTIME_STATS_FILE_NAME)
        .expect("lookup runtime stats surface");
    let getattr_attr = vfs
        .getattr(RUNTIME_STATS_FILE_INO)
        .expect("getattr runtime stats surface by synthetic ino");
    assert_eq!(attr.ino, RUNTIME_STATS_FILE_INO);
    assert_eq!(getattr_attr.ino, RUNTIME_STATS_FILE_INO);
    assert_eq!(getattr_attr.size, RUNTIME_STATS_FILE_REPORTED_SIZE);
    assert_eq!(attr.kind, FileType::RegularFile);
    assert_eq!(attr.mode, 0o444);
    assert!(
        meta.lookup(ROOT_INO, RUNTIME_STATS_FILE_NAME).is_err(),
        "runtime stats surface must not persist into volume metadata"
    );
    match vfs.setattr(RUNTIME_STATS_FILE_INO, SetattrArgs::size_only(0)) {
        Err(VfsError::Meta(MetaError::AccessDenied { ino, .. })) => {
            assert_eq!(ino, RUNTIME_STATS_FILE_INO);
        }
        other => panic!("expected AccessDenied for runtime stats setattr, got {other:?}"),
    }

    let entries = vfs.readdir(ROOT_INO).expect("readdir root");
    assert!(
        !entries
            .iter()
            .any(|entry| entry.name == RUNTIME_STATS_FILE_NAME),
        "runtime stats surface is explicit-path only and must not alter normal root readdir"
    );

    let mut out = vec![0u8; attr.size as usize];
    let read = vfs.read_runtime_stats_file(0, &mut out);
    out.truncate(read as usize);
    let text = String::from_utf8(out).expect("runtime stats text is utf-8");
    assert!(text.starts_with("# studiofs_runtime_stats schema_version=1\n"));

    let parsed = RuntimeStatsSnapshot::parse_text(&text).expect("parse runtime stats text");
    assert_eq!(parsed.schema_version, RUNTIME_STATS_SCHEMA_VERSION);
    assert!(parsed.generated_unix_ms > 0);
    assert!(parsed.bridge.counters.contains_key("read_callbacks"));
    assert!(parsed.vfs_read.counters.contains_key("calls"));
    assert!(parsed.cache.counters.contains_key("prefetch_skips"));
    assert!(
        parsed
            .cache
            .counters
            .contains_key("readahead_scheduled_ops")
    );
    assert!(parsed.cache.counters.contains_key("readahead_dropped_ops"));
    assert!(parsed.cache.counters.contains_key("readahead_useful_bytes"));
    assert!(parsed.upload_queue.gauges.contains_key("pending_depth"));
}

#[test]
fn runtime_stats_surface_has_stable_attributes() {
    let tmp = runtime_stats_test_tempdir();
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "runtime-stats-surface-stable-attr");

    let first = vfs.runtime_stats_file_attr();
    let second = vfs.runtime_stats_file_attr();

    assert_eq!(first.atime, second.atime);
    assert_eq!(first.mtime, second.mtime);
    assert_eq!(first.ctime, second.ctime);
    assert_eq!(first.crtime, second.crtime);
}

#[test]
fn runtime_stats_reserved_name_rejects_namespace_mutations() {
    let tmp = runtime_stats_test_tempdir();
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "runtime-stats-surface-reserved-name");

    let assert_entry_exists = |result: VfsResult<InodeAttr>| match result {
        Err(VfsError::Meta(MetaError::EntryExists { parent, name })) => {
            assert_eq!(parent, ROOT_INO);
            assert_eq!(name, ".studiofs-runtime-stats");
        }
        other => panic!("expected EntryExists, got {other:?}"),
    };
    let assert_access_denied = |result: VfsResult<()>| match result {
        Err(VfsError::Meta(MetaError::AccessDenied { ino, .. })) => {
            assert_eq!(ino, RUNTIME_STATS_FILE_INO);
        }
        other => panic!("expected AccessDenied, got {other:?}"),
    };
    let assert_attr_access_denied = |result: VfsResult<InodeAttr>| match result {
        Err(VfsError::Meta(MetaError::AccessDenied { ino, .. })) => {
            assert_eq!(ino, RUNTIME_STATS_FILE_INO);
        }
        other => panic!("expected AccessDenied, got {other:?}"),
    };

    assert_entry_exists(vfs.create(ROOT_INO, RUNTIME_STATS_FILE_NAME, 0, 0, 0o644));
    assert_entry_exists(vfs.mkdir(ROOT_INO, RUNTIME_STATS_FILE_NAME, 0, 0, 0o755));
    assert_entry_exists(vfs.symlink(ROOT_INO, RUNTIME_STATS_FILE_NAME, b"target", 0, 0));

    let source = vfs
        .create(ROOT_INO, b"runtime-stats-source", 0, 0, 0o644)
        .expect("create source file");
    assert_entry_exists(vfs.link(source.ino, ROOT_INO, RUNTIME_STATS_FILE_NAME));
    assert_attr_access_denied(vfs.link(RUNTIME_STATS_FILE_INO, ROOT_INO, b"runtime-stats-link"));
    assert_access_denied(vfs.unlink(ROOT_INO, RUNTIME_STATS_FILE_NAME));
    assert_access_denied(vfs.rmdir(ROOT_INO, RUNTIME_STATS_FILE_NAME));
    assert_access_denied(vfs.rename(
        ROOT_INO,
        b"runtime-stats-source",
        ROOT_INO,
        RUNTIME_STATS_FILE_NAME,
    ));
    assert_access_denied(vfs.rename(
        ROOT_INO,
        RUNTIME_STATS_FILE_NAME,
        ROOT_INO,
        b"runtime-stats-target",
    ));

    assert!(
        meta.lookup(ROOT_INO, RUNTIME_STATS_FILE_NAME).is_err(),
        "failed namespace mutations must not persist runtime stats name"
    );
    vfs.lookup(ROOT_INO, b"runtime-stats-source")
        .expect("source should remain after rejected rename");
}

#[test]
fn runtime_stats_surface_read_respects_offset_and_buffer_len() {
    let tmp = runtime_stats_test_tempdir();
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "runtime-stats-surface-offset");
    let text = vfs.runtime_stats_file_text();
    assert!(text.len() > 16);

    let mut out = [0u8; 8];
    let read = vfs.read_runtime_stats_file(2, &mut out);
    assert_eq!(read, 8);
    assert_eq!(&out, &text.as_bytes()[2..10]);

    let mut eof = [0u8; 4];
    let read = vfs.read_runtime_stats_file(u64::MAX, &mut eof);
    assert_eq!(read, 0);
}

#[test]
fn runtime_stats_surface_reports_overlay_batch_metrics() {
    let tmp = runtime_stats_test_tempdir();
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "runtime-stats-overlay-batch");
    let snapshot = vfs.runtime_stats_snapshot();

    assert!(snapshot.metadata.counters.contains_key("overlay_enqueue"));
    assert!(snapshot.metadata.gauges.contains_key("overlay_queue_depth"));
    assert!(
        snapshot
            .metadata
            .gauges
            .contains_key("overlay_dirty_age_ms")
    );
}

#[test]
fn contract_vfs_harness_reopens_fixture_and_runs_core_invariants() {
    let mut fixture =
        ContractVfsFixture::new("vfs-contract-harness").expect("create vfs contract fixture");

    let shots = fixture
        .apply_op(ContractVfsOp::Mkdir {
            parent: ROOT_INO,
            name: b"shots".to_vec(),
            mode: 0o755,
        })
        .expect("mkdir through vfs")
        .created_attr()
        .expect("dir attr");
    fixture
        .apply_op(ContractVfsOp::CreateFile {
            parent: shots.ino,
            name: b"take-001.mov".to_vec(),
            mode: 0o644,
        })
        .expect("create through vfs");
    fixture
        .apply_op(ContractVfsOp::Rename {
            parent: shots.ino,
            name: b"take-001.mov".to_vec(),
            new_parent: shots.ino,
            new_name: b"take-final.mov".to_vec(),
        })
        .expect("rename through vfs");
    fixture
        .apply_op(ContractVfsOp::CreateFile {
            parent: shots.ino,
            name: b"scratch.mov".to_vec(),
            mode: 0o644,
        })
        .expect("create scratch file through vfs");
    fixture
        .apply_op(ContractVfsOp::Unlink {
            parent: shots.ino,
            name: b"scratch.mov".to_vec(),
        })
        .expect("unlink through vfs");
    fixture
        .apply_op(ContractVfsOp::Mkdir {
            parent: shots.ino,
            name: b"empty".to_vec(),
            mode: 0o755,
        })
        .expect("mkdir empty through vfs");
    fixture
        .apply_op(ContractVfsOp::Rmdir {
            parent: shots.ino,
            name: b"empty".to_vec(),
        })
        .expect("rmdir through vfs");

    let open_unlink = fixture
        .apply_op(ContractVfsOp::CreateFile {
            parent: shots.ino,
            name: b"open-unlink.mov".to_vec(),
            mode: 0o644,
        })
        .expect("create open-unlink file through vfs")
        .created_attr()
        .expect("open-unlink attr");
    let vfs = fixture.vfs();
    let fh = vfs
        .open_file(open_unlink.ino, OpenFlags::RDWR, false)
        .expect("open file handle");
    fixture
        .apply_op(ContractVfsOp::Unlink {
            parent: shots.ino,
            name: b"open-unlink.mov".to_vec(),
        })
        .expect("unlink open file through vfs");

    let diagnostics = fixture
        .assert_invariants("after vfs namespace ops")
        .expect("vfs invariants");
    assert!(
        diagnostics
            .entries
            .iter()
            .any(|entry| entry.name == b"take-final.mov"),
        "diagnostics should include the renamed VFS dentry: {diagnostics}"
    );
    assert!(
        diagnostics
            .deferred_deletes
            .iter()
            .any(|record| record.ino == open_unlink.ino && record.parent == shots.ino),
        "diagnostics should explain the open-unlinked VFS inode through deferred delete state: {diagnostics}"
    );

    fixture
        .runtime()
        .block_on(vfs.release_fh(fh))
        .expect("release open-unlinked file handle");
    fixture
        .assert_invariants("after vfs open-unlink release")
        .expect("released open-unlink invariants");
    drop(vfs);

    fixture.reopen_vfs().expect("reopen vfs fixture");
    fixture
        .assert_invariants("after vfs reopen")
        .expect("reopened vfs invariants");
}

#[test]
fn contract_vfs_harness_allows_shutdown_grace_timeout_override() {
    let fixture = ContractVfsFixture::new("vfs-contract-shutdown-timeout")
        .expect("create vfs contract fixture")
        .with_shutdown_timeout(Duration::from_millis(100));

    assert_eq!(fixture.shutdown_timeout(), Duration::from_millis(100));
}

#[test]
fn contract_vfs_harness_drop_inside_tokio_runtime_does_not_panic() {
    let rt = Runtime::new().expect("outer runtime");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            let _fixture =
                ContractVfsFixture::new("vfs-contract-drop-in-runtime").expect("create fixture");
        });
    }));

    assert!(
        result.is_ok(),
        "dropping a contract VFS fixture inside an existing Tokio runtime should not panic"
    );
}

pub(super) trait FileGraphMetaFixture {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<Option<PayloadRef>>;
    fn lookup_payload_slices(&self, ino: Ino) -> MetaResult<Vec<(u32, PayloadRef)>>;
}

fn fixture_payload_slices(
    block_index: u32,
    payload: Option<&PayloadRef>,
) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
    let block_start = u64::from(block_index)
        .checked_mul(u64::from(DEFAULT_BLOCK_SIZE))
        .ok_or_else(|| MetaError::Storage("fixture block offset overflow".to_string()))?;
    let data_end = block_start
        .checked_add(u64::from(payload.map_or(0, |payload| payload.length)))
        .ok_or_else(|| MetaError::Storage("fixture block data overflow".to_string()))?;
    let block_end = block_start
        .checked_add(u64::from(DEFAULT_BLOCK_SIZE))
        .ok_or_else(|| MetaError::Storage("fixture block range overflow".to_string()))?;
    let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut push = |start: u64, end: u64, source: Option<&PayloadRef>| -> MetaResult<()> {
        let mut cursor = start;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let piece_end = end.min(chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64));
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("fixture invalid slice range".to_string()))?;
            let mut slice = if let Some(source) = source {
                let delta = u32::try_from(cursor.saturating_sub(block_start))
                    .map_err(|_| MetaError::Storage("fixture slice delta overflow".to_string()))?;
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    range,
                    PayloadRef {
                        object_id: source.object_id,
                        offset: source.offset.saturating_add(delta),
                        length: u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                            MetaError::Storage("fixture slice length overflow".to_string())
                        })?,
                        flags: source.flags,
                    },
                )
            } else {
                SliceRecord::tombstone(SliceId(0), SliceOrder(0), range)
            };
            slice.payload_state = PayloadRefState::LocalOnly;
            chunks.entry(chunk_id).or_default().push(slice);
            cursor = piece_end;
        }
        Ok(())
    };
    push(block_start, data_end, payload)?;
    if data_end < block_end {
        push(data_end, block_end, None)?;
    }
    Ok(chunks.into_iter().collect())
}

impl FileGraphMetaFixture for Arc<RedbMetaEngine> {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<Option<PayloadRef>> {
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_payload_slices(block_index, Some(payload))?.as_slice(),
        )?;
        Ok(None)
    }

    fn lookup_payload_slices(&self, ino: Ino) -> MetaResult<Vec<(u32, PayloadRef)>> {
        let attr = self.get_attr(ino)?;
        let mut blocks = BTreeMap::<u32, PayloadRef>::new();
        for span in self.lookup_regular_file_slices(ino, 0, attr.size)? {
            let Some(block) = span.payload else {
                continue;
            };
            let chunk_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let span_start = chunk_start.saturating_add(u64::from(span.range.start));
            let span_end = chunk_start.saturating_add(u64::from(span.range.end));
            let mut cursor = span_start;
            while cursor < span_end {
                let block_index = u32::try_from(cursor / u64::from(DEFAULT_BLOCK_SIZE))
                    .map_err(|_| MetaError::Storage("fixture block index overflow".to_string()))?;
                let logical_block_end =
                    (u64::from(block_index) + 1).saturating_mul(u64::from(DEFAULT_BLOCK_SIZE));
                let piece_end = span_end.min(logical_block_end);
                let piece_len = u32::try_from(piece_end.saturating_sub(cursor))
                    .map_err(|_| MetaError::Storage("fixture block length overflow".to_string()))?;
                let delta = u32::try_from(cursor.saturating_sub(span_start))
                    .map_err(|_| MetaError::Storage("fixture block offset overflow".to_string()))?;
                blocks
                    .entry(block_index)
                    .and_modify(|existing| {
                        existing.length = existing.length.saturating_add(piece_len);
                        existing.flags |= block.flags;
                    })
                    .or_insert_with(|| PayloadRef {
                        object_id: block.object_id,
                        offset: block.offset.saturating_add(delta),
                        length: piece_len,
                        flags: block.flags,
                    });
                cursor = piece_end;
            }
        }
        Ok(blocks.into_iter().collect())
    }
}

fn reopen_existing_test_vfs(tmp: &TempDir) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("reopen RedbMetaEngine"));
    let format_info = meta.get_format_info().expect("load format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let rt = Arc::new(Runtime::new().expect("runtime"));
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("reopen vfs");
    (rt, vfs, meta)
}

fn assert_writer_state_only_boundary(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    len: u64,
    label: &str,
) {
    assert!(
        vfs.state.writer_stack.has_dirty_writers(ino),
        "{label}: accepted bytes should still be owned by active writer state"
    );
    let spans = meta
        .lookup_regular_file_slices(ino, 0, len)
        .expect("lookup local slice view");
    assert!(
        spans.iter().all(|span| span.payload.is_none()),
        "{label}: writer-state-only bytes should not have materialized local payload slices: {spans:?}"
    );
    assert!(
        vfs.pending_uploads_for_inode(ino).is_empty(),
        "{label}: writer-state-only bytes should not be queued for remote upload"
    );
}

fn assert_local_durable_not_remote_ready_boundary(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    len: u64,
    expected_committed_size: u64,
    label: &str,
) {
    assert!(
        !vfs.state.writer_stack.has_dirty_writers(ino),
        "{label}: local durability boundary should drain active writer state"
    );
    let spans = meta
        .lookup_regular_file_slices(ino, 0, len)
        .expect("lookup local slice view");
    assert!(
        spans.iter().any(|span| span.payload.is_some()),
        "{label}: local durability boundary should materialize payload-backed local slices: {spans:?}"
    );
    assert!(
        !vfs.pending_uploads_for_inode(ino).is_empty(),
        "{label}: locally durable unpublished bytes should remain queued for remote upload"
    );

    let attr = meta.get_attr(ino).expect("load locally durable attr");
    assert_eq!(
        attr.size, len,
        "{label}: local metadata view should expose the accepted file size"
    );
    assert_eq!(
        attr.committed_size, expected_committed_size,
        "{label}: local durability must not change the remote-visible committed size"
    );
    assert!(
        attr.content_epoch > attr.committed_content_epoch,
        "{label}: local content epoch should stay ahead of the remote-visible epoch"
    );
    assert_ne!(
        attr.upload_state,
        MetaUploadState::Ready,
        "{label}: local durability must not mark the inode remote-ready"
    );
}

fn mark_all_overlay_objects_uploaded(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
) -> (u64, Vec<u64>) {
    let attr = meta.get_attr(ino).expect("load inode attrs");
    let object_ids = meta
        .get_regular_file_overlay_object_ids(ino)
        .expect("load overlay object ids");
    let pending_uploads = vfs.pending_uploads_snapshot();
    for object_id in &object_ids {
        vfs.local_payload_backend()
            .mark_uploaded(*object_id)
            .expect("mark uploaded local payload");
        for pending in pending_uploads
            .iter()
            .filter(|pending| pending.object_id == *object_id)
        {
            vfs.cached_store().complete_upload(pending);
        }
        assert!(
            meta.mark_regular_file_overlay_object_uploaded_if_matches(
                ino,
                attr.content_epoch,
                *object_id
            )
            .expect("mark overlay object uploaded"),
            "expected metadata upload mark to apply for every overlay object"
        );
    }
    (attr.content_epoch, object_ids)
}

fn publish_current_file_graph(
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    content_epoch: u64,
    _object_ids: &[u64],
) {
    assert!(
        meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
            .expect("publish current file graph"),
        "expected file graph publish to apply"
    );
}

fn install_fragmented_overlay_block(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    block_size: usize,
    left_fill: u8,
    right_fill: u8,
) -> Vec<u8> {
    let half = block_size / 2;
    let left = vec![left_fill; half];
    let right = vec![right_fill; block_size - half];
    let object_ids = meta
        .next_object_ids(2)
        .expect("allocate fragmented object ids");

    vfs.local_payload_backend()
        .put(object_ids[0], &left, false)
        .expect("persist left fragmented object");
    vfs.local_payload_backend()
        .put(object_ids[1], &right, false)
        .expect("persist right fragmented object");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync fragmented overlay objects");

    meta.flush_regular_file_overlay_slices_with_attr(
        ino,
        &[(
            ChunkId(0),
            vec![
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(0, half as u32).expect("left slice range"),
                    PayloadRef::new_partial(object_ids[0], half as u32),
                ),
                SliceRecord::new(
                    SliceId(1),
                    SliceOrder(1),
                    SliceRange::new(half as u32, block_size as u32).expect("right slice range"),
                    PayloadRef::new_partial(object_ids[1], (block_size - half) as u32),
                ),
            ],
        )],
        None,
    )
    .expect("persist fragmented overlay slices");

    vfs.remove_live_extents_for_inode(ino);
    vfs.cancel_pending_uploads_for_inode(ino);

    let mut expected = left;
    expected.extend_from_slice(&right);
    expected
}

#[test]
fn startup_replay_does_not_override_current_epoch_live_extent_with_stale_pending_upload() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-stale-pending");

    let created = vfs
        .create(1, b"startup-stale-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(9001, 4096))
        .expect("persist current payload slice");

    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_file_offset: 0,
        object_id: 7001,
        payload_len: 4096,
        content_epoch: 4,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(ino),
        vec![(0, 9001, 5)],
        "startup replay must preserve the current metadata epoch instead of overwriting it with stale pending uploads"
    );
}

#[test]
fn startup_replay_cancels_ready_inode_stale_pending_upload() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-ready-stale-pending");

    let created = vfs
        .create(1, b"startup-ready-stale-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let current_object_id = 9_001;
    let stale_object_id = 7_001;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 5;
    attr.upload_state = MetaUploadState::Ready;
    meta.set_attr(&attr).expect("persist ready attr");
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(current_object_id, 4096))
        .expect("persist current payload slice");

    vfs.cached_store()
        .submit_upload(ino, 0, stale_object_id, attr.content_epoch - 1);
    let pending_uploads = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(
        pending_uploads.len(),
        1,
        "test setup should install one stale runtime upload"
    );

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, std::slice::from_ref(&attr));

    assert!(
        vfs.live_extent_records_for_inode(ino).is_empty(),
        "startup replay must not resurrect stale pending upload live extents for a ready inode"
    );
    assert!(
        vfs.pending_uploads_for_inode(ino).is_empty(),
        "startup replay should cancel pending uploads that metadata has already made obsolete"
    );
    assert!(
        !vfs.cached_store().is_pinned(stale_object_id),
        "canceling the stale pending upload should release its runtime pin"
    );

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, std::slice::from_ref(&attr));
    assert!(
        vfs.pending_uploads_for_inode(ino).is_empty(),
        "replaying the same stale pending upload cancellation should remain idempotent"
    );
    assert!(
        !vfs.cached_store().is_pinned(stale_object_id),
        "replaying stale cancellation must not restore the canceled runtime pin"
    );
}

#[test]
fn startup_replay_removes_lost_unpublished_upload_inode_without_payload_graph() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-lost-unpublished-upload");

    let created = vfs
        .create(1, b"startup-lost-unpublished-upload.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let stale_object_id = 7_001;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 48;
    attr.committed_size = 0;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 0;
    attr.upload_state = MetaUploadState::Uploading;
    attr.nlink = 0;
    meta.set_attr(&attr).expect("persist staged attr");

    vfs.cached_store()
        .submit_upload(ino, 0, stale_object_id, attr.content_epoch);
    let pending_uploads = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(
        pending_uploads.len(),
        1,
        "test setup should install one runtime upload without a durable payload graph"
    );

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, std::slice::from_ref(&attr));

    assert!(
        matches!(
            meta.lookup(1, b"startup-lost-unpublished-upload.bin"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "startup replay must not leave a local zero-byte placeholder for a lost unpublished upload"
    );
    assert!(
        matches!(meta.get_attr(ino), Err(MetaError::InodeNotFound(_))),
        "startup replay should remove the orphaned inode once its payload graph is unrecoverable"
    );
    assert!(
        vfs.pending_uploads_for_inode(ino).is_empty(),
        "startup replay should cancel stale runtime uploads for the removed inode"
    );
}

#[test]
fn startup_replay_preserves_ready_empty_file_without_landing_record() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "startup-ready-empty-file-without-landing-record");

    let created = vfs
        .create(1, b"preserved-empty-file.bin", 0, 0, 0o644)
        .expect("create empty file");
    let attr = meta.get_attr(created.ino).expect("load empty file");
    assert!(attr.is_unpublished_empty_draft());

    vfs.replay_dirty_uploads_after_unclean_shutdown(&[], &[attr], true, Some(HashSet::new()));

    let recovered = meta
        .get_attr(created.ino)
        .expect("load recovered empty file");
    assert!(
        recovered.is_unpublished_empty_draft(),
        "startup replay must preserve a ready empty draft locally without publishing it: {recovered:?}"
    );
}

#[test]
fn unclean_restart_removes_writer_only_empty_placeholder() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let name = b"unclean-writer-only-placeholder.bin";
    let payload = b"writer-state-only-before-crash";

    {
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "unclean-writer-placeholder");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, name, 0, 0, 0o644)
                .expect("create unpublished file");
            let fh = vfs
                .open_file(created.ino, OpenFlags::RDWR, false)
                .expect("open unpublished file");

            vfs.write_fh(fh, created.ino, 0, payload)
                .await
                .expect("accept memory-only write");
            let marker_attr = meta
                .get_attr(created.ino)
                .expect("load writer intent metadata");
            assert_eq!(marker_attr.size, 0);
            assert_eq!(marker_attr.committed_size, 0);
            assert_eq!(marker_attr.content_epoch, 1);
            assert_eq!(marker_attr.committed_content_epoch, 0);
            assert_eq!(marker_attr.upload_state, MetaUploadState::Uploading);
            assert_writer_state_only_boundary(
                &vfs,
                &meta,
                created.ino,
                payload.len() as u64,
                "before simulated crash",
            );
            let read = vfs
                .read(created.ino, 0, payload.len() as u32)
                .await
                .expect("read accepted bytes before simulated crash");
            assert_eq!(read.as_ref(), payload);
        });
    }

    let (_rt, _reopened, meta) = reopen_existing_test_vfs(&tmp);
    assert!(
        matches!(
            meta.lookup(ROOT_INO, name),
            Err(MetaError::EntryNotFound { .. })
        ),
        "unclean restart must discard a writer-only empty placeholder when its unbarriered bytes were lost"
    );
}

#[test]
fn unclean_restart_removes_retried_writer_only_empty_placeholder() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let name = b"unclean-retried-writer-only-placeholder.bin";

    {
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "unclean-retried-writer-placeholder");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, name, 0, 0, 0o644)
                .expect("create unpublished file");
            let fh = vfs
                .open_file(created.ino, OpenFlags::RDWR, false)
                .expect("open unpublished file");

            vfs.write_fh(fh, created.ino, u64::MAX, b"rejected")
                .await
                .expect_err("the first write should fail before accepting writer bytes");
            let restored = meta
                .get_attr(created.ino)
                .expect("load restored empty draft metadata");
            assert!(restored.is_unpublished_empty_draft());

            vfs.write_fh(fh, created.ino, 0, b"x")
                .await
                .expect("retry should accept one byte");
            let marker_attr = meta
                .get_attr(created.ino)
                .expect("load retried writer intent metadata");
            assert!(
                marker_attr.content_epoch > 1,
                "retry should exercise a later pending incarnation: {marker_attr:?}"
            );
            assert_writer_state_only_boundary(&vfs, &meta, created.ino, 1, "after retry");
        });
    }

    let (_rt, _reopened, meta) = reopen_existing_test_vfs(&tmp);
    assert!(
        matches!(
            meta.lookup(ROOT_INO, name),
            Err(MetaError::EntryNotFound { .. })
        ),
        "unclean restart must discard a retried writer-only empty placeholder"
    );
}

#[test]
fn startup_replay_removes_lost_unpublished_upload_inode_after_orphan_payload_cleanup() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-lost-unpublished-orphan");

    let created = vfs
        .create(1, b"startup-lost-unpublished-orphan.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let orphan_object_id = 7_002;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 48;
    attr.committed_size = 0;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 0;
    attr.upload_state = MetaUploadState::Uploading;
    attr.nlink = 0;
    meta.set_attr(&attr).expect("persist staged attr");

    vfs.local_payload_backend()
        .put(orphan_object_id, b"lost-unpublished-payload", false)
        .expect("persist orphan dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync orphan dirty payload");

    assert!(
        vfs.cached_store().snapshot_pending_uploads().is_empty(),
        "test setup should mirror a crash where pending upload catalog was lost"
    );
    assert!(
        vfs.local_payload_backend()
            .dirty_object_ids()
            .contains(&orphan_object_id),
        "test setup should expose the unreferenced payload as dirty before replay"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], std::slice::from_ref(&attr));

    assert!(
        matches!(
            meta.lookup(1, b"startup-lost-unpublished-orphan.bin"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "startup replay must remove the dentry after the payload was reduced to an orphan"
    );
    assert!(
        matches!(meta.get_attr(ino), Err(MetaError::InodeNotFound(_))),
        "startup replay should remove a lost unpublished inode even when no pending upload survived"
    );
    assert!(
        !vfs.local_payload_backend().contains(orphan_object_id),
        "startup replay should still remove the orphan dirty payload"
    );
}

#[test]
fn upload_pipeline_runtime_events_only_emit_active_samples_and_transitions() {
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(true, false, true),
        Some(("became_active", "upload pipeline active"))
    );
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(false, true, false),
        Some(("became_idle", "upload pipeline idle"))
    );
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(false, false, true),
        Some(("sample", "upload pipeline sample"))
    );
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(false, false, false),
        None
    );
}

#[test]
fn s3_upload_problem_detector_timeout_delta_enters_warning_state() {
    let mut state = super::workers::UploadTroubleState::default();
    let counters = super::workers::UploadTroubleCounters {
        uploads_err: 1,
        timeouts: 1,
        ..Default::default()
    };

    assert_eq!(
        state.observe(counters, 2, 0, 0),
        super::workers::UploadTroubleSignal::Warn
    );
    assert!(state.is_active());
}

#[test]
fn s3_upload_problem_detector_repeated_pending_ticks_keep_warning_state() {
    let mut state = super::workers::UploadTroubleState::default();
    let counters = super::workers::UploadTroubleCounters {
        uploads_err: 1,
        network_errors: 1,
        retries: 1,
        ..Default::default()
    };

    assert_eq!(
        state.observe(counters, 1, 1, 1),
        super::workers::UploadTroubleSignal::Warn
    );
    assert_eq!(
        state.observe(counters, 1, 0, 1),
        super::workers::UploadTroubleSignal::Warn
    );
    assert!(state.is_active());
}

#[test]
fn s3_upload_problem_detector_drain_emits_one_recovery_event() {
    let mut state = super::workers::UploadTroubleState::default();
    let failed = super::workers::UploadTroubleCounters {
        uploads_err: 1,
        status_5xx_errors: 1,
        retries: 1,
        ..Default::default()
    };
    assert_eq!(
        state.observe(failed, 1, 1, 0),
        super::workers::UploadTroubleSignal::Warn
    );

    let recovered = super::workers::UploadTroubleCounters {
        uploads_ok: 1,
        uploads_err: 1,
        status_5xx_errors: 1,
        retries: 1,
        bytes_uploaded: 4096,
        ..Default::default()
    };
    assert_eq!(
        state.observe(recovered, 0, 0, 0),
        super::workers::UploadTroubleSignal::Recovered
    );
    assert_eq!(
        state.observe(recovered, 0, 0, 0),
        super::workers::UploadTroubleSignal::None
    );
    assert!(!state.is_active());
}

#[test]
fn startup_replay_keeps_metadata_live_extent_when_pending_upload_has_same_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-same-epoch");

    let created = vfs
        .create(1, b"startup-same-epoch.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(9001, 4096))
        .expect("persist current payload slice");

    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_file_offset: 0,
        object_id: 7001,
        payload_len: 4096,
        content_epoch: 5,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(ino),
        vec![(0, 9001, 5)],
        "startup replay must keep the metadata-derived block when pending upload only matches the same epoch"
    );
}

#[test]
fn startup_replay_recovers_current_epoch_pending_upload_when_metadata_graph_is_missing() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-missing-graph");

    let created = vfs
        .create(1, b"startup-missing-graph.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 9_101;
    let payload = b"locally durable payload whose metadata commit was interrupted";

    vfs.local_payload_backend()
        .put(object_id, payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 0;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");

    vfs.cached_store()
        .submit_upload(ino, 0, object_id, attr.content_epoch);
    let pending_uploads = vfs.cached_store().snapshot_pending_uploads();

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr.clone()]);

    assert_eq!(
        (
            vfs.live_extent_records_for_inode(ino),
            vfs.pending_uploads_for_inode(ino),
            vfs.local_payload_backend().contains(object_id),
        ),
        (
            vec![(0, object_id, attr.content_epoch)],
            vec![(0, object_id, attr.content_epoch)],
            true,
        ),
        "startup replay must recover a durable current-epoch upload when the matching metadata graph commit was interrupted"
    );
}

#[test]
fn startup_replay_recovers_uploaded_pending_payload_when_metadata_graph_is_missing() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-uploaded-missing-graph");

    let created = vfs
        .create(1, b"startup-uploaded-missing-graph.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 9_102;
    let payload = b"uploaded payload whose metadata commit was interrupted";

    vfs.local_payload_backend()
        .put(object_id, payload, true)
        .expect("persist uploaded local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");
    assert!(
        vfs.local_payload_backend().dirty_object_ids().is_empty(),
        "uploaded payload must not be present in the dirty-only startup snapshot"
    );

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 0;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");

    vfs.cached_store()
        .submit_upload(ino, 0, object_id, attr.content_epoch);
    let pending_uploads = vfs.cached_store().snapshot_pending_uploads();

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr.clone()]);

    assert_eq!(
        (
            vfs.live_extent_records_for_inode(ino),
            vfs.pending_uploads_for_inode(ino),
        ),
        (
            vec![(0, object_id, attr.content_epoch)],
            vec![(0, object_id, attr.content_epoch)],
        ),
        "startup replay must recover an uploaded current-epoch payload when metadata commit was interrupted"
    );
}

#[test]
fn startup_replay_does_not_recover_pending_upload_beyond_truncated_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-truncated-pending-upload");

    let created = vfs
        .create(1, b"startup-truncated-pending-upload.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 9_103;

    vfs.local_payload_backend()
        .put(object_id, b"payload truncated before startup replay", false)
        .expect("persist truncated local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync truncated local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 1;
    attr.committed_size = 0;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 0;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist truncated attr");

    vfs.cached_store().submit_upload(
        ino,
        u64::from(vfs.block_size),
        object_id,
        attr.content_epoch,
    );
    let pending_uploads = vfs.cached_store().snapshot_pending_uploads();

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        (
            vfs.live_extent_records_for_inode(ino),
            vfs.pending_uploads_for_inode(ino),
        ),
        (Vec::new(), Vec::new()),
        "startup replay must cancel a pending upload whose offset is beyond the truncated file size"
    );
}

#[test]
fn startup_replay_prefers_overlay_metadata_live_extent_over_base_for_same_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-overlay-same-epoch");

    let created = vfs
        .create(1, b"startup-overlay-same-epoch.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Updating;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(9001, 4096))
        .expect("persist base payload slice");
    meta.flush_regular_file_overlay_slices_with_attr(
        ino,
        &[(
            studiofs_meta::schema::ChunkId(0),
            vec![studiofs_meta::schema::SliceRecord {
                slice_id: studiofs_meta::schema::SliceId::default(),
                order: studiofs_meta::schema::SliceOrder::default(),
                range: studiofs_meta::schema::SliceRange::new(0, 4096)
                    .expect("overlay slice range"),
                payload: Some(studiofs_meta::schema::PayloadRef {
                    object_id: 9002,
                    offset: 0,
                    length: 4096,
                    flags: studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK,
                }),
                payload_state: studiofs_meta::schema::PayloadRefState::LocalOnly,
            }],
        )],
        Some(&attr),
    )
    .expect("persist overlay slice state");

    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_file_offset: 0,
        object_id: 9002,
        payload_len: 4096,
        content_epoch: 5,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(ino),
        vec![(0, 9002, 5)],
        "startup replay must prefer overlay metadata over the published base when rebuilding same-epoch live extents"
    );
}

#[test]
fn pending_upload_views_follow_runtime_queue_after_startup_replay() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-views-runtime-queue");

    let created = vfs
        .create(1, b"persistent-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_001;
    let payload = vec![0xAB; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist current payload slice");
    assert!(
        vfs.cached_store().snapshot_pending_uploads().is_empty(),
        "test setup requires startup replay to seed the runtime queue"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr.clone()]);

    assert_eq!(
        vfs.pending_uploads_for_inode(ino),
        vec![(0, object_id, attr.content_epoch)],
        "pending upload helpers must follow the runtime queue rebuilt during startup replay"
    );

    let pending_upload_index = vfs.snapshot_pending_upload_index();
    assert_eq!(
        pending_upload_index.latest_extent_record(ino, 0, Some(attr.content_epoch)),
        Some(ExtentRecordId {
            object_id,
            content_epoch: attr.content_epoch,
        }),
        "pending upload catalog must derive live extents from the replayed runtime queue"
    );
}

#[test]
fn startup_replay_rebuilds_runtime_pending_upload_index_from_persisted_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-runtime-uploads");

    let created = vfs
        .create(1, b"startup-replay.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_011;
    let payload = vec![0xDA; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 9;
    attr.committed_content_epoch = 8;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist current payload slice");

    assert!(
        vfs.cached_store().snapshot_pending_uploads().is_empty(),
        "test setup requires the runtime queue to start empty"
    );
    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr.clone()]);

    let runtime_pending = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(runtime_pending.len(), 1);
    assert_eq!(runtime_pending[0].ino, ino);
    assert_eq!(runtime_pending[0].object_id, object_id);
    assert!(
        vfs.cached_store().is_pinned(object_id),
        "requeued runtime upload should repin the durable local payload"
    );
    assert!(
        vfs.cached_store().has_staged_upload(object_id),
        "startup replay should restore upload links so recovered uploads do not depend on cold rereads"
    );
}

#[test]
fn startup_replay_summary_reports_recovered_bytes_and_queued_uploads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-summary-bytes");

    let created = vfs
        .create(1, b"startup-summary.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_021;
    let payload = vec![0xE1; 8192];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 11;
    attr.committed_content_epoch = 10;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist current payload slice");

    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr]);

    let summary = vfs
        .zeroio_runtime_snapshot()
        .startup_replay
        .expect("startup replay summary should be visible");
    assert_eq!(summary.replayed_blocks, 1);
    assert_eq!(summary.replayed_bytes, payload.len() as u64);
    assert_eq!(summary.queued_uploads, 1);
    assert_eq!(summary.discarded_memory_only_bytes, 0);
}

#[test]
fn startup_replay_restores_live_extents_for_duplicate_dirty_object_references() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-duplicate-object");

    let first = vfs
        .create(1, b"duplicate-object-a.bin", 0, 0, 0o644)
        .expect("create first file");
    let second = vfs
        .create(1, b"duplicate-object-b.bin", 0, 0, 0o644)
        .expect("create second file");
    let object_id = 7_031;
    let payload = vec![0xC7; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist shared dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync shared dirty payload");

    let mut attrs = Vec::new();
    for ino in [first.ino, second.ino] {
        let mut attr = meta.get_attr(ino).expect("load attr");
        attr.size = payload.len() as u64;
        attr.committed_size = 0;
        attr.content_epoch = 12;
        attr.committed_content_epoch = 11;
        attr.upload_state = MetaUploadState::Uploading;
        meta.set_attr(&attr).expect("persist staged attr");
        meta.stage_payload_slice(
            ino,
            0,
            &PayloadRef::new_partial(object_id, payload.len() as u32),
        )
        .expect("persist duplicate object reference");
        attrs.push(attr);
    }

    vfs.replay_dirty_uploads_from_local_backend(&[], &attrs);

    assert_eq!(
        vfs.live_extent_records_for_inode(first.ino),
        vec![(0, object_id, 12)],
        "first file should recover a live extent for the shared dirty object"
    );
    assert_eq!(
        vfs.live_extent_records_for_inode(second.ino),
        vec![(0, object_id, 12)],
        "second file should also recover a live extent even though upload replay is object-deduped"
    );
    assert_eq!(
        vfs.cached_store().snapshot_pending_uploads().len(),
        1,
        "startup replay should still submit only one upload for one dirty object"
    );
}

#[test]
fn startup_replay_restores_all_block_extents_when_one_file_reuses_dirty_object_id() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-duplicate-blocks");

    let created = vfs
        .create(1, b"duplicate-blocks.bin", 0, 0, 0o644)
        .expect("create test file");
    let object_id = 7_032;
    let block_len = vfs.block_size;

    vfs.local_payload_backend()
        .put(object_id, &vec![0xD4; block_len as usize], false)
        .expect("persist shared dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync shared dirty payload");

    let mut attr = meta.get_attr(created.ino).expect("load attr");
    attr.size = u64::from(block_len) * 2;
    attr.committed_size = 0;
    attr.content_epoch = 13;
    attr.committed_content_epoch = 12;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        created.ino,
        0,
        &PayloadRef::new_partial(object_id, block_len),
    )
    .expect("persist first duplicate object reference");
    meta.stage_payload_slice(
        created.ino,
        1,
        &PayloadRef::new_partial(object_id, block_len),
    )
    .expect("persist second duplicate object reference");

    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(created.ino),
        vec![(0, object_id, 13), (1, object_id, 13)],
        "startup replay should restore every logical block covered by the reused dirty object"
    );
    assert_eq!(
        vfs.cached_store().snapshot_pending_uploads().len(),
        1,
        "one physical dirty object should still map to one upload submission"
    );
    let summary = vfs
        .zeroio_runtime_snapshot()
        .startup_replay
        .expect("startup replay summary should be visible");
    assert_eq!(
        summary.replayed_blocks, 2,
        "replayed_blocks should count restored logical extents, not deduped upload ops"
    );
    assert_eq!(
        summary.queued_uploads, 1,
        "queued_uploads should stay deduped to one physical upload op"
    );
}

#[test]
fn flush_queues_runtime_upload_immediately_after_local_durability() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "flush-queues-runtime-upload");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"flush-truth.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let payload = vec![0x5Au8; vfs.block_size as usize];

    rt.block_on(async {
        vfs.write(ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(ino).await.expect("flush durable local payload");
    });

    let pending_uploads = vfs.pending_uploads_for_inode(ino);
    assert_eq!(
        pending_uploads.len(),
        1,
        "local durability should enqueue the unpublished payload object into the runtime upload queue"
    );

    let runtime_pending = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(runtime_pending.len(), 1);
    assert_eq!(runtime_pending[0].ino, ino);
    assert_eq!(runtime_pending[0].object_id, pending_uploads[0].1);
}

#[test]
fn sfs312_write_is_visible_from_writer_state_before_local_durability_boundary() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-312-writer-state-visible");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-312-writer-state-visible.bin", 0, 0, 0o644)
            .expect("create writer-state file");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open writer-state handle");
        let payload = b"writer-state-before-flush";

        vfs.write_fh(fh, attr.ino, 0, payload)
            .await
            .expect("accept writer-state payload");

        let read = vfs
            .read(attr.ino, 0, payload.len() as u32)
            .await
            .expect("read writer-state payload");
        assert_eq!(
            read.as_ref(),
            payload,
            "same-mount reads should observe accepted bytes through active writer state"
        );
        assert_writer_state_only_boundary(
            &vfs,
            &meta,
            attr.ino,
            payload.len() as u64,
            "write-before-flush",
        );

        vfs.release_fh(fh).await.expect("release writer handle");
    });
}

#[test]
fn sfs312_flush_materializes_local_slice_boundary_without_remote_visibility() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-312-flush-boundary");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-312-flush-boundary.bin", 0, 0, 0o644)
            .expect("create flush-boundary file");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open flush-boundary handle");
        let payload = b"flush-local-not-remote";

        vfs.write_fh(fh, attr.ino, 0, payload)
            .await
            .expect("accept flush-boundary payload");
        assert_writer_state_only_boundary(
            &vfs,
            &meta,
            attr.ino,
            payload.len() as u64,
            "flush-before-boundary",
        );

        let outcome = vfs
            .flush_fh_with_outcome(fh)
            .await
            .expect("flush handle boundary");
        assert!(
            outcome.flushed_local_dirty_state,
            "flush should report materializing the accepted local dirty state"
        );
        assert_local_durable_not_remote_ready_boundary(
            &vfs,
            &meta,
            attr.ino,
            payload.len() as u64,
            0,
            "flush-after-boundary",
        );

        vfs.release_fh(fh).await.expect("release flushed handle");
    });
}

#[test]
fn sfs312_fsync_persists_local_state_without_remote_visibility() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-312-fsync-boundary");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-312-fsync-boundary.bin", 0, 0, 0o644)
            .expect("create fsync-boundary file");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open fsync-boundary handle");
        let payload = b"fsync-local-not-remote";

        vfs.write_fh(fh, attr.ino, 0, payload)
            .await
            .expect("accept fsync-boundary payload");
        let outcome = vfs
            .fsync_with_outcome(attr.ino, false)
            .await
            .expect("fsync local boundary");
        assert!(
            outcome.flushed_file_data,
            "fsync should report persisting caller-requested local file data"
        );
        assert_local_durable_not_remote_ready_boundary(
            &vfs,
            &meta,
            attr.ino,
            payload.len() as u64,
            0,
            "fsync-after-boundary",
        );

        let release = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release fsynced handle");
        assert!(
            release.closed_last_handle,
            "single handle release should still close the last local writer handle"
        );
        assert!(
            !release.flushed_local_dirty_state,
            "release after fsync should not report another local dirty drain"
        );
    });
}

#[test]
fn sfs312_last_handle_release_drains_local_durability_boundary() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-312-release-boundary");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-312-release-boundary.bin", 0, 0, 0o644)
            .expect("create release-boundary file");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open release-boundary handle");
        let payload = b"release-local-not-remote";

        vfs.write_fh(fh, attr.ino, 0, payload)
            .await
            .expect("accept release-boundary payload");
        assert_writer_state_only_boundary(
            &vfs,
            &meta,
            attr.ino,
            payload.len() as u64,
            "release-before-boundary",
        );

        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release last handle boundary");
        assert!(
            outcome.closed_last_handle,
            "the only handle should own the last-handle release boundary"
        );
        assert!(
            outcome.flushed_local_dirty_state,
            "last-handle release should report draining accepted local dirty state"
        );
        assert_local_durable_not_remote_ready_boundary(
            &vfs,
            &meta,
            attr.ino,
            payload.len() as u64,
            0,
            "release-after-boundary",
        );
    });
}

#[test]
fn buffered_partial_write_uses_fragmented_overlay_slice_graph_as_base() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "fragmented-overlay-buffered-base");
    let _guard = rt.enter();
    rt.block_on(async {
        let created = vfs
            .create(1, b"fragmented-buffered-base.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = created.ino;
        let block_size = vfs.block_size as usize;
        let baseline = vec![0x33u8; block_size];

        vfs.write(ino, 0, &baseline).await.expect("write baseline");
        vfs.flush(ino).await.expect("flush baseline");

        let mut expected =
            install_fragmented_overlay_block(&vfs, &meta, ino, block_size, 0x11, 0x22);
        let patch_offset = 128 * 1024usize;
        let patch = vec![0x7Au8; 8192];
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);

        vfs.write(ino, patch_offset as u64, &patch)
            .await
            .expect("write patch over fragmented overlay");
        vfs.flush(ino)
            .await
            .expect("flush patch over fragmented overlay");

        let read = vfs
            .read(ino, 0, block_size as u32)
            .await
            .expect("read patched block");
        assert_eq!(
            read.as_ref(),
            expected.as_slice(),
            "buffered write path should reuse fragmented overlay slice graph as its base payload"
        );
    });
}

#[test]
fn release_fh_materializes_fragmented_overlay_slice_graph_base() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "fragmented-overlay-handle-base");
    let _guard = rt.enter();
    rt.block_on(async {
        let created = vfs
            .create(1, b"fragmented-handle-base.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = created.ino;
        let block_size = vfs.block_size as usize;
        let baseline = vec![0x44u8; block_size];

        vfs.write(ino, 0, &baseline).await.expect("write baseline");
        vfs.flush(ino).await.expect("flush baseline");

        let mut expected =
            install_fragmented_overlay_block(&vfs, &meta, ino, block_size, 0x21, 0x42);
        let patch_offset = 64 * 1024usize;
        let patch = vec![0x6Bu8; 4096];
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open test handle");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            patch_offset as u64,
            &patch,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write handle patch over fragmented overlay");

        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release should materialize fragmented overlay base");
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        let read = vfs
            .read(ino, 0, block_size as u32)
            .await
            .expect("read released handle block");
        assert_eq!(
            read.as_ref(),
            expected.as_slice(),
            "writer-stack materialization should reuse fragmented overlay slice graph as its base payload"
        );
    });
}

#[test]
fn wait_for_uploads_idle_observes_runtime_upload_queue() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "wait-for-runtime-upload-idle");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"wait-for-truth-idle.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let payload = vec![0x33u8; vfs.block_size as usize];

    rt.block_on(async {
        vfs.write(ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(ino).await.expect("flush durable local payload");
    });

    assert!(
        !rt.block_on(vfs.wait_for_uploads_idle(Duration::from_millis(25))),
        "wait_for_uploads_idle must not report idle while runtime uploads are still queued"
    );
}

#[test]
fn upload_stats_report_backlog_bytes_age_retries_active_and_completion_rate() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "upload-stats-backlog");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"upload-stats.bin", 0, 0, 0o644)
        .expect("create test file");
    let payload = vec![0x44u8; vfs.block_size as usize];

    rt.block_on(async {
        vfs.write(created.ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(created.ino)
            .await
            .expect("flush durable local payload");
    });

    let pending = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(pending.len(), 1, "test should stage one upload object");

    let stats = vfs.upload_stats();
    assert_eq!(stats.pending_uploads, 1);
    assert_eq!(stats.pending_upload_bytes, payload.len() as u64);
    assert!(
        stats.oldest_pending_upload_age_ms < 5_000,
        "newly staged upload should report a fresh backlog age, got {} ms",
        stats.oldest_pending_upload_age_ms
    );
    assert_eq!(stats.active_uploads, 0);
    assert_eq!(stats.pending_retry_count, 0);
    assert_eq!(stats.max_pending_retry_count, 0);
    assert_eq!(stats.completed_uploads, 0);
    assert_eq!(stats.completed_upload_bytes, 0);
    assert_eq!(stats.completed_uploads_per_sec, 0.0);
    assert_eq!(stats.completed_upload_bytes_per_sec, 0.0);

    let mut retry = pending[0].clone();
    retry.retry_count = 3;
    retry.not_before_unix_ms = retry.created_unix_ms.saturating_add(1_000);
    assert!(vfs.cached_store().requeue_upload(retry));
    let retry_stats = vfs.upload_stats();
    assert_eq!(retry_stats.pending_uploads, 1);
    assert_eq!(retry_stats.pending_retry_count, 3);
    assert_eq!(retry_stats.max_pending_retry_count, 3);

    vfs.cached_store().complete_upload(&pending[0]);

    let completed = vfs.upload_stats();
    assert_eq!(completed.pending_uploads, 0);
    assert_eq!(completed.pending_upload_bytes, 0);
    assert_eq!(completed.completed_uploads, 1);
    assert_eq!(completed.completed_upload_bytes, payload.len() as u64);
    assert!(completed.completed_uploads_per_sec >= 0.0);
    assert!(completed.completed_upload_bytes_per_sec >= 0.0);
}

#[test]
fn upload_stats_deserializes_snapshots_without_new_backlog_fields() {
    let stats: VfsUploadStats = serde_json::from_value(serde_json::json!({
        "pending_uploads": 2,
        "upload_requeues": 1,
        "canceled_deleted": 0,
        "canceled_stale": 0
    }))
    .expect("old upload stats snapshots should remain readable");

    assert_eq!(stats.pending_uploads, 2);
    assert_eq!(stats.pending_upload_bytes, 0);
    assert_eq!(stats.oldest_pending_upload_age_ms, 0);
    assert_eq!(stats.active_uploads, 0);
    assert_eq!(stats.pending_retry_count, 0);
    assert_eq!(stats.max_pending_retry_count, 0);
    assert_eq!(stats.completed_uploads, 0);
    assert_eq!(stats.completed_upload_bytes, 0);
    assert_eq!(stats.completed_uploads_per_sec, 0.0);
    assert_eq!(stats.completed_upload_bytes_per_sec, 0.0);
    assert_eq!(stats.upload_requeues, 1);
}

#[test]
fn pending_upload_truth_ignores_deleted_inode_local_payloads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-views-deleted-inode");

    let created = vfs
        .create(1, b"deleted-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_002;
    let payload = vec![0xCD; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 6;
    attr.committed_content_epoch = 5;
    attr.nlink = 0;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist deleted attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist deleted payload slice");

    assert!(
        vfs.pending_uploads_for_inode(ino).is_empty(),
        "deleted inodes must keep local payloads readable without advertising them as pending uploads"
    );

    let pending_upload_index = vfs.snapshot_pending_upload_index();
    assert_eq!(
        pending_upload_index.latest_extent_record(ino, 0, Some(attr.content_epoch)),
        None,
        "pending upload catalog must not expose deleted inode blocks as uploadable extents"
    );
}

#[test]
fn startup_replay_removes_orphan_dirty_payloads_and_clears_pending_evictions() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "startup-replay-orphan-cleanup");
    let object_id = 991_337;

    vfs.local_payload_backend()
        .put(object_id, b"orphan-dirty-payload", false)
        .expect("persist orphan dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync orphan dirty payload");
    vfs.cache_upload_hot_batch(vec![studiofs_cache::local::LocalPayloadPut {
        object_id,
        data: Bytes::from_static(b"orphan-dirty-payload"),
        evictable: false,
    }]);
    vfs.cached_store
        .link_upload_stage_batch(&[object_id])
        .expect("seed stale dirty upload-stage hardlink");
    vfs.state.upload.pending_evictions.lock().insert(object_id);

    assert!(
        vfs.local_payload_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "test setup should expose orphan as dirty before replay"
    );
    assert!(
        vfs.state
            .upload
            .pending_evictions
            .lock()
            .contains(&object_id),
        "test setup should seed pending eviction before replay"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], &[]);

    assert!(
        !vfs.local_payload_backend().contains(object_id),
        "startup replay should remove orphan dirty payload without metadata reference"
    );
    assert!(
        !vfs.local_payload_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "startup replay should clear orphan dirty payload from dirty index"
    );
    assert!(
        !vfs.state
            .upload
            .pending_evictions
            .lock()
            .contains(&object_id),
        "startup replay should clear pending eviction bookkeeping for removed orphan"
    );
    assert!(
        vfs.cached_store().get_hot_upload(object_id).is_none(),
        "startup replay should clear stale hot-upload cache entries for removed orphan"
    );
    assert!(
        vfs.cached_store()
            .get_staged_for_upload(object_id)
            .is_none(),
        "startup replay should clear stale upload-stage links for removed orphan"
    );
    assert_eq!(
        vfs.state
            .upload
            .upload_canceled_stale
            .load(Ordering::Relaxed),
        1,
        "startup replay should attribute removed orphan to stale-upload cleanup"
    );
}

#[test]
fn startup_replay_removes_unreferenced_warm_payloads_and_stage_links() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "startup-replay-warm-cache-cleanup");
    let object_id = 991_338;

    vfs.local_payload_backend()
        .put(object_id, b"orphan-warm-payload", true)
        .expect("persist orphan warm payload");
    vfs.local_payload_backend()
        .mark_uploaded(object_id)
        .expect("mark uploaded local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync orphan warm payload");
    vfs.cached_store
        .link_upload_stage_batch(&[object_id])
        .expect("seed stale upload-stage hardlink");
    vfs.state.upload.pending_evictions.lock().insert(object_id);

    assert!(
        vfs.local_payload_backend().contains(object_id),
        "test setup should expose orphan warm payload before replay"
    );
    assert!(
        !vfs.local_payload_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "warm uploaded payload should not be covered by dirty-only orphan cleanup"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], &[]);

    assert!(
        !vfs.local_payload_backend().contains(object_id),
        "startup replay should remove unreferenced warm payload cache entries"
    );
    assert!(
        !vfs.state
            .upload
            .pending_evictions
            .lock()
            .contains(&object_id),
        "startup replay should clear stale pending eviction bookkeeping"
    );
    assert_eq!(
        vfs.state
            .upload
            .upload_canceled_stale
            .load(Ordering::Relaxed),
        1,
        "startup replay should attribute warm orphan removal to stale-cache cleanup"
    );
    let summary = vfs
        .zeroio_runtime_snapshot()
        .startup_replay
        .expect("startup replay summary should be visible");
    assert_eq!(summary.unreferenced_local_payloads_removed, 1);
}

#[test]
fn zeroio_snapshot_distinguishes_single_store_payload_classes_and_publish_backlog() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-snapshot-classes");
    let _guard = rt.enter();

    rt.block_on(async {
        let pinned = vfs.create(1, b"pinned.bin", 0, 0, 0o644).unwrap();
        vfs.write(pinned.ino, 0, b"pinned-unpublished-payload")
            .await
            .unwrap();
        vfs.flush(pinned.ino)
            .await
            .expect("materialize pinned unpublished payload");

        let publish_ready = vfs.create(1, b"publish-ready.bin", 0, 0, 0o644).unwrap();
        vfs.write(publish_ready.ino, 0, b"publish-ready-payload")
            .await
            .unwrap();
        vfs.flush(publish_ready.ino)
            .await
            .expect("materialize publish-ready payload");

        let published = vfs.create(1, b"published.bin", 0, 0, 0o644).unwrap();
        vfs.write(published.ino, 0, b"published-warm-payload")
            .await
            .unwrap();
        vfs.flush(published.ino)
            .await
            .expect("materialize published warm payload");

        let (publish_ready_epoch, publish_ready_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, publish_ready.ino);
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, published.ino);
        publish_current_file_graph(&meta, published.ino, published_epoch, &published_blocks);

        let snapshot = vfs.zeroio_runtime_snapshot();

        assert!(
            snapshot.unpublished_files >= 2,
            "expected pinned and publish-ready epochs to remain unpublished"
        );
        assert!(
            snapshot.cache_bytes.pinned_unpublished_bytes > 0,
            "single-store snapshot must report correctness-critical pinned unpublished bytes"
        );
        assert!(
            snapshot.cache_bytes.publish_ready_retained_bytes > 0,
            "single-store snapshot must report uploaded-but-unpublished retained bytes"
        );
        assert!(
            snapshot.cache_bytes.published_warm_bytes > 0,
            "single-store snapshot must report published evictable warm-cache bytes"
        );
        assert!(
            snapshot.publish_ready_backlog_epochs >= 1,
            "publish-ready backlog should include fully uploaded unpublished epochs"
        );

        let publish_ready_entry = snapshot
            .drilldown
            .iter()
            .find(|entry| entry.ino == publish_ready.ino)
            .expect("publish-ready inode should appear in zeroio drilldown");
        assert_eq!(
            publish_ready_entry.lifecycle_state,
            InodeLifecycleState::PublishReady
        );
        assert_eq!(
            publish_ready_entry.retention,
            PayloadRetentionClass::PublishReadyRetained
        );
        assert_eq!(publish_ready_entry.local_content_epoch, publish_ready_epoch);
        assert!(publish_ready_entry.publish_ready_queued);
        assert_eq!(publish_ready_entry.status, "publish_ready_queued");

        assert!(
            !snapshot
                .drilldown
                .iter()
                .any(|entry| entry.ino == published.ino),
            "published warm-cache files should not force per-file drilldown work"
        );

        assert!(
            !publish_ready_blocks.is_empty(),
            "test setup should persist current unpublished slice payloads"
        );
    });
}

#[test]
fn zeroio_snapshot_reports_delete_before_publish_cleanup_visibility() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-delete-before-publish");
    let _guard = rt.enter();

    let attr = vfs
        .create(1, b"delete-before-publish.bin", 0, 0, 0o644)
        .unwrap();
    let mut staged_attr = meta
        .get_attr(attr.ino)
        .expect("load delete-before-publish attr");
    staged_attr.size = 8192;
    staged_attr.content_epoch = 1;
    meta.set_attr(&staged_attr).expect("persist staged attr");
    vfs.local_payload_backend()
        .put(950, b"local-only", false)
        .expect("persist local-only object");
    vfs.local_payload_backend()
        .put(951, b"uploaded", true)
        .expect("persist uploaded object");
    vfs.local_payload_backend()
        .mark_uploaded(951)
        .expect("mark uploaded local payload");
    meta.stage_payload_slice(attr.ino, 0, &PayloadRef::new_partial(950, 4096))
        .expect("persist local-only block");
    let uploaded = PayloadRef::new_partial(951, 4096);
    meta.stage_payload_slice(attr.ino, 1, &uploaded)
        .expect("persist uploaded payload object");
    assert!(
        meta.mark_regular_file_overlay_object_uploaded_if_matches(attr.ino, 1, uploaded.object_id)
            .expect("mark uploaded object"),
        "expected uploaded object transition to apply"
    );

    meta.unlink(ROOT_INO, b"delete-before-publish.bin")
        .expect("persist delete-before-publish lifecycle");

    let lifecycle = meta
        .get_inode_lifecycle(attr.ino)
        .expect("load lifecycle tombstone")
        .expect("deleted-before-publish lifecycle should persist");
    assert_eq!(lifecycle.state, InodeLifecycleState::DeletedBeforePublish);
    assert!(
        !lifecycle.deferred_cleanup_object_ids.is_empty(),
        "test setup should preserve uploaded object ids for deferred cleanup"
    );

    let snapshot = vfs.zeroio_runtime_snapshot();
    assert_eq!(snapshot.deferred_delete_backlog_epochs, 1);
    assert!(
        snapshot.deferred_delete_backlog_objects > 0,
        "deleted-before-publish epochs should retain uploaded objects for deferred cleanup"
    );

    let entry = snapshot
        .drilldown
        .iter()
        .find(|entry| entry.ino == attr.ino)
        .expect("deleted-before-publish inode should remain drill-down visible");
    assert_eq!(
        entry.lifecycle_state,
        InodeLifecycleState::DeletedBeforePublish
    );
    assert_eq!(entry.status, "awaiting_deferred_cleanup");
    assert!(entry.deferred_cleanup_object_count > 0);
}

#[test]
fn epoch_finalizer_publishes_slice_graph_zero_length_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-zero-length");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"slice-graph-finalizer-zero-length.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = b"slice-graph-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize unpublished slice payloads before publish setup");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);

        let mut truncated_attr = meta.get_attr(attr.ino).expect("load published attr");
        truncated_attr.size = 0;
        truncated_attr.content_epoch = truncated_attr.content_epoch.saturating_add(1);
        truncated_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&truncated_attr)
            .expect("stage slice-graph zero-length epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("remove unpublished slice graph for zero-length truncate");

        assert!(
            !meta
                .has_local_regular_file_overlay(attr.ino)
                .expect("query overlay state"),
            "regression setup should exercise a payload-free slice-graph epoch"
        );

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        let lifecycle = meta
            .get_inode_lifecycle(attr.ino)
            .expect("load lifecycle")
            .expect("slice-graph epoch should keep lifecycle state");
        assert_eq!(unpublished_attr.size, 0);
        assert_ne!(
            unpublished_attr.upload_state,
            MetaUploadState::Ready,
            "regression setup should keep the staged epoch on a non-ready upload path"
        );
        assert_ne!(
            unpublished_attr.committed_content_epoch, unpublished_attr.content_epoch,
            "regression setup should leave the zero-length epoch unpublished"
        );
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "payload-free slice-graph zero-length epochs should become publish-ready immediately"
        );
        assert_eq!(
            meta.lookup_payload_slices(attr.ino)
                .expect("load unpublished graph")
                .len(),
            0,
            "regression setup should not require remote object visibility checks"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize zero-length slice-graph epoch");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "slice-graph zero-length epoch should still publish"
        );

        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        assert_eq!(published_attr.size, 0);
        assert_eq!(published_attr.committed_size, 0);
        assert_eq!(published_attr.upload_state, MetaUploadState::Ready);
        assert_eq!(
            published_attr.committed_content_epoch,
            published_attr.content_epoch
        );
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "publication that truncates visible size to zero must invalidate mounted kernel data so cached pages past EOF are dropped"
        );
    });
}

#[test]
fn epoch_finalizer_invalidates_kernel_when_published_dirty_attr_was_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-dirty-attr-invalidate");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"slice-graph-finalizer-dirty-attr.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = b"slice-graph-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize unpublished slice payloads before publish setup");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);

        let mut staged_attr = meta.get_attr(attr.ino).expect("load published attr");
        staged_attr.size = 0;
        staged_attr.content_epoch = staged_attr.content_epoch.saturating_add(1);
        staged_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&staged_attr)
            .expect("stage slice-graph zero-length epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("remove unpublished slice graph for zero-length truncate");

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, unpublished_attr.clone());
        assert!(
            !vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(attr.ino),
            "regression setup should exercise the dirty-attr resolver path without attr cache"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize zero-length dirty-attr epoch");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "slice-graph zero-length epoch should still publish"
        );
        assert!(
            !vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .contains_key(&attr.ino),
            "finalization should clear the matching dirty attr"
        );
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "a dirty attr visible to getattr must invalidate the kernel after publication"
        );
    });
}

#[test]
fn epoch_finalizer_invalidates_kernel_when_newer_dirty_attr_remains_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "graph-finalizer-newer-dirty-attr-invalidate");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"slice-graph-finalizer-newer-dirty-attr.bin",
                0,
                0,
                0o644,
            )
            .expect("create test file");
        let payload = b"slice-graph-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize unpublished slice payloads before publish setup");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);

        let mut staged_attr = meta.get_attr(attr.ino).expect("load published attr");
        staged_attr.size = 0;
        staged_attr.content_epoch = staged_attr.content_epoch.saturating_add(1);
        staged_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&staged_attr)
            .expect("stage slice-graph zero-length epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("remove unpublished slice graph for zero-length truncate");

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        let mut newer_dirty_attr = unpublished_attr.clone();
        newer_dirty_attr.content_epoch = newer_dirty_attr.content_epoch.saturating_add(1);
        newer_dirty_attr.size = newer_dirty_attr.size.saturating_add(1);
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, newer_dirty_attr.clone());
        assert!(
            !vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(attr.ino),
            "regression setup should not rely on attr_read_cache invalidation"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize zero-length dirty-attr epoch");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "slice-graph zero-length epoch should still publish"
        );
        assert_eq!(
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .get(&attr.ino)
                .map(|attr| attr.content_epoch),
            Some(newer_dirty_attr.content_epoch),
            "finalization must preserve a newer concurrent dirty attr"
        );
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "a visible dirty attr must invalidate the kernel even when it belongs to a newer epoch"
        );
    });
}

#[test]
fn epoch_finalizer_invalidates_kernel_when_publication_advances_visible_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-size-advance-invalidate");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-size-advance.bin", 0, 0, 0o644)
            .expect("create test file");
        let baseline_attr = meta.get_attr(attr.ino).expect("load baseline attr");
        assert_eq!(baseline_attr.size, 0);
        assert_eq!(baseline_attr.committed_size, 0);
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);
        vfs.state.namespace.attr_read_cache.lock().remove(attr.ino);

        let mut staged_attr = baseline_attr.clone();
        staged_attr.size = 4096;
        staged_attr.content_epoch = staged_attr.content_epoch.saturating_add(1);
        staged_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&staged_attr)
            .expect("stage sparse size-increase epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("stage sparse graph for size-increase epoch");
        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished append attr");
        assert!(
            unpublished_attr.committed_size < unpublished_attr.size,
            "regression setup should publish a larger visible file"
        );
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);
        vfs.state.namespace.attr_read_cache.lock().remove(attr.ino);

        let (append_epoch, _) = mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        assert_eq!(append_epoch, unpublished_attr.content_epoch);
        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            append_epoch,
        )
        .await
        .expect("finalize append epoch");
        assert!(matches!(status, super::commit::EpochFinalizeStatus::Published));
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "publication that advances visible size must invalidate mounted kernel data even without dirty attr cache"
        );
        let published = meta.get_attr(attr.ino).expect("load published append attr");
        assert_eq!(published.size, 4096);
        assert_eq!(published.committed_size, published.size);
    });
}

#[test]
fn epoch_finalizer_invalidates_kernel_when_publication_shrinks_visible_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-size-shrink-invalidate");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-size-shrink.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = vec![0xA5; 8192];
        vfs.write(attr.ino, 0, &payload)
            .await
            .expect("write baseline payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize baseline payload");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);
        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        assert_eq!(published_attr.size, 8192);
        assert_eq!(published_attr.committed_size, 8192);

        vfs.setattr(attr.ino, SetattrArgs::size_only(4096))
            .expect("stage local shrink epoch");
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);
        vfs.state.namespace.attr_read_cache.lock().remove(attr.ino);
        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished shrink attr");
        assert!(
            unpublished_attr.committed_size > unpublished_attr.size,
            "regression setup should publish a smaller visible file"
        );

        let (shrink_epoch, _) = mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        assert_eq!(shrink_epoch, unpublished_attr.content_epoch);
        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            shrink_epoch,
        )
        .await
        .expect("finalize shrink epoch");
        assert!(matches!(
            status,
            super::commit::EpochFinalizeStatus::Published
        ));
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "publication that shrinks visible size must invalidate mounted kernel data even without dirty attr cache"
        );
        let published = meta.get_attr(attr.ino).expect("load published shrink attr");
        assert_eq!(published.size, 4096);
        assert_eq!(published.committed_size, published.size);
    });
}

#[test]
fn epoch_finalizer_keeps_data_cache_for_own_local_write_publication() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-local-write-attr-only");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-local-write-attr-only.bin", 0, 0, 0o644)
            .expect("create test file");
        let baseline_attr = meta.get_attr(attr.ino).expect("load baseline attr");
        let mut staged_attr = baseline_attr.clone();
        staged_attr.size = 4096;
        staged_attr.content_epoch = staged_attr.content_epoch.saturating_add(1);
        staged_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&staged_attr)
            .expect("stage local sparse publication");
        meta.remove_regular_file_graph(attr.ino)
            .expect("stage sparse graph for local publication");
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);
        vfs.state.namespace.attr_read_cache.lock().remove(attr.ino);

        let (content_epoch, uploaded_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        assert!(
            uploaded_blocks.is_empty(),
            "regression setup should avoid remote object visibility waits"
        );
        vfs.state.incarnation.live_extent_records.lock().insert(
            (attr.ino, 0),
            ExtentRecordId {
                object_id: 0xABCD,
                content_epoch,
            },
        );
        assert!(
            vfs.live_extent_records_for_inode(attr.ino)
                .iter()
                .any(|(_, _, epoch)| *epoch == content_epoch),
            "local write publication should retain live extent records until finalization"
        );
        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status =
            super::commit::CommitCoordinator::finalize_inode_epoch(&vfs, attr.ino, content_epoch)
                .await
                .expect("finalize local write publication");
        assert!(matches!(
            status,
            super::commit::EpochFinalizeStatus::Published
        ));
        assert_eq!(
            invalidator.snapshot(),
            Vec::<Ino>::new(),
            "publishing bytes already accepted by this mount must not purge the kernel data cache"
        );
        assert_eq!(
            invalidator.attr_snapshot(),
            vec![attr.ino],
            "local write publication should still refresh kernel attributes"
        );
    });
}

#[test]
fn epoch_finalizer_keeps_captured_larger_attr_when_same_epoch_current_attr_is_smaller() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "finalizer-same-epoch-smaller-attr");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"finalizer-same-epoch-smaller-attr.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writer");
        let mut captured_newer_attr = attr.clone();
        captured_newer_attr.content_epoch = captured_newer_attr.content_epoch.saturating_add(1);
        captured_newer_attr.size = 16 * 1024 * 1024 * 1024_u64;
        captured_newer_attr.upload_state = MetaUploadState::Updating;
        let mut stale_current_attr = captured_newer_attr.clone();
        stale_current_attr.size = 4 * 1024 * 1024 * 1024_u64;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, stale_current_attr);

        super::commit::CommitCoordinator::restore_newer_dirty_attr_and_refresh_open_handles(
            &vfs,
            ino,
            captured_newer_attr.clone(),
        );

        let open_attr = vfs
            .get_open_file(fh)
            .and_then(|open_file| open_file.open_attr())
            .expect("open handle attr after finalizer refresh");
        assert_eq!(
            (open_attr.content_epoch, open_attr.size),
            (captured_newer_attr.content_epoch, captured_newer_attr.size),
            "a same-epoch smaller dirty snapshot must not roll back the open handle EOF"
        );
        let canonical_dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("canonical dirty attr after finalizer refresh");
        assert_eq!(
            (
                canonical_dirty_attr.content_epoch,
                canonical_dirty_attr.size
            ),
            (captured_newer_attr.content_epoch, captured_newer_attr.size),
            "a same-epoch smaller dirty snapshot must not remain the canonical inode EOF"
        );

        vfs.state.namespace.dirty_attrs.lock().remove(&ino);
        vfs.release_fh(fh).await.expect("release writer");
    });
}

#[test]
fn epoch_finalizer_preserves_larger_current_attr_at_same_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "finalizer-same-epoch-larger-attr");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"finalizer-same-epoch-larger-attr.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writer");
        let mut captured_newer_attr = attr.clone();
        captured_newer_attr.content_epoch = captured_newer_attr.content_epoch.saturating_add(1);
        captured_newer_attr.size = 4 * 1024 * 1024 * 1024_u64;
        captured_newer_attr.upload_state = MetaUploadState::Updating;
        let mut current_larger_attr = captured_newer_attr.clone();
        current_larger_attr.size = 16 * 1024 * 1024 * 1024_u64;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, current_larger_attr.clone());

        super::commit::CommitCoordinator::restore_newer_dirty_attr_and_refresh_open_handles(
            &vfs,
            ino,
            captured_newer_attr,
        );

        let open_attr = vfs
            .get_open_file(fh)
            .and_then(|open_file| open_file.open_attr())
            .expect("open handle attr after finalizer refresh");
        let canonical_dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("canonical dirty attr after finalizer refresh");
        assert_eq!(
            (
                canonical_dirty_attr.content_epoch,
                canonical_dirty_attr.size
            ),
            (current_larger_attr.content_epoch, current_larger_attr.size),
            "the finalizer must preserve a larger concurrent tail at the same epoch"
        );
        assert_eq!(
            (open_attr.content_epoch, open_attr.size),
            (current_larger_attr.content_epoch, current_larger_attr.size),
            "the open handle must observe the larger concurrent tail"
        );

        vfs.state.namespace.dirty_attrs.lock().remove(&ino);
        vfs.release_fh(fh).await.expect("release writer");
    });
}

#[test]
fn epoch_finalizer_refreshes_open_writer_before_followup_tail_write() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "finalizer-refreshes-open-writer");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"finalizer-refreshes-open-writer.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writer");
        let published_prefix_len = 4096_u64;
        let mut staged_attr = meta.get_attr(ino).expect("load created attr");
        staged_attr.size = published_prefix_len;
        staged_attr.content_epoch = staged_attr.content_epoch.saturating_add(1);
        staged_attr.upload_state = MetaUploadState::Uploading;
        meta.set_attr(&staged_attr)
            .expect("stage sparse prefix publication");
        meta.remove_regular_file_graph(ino)
            .expect("stage sparse prefix graph");
        vfs.state.namespace.dirty_attrs.lock().remove(&ino);
        vfs.state.namespace.attr_read_cache.lock().remove(ino);
        let (published_epoch, uploaded_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(uploaded_blocks.is_empty());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            ino,
            published_epoch,
        )
        .await
        .expect("finalize prefix while writer stays open");
        assert!(matches!(
            status,
            super::commit::EpochFinalizeStatus::Published
        ), "prefix finalization did not publish; attr={:?}; lifecycle={:?}; pending={:?}; writer_dirty={}; local_dirty={}; inflight={}",
            meta.get_attr(ino),
            meta.get_inode_lifecycle(ino),
            vfs.pending_uploads_for_inode(ino),
            vfs.state.writer_stack.has_dirty_writers(ino),
            vfs.has_pending_local_dirty_payloads_for_inode(ino),
            vfs.state.deferred_materialize_inflight.contains(ino),
        );
        let published_attr = meta.get_attr(ino).expect("load published prefix attr");
        assert_eq!(published_attr.size, published_prefix_len);
        assert_eq!(published_attr.committed_size, published_attr.size);

        let tail = b" accepted tail";
        vfs.write_fh(fh, ino, published_attr.size, tail)
            .await
            .expect("write tail through the still-open handle");

        let dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("follow-up tail should leave a dirty attr");
        assert!(
            dirty_attr.content_epoch > published_attr.content_epoch,
            "a write after prefix publication must start a new epoch instead of reusing the stale open-time attr: published={published_attr:?}, dirty={dirty_attr:?}"
        );
        assert_eq!(
            dirty_attr.size,
            published_prefix_len + tail.len() as u64,
            "the visible size must retain the already-published prefix and the new tail"
        );

        vfs.flush_fh(fh).await.expect("flush follow-up tail");
        let actual = vfs
            .read(ino, 0, (published_prefix_len + tail.len() as u64) as u32)
            .await
            .expect("read complete payload");
        let mut expected = vec![0; published_prefix_len as usize];
        expected.extend_from_slice(tail);
        assert_eq!(actual.as_ref(), expected.as_slice());
        vfs.release_fh(fh).await.expect("release writer");
    });
}

#[test]
fn epoch_finalizer_invalidates_kernel_when_same_size_sparse_graph_changes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-same-size-graph");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-same-size-graph.bin", 0, 0, 0o644)
            .expect("create test file");
        let logical_size = 3 * 1024 * 1024_u64;
        let tail_offset = logical_size - 256;
        let tail_len = 128_u32;
        let tail_start = u32::try_from(tail_offset).expect("tail offset fits in one chunk");
        let tail_end = tail_start + tail_len;

        let mut published_attr = meta.get_attr(attr.ino).expect("load created attr");
        published_attr.size = logical_size;
        published_attr.content_epoch = 1;
        published_attr.upload_state = MetaUploadState::Updating;
        meta.flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(
                ChunkId(0),
                vec![SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(tail_start, tail_end).expect("valid tail payload range"),
                    PayloadRef::new_partial(70_001, tail_len),
                )],
            )],
            Some(&published_attr),
        )
        .expect("stage published sparse tail payload");
        assert!(
            meta.mark_regular_file_overlay_object_uploaded_if_matches(attr.ino, 1, 70_001)
                .expect("mark initial tail uploaded")
        );
        publish_current_file_graph(&meta, attr.ino, 1, &[70_001]);
        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        assert_eq!(published_attr.size, logical_size);
        assert_eq!(published_attr.committed_size, logical_size);

        let mut tombstone_attr = published_attr.clone();
        tombstone_attr.content_epoch = tombstone_attr.content_epoch.saturating_add(1);
        tombstone_attr.upload_state = MetaUploadState::Updating;
        meta.flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(
                ChunkId(0),
                vec![SliceRecord::tombstone(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(tail_start, tail_end).expect("valid tail tombstone range"),
                )],
            )],
            Some(&tombstone_attr),
        )
        .expect("stage same-size sparse tail tombstone");
        let tail_epoch = tombstone_attr.content_epoch;
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);
        vfs.state.namespace.attr_read_cache.lock().remove(attr.ino);
        assert_eq!(
            meta.get_attr(attr.ino).expect("load staged tombstone attr").size,
            logical_size,
            "regression setup should publish graph coverage without changing EOF"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs, attr.ino, tail_epoch,
        )
        .await
        .expect("finalize same-size sparse tail");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "expected tail payload publication, got status {} with attr {:?}",
            match status {
                super::commit::EpochFinalizeStatus::Published => "published",
                super::commit::EpochFinalizeStatus::WaitingForUploads => "waiting_uploads",
                super::commit::EpochFinalizeStatus::Deferred => "deferred",
                super::commit::EpochFinalizeStatus::Skipped => "skipped",
            },
            meta.get_attr(attr.ino)
                .expect("load attr after finalize attempt")
        );
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "publication that changes payload coverage at the same logical size must invalidate cached reads"
        );
    });
}

#[test]
fn epoch_finalizer_invalidates_kernel_when_local_matching_sparse_overlay_changes_same_size_graph() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "graph-finalizer-local-sparse-same-size-graph");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"graph-finalizer-local-sparse-same-size-graph.bin",
                0,
                0,
                0o644,
            )
            .expect("create test file");
        let logical_size = 3 * 1024 * 1024_u64;
        let tail_offset = logical_size - 256;
        let tail_len = 128_u32;
        let tail_start = u32::try_from(tail_offset).expect("tail offset fits in one chunk");
        let tail_end = tail_start + tail_len;

        let mut published_attr = meta.get_attr(attr.ino).expect("load created attr");
        published_attr.size = logical_size;
        published_attr.content_epoch = 1;
        published_attr.upload_state = MetaUploadState::Updating;
        meta.flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(
                ChunkId(0),
                vec![SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(tail_start, tail_end).expect("valid tail payload range"),
                    PayloadRef::new_partial(70_001, tail_len),
                )],
            )],
            Some(&published_attr),
        )
        .expect("stage published sparse tail payload");
        assert!(
            meta.mark_regular_file_overlay_object_uploaded_if_matches(attr.ino, 1, 70_001)
                .expect("mark initial tail uploaded")
        );
        publish_current_file_graph(&meta, attr.ino, 1, &[70_001]);

        let mut tombstone_attr = meta.get_attr(attr.ino).expect("load published attr");
        tombstone_attr.content_epoch = tombstone_attr.content_epoch.saturating_add(1);
        tombstone_attr.upload_state = MetaUploadState::Updating;
        meta.flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(
                ChunkId(0),
                vec![SliceRecord::tombstone(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(tail_start, tail_end).expect("valid tail tombstone range"),
                )],
            )],
            Some(&tombstone_attr),
        )
        .expect("stage same-size sparse tail tombstone");
        let content_epoch = tombstone_attr.content_epoch;
        vfs.state.incarnation.live_extent_records.lock().insert(
            (attr.ino, 0),
            ExtentRecordId {
                object_id: 70_001,
                content_epoch,
            },
        );
        vfs.state.namespace.dirty_attrs.lock().remove(&attr.ino);
        vfs.state.namespace.attr_read_cache.lock().remove(attr.ino);

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        let status =
            super::commit::CommitCoordinator::finalize_inode_epoch(&vfs, attr.ino, content_epoch)
                .await
                .expect("finalize local matching same-size sparse replacement");
        assert!(matches!(
            status,
            super::commit::EpochFinalizeStatus::Published
        ));
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "local matching sparse overlay publication can change cached bytes without changing EOF"
        );
    });
}

#[test]
fn remote_ready_attr_superseding_local_dirty_state_invalidates_kernel_inode() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "remote-ready-supersede-kernel-inval");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"remote-ready-supersede-kernel-inval.bin", 0, 0, 0o644)
            .expect("create test file");
        let winner = b"remote winner payload";
        vfs.write(attr.ino, 0, winner)
            .await
            .expect("write winner payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize winner payload");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);
        vfs.cancel_pending_uploads_for_inode(attr.ino);
        vfs.clear_pending_payloads_for_inode(attr.ino);
        vfs.state.writer_stack.clear_inode(attr.ino);
        vfs.remove_live_extents_for_inode(attr.ino);
        vfs.remove_pending_epoch_finalizations_for_inode(attr.ino);
        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        assert_eq!(published_attr.upload_state, MetaUploadState::Ready);

        let mut stale_dirty_attr = published_attr.clone();
        stale_dirty_attr.upload_state = MetaUploadState::Updating;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, stale_dirty_attr);

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        vfs.refresh_inode_incarnation_state_from_attr(attr.ino, &published_attr);

        assert!(
            !vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .contains_key(&attr.ino),
            "authoritative ready metadata should clear superseded local dirty attrs"
        );
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "remote ready metadata that supersedes locally visible dirty state must invalidate mounted kernel data"
        );
    });
}

#[test]
fn authoritative_ready_metadata_does_not_clear_newer_local_dirty_attr() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "ready-keeps-newer-dirty-attr");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"newer-dirty-attr.txt", 0, 0, 0o644)
            .expect("create file");
        vfs.write(attr.ino, 0, b"published")
            .await
            .expect("write base");
        vfs.flush(attr.ino).await.expect("flush base");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);
        vfs.cancel_pending_uploads_for_inode(attr.ino);
        vfs.clear_pending_payloads_for_inode(attr.ino);
        vfs.state.writer_stack.clear_inode(attr.ino);
        vfs.remove_live_extents_for_inode(attr.ino);
        vfs.remove_pending_epoch_finalizations_for_inode(attr.ino);

        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        let mut newer_dirty_attr = published_attr.clone();
        newer_dirty_attr.size = published_attr.size + 4096;
        newer_dirty_attr.content_epoch = published_attr.committed_content_epoch + 1;
        newer_dirty_attr.upload_state = MetaUploadState::Updating;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, newer_dirty_attr.clone());

        vfs.refresh_inode_incarnation_state_from_attr(attr.ino, &published_attr);

        let retained = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&attr.ino)
            .cloned()
            .expect("newer local dirty attr must remain visible");
        assert_eq!(retained.size, newer_dirty_attr.size);
        assert_eq!(
            vfs.getattr(attr.ino).expect("visible attr").size,
            newer_dirty_attr.size
        );
    });
}

#[test]
fn authoritative_ready_metadata_does_not_clear_same_size_newer_dirty_attr() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "ready-keeps-same-size-newer-dirty-attr");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"same-size-newer-dirty-attr.txt", 0, 0, 0o644)
            .expect("create file");
        vfs.write(attr.ino, 0, b"published")
            .await
            .expect("write base");
        vfs.flush(attr.ino).await.expect("flush base");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);
        vfs.cancel_pending_uploads_for_inode(attr.ino);
        vfs.clear_pending_payloads_for_inode(attr.ino);
        vfs.state.writer_stack.clear_inode(attr.ino);
        vfs.remove_live_extents_for_inode(attr.ino);
        vfs.remove_pending_epoch_finalizations_for_inode(attr.ino);

        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        let mut newer_dirty_attr = published_attr.clone();
        newer_dirty_attr.content_epoch = published_attr.committed_content_epoch + 1;
        newer_dirty_attr.upload_state = MetaUploadState::Updating;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, newer_dirty_attr.clone());

        vfs.refresh_inode_incarnation_state_from_attr(attr.ino, &published_attr);

        let retained = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&attr.ino)
            .cloned()
            .expect("same-size newer dirty attr must remain visible");
        assert_eq!(retained.size, newer_dirty_attr.size);
        assert_eq!(retained.content_epoch, newer_dirty_attr.content_epoch);
    });
}

#[test]
fn rename_invalidates_moved_inode_attr_cache_without_kernel_callback() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "rename-invalidates-moved-inode");

    let attr = vfs
        .create(ROOT_INO, b"rename-cache-source.txt", 0, 0, 0o644)
        .expect("create test file");
    let mut stale_attr = meta.get_attr(attr.ino).expect("load attr");
    stale_attr.size = 0;
    vfs.state.namespace.attr_read_cache.lock().put(
        attr.ino,
        stale_attr,
        ATTR_CACHE_TTL,
        std::time::Instant::now(),
    );

    vfs.rename(
        ROOT_INO,
        b"rename-cache-source.txt",
        ROOT_INO,
        b"rename-cache-target.txt",
    )
    .expect("rename test file");

    assert!(
        !vfs.state
            .namespace
            .attr_read_cache
            .lock()
            .contains(attr.ino),
        "successful rename must drop cached attrs for the moved inode"
    );
}

#[test]
fn open_file_with_known_attr_uses_supplied_create_attr() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "open-known-attr");

    let attr = vfs
        .create(ROOT_INO, b"known-open-attr.txt", 0, 0, 0o644)
        .expect("create test file");
    let mut stale_attr = attr.clone();
    stale_attr.size = 99;
    vfs.state.namespace.attr_read_cache.lock().put(
        attr.ino,
        stale_attr,
        ATTR_CACHE_TTL,
        std::time::Instant::now(),
    );

    let fh = vfs
        .open_file_with_known_attr(&attr, OpenFlags::RDWR, false)
        .expect("open file with create attr");
    let open_file = vfs.get_open_file(fh).expect("open handle");

    assert_eq!(open_file.open_attr().expect("open attr").size, attr.size);
}

#[test]
fn handle_write_uses_open_attr_before_meta_lookup() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "handle-write-open-attr");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"handle-write-open-attr.txt", 0, 0, 0o644)
            .expect("create test file");
        let fh = vfs
            .open_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .expect("open file with create attr");

        let mut stale_attr = attr.clone();
        stale_attr.size = 99;
        vfs.state.namespace.attr_read_cache.lock().put(
            attr.ino,
            stale_attr,
            ATTR_CACHE_TTL,
            std::time::Instant::now(),
        );

        vfs.write_fh_with_deferred_materialize(
            fh,
            attr.ino,
            0,
            b"x",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write through open handle");

        let dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&attr.ino)
            .cloned()
            .expect("dirty attr after write");
        assert_eq!(
            dirty_attr.size, 1,
            "first handle write should use the fresh open attr, not a stale meta lookup"
        );

        vfs.release_fh(fh).await.expect("release file handle");
    });
}

#[test]
fn newer_dirty_epoch_keeps_its_upload_state_during_write_merge() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "newer-dirty-epoch-upload-state");
    let attr = vfs
        .create(1, b"newer-dirty-epoch-upload-state.bin", 0, 0, 0o644)
        .expect("create test file");

    let mut newer = attr.clone();
    newer.content_epoch = attr.content_epoch.saturating_add(1);
    newer.upload_state = MetaUploadState::Ready;
    let mut older = attr;
    older.upload_state = MetaUploadState::Uploading;

    super::sync_ops::merge_write_dirty_attr(&mut newer, &older);

    assert_eq!(newer.upload_state, MetaUploadState::Ready);
}

#[test]
fn equal_dirty_epoch_updates_upload_state_from_non_ready_write_base() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "equal-dirty-epoch-upload-state");
    let attr = vfs
        .create(1, b"equal-dirty-epoch-upload-state.bin", 0, 0, 0o644)
        .expect("create test file");

    let mut current = attr.clone();
    current.upload_state = MetaUploadState::Ready;
    let mut write_base = attr;
    write_base.upload_state = MetaUploadState::Uploading;

    super::sync_ops::merge_write_dirty_attr(&mut current, &write_base);

    assert_eq!(current.upload_state, MetaUploadState::Uploading);
}

#[test]
fn open_created_file_with_known_attr_does_not_recheck_mutation_authority() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "open-created-known-attr");

    let attr = vfs
        .create(ROOT_INO, b"created-open-known-attr.txt", 0, 0, 0o644)
        .expect("create test file");
    vfs.set_metadata_authority_status_for_test(Some(MetadataAuthorityStatus::read_only()));

    assert!(
        vfs.open_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .is_err(),
        "regular write-open still enforces current mutation authority"
    );

    let fh = vfs
        .open_created_file_with_known_attr(&attr, OpenFlags::RDWR, false)
        .expect("open freshly created file with create attr");
    let open_file = vfs.get_open_file(fh).expect("open handle");

    assert_eq!(open_file.open_attr().expect("open attr").ino, attr.ino);
}

#[test]
fn remote_ready_attr_with_same_epoch_preserves_pending_local_write() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "remote-ready-same-epoch-keeps-local");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"remote-ready-same-epoch-keeps-local.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;

        vfs.write_with_local_sync_policy(
            ino,
            64 * 1024,
            b"pending local tail",
            LocalSyncPolicy::Strict,
        )
        .await
        .expect("write pending local payload");

        let local_attr = meta.get_attr(ino).expect("load local pending attr");
        let pending_epoch = local_attr.content_epoch;
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty()
                || vfs
                    .state
                    .upload
                    .pending_epoch_finalizations
                    .lock()
                    .iter()
                    .any(|pending| pending.0 == ino && pending.1 == pending_epoch),
            "setup should leave local content work in the pending epoch"
        );

        let mut remote_ready = local_attr.clone();
        remote_ready.upload_state = MetaUploadState::Ready;
        remote_ready.committed_size = remote_ready.size;
        remote_ready.committed_content_epoch = remote_ready.content_epoch;
        meta.set_attr(&remote_ready)
            .expect("simulate same-epoch remote ready attr");

        vfs.refresh_inode_incarnation_state_from_attr(ino, &remote_ready);

        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty()
                || vfs
                    .state
                    .upload
                    .pending_epoch_finalizations
                    .lock()
                    .iter()
                    .any(|pending| pending.0 == ino && pending.1 == pending_epoch),
            "same-epoch remote ready metadata must not drop the local payload publication work"
        );
    });
}

#[test]
fn remote_ready_attr_with_same_epoch_preserves_buffered_writer_stack_tail() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "remote-ready-same-epoch-keeps-writer");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"remote-ready-same-epoch-keeps-writer.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;

        vfs.write(ino, 0, b"published-prefix")
            .await
            .expect("write base payload");
        vfs.flush(ino).await.expect("flush base payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
        vfs.cancel_pending_uploads_for_inode(ino);
        vfs.clear_pending_payloads_for_inode(ino);
        vfs.state.writer_stack.clear_inode(ino);
        vfs.remove_live_extents_for_inode(ino);
        vfs.remove_pending_epoch_finalizations_for_inode(ino);

        let published_attr = meta.get_attr(ino).expect("load published attr");
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let tail_offset = published_attr.size + 4096;
        let tail = b"local writer-stack tail";

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            tail_offset,
            tail,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("buffer local tail");
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "setup should keep the tail only in the mounted writer stack"
        );

        let dirty_epoch = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .expect("dirty attr")
            .content_epoch;
        let mut remote_ready = published_attr.clone();
        remote_ready.size = tail_offset + tail.len() as u64;
        remote_ready.committed_size = remote_ready.size;
        remote_ready.content_epoch = dirty_epoch;
        remote_ready.committed_content_epoch = dirty_epoch;
        remote_ready.upload_state = MetaUploadState::Ready;

        vfs.refresh_inode_incarnation_state_from_attr(ino, &remote_ready);

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "same-epoch remote ready metadata must not discard local writer-stack bytes"
        );
        let actual = vfs
            .read(ino, tail_offset, tail.len() as u32)
            .await
            .expect("read local tail");
        assert_eq!(actual.as_ref(), tail);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn same_size_strict_sparse_tail_write_advances_past_committed_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "same-size-strict-tail-publish");
    let _guard = rt.enter();

    rt.block_on(async {
        let mut attr = vfs
            .create(1, b"same-size-strict-tail-publish.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let logical_size = 3_146_158;
        let tail_offset = 3_145_900;
        let payload = b"strict sparse tail payload";

        attr.size = logical_size;
        attr.committed_size = logical_size;
        attr.content_epoch = 1;
        attr.committed_content_epoch = 1;
        attr.upload_state = MetaUploadState::Ready;
        meta.set_attr(&attr).expect("seed ready sparse attr");
        vfs.refresh_inode_incarnation_state_from_attr(ino, &attr);

        vfs.write_with_local_sync_policy(ino, tail_offset, payload, LocalSyncPolicy::Strict)
            .await
            .expect("write strict tail payload");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            !blocks.is_empty(),
            "strict tail write should stage an overlay payload"
        );

        let pending_attr = meta.get_attr(ino).expect("load pending tail attr");
        assert_eq!(pending_attr.content_epoch, content_epoch);
        assert!(
            pending_attr.content_epoch > pending_attr.committed_content_epoch,
            "same-size payload writes still need a publishable epoch: {pending_attr:?}"
        );
    });
}

#[test]
fn size_change_after_unpublished_flush_advances_content_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "setattr-size-advances-epoch");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-size-advances-epoch.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = b"unpublished payload";

        vfs.write(ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");

        let flushed_attr = meta.get_attr(ino).expect("load flushed attr");
        let old_epoch = flushed_attr.content_epoch;
        let old_objects = meta
            .get_regular_file_overlay_object_ids(ino)
            .expect("load old overlay object ids");
        assert!(
            !old_objects.is_empty(),
            "setup should leave unpublished upload work in the old epoch"
        );

        vfs.setattr(ino, SetattrArgs::size_only(0))
            .expect("truncate flushed unpublished file to zero");

        let truncated_attr = meta.get_attr(ino).expect("load truncated attr");
        assert_eq!(truncated_attr.size, 0);
        assert!(
            truncated_attr.content_epoch > old_epoch,
            "truncate-to-zero must stale queued upload/finalize work from the prior epoch"
        );
        assert!(
            !meta
                .mark_regular_file_overlay_object_uploaded_if_matches(
                    ino,
                    old_epoch,
                    old_objects[0],
                )
                .expect("stale upload completion should be ignored"),
            "old upload completion must not match the newer size-changing epoch"
        );
        assert!(
            meta.lookup_payload_slices(ino)
                .expect("load truncated payload slices")
                .is_empty(),
            "truncate-to-zero should leave no visible payload slices"
        );
    });
}

#[test]
fn handle_overwrite_after_truncate_publishes_recovered_payload_not_tombstone() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "overwrite-truncate-publishes-payload");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"overwrite-truncate-publishes-payload.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";
        let recovered = b"rustfs outage recovered payload";

        vfs.write(ino, 0, initial).await.expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        vfs.write_fh(fh, ino, 0, recovered)
            .await
            .expect("write recovered payload after truncate");
        vfs.flush_fh(fh)
            .await
            .expect("flush recovered overwrite");

        let local = vfs
            .read(ino, 0, recovered.len() as u32)
            .await
            .expect("read accepted recovered payload locally");
        assert_eq!(local.as_ref(), recovered);

        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            !blocks.is_empty(),
            "recovered overwrite must carry payload objects into publication"
        );
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish recovered overwrite epoch"),
            "recovered overwrite epoch should be publish-ready; attr={:?}; lifecycle={:?}; blocks={blocks:?}",
            meta.get_attr(ino).expect("load attr before publish attempt"),
            meta.get_inode_lifecycle(ino).expect("load lifecycle before publish attempt")
        );

        let published_attr = meta.get_attr(ino).expect("load published overwrite attr");
        assert_eq!(published_attr.size, recovered.len() as u64);
        assert_eq!(published_attr.committed_size, recovered.len() as u64);
        assert_eq!(published_attr.upload_state, MetaUploadState::Ready);
        assert_eq!(
            published_attr.committed_content_epoch,
            published_attr.content_epoch
        );

        let spans = meta
            .lookup_regular_file_slices(ino, 0, recovered.len() as u64)
            .expect("load published recovered slices");
        let published_objects: Vec<_> = spans
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(
            published_objects, blocks,
            "published graph must reference the recovered payload objects, not a tombstone-only epoch"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn zero_size_epoch_finalization_defers_while_overwrite_handle_is_open() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zero-finalize-defers-open-handle");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"zero-finalize-defers-open-handle.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";

        vfs.write(ino, 0, initial)
            .await
            .expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        let zero_attr = meta.get_attr(ino).expect("load zero-size attr");
        assert_eq!(zero_attr.size, 0);
        assert_ne!(zero_attr.content_epoch, zero_attr.committed_content_epoch);

        let status =
            commit::CommitCoordinator::finalize_inode_epoch(&vfs, ino, zero_attr.content_epoch)
                .await
                .expect("zero finalization should not fail");
        assert!(
            matches!(status, commit::EpochFinalizeStatus::Deferred),
            "zero-size finalization must wait for the overwrite handle to close"
        );

        let after = meta
            .get_attr(ino)
            .expect("load attr after deferred finalizer");
        assert_eq!(after.size, 0);
        assert_eq!(
            after.committed_size,
            initial.len() as u64,
            "open-handle zero truncate must not publish over the previous payload yet"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn stale_zero_size_dirty_attr_after_recovered_publish_would_revert_to_tombstone() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "stale-zero-dirty-attr-after-publish");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"stale-zero-dirty-attr-after-publish.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";
        let recovered = b"rustfs outage recovered payload";

        vfs.write(ino, 0, initial).await.expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
        let base_attr = meta.get_attr(ino).expect("load initial published attr");

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        let stale_zero_attr = meta.get_attr(ino).expect("load staged zero-size attr");
        assert_eq!(stale_zero_attr.size, 0);
        assert_eq!(stale_zero_attr.committed_size, base_attr.committed_size);

        vfs.write_fh(fh, ino, 0, recovered)
            .await
            .expect("write recovered payload after truncate");
        vfs.flush_fh(fh).await.expect("flush recovered overwrite");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish recovered overwrite epoch"),
            "recovered overwrite epoch should publish before the stale dirty attr retry"
        );

        let published_attr = meta.get_attr(ino).expect("load recovered published attr");
        assert_eq!(published_attr.size, recovered.len() as u64);
        assert_eq!(published_attr.committed_size, recovered.len() as u64);
        assert_eq!(published_attr.committed_content_epoch, content_epoch);

        let mut stale_retry = stale_zero_attr;
        stale_retry.content_epoch = content_epoch;
        stale_retry.upload_state = MetaUploadState::Ready;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, stale_retry);
        vfs.clear_matching_dirty_attr_if_committed(ino, &published_attr);
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "a stale zero-size dirty attr from the truncate half of an overwrite must be discarded after recovered payload publication"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Strict)
            .await
            .expect("retrying local dirty flush after publication should be harmless");
        let final_attr = meta.get_attr(ino).expect("load attr after stale retry");
        assert_eq!(final_attr.size, recovered.len() as u64);
        assert_eq!(final_attr.committed_size, recovered.len() as u64);
        assert_eq!(final_attr.committed_content_epoch, content_epoch);
        let published_objects: Vec<_> = meta
            .lookup_regular_file_slices(ino, 0, recovered.len() as u64)
            .expect("load recovered published graph")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(
            published_objects, blocks,
            "stale zero-size dirty attr retry must not replace recovered payload graph with tombstones"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn stale_newer_zero_size_dirty_attr_after_recovered_publish_is_discarded() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "stale-newer-zero-dirty-attr-after-publish");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"stale-newer-zero-dirty-attr-after-publish.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";
        let recovered = b"rustfs outage recovered payload";

        vfs.write(ino, 0, initial).await.expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
        let base_attr = meta.get_attr(ino).expect("load initial published attr");

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        let stale_zero_attr = meta.get_attr(ino).expect("load staged zero-size attr");
        assert_eq!(stale_zero_attr.size, 0);
        assert_eq!(stale_zero_attr.committed_size, base_attr.committed_size);

        vfs.write_fh(fh, ino, 0, recovered)
            .await
            .expect("write recovered payload after truncate");
        vfs.flush_fh(fh).await.expect("flush recovered overwrite");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish recovered overwrite epoch"),
            "recovered overwrite epoch should publish before the stale dirty attr retry"
        );

        let published_attr = meta.get_attr(ino).expect("load recovered published attr");
        assert_eq!(published_attr.size, recovered.len() as u64);
        assert_eq!(published_attr.committed_size, recovered.len() as u64);

        let mut stale_retry = stale_zero_attr;
        stale_retry.content_epoch = content_epoch.saturating_add(1);
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, stale_retry);
        vfs.clear_matching_dirty_attr_if_committed(ino, &published_attr);
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "a stale zero-size attr that sorts after the recovered publish must not survive as a newer dirty attr"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Strict)
            .await
            .expect("retrying local dirty flush after publication should be harmless");
        let final_attr = meta.get_attr(ino).expect("load attr after stale retry");
        assert_eq!(final_attr.size, recovered.len() as u64);
        assert_eq!(final_attr.committed_size, recovered.len() as u64);
        let published_objects: Vec<_> = meta
            .lookup_regular_file_slices(ino, 0, recovered.len() as u64)
            .expect("load recovered published graph")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(
            published_objects, blocks,
            "stale newer zero-size dirty attr retry must not replace recovered payload graph"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn stale_zero_dirty_attr_with_equal_committed_epoch_after_recovered_publish_is_discarded() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(
        &tmp,
        "stale-zero-equal-committed-epoch-dirty-attr-after-publish",
    );
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"stale-zero-equal-committed-epoch-dirty-attr-after-publish.bin",
                0,
                0,
                0o644,
            )
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";
        let recovered = b"rustfs outage recovered payload";

        vfs.write(ino, 0, initial)
            .await
            .expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        let stale_zero_attr = meta.get_attr(ino).expect("load staged zero-size attr");
        assert_eq!(stale_zero_attr.size, 0);

        vfs.write_fh(fh, ino, 0, recovered)
            .await
            .expect("write recovered payload after truncate");
        vfs.flush_fh(fh).await.expect("flush recovered overwrite");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish recovered overwrite epoch"),
            "recovered overwrite epoch should publish before the stale dirty attr retry"
        );

        let published_attr = meta.get_attr(ino).expect("load recovered published attr");
        assert_eq!(published_attr.size, recovered.len() as u64);
        assert_eq!(published_attr.committed_size, recovered.len() as u64);
        assert_eq!(published_attr.committed_content_epoch, content_epoch);

        let mut stale_retry = stale_zero_attr;
        stale_retry.content_epoch = content_epoch.saturating_add(1);
        stale_retry.committed_content_epoch = published_attr.committed_content_epoch;
        stale_retry.committed_size = published_attr.committed_size;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, stale_retry);

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Strict)
            .await
            .expect("local dirty flush should discard stale equal-committed zero dirty attr");

        let final_attr = meta.get_attr(ino).expect("load attr after stale retry");
        assert_eq!(final_attr.size, recovered.len() as u64);
        assert_eq!(final_attr.committed_size, recovered.len() as u64);
        let published_objects: Vec<_> = meta
            .lookup_regular_file_slices(ino, 0, recovered.len() as u64)
            .expect("load recovered published graph")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(
            published_objects, blocks,
            "equal-committed stale zero attr must not replace recovered payload graph"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn stale_zero_dirty_attr_after_recovered_publish_is_not_persisted_by_flush_fh() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "stale-zero-dirty-attr-not-persisted-by-flush-fh");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"stale-zero-dirty-attr-not-persisted-by-flush-fh.bin",
                0,
                0,
                0o644,
            )
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";
        let recovered = b"rustfs outage recovered payload";

        vfs.write(ino, 0, initial)
            .await
            .expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
        let base_attr = meta.get_attr(ino).expect("load initial published attr");

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        let stale_zero_attr = meta.get_attr(ino).expect("load staged zero-size attr");
        assert_eq!(stale_zero_attr.size, 0);
        assert_eq!(stale_zero_attr.committed_size, base_attr.committed_size);

        vfs.write_fh(fh, ino, 0, recovered)
            .await
            .expect("write recovered payload after truncate");
        vfs.flush_fh(fh).await.expect("flush recovered overwrite");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish recovered overwrite epoch"),
            "recovered overwrite epoch should publish before the stale dirty attr retry"
        );

        let published_attr = meta.get_attr(ino).expect("load recovered published attr");
        assert_eq!(published_attr.size, recovered.len() as u64);
        assert_eq!(published_attr.committed_size, recovered.len() as u64);

        let mut stale_retry = stale_zero_attr;
        stale_retry.content_epoch = content_epoch.saturating_add(1);
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, stale_retry);

        vfs.flush_fh_with_outcome(fh)
            .await
            .expect("flush_fh should discard stale zero dirty attr");

        let final_attr = meta.get_attr(ino).expect("load attr after stale retry");
        assert_eq!(final_attr.size, recovered.len() as u64);
        assert_eq!(final_attr.committed_size, recovered.len() as u64);
        let published_objects: Vec<_> = meta
            .lookup_regular_file_slices(ino, 0, recovered.len() as u64)
            .expect("load recovered published graph")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(
            published_objects, blocks,
            "flush_fh must not replace recovered payload graph with stale tombstones"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn stale_zero_dirty_attr_after_recovered_publish_is_not_persisted_by_local_dirty_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(
        &tmp,
        "stale-zero-dirty-attr-not-persisted-by-local-dirty-flush",
    );
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"stale-zero-dirty-attr-not-persisted-by-local-dirty-flush.bin",
                0,
                0,
                0o644,
            )
            .expect("create test file");
        let ino = attr.ino;
        let initial = b"rustfs outage initial payload";
        let recovered = b"rustfs outage recovered payload";

        vfs.write(ino, 0, initial)
            .await
            .expect("write initial payload");
        vfs.flush(ino).await.expect("flush initial payload");
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
        let base_attr = meta.get_attr(ino).expect("load initial published attr");

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(0))
            .expect("truncate through overwrite handle");
        let stale_zero_attr = meta.get_attr(ino).expect("load staged zero-size attr");
        assert_eq!(stale_zero_attr.size, 0);
        assert_eq!(stale_zero_attr.committed_size, base_attr.committed_size);

        vfs.write_fh(fh, ino, 0, recovered)
            .await
            .expect("write recovered payload after truncate");
        vfs.flush_fh(fh).await.expect("flush recovered overwrite");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish recovered overwrite epoch"),
            "recovered overwrite epoch should publish before the stale dirty attr retry"
        );

        let published_attr = meta.get_attr(ino).expect("load recovered published attr");
        assert_eq!(published_attr.size, recovered.len() as u64);
        assert_eq!(published_attr.committed_size, recovered.len() as u64);

        let mut stale_retry = stale_zero_attr;
        stale_retry.content_epoch = content_epoch.saturating_add(1);
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, stale_retry);

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Strict)
            .await
            .expect("local dirty flush should discard stale zero dirty attr");

        let final_attr = meta.get_attr(ino).expect("load attr after stale retry");
        assert_eq!(final_attr.size, recovered.len() as u64);
        assert_eq!(final_attr.committed_size, recovered.len() as u64);
        let published_objects: Vec<_> = meta
            .lookup_regular_file_slices(ino, 0, recovered.len() as u64)
            .expect("load recovered published graph")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(
            published_objects, blocks,
            "local dirty flush must not replace recovered payload graph with stale tombstones"
        );

        vfs.release_fh(fh).await.expect("release overwrite handle");
    });
}

#[test]
fn handle_backed_extend_after_write_through_upload_work_advances_content_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-extend-after-write-through");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-extend-after-write-through.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, b"sparse-head", LocalSyncPolicy::Strict)
            .await
            .expect("write-through sparse head");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            tail_offset,
            b"sparse-tail-range",
            LocalSyncPolicy::Strict,
        )
        .await
        .expect("write-through sparse tail");

        let payload_epoch = meta.get_attr(ino).expect("load payload attr").content_epoch;
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty()
                || vfs
                    .state
                    .upload
                    .pending_epoch_finalizations
                    .lock()
                    .iter()
                    .any(|pending| pending.0 == ino && pending.1 == payload_epoch),
            "setup should leave old write-through publish work in flight"
        );

        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF after write-through payloads");

        let dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("handle-backed growth should stage a dirty attr");
        assert_eq!(dirty_attr.size, logical_size);
        assert!(
            dirty_attr.content_epoch > payload_epoch,
            "logical-size growth must not share the epoch already queued for payload publication"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_through_same_size_overwrite_queues_new_upload() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "same-size-overwrite-upload");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"same-size-overwrite-upload.bin", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let initial = b"published-data!!";
        let replacement = b"replacement-data";
        assert_eq!(initial.len(), replacement.len());

        let initial_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open initial writer");
        vfs.write_fh_with_deferred_materialize(
            initial_fh,
            ino,
            0,
            initial,
            LocalSyncPolicy::WriteThrough,
        )
        .await
        .expect("write initial payload");
        vfs.release_fh(initial_fh)
            .await
            .expect("release initial writer");

        let (published_epoch, published_objects) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_objects);
        let ready_attr = meta.get_attr(ino).expect("load ready attr");
        assert_eq!(ready_attr.upload_state, MetaUploadState::Ready);
        assert_eq!(ready_attr.committed_size, replacement.len() as u64);

        let overwrite_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open overwrite writer");
        vfs.write_fh_with_deferred_materialize(
            overwrite_fh,
            ino,
            0,
            replacement,
            LocalSyncPolicy::WriteThrough,
        )
        .await
        .expect("write same-size replacement");

        let dirty_attr = meta.get_attr(ino).expect("load replacement attr");
        assert!(
            dirty_attr.content_epoch > ready_attr.content_epoch,
            "same-size replacement must advance content epoch"
        );
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "same-size replacement must queue replacement payload upload"
        );

        vfs.release_fh(overwrite_fh)
            .await
            .expect("release overwrite writer");
    });
}

#[test]
fn write_through_sparse_extend_requeues_all_retained_overlay_uploads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "write-through-sparse-extend-retains");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"write-through-sparse-extend-retains.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = vec![b't'; 258];
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        let head_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open head writer");
        vfs.write_fh_with_deferred_materialize(
            head_fh,
            ino,
            0,
            head,
            LocalSyncPolicy::WriteThrough,
        )
        .await
        .expect("write-through sparse head");
        vfs.release_fh(head_fh).await.expect("release head writer");

        let tail_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open tail writer");
        vfs.write_fh_with_deferred_materialize(
            tail_fh,
            ino,
            tail_offset,
            &tail,
            LocalSyncPolicy::WriteThrough,
        )
        .await
        .expect("write-through sparse tail");
        vfs.release_fh(tail_fh).await.expect("release tail writer");

        let object_ids_for_range = |length| -> studiofs_meta::MetaResult<Vec<u64>> {
            Ok(meta
                .lookup_regular_file_slices(ino, 0, length)?
                .into_iter()
                .filter_map(|span| span.payload.map(|payload| payload.object_id))
                .collect())
        };
        let before_extend_objects = object_ids_for_range(tail_offset + tail.len() as u64)
            .expect("lookup sparse overlay before extend");
        assert_eq!(
            before_extend_objects.len(),
            2,
            "setup should retain both sparse payload ranges before set_len"
        );
        let pending_offsets_before_extend = vfs
            .pending_uploads_for_inode(ino)
            .iter()
            .map(|(offset, _, _)| *offset)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_offsets_before_extend,
            BTreeSet::from([0, tail_offset]),
            "sparse write-through uploads must retain their file offsets"
        );

        let payload_epoch = meta.get_attr(ino).expect("load payload attr").content_epoch;
        let grow_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open grow writer");
        vfs.setattr_with_handle(Some(grow_fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");
        vfs.flush_volatile_writeback_with_outcome(ino, grow_fh)
            .await
            .expect("flush handle-backed grow");

        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        assert_eq!(extended_attr.size, logical_size);
        assert!(
            extended_attr.content_epoch > payload_epoch,
            "set_len after queued payload work must advance the content epoch"
        );

        let pending_after_extend = vfs.pending_uploads_for_inode(ino);
        let pending_object_ids = pending_after_extend
            .iter()
            .map(|(_, object_id, _)| *object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_object_ids,
            before_extend_objects
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            "set_len must requeue every retained sparse payload object for the new epoch"
        );
        assert!(
            pending_after_extend
                .iter()
                .all(|(_, _, content_epoch)| *content_epoch == extended_attr.content_epoch),
            "retained sparse uploads must be queued under the size-change epoch"
        );

        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert_eq!(content_epoch, extended_attr.content_epoch);
        publish_current_file_graph(&meta, ino, content_epoch, &blocks);

        let published_attr = meta.get_attr(ino).expect("load published attr");
        assert_eq!(published_attr.committed_size, logical_size);
        let published_objects =
            object_ids_for_range(logical_size).expect("lookup published sparse graph");
        assert_eq!(
            published_objects, before_extend_objects,
            "published sparse graph must include both retained payload ranges"
        );

        vfs.release_fh(grow_fh).await.expect("release grow writer");
    });
}

#[test]
fn explicit_flush_sparse_extend_requeues_all_retained_overlay_uploads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "explicit-flush-sparse-extend-retains");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"explicit-flush-sparse-extend-retains.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = vec![b't'; 258];
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        let head_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open head writer");
        vfs.write_fh(head_fh, ino, 0, head)
            .await
            .expect("write sparse head");
        vfs.flush_fh(head_fh)
            .await
            .expect("explicitly flush sparse head");
        vfs.release_fh(head_fh).await.expect("release head writer");

        let tail_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open tail writer");
        vfs.write_fh(tail_fh, ino, tail_offset, &tail)
            .await
            .expect("write sparse tail");
        vfs.flush_fh(tail_fh)
            .await
            .expect("explicitly flush sparse tail");
        vfs.release_fh(tail_fh).await.expect("release tail writer");

        let pending_before_extend = vfs
            .pending_uploads_for_inode(ino)
            .into_iter()
            .map(|(offset, _, _)| offset)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_before_extend,
            BTreeSet::from([0, tail_offset]),
            "explicit fsync-style flushes must queue both sparse ranges before set_len"
        );

        let grow_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open grow writer");
        vfs.setattr_with_handle(Some(grow_fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");

        let pending_after_extend = vfs
            .pending_uploads_for_inode(ino)
            .into_iter()
            .map(|(offset, _, _)| offset)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_after_extend,
            BTreeSet::from([0, tail_offset]),
            "set_len must retain both explicitly flushed sparse ranges"
        );

        vfs.release_fh(grow_fh).await.expect("release grow writer");
    });
}

#[test]
fn explicit_flush_sparse_extend_retains_tail_after_head_publication() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "explicit-flush-sparse-after-head-publish");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"explicit-flush-sparse-after-head-publish.bin",
                0,
                0,
                0o644,
            )
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = vec![b't'; 258];
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        let head_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open head writer");
        vfs.write_fh(head_fh, ino, 0, head)
            .await
            .expect("write sparse head");
        vfs.flush_fh(head_fh)
            .await
            .expect("explicitly flush sparse head");
        vfs.release_fh(head_fh).await.expect("release head writer");

        let (head_epoch, head_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, head_epoch, &head_blocks);
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup should model head publication before the sparse tail write"
        );

        let tail_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open tail writer");
        vfs.write_fh(tail_fh, ino, tail_offset, &tail)
            .await
            .expect("write sparse tail");
        vfs.flush_fh(tail_fh)
            .await
            .expect("explicitly flush sparse tail");
        vfs.release_fh(tail_fh).await.expect("release tail writer");

        let pending_before_extend = vfs
            .pending_uploads_for_inode(ino)
            .into_iter()
            .map(|(offset, _, _)| offset)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_before_extend,
            BTreeSet::from([tail_offset]),
            "tail flush after head publication must queue the tail upload"
        );
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "tail flush after head publication must retain a live extent guard"
        );
        let stale_tail_upload = vfs
            .pending_uploads_snapshot()
            .into_iter()
            .find(|op| op.ino == ino && op.offset == tail_offset)
            .expect("tail upload should be pending before sparse extend");

        let grow_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open grow writer");
        vfs.setattr_with_handle(Some(grow_fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");

        let pending_after_extend = vfs
            .pending_uploads_for_inode(ino)
            .into_iter()
            .map(|(offset, _, content_epoch)| (offset, content_epoch))
            .collect::<BTreeSet<_>>();
        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        assert_eq!(
            pending_after_extend,
            BTreeSet::from([(tail_offset, extended_attr.content_epoch)]),
            "set_len after head publication must retain the sparse tail upload under the size-change epoch"
        );
        assert_ne!(
            stale_tail_upload.content_epoch, extended_attr.content_epoch,
            "test setup must keep the pre-extend worker op stale"
        );

        vfs.cached_store().cancel_upload(&stale_tail_upload);
        let pending_after_stale_cancel = vfs
            .pending_uploads_for_inode(ino)
            .into_iter()
            .map(|(offset, _, content_epoch)| (offset, content_epoch))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_after_stale_cancel,
            BTreeSet::from([(tail_offset, extended_attr.content_epoch)]),
            "stale pre-extend upload cancellation must not remove the requeued sparse tail"
        );

        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert_eq!(content_epoch, extended_attr.content_epoch);
        publish_current_file_graph(&meta, ino, content_epoch, &blocks);

        let published_attr = meta.get_attr(ino).expect("load published attr");
        assert_eq!(published_attr.committed_size, logical_size);
        let published_tail = meta
            .lookup_regular_file_slices(ino, tail_offset, tail.len() as u64)
            .expect("lookup published sparse tail")
            .into_iter()
            .filter_map(|span| span.payload)
            .collect::<Vec<_>>();
        assert!(
            !published_tail.is_empty(),
            "published graph after sparse extend must retain the post-prefix tail payload"
        );

        vfs.release_fh(grow_fh).await.expect("release grow writer");
    });
}

#[test]
fn pending_sparse_slice_commit_rebases_when_size_growth_overtakes_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sparse-tail-rebases-after-size-growth");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"sparse-tail-rebases-after-size-growth.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let tail = vec![b't'; 258];
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open tail writer");
        vfs.write_fh(fh, ino, tail_offset, &tail)
            .await
            .expect("write sparse tail");
        vfs.materialize_writer_stack_for_inode(ino)
            .await
            .expect("materialize sparse tail before size growth");

        let stale_dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("tail write should leave dirty attr");
        let mut grown_attr = stale_dirty_attr.clone();
        grown_attr.size = logical_size;
        grown_attr.content_epoch = grown_attr.content_epoch.saturating_add(1);
        grown_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&grown_attr)
            .expect("simulate size growth committing before tail flush");

        vfs.flush_fh(fh)
            .await
            .expect("flush should rebase sparse tail to size-growth epoch");

        let pending = vfs.pending_uploads_for_inode(ino);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, tail_offset);
        assert_eq!(
            pending[0].2, grown_attr.content_epoch,
            "overtaken sparse tail must upload under the size-growth epoch"
        );

        vfs.release_fh(fh).await.expect("release tail writer");
    });
}

#[test]
fn deferred_sparse_tail_rebases_when_size_growth_overtakes_materialization() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "deferred-sparse-tail-rebases-after-size-growth");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"deferred-sparse-tail-rebases-after-size-growth.bin",
                0,
                0,
                0o644,
            )
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = vec![b't'; 258];
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        let (entry, guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should hold deferred materialization before sparse writes");
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open sparse writer");
        vfs.write_fh_with_deferred_materialize(fh, ino, 0, head, LocalSyncPolicy::Opportunistic)
            .await
            .expect("write sparse head");
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            tail_offset,
            &tail,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write sparse tail");
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "held deferred gate should keep sparse writes in the writer stack"
        );

        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF before deferred sparse tail materializes");
        let grown_attr = vfs.getattr(ino).expect("load grown attr");
        assert_eq!(grown_attr.size, logical_size);

        drop(guard);
        vfs.finish_deferred_materialization_task(entry, true);
        vfs.wait_for_deferred_materialization_inflight(ino).await;
        vfs.flush_local_dirty_state_background(ino, LocalSyncPolicy::Opportunistic)
            .await
            .expect("background local-dirty flush should commit deferred sparse slices");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pending_offsets = vfs
                    .pending_uploads_for_inode(ino)
                    .into_iter()
                    .map(|(offset, _, content_epoch)| (offset, content_epoch))
                    .collect::<BTreeSet<_>>();
                if pending_offsets
                    == BTreeSet::from([
                        (0, grown_attr.content_epoch),
                        (tail_offset, grown_attr.content_epoch),
                    ])
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("deferred sparse payloads should rebase to the size-growth epoch");

        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert_eq!(content_epoch, grown_attr.content_epoch);
        publish_current_file_graph(&meta, ino, content_epoch, &blocks);

        let published_attr = meta.get_attr(ino).expect("load published attr");
        assert_eq!(published_attr.committed_size, logical_size);
        let published_offsets = meta
            .lookup_regular_file_slices(ino, 0, logical_size)
            .expect("lookup published sparse graph")
            .into_iter()
            .filter_map(|span| {
                span.payload.map(|_| {
                    chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE)
                        .saturating_add(span.range.start as u64)
                })
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            published_offsets,
            BTreeSet::from([0, tail_offset]),
            "published sparse graph must retain payloads materialized after size growth"
        );

        vfs.release_fh(fh).await.expect("release sparse writer");
    });
}

#[test]
fn explicit_flush_sparse_extend_retains_kernel_zero_filled_tail_write() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "explicit-flush-sparse-zero-filled-tail");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"explicit-flush-sparse-zero-filled-tail.bin",
                0,
                0,
                0o644,
            )
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = vec![b't'; 258];
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        let head_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open head writer");
        vfs.write_fh(head_fh, ino, 0, head)
            .await
            .expect("write sparse head");
        vfs.flush_fh(head_fh)
            .await
            .expect("explicitly flush sparse head");
        vfs.release_fh(head_fh).await.expect("release head writer");

        let (head_epoch, head_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, head_epoch, &head_blocks);

        let mut zero_filled_tail = vec![0; tail_offset as usize];
        zero_filled_tail.extend_from_slice(&tail);
        let tail_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open tail writer");
        vfs.write_fh(tail_fh, ino, 0, &zero_filled_tail)
            .await
            .expect("write kernel zero-filled sparse tail");
        vfs.flush_fh(tail_fh)
            .await
            .expect("explicitly flush zero-filled sparse tail");
        vfs.release_fh(tail_fh).await.expect("release tail writer");

        let tail_upload_before_extend = vfs
            .pending_uploads_snapshot()
            .into_iter()
            .find(|op| op.ino == ino && op.payload_len == tail_offset + tail.len() as u64)
            .expect("zero-filled sparse tail write must leave upload work queued before set_len");
        assert_eq!(
            tail_upload_before_extend.offset, 0,
            "zero-filled write is queued from the start of the uploaded object"
        );
        assert_eq!(
            tail_upload_before_extend.object_file_offset, 0,
            "tail payload is embedded in a larger zero-filled object that starts at file offset 0"
        );
        assert_eq!(
            tail_upload_before_extend.payload_len,
            tail_offset + tail.len() as u64,
            "queued upload must remember the full uploaded object length"
        );

        let grow_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open grow writer");
        vfs.setattr_with_handle(Some(grow_fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");
        let tail_upload_after_extend = vfs
            .pending_uploads_snapshot()
            .into_iter()
            .find(|op| op.ino == ino && op.payload_len == tail_offset + tail.len() as u64)
            .expect("set_len must not drop upload work for a zero-filled sparse tail write");
        assert_eq!(
            tail_upload_after_extend.offset, 0,
            "set_len requeue must preserve the queued object extent offset"
        );
        assert_eq!(
            tail_upload_after_extend.object_file_offset, 0,
            "set_len requeue must preserve the uploaded object's file start"
        );
        assert_eq!(
            tail_upload_after_extend.payload_len,
            tail_offset + tail.len() as u64,
            "set_len requeue must preserve the uploaded object's full length"
        );

        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert_eq!(content_epoch, extended_attr.content_epoch);
        publish_current_file_graph(&meta, ino, content_epoch, &blocks);
        let published_tail = meta
            .lookup_regular_file_slices(ino, tail_offset, tail.len() as u64)
            .expect("lookup published zero-filled tail")
            .into_iter()
            .filter_map(|span| span.payload)
            .collect::<Vec<_>>();
        assert!(
            !published_tail.is_empty(),
            "publishing the size-change epoch must retain the zero-filled sparse tail payload"
        );

        vfs.release_fh(grow_fh).await.expect("release grow writer");
    });
}

#[test]
fn sparse_extend_requeues_retained_overlay_when_runtime_upload_queue_is_empty() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sparse-extend-retains-metadata-overlay");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"sparse-extend-retains-metadata-overlay.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = b"sparse-tail-range";
        let tail_offset = u64::from(vfs.block_size) + 7;
        let logical_size = u64::from(vfs.block_size) * 2 + 123;

        vfs.write(ino, 0, head)
            .await
            .expect("write sparse head");
        vfs.write(ino, tail_offset, tail)
            .await
            .expect("write sparse tail");
        vfs.flush(ino).await.expect("flush sparse overlay");

        let retained_objects = meta
            .lookup_regular_file_slices(ino, 0, tail_offset + tail.len() as u64)
            .expect("lookup retained overlay")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect::<BTreeSet<_>>();
        assert_eq!(retained_objects.len(), 2);
        let payload_epoch = meta.get_attr(ino).expect("load payload attr").content_epoch;

        vfs.cancel_pending_uploads_for_inode(ino);
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup models the post-PUT window where metadata overlay exists but the runtime queue is empty"
        );

        vfs.setattr(ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");

        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        assert!(
            extended_attr.content_epoch > payload_epoch,
            "size growth over retained overlay metadata must advance to a fresh content epoch"
        );
        let pending_after_extend = vfs.pending_uploads_for_inode(ino);
        let pending_object_ids = pending_after_extend
            .iter()
            .map(|(_, object_id, _)| *object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_object_ids, retained_objects,
            "set_len must requeue retained overlay objects even when the runtime queue was empty"
        );
        assert!(
            pending_after_extend
                .iter()
                .all(|(_, _, content_epoch)| *content_epoch == extended_attr.content_epoch),
            "retained uploads must be queued under the size-change epoch"
        );
    });
}

#[test]
fn chunk_boundary_sparse_extend_requeues_retained_overlay_when_runtime_queue_is_empty() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "chunk-boundary-sparse-extend-retains-overlay");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"chunk-boundary-sparse-extend-retains-overlay.bin",
                0,
                0,
                0o644,
            )
            .expect("create sparse file");
        let ino = attr.ino;
        let head = b"sparse-head";
        let tail = b"sparse-tail-range-crossing-chunk-boundary";
        let tail_offset = DEFAULT_CHUNK_SIZE as u64 - 7;
        let logical_size = DEFAULT_CHUNK_SIZE as u64 * 2 + 123;

        vfs.write(ino, 0, head).await.expect("write sparse head");
        vfs.write(ino, tail_offset, tail)
            .await
            .expect("write cross-chunk sparse tail");
        vfs.flush(ino).await.expect("flush sparse overlay");

        let retained_objects = meta
            .lookup_regular_file_slices(ino, 0, tail_offset + tail.len() as u64)
            .expect("lookup retained overlay")
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            retained_objects.len(),
            3,
            "setup should retain head and both cross-chunk tail payload fragments"
        );
        let payload_epoch = meta.get_attr(ino).expect("load payload attr").content_epoch;

        vfs.cancel_pending_uploads_for_inode(ino);
        assert!(vfs.pending_uploads_for_inode(ino).is_empty());

        vfs.setattr(ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");

        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        assert!(
            extended_attr.content_epoch > payload_epoch,
            "chunk-boundary size growth over retained overlay must advance epoch"
        );
        let pending_object_ids = vfs
            .pending_uploads_for_inode(ino)
            .iter()
            .map(|(_, object_id, _)| *object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_object_ids, retained_objects,
            "set_len must requeue retained cross-chunk overlay objects"
        );
    });
}

#[test]
fn zero_filled_sparse_extend_requeues_retained_overlay_when_runtime_queue_is_empty() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) =
        make_unstarted_test_vfs(&tmp, "zero-filled-sparse-extend-retains-overlay");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"zero-filled-sparse-extend-retains-overlay.bin",
                0,
                0,
                0o644,
            )
            .expect("create sparse file");
        let ino = attr.ino;
        let head = vec![0xA1; 233];
        let tail = vec![0xB2; 262];
        let tail_offset = 1_048_569;
        let logical_size = 2_097_275;

        vfs.write(ino, 0, &head).await.expect("write sparse head");
        vfs.flush(ino).await.expect("flush sparse head");
        let (head_epoch, head_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, head_epoch, &head_blocks);

        let mut zero_filled_tail = vec![0; tail_offset as usize];
        zero_filled_tail.extend_from_slice(&tail);
        vfs.write(ino, 0, &zero_filled_tail)
            .await
            .expect("write zero-filled sparse tail");
        vfs.flush(ino).await.expect("flush zero-filled sparse tail");

        let retained_objects = meta
            .lookup_regular_file_slices(ino, 0, tail_offset + tail.len() as u64)
            .expect("lookup retained overlay")
            .into_iter()
            .filter(|span| span.source == ResolvedSliceSource::Overlay)
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect::<BTreeSet<_>>();
        assert!(
            !retained_objects.is_empty(),
            "setup should retain the scenario-sized zero-filled tail overlay object"
        );
        let payload_epoch = meta.get_attr(ino).expect("load payload attr").content_epoch;

        vfs.cancel_pending_uploads_for_inode(ino);
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup models the post-PUT window where the runtime queue is empty"
        );

        vfs.setattr(ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF");

        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        assert!(
            extended_attr.content_epoch > payload_epoch,
            "size growth over the zero-filled overlay must advance to a fresh content epoch"
        );
        let pending_object_ids = vfs
            .pending_uploads_for_inode(ino)
            .iter()
            .map(|(_, object_id, _)| *object_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_object_ids, retained_objects,
            "set_len must requeue retained zero-filled overlay objects"
        );
        let full_zero_filled_upload = vfs
            .pending_uploads_snapshot()
            .into_iter()
            .find(|op| op.ino == ino && op.payload_len == tail_offset + tail.len() as u64)
            .expect("requeue must preserve the full zero-filled sparse object upload");
        assert_eq!(
            full_zero_filled_upload.offset, 0,
            "requeued sparse object must keep the original file start so an in-flight upload can be updated"
        );
        assert_eq!(
            full_zero_filled_upload.object_file_offset, 0,
            "requeued sparse object must read from the start of the retained payload"
        );
    });
}

#[test]
fn stale_ready_attr_refresh_keeps_uploaded_overlay_finalization_queued() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "stale-ready-keeps-overlay-finalize");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"stale-ready-keeps-overlay-finalize.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let ino = attr.ino;
        let head = vec![0xA1; 233];
        let tail = vec![0xB2; 262];
        let tail_offset = DEFAULT_CHUNK_SIZE as u64 - 7;

        vfs.write(ino, 0, &head).await.expect("write sparse head");
        vfs.flush(ino).await.expect("flush sparse head");
        let (head_epoch, head_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, head_epoch, &head_blocks);
        let stale_ready_attr = meta.get_attr(ino).expect("load published head attr");

        let mut zero_filled_tail = vec![0; tail_offset as usize];
        zero_filled_tail.extend_from_slice(&tail);
        vfs.write(ino, 0, &zero_filled_tail)
            .await
            .expect("write uploaded zero-filled sparse tail");
        vfs.flush(ino).await.expect("flush zero-filled sparse tail");
        let (tail_epoch, _tail_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup models the post-upload window before overlay publication"
        );
        vfs.queue_epoch_finalization(ino, tail_epoch);
        assert!(
            vfs.state
                .upload
                .pending_epoch_finalizations
                .lock()
                .contains(&(ino, tail_epoch)),
            "setup should queue publication for the uploaded overlay epoch"
        );

        vfs.refresh_inode_incarnation_state_from_attr(ino, &stale_ready_attr);

        assert!(
            vfs.state
                .upload
                .pending_epoch_finalizations
                .lock()
                .contains(&(ino, tail_epoch)),
            "stale ready metadata must not clear a newer uploaded overlay finalization"
        );
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "stale ready metadata must not clear newer live overlay extents"
        );
    });
}

#[test]
fn nonzero_truncate_requeues_retained_overlay_uploads_with_new_epoch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "truncate-requeues-retained");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"truncate-requeues-retained.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = vec![0x5Au8; 8192];

        vfs.write(ino, 0, &payload).await.expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");

        let flushed_attr = meta.get_attr(ino).expect("load flushed attr");
        let old_epoch = flushed_attr.content_epoch;
        let old_objects = meta
            .get_regular_file_overlay_object_ids(ino)
            .expect("load old overlay object ids");
        assert_eq!(
            vfs.pending_uploads_for_inode(ino)
                .iter()
                .map(|(_, object_id, content_epoch)| (*object_id, *content_epoch))
                .collect::<Vec<_>>(),
            old_objects
                .iter()
                .map(|object_id| (*object_id, old_epoch))
                .collect::<Vec<_>>(),
            "setup should queue uploads for the original unpublished epoch"
        );

        vfs.setattr(ino, SetattrArgs::size_only(4096))
            .expect("truncate while retaining the first overlay payload");

        let truncated_attr = meta.get_attr(ino).expect("load truncated attr");
        assert!(truncated_attr.content_epoch > old_epoch);
        let pending = vfs.pending_uploads_for_inode(ino);
        assert!(
            !pending.is_empty(),
            "retained overlay payloads must remain queued after the epoch advances"
        );
        assert!(
            pending
                .iter()
                .all(|(_, _, content_epoch)| *content_epoch == truncated_attr.content_epoch),
            "retained payload uploads must be queued under the new content epoch"
        );
    });
}

#[test]
fn handle_backed_nonzero_truncate_clears_drained_dirty_attr_before_fsync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-truncate-clears-drained-attr");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-truncate-clears-drained-attr.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = vec![0x5Au8; 8192];

        vfs.write(ino, 0, &payload).await.expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");

        let old_epoch = meta.get_attr(ino).expect("load flushed attr").content_epoch;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open truncate handle");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(4096))
            .expect("truncate while retaining the first overlay payload");

        let truncated_attr = meta.get_attr(ino).expect("load truncated attr");
        assert!(truncated_attr.content_epoch > old_epoch);
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "handle-backed shrink should not leave already-drained dirty attrs for fsync"
        );

        let flush = vfs.flush_fh_with_outcome(fh).await.expect("flush handle");
        assert!(
            !flush.flushed_local_dirty_state,
            "post-truncate fsync/flush should see no extra foreground dirty state"
        );
        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn nonzero_extend_requeues_retained_overlay_uploads_when_publish_work_is_in_flight() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "extend-keeps-retained");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"extend-keeps-retained.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = vec![0x7Bu8; 8192];

        vfs.write(ino, 0, &payload).await.expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");

        let pending_before = vfs.pending_uploads_for_inode(ino);
        assert!(
            !pending_before.is_empty(),
            "setup should queue uploads before the logical-size extension"
        );

        vfs.setattr(ino, SetattrArgs::size_only(16 * 1024))
            .expect("extend while retaining existing overlay payloads");

        let pending_after = vfs.pending_uploads_for_inode(ino);
        assert_eq!(
            pending_after.len(),
            pending_before.len(),
            "logical-size growth must retain existing overlay uploads"
        );
        assert!(
            pending_after
                .iter()
                .all(|(_, _, content_epoch)| *content_epoch > pending_before[0].2),
            "retained overlay uploads must move to the new size-change epoch"
        );
    });
}

#[test]
fn size_change_requeue_cancels_superseded_upload_token_and_rotates_for_retained_uploads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "extend-rotates-upload-token");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"extend-rotates-upload-token.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = vec![0x47u8; 8192];

        vfs.write(ino, 0, &payload).await.expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup should queue an upload before the logical-size extension"
        );

        let superseded_token = vfs.uploader_cancel_token_for_inode(ino);
        assert!(
            !superseded_token.is_cancelled(),
            "setup token should represent an active upload"
        );

        vfs.setattr(ino, SetattrArgs::size_only(16 * 1024))
            .expect("extend while retaining existing overlay payloads");

        assert!(
            superseded_token.is_cancelled(),
            "size-change requeue must cancel already-active uploads for the old epoch"
        );
        assert!(
            !vfs.uploader_cancel_token_for_inode(ino).is_cancelled(),
            "retained uploads queued for the new epoch need a fresh live token"
        );
    });
}

#[test]
fn handle_backed_extend_keeps_writer_stack_buffered_without_strict_sync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "handle-extend-no-strict-sync");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-extend-no-strict-sync.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let payload = b"handle-backed-extend";

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .expect("buffer writer-stack payload");
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        let sync_calls_before = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);

        let extended = vfs
            .setattr_with_handle(
                Some(fh),
                ino,
                SetattrArgs::size_only((payload.len() + 4096) as u64),
            )
            .expect("handle-backed logical-size growth");

        let sync_calls_after = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        assert_eq!(extended.size, (payload.len() + 4096) as u64);
        #[cfg(target_os = "macos")]
        {
            assert!(
                sync_calls_after > sync_calls_before,
                "macOS handle-backed EOF growth must publish local size before replying"
            );
            assert!(
                !vfs.state.writer_stack.has_dirty_writers(ino),
                "macOS strict EOF barrier should materialize foreground writer data"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(
                sync_calls_after, sync_calls_before,
                "handle-backed logical-size growth must not force strict sync_data"
            );
            assert!(
                vfs.state.writer_stack.has_dirty_writers(ino),
                "logical-size growth should leave foreground write data buffered"
            );
            assert!(
                vfs.pending_uploads_for_inode(ino).is_empty(),
                "logical-size growth should not materialize or enqueue uploads"
            );
        }

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn handle_backed_extend_with_dirty_writer_and_pending_upload_does_not_cancel_active_uploads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-extend-active-upload");
    #[cfg(target_os = "macos")]
    let _ = &meta;
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-extend-active-upload.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let flushed_payload = vec![0x38u8; 8192];

        vfs.write(ino, 0, &flushed_payload)
            .await
            .expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");
        let pending_before = vfs.pending_uploads_for_inode(ino);
        assert!(
            !pending_before.is_empty(),
            "setup should leave an upload queued for the first epoch"
        );
        #[cfg(not(target_os = "macos"))]
        let pending_epoch = meta.get_attr(ino).expect("load flushed attr").content_epoch;
        #[cfg(not(target_os = "macos"))]
        let active_upload_token = vfs.uploader_cancel_token_for_inode(ino);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            flushed_payload.len() as u64,
            b"active-writer-payload",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("buffer active writer payload");
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        let sync_calls_before = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);

        let extended = vfs
            .setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(32 * 1024))
            .expect("handle-backed logical-size growth");

        let sync_calls_after = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        assert_eq!(extended.size, 32 * 1024);
        #[cfg(target_os = "macos")]
        {
            assert!(
                sync_calls_after > sync_calls_before,
                "macOS active handle growth must force a local EOF/payload barrier"
            );
            assert!(
                !vfs.state.writer_stack.has_dirty_writers(ino),
                "macOS strict EOF barrier should materialize active writer-stack data"
            );
            assert!(
                vfs.pending_uploads_for_inode(ino)
                    .iter()
                    .any(|(_, _, content_epoch)| *content_epoch == extended.content_epoch),
                "macOS strict EOF barrier should queue publish work for the new local epoch"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(
                sync_calls_after, sync_calls_before,
                "active handle growth must not force strict payload-store sync"
            );
            assert!(
                !active_upload_token.is_cancelled(),
                "active handle growth must not cancel in-flight uploads for an older queued epoch"
            );
            assert!(vfs.state.writer_stack.has_dirty_writers(ino));
            assert!(
                vfs.pending_uploads_for_inode(ino)
                    .iter()
                    .any(|(_, _, content_epoch)| *content_epoch == pending_epoch),
                "old queued uploads should remain queued instead of being requeued by EOF growth"
            );
        }

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn handle_backed_extend_with_mtime_and_dirty_writer_forces_strict_sync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-extend-mtime-strict-sync");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-extend-mtime-strict-sync.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let flushed_payload = vec![0x41u8; 8192];

        vfs.write(ino, 0, &flushed_payload)
            .await
            .expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup should leave an upload queued for the first epoch"
        );
        let pending_epoch = meta.get_attr(ino).expect("load flushed attr").content_epoch;

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            flushed_payload.len() as u64,
            b"active-writer-payload",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("buffer active writer payload");
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        let sync_calls_before = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        let target_mtime = 1_700_000_000_123_456_789;

        let extended = vfs
            .setattr_with_handle(
                Some(fh),
                ino,
                SetattrArgs {
                    size: Some(32 * 1024),
                    mtime: TimeSpec::Set(target_mtime),
                    ..Default::default()
                },
            )
            .expect("handle-backed non-size-only growth");

        let sync_calls_after = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        assert!(
            sync_calls_after > sync_calls_before,
            "size+mtime setattr must not use the size-only strict-sync deferral"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "strict sync should materialize active writer-stack data"
        );
        assert_eq!(extended.size, 32 * 1024);
        assert_eq!(extended.mtime, target_mtime);
        assert!(
            extended.content_epoch > pending_epoch,
            "non-size-only growth with pending publish work should advance the incarnation"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn handle_backed_shrink_with_dirty_writer_forces_strict_sync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-shrink-dirty-writer-strict-sync");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"handle-shrink-dirty-writer-strict-sync.bin",
                0,
                0,
                0o644,
            )
            .expect("create test file");
        let ino = attr.ino;
        let flushed_payload = vec![0x53u8; 8192];

        vfs.write(ino, 0, &flushed_payload)
            .await
            .expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup should leave an upload queued for the first epoch"
        );
        let pending_epoch = meta.get_attr(ino).expect("load flushed attr").content_epoch;

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            flushed_payload.len() as u64,
            b"dirty-tail-before-shrink",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("buffer dirty writer payload");
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        let sync_calls_before = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        let shrink_size = 4096u64;

        let shrunk = vfs
            .setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(shrink_size))
            .expect("handle-backed shrink");

        let sync_calls_after = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        assert!(
            sync_calls_after > sync_calls_before,
            "shrink must not use the growth-only strict-sync deferral"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "strict sync should materialize or discard active writer-stack data before shrink"
        );
        assert_eq!(shrunk.size, shrink_size);
        assert!(
            shrunk.content_epoch > pending_epoch,
            "shrink with pending publish work should advance the incarnation"
        );
        assert!(
            vfs.pending_uploads_for_inode(ino)
                .iter()
                .all(|(_, _, content_epoch)| *content_epoch == shrunk.content_epoch),
            "retained uploads after shrink should be requeued for the new epoch"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn fsync_after_clean_handle_extend_queues_metadata_only_publish() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "clean-handle-extend-flush-publish");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"clean-handle-extend-flush-publish.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = b"already-published-head";

        vfs.write(ino, 0, payload)
            .await
            .expect("write published payload");
        vfs.flush(ino).await.expect("flush published payload");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, content_epoch, &blocks);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open clean writable handle");
        let logical_size = 8192u64;
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend clean handle");
        let outcome = vfs
            .fsync_with_outcome(ino, false)
            .await
            .expect("fsync should persist dirty attr");
        assert!(
            outcome.persisted_dirty_attrs,
            "fsync should report the metadata-only dirty attr"
        );

        let extended_attr = meta.get_attr(ino).expect("load extended attr");
        assert_eq!(extended_attr.size, logical_size);
        assert!(
            vfs.state
                .upload
                .pending_epoch_finalizations
                .lock()
                .contains(&(ino, extended_attr.content_epoch)),
            "inline dirty-attr persistence must queue metadata-only publication"
        );
        assert!(
            !vfs.inode_has_unmaterialized_overlay_for_finalize(ino),
            "metadata-only grow should not wait on local payload work"
        );
        let lifecycle = meta
            .get_inode_lifecycle(ino)
            .expect("load metadata-only grow lifecycle")
            .expect("metadata-only grow should have lifecycle");
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "metadata-only grow should be publish-ready"
        );

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            ino,
            extended_attr.content_epoch,
        )
        .await
        .expect("finalize metadata-only grow");
        match status {
            super::commit::EpochFinalizeStatus::Published => {}
            super::commit::EpochFinalizeStatus::WaitingForUploads => {
                panic!("metadata-only grow unexpectedly waited for uploads")
            }
            super::commit::EpochFinalizeStatus::Deferred => {
                panic!("metadata-only grow unexpectedly deferred")
            }
            super::commit::EpochFinalizeStatus::Skipped => {
                panic!("metadata-only grow unexpectedly skipped")
            }
        }
        let published_attr = meta.get_attr(ino).expect("load published attr");
        assert_eq!(published_attr.committed_size, logical_size);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn handle_backed_clean_growth_persistence_matches_platform_policy() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-growth-platform-policy");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-growth-platform-policy.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;

        vfs.write(ino, 0, b"published-head")
            .await
            .expect("write seed payload");
        vfs.flush(ino).await.expect("flush seed payload");
        let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, content_epoch, &blocks);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open clean writable handle");
        let logical_size = 8192u64;
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend clean handle");

        #[cfg(target_os = "macos")]
        {
            let extended_attr = meta.get_attr(ino).expect("load strict macOS attr");
            assert_eq!(extended_attr.size, logical_size);
            assert!(
                vfs.state
                    .upload
                    .pending_epoch_finalizations
                    .lock()
                    .contains(&(ino, extended_attr.content_epoch)),
                "macOS handle-backed growth should queue metadata-only publication before reply"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            let dirty_attr = vfs
                .state
                .namespace
                .dirty_attrs
                .lock()
                .get(&ino)
                .cloned()
                .expect("non-macOS handle-backed growth should defer strict persistence");
            assert_eq!(dirty_attr.size, logical_size);
        }

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn handle_backed_sparse_extend_publishes_logical_size_without_tail_payload() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "handle-sparse-extend-publish-size");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-sparse-extend-publish-size.bin", 0, 0, 0o644)
            .expect("create sparse test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let head = b"sparse-head";
        let tail = b"sparse-tail-range";
        let tail_offset = 1_048_569u64;
        let logical_size = 2_097_275u64;

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, head, LocalSyncPolicy::Opportunistic)
            .await
            .expect("write sparse head");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            tail_offset,
            tail,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write sparse tail range");
        vfs.setattr_with_handle(Some(fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF without tail payload");

        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release should persist local sparse metadata");
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        let (content_epoch, _blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "setup should model completed uploads before finalization"
        );
        assert!(
            !vfs.inode_has_unmaterialized_overlay_for_finalize(ino),
            "completed uploads and drained writer state should allow finalization"
        );
        let lifecycle = meta
            .get_inode_lifecycle(ino)
            .expect("load lifecycle")
            .expect("published candidate should have lifecycle state");
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "uploaded overlay should become publish-ready"
        );
        publish_current_file_graph(&meta, ino, content_epoch, &_blocks);

        let published_attr = meta.get_attr(ino).expect("load published attr");
        assert_eq!(published_attr.size, logical_size);
        assert_eq!(
            published_attr.committed_size, logical_size,
            "published sparse truncate-extend must expose the logical EOF to peers"
        );
        let published_tail = meta
            .lookup_regular_file_slices(ino, tail_offset, tail.len() as u64)
            .expect("resolve sparse tail payload");
        assert!(
            published_tail.iter().any(|span| span.payload.is_some()),
            "published sparse truncate-extend must retain the written tail payload"
        );
        let hole = meta
            .lookup_regular_file_slices(
                ino,
                tail_offset + tail.len() as u64,
                logical_size - tail_offset - tail.len() as u64,
            )
            .expect("resolve sparse EOF tail");
        assert!(
            hole.iter().all(|span| span.payload.is_none()),
            "logical EOF extension should remain hole-backed"
        );

        let shrink_size = 1_048_833u64;
        let shrink_fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open handle for sparse shrink");
        vfs.setattr_with_handle(Some(shrink_fh), ino, SetattrArgs::size_only(shrink_size))
            .expect("shrink logical EOF without new payload");
        let outcome = vfs
            .release_fh_with_outcome(shrink_fh)
            .await
            .expect("release shrink handle");
        assert!(outcome.closed_last_handle);
        assert!(
            !outcome.flushed_local_dirty_state,
            "handle-backed shrink is strictly persisted by setattr before last close"
        );

        let shrink_attr = meta.get_attr(ino).expect("load shrink candidate attr");
        assert_eq!(shrink_attr.size, shrink_size);
        assert_ne!(shrink_attr.upload_state, MetaUploadState::Ready);
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "metadata-only sparse shrink should not wait on payload uploads"
        );
        assert!(
            !vfs.inode_has_unmaterialized_overlay_for_finalize(ino),
            "metadata-only sparse shrink should be materialized enough to finalize"
        );
        let shrink_lifecycle = meta
            .get_inode_lifecycle(ino)
            .expect("load shrink lifecycle")
            .expect("shrink candidate should have lifecycle");
        assert_eq!(
            shrink_lifecycle.state,
            InodeLifecycleState::PublishReady,
            "metadata-only sparse shrink should be publish-ready"
        );
        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            ino,
            shrink_attr.content_epoch,
        )
        .await
        .expect("finalize sparse shrink");
        match status {
            super::commit::EpochFinalizeStatus::Published => {}
            super::commit::EpochFinalizeStatus::WaitingForUploads => {
                panic!("sparse shrink unexpectedly waited for uploads")
            }
            super::commit::EpochFinalizeStatus::Deferred => {
                panic!("sparse shrink unexpectedly deferred")
            }
            super::commit::EpochFinalizeStatus::Skipped => {
                panic!("sparse shrink unexpectedly skipped")
            }
        }

        let shrunk_attr = meta.get_attr(ino).expect("load published shrink attr");
        assert_eq!(shrunk_attr.size, shrink_size);
        assert_eq!(
            shrunk_attr.committed_size, shrink_size,
            "published sparse shrink must reduce the peer-visible logical EOF"
        );
    });
}

#[test]
fn epoch_finalizer_guard_observes_writer_stack_tail() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-writer-stack-tail");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-writer-stack-tail.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let payload = b"writer-stack-only-tail";

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .expect("buffer writer-stack tail");

        assert!(vfs.state.writer_stack.has_dirty_writers(ino));
        assert!(!vfs.has_pending_local_dirty_payloads_for_inode(ino));
        assert!(
            vfs.inode_has_unmaterialized_overlay_for_finalize(ino),
            "epoch finalization must defer while a mounted write is only buffered in the writer stack"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn writer_stack_object_reservation_counts_only_current_write() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "writer-stack-object-reservation");
    let block_size = vfs.block_size as usize;
    let expected_dirty_limit_objects = vfs.max_dirty_bytes().div_ceil(block_size).saturating_add(1);

    assert!(
        expected_dirty_limit_objects > 1,
        "setup should distinguish exact write reservation from dirty-limit reservation"
    );
    assert_eq!(vfs.writer_stack_object_upper_bound(0, 1).unwrap(), 1);
    assert_eq!(
        vfs.writer_stack_object_upper_bound(0, block_size + 1)
            .unwrap(),
        2
    );
}

#[test]
fn epoch_finalizer_waits_for_pending_upload_completion_without_retrying() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-waiting-uploads");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-waiting-uploads.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = vec![0xCAu8; vfs.block_size as usize];
        vfs.write(attr.ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino).await.expect("flush slice-graph epoch");

        let pending_attr = meta
            .get_attr(attr.ino)
            .expect("load pending-upload epoch attr");
        assert!(
            !vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "setup must leave an uploaded extent in the local pending queue"
        );

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            pending_attr.content_epoch,
        )
        .await
        .expect("finalize should observe pending uploads as a normal wait state");
        assert!(
            matches!(
                status,
                super::commit::EpochFinalizeStatus::WaitingForUploads
            ),
            "pending uploads should not use the timed deferred retry path"
        );
    });
}

#[test]
fn epoch_finalizer_defers_while_deferred_materialization_is_inflight() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-inflight");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-inflight.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = b"slice-graph-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize unpublished slice payloads before publish setup");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);

        let mut truncated_attr = meta.get_attr(attr.ino).expect("load published attr");
        truncated_attr.size = 0;
        truncated_attr.content_epoch = truncated_attr.content_epoch.saturating_add(1);
        truncated_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&truncated_attr)
            .expect("stage slice-graph zero-length epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("remove unpublished slice graph for zero-length truncate");

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        let lifecycle = meta
            .get_inode_lifecycle(attr.ino)
            .expect("load lifecycle")
            .expect("zero-length epoch should keep lifecycle state");
        assert!(
            !vfs.inode_has_unmaterialized_overlay_for_finalize(attr.ino),
            "setup should otherwise be ready to finalize once uploads are visible"
        );
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "once zero-length slice-graph state is durable, only publication should remain"
        );

        let (entry, _guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(attr.ino)
            .expect("test should reserve the deferred materialization gate");

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize should defer while deferred materialization is in flight");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Deferred),
            "finalize must not race ahead of an in-flight deferred materialization task"
        );

        drop(entry);

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize should publish once deferred materialization completes");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "once the in-flight task clears, finalize should publish the prepared epoch"
        );
    });
}

#[test]
fn graph_finalizer_visibility_timeout_does_not_requeue_uploaded_blocks() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        publish_visibility_timeout_secs: 1,
        ..VfsConfig::default()
    };
    let (rt, vfs, meta) =
        make_unstarted_test_vfs_with_config(&tmp, "graph-finalizer-no-requeue", cfg);
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-no-requeue.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = vec![0x5Au8; vfs.block_size as usize];
        vfs.write(attr.ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("flush slice-graph epoch");

        let (content_epoch, _blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        for pending in vfs.pending_uploads_snapshot() {
            if pending.ino == attr.ino && pending.content_epoch == content_epoch {
                vfs.cached_store().complete_upload(&pending);
            }
        }
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "setup should simulate uploaded payload objects that are no longer pending locally"
        );

        let status =
            super::commit::CommitCoordinator::finalize_inode_epoch(&vfs, attr.ino, content_epoch)
                .await
                .expect("visibility timeout should defer instead of failing");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Deferred),
            "visibility lag should defer publish instead of requeueing uploaded payload objects"
        );
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "uploaded payload objects awaiting remote visibility should not be requeued into the upload backlog"
        );
    });
}

#[test]
fn zeroio_snapshot_reports_repeated_restart_replay_as_health_signal() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-repeated-replay");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"replay-health.bin", 0, 0, 0o644)
        .expect("create replay test file");
    let ino = created.ino;
    rt.block_on(async {
        vfs.write(ino, 0, b"replay-health-payload").await.unwrap();
        vfs.flush(ino)
            .await
            .expect("materialize replay health payload");
    });
    let attr = meta.get_attr(ino).expect("load replay attr");
    let block = meta
        .lookup_payload_slices(ino)
        .expect("load replay blocks")
        .into_iter()
        .next()
        .expect("at least one persisted block")
        .1;
    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_file_offset: 0,
        object_id: block.object_id,
        payload_len: u64::from(block.length),
        content_epoch: attr.content_epoch,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, std::slice::from_ref(&attr));
    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    let snapshot = vfs.zeroio_runtime_snapshot();
    assert!(
        snapshot
            .startup_replay
            .as_ref()
            .map(|summary| summary.replayed_blocks)
            .unwrap_or(0)
            >= 1,
        "startup replay summary should be surfaced in zeroio diagnostics"
    );
    assert!(
        snapshot
            .health_signals
            .iter()
            .any(|signal| signal.code == "repeated_replay" && signal.ino == Some(ino)),
        "repeated restart replay should surface as an explicit zeroio health signal"
    );
    let entry = snapshot
        .drilldown
        .iter()
        .find(|entry| entry.ino == ino)
        .expect("replayed inode should appear in zeroio drilldown");
    assert!(entry.replayed_after_restart);
    assert!(entry.repeated_replay_count >= 2);
}

#[test]
fn zeroio_snapshot_loads_last_graceful_shutdown_after_restart() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "zeroio-shutdown-persisted");
    let _guard = rt.enter();

    rt.block_on(async {
        let created = vfs
            .create(1, b"shutdown-persisted.bin", 0, 0, 0o644)
            .expect("create shutdown test file");
        vfs.write(created.ino, 0, b"shutdown-payload")
            .await
            .expect("write shutdown payload");
        vfs.shutdown(std::time::Duration::ZERO)
            .await
            .expect("VFS shutdown should succeed");
    });
    drop(vfs);
    drop(meta);
    drop(rt);

    let (_reopen_rt, reopened, _reopened_meta) = reopen_existing_test_vfs(&tmp);
    assert!(
        reopened
            .recent_runtime_events(8)
            .into_iter()
            .all(|event| event.kind != "shutdown_local_state"),
        "freshly reopened VFS should not have inherited runtime events in memory"
    );

    let snapshot = reopened.zeroio_runtime_snapshot();
    let shutdown = snapshot
        .last_graceful_shutdown
        .expect("shutdown evidence should survive restart");
    assert!(shutdown.generated_unix_ms > 0);
    assert_eq!(shutdown.metadata_flush_outcome, "success");
    assert!(shutdown.local_payload_sync_ok);
    assert!(shutdown.recoverable_on_restart);
}

#[test]
fn local_first_shutdown_reports_and_recovers_memory_only_writer_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-312-shutdown-writer-state");
    let _guard = rt.enter();
    let payload = b"sfs-312-memory-only-before-shutdown";

    let ino = rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-312-shutdown-writer-state.bin", 0, 0, 0o644)
            .expect("create shutdown writer-state file");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open writer-state handle");

        vfs.write_fh_with_local_sync_policy(
            fh,
            attr.ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("accept writer-state bytes");
        assert!(
            vfs.state.writer_stack.has_dirty_writers(attr.ino),
            "setup should leave accepted bytes in memory-only writer state before shutdown"
        );

        let read = vfs
            .read(attr.ino, 0, payload.len() as u32)
            .await
            .expect("read accepted writer-state bytes before shutdown");
        assert_eq!(
            read.as_ref(),
            payload,
            "same-mount reads should observe accepted writer-state bytes before a durability boundary"
        );

        vfs.shutdown(std::time::Duration::ZERO)
            .await
            .expect("VFS shutdown should succeed");
        attr.ino
    });
    drop(vfs);
    drop(meta);
    drop(rt);

    let (reopen_rt, reopened, _reopened_meta) = reopen_existing_test_vfs(&tmp);
    let _reopen_guard = reopen_rt.enter();
    let snapshot = reopened.zeroio_runtime_snapshot();
    let shutdown = snapshot
        .last_graceful_shutdown
        .expect("shutdown diagnostics should survive metadata/cache reopen");
    assert!(
        shutdown.accepted_local_dirty_bytes_before >= payload.len() as u64,
        "shutdown should report memory-only accepted bytes before the local durability drain"
    );
    assert_eq!(
        shutdown.accepted_local_dirty_bytes_after, 0,
        "shutdown should drain accepted bytes across the local durability boundary"
    );
    assert!(
        shutdown.recoverable_on_restart,
        "shutdown with accepted writer-state bytes should report a recoverable local view"
    );

    reopen_rt.block_on(async {
        let read = reopened
            .read(ino, 0, payload.len() as u32)
            .await
            .expect("read recovered local writer-state bytes after reopen");
        assert_eq!(
            read.as_ref(),
            payload,
            "metadata/cache reopen should recover bytes that graceful shutdown moved across the local durability boundary"
        );
    });
}

#[test]
fn zeroio_snapshot_reports_replay_backlog_beyond_recent_event_sample_limit() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-replay-sample-limit");
    let _guard = rt.enter();

    let mut replayed_epochs = Vec::new();
    rt.block_on(async {
        for idx in 0..17 {
            let name = format!("replay-{idx}.bin");
            let created = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
            vfs.write(created.ino, 0, b"replay-payload").await.unwrap();
            let attr = meta.get_attr(created.ino).expect("load replay attr");
            replayed_epochs.push((created.ino, attr.content_epoch));
        }
    });

    {
        let mut replay_counts = vfs.state.upload.startup_replay_counts.lock();
        for (ino, content_epoch) in &replayed_epochs {
            replay_counts.insert((*ino, *content_epoch), 1);
        }
    }

    let snapshot = vfs.zeroio_runtime_snapshot();
    assert_eq!(
        snapshot.replayed_after_restart_epochs,
        replayed_epochs.len() as u64
    );
    assert_eq!(snapshot.replay_backlog_epochs, replayed_epochs.len());
    let last_ino = replayed_epochs.last().map(|(ino, _)| *ino).unwrap();
    let entry = snapshot
        .drilldown
        .iter()
        .find(|entry| entry.ino == last_ino)
        .expect("epochs beyond the sampled runtime-event limit must still be marked replayed");
    assert!(entry.replayed_after_restart);
}

#[test]
fn cache_backend_detection_errors_are_structured() {
    let error = std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "simulated permission error",
    );
    let mapped = map_cache_backend_detection_error("E:\\cache-root", error);
    match mapped {
        VfsError::LocalPayloadRuntimeDetection { cache_root, source } => {
            assert_eq!(cache_root, "E:\\cache-root");
            assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
        }
        other => panic!("unexpected error variant: {other}"),
    }
}

#[test]
fn test_block_index_calculation() {
    let block_size = 4 * 1024 * 1024; // 4MB

    assert_eq!(block_index_from_offset(0, block_size), 0);
    assert_eq!(block_index_from_offset(1, block_size), 0);
    assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
    assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
}

#[test]
fn cache_runtime_tuning_enforces_bounds() {
    let tiny =
        derive_cache_runtime_tuning(2 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024);
    let huge = derive_cache_runtime_tuning(
        16 * 1024 * 1024 * 1024 * 1024,
        16 * 1024 * 1024,
        8 * 1024 * 1024 * 1024,
    );

    assert!(tiny.max_dirty_bytes > 0);
    assert!(tiny.upload_cache_io_slots > 0);
    assert_eq!(tiny.max_dirty_bytes, CACHE_RUNTIME_MIN_DIRTY_BYTES);
    assert_eq!(huge.max_dirty_bytes, CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE);
    assert!(huge.max_dirty_bytes >= tiny.max_dirty_bytes);
    assert!(huge.upload_cache_io_slots >= tiny.upload_cache_io_slots);
}

#[test]
fn cache_runtime_tuning_defaults_match_juicefs_sized_windows() {
    let derived =
        derive_cache_runtime_tuning(100 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024);

    assert_eq!(DEFAULT_MAX_DIRTY_BYTES, 800 * 1024 * 1024);
    assert_eq!(DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES, 64 * 1024 * 1024);
    assert_eq!(derived.max_dirty_bytes, 800 * 1024 * 1024);
    assert_eq!(derived.write_eager_flush_batch_bytes, 64 * 1024 * 1024);
    assert_eq!(derived.transit_memory_budget_bytes, 800 * 1024 * 1024);
}

#[test]
fn upload_hot_buffer_budget_does_not_collapse_with_tiny_dirty_limit() {
    assert_eq!(
        bootstrap::derive_upload_hot_buffer_bytes(1, 64 * 1024 * 1024, 16 * 1024 * 1024),
        64 * 1024 * 1024
    );
    assert_eq!(
        bootstrap::derive_upload_hot_buffer_bytes(1, 0, 16 * 1024 * 1024),
        16 * 1024 * 1024
    );
}

#[test]
fn fast_mode_sync_group_defaults_to_drain_chunk_window() {
    assert_eq!(
        derive_fast_mode_sync_group_min_bytes(64 * 1024 * 1024, 32 * 1024 * 1024),
        32 * 1024 * 1024
    );
}

#[test]
fn write_admission_pressure_total_excludes_pending_upload_backlog() {
    let pressure = WriteAdmissionPressure {
        writer_stack_dirty_bytes: 10,
        pending_local_dirty_bytes: 20,
        pending_persisted_slice_bytes: 30,
        pending_upload_bytes: 40,
    };

    assert_eq!(pressure.pending_upload_bytes, 40);
    assert_eq!(pressure.total_bytes(), 60);
}

#[test]
fn write_admission_pressure_samples_cached_store_pending_upload_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "admission-pending-upload");
    let mut writer = vfs
        .cached_store()
        .open_writer_slice(CachedStoreWriterConfig {
            ino: 7,
            base_offset: 0,
            first_object_id: 10_000,
            content_epoch: 1,
            block_size: 8,
            page_size: 4,
            dirty_byte_limit: None,
            max_buffer_bytes: None,
            submit_upload: true,
        });

    writer.write_at(0, b"abcdefgh").expect("write full block");
    let detached = writer
        .detach_completed_blocks(1)
        .expect("detach completed block");
    vfs.cached_store()
        .persist_detached_staged_slices(detached)
        .expect("persist pending upload");

    let pressure = vfs.write_admission_pressure();

    assert_eq!(pressure.pending_upload_bytes, 8);
    assert_eq!(pressure.total_bytes(), 0);
}

#[test]
fn write_admission_first_sample_below_budget_without_pacing() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "admission-admits-buffer-window");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"admission-admits-buffer-window.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let buffer_budget_bytes = vfs.block_size as usize;
        vfs.set_max_dirty_bytes(buffer_budget_bytes);
        let seeded_len = buffer_budget_bytes / 2;
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino: attr.ino,
                    current_size: 0,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xAB; seeded_len]),
            )
            .unwrap();

        tokio::time::timeout(
            Duration::from_millis(WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS / 2),
            vfs.maybe_throttle_write_admission(1, 0),
        )
        .await
        .expect("first adaptive admission sample should not sleep below budget");
        assert_eq!(
            vfs.state
                .metrics
                .drain_lag_pacing_events
                .load(Ordering::Relaxed),
            0
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_admission_does_not_sleep_for_soft_pressure_below_hard_window() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "admission-no-soft-pacing");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"admission-no-soft-pacing.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let buffer_budget_bytes = vfs.block_size as usize;
        vfs.set_max_dirty_bytes(buffer_budget_bytes);
        let pressure_bytes = buffer_budget_bytes * 7 / 10;
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino: attr.ino,
                    current_size: 0,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xAB; pressure_bytes]),
            )
            .unwrap();

        {
            let mut governor = vfs.state.write_admission_governor.lock();
            governor.last_sample_ms =
                now_unix_millis().saturating_sub(WRITE_ADMISSION_SAMPLE_MIN_INTERVAL_MS);
            governor.last_pressure_bytes = buffer_budget_bytes / 5;
            governor.accepted_since_sample = (buffer_budget_bytes * 3 / 5) as u64;
        }

        tokio::time::timeout(
            Duration::from_millis(WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS / 2),
            vfs.maybe_throttle_write_admission(buffer_budget_bytes / 10, 0),
        )
        .await
        .expect("soft pressure below the hard window must not delay a local-first write");
        assert!(
            vfs.state
                .write_admission_governor
                .lock()
                .stats()
                .last_delay_ms
                > 0,
            "soft pacing estimate should remain visible without pacing a foreground write"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_admission_admits_oversized_write_when_pressure_is_empty() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "admission-admits-oversized-write");
    let _guard = rt.enter();
    rt.block_on(async {
        let buffer_budget_bytes = vfs.block_size as usize;
        vfs.set_max_dirty_bytes(buffer_budget_bytes);

        tokio::time::timeout(
            Duration::from_millis(WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS * 2),
            vfs.maybe_throttle_write_admission(buffer_budget_bytes * 2 + 1, 0),
        )
        .await
        .expect("empty pressure should admit a single oversized write");
    });
}

#[test]
fn write_admission_ignores_pending_upload_backlog_for_hard_window() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "admission-ignores-upload-backlog");
    let _guard = rt.enter();
    rt.block_on(async {
        let buffer_budget_bytes = vfs.block_size as usize;
        vfs.set_max_dirty_bytes(buffer_budget_bytes);
        let mut writer = vfs
            .cached_store()
            .open_writer_slice(CachedStoreWriterConfig {
                ino: 7,
                base_offset: 0,
                first_object_id: 10_000,
                content_epoch: 1,
                block_size: buffer_budget_bytes,
                page_size: buffer_budget_bytes,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
                submit_upload: true,
            });

        writer
            .write_at(0, &vec![0xAB; buffer_budget_bytes * 3])
            .expect("write pending upload blocks");
        let detached = writer
            .detach_completed_blocks(usize::MAX)
            .expect("detach pending upload blocks");
        vfs.cached_store()
            .persist_detached_staged_slices(detached)
            .expect("persist pending upload backlog");
        assert!(
            vfs.write_admission_pressure().pending_upload_bytes
                > buffer_budget_bytes * HARD_BACKPRESSURE_DIRTY_MULTIPLIER,
            "test should seed upload backlog above the hard window"
        );

        tokio::time::timeout(
            Duration::from_millis(WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS / 2),
            vfs.maybe_throttle_write_admission(1, 0),
        )
        .await
        .expect("pending upload backlog must not trigger hard admission wait");
    });
}

#[test]
fn write_admission_waits_until_writer_stack_pressure_drops_below_hard_window() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "admission-waits-hard-window");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"admission-waits-hard-window.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let buffer_budget_bytes = vfs.block_size as usize;
        vfs.set_max_dirty_bytes(buffer_budget_bytes);
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino: attr.ino,
                    current_size: 0,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xAB; buffer_budget_bytes * 2 + 1]),
            )
            .unwrap();

        let admission_vfs = Arc::clone(&vfs);
        let wait_task =
            tokio::spawn(async move { admission_vfs.maybe_throttle_write_admission(1, 0).await });
        tokio::time::sleep(Duration::from_millis(
            WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS * 3 + 10,
        ))
        .await;
        assert!(
            !wait_task.is_finished(),
            "admission should keep waiting while writer-stack pressure remains above the hard window"
        );

        vfs.state.writer_stack.clear_inode(attr.ino);
        tokio::time::timeout(
            Duration::from_millis(WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS * 2),
            wait_task,
        )
        .await
        .expect("admission should unblock after writer-stack pressure drops")
        .expect("admission task should not panic");
        assert!(
            vfs.state
                .metrics
                .drain_lag_pacing_events
                .load(Ordering::Relaxed)
                >= 1,
            "hard admission wait should be recorded"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn sustained_pre_drain_marks_deferred_materialization_reschedule_under_pressure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sustained-pre-drain-under-pressure");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sustained-pre-drain-under-pressure.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.set_max_dirty_bytes(64 * 1024 * 1024);

        let seeded_len = 33 * 1024 * 1024;
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino,
                    current_size: 0,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xAB; seeded_len]),
            )
            .unwrap();
        let now = now_unix_millis();
        vfs.state.sustained_write_streams.lock().insert(
            ino,
            SustainedWriteState {
                streak_start_ms: now.saturating_sub(SUSTAINED_WRITE_DELAY_START_MS),
                last_write_ms: now.saturating_sub(1),
                last_end_offset: seeded_len as u64,
            },
        );
        let (entry, guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve deferred materialization gate");

        let delay_ms = vfs
            .maybe_pre_drain_sustained_write_buffer(
                ino,
                seeded_len as u64,
                1,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
        assert_eq!(
            delay_ms, 0,
            "sustained pre-drain should not add a second foreground pacing delay"
        );

        assert!(
            vfs.state
                .deferred_materialize_reschedule_needed
                .lock()
                .contains(&ino),
            "sustained pre-drain should request background materialization before admission pacing"
        );
        let _ = vfs
            .state
            .deferred_materialize_inflight
            .replace_if_current_or_remove(&entry, || false);
        drop(guard);
        vfs.state.writer_stack.clear_inode(ino);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn sustained_pre_drain_schedules_without_pacing_when_dirty_runway_is_long() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sustained-pre-drain-long-runway");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sustained-pre-drain-long-runway.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.set_max_dirty_bytes(512 * 1024 * 1024);

        let seeded_len = 33 * 1024 * 1024;
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino,
                    current_size: 0,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xAB; seeded_len]),
            )
            .unwrap();
        let now = now_unix_millis();
        vfs.state.sustained_write_streams.lock().insert(
            ino,
            SustainedWriteState {
                streak_start_ms: now.saturating_sub(SUSTAINED_WRITE_DELAY_START_MS),
                last_write_ms: now.saturating_sub(1),
                last_end_offset: seeded_len as u64,
            },
        );
        let (entry, guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve deferred materialization gate");

        let delay_ms = vfs
            .maybe_pre_drain_sustained_write_buffer(
                ino,
                seeded_len as u64,
                1,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
        assert_eq!(
            delay_ms, 0,
            "sustained pre-drain should schedule materialization without sleeping while dirty runway is long"
        );

        assert!(
            vfs.state
                .deferred_materialize_reschedule_needed
                .lock()
                .contains(&ino),
            "sustained pre-drain should still request background materialization"
        );
        let _ = vfs
            .state
            .deferred_materialize_inflight
            .replace_if_current_or_remove(&entry, || false);
        drop(guard);
        vfs.state.writer_stack.clear_inode(ino);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn sustained_write_delay_ramps_to_stable_plateau_for_long_streams() {
    let drain = 32 * 1024 * 1024;
    let mut state = SustainedWriteState::default();

    let first = state.observe(1_000, 0, 4, drain, 4);
    assert_eq!(first.delay_ms, 0);
    assert!(!first.pre_drain);

    let one = state.observe(2_500, 4, 4, drain, drain + 1);
    assert_eq!(one.delay_ms, 1);
    assert!(one.pre_drain);

    let two = state.observe(4_000, 8, 4, drain, drain + 1);
    assert_eq!(two.delay_ms, 2);
    assert!(two.pre_drain);

    let mut plateau = two;
    for step in 3..=SUSTAINED_WRITE_MAX_DELAY_MS {
        plateau = state.observe(2_500 + (step - 1) * 1_500, step * 4, 4, drain, drain + 1);
    }
    assert_eq!(plateau.delay_ms, SUSTAINED_WRITE_MAX_DELAY_MS);
    assert!(plateau.pre_drain);
}

#[test]
fn sustained_write_window_resets_after_idle_or_seek() {
    let drain = 32 * 1024 * 1024;
    let mut state = SustainedWriteState::default();

    state.observe(1_000, 0, 4, drain, drain + 1);
    state.observe(2_500, 4, 4, drain, drain + 1);

    let after_idle = state.observe(
        2_500 + SUSTAINED_WRITE_IDLE_RESET_MS + 1,
        8,
        4,
        drain,
        drain + 1,
    );
    assert_eq!(after_idle.duration_ms, 0);
    assert_eq!(after_idle.delay_ms, 0);
    assert!(!after_idle.pre_drain);

    let after_seek = state.observe(4_000, 128, 4, drain, drain + 1);
    assert_eq!(after_seek.duration_ms, 0);
    assert_eq!(after_seek.delay_ms, 0);
    assert!(!after_seek.pre_drain);
}

#[test]
fn write_admission_governor_throttles_on_short_runway_before_limit() {
    let mut governor = WriteAdmissionGovernor::default();

    let initial = governor.plan(WriteAdmissionSample {
        now_ms: 1_000,
        pressure_bytes: 200,
        budget_bytes: 1_000,
        next_bytes: 100,
    });
    assert_eq!(initial.delay_ms, 0);

    governor.record_accepted_bytes(600);
    let decision = governor.plan(WriteAdmissionSample {
        now_ms: 2_000,
        pressure_bytes: 700,
        budget_bytes: 1_000,
        next_bytes: 100,
    });

    assert!(
        decision.delay_ms > 0,
        "growing pressure with a short runway should gently pace writes before the hard limit"
    );
    assert!(decision.runway_ms < WRITE_ADMISSION_RUNWAY_TARGET_MS);
    assert!(decision.ingress_bytes_per_sec > decision.drain_bytes_per_sec);

    let stats = governor.stats();
    assert_eq!(stats.pressure_bytes, 700);
    assert_eq!(stats.budget_bytes, 1_000);
}

#[test]
fn write_admission_governor_uses_remaining_runway_instead_of_full_budget() {
    let mut governor = WriteAdmissionGovernor::default();

    governor.plan(WriteAdmissionSample {
        now_ms: 1_000,
        pressure_bytes: 200,
        budget_bytes: 1_000,
        next_bytes: 100,
    });
    governor.record_accepted_bytes(600);
    let decision = governor.plan(WriteAdmissionSample {
        now_ms: 2_000,
        pressure_bytes: 500,
        budget_bytes: 1_000,
        next_bytes: 100,
    });

    assert!(decision.runway_ms < WRITE_ADMISSION_RUNWAY_TARGET_MS);
    assert!(decision.ingress_bytes_per_sec > decision.drain_bytes_per_sec);
    assert!(
        decision.delay_ms > 0,
        "pacing allowance must use the remaining buffer runway, not reuse the full budget"
    );
}

#[test]
fn write_admission_governor_does_not_throttle_when_drain_keeps_up() {
    let mut governor = WriteAdmissionGovernor::default();

    let initial = governor.plan(WriteAdmissionSample {
        now_ms: 1_000,
        pressure_bytes: 200,
        budget_bytes: 1_000,
        next_bytes: 100,
    });
    assert_eq!(initial.delay_ms, 0);

    governor.record_accepted_bytes(600);
    let decision = governor.plan(WriteAdmissionSample {
        now_ms: 2_000,
        pressure_bytes: 200,
        budget_bytes: 1_000,
        next_bytes: 100,
    });

    assert_eq!(decision.delay_ms, 0);
    assert!(decision.drain_bytes_per_sec >= decision.ingress_bytes_per_sec);
}

#[test]
fn write_admission_governor_handles_empty_drain_and_runway() {
    let governor = WriteAdmissionGovernor {
        ingress_bytes_per_sec: 1_000,
        drain_bytes_per_sec: 0,
        ..WriteAdmissionGovernor::default()
    };

    let delay_ms = governor.delay_ms(4 * 1024 * 1024, 1_024, 1_024, 0);

    assert_eq!(
        delay_ms, WRITE_ADMISSION_MAX_DELAY_MS,
        "zero drain and zero runway allowance must use a non-zero divisor and saturate at the configured delay cap"
    );
}

#[test]
fn default_readahead_tracks_the_juicefs_sized_buffer_budget() {
    let buffer_blocks = derive_buffer_blocks_default(DEFAULT_READ_BUFFER_MB, 4 * 1024 * 1024);
    let readahead_max_width = derive_readahead_max_width(
        DEFAULT_READ_BUFFER_MB,
        4 * 1024 * 1024,
        DEFAULT_READAHEAD_WIDTH,
    );
    let fetch_aimd = build_fetch_aimd_config(readahead_max_width, &VfsConfig::default());

    assert_eq!(buffer_blocks, 75);
    assert_eq!(readahead_max_width, 75);
    assert_eq!(fetch_aimd.max_window, 75.0);
    assert_eq!(fetch_aimd.initial_window, 18.75);
    assert_eq!(
        fetch_aimd.idle_reset_after,
        Some(Duration::from_secs(DEFAULT_FETCH_IDLE_RESET_SECS))
    );
}

#[test]
fn zero_readahead_width_disables_derived_max_width() {
    assert_eq!(
        derive_readahead_max_width(DEFAULT_READ_BUFFER_MB, 4 * 1024 * 1024, 0),
        0
    );
}

#[test]
fn upload_aimd_uses_independent_idle_reset_policy() {
    let upload_aimd = build_upload_aimd_config(&VfsConfig {
        upload_idle_reset_secs: 120,
        ..VfsConfig::default()
    });

    assert_eq!(upload_aimd.idle_reset_after, Some(Duration::from_secs(120)));
}

#[test]
fn local_backend_budget_honors_runtime_dirty_window() {
    let runtime_tuning = CacheRuntimeTuning {
        max_dirty_bytes: 1024 * 1024,
        write_eager_flush_batch_bytes: 512 * 1024,
        upload_cache_io_slots: 16,
        transit_memory_budget_bytes: 1024 * 1024,
    };

    assert_eq!(
        derive_local_backend_budget_bytes(4096, 512 * 1024, runtime_tuning),
        1024 * 1024
    );
}

#[test]
fn local_backend_budget_never_drops_below_block_size() {
    let runtime_tuning = CacheRuntimeTuning {
        max_dirty_bytes: 128 * 1024,
        write_eager_flush_batch_bytes: 128 * 1024,
        upload_cache_io_slots: 16,
        transit_memory_budget_bytes: 1024 * 1024,
    };

    assert_eq!(
        derive_local_backend_budget_bytes(64 * 1024, 512 * 1024, runtime_tuning),
        512 * 1024
    );
}

#[test]
fn cache_runtime_tuning_uses_internal_config_overrides_when_non_default() {
    let derived = derive_cache_runtime_tuning(16 * 1024 * 1024, 512 * 1024, 16 * 1024 * 1024);
    let defaults = VfsConfig::default();
    let mut config = defaults.clone();
    config.max_dirty_bytes = 1024 * 1024;
    config.write_eager_flush_batch_bytes = 1024 * 1024;
    config.upload_cache_io_slots = 7;
    let overrides = CacheRuntimeTuningOverrides {
        max_dirty_bytes: true,
        write_eager_flush_batch_bytes: true,
        upload_cache_io_slots: true,
        ..Default::default()
    };

    let resolved = resolve_cache_runtime_tuning_overrides(derived, &config, overrides);

    assert_eq!(resolved.max_dirty_bytes, 1024 * 1024);
    assert_eq!(resolved.write_eager_flush_batch_bytes, 1024 * 1024);
    assert_eq!(resolved.upload_cache_io_slots, 7);
}

#[test]
fn cache_runtime_tuning_respects_explicit_default_values_from_raw_config() {
    let validated = VfsConfigRaw {
        max_dirty_bytes: Some(DEFAULT_MAX_DIRTY_BYTES),
        write_eager_flush_batch_bytes: Some(DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES),
        ..Default::default()
    }
    .validate()
    .expect("validate explicit default runtime tuning values");
    let derived =
        derive_cache_runtime_tuning(16 * 1024 * 1024 * 1024, 512 * 1024, 16 * 1024 * 1024);
    let resolved = resolve_cache_runtime_tuning_overrides(
        derived,
        &validated,
        validated.cache_runtime_overrides(),
    );

    assert_eq!(derived.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
    assert_eq!(resolved.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
    assert_eq!(
        resolved.write_eager_flush_batch_bytes,
        DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES
    );
}

#[test]
fn vfs_config_raw_validate_clamps_and_normalizes() {
    let validated = VfsConfigRaw {
        janitor_interval_secs: Some(0),
        prefetch_concurrency: Some(0),
        slab_flush_interval_ms: Some(0),
        upload_worker_count: Some(0),
        principal: Some("   ".to_string()),
        ..Default::default()
    }
    .validate()
    .expect("validate clamping");

    assert_eq!(validated.janitor_interval_secs, 1);
    assert_eq!(
        validated.prefetch_concurrency,
        DEFAULT_PREFETCH_CONCURRENCY_MIN
    );
    assert_eq!(validated.slab_flush_interval_ms, 1);
    assert_eq!(validated.upload_worker_count, DEFAULT_UPLOAD_WORKER_COUNT);
    assert_eq!(validated.principal, "*");
}

#[test]
fn vfs_config_raw_validate_preserves_explicit_upload_worker_count() {
    let validated = VfsConfigRaw {
        upload_worker_count: Some(20),
        ..Default::default()
    }
    .validate()
    .expect("validate explicit upload worker count");

    assert_eq!(validated.upload_worker_count, 20);
}

#[test]
fn upload_pipeline_summary_logs_only_when_pending_work_exists() {
    assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 0, 0, 0));
    assert!(StudioVfs::upload_pipeline_summary_is_active(1, 0, 0, 0));
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 1, 0, 0));
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 0, 1, 0));
    assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 0, 0, 1)); // < 10 threshold
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 0, 0, 10));
}

#[test]
fn upload_pipeline_summary_is_active_checks() {
    assert!(StudioVfs::upload_pipeline_summary_is_active(256, 0, 0, 0));
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 4, 0, 0));
    assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 0, 0, 9));
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 0, 0, 11));
}

#[test]
fn upload_pipeline_summary_formats_pending_uploads() {
    assert_eq!(StudioVfs::format_upload_pipeline_pending_uploads(0), "0");
    assert_eq!(
        StudioVfs::format_upload_pipeline_pending_uploads(512),
        "512B"
    );
    assert_eq!(
        StudioVfs::format_upload_pipeline_pending_uploads(12 * 1024),
        "12KB"
    );
    assert_eq!(
        StudioVfs::format_upload_pipeline_pending_uploads(1904 * 1024 * 1024),
        "1.9GB"
    );
}

#[test]
fn foreground_speedometer_p90_uses_recent_tail() {
    let p90 = StudioVfs::p90_ns(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(p90, Some(9));
}

#[test]
fn foreground_speedometer_p90_handles_empty_input() {
    assert_eq!(StudioVfs::p90_ns(vec![]), None);
}

#[test]
fn foreground_speedometer_trim_drops_expired_samples() {
    let mut samples = std::collections::VecDeque::from([
        ForegroundReadSample {
            timestamp_ms: 100,
            latency_ns: 10,
            bytes_read: 4_096,
            access_class: AccessClass::RandomInteractiveRead,
            policy: AccessPolicy::default(),
        },
        ForegroundReadSample {
            timestamp_ms: 200,
            latency_ns: 20,
            bytes_read: 4_096,
            access_class: AccessClass::RandomInteractiveRead,
            policy: AccessPolicy::default(),
        },
        ForegroundReadSample {
            timestamp_ms: 350,
            latency_ns: 30,
            bytes_read: 4_096,
            access_class: AccessClass::SequentialMediaRead,
            policy: AccessPolicy::from_class(
                AccessClass::SequentialMediaRead,
                AccessTrackerConfig {
                    block_size: 4096,
                    readahead_base_width: 8,
                    readahead_max_width: 32,
                    readahead_min_prefetch: 2,
                },
            ),
        },
    ]);
    ReadState::trim_foreground_latency_samples(&mut samples, 400, Duration::from_millis(150));
    let kept: Vec<(u64, u64, u64)> = samples
        .into_iter()
        .map(|sample| (sample.timestamp_ms, sample.latency_ns, sample.bytes_read))
        .collect();
    assert_eq!(kept, vec![(350, 30, 4_096)]);
}

#[test]
fn foreground_read_active_respects_idle_grace_after_last_read() {
    let state = ReadState::new(1);
    let grace = Duration::from_millis(50);

    state.mark_foreground_read(100);
    assert!(state.foreground_read_active(100, grace));

    state.unmark_foreground_read(120);
    assert!(
        state.foreground_read_active(169, grace),
        "idle grace should keep reads active shortly after scope exit"
    );
    assert!(
        !state.foreground_read_active(171, grace),
        "foreground read activity should expire after the configured idle grace"
    );
}

#[tokio::test]
async fn foreground_idle_notify_fires_when_last_read_finishes() {
    let state = ReadState::new(1);
    state.mark_foreground_read(100);
    let notified = state.foreground_idle_notify.notified();
    state.unmark_foreground_read(120);
    tokio::time::timeout(Duration::from_millis(20), notified)
        .await
        .expect("dropping the last read should notify idle waiters");
}

#[test]
fn wait_for_foreground_reads_idle_honors_idle_grace() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        upload_user_idle_grace_ms: 20,
        ..VfsConfig::default()
    };
    let (rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "foreground-read-idle-wait", cfg);
    let _guard = rt.enter();

    rt.block_on(async {
        vfs.state.read.mark_foreground_read(now_unix_millis());
        let waiter = {
            let vfs = vfs.clone();
            tokio::spawn(async move {
                vfs.wait_for_foreground_reads_idle(Duration::from_millis(80))
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(5)).await;
        vfs.state.read.unmark_foreground_read(now_unix_millis());
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            !waiter.is_finished(),
            "helper should keep upload rereads paused until the idle grace expires"
        );

        assert!(waiter.await.expect("join foreground idle waiter"));
    });
}

#[test]
fn foreground_io_active_includes_active_writers() {
    let state = ReadState::new(1);

    state.increment_active_access_class(AccessClass::SequentialWriter);

    assert!(
        state.foreground_io_active(100, Duration::from_millis(50)),
        "active sequential writers must protect local materialization from upload rereads"
    );
}

#[test]
fn foreground_read_scope_records_sample_and_clears_inflight() {
    let state = ReadState::new(1);

    {
        let mut scope = ForegroundReadScope::new(
            &state,
            Duration::from_secs(1),
            AccessPolicy::from_class(
                AccessClass::SequentialMediaRead,
                AccessTrackerConfig {
                    block_size: 4096,
                    readahead_base_width: 8,
                    readahead_max_width: 32,
                    readahead_min_prefetch: 2,
                },
            ),
        );
        scope.record_bytes(4096);
        assert_eq!(
            state.foreground_reads_inflight.load(Ordering::Relaxed),
            1,
            "creating a scope should mark one active foreground read"
        );
    }

    assert_eq!(
        state.foreground_reads_inflight.load(Ordering::Relaxed),
        0,
        "dropping the scope should clear the inflight foreground read"
    );

    let (p90_ns, total_samples, window_bytes, window_coverage_ms) =
        state.foreground_speedometer_snapshot(now_unix_millis(), Duration::from_secs(1));
    assert!(
        p90_ns.is_some(),
        "dropping the scope should record latency telemetry"
    );
    assert_eq!(total_samples, 1);
    assert_eq!(window_bytes, 4096);
    assert!(window_coverage_ms >= 1);
}

#[test]
fn release_fh_clears_per_stream_read_tracking() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-clears-stream-state");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs.create(1, b"stream-state.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let stream_id = vfs.get_open_file(fh).unwrap().stream_id();
        let key = ReadStreamKey { ino, stream_id };
        let policy = AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_tracker)
            .readahead_policy();

        let _ = vfs.readahead.record_read_for_stream(key, 0, 0, policy);
        vfs.state
            .read
            .prefetch_last_hint
            .lock()
            .insert((ino, stream_id), (0, 1));

        assert!(vfs.readahead.debug_has_stream(key));
        assert!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, stream_id))
        );

        vfs.release_fh(fh).await.unwrap();

        assert!(!vfs.readahead.debug_has_stream(key));
        assert!(
            !vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, stream_id))
        );
    });
}

#[test]
fn active_open_overlay_tracks_only_mutating_handles() {
    assert!(!StudioVfs::active_open_overlay_required(
        OpenFlags::RDONLY,
        false
    ));
    assert!(StudioVfs::active_open_overlay_required(
        OpenFlags::RDONLY,
        true
    ));
    assert!(StudioVfs::active_open_overlay_required(
        OpenFlags::WRONLY,
        false
    ));
    assert!(StudioVfs::active_open_overlay_required(
        OpenFlags::RDWR,
        false
    ));
    assert!(StudioVfs::active_open_overlay_required(
        OpenFlags::RDONLY | OpenFlags::APPEND,
        false
    ));
}

#[test]
fn fallback_read_access_tracking_does_not_increment_active_handle_counts() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "fallback-access-tracking");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"fallback-access-tracking.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let block = vfs.access_tracker.block_size as usize;

        let mut last = AccessClass::RandomInteractiveRead;
        for step in 0..4u64 {
            last = vfs
                .state
                .read
                .observe_read_access(None, ino, step * block as u64, block, vfs.access_tracker)
                .class;
        }

        assert_eq!(
            last,
            AccessClass::SequentialMediaRead,
            "contiguous fallback reads should still classify as sequential"
        );

        let stats = vfs.foreground_read_protection_stats();
        assert_eq!(
            stats.active_sequential_media_reads, 0,
            "fallback tracking must not pretend there is an active handle"
        );
        assert_eq!(
            stats.active_random_interactive_reads, 0,
            "fallback tracking must not perturb active read-handle counts"
        );
        assert!(
            vfs.state.read.access_class_transitions[AccessClass::SequentialMediaRead.index()]
                .load(Ordering::Relaxed)
                >= 1,
            "fallback tracking should still contribute access-class transition telemetry"
        );
    });
}

#[test]
fn handle_read_access_tracking_promotes_active_class_counts() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "handle-access-tracking");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-access-tracking.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let block = vfs.access_tracker.block_size as usize;

        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::RandomInteractiveRead),
            1,
            "new read handle should start in the random-interactive class"
        );
        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::SequentialMediaRead),
            0
        );

        let mut last = AccessClass::RandomInteractiveRead;
        for step in 0..4u64 {
            let open_file = vfs.get_open_file(fh).expect("open file");
            last = vfs
                .state
                .read
                .observe_read_access(
                    Some(&open_file),
                    ino,
                    step * block as u64,
                    block,
                    vfs.access_tracker,
                )
                .class;
        }

        assert_eq!(
            last,
            AccessClass::SequentialMediaRead,
            "contiguous handle reads should promote the handle into sequential classification"
        );
        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::RandomInteractiveRead),
            0,
            "promotion should decrement the previous active class"
        );
        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::SequentialMediaRead),
            1,
            "promotion should increment the new active class"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn clear_access_tracking_clears_fallback_states_and_hints() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "clear-access-tracking");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"clear-access-tracking.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let block = vfs.access_tracker.block_size as usize;

        let _ = vfs
            .state
            .read
            .observe_read_access(None, ino, 0, block, vfs.access_tracker);
        let _ = vfs
            .state
            .read
            .observe_write_access(None, ino, 0, block, vfs.access_tracker);
        vfs.state
            .read
            .prefetch_last_hint
            .lock()
            .insert((ino, 77), (4, 2));

        assert!(
            vfs.state
                .read
                .read_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            vfs.state
                .read
                .write_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, 77))
        );

        vfs.clear_access_tracking(ino);

        assert!(
            !vfs.state
                .read
                .read_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            !vfs.state
                .read
                .write_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            !vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, 77))
        );
    });
}

#[test]
fn foreground_throughput_uses_minimum_coverage_floor_for_short_windows() {
    let throughput = StudioVfs::foreground_throughput_bytes_per_sec(1, 1_048_576, 10);
    assert_eq!(throughput, 10_485_760.0);
}

#[test]
fn foreground_throughput_is_zero_without_samples() {
    let throughput = StudioVfs::foreground_throughput_bytes_per_sec(0, 1_048_576, 10);
    assert_eq!(throughput, 0.0);
}

#[test]
fn test_run_sync_with_timeout_returns_result_before_deadline() {
    let result = run_sync_with_timeout(Duration::from_millis(100), || Ok::<_, VfsError>(7))
        .expect("operation should succeed");
    assert_eq!(result, Some(7));
}

#[test]
fn test_run_sync_with_timeout_returns_none_on_timeout() {
    let finished = Arc::new(AtomicBool::new(false));
    let finished_clone = finished.clone();
    let t0 = std::time::Instant::now();
    let result = run_sync_with_timeout(Duration::from_millis(10), move || {
        std::thread::sleep(Duration::from_millis(100));
        finished_clone.store(true, AtomicOrdering::Release);
        Ok::<_, VfsError>(())
    })
    .expect("timeout helper should not fail");

    // The primary timeout should report None (timed out).
    assert_eq!(result, None);
    // The thread is joined to ensure clean Arc release, so the operation
    // finishes by the time we return.
    assert!(
        finished.load(AtomicOrdering::Acquire),
        "thread should have been joined after timeout"
    );
    assert!(
        t0.elapsed() >= Duration::from_millis(100),
        "run_sync_with_timeout should wait for thread join"
    );
}

#[test]
fn test_offset_within_block() {
    let block_size = 4 * 1024 * 1024; // 4MB

    assert_eq!(offset_within_block(0, block_size), 0);
    assert_eq!(offset_within_block(100, block_size), 100);
    assert_eq!(offset_within_block(4 * 1024 * 1024, block_size), 0);
    assert_eq!(offset_within_block(4 * 1024 * 1024 + 100, block_size), 100);
}

#[test]
fn test_sticky_upload_error_is_deterministic_across_sync_points() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-upload-error");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sticky.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

        let fsync_err = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err.to_string().contains("sticky upload error"),
            "fsync should surface sticky upload errors"
        );
        let fsync_err_again = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err_again.to_string().contains("sticky upload error"),
            "sticky upload errors should be deterministic across fsync calls"
        );
        vfs.clear_upload_sticky_error(ino);
        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.fsync(ino, false).await.unwrap();

        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));
        // flush_fh guarantees local durability but ignores background upload
        // errors — the data is safe in NVMe and uploads will be retried.
        vfs.flush_fh(fh).await.unwrap();
        // A second flush also succeeds (sticky error is harmless).
        vfs.flush_fh(fh).await.unwrap();
        vfs.clear_upload_sticky_error(ino);
        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush_fh(fh).await.unwrap();

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_fsync_reports_sticky_error_after_persisting_current_changes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-fsync-order");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sticky-fsync.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let payload = b"persist-before-sticky-report";

        vfs.write(ino, 0, payload).await.unwrap();
        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

        let fsync_err = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err.to_string().contains("sticky upload error"),
            "fsync should still report sticky upload errors"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "fsync must persist current dirty attrs before reporting sticky EIO"
        );

        let fsync_err_again = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err_again.to_string().contains("sticky upload error"),
            "sticky upload errors should remain visible until recovery"
        );
        vfs.clear_upload_sticky_error(ino);
        vfs.fsync(ino, false).await.unwrap();
    });
}

#[test]
fn test_release_fh_reports_sticky_error_after_persisting_current_changes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-release-order");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sticky-release.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"persist-before-release-sticky-report";

        vfs.write(ino, 0, payload).await.unwrap();
        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

        let release_err = vfs.release_fh(fh).await.unwrap_err();
        assert!(
            release_err.to_string().contains("sticky upload error"),
            "release_fh should surface sticky upload errors"
        );
        assert_eq!(
            vfs.open_count(ino),
            0,
            "release_fh should still close the handle"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "release_fh must persist local dirty state before reporting sticky EIO"
        );
    });
}

#[test]
fn test_release_fh_with_outcome_materializes_pending_inode_dirty_state_on_last_close() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-close-ownership");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"release-outcome.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should remain pending until an explicit barrier or last close"
        );

        let first = vfs.release_fh_with_outcome(fh1).await.unwrap();
        assert!(
            !first.closed_last_handle,
            "closing one of multiple handles must not report last-handle ownership"
        );
        assert!(
            !first.flushed_local_dirty_state,
            "non-final close should not attribute a local dirty-state drain"
        );
        assert_eq!(vfs.open_count(ino), 1, "one handle should remain open");

        let second = vfs.release_fh_with_outcome(fh2).await.unwrap();
        assert!(
            second.closed_last_handle,
            "the final close should report last-handle ownership"
        );
        assert!(
            second.flushed_local_dirty_state,
            "the final close should report that it materialized the pending inode dirty state"
        );
        assert_eq!(vfs.open_count(ino), 0, "all handles should be closed");
    });
}

#[test]
fn test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-clean-skip");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-outcome-clean-skip.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush_fh(fh).await.unwrap();

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(
            outcome.closed_last_handle,
            "the only handle should still be reported as the last-handle close"
        );
        assert!(
            !outcome.flushed_local_dirty_state,
            "release should report a clean skip after explicit flush already drained local state"
        );
        assert_eq!(
            vfs.open_count(ino),
            0,
            "release should still close the handle"
        );
    });
}

#[test]
fn test_release_fh_clean_skip_does_not_reenter_sticky_error_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-clean-sticky-skip");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-outcome-clean-sticky-skip.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        let flush = vfs.flush_fh_with_outcome(fh).await.unwrap();
        assert!(
            flush.flushed_local_dirty_state,
            "explicit flush should drain the local dirty state"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "release precondition should be a clean inode"
        );

        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));
        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();

        assert!(
            outcome.closed_last_handle,
            "the only handle should still be reported as the last-handle close"
        );
        assert!(
            !outcome.flushed_local_dirty_state,
            "clean release should not report another local dirty-state drain"
        );
        assert_eq!(
            vfs.open_count(ino),
            0,
            "release should still close the handle"
        );
    });
}

#[test]
fn test_release_fh_clean_waits_for_file_session_gate() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-clean-session-skip");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-outcome-clean-session-skip.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "test expects a clean inode before release"
        );

        let _read_session = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let release_vfs = Arc::clone(&vfs);
        let release_task =
            tokio::spawn(async move { release_vfs.release_fh_with_outcome(fh).await });

        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if vfs.file_session_stats().queued_ops >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("clean release should wait behind the file-session gate while read is active");

        drop(_read_session);
        let release = release_task
            .await
            .expect("clean release task should not panic")
            .expect("clean release should succeed");

        assert!(
            release.closed_last_handle,
            "the only handle should still be reported as the last-handle close"
        );
        assert!(
            !release.flushed_local_dirty_state,
            "clean release should not report local dirty-state flushing"
        );
    });
}

#[test]
fn flush_then_release_keeps_written_bytes_visible_to_reopened_reader() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-release-reopen-read");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-release-read.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"flush-close-contract";

        vfs.write(ino, 0, payload).await.unwrap();
        vfs.flush_fh(fh).await.unwrap();
        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(
            !outcome.flushed_local_dirty_state,
            "explicit flush should leave release as a clean close"
        );

        let reopened = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);
        vfs.release_fh(reopened).await.unwrap();
    });
}

#[test]
fn ignored_entry_writes_preserve_synthetic_attr_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-entry-write-size");
    let _guard = rt.enter();
    rt.block_on(async {
        let inode_attr = vfs.create(1, b"._DS_Store", 0, 0, 0o644).unwrap();
        let inode_payload = b"synthetic-inode";
        vfs.write_with_local_sync_policy(
            inode_attr.ino,
            0,
            inode_payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        let inode_after = vfs.getattr(inode_attr.ino).unwrap();
        assert_eq!(
            inode_after.size,
            inode_payload.len() as u64,
            "successful ignored-entry inode writes must still advance the synthetic attr size"
        );
        let inode_read = vfs
            .read(inode_attr.ino, 0, inode_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            inode_read.len(),
            inode_payload.len(),
            "ignored-entry reads should observe the updated synthetic file length"
        );

        let handle_attr = vfs.create(1, b"._synthetic", 0, 0, 0o644).unwrap();
        let handle_payload = b"synthetic-handle";
        let fh = vfs
            .open_file(handle_attr.ino, OpenFlags::RDWR, false)
            .unwrap();
        vfs.write_fh_with_local_sync_policy(
            fh,
            handle_attr.ino,
            0,
            handle_payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        let handle_after = vfs.getattr(handle_attr.ino).unwrap();
        assert_eq!(
            handle_after.size,
            handle_payload.len() as u64,
            "successful ignored-entry handle writes must still advance the synthetic attr size"
        );
        let handle_read = vfs
            .read(handle_attr.ino, 0, handle_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            handle_read.len(),
            handle_payload.len(),
            "ignored-entry handle writes should remain readable at the new synthetic length"
        );
        vfs.release_fh(fh).await.unwrap();
        let handle_after_release = vfs.getattr(handle_attr.ino).unwrap();
        assert_eq!(
            handle_after_release.size,
            handle_payload.len() as u64,
            "ignored-entry close must not remove the synthetic inode before copyfile metadata completes"
        );
    });
}

#[test]
fn same_mount_read_keeps_opportunistic_handle_write_visible_after_local_dirty_clear() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-opportunistic-handle-read");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-opportunistic-handle-read.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"slice-writer-visible";

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let overlay_spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            overlay_spans.iter().all(|span| span.payload.is_none()),
            "opportunistic handle writes should not materialize block-backed metadata before a barrier: {overlay_spans:?}"
        );

        vfs.clear_pending_payloads_for_inode(ino);

        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn same_mount_read_prefers_newer_opportunistic_writer_stack_spans() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-overlapping-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-overlapping-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(fh1, ino, 0, b"hello", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        vfs.write_fh_with_local_sync_policy(fh2, ino, 1, b"XYZ", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        vfs.clear_pending_payloads_for_inode(ino);

        let bytes = vfs.read(ino, 0, 5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"hXYZo");

        vfs.release_fh(fh1).await.unwrap();
        vfs.release_fh(fh2).await.unwrap();
    });
}

#[test]
fn writer_stack_global_dirty_cap_drains_older_inodes_after_limit_drop() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-writer-stack-global-cap");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr1 = vfs.create(1, b"cap-a.txt", 0, 0, 0o644).unwrap();
        let attr2 = vfs.create(1, b"cap-b.txt", 0, 0, 0o644).unwrap();
        let attr3 = vfs.create(1, b"cap-c.txt", 0, 0, 0o644).unwrap();
        let fh1 = vfs.open_file(attr1.ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(attr2.ino, OpenFlags::RDWR, false).unwrap();
        let fh3 = vfs.open_file(attr3.ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(
            fh1,
            attr1.ino,
            0,
            b"12345678",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        vfs.write_fh_with_local_sync_policy(
            fh2,
            attr2.ino,
            0,
            b"abcdefgh",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        assert_eq!(vfs.state.writer_stack.total_dirty_bytes(), 16);

        vfs.set_max_dirty_bytes(8);

        vfs.write_fh_with_local_sync_policy(
            fh3,
            attr3.ino,
            0,
            b"z",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        assert!(
            vfs.state.writer_stack.total_dirty_bytes() <= vfs.max_dirty_bytes(),
            "once the runtime dirty limit drops, the next opportunistic write should drain enough writer-stack state to get back under the global cap"
        );

        vfs.release_fh(fh1).await.unwrap();
        vfs.release_fh(fh2).await.unwrap();
        vfs.release_fh(fh3).await.unwrap();
    });
}

#[test]
fn aggressive_materialization_threshold_is_half_of_dirty_budget() {
    assert_eq!(aggressive_materialization_threshold_bytes(0), 1);
    assert_eq!(aggressive_materialization_threshold_bytes(1), 1);
    assert_eq!(aggressive_materialization_threshold_bytes(8), 4);
    assert_eq!(hard_backpressure_threshold_bytes(8), 16);
}

#[test]
fn completed_block_materialization_respects_eager_batch_limit() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        write_eager_flush_batch_bytes: 8 * 1024 * 1024,
        ..VfsConfig::default()
    };
    let (rt, vfs, _meta) =
        make_unstarted_test_vfs_with_config(&tmp, "writer-stack-completed-batch-cap", cfg);
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"writer-stack-completed-batch-cap.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x5Au8; block_size * 3];

        vfs.record_writer_stack_state_bytes(fh, attr.ino, 0, &payload)
            .await
            .unwrap();
        let materialized = vfs
            .materialize_writer_stack_completed_blocks_for_inode(attr.ino)
            .await
            .unwrap();

        assert_eq!(
            materialized,
            block_size * 2,
            "one completed-block pass should not detach more than the eager batch limit"
        );
        assert_eq!(
            vfs.state
                .writer_stack
                .completed_block_count_for_inode(attr.ino)
                .unwrap(),
            1,
            "blocks beyond the batch cap should remain buffered for the next drain pass"
        );
    });
}

#[test]
fn completed_block_materialization_uses_prefetched_object_ids() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-545-completed-prefetch-ids");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-545-completed-prefetch-ids.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x45u8; block_size];
        let before = meta.next_object_ids(1).unwrap()[0];

        vfs.record_writer_stack_state_bytes(fh, attr.ino, 0, &payload)
            .await
            .unwrap();
        vfs.prefetch_writer_stack_object_ids_for_completed_blocks(attr.ino)
            .await
            .unwrap();
        let after_prefetch = meta.next_object_ids(1).unwrap()[0];

        let materialized = vfs
            .materialize_writer_stack_completed_blocks_for_inode(attr.ino)
            .await
            .unwrap();
        let after_materialize = meta.next_object_ids(1).unwrap()[0];

        assert_eq!(materialized, block_size);
        assert_eq!(after_prefetch, before + 2);
        assert_eq!(
            after_materialize,
            after_prefetch + 1,
            "completed-block materialization should consume prefetched IDs without reserving more"
        );
    });
}

#[test]
fn completed_block_materialization_does_not_join_gapped_prefetch_batches() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-545-gapped-prefetch-ids");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-545-gapped-prefetch-ids.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let block = vec![0x46u8; block_size];
        let before = meta.next_object_ids(1).unwrap()[0];

        vfs.record_writer_stack_state_bytes(fh, attr.ino, 0, &block)
            .await
            .unwrap();
        vfs.prefetch_writer_stack_object_ids_for_completed_blocks(attr.ino)
            .await
            .unwrap();
        let externally_reserved = meta.next_object_ids(1).unwrap()[0];

        vfs.record_writer_stack_state_bytes(fh, attr.ino, block_size as u64, &block)
            .await
            .unwrap();
        vfs.prefetch_writer_stack_object_ids_for_completed_blocks(attr.ino)
            .await
            .unwrap();

        let materialized = vfs
            .materialize_writer_stack_completed_blocks_for_inode(attr.ino)
            .await
            .unwrap();
        let mut object_ids = vfs.local_payload_backend().object_ids();
        object_ids.sort_unstable();

        assert_eq!(materialized, block_size * 2);
        assert_eq!(externally_reserved, before + 2);
        assert!(
            !object_ids.contains(&externally_reserved),
            "gapped prefetched batches must not be exposed as one contiguous object ID range"
        );
        assert_eq!(object_ids[1], object_ids[0] + 1);
    });
}

#[test]
fn completed_block_prefetch_caps_skipped_range_queue() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-545-prefetch-queue-cap");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-545-prefetch-queue-cap.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x47u8; block_size * 2];

        {
            let mut queue = vfs.state.writer_stack_prefetched_object_ids.lock();
            for index in 0..WRITER_STACK_PREFETCHED_OBJECT_ID_RANGE_LIMIT {
                let start = 10_000 + u64::try_from(index).unwrap() * 10;
                queue.push_back(PrefetchedWriterStackObjectIds::new(attr.ino, start, 1).unwrap());
            }
        }

        vfs.record_writer_stack_state_bytes(fh, attr.ino, 0, &payload)
            .await
            .unwrap();
        vfs.prefetch_writer_stack_object_ids_for_completed_blocks(attr.ino)
            .await
            .unwrap();

        assert!(
            vfs.state.writer_stack_prefetched_object_ids.lock().len()
                <= WRITER_STACK_PREFETCHED_OBJECT_ID_RANGE_LIMIT,
            "prefetched object ID range queue should stay bounded"
        );
    });
}

#[test]
fn completed_block_prefetch_does_not_skip_due_to_other_inode_range() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-545-prefetch-inode-affinity");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr_a = vfs
            .create(1, b"sfs-545-prefetch-inode-affinity-a.bin", 0, 0, 0o644)
            .unwrap();
        let attr_b = vfs
            .create(1, b"sfs-545-prefetch-inode-affinity-b.bin", 0, 0, 0o644)
            .unwrap();
        let fh_a = vfs.open_file(attr_a.ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x48u8; block_size];

        vfs.state
            .writer_stack_prefetched_object_ids
            .lock()
            .push_back(PrefetchedWriterStackObjectIds::new(attr_b.ino, 50_000, 32).unwrap());

        vfs.record_writer_stack_state_bytes(fh_a, attr_a.ino, 0, &payload)
            .await
            .unwrap();
        vfs.prefetch_writer_stack_object_ids_for_completed_blocks(attr_a.ino)
            .await
            .unwrap();

        assert_eq!(
            vfs.state.writer_stack_prefetched_object_ids.lock().len(),
            2,
            "one inode should not skip prefetch because another inode owns a large range"
        );
    });
}

#[test]
fn ingress_pressure_counts_buffered_writer_stack_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-257-ingress-pressure-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-ingress-pressure-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"buffer-me";

        vfs.set_max_dirty_bytes(payload.len());
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "clean ingress state should accept the first buffered handle write"
        );

        vfs.write_fh_with_local_sync_policy(
            fh,
            attr.ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        assert!(
            vfs.state.writer_stack.has_dirty_writers(attr.ino),
            "setup should keep the payload buffered in the writer stack"
        );
        let pressure = vfs.ingress_pressure_for(1);
        assert_eq!(
            pressure.dirty_mem_bytes,
            payload.len(),
            "ingress pressure should include buffered writer-stack bytes before they materialize into local_dirty"
        );
        assert_eq!(
            pressure.lane,
            VfsIngressLane::Backpressure,
            "once buffered writer-stack bytes consume the dirty budget, follow-up ingress should see backpressure"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_deferred_write_rechecks_hard_pressure_without_inline_fallback() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) =
        make_unstarted_test_vfs(&tmp, "sfs-306-deferred-backpressure-nonblocking");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-305-deferred-backpressure-recheck.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let pressure_attr = vfs
            .create(1, b"sfs-305-existing-pressure.txt", 0, 0, 0o644)
            .unwrap();
        let pressure_fh = vfs
            .open_file(pressure_attr.ino, OpenFlags::RDWR, false)
            .unwrap();

        vfs.set_max_dirty_bytes(8);
        let held_gate = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let (entry, _guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve the deferred materialization gate");

        let queued_vfs = Arc::clone(&vfs);
        let queued_write = tokio::spawn(async move {
            queued_vfs
                .write_fh_with_deferred_materialize(
                    fh,
                    ino,
                    0,
                    b"x",
                    LocalSyncPolicy::Opportunistic,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if vfs.file_session_stats().queued_ops >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deferred write should queue behind the held inode gate");

        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh: pressure_fh,
                    ino: pressure_attr.ino,
                    current_size: 0,
                    content_epoch: pressure_attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xAB; 17]),
            )
            .expect("seed writer-stack pressure above the hard threshold");
        assert!(
            vfs.total_pending_write_buffer_bytes() > super::hard_backpressure_threshold_bytes(8),
            "setup should exceed the hard threshold while the deferred write is still waiting"
        );

        drop(held_gate);
        queued_write
            .await
            .expect("queued write task should not panic")
            .expect("queued write should stay deferred and complete");
        drop(entry);

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.write_backpressure.hard_events, 1,
            "deferred write must re-check hard backpressure after waiting on the inode gate"
        );
        assert!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino) > 0,
            "opportunistic deferred writes must stay in the foreground buffer instead of materializing inline under hard pressure"
        );

        vfs.release_fh(fh).await.unwrap();
        vfs.release_fh(pressure_fh).await.unwrap();
    });
}

#[test]
fn opportunistic_sustained_pre_drain_schedules_without_inline_materialization() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-306-sustained-pre-drain-nonblocking");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-306-sustained-pre-drain.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.set_max_dirty_bytes(64 * 1024 * 1024);
        let seeded_len = 33 * 1024 * 1024;
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino,
                    current_size: 0,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xCD; seeded_len]),
            )
            .expect("seed sustained writer-stack pressure");
        let (entry, _guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve the deferred materialization gate");
        let now = now_unix_millis();
        vfs.state.sustained_write_streams.lock().insert(
            ino,
            SustainedWriteState {
                streak_start_ms: now.saturating_sub(SUSTAINED_WRITE_DELAY_START_MS),
                last_write_ms: now.saturating_sub(1),
                last_end_offset: seeded_len as u64,
            },
        );

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            seeded_len as u64,
            b"x",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        assert!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino) >= seeded_len,
            "sustained pre-drain must not materialize buffered data inline on the foreground write"
        );

        drop(entry);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn deferred_write_keeps_writeback_path_when_soft_dirty_budget_is_full() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-305-deferred-soft-writeback");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-305-deferred-soft-writeback.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let pressure_attr = vfs
            .create(1, b"sfs-305-existing-soft-writeback-pressure.txt", 0, 0, 0o644)
            .unwrap();
        let pressure_fh = vfs
            .open_file(pressure_attr.ino, OpenFlags::RDWR, false)
            .unwrap();

        vfs.set_max_dirty_bytes(8);
        let (entry, _guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve the deferred materialization gate");
        vfs.state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh: pressure_fh,
                    ino: pressure_attr.ino,
                    current_size: 0,
                    content_epoch: pressure_attr.content_epoch,
                    first_object_id: 0,
                    offset: 0,
                },
                Bytes::from(vec![0xCD; 9]),
            )
            .expect("seed writer-stack pressure above the soft dirty budget");
        assert_eq!(
            vfs.ingress_pressure_for(1).lane,
            VfsIngressLane::Backpressure,
            "setup should be over the soft dirty budget but below hard backpressure"
        );

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            b"x",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("soft dirty budget should not force a synchronous writeback fallback");
        drop(entry);

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.write_backpressure.hard_events, 0,
            "soft dirty-budget pressure should not be counted as hard backpressure"
        );
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "soft dirty-budget pressure should preserve JuiceFS-like writeback acceptance instead of forcing inline sync materialization"
        );

        vfs.release_fh(fh).await.unwrap();
        vfs.release_fh(pressure_fh).await.unwrap();
    });
}

#[test]
fn writer_stack_write_invalidates_attr_cache_without_kernel_notify() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "writer-stack-write-invalidation");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"writer-stack-write-invalidation.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();

        vfs.getattr_async(attr.ino).await.unwrap();
        assert!(
            vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(attr.ino),
            "setup should populate the attr read cache before the write"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        vfs.write_fh_with_deferred_materialize(
            fh,
            attr.ino,
            0,
            b"juicefs-style-invalidate",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        assert_eq!(
            invalidator.snapshot(),
            Vec::<Ino>::new(),
            "accepted local writes should not notify the kernel cache invalidator; the write reply already carries fresh file info for the writer, matching JuiceFS"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn namespace_invalidation_includes_runtime_only_inodes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "namespace-invalidation-runtime-inodes");
    let _guard = rt.enter();
    rt.block_on(async {
        let dirty = vfs.create(1, b"dirty-runtime-only.bin", 0, 0, 0o644).unwrap();
        let cached = vfs.create(1, b"cached-runtime-only.bin", 0, 0, 0o644).unwrap();

        let mut dirty_attr = dirty.clone();
        dirty_attr.size = 227;
        dirty_attr.upload_state = MetaUploadState::Updating;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(dirty.ino, dirty_attr);
        vfs.state.namespace.attr_read_cache.lock().put(
            cached.ino,
            cached.clone(),
            ATTR_CACHE_TTL,
            Instant::now(),
        );

        let inodes = vfs.runtime_namespace_invalidation_inodes();

        assert!(
            inodes.contains(&dirty.ino),
            "namespace invalidation must include dirty attrs for local loser inodes that may not appear in metadata traversal snapshots"
        );
        assert!(
            inodes.contains(&cached.ino),
            "namespace invalidation must include attr/read cache keys whose path may have been replaced by remote rename reconciliation"
        );
    });
}

#[test]
fn inode_write_keeps_partial_state_buffered_until_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-inode-write-direct");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-inode-write-direct.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let payload = b"inode-direct";

        vfs.write_with_local_sync_policy(ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        assert!(
            vfs.has_pending_local_dirty_payloads_for_inode(ino),
            "ordinary opportunistic inode writes should keep partial state buffered until the flush barrier"
        );
        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().all(|span| span.payload.is_none()),
            "ordinary opportunistic inode writes should not materialize block-backed unpublished slices before flush: {spans:?}"
        );

        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        vfs.flush(ino).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "flush should materialize block-backed unpublished slices for the buffered inode write: {spans:?}"
        );
    });
}

#[test]
fn write_fh_default_buffers_partial_state_until_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-257-write-fh-default-buffered");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-default-buffered.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"buffer-me";

        vfs.write_fh(fh, ino, 0, payload).await.unwrap();

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "default mounted handle writes should stay in writer-stack state until an explicit barrier"
        );
        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().all(|span| span.payload.is_none()),
            "default buffered handle writes should not materialize block-backed unpublished slices before flush: {spans:?}"
        );

        vfs.flush_fh(fh).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "flush_fh should materialize block-backed unpublished slices for the buffered handle write: {spans:?}"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_buffers_partial_tail_after_full_block_boundary_until_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-full-block-plus-tail");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-full-block-plus-tail.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let mut payload = vec![0x41u8; block_size];
        payload.extend_from_slice(&vec![0x42u8; block_size / 8]);

        vfs.write_fh(fh, ino, 0, &payload).await.unwrap();

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "stage-first opportunistic writes should keep the completed full block in local staging until a background or explicit flush commits metadata"
        );
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "the partial tail should remain in the writer stack after the full-block prefix is accepted"
        );
        let visible_attr = vfs.getattr_unchecked(ino).unwrap();
        assert_eq!(
            visible_attr.size,
            payload.len() as u64,
            "read-visible inode size should advance immediately even when the full-block prefix is only staged locally"
        );
        let buffered = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(buffered.as_ref(), payload.as_slice());

        vfs.flush_fh(fh).await.unwrap();

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            2,
            "flush should materialize the buffered tail block"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_routes_mounted_write_through_writer_stack() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-292-write-fh-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-292-write-fh-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"mounted-writer-stack";

        vfs.write_fh(fh, ino, 0, payload).await.unwrap();

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "mounted write callbacks should land in the new writer stack"
        );
        let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_through_writes_use_single_strict_materialization_stage() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-307-write-through-single-stage");
    let _guard = rt.enter();
    rt.block_on(async {
        let direct_attr = vfs
            .create(1, b"sfs-307-write-through-direct.txt", 0, 0, 0o644)
            .unwrap();
        let direct_fh = vfs
            .open_file(direct_attr.ino, OpenFlags::RDWR, false)
            .unwrap();

        vfs.write_fh_with_local_sync_policy(
            direct_fh,
            direct_attr.ino,
            0,
            b"direct-write-through",
            LocalSyncPolicy::WriteThrough,
        )
        .await
        .unwrap();

        let direct_snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            vfs.state
                .metrics
                .metadata_sync_barrier_calls
                .load(AtomicOrdering::Acquire),
            1,
            "direct write-through must durably sync local metadata"
        );
        assert_eq!(
            direct_snapshot.vfs_write_path.materialize_strict.calls, 1,
            "direct write-through should materialize through the strict stage"
        );
        assert_eq!(
            direct_snapshot.vfs_write_path.materialize_for_limits.calls, 0,
            "direct write-through below dirty limits should not run a redundant limits pass"
        );

        let deferred_attr = vfs
            .create(1, b"sfs-307-write-through-deferred.txt", 0, 0, 0o644)
            .unwrap();
        let deferred_fh = vfs
            .open_file(deferred_attr.ino, OpenFlags::RDWR, false)
            .unwrap();

        vfs.write_fh_with_deferred_materialize(
            deferred_fh,
            deferred_attr.ino,
            0,
            b"deferred-write-through",
            LocalSyncPolicy::WriteThrough,
        )
        .await
        .unwrap();

        let deferred_snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            vfs.state
                .metrics
                .metadata_sync_barrier_calls
                .load(AtomicOrdering::Acquire),
            2,
            "deferred write-through must also durably sync local metadata"
        );
        assert_eq!(
            deferred_snapshot.vfs_write_path.materialize_strict.calls, 2,
            "deferred write-through should also materialize through the strict stage"
        );
        assert_eq!(
            deferred_snapshot
                .vfs_write_path
                .materialize_for_limits
                .calls,
            0,
            "deferred write-through below dirty limits should not run a redundant limits pass"
        );

        vfs.release_fh(direct_fh).await.unwrap();
        vfs.release_fh(deferred_fh).await.unwrap();
    });
}

#[test]
fn write_fh_does_not_allocate_object_ids_before_materialization() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-292-write-fh-no-object-ids");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-292-write-fh-no-object-ids.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let before = meta.next_object_ids(1).unwrap()[0];

        vfs.write_fh(fh, ino, 0, b"buffered").await.unwrap();

        let after = meta.next_object_ids(1).unwrap()[0];
        assert_eq!(
            after,
            before + 1,
            "foreground write_fh should not allocate payload object ids before materialization"
        );

        vfs.flush_fh(fh).await.unwrap();
        let spans = meta.lookup_payload_slices(ino).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].1.object_id, before + 2);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_path_steady_drain_quota_uses_configured_chunk() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-257-write-fh-multi-block-burst");

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.drain_lag.steady_quota_bytes,
        snapshot.tuning.eager_flush_drain_chunk_bytes
    );
    assert!(
        snapshot.drain_lag.steady_quota_bytes > vfs.block_size as usize,
        "steady drain quota should batch multiple completed blocks per local-cache payload"
    );
}

#[test]
fn opportunistic_write_fh_uses_writer_cache_without_landing_journal() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-284-no-landing-zone");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-284-no-landing-zone.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"writer-cache-visible";

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload);

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.landing_zone.accepted_writes, 0,
            "ordinary mounted writeback must not duplicate caller bytes into the landing journal"
        );
        assert_eq!(
            snapshot.landing_zone.accepted_bytes, 0,
            "the cached-store writer is the only payload acceptance pipeline"
        );

        let recovered = vfs.recover_landing_zone_segments().unwrap();
        assert!(
            recovered.is_empty(),
            "ordinary writeback no longer creates a second recoverable payload copy before a durability boundary"
        );

        vfs.flush_fh(fh).await.unwrap();
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "strict local drain should not invent landing records"
        );
        let drained_snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(drained_snapshot.landing_zone.data_syncs, 0);
        assert_eq!(drained_snapshot.landing_zone.manifest_syncs, 0);
        assert_eq!(drained_snapshot.landing_zone.manifest_rewrites, 0);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_write_fh_foreground_boundary_stays_local_first_until_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    // Keep upload workers stopped so this observes the foreground boundary, not uploader timing.
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-547-foreground-boundary");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"sfs-547-foreground-boundary.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.reset_diagnostics_observability();

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            b"accepted-locally",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let accepted = vfs.write_path_observability_snapshot();
        assert_eq!(accepted.flush_barrier_calls, 0);
        assert_eq!(accepted.metadata_commit.calls, 0);
        assert_eq!(accepted.upload.completed_uploads, 0);

        vfs.flush_fh(fh).await.unwrap();

        let flushed = vfs.write_path_observability_snapshot();
        if flushed.flush_barrier_calls == 0 {
            assert!(
                !vfs.state.writer_stack.has_dirty_writers(ino)
                    && !vfs.has_pending_local_dirty_payloads_for_inode(ino)
                    && !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
                "flush_fh may skip the barrier only when background materialization already drained local dirty state"
            );
        } else {
            assert!(
                flushed.metadata_commit.calls > 0,
                "flush_fh should commit local metadata without turning into foreground PG publication"
            );
        }
        assert_eq!(flushed.upload.completed_uploads, 0);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn fsync_does_not_use_landing_journal_for_ordinary_writeback() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-285-no-landing-fsync");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-285-no-landing-fsync.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"writer-cache-fsync-boundary";

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.recover_landing_zone_segments().unwrap().len(),
            0,
            "ordinary writeback should not create a recoverable landing record before fsync"
        );

        vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "deferred materialization or fsync should retire landed records for this inode"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn last_handle_release_does_not_use_landing_journal_for_ordinary_writeback() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-285-no-landing-release");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-285-no-landing-release.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"writer-cache-release-boundary";

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.recover_landing_zone_segments().unwrap().len(),
            0,
            "ordinary writeback should not create a recoverable landing record before last close"
        );

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "deferred materialization or last-handle release should retire landed records for this inode"
        );
    });
}

#[test]
fn startup_replays_legacy_landed_write_after_restart() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let volume_suffix = "sfs-284-landing-restart";
    let payload = b"restart-visible-landed";

    let ino = {
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, volume_suffix);
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"sfs-284-landing-restart.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.landing_zone
                .accept(crate::landing_zone::LandingWrite {
                    ino,
                    fh,
                    offset: 0,
                    data: Bytes::copy_from_slice(payload),
                })
                .unwrap();
            ino
        })
    };

    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("reopen RedbMetaEngine"));
    let format_info = meta.get_format_info().expect("get format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let rt = Arc::new(Runtime::new().expect("runtime"));
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
    .expect("reopen vfs");
    vfs.start().expect("start recovery workers");

    rt.block_on(async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
            if read.as_ref() == payload {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "landing-zone startup recovery did not make accepted bytes visible"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "startup recovery should retire replayed landed records"
        );
    });
}

#[test]
fn drain_lag_pacing_attribution_scales_drain_lag_share_to_actual_wait() {
    assert_eq!(
        super::drain_lag_pacing_attribution_ms(12, 12, Some(4)),
        Some(4)
    );
    assert_eq!(
        super::drain_lag_pacing_attribution_ms(18, 12, Some(4)),
        Some(6)
    );
    assert_eq!(
        super::drain_lag_pacing_attribution_ms(18, 12, Some(12)),
        Some(18)
    );
    assert_eq!(super::drain_lag_pacing_attribution_ms(12, 12, None), None);
}

#[test]
fn write_path_observability_reports_writer_cache_drain_lag_without_landing_journal() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-287-drain-lag-observability");
    let _guard = rt.enter();
    rt.block_on(async {
        vfs.set_max_dirty_bytes(128 * 1024 * 1024);
        let attr = vfs
            .create(1, b"sfs-287-drain-lag-observability.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;
        let steady_quota_bytes = vfs
            .write_path_observability_snapshot()
            .drain_lag
            .steady_quota_bytes;
        let slice_count = steady_quota_bytes.div_ceil(slice_bytes) + 1;

        for index in 0..slice_count as u64 {
            let payload = vec![0x60u8.saturating_add(index as u8); slice_bytes];
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                index * slice_bytes as u64,
                &payload,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
        }

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.landing_zone.accepted_writes, 0,
            "ordinary mounted writeback should not enqueue duplicate landing-zone writes"
        );
        assert_eq!(
            snapshot.landing_zone.accepted_bytes, 0,
            "writer/cache dirty state is the only accepted-byte pipeline for this path"
        );
        assert_eq!(
            snapshot.drain_lag.current_bytes,
            vfs.total_landed_write_drain_lag_bytes(),
            "snapshot should expose the same lag signal used by soft pacing"
        );
        assert!(
            snapshot.drain_lag.current_bytes > 0,
            "writer-cache dirty bytes should be visible as drain lag before flush"
        );
        assert_eq!(
            snapshot.drain_lag.soft_pacing_events, 0,
            "opportunistic writeback must not sleep on foreground drain-lag pacing"
        );
        assert_eq!(
            snapshot.drain_lag.soft_pacing_total_ms, 0,
            "writeback callbacks should remain free of soft pacing wait time"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_aligned_full_block_callback_is_accepted_immediately() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-aligned-full-block");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-aligned-full-block.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x7Au8; block_size];

        vfs.write_fh(fh, ino, 0, &payload).await.unwrap();

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "aligned full-block callbacks should be staged immediately instead of forcing inline metadata commits"
        );

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "aligned full-block callbacks should land in the writer stack before flush"
        );
        let visible_attr = vfs.getattr_unchecked(ino).unwrap();
        assert_eq!(
            visible_attr.size,
            block_size as u64,
            "read-visible size should advance with the staged full-block accept"
        );
        let read = vfs.read(ino, 0, block_size as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            1,
            "flush should durably materialize the staged full block"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_sub_block_callbacks_batch_completed_blocks_until_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-257-write-fh-sub-block-burst");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-sub-block-burst.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let half_block = block_size / 2;

        for block_index in 0..3usize {
            let prefix = vec![0x40 + block_index as u8; half_block];
            let suffix = vec![0x50 + block_index as u8; half_block];
            let base_offset = block_index * block_size;
            vfs.write_fh(fh, ino, base_offset as u64, &prefix)
                .await
                .unwrap();
            vfs.write_fh(fh, ino, (base_offset + half_block) as u64, &suffix)
                .await
                .unwrap();
        }

        let buffered = vfs.read(ino, 0, (block_size * 3) as u32).await.unwrap();
        for block_index in 0..3usize {
            let base = block_index * block_size;
            assert_eq!(
                &buffered[base..base + half_block],
                vec![0x40 + block_index as u8; half_block].as_slice()
            );
            assert_eq!(
                &buffered[base + half_block..base + block_size],
                vec![0x50 + block_index as u8; half_block].as_slice()
            );
        }

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "sub-block sequential callbacks should keep completed blocks buffered until flush can drain a larger burst"
        );

        vfs.flush_fh(fh).await.unwrap();
        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            3,
            "flush should materialize all completed sequential logical blocks"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "flush should drain the mounted writer stack after batching sub-block callbacks"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn writer_page_size_override_reaches_mounted_writer_stack() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        writer_page_size: Some(64 * 1024),
        ..VfsConfig::default()
    };
    let (_rt, vfs, _meta) =
        make_unstarted_test_vfs_with_config(&tmp, "sfs-471-writer-page-size-config", cfg);

    assert_eq!(vfs.state.writer_stack.page_size(), 64 * 1024);
}

#[test]
fn writer_page_size_override_is_capped_to_block_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        writer_page_size: Some(8 * 1024 * 1024),
        ..VfsConfig::default()
    };
    let (_rt, vfs, _meta) =
        make_unstarted_test_vfs_with_config(&tmp, "sfs-471-writer-page-size-capped", cfg);

    assert_eq!(vfs.state.writer_stack.page_size(), vfs.block_size as usize);
}

#[test]
fn writer_page_size_override_falls_back_to_block_size_when_not_divisor() {
    let block_size = 12 * 1024 * 1024;
    let non_divisor = Some(8 * 1024 * 1024);
    let divisor = Some(4 * 1024 * 1024);

    assert_eq!(
        resolve_writer_page_size(block_size, non_divisor),
        block_size as usize,
        "non-divisor page size should fall back to the logical block size"
    );
    assert_eq!(
        resolve_writer_page_size(block_size, divisor),
        4 * 1024 * 1024,
        "divisor page size should be preserved"
    );
}

#[test]
fn small_writer_pages_preserve_sparse_read_flush_and_accounting() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        writer_page_size: Some(64 * 1024),
        ..VfsConfig::default()
    };
    let (rt, vfs, meta) = make_test_vfs_with_config(&tmp, "sfs-471-small-writer-pages-sparse", cfg);
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-471-small-writer-pages-sparse.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let page_size = 64 * 1024;
        let block_size = vfs.block_size as usize;
        let head = vec![0xA1; page_size];
        let tail = vec![0xB2; page_size];

        vfs.write_fh(fh, ino, 0, &head).await.unwrap();
        vfs.write_fh(fh, ino, (block_size - page_size) as u64, &tail)
            .await
            .unwrap();

        assert_eq!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino),
            page_size * 2,
            "sparse page writes should account written bytes without charging the zero-filled gap"
        );

        let visible = vfs.read(ino, 0, block_size as u32).await.unwrap();
        assert_eq!(&visible[..page_size], head.as_slice());
        assert!(
            visible[page_size..block_size - page_size]
                .iter()
                .all(|byte| *byte == 0),
            "sparse gap should read as zeroes before materialization"
        );
        assert_eq!(&visible[block_size - page_size..], tail.as_slice());

        vfs.flush_fh(fh).await.unwrap();

        assert_eq!(vfs.state.writer_stack.dirty_bytes_for_inode(ino), 0);
        let spans = meta
            .lookup_regular_file_slices(ino, 0, block_size as u64)
            .unwrap();
        assert_eq!(
            spans.iter().filter(|span| span.payload.is_some()).count(),
            2,
            "flush should materialize only the sparse written pages"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn small_writer_pages_materialize_completed_block_once() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig {
        writer_page_size: Some(64 * 1024),
        ..VfsConfig::default()
    };
    let (rt, vfs, meta) =
        make_test_vfs_with_config(&tmp, "sfs-471-small-writer-pages-completed", cfg);
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-471-small-writer-pages-completed.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let page_size = 64 * 1024;
        let block_size = vfs.block_size as usize;

        let mut payload = vec![0u8; page_size];
        for page_index in 0..block_size / page_size {
            payload.fill(page_index as u8);
            vfs.write_fh(fh, ino, (page_index * page_size) as u64, &payload)
                .await
                .unwrap();
        }

        assert_eq!(vfs.state.writer_stack.dirty_bytes_for_inode(ino), block_size);
        assert_eq!(
            vfs.state
                .writer_stack
                .completed_block_count_for_inode(ino)
                .unwrap(),
            1,
            "a full logical block assembled from 64KB callbacks should be eligible for completed-block materialization"
        );

        vfs.flush_fh(fh).await.unwrap();

        assert_eq!(vfs.state.writer_stack.dirty_bytes_for_inode(ino), 0);
        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            1,
            "flush should publish the completed logical block as one materialized payload slice"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn inode_write_overwrite_updates_current_slice_graph_over_published_base() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-245-inode-write-overwrite");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-inode-write-overwrite.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let baseline = b"published-baseline";
        let replacement = b"replacement-bytes";

        vfs.write(ino, 0, baseline).await.unwrap();
        vfs.flush(ino).await.unwrap();
        let (published_epoch, published_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);

        vfs.write(ino, 0, replacement).await.unwrap();
        vfs.flush(ino).await.unwrap();

        let current_blocks = meta
            .lookup_payload_slices(ino)
            .expect("load current file graph");
        assert_eq!(current_blocks.len(), 1, "overwrite test should stay single-block");
        assert_ne!(
            current_blocks[0].1.object_id,
            published_blocks[0],
            "current file graph must advance to the replacement payload slice instead of reusing the published base"
        );

        let spans = meta
            .lookup_regular_file_slices(ino, 0, replacement.len() as u64)
            .expect("load current regular-file slices");
        assert_eq!(
            spans.iter().filter_map(|span| span.payload.as_ref()).count(),
            1,
            "overwrite should resolve to one block-backed span"
        );
        assert_eq!(
            spans
                .iter()
                .filter_map(|span| span.payload.as_ref())
                .next()
                .expect("block-backed overwrite span")
                .object_id,
            current_blocks[0].1.object_id,
            "regular-file slice lookup and payload helper must agree on the current unpublished payload"
        );
    });
}

#[test]
fn ready_inode_refresh_prunes_stale_live_extents_like_remount() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "ready-refresh-prunes-live-extents");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"ready-refresh-prunes-live-extents.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let payload = vec![0x5Au8; vfs.block_size as usize];

        vfs.write(ino, 0, &payload).await.unwrap();
        vfs.flush(ino).await.unwrap();
        let (published_epoch, published_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
        vfs.cancel_pending_uploads_for_inode(ino);
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "setup should model the pre-remount process state with live extents still resident"
        );

        let published_attr = meta.get_attr(ino).unwrap();
        assert_eq!(published_attr.upload_state, MetaUploadState::Ready);
        assert_eq!(published_attr.content_epoch, published_attr.committed_content_epoch);
        assert_eq!(published_attr.size, published_attr.committed_size);

        let state = vfs.refresh_inode_incarnation_state_from_attr(ino, &published_attr);
        assert!(
            state.pending_incarnation.is_none(),
            "a fully published inode should refresh to the same idle state a remount would see"
        );
        assert!(
            vfs.live_extent_records_for_inode(ino).is_empty(),
            "fully published inode refresh should prune stale live extents so reads stop using the pending overlay path"
        );
    });
}

#[test]
fn test_release_fh_with_outcome_orphan_cleanup_does_not_report_flush_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-orphan-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-outcome-orphan.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.unlink(1, b"release-outcome-orphan.txt").unwrap();

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(
            outcome.closed_last_handle,
            "orphan cleanup should still be owned by the last-handle release"
        );
        assert!(
            !outcome.flushed_local_dirty_state,
            "orphan cleanup should not be reported as a normal release flush barrier"
        );
        assert!(
            vfs.getattr(ino).is_err(),
            "orphan cleanup should remove the inode"
        );
    });
}

#[test]
fn flush_fh_survives_unlink_of_open_append_handle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-open-append-unlink");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-open-append-unlink.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::WRONLY | OpenFlags::APPEND, false)
            .unwrap();

        vfs.unlink(1, b"flush-open-append-unlink.txt").unwrap();
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            b"payload after unlink",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        vfs.flush_fh(fh)
            .await
            .expect("handle-backed flush must survive open-unlink append writes");
    });
}

#[test]
fn flush_fh_survives_removed_inode_for_handle_with_cached_dirty_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "flush-open-readonly-removed-dirty");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-open-readonly-removed-dirty.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, attr.clone());

        meta.unlink(1, b"flush-open-readonly-removed-dirty.txt")
            .unwrap();

        vfs.flush_fh(fh)
            .await
            .expect("handle-backed flush must tolerate removed authoritative inode");
    });
}

#[test]
fn release_fh_cleans_cached_dirty_attr_after_foreign_unlink() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "release-open-foreign-unlink-cached-dirty");
    let _guard = rt.enter();
    rt.block_on(async {
        let name = b"release-open-foreign-unlink-cached-dirty.txt";
        let attr = vfs.create(ROOT_INO, name, 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.state.namespace.dirty_attrs.lock().insert(ino, attr);
        meta.unlink(ROOT_INO, name)
            .expect("simulate foreign unlink before close");
        vfs.handle_table.mark_orphan(ino);

        vfs.release_fh(fh)
            .await
            .expect("last-handle release should clean a foreign-unlinked dirty inode");

        assert!(!vfs.inode_has_local_dirty_state(ino));
    });
}

#[test]
fn release_fh_preserves_dirty_state_when_metadata_is_missing_without_orphan_marker() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "release-missing-attr-keeps-upload-state");
    let _guard = rt.enter();
    rt.block_on(async {
        let name = b"release-missing-attr-keeps-upload-state.txt";
        let attr = vfs.create(ROOT_INO, name, 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.state.namespace.dirty_attrs.lock().insert(ino, attr);
        meta.unlink(ROOT_INO, name)
            .expect("simulate missing metadata without an invalidation tombstone");

        vfs.release_fh(fh)
            .await
            .expect_err("unmarked missing metadata must not trigger destructive orphan cleanup");

        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "a transient metadata miss must preserve local data queued for upload"
        );
    });
}

#[test]
fn deferred_release_cleans_dirty_state_when_orphan_marker_arrives_after_action_selection() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-release-late-orphan-marker");
    let _guard = rt.enter();
    rt.block_on(async {
        let name = b"deferred-release-late-orphan-marker.txt";
        let attr = vfs.create(ROOT_INO, name, 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.state.namespace.dirty_attrs.lock().insert(ino, attr);

        let read_session = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let outcome = vfs.release_fh_with_deferred_last_handle_barrier(fh, rt.handle());
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if vfs.file_session_stats().queued_ops >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("deferred release should wait behind the file-session gate");

        meta.unlink(ROOT_INO, name)
            .expect("simulate foreign unlink after deferred action selection");
        vfs.handle_table.mark_orphan(ino);
        drop(read_session);

        vfs.wait_for_deferred_release_inflight(ino).await;

        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "a late orphan marker must still permit deferred last-handle cleanup"
        );
    });
}

#[test]
fn test_open_unlink_handle_accepts_followup_write_until_release() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "open-unlink-followup-write");
    let _guard = rt.enter();
    rt.block_on(async {
        let name = b"open-unlink-followup-write.txt";
        let attr = vfs.create(1, name, 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            b"initial",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        vfs.flush_fh(fh).await.unwrap();
        meta.unlink(1, name)
            .expect("simulate foreign unlink while handle stays open");

        let written = vfs
            .write_fh_with_deferred_materialize(
                fh,
                ino,
                7,
                b"-after-unlink",
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .expect("write through open unlinked handle");

        assert_eq!(written, 13);
        vfs.flush_fh(fh)
            .await
            .expect("flush through open unlinked handle");
        vfs.handle_table.mark_orphan(ino);
        vfs.release_fh(fh).await.unwrap();
        assert!(vfs.getattr(ino).is_err());
    });
}

#[test]
fn kernel_invalidation_skips_unresolved_active_open_overlay() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "active-open-unlinked-kernel-inval");
    let _guard = rt.enter();

    rt.block_on(async {
        let name = b"active-open-unlinked-kernel-inval.txt";
        let attr = vfs.create(ROOT_INO, name, 0, 0, 0o644).expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        meta.unlink(ROOT_INO, name)
            .expect("simulate foreign unlink while handle stays open");
        assert!(
            vfs.best_effort_path_for_inode(attr.ino).is_none(),
            "test setup needs the inode to be unresolvable like WinFSP's notify filter"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());
        vfs.invalidate_kernel_inodes([attr.ino]);

        assert!(
            invalidator.snapshot().is_empty(),
            "kernel invalidation must not evict an unlinked inode that still has an active writable handle"
        );
        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn kernel_invalidation_skips_active_open_overlay_before_foreign_unlink_is_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "active-open-pending-unlink-kernel-inval");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(
                ROOT_INO,
                b"active-open-pending-unlink-kernel-inval.txt",
                0,
                0,
                0o644,
            )
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::APPEND, false)
            .expect("open append handle");
        assert!(
            vfs.best_effort_path_for_inode(attr.ino).is_some(),
            "the invalidation must race ahead of the foreign unlink becoming locally visible"
        );

        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());
        vfs.invalidate_kernel_inodes([attr.ino]);

        assert!(
            invalidator.snapshot().is_empty(),
            "kernel invalidation must not evict a live append handle while unlink visibility catches up"
        );
        vfs.release_fh(fh).await.expect("release handle");
        assert_eq!(
            invalidator.snapshot(),
            vec![attr.ino],
            "a skipped kernel invalidation must be replayed after the last overlay handle closes"
        );
    });
}

#[test]
fn test_release_fh_orphan_cleanup_clears_live_extents() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-orphan-live-extents");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-orphan-live-extents.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush(ino).await.unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "flush should materialize live extents before orphan cleanup"
        );

        vfs.unlink(1, b"release-orphan-live-extents.txt").unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "open-unlink should keep live extents available until the last handle closes"
        );

        vfs.release_fh(fh).await.unwrap();
        assert!(
            vfs.live_extent_records_for_inode(ino).is_empty(),
            "orphan cleanup should clear live extent identities on final close"
        );
    });
}

#[test]
fn test_release_fh_orphan_cleanup_waits_for_final_handle_before_clearing_live_extents() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-orphan-final-handle-live-extents");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-orphan-final-handle.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush(ino).await.unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "flush should materialize live extents before orphan cleanup"
        );

        vfs.unlink(1, b"release-orphan-final-handle.txt").unwrap();
        vfs.release_fh(fh1).await.unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "non-final close must not clear orphan live extents"
        );

        vfs.release_fh(fh2).await.unwrap();
        assert!(
            vfs.live_extent_records_for_inode(ino).is_empty(),
            "final orphan close must clear live extents"
        );
    });
}

#[test]
fn test_unlink_immediate_cleanup_clears_pending_inode_dirty_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "unlink-immediate-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"unlink-immediate-cleanup.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        vfs.write(ino, 0, b"payload").await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should remain pending until unlink removes the inode"
        );

        vfs.unlink(1, b"unlink-immediate-cleanup.txt").unwrap();

        assert!(
            vfs.getattr(ino).is_err(),
            "unlink should remove the inode immediately"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "immediate unlink should clear local dirty state for the deleted inode"
        );
    });
}

#[test]
fn unlink_immediate_cleanup_removes_pending_upload_payload_artifacts() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "unlink-pending-upload-cleanup");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"unlink-pending-upload-cleanup.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0xA5; vfs.block_size as usize];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let object_id = meta
            .lookup_payload_slices(attr.ino)
            .expect("load flushed payload slices")
            .into_iter()
            .next()
            .expect("flushed file should have one payload object")
            .1
            .object_id;

        assert!(
            vfs.local_payload_backend().contains(object_id),
            "setup should leave the flushed payload in the local backend"
        );
        assert!(
            vfs.cached_store().has_staged_upload(object_id),
            "flush should keep an upload-stage hardlink for retryable pending uploads"
        );
        assert!(
            vfs.cached_store().is_pinned(object_id),
            "pending upload should pin the local payload before unlink"
        );
        assert!(
            !vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "setup should queue a pending upload before unlink"
        );

        vfs.unlink(1, b"unlink-pending-upload-cleanup.bin").unwrap();

        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "unlink should cancel queued uploads for the deleted inode"
        );
        assert!(
            vfs.state.upload.deleted_inodes.lock().contains(&attr.ino),
            "deleted inode marker must remain after metadata/cache cleanup so uploader channel backlog cannot start later uploads"
        );
        assert!(
            !vfs.cached_store().is_pinned(object_id),
            "unlink should unpin canceled upload payloads so they can be removed immediately"
        );
        assert!(
            !vfs.cached_store().has_staged_upload(object_id),
            "unlink should remove upload-stage hardlinks for deleted payloads"
        );
        assert!(
            vfs.cached_store().get_hot_upload(object_id).is_none(),
            "unlink should clear any hot upload copy for deleted payloads"
        );
        assert!(
            !vfs.local_payload_backend().contains(object_id),
            "unlink should remove deleted file payloads from the local backend before returning"
        );
        assert!(
            !vfs.state
                .upload
                .pending_evictions
                .lock()
                .contains(&object_id),
            "immediate cleanup should not leave a deferred eviction for the canceled upload"
        );
    });
}

#[test]
fn foreign_delete_lazy_evict_removes_local_payload_artifacts() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "foreign-delete-lazy-evict");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"foreign-delete-lazy-evict.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0x5C; vfs.block_size as usize];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let object_id = meta
            .lookup_payload_slices(attr.ino)
            .expect("load flushed payload slices")
            .into_iter()
            .next()
            .expect("flushed file should have one payload object")
            .1
            .object_id;

        assert!(vfs.local_payload_backend().contains(object_id));
        assert!(vfs.cached_store().has_staged_upload(object_id));
        assert!(vfs.cached_store().is_pinned(object_id));

        vfs.lazy_evict_removed_metadata_payloads(
            std::collections::HashMap::from([(attr.ino, vec![object_id])]),
            &std::collections::HashSet::new(),
        )
        .await;

        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "lazy eviction should cancel queued uploads for a remotely deleted inode"
        );
        assert!(
            !vfs.cached_store().is_pinned(object_id),
            "lazy eviction should unpin remotely deleted payloads"
        );
        assert!(
            !vfs.cached_store().has_staged_upload(object_id),
            "lazy eviction should remove upload-stage hardlinks"
        );
        assert!(
            !vfs.local_payload_backend().contains(object_id),
            "lazy eviction should remove local cached payload bytes"
        );
    });
}

#[test]
fn retained_inode_payload_evict_does_not_mark_inode_deleted() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "retained-inode-payload-evict");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"retained-inode-payload-evict.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0x7A; vfs.block_size as usize];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let object_id = vfs
            .local_payload_backend()
            .object_ids()
            .into_iter()
            .next()
            .expect("flushed file should seed one local payload object");

        vfs.lazy_evict_removed_metadata_payloads(
            std::collections::HashMap::from([(attr.ino, vec![object_id])]),
            &std::collections::HashSet::from([ROOT_INO, attr.ino]),
        )
        .await;

        assert!(
            !vfs.state.upload.deleted_inodes.lock().contains(&attr.ino),
            "removed payloads for a retained inode must not poison future uploads with a delete marker"
        );
        assert!(
            vfs.local_payload_backend().contains(object_id),
            "retained-inode payload eviction must not remove bytes while upload is still pending"
        );
    });
}

#[test]
fn metadata_payload_diff_detects_removed_objects_for_retained_inode() {
    let previous = std::collections::HashMap::from([(42, vec![1001, 1002]), (77, vec![2001])]);
    let current = std::collections::HashMap::from([(42, vec![1002, 1003])]);

    let removed = StudioVfs::removed_metadata_payloads(&previous, &current);

    assert_eq!(
        removed.get(&42).cloned(),
        Some(vec![1001]),
        "payload diff should evict objects removed by a same-inode remote update"
    );
    assert_eq!(
        removed.get(&77).cloned(),
        Some(vec![2001]),
        "payload diff should still evict all objects when the inode disappears"
    );
    assert!(
        !removed
            .get(&42)
            .is_some_and(|object_ids| object_ids.contains(&1002)),
        "payload diff must retain objects still referenced by the current snapshot"
    );
}

#[test]
fn metadata_payload_diff_marks_inodes_when_graph_object_set_changes() {
    let previous = std::collections::HashMap::from([
        (42, vec![1001, 1002]),
        (77, vec![2001]),
        (88, vec![3001]),
    ]);
    let current = std::collections::HashMap::from([
        (42, vec![1001, 1002, 1003]),
        (77, vec![2001]),
        (99, vec![4001]),
    ]);

    let changed = StudioVfs::changed_metadata_payload_inodes(&previous, &current);

    assert_eq!(
        changed,
        std::collections::HashSet::from([42, 88, 99]),
        "metadata invalidation must drop kernel data for added, removed, or replaced payload graphs"
    );
}

#[test]
fn test_rename_over_existing_target_discards_pending_local_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "rename-displaced-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let source = vfs.create(1, b"rename-source.txt", 0, 0, 0o644).unwrap();
        let displaced = vfs.create(1, b"rename-target.txt", 0, 0, 0o644).unwrap();

        vfs.write(source.ino, 0, b"source").await.unwrap();
        vfs.write(displaced.ino, 0, b"target").await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(displaced.ino),
            "a displaced overwrite target may still carry pending local state before rename"
        );

        vfs.rename(1, b"rename-source.txt", 1, b"rename-target.txt")
            .unwrap();

        assert!(
            vfs.getattr(displaced.ino).is_err(),
            "displaced inode should be removed after rename overwrite"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(displaced.ino),
            "rename overwrite should clear local dirty state for the displaced inode"
        );
    });
}

#[test]
fn rename_over_existing_target_invalidates_displaced_inode_attr_cache() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "rename-displaced-cache-invalidation");
    let _guard = rt.enter();
    rt.block_on(async {
        let source = vfs.create(1, b"rename-source.txt", 0, 0, 0o644).unwrap();
        let displaced = vfs.create(1, b"rename-target.txt", 0, 0, 0o644).unwrap();

        vfs.write(source.ino, 0, b"source").await.unwrap();
        vfs.write(displaced.ino, 0, b"target").await.unwrap();
        let cached_displaced = meta
            .get_attr(displaced.ino)
            .expect("load displaced attr before rename");
        vfs.state.namespace.attr_read_cache.lock().put(
            displaced.ino,
            cached_displaced,
            ATTR_CACHE_TTL,
            Instant::now(),
        );
        assert!(
            vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(displaced.ino),
            "setup must prime displaced inode attr cache"
        );

        vfs.rename(1, b"rename-source.txt", 1, b"rename-target.txt")
            .unwrap();

        assert!(
            !vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(displaced.ino),
            "rename overwrite must drop stale cache for the deleted target inode"
        );
    });
}

#[test]
fn orphan_cleanup_clears_dirty_writer_stack_state_for_deleted_inode() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "orphan-writer-stack-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"orphan-writer-stack-cleanup.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, b"payload", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "orphan cleanup test requires buffered writer-stack state before unlink"
        );

        vfs.unlink(1, b"orphan-writer-stack-cleanup.txt").unwrap();
        vfs.release_fh(fh).await.unwrap();

        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "final orphan cleanup must clear stale writer-stack state for the deleted inode"
        );
        assert_eq!(
            vfs.state.writer_stack.total_dirty_bytes(),
            0,
            "deleted inode must not retain dirty writer-stack bytes after last-handle cleanup"
        );
    });
}

#[test]
fn test_flush_fh_with_outcome_materializes_pending_inode_dirty_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-handle-outcome");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-handle-outcome.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let clean = vfs.flush_fh_with_outcome(fh).await.unwrap();
        assert!(
            !clean.flushed_local_dirty_state,
            "clean handle flush should report a clean skip"
        );

        let payload = b"payload";
        vfs.write(ino, 0, payload).await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should stay buffered until the explicit flush barrier"
        );

        let after_write = vfs.flush_fh_with_outcome(fh).await.unwrap();
        assert!(
            after_write.flushed_local_dirty_state,
            "flush_fh should report that it materialized the pending inode dirty state"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "flush_fh_with_outcome should preserve the write-committed file size"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_flush_fh_clean_skip_waits_for_deferred_release_but_not_file_session_gate() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "flush-clean-waits-release");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-clean-waits-release.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "test expects a clean inode before flush"
        );

        let _read_session = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let (entry, guard) = vfs
            .state
            .deferred_release_inflight
            .try_register(ino)
            .expect("test should reserve the deferred release gate");

        let flushing_vfs = Arc::clone(&vfs);
        let flush = tokio::spawn(async move { flushing_vfs.flush_fh_with_outcome(fh).await });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !flush.is_finished(),
            "clean handle flush must still wait for an in-flight deferred release barrier"
        );

        drop(entry);
        drop(guard);
        let outcome = tokio::time::timeout(Duration::from_millis(100), flush)
            .await
            .expect("clean flush should not wait behind the file-session gate")
            .expect("flush task should not panic")
            .expect("flush should complete after deferred release barrier drains");
        assert!(
            !outcome.flushed_local_dirty_state,
            "clean handle flush should report a clean skip"
        );
        drop(_read_session);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_flush_with_outcome_materializes_pending_inode_dirty_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-inode-outcome");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-inode-outcome.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let clean = vfs.flush_with_outcome(ino).await.unwrap();
        assert!(
            !clean.flushed_local_dirty_state,
            "clean inode flush should report a clean skip"
        );

        let payload = b"payload";
        vfs.write(ino, 0, payload).await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should stay buffered until the explicit inode flush barrier"
        );

        let after_write = vfs.flush_with_outcome(ino).await.unwrap();
        assert!(
            after_write.flushed_local_dirty_state,
            "flush_with_outcome should report that it materialized the pending inode dirty state"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "flush_with_outcome should preserve the write-committed file size"
        );
    });
}

#[test]
fn test_fsync_with_outcome_materializes_pending_inode_dirty_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "fsync-outcome-clean-data");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"fsync-outcome-clean-data.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let clean = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            !clean.flushed_file_data,
            "clean fsync should not report a file-data barrier"
        );
        assert!(
            !clean.persisted_dirty_attrs,
            "clean fsync should not report metadata persistence work"
        );

        let payload = b"payload";
        vfs.write(ino, 0, payload).await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should remain pending until fsync forces the local barrier"
        );

        let after_write = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            after_write.flushed_file_data,
            "fsync should report that it flushed the pending file data"
        );
        assert!(
            !after_write.persisted_dirty_attrs,
            "fsync should not report extra dirty-attr work once the file-data barrier already committed the pending state"
        );
    });
}

#[test]
fn winfsp_flush_is_volatile_until_fsync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "winfsp-volatile-flush");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"winfsp-volatile-flush.txt", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file");

        vfs.write(ino, 0, b"payload").await.expect("write payload");
        assert!(vfs.inode_has_local_dirty_state(ino));

        let outcome = vfs
            .flush_volatile_writeback_with_outcome(ino, fh)
            .await
            .expect("volatile flush admission");
        assert!(outcome.flushed_local_dirty_state);
        assert!(
            vfs.inode_has_local_dirty_state(ino),
            "ordinary flush must leave the accepted state for the background worker"
        );

        vfs.fsync_with_outcome(ino, false)
            .await
            .expect("fsync must establish the strict local barrier");
        assert!(!vfs.inode_has_local_dirty_state(ino));
        vfs.release_fh(fh).await.expect("release file handle");
    });
}

#[test]
fn volatile_flush_reports_a_previous_metadata_commit_failure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "volatile-flush-degraded");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"volatile-flush-degraded.txt", 0, 0, 0o644)
            .expect("create file");
        vfs.state
            .local_dirty
            .mark_overlay_metadata_error("simulated redb outage".to_string());

        let error = vfs
            .flush_volatile_writeback_with_outcome(attr.ino, 0)
            .await
            .expect_err("degraded local metadata must fail an ordinary flush");
        assert!(error.to_string().contains("simulated redb outage"));
    });
}

#[test]
fn strict_metadata_barrier_surfaces_previous_volatile_overlay_error() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "volatile-flush-recovery");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"volatile-flush-recovery.txt", 0, 0, 0o644)
            .expect("create file");
        vfs.state
            .local_dirty
            .mark_overlay_metadata_error("simulated redb outage".to_string());

        let error = vfs
            .fsync_with_outcome(attr.ino, false)
            .await
            .expect_err("strict metadata barrier must surface the retained failure");
        assert!(error.to_string().contains("simulated redb outage"));
    });
}

#[test]
fn test_fsync_with_outcome_reports_writer_stack_file_data_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "fsync-outcome-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"fsync-outcome-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, b"payload", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        let after_write = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            after_write.flushed_file_data,
            "fsync should report file-data work when it materializes dirty writer-stack state"
        );

        let clean = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            !clean.flushed_file_data,
            "once the writer-stack state is drained, the next fsync should report a clean data path"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_flush_fh_retries_inline_dirty_attr_persist_after_transient_failure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "flush-fh-inline-dirty-attr-retry");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-fh-inline-dirty-attr-retry.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let mut dirty_attr = meta.get_attr(ino).expect("load attr");
        dirty_attr.mtime = InodeAttr::now_nanos();
        dirty_attr.ctime = dirty_attr.mtime;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, dirty_attr.clone());

        vfs.arm_fail_next_inline_dirty_attr_persist_for_test();
        let err = vfs
            .flush_fh_with_outcome(fh)
            .await
            .expect_err("simulated inline dirty attr persist failure should fail flush_fh");
        assert!(
            err.to_string()
                .contains("simulated inline dirty attr persist failure"),
            "expected the injected inline dirty attr persist failure to surface"
        );
        assert!(
            vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "failed inline dirty attr persist must leave dirty attrs queued for retry"
        );

        vfs.flush_fh_with_outcome(fh)
            .await
            .expect("retry flush_fh after transient dirty attr persist failure");
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "successful retry should clear the queued dirty attrs"
        );

        let persisted = meta.get_attr(ino).expect("load persisted attr");
        assert_eq!(
            persisted.mtime, dirty_attr.mtime,
            "retry flush should eventually persist the queued dirty attr update"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn deferred_namespace_create_is_visible_before_background_commit() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-namespace-create");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-namespace-create.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let ino = attr.ino;

        let visible = vfs
            .lookup(1, b"deferred-namespace-create.txt")
            .expect("admitted create should be visible immediately");
        assert_eq!(visible.ino, ino);

        vfs.wait_for_deferred_namespace_metadata(ino)
            .await
            .expect("background create commit should finish");

        let persisted = meta
            .lookup(1, b"deferred-namespace-create.txt")
            .expect("load persisted dentry");
        assert_eq!(persisted.ino, ino);
    });
}

#[test]
fn flush_waits_for_deferred_namespace_create() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-create-durability-barrier");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-create-flush-barrier.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let flush_fh = vfs
            .open_created_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .expect("open deferred flush file");
        vfs.flush_fh(flush_fh)
            .await
            .expect("flush should wait for deferred create");
        let looked_up = meta
            .lookup(1, b"deferred-create-flush-barrier.txt")
            .expect("flushed create should be persisted");
        assert_eq!(looked_up.ino, attr.ino);
        vfs.release_fh(flush_fh).await.unwrap();
    });
}

#[test]
fn fsync_waits_for_deferred_namespace_create() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-create-fsync-barrier");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-create-fsync-barrier.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        vfs.fsync(attr.ino, false)
            .await
            .expect("fsync should wait for deferred create");
        let looked_up = meta
            .lookup(1, b"deferred-create-fsync-barrier.txt")
            .expect("fsynced create should be persisted");
        assert_eq!(looked_up.ino, attr.ino);
    });
}

#[test]
fn deferred_namespace_create_commit_does_not_clear_larger_dirty_attr() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-create-keeps-written-dirty-attr");
    let _guard = rt.enter();
    rt.block_on(async {
        let committed_create_attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-create-keeps-written-dirty-attr.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let ino = committed_create_attr.ino;

        let mut written_attr = committed_create_attr.clone();
        written_attr.size = 37;
        written_attr.touch_mtime();
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, written_attr);

        vfs.clear_matching_dirty_attr_if_committed(ino, &committed_create_attr);

        let retained = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("written dirty attr must survive zero-size create commit");
        assert_eq!(retained.size, 37);

        vfs.refresh_inode_incarnation_state_from_attr(ino, &committed_create_attr);

        let retained = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("written dirty attr must survive zero-size authoritative refresh");
        assert_eq!(retained.size, 37);
    });
}

#[test]
fn deferred_namespace_first_write_marks_dirty_attr_uploading() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-create-first-write-uploading");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-create-first-write-uploading.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let ino = attr.ino;
        let fh = vfs
            .open_created_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .expect("open deferred-created file");

        vfs.write_fh(fh, ino, 0, b"payload after deferred create")
            .await
            .expect("write deferred-created file");

        let dirty = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("first write should leave a dirty attr");
        assert_eq!(dirty.upload_state, MetaUploadState::Uploading);
        assert!(dirty.content_epoch > attr.content_epoch);
        assert!(dirty.size > 0);

        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn deferred_namespace_written_file_reads_back_after_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-create-written-readback");
    let _guard = rt.enter();
    rt.block_on(async {
        let payload = b"payload after deferred create";
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-create-written-readback.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let ino = attr.ino;
        let fh = vfs
            .open_created_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .expect("open deferred-created file");

        vfs.write_fh(fh, ino, 0, payload)
            .await
            .expect("write deferred-created file");
        vfs.flush_fh(fh).await.expect("flush deferred-created file");
        vfs.wait_for_deferred_namespace_metadata(ino)
            .await
            .expect("background create commit should finish");

        let read = vfs
            .read(ino, 0, payload.len() as u32)
            .await
            .expect("read deferred-created file");
        assert_eq!(read.as_ref(), payload);

        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn deferred_namespace_written_file_commits_locally_without_explicit_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-create-written-background-local");
    let _guard = rt.enter();
    rt.block_on(async {
        let payload = b"background local payload after deferred create";
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-create-written-background-local.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let ino = attr.ino;
        let fh = vfs
            .open_created_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .expect("open deferred-created file");

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .expect("write deferred-created file");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let persisted_size = meta.get_attr(ino).map(|attr| attr.size).unwrap_or(0);
                if persisted_size == payload.len() as u64
                    && !vfs.pending_uploads_for_inode(ino).is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background materialization should commit local metadata before close");

        let read = vfs
            .read(ino, 0, payload.len() as u32)
            .await
            .expect("read background-committed deferred-created file");
        assert_eq!(read.as_ref(), payload);

        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn deferred_attr_only_commit_does_not_revert_written_size() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-attr-only-keeps-written-size");
    let _guard = rt.enter();
    rt.block_on(async {
        let payload = b"linux write then utimens";
        let attr = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"deferred-attr-only-keeps-written-size.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();
        let ino = attr.ino;
        let fh = vfs
            .open_created_file_with_known_attr(&attr, OpenFlags::RDWR, false)
            .expect("open deferred-created file");

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .expect("write deferred-created file");
        let updated_attr = vfs
            .setattr_attr_only_deferred_metadata(
                ino,
                SetattrArgs {
                    mtime: TimeSpec::Set(attr.mtime.saturating_add(1)),
                    ..SetattrArgs::default()
                },
                rt.handle(),
            )
            .expect("defer attr-only setattr");
        assert_eq!(
            updated_attr.size,
            payload.len() as u64,
            "attr-only setattr should preserve the dirty write size immediately"
        );
        assert_eq!(
            vfs.getattr(ino).expect("get dirty attr after setattr").size,
            payload.len() as u64,
            "getattr should report local dirty size before background commit"
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let persisted_size = meta.get_attr(ino).map(|attr| attr.size).unwrap_or(0);
                if persisted_size == payload.len() as u64 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("attr-only commit must not revert background-written size");

        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn deferred_namespace_wait_keeps_blocking_until_all_inode_ops_complete() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-wait-refcount");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"deferred-namespace-wait-refcount.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        {
            let mut inner = vfs.state.namespace.deferred.inner.lock();
            inner.pending_inodes.insert(ino, 2);
        }

        let waiter_vfs = vfs.clone();
        let waiter =
            tokio::spawn(async move { waiter_vfs.wait_for_deferred_namespace_metadata(ino).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !waiter.is_finished(),
            "wait should block while multiple deferred ops remain for the inode"
        );

        {
            let mut inner = vfs.state.namespace.deferred.inner.lock();
            inner.pending_inodes.insert(ino, 1);
        }
        vfs.state.namespace.deferred.notify.notify_waiters();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !waiter.is_finished(),
            "wait must not unblock after only one deferred op completes"
        );

        vfs.state
            .namespace
            .deferred
            .inner
            .lock()
            .pending_inodes
            .remove(&ino);
        vfs.state.namespace.deferred.notify.notify_waiters();

        waiter
            .await
            .expect("wait task should join")
            .expect("wait should finish after all deferred ops complete");
    });
}

#[test]
fn deferred_namespace_drain_abandons_permanent_failure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-abandons-failure");
    let _guard = rt.enter();
    rt.block_on(async {
        let parent = 9_000_001;
        let attr = InodeAttr::new_file(9_000_002, parent, 0, 0, 0o644);
        vfs.enqueue_deferred_namespace_op(
            Some(super::namespace::DeferredNamespaceKey::new(
                parent,
                b"never.txt",
            )),
            attr.clone(),
            super::namespace::DeferredNamespaceOp::CreateFile {
                attr: attr.clone(),
                parent,
                name: b"never.txt".to_vec(),
                uid: 0,
                gid: 0,
                mode: 0o644,
            },
        )
        .expect("admit permanently failing operation");
        vfs.schedule_deferred_namespace_metadata_drain(rt.handle());

        vfs.fsync(attr.ino, false)
            .await
            .expect_err("fsync should report failed deferred namespace persistence");

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if !vfs
                    .state
                    .namespace
                    .deferred
                    .inner
                    .lock()
                    .pending_inodes
                    .contains_key(&attr.ino)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("permanent failure should eventually clear pending accounting");

        let inner = vfs.state.namespace.deferred.inner.lock();
        assert!(!inner.pending_inodes.contains_key(&attr.ino));
        assert!(!inner.attrs.contains_key(&attr.ino));
        assert!(
            inner.queue.is_empty(),
            "permanent failure should not block later namespace ops"
        );
        drop(inner);

        let admission_error = vfs
            .create_with_deferred_metadata_admission(
                ROOT_INO,
                b"rejected-after-degraded-state.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .expect_err("permanent deferred persistence failure should degrade shared admission");
        assert!(matches!(
            admission_error,
            VfsError::Meta(MetaError::AccessDenied {
                actual: AccessLevel::ReadOnly,
                ..
            })
        ));
    });
}

#[test]
fn fsync_handle_uses_one_metadata_sync_barrier_after_dirty_flush() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "fsync-handle-single-barrier");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(ROOT_INO, b"fsync-single-barrier.txt", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh_with_deferred_materialize(
            fh,
            attr.ino,
            0,
            b"dirty",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        vfs.fsync_fh(fh).await.unwrap();

        assert_eq!(
            vfs.state
                .metrics
                .metadata_sync_barrier_calls
                .load(AtomicOrdering::Acquire),
            1
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn transient_deferred_namespace_backlog_does_not_fail_release_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-transient-backlog");

    vfs.state
        .namespace
        .deferred
        .draining
        .store(true, AtomicOrdering::Release);
    assert!(
        vfs.deferred_namespace_shutdown_error().is_none(),
        "a concurrent deferred drain is transient during normal release"
    );

    vfs.state
        .shutdown
        .shutdown_requested
        .store(true, AtomicOrdering::Release);
    assert!(
        vfs.deferred_namespace_shutdown_error().is_some(),
        "an unfinished deferred drain must still fail shutdown"
    );
}

#[test]
fn deferred_namespace_dir_commit_clears_admitted_dirty_attr() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-dir-clears-dirty-attr");
    let _guard = rt.enter();
    rt.block_on(async {
        let dir = vfs
            .mkdir_with_deferred_metadata_admission(
                1,
                b"deferred-dir-clears-dirty-attr",
                0,
                0,
                0o755,
                rt.handle(),
            )
            .unwrap();

        vfs.wait_for_deferred_namespace_metadata(dir.ino)
            .await
            .expect("background mkdir commit should finish");

        assert!(
            !vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .contains_key(&dir.ino),
            "committed deferred mkdir should not leave directory attrs dirty until shutdown"
        );
    });
}

#[test]
fn deferred_create_respects_dentry_cache_admission() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-create-dentry-cache-admission");
    let _guard = rt.enter();
    rt.block_on(async {
        assert!(vfs.lookup(1, b"negative-cache-create.txt").is_err());
        vfs.create_with_deferred_metadata_admission(
            1,
            b"negative-cache-create.txt",
            0,
            0,
            0o644,
            rt.handle(),
        )
        .expect("negative dentry cache should allow deferred create admission");

        let existing = vfs
            .create(1, b"positive-cache-create.txt", 0, 0, 0o644)
            .expect("create existing file");
        let looked_up = vfs
            .lookup(1, b"positive-cache-create.txt")
            .expect("prime positive dentry cache");
        assert_eq!(looked_up.ino, existing.ino);

        let error = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"positive-cache-create.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .expect_err("positive dentry cache should reject duplicate create");
        assert!(matches!(
            error,
            VfsError::Meta(MetaError::EntryExists { .. })
        ));
    });
}

#[test]
fn deferred_lookup_under_locally_created_dir_uses_negative_cache() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "deferred-local-dir-negative-lookup");
    let dir = vfs
        .mkdir_with_deferred_metadata_admission(1, b"locally-created-dir", 0, 0, 0o755, rt.handle())
        .expect("admit deferred dir");

    assert!(vfs.lookup(dir.ino, b"child.txt").is_err());
    assert_eq!(
        vfs.dentry_cache_lookup(dir.ino, b"child.txt"),
        Some(DentryCacheValue::Negative)
    );

    vfs.create_with_deferred_metadata_admission(dir.ino, b"child.txt", 0, 0, 0o644, rt.handle())
        .expect("negative cache under local dir should allow create");
}

#[test]
fn deferred_namespace_create_under_deferred_dir_is_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-namespace-nested-create");
    let _guard = rt.enter();
    rt.block_on(async {
        let dir = vfs
            .mkdir_with_deferred_metadata_admission(
                1,
                b"deferred-namespace-dir",
                0,
                0,
                0o755,
                rt.handle(),
            )
            .unwrap();
        let file = vfs
            .create_with_deferred_metadata_admission(
                dir.ino,
                b"child.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .unwrap();

        let visible = vfs
            .lookup(dir.ino, b"child.txt")
            .expect("admitted nested create should be visible immediately");
        assert_eq!(visible.ino, file.ino);

        vfs.wait_for_deferred_namespace_metadata(dir.ino)
            .await
            .expect("background mkdir commit should finish");
        vfs.wait_for_deferred_namespace_metadata(file.ino)
            .await
            .expect("background child create commit should finish");

        let persisted = meta
            .lookup(dir.ino, b"child.txt")
            .expect("load persisted child dentry");
        assert_eq!(persisted.ino, file.ino);
    });
}

#[test]
fn deferred_namespace_drain_commits_queued_operations_in_one_batch() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-group-commit");
    let before = vfs.write_path_observability_snapshot();
    let _guard = rt.enter();
    rt.block_on(async {
        let first = InodeAttr::new_file(9_000_011, ROOT_INO, 0, 0, 0o644);
        let second = InodeAttr::new_file(9_000_012, ROOT_INO, 0, 0, 0o644);

        for (attr, name) in [
            (&first, b"first.txt".as_slice()),
            (&second, b"second.txt".as_slice()),
        ] {
            vfs.enqueue_deferred_namespace_op(
                Some(super::namespace::DeferredNamespaceKey::new(ROOT_INO, name)),
                attr.clone(),
                super::namespace::DeferredNamespaceOp::CreateFile {
                    attr: attr.clone(),
                    parent: ROOT_INO,
                    name: name.to_vec(),
                    uid: 0,
                    gid: 0,
                    mode: 0o644,
                },
            )
            .expect("admit queued operation");
        }
        vfs.schedule_deferred_namespace_metadata_drain(rt.handle());
        vfs.wait_for_deferred_namespace_metadata(first.ino)
            .await
            .expect("first queued operation should commit");
        vfs.wait_for_deferred_namespace_metadata(second.ino)
            .await
            .expect("second queued operation should commit");
    });

    let after = vfs.write_path_observability_snapshot();
    assert_eq!(
        after.metadata_commit.calls,
        before.metadata_commit.calls + 1
    );
    assert_eq!(
        after.metadata_engine.commit.calls,
        before.metadata_engine.commit.calls + 1
    );
}

#[test]
fn deferred_namespace_barrier_takes_over_after_drain_stops_with_queued_work() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-namespace-barrier-takeover");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = InodeAttr::new_file(9_000_031, ROOT_INO, 0, 0, 0o644);
        vfs.enqueue_deferred_namespace_op(
            Some(super::namespace::DeferredNamespaceKey::new(
                ROOT_INO,
                b"takeover.txt",
            )),
            attr.clone(),
            super::namespace::DeferredNamespaceOp::CreateFile {
                attr: attr.clone(),
                parent: ROOT_INO,
                name: b"takeover.txt".to_vec(),
                uid: 0,
                gid: 0,
                mode: 0o644,
            },
        )
        .expect("admit queued operation");
        vfs.state
            .namespace
            .deferred
            .draining
            .store(true, AtomicOrdering::Release);

        let barrier_vfs = Arc::clone(&vfs);
        let barrier = tokio::spawn(async move {
            barrier_vfs
                .drain_deferred_namespace_metadata_for_barrier()
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        vfs.state
            .namespace
            .deferred
            .draining
            .store(false, AtomicOrdering::Release);
        vfs.state.namespace.deferred.notify.notify_waiters();

        tokio::time::timeout(Duration::from_millis(250), barrier)
            .await
            .expect("barrier should take over queued work after the prior drain stops")
            .expect("barrier task should not panic")
            .expect("barrier should persist queued work");
        assert_eq!(
            meta.lookup(ROOT_INO, b"takeover.txt")
                .expect("load persisted entry")
                .ino,
            attr.ino
        );
    });
}

#[test]
fn deferred_namespace_barrier_ignores_admissions_after_its_cutoff() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "deferred-namespace-barrier-cutoff");
    let _guard = rt.enter();
    rt.block_on(async {
        let early = InodeAttr::new_file(9_000_041, ROOT_INO, 0, 0, 0o644);
        let late = InodeAttr::new_file(9_000_042, ROOT_INO, 0, 0, 0o644);
        enqueue_deferred_namespace_create(&vfs, &early, b"early.txt");
        vfs.state
            .namespace
            .deferred
            .draining
            .store(true, AtomicOrdering::Release);

        let barrier_vfs = Arc::clone(&vfs);
        let barrier = tokio::spawn(async move {
            barrier_vfs
                .drain_deferred_namespace_metadata_for_barrier()
                .await
        });
        tokio::time::timeout(Duration::from_millis(100), async {
            while vfs
                .state
                .namespace
                .deferred
                .barrier_cutoff
                .load(AtomicOrdering::Acquire)
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("barrier should capture the first admission before the late admission");

        enqueue_deferred_namespace_create(&vfs, &late, b"late.txt");
        vfs.state.namespace.deferred.inner.lock().settled_operations = 1;
        vfs.state.namespace.deferred.notify.notify_waiters();

        tokio::time::timeout(Duration::from_millis(100), barrier)
            .await
            .expect("barrier should stop once its pre-cutoff operation settles")
            .expect("barrier task should not panic")
            .expect("barrier should not wait for the later admission");
    });
}

#[test]
fn deferred_namespace_barrier_resumes_background_drain_after_its_cutoff() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "deferred-namespace-barrier-resume");
    let _guard = rt.enter();
    rt.block_on(async {
        let early = InodeAttr::new_file(9_000_051, ROOT_INO, 0, 0, 0o644);
        let late = InodeAttr::new_file(9_000_052, ROOT_INO, 0, 0, 0o644);
        enqueue_deferred_namespace_create(&vfs, &early, b"early.txt");

        let barrier_vfs = Arc::clone(&vfs);
        let barrier = tokio::spawn(async move {
            barrier_vfs
                .drain_deferred_namespace_metadata_for_barrier()
                .await
        });
        tokio::time::timeout(Duration::from_millis(100), async {
            while vfs
                .state
                .namespace
                .deferred
                .barrier_cutoff
                .load(AtomicOrdering::Acquire)
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("barrier should capture the first admission before the late admission");

        enqueue_deferred_namespace_create(&vfs, &late, b"late.txt");

        tokio::time::timeout(Duration::from_secs(1), barrier)
            .await
            .expect("barrier should complete after its early operation")
            .expect("barrier task should not panic")
            .expect("barrier should persist its early operation");
        tokio::time::timeout(Duration::from_secs(1), async {
            vfs.wait_for_deferred_namespace_metadata(late.ino).await
        })
        .await
        .expect("background drain should resume for the late operation")
        .expect("late operation should commit");
        assert_eq!(
            meta.lookup(ROOT_INO, b"late.txt")
                .expect("load late persisted entry")
                .ino,
            late.ino
        );
    });
}

#[test]
fn shutdown_bounds_a_stalled_deferred_namespace_drain() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "shutdown-deferred-namespace-timeout");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = InodeAttr::new_file(9_000_061, ROOT_INO, 0, 0, 0o644);
        enqueue_deferred_namespace_create(&vfs, &attr, b"stalled.txt");
        vfs.state
            .namespace
            .deferred
            .draining
            .store(true, AtomicOrdering::Release);

        let shutdown = tokio::time::timeout(Duration::from_secs(3), async {
            vfs.wait_for_deferred_namespace_metadata_during_shutdown(Duration::from_secs(2))
                .await
        })
        .await
        .expect("shutdown must not wait forever for a stalled namespace drain");
        assert!(
            shutdown.is_err(),
            "shutdown should report the incomplete deferred namespace drain"
        );
    });
}

#[test]
fn shutdown_waits_for_an_active_empty_deferred_namespace_worker() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "shutdown-empty-deferred-worker");
    let _guard = rt.enter();
    rt.block_on(async {
        vfs.state
            .shutdown
            .shutdown_requested
            .store(true, AtomicOrdering::Release);
        vfs.state
            .namespace
            .deferred
            .draining
            .store(true, AtomicOrdering::Release);

        let waiter_vfs = Arc::clone(&vfs);
        let waiter = tokio::spawn(async move {
            waiter_vfs
                .wait_for_deferred_namespace_metadata_during_shutdown(Duration::from_secs(1))
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        vfs.state
            .namespace
            .deferred
            .draining
            .store(false, AtomicOrdering::Release);
        vfs.state.namespace.deferred.notify.notify_waiters();

        waiter
            .await
            .expect("shutdown drain waiter should not panic")
            .expect("an empty worker should settle cleanly before shutdown continues");
    });
}

#[test]
fn deferred_namespace_admission_rejects_an_oversized_backlog_entry() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-backlog-limit");
    let attr = InodeAttr::new_file(9_000_013, ROOT_INO, 0, 0, 0o644);
    let name = vec![b'x'; 1024 * 1024];

    let error = vfs.enqueue_deferred_namespace_op(
        Some(super::namespace::DeferredNamespaceKey::new(ROOT_INO, &name)),
        attr.clone(),
        super::namespace::DeferredNamespaceOp::CreateFile {
            attr,
            parent: ROOT_INO,
            name,
            uid: 0,
            gid: 0,
            mode: 0o644,
        },
    );
    assert!(matches!(error, Err(VfsError::WouldBlock)));

    let inner = vfs.state.namespace.deferred.inner.lock();
    assert!(inner.queue.is_empty());
    assert!(inner.entries.is_empty());
    assert!(inner.attrs.is_empty());
}

#[test]
fn deferred_namespace_admission_rejects_duplicate_deferred_key() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-duplicate-key");
    let first = InodeAttr::new_file(9_000_021, ROOT_INO, 0, 0, 0o644);
    let second = InodeAttr::new_file(9_000_022, ROOT_INO, 0, 0, 0o644);
    let key = super::namespace::DeferredNamespaceKey::new(ROOT_INO, b"duplicate.txt");

    vfs.enqueue_deferred_namespace_op(
        Some(key.clone()),
        first.clone(),
        super::namespace::DeferredNamespaceOp::CreateFile {
            attr: first.clone(),
            parent: ROOT_INO,
            name: b"duplicate.txt".to_vec(),
            uid: 0,
            gid: 0,
            mode: 0o644,
        },
    )
    .expect("first deferred key admission");

    let error = vfs
        .enqueue_deferred_namespace_op(
            Some(key.clone()),
            second.clone(),
            super::namespace::DeferredNamespaceOp::CreateFile {
                attr: second,
                parent: ROOT_INO,
                name: b"duplicate.txt".to_vec(),
                uid: 0,
                gid: 0,
                mode: 0o644,
            },
        )
        .expect_err("duplicate deferred key must be rejected atomically");
    assert!(matches!(
        error,
        VfsError::Meta(MetaError::EntryExists { parent, name })
            if parent == ROOT_INO && name == "duplicate.txt"
    ));
    assert_eq!(
        vfs.state.namespace.deferred.inner.lock().entries.get(&key),
        Some(&first.ino)
    );
}

#[test]
fn deferred_namespace_admission_bounds_operation_count() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-backlog-count");

    for index in 0..super::namespace_ops::DEFERRED_NAMESPACE_BACKLOG_MAX_OPERATIONS {
        let ino = 10_000_000 + index as u64;
        let name = format!("queued-{index}").into_bytes();
        let attr = InodeAttr::new_file(ino, ROOT_INO, 0, 0, 0o644);
        vfs.enqueue_deferred_namespace_op(
            Some(super::namespace::DeferredNamespaceKey::new(ROOT_INO, &name)),
            attr.clone(),
            super::namespace::DeferredNamespaceOp::CreateFile {
                attr,
                parent: ROOT_INO,
                name,
                uid: 0,
                gid: 0,
                mode: 0o644,
            },
        )
        .expect("admit operation within backlog limit");
    }

    let overflow_attr = InodeAttr::new_file(20_000_000, ROOT_INO, 0, 0, 0o644);
    let overflow = vfs.enqueue_deferred_namespace_op(
        Some(super::namespace::DeferredNamespaceKey::new(
            ROOT_INO,
            b"overflow",
        )),
        overflow_attr.clone(),
        super::namespace::DeferredNamespaceOp::CreateFile {
            attr: overflow_attr,
            parent: ROOT_INO,
            name: b"overflow".to_vec(),
            uid: 0,
            gid: 0,
            mode: 0o644,
        },
    );
    assert!(overflow.is_err());

    let callback_error = vfs
        .create_with_deferred_metadata_admission(
            ROOT_INO,
            b"callback-overflow",
            0,
            0,
            0o644,
            _rt.handle(),
        )
        .expect_err("shared admission should apply backpressure before create succeeds");
    assert!(matches!(callback_error, VfsError::WouldBlock));

    let inner = vfs.state.namespace.deferred.inner.lock();
    assert_eq!(
        inner.backlog_operations,
        super::namespace_ops::DEFERRED_NAMESPACE_BACKLOG_MAX_OPERATIONS
    );
    assert_eq!(inner.queue.len(), inner.backlog_operations);
    assert!(
        !inner
            .entries
            .contains_key(&super::namespace::DeferredNamespaceKey::new(
                ROOT_INO,
                b"callback-overflow"
            ))
    );
}

#[test]
fn deferred_namespace_admission_stops_after_shutdown_requested() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-shutdown-admission");
    vfs.state
        .shutdown
        .shutdown_requested
        .store(true, AtomicOrdering::Release);

    let error = vfs
        .create_with_deferred_metadata_admission(
            ROOT_INO,
            b"rejected-after-shutdown-request",
            0,
            0,
            0o644,
            rt.handle(),
        )
        .expect_err("shutdown must stop shared deferred namespace admission");
    assert!(matches!(error, VfsError::Other(message) if message.contains("shutdown")));
    assert!(vfs.state.namespace.deferred.inner.lock().queue.is_empty());
}

#[test]
fn deferred_namespace_durable_mutations_wait_for_admitted_entries() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "deferred-namespace-mutation-wait");
    let _guard = rt.enter();
    rt.block_on(async {
        let unlinked = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"unlink-before-commit.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .expect("admit file for unlink");
        assert!(vfs.has_deferred_namespace_metadata(unlinked.ino));
        vfs.unlink(1, b"unlink-before-commit.txt")
            .expect("unlink should wait for deferred create commit");
        assert!(vfs.lookup(1, b"unlink-before-commit.txt").is_err());

        let renamed = vfs
            .create_with_deferred_metadata_admission(
                1,
                b"rename-before-commit.txt",
                0,
                0,
                0o644,
                rt.handle(),
            )
            .expect("admit file for rename");
        assert!(vfs.has_deferred_namespace_metadata(renamed.ino));
        vfs.rename(1, b"rename-before-commit.txt", 1, b"renamed-after-wait.txt")
            .expect("rename should wait for deferred create commit");
        assert_eq!(
            vfs.lookup(1, b"renamed-after-wait.txt")
                .expect("renamed file should be visible")
                .ino,
            renamed.ino
        );

        let dir = vfs
            .mkdir_with_deferred_metadata_admission(
                1,
                b"rmdir-before-commit",
                0,
                0,
                0o755,
                rt.handle(),
            )
            .expect("admit dir for rmdir");
        assert!(vfs.has_deferred_namespace_metadata(dir.ino));
        vfs.rmdir(1, b"rmdir-before-commit")
            .expect("rmdir should wait for deferred mkdir commit");
        assert!(vfs.lookup(1, b"rmdir-before-commit").is_err());
    });
}

#[test]
fn test_datasync_with_outcome_treats_already_persisted_metadata_as_clean() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "datasync-outcome-persisted-metadata");
    let _guard = rt.enter();
    rt.block_on(async {
            let attr = vfs
                .create(1, b"datasync-outcome-persisted-metadata.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            vfs.setattr(
                ino,
                SetattrArgs {
                    mtime: studiofs_meta::types::TimeSpec::Now,
                    ..Default::default()
                },
            )
            .unwrap();

            let datasync = vfs.fsync_with_outcome(ino, true).await.unwrap();
            assert!(
                !datasync.flushed_file_data,
                "metadata-only datasync should not report a file-data barrier"
            );
            assert!(
                !datasync.persisted_dirty_attrs,
                "datasync should not report metadata persistence when setattr already committed it"
            );

            let full = vfs.fsync_with_outcome(ino, false).await.unwrap();
            assert!(
                !full.flushed_file_data,
                "full fsync after already-persisted metadata should stay data-clean"
            );
            assert!(
                !full.persisted_dirty_attrs,
                "full fsync should not invent metadata persistence work after setattr already committed it"
            );
        });
}

#[test]
fn test_file_session_wait_reasons_and_transitions_are_observable() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-observability");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-observability.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let _pin = vfs.state.file_session.session(ino);

        let session_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let vfs_clone = vfs.clone();
        let waiter = tokio::spawn(async move {
            let _queued = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Read)
                .await;
        });

        let mid = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let stats = vfs.file_session_stats();
                if stats.queued_ops >= 1 {
                    break stats;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expected at least one queued file-session operation");
        assert!(
            mid.queued_ops >= 1,
            "expected at least one queued file-session operation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(session_guard);
        waiter.await.unwrap();

        let final_stats = vfs.file_session_stats();
        assert!(
            final_stats.wait_events >= 1,
            "expected queued operation wait to be tracked"
        );
        assert!(
            final_stats.transitions >= 2,
            "expected start/finish transitions to be tracked"
        );
        assert!(
            final_stats
                .wait_reasons
                .iter()
                .any(|(reason, count)| reason == "read" && *count >= 1),
            "expected wait reason for read operation"
        );
    });
}

#[test]
fn test_size_changing_setattr_waits_on_file_session_gate() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-setattr-size-gate");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-setattr-size-gate.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let session_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let vfs_clone = vfs.clone();
        let setter = std::thread::spawn(move || vfs_clone.setattr(ino, SetattrArgs::size_only(1)));

        let mid = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let stats = vfs.file_session_stats();
                if stats.queued_ops >= 1 {
                    break stats;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expected size-changing setattr to wait behind the file-session gate");
        assert!(
            mid.queued_ops >= 1,
            "expected size-changing setattr to queue behind the file-session gate"
        );

        drop(session_guard);
        let updated = setter.join().unwrap().unwrap();
        assert_eq!(updated.size, 1);
    });
}

#[test]
fn test_size_changing_setattr_rollback_restores_old_local_state_on_failure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "setattr-size-rollback");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-size-rollback.txt", 0, 0, 0o644)
            .unwrap();
        let payload = b"setattr rollback payload";
        vfs.write(attr.ino, 0, payload).await.unwrap();

        vfs.arm_fail_next_size_change_setattr_before_flush_for_test();
        let error = vfs
            .setattr(attr.ino, SetattrArgs::size_only(8))
            .expect_err("simulated local persist failure should fail setattr");
        assert!(
            error
                .to_string()
                .contains("simulated size-changing setattr local persist failure"),
            "expected the injected size-changing setattr failure to surface"
        );

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            payload.len() as u64,
            "failed size-changing setattr must restore the previous durable size"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(attr.ino),
            "failed size-changing setattr must not leave dirty local state behind"
        );
        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload,
            "failed size-changing setattr must preserve the last durable local contents"
        );

        let blocks = meta.lookup_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "failed rollback should restore the original logical block summary"
        );
        assert_eq!(
            blocks[0].1.length as usize,
            payload.len(),
            "failed size-changing setattr must restore the previous block length"
        );
    });
}

#[test]
fn test_size_changing_setattr_rolls_back_partial_truncate_mutation_failure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "setattr-truncate-mutation-rollback");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-truncate-mutation-rollback.bin", 0, 0, 0o644)
            .unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x5Au8; block_size * 2];
        vfs.write(attr.ino, 0, &payload).await.unwrap();

        vfs.arm_fail_next_size_change_truncate_after_first_removed_block_for_test();
        let error = vfs
            .setattr(attr.ino, SetattrArgs::size_only((block_size / 2) as u64))
            .expect_err("simulated truncate mutation failure should fail setattr");
        assert!(
            error
                .to_string()
                .contains("simulated truncate mutation failure after first removed block"),
            "expected the injected truncate mutation failure to surface"
        );

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            payload.len() as u64,
            "failed truncate mutation must restore the previous durable size"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(attr.ino),
            "failed truncate mutation must not leave dirty local state behind"
        );

        let blocks = meta.lookup_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            2,
            "rollback should restore both originally committed payload slices"
        );
        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "failed truncate mutation must preserve the last durable local contents"
        );
    });
}

#[test]
fn test_file_session_read_lock_allows_parallel_readers() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-read-parallel");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-read-parallel.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let read_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let vfs_clone = vfs.clone();
        let second_reader = tokio::spawn(async move {
            let _second = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Read)
                .await;
        });

        tokio::time::timeout(Duration::from_millis(150), second_reader)
            .await
            .expect("parallel read lock acquisition should not block")
            .unwrap();
        drop(read_guard);
    });
}

#[test]
fn test_file_session_finalize_allows_parallel_readers() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-finalize-read-parallel");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-finalize-read-parallel.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let finalize_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Finalize)
            .await;
        let vfs_clone = vfs.clone();
        let reader = tokio::spawn(async move {
            let _reader = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Read)
                .await;
        });

        tokio::time::timeout(Duration::from_millis(150), reader)
            .await
            .expect("foreground reads must not wait for background upload finalization")
            .unwrap();
        drop(finalize_guard);
    });
}

#[test]
fn test_file_session_finalize_still_blocks_writers() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-finalize-write-gate");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-finalize-write-gate.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let finalize_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Finalize)
            .await;
        let vfs_clone = vfs.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut writer = Box::pin(tokio::spawn(async move {
            let _ = started_tx.send(());
            let _writer = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await;
        }));

        started_rx
            .await
            .expect("writer task should start before blocked-writer assertion");
        tokio::time::timeout(Duration::from_millis(50), &mut writer)
            .await
            .expect_err("writes must still wait for background upload finalization");
        drop(finalize_guard);
        tokio::time::timeout(Duration::from_millis(150), writer)
            .await
            .expect("writer should pass after finalization finishes")
            .unwrap();
    });
}

#[test]
fn test_file_session_finalize_serializes_other_finalizers() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-finalize-finalize-gate");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-finalize-finalize-gate.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let finalize_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Finalize)
            .await;
        let vfs_clone = vfs.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut second_finalize = Box::pin(tokio::spawn(async move {
            let _ = started_tx.send(());
            let _finalize = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Finalize)
                .await;
        }));

        started_rx
            .await
            .expect("second finalizer task should start before blocked-finalizer assertion");
        tokio::time::timeout(Duration::from_millis(50), &mut second_finalize)
            .await
            .expect_err("background finalizers for one inode must serialize");
        drop(finalize_guard);
        tokio::time::timeout(Duration::from_millis(150), second_finalize)
            .await
            .expect("second finalizer should pass after first finalizer finishes")
            .unwrap();
    });
}

#[test]
fn test_file_session_open_handle_stats_track_release_lifecycle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-open-handle-stats");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-open-handle-stats.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let after_first = vfs.file_session_stats();
        assert_eq!(
            after_first.active_sessions, 1,
            "opening the first handle should materialize one file session"
        );
        assert_eq!(
            after_first.open_handles, 1,
            "opening the first handle should increment open-handle tracking"
        );

        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let after_second = vfs.file_session_stats();
        assert_eq!(
            after_second.open_handles, 2,
            "opening a second handle should keep both handles observable"
        );

        vfs.release_fh(fh1).await.unwrap();
        let after_first_release = vfs.file_session_stats();
        assert_eq!(
            after_first_release.active_sessions, 1,
            "the session should stay alive until the last handle closes"
        );
        assert_eq!(
            after_first_release.open_handles, 1,
            "releasing one of two handles should decrement the handle count only once"
        );

        vfs.release_fh(fh2).await.unwrap();
        let final_stats = vfs.file_session_stats();
        assert_eq!(
            final_stats.open_handles, 0,
            "last-handle release should clear file-session open-handle tracking"
        );
        assert_eq!(
            final_stats.active_sessions, 0,
            "last-handle release should collect the idle file session"
        );
    });
}

#[test]
fn test_file_session_gc_keeps_session_while_handle_is_open() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-open-handle-gc");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-open-handle-gc.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        assert!(
            !vfs.state.file_session.gc_if_idle(ino),
            "gc must not collect a file session while a handle is open"
        );
        assert!(
            vfs.state.file_session.debug_snapshot(ino).is_some(),
            "open handles must keep their file session observable"
        );

        vfs.release_fh(fh).await.unwrap();
        assert!(
            vfs.state.file_session.debug_snapshot(ino).is_none(),
            "last-handle release should collect the now-idle file session"
        );
    });
}

#[test]
fn test_file_session_background_complete_gc_removes_idle_session() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-background-gc");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-background-gc.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let _pin = vfs.state.file_session.session(ino);
        assert_eq!(
            vfs.file_session_stats().active_sessions,
            1,
            "pinning a session should materialize it for the test"
        );

        drop(_pin);
        vfs.state.file_session.note_background_transition(
            ino,
            FileSessionOperation::Upload,
            "complete",
        );

        assert_eq!(
            vfs.file_session_stats().active_sessions,
            0,
            "background completion should collect an otherwise idle session"
        );
    });
}

#[test]
fn test_file_session_gc_removes_idle_read_only_session() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-gc");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"session-gc.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let _ = vfs.read(ino, 0, 1).await.unwrap();
        let stats = vfs.file_session_stats();
        assert_eq!(
            stats.active_sessions, 0,
            "idle read-only session should be collected"
        );
    });
}

#[test]
fn test_file_session_gc_reports_only_real_removals() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-gc-idempotent");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-gc-idempotent.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let pin = vfs.state.file_session.session(ino);
        drop(pin);

        assert!(
            vfs.state.file_session.gc_if_idle(ino),
            "first idle gc should remove the expired session entry"
        );
        assert!(
            !vfs.state.file_session.gc_if_idle(ino),
            "second idle gc should not report a removal after the entry is gone"
        );
    });
}

#[test]
fn test_file_session_background_transition_does_not_recreate_collected_session() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-background-transition-gc");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-background-transition-gc.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let pin = vfs.state.file_session.session(ino);
        drop(pin);

        assert!(
            vfs.state.file_session.gc_if_idle(ino),
            "setup should remove the expired session entry"
        );
        vfs.state.file_session.note_background_transition(
            ino,
            FileSessionOperation::Upload,
            "uploaded",
        );
        assert!(
            !vfs.state.file_session.gc_if_idle(ino),
            "background transition should not recreate a collected idle session"
        );
    });
}

#[test]
fn test_validate_name_valid() {
    assert!(validate_name(b"hello.txt").is_ok());
    assert!(validate_name(b"a").is_ok());
    assert!(validate_name(b"...").is_ok());
    assert!(validate_name(&vec![b'x'; 255]).is_ok());
}

#[test]
fn test_validate_name_empty() {
    assert!(matches!(validate_name(b""), Err(VfsError::InvalidName(_))));
}

#[test]
fn test_validate_name_too_long() {
    let long = vec![b'a'; 256];
    assert!(matches!(
        validate_name(&long),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn test_validate_name_slash() {
    assert!(matches!(
        validate_name(b"a/b"),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn test_validate_name_dot() {
    assert!(matches!(validate_name(b"."), Err(VfsError::InvalidName(_))));
    assert!(matches!(
        validate_name(b".."),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn test_validate_name_windows_special_chars() {
    // Test each Windows-invalid character: < > : " \ | ? *
    assert!(matches!(
        validate_name(b"file<name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file>name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file:name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file\"name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file\\name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file|name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file?name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file*name"),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn attr_read_cache_is_bounded_and_tracks_hotset_hits() {
    let mut cache = AttrReadCache::new(2, AttrReadCache::estimate_entry_charge() * 4);
    let now = Instant::now();
    cache.put(
        1,
        InodeAttr::new_file(1, ROOT_INO, 1000, 1000, 0o644),
        ATTR_CACHE_TTL,
        now,
    );
    cache.put(
        2,
        InodeAttr::new_file(2, ROOT_INO, 1000, 1000, 0o644),
        ATTR_CACHE_TTL,
        now,
    );

    assert!(cache.get(1, now).is_some());
    assert!(cache.get(1, now).is_some());

    cache.put(
        3,
        InodeAttr::new_file(3, ROOT_INO, 1000, 1000, 0o644),
        ATTR_CACHE_TTL,
        now,
    );
    let metrics = cache.metrics();

    assert!(metrics.occupancy_entries <= metrics.entry_budget);
    assert!(metrics.occupancy_bytes <= metrics.byte_budget);
    assert!(metrics.protected_occupancy_entries >= 1);
    assert!(metrics.protected_hits >= 1);
    assert!(metrics.hot_hit_ratio() > 0.0);
}

#[test]
fn attr_read_cache_expired_entries_do_not_increment_evictions() {
    let mut cache = AttrReadCache::new(8, AttrReadCache::estimate_entry_charge() * 8);
    let now = Instant::now();
    cache.put(
        1,
        InodeAttr::new_file(1, ROOT_INO, 1000, 1000, 0o644),
        Duration::from_millis(10),
        now,
    );

    assert!(
        cache.get(1, now + Duration::from_secs(1)).is_none(),
        "expired entry should be treated as stale miss"
    );
    let metrics = cache.metrics();
    assert_eq!(metrics.evictions, 0);
    assert_eq!(metrics.misses, 1);
}

#[test]
fn test_probable_trash_component_names() {
    assert!(is_probable_trash_component(b".Trash"));
    assert!(is_probable_trash_component(b".Trashes"));
    assert!(is_probable_trash_component(b".Trash-501"));
    assert!(is_probable_trash_component(b"$RECYCLE.BIN"));
    assert!(is_probable_trash_component(b"$Recycle.Bin"));
    assert!(!is_probable_trash_component(b"Trash"));
    assert!(!is_probable_trash_component(b".trash"));
    assert!(!is_probable_trash_component(b".Trashcan"));
}

#[test]
fn root_readdir_hides_trash_like_directories() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "root-readdir-hides-trash-like");

    for name in [
        b".Trash".as_slice(),
        b".Trashes",
        b".Trash-1000",
        b"$RECYCLE.BIN",
    ] {
        vfs.mkdir(ROOT_INO, name, 1000, 1000, 0o755)
            .unwrap_or_else(|err| panic!("create {}: {err}", String::from_utf8_lossy(name)));
    }
    vfs.mkdir(ROOT_INO, b"visible", 1000, 1000, 0o755)
        .expect("create visible directory");

    let mut names = vfs
        .readdir(ROOT_INO)
        .expect("readdir root")
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();

    assert_eq!(names, vec![b"visible".to_vec()]);
}

#[test]
fn trash_rename_deletes_file_entry() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "trash-rename-deletes-file");

    vfs.create(ROOT_INO, b"recover-me.txt", 1000, 1000, 0o644)
        .expect("create source file");
    let trash_root = vfs
        .mkdir(ROOT_INO, b".Trashes", 1000, 1000, 0o755)
        .expect("create .Trashes");
    let trash_user = vfs
        .mkdir(trash_root.ino, b"501", 1000, 1000, 0o755)
        .expect("create user trash");

    vfs.rename(
        ROOT_INO,
        b"recover-me.txt",
        trash_user.ino,
        b"recover-me.txt",
    )
    .expect("rename into trash-like directory");

    assert!(vfs.lookup(ROOT_INO, b"recover-me.txt").is_err());
    assert!(
        vfs.lookup(trash_user.ino, b"recover-me.txt").is_err(),
        "trash rename must delete instead of preserving a recoverable destination entry"
    );
}

#[test]
fn linux_trash_rename_deletes_directory_subtree() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "linux-trash-directory-delete");

    let source = vfs
        .mkdir(ROOT_INO, b"delete-tree", 1000, 1000, 0o755)
        .expect("create source directory");
    let child = vfs
        .mkdir(source.ino, b"nested", 1000, 1000, 0o755)
        .expect("create nested directory");
    vfs.create(child.ino, b"leaf.txt", 1000, 1000, 0o644)
        .expect("create leaf file");
    let trash_root = vfs
        .mkdir(ROOT_INO, b".Trash-1000", 1000, 1000, 0o755)
        .expect("create Linux trash root");
    let trash_files = vfs
        .mkdir(trash_root.ino, b"files", 1000, 1000, 0o755)
        .expect("create Linux trash files directory");

    vfs.rename(ROOT_INO, b"delete-tree", trash_files.ino, b"delete-tree")
        .expect("rename directory into Linux trash files directory");

    assert!(vfs.lookup(ROOT_INO, b"delete-tree").is_err());
    assert!(
        vfs.lookup(trash_files.ino, b"delete-tree").is_err(),
        "Linux trash directory rename must delete, not preserve a trash entry"
    );
}

#[test]
fn windows_recycle_bin_rename_deletes_directory_subtree() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "windows-recycle-bin-directory-delete");

    let source = vfs
        .mkdir(ROOT_INO, b"delete-tree", 1000, 1000, 0o755)
        .expect("create source directory");
    let child = vfs
        .mkdir(source.ino, b"nested", 1000, 1000, 0o755)
        .expect("create nested directory");
    vfs.create(child.ino, b"leaf.txt", 1000, 1000, 0o644)
        .expect("create leaf file");
    let recycle_bin = vfs
        .mkdir(ROOT_INO, b"$RECYCLE.BIN", 1000, 1000, 0o755)
        .expect("create recycle bin");

    vfs.rename(ROOT_INO, b"delete-tree", recycle_bin.ino, b"$RDIR")
        .expect("rename directory into recycle bin");

    assert!(vfs.lookup(ROOT_INO, b"delete-tree").is_err());
    assert!(
        vfs.lookup(recycle_bin.ino, b"$RDIR").is_err(),
        "Windows recycle-bin directory rename must delete, not preserve a trash entry"
    );
}

#[test]
fn strict_local_sync_flushes_pending_group_commit_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "strict-local-sync-group");
    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::Strict)
            .await
            .expect("strict local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "strict sync should force backend sync_data"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "strict sync should account for pending group-commit bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(group.pending_bytes, 0);
    });
}

#[test]
fn write_through_local_sync_flushes_pending_group_commit_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "write-through-local-sync-group");
    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::WriteThrough)
            .await
            .expect("write-through local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "write-through sync should force backend sync_data"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "write-through sync should account for pending group-commit bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(group.pending_bytes, 0);
    });
}

#[test]
fn opportunistic_local_sync_defers_when_threshold_not_reached() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-group");
    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 1024;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let deferred_before = vfs
        .state
        .metrics
        .payload_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::Opportunistic)
            .await
            .expect("opportunistic local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let deferred_after = vfs
        .state
        .metrics
        .payload_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);
    assert_eq!(
        calls_after, calls_before,
        "opportunistic sync should not force sync_data while below the byte threshold"
    );
    assert!(
        deferred_after > deferred_before,
        "opportunistic defer should be tracked"
    );
}

#[test]
fn opportunistic_local_sync_tracks_unsynced_bytes_until_group_sync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-unsynced");

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(1024, LocalSyncPolicy::Opportunistic)
            .await
            .expect("defer opportunistic local sync");
    });

    assert_eq!(
        vfs.state
            .local_dirty
            .local_sync_unsynced_bytes
            .load(Ordering::Relaxed),
        1024
    );

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::Strict)
            .await
            .expect("strict local sync");
    });

    assert_eq!(
        vfs.state
            .local_dirty
            .local_sync_unsynced_bytes
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn opportunistic_local_sync_defers_when_threshold_reached() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-threshold");
    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = vfs.local_sync_group_min_bytes;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    let deferred_before = vfs
        .state
        .metrics
        .payload_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::Opportunistic)
            .await
            .expect("opportunistic threshold local sync defers");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    let deferred_after = vfs
        .state
        .metrics
        .payload_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);
    assert_eq!(
        calls_after, calls_before,
        "opportunistic threshold should not force backend sync_data"
    );
    assert_eq!(
        synced_after, synced_before,
        "opportunistic threshold should leave grouped dirty bytes unsynced"
    );
    assert!(
        deferred_after > deferred_before,
        "opportunistic threshold defer should be tracked"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(group.pending_bytes, vfs.local_sync_group_min_bytes);
    });
}

#[test]
fn opportunistic_local_sync_does_not_queue_behind_inflight_sync() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-inflight");

    rt.block_on(async {
        let _held_gate = vfs.state.local_dirty.local_sync_gate.lock().await;
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = vfs.local_sync_group_min_bytes;
        group.last_sync_at = Instant::now();
        drop(group);

        tokio::time::timeout(Duration::from_millis(25), async {
            vfs.maybe_payload_store_sync_data_async(1024, LocalSyncPolicy::Opportunistic)
                .await
        })
        .await
        .expect("opportunistic sync must not wait behind an in-flight sync_data")
        .expect("opportunistic sync should defer when another sync is in-flight");

        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert!(
            group.pending_bytes >= vfs.local_sync_group_min_bytes + 1024,
            "deferred opportunistic sync should leave bytes queued for the next sync pass"
        );
        assert_eq!(
            vfs.state
                .metrics
                .payload_store_sync_data_calls
                .load(Ordering::Relaxed),
            0,
            "opportunistic skip should not start a second sync_data while the gate is held"
        );
    });
}

#[test]
fn flush_fh_enforces_strict_local_durability_in_single_mode() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-fh-fast-local-sync");
    let attr = vfs
        .create(1, b"flush-fh-fast-local-sync.txt", 0, 0, 0o644)
        .expect("create test file");
    let ino = attr.ino;
    let fh = vfs
        .open_file(ino, OpenFlags::RDWR, false)
        .expect("open writable handle");

    let mut dirty_attr = vfs.getattr(ino).expect("load attr");
    dirty_attr.size = 4096;
    dirty_attr.touch_mtime();
    dirty_attr.touch_ctime();
    vfs.state
        .namespace
        .dirty_attrs
        .lock()
        .insert(ino, dirty_attr.clone());

    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.flush_fh(fh).await.expect("flush_fh barrier");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "explicit flush must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "explicit flush should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "explicit flush should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "explicit flush should commit the dirty local attr state"
    );

    rt.block_on(async {
        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn flush_fh_default_config_enforces_strict_local_durability() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig::default();
    let (rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "flush-fh-durable-local-sync", cfg);
    let attr = vfs
        .create(1, b"flush-fh-durable-local-sync.txt", 0, 0, 0o644)
        .expect("create test file");
    let ino = attr.ino;
    let fh = vfs
        .open_file(ino, OpenFlags::RDWR, false)
        .expect("open writable handle");

    let mut dirty_attr = vfs.getattr(ino).expect("load attr");
    dirty_attr.size = 4096;
    dirty_attr.touch_mtime();
    dirty_attr.touch_ctime();
    vfs.state
        .namespace
        .dirty_attrs
        .lock()
        .insert(ino, dirty_attr.clone());

    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.flush_fh(fh).await.expect("flush_fh barrier");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "default-config explicit flush must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "default-config explicit flush should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "default-config explicit flush should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "default-config explicit flush should still commit the dirty local attr state"
    );

    rt.block_on(async {
        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn flush_fh_materializes_opportunistic_writer_stack_state_into_overlay_slices() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-flush-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-flush-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"flush-materialize";

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        vfs.flush_fh(fh).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "flush_fh should materialize block-backed local unpublished slices: {spans:?}"
        );
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "flush_fh should enqueue writer-stack payloads for background upload after metadata commit"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "flush_fh should drain writer-stack dirty state"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn size_changing_setattr_syncs_materialized_writer_stack_payloads() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "setattr-syncs-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-syncs-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"setattr-writer-stack-local-durability";

        vfs.write_fh(fh, ino, 0, payload).await.unwrap();
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "setup should leave payload bytes in the writer stack"
        );

        let calls_before = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        let synced_before = vfs
            .state
            .metrics
            .payload_store_sync_data_synced_bytes
            .load(Ordering::Relaxed);

        vfs.setattr(ino, SetattrArgs::size_only(8))
            .expect("truncate through size-changing setattr");

        let calls_after = vfs
            .state
            .metrics
            .payload_store_sync_data_calls
            .load(Ordering::Relaxed);
        let synced_after = vfs
            .state
            .metrics
            .payload_store_sync_data_synced_bytes
            .load(Ordering::Relaxed);
        assert!(
            calls_after > calls_before,
            "size-changing setattr must force a strict payload-store sync"
        );
        assert!(
            synced_after >= synced_before + payload.len() as u64,
            "strict sync should account for materialized writer-stack bytes"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn flush_fh_materializes_writer_stack_state_through_strict_local_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-245-flush-without-ordered-flush");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-flush-without-ordered-flush.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            b"direct-materialize",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let ordered_flushes_before = vfs
            .state
            .metrics
            .flush_barrier_calls
            .load(Ordering::Relaxed);

        vfs.flush_fh(fh).await.unwrap();

        let ordered_flushes_after = vfs
            .state
            .metrics
            .flush_barrier_calls
            .load(Ordering::Relaxed);
        assert_eq!(
            ordered_flushes_after,
            ordered_flushes_before + 1,
            "slice-first writer-stack flush should route through one strict local barrier to commit staged slices"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_empty_payload_is_a_noop_for_writer_stack_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-245-empty-handle-write");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-empty-handle-write.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let written = vfs
            .write_fh_with_deferred_materialize(fh, ino, 0, &[], LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        assert_eq!(written, 0);
        assert!(
            !vfs.state.deferred_materialize_inflight.contains(ino),
            "empty handle writes should not schedule background materialization"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "empty handle writes should not create dirty writer-stack state"
        );
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "empty handle writes should not stage dirty attrs"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn writer_stack_dirty_attrs_keep_nanos_timestamps() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-handle-write-nanos");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-handle-write-nanos.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let before = vfs.getattr(ino).unwrap();

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            b"timestamp-check",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("dirty attr");
        assert!(
            dirty_attr.mtime >= before.mtime,
            "writer-stack mtime should not go backwards"
        );
        assert!(
            dirty_attr.ctime >= before.ctime,
            "writer-stack ctime should not go backwards"
        );
        assert!(
            dirty_attr.mtime > 1_000_000_000_000,
            "writer-stack timestamps should remain in nanoseconds"
        );
        assert!(
            dirty_attr.ctime > 1_000_000_000_000,
            "writer-stack timestamps should remain in nanoseconds"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn release_fh_enforces_strict_local_durability_on_last_handle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-fh-fast-local-sync");
    let attr = vfs
        .create(1, b"release-fh-fast-local-sync.txt", 0, 0, 0o644)
        .expect("create test file");
    let ino = attr.ino;
    let fh = vfs
        .open_file(ino, OpenFlags::RDWR, false)
        .expect("open writable handle");

    let mut dirty_attr = vfs.getattr(ino).expect("load attr");
    dirty_attr.size = 8192;
    dirty_attr.touch_mtime();
    dirty_attr.touch_ctime();
    vfs.state
        .namespace
        .dirty_attrs
        .lock()
        .insert(ino, dirty_attr.clone());

    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 8192;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release_fh barrier");
        assert!(
            outcome.closed_last_handle,
            "the only handle should own the last-handle release path"
        );
        assert!(
            outcome.flushed_local_dirty_state,
            "last-handle release should report draining dirty local state"
        );
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "last-handle release must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 8192,
        "last-handle release should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "last-handle release should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "last-handle release should preserve the committed local attr state"
    );
}

#[test]
fn release_fh_default_config_enforces_strict_local_durability() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let cfg = VfsConfig::default();
    let (rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "release-fh-durable-local-sync", cfg);
    let attr = vfs
        .create(1, b"release-fh-durable-local-sync.txt", 0, 0, 0o644)
        .expect("create test file");
    let ino = attr.ino;
    let fh = vfs
        .open_file(ino, OpenFlags::RDWR, false)
        .expect("open writable handle");

    let mut dirty_attr = vfs.getattr(ino).expect("load attr");
    dirty_attr.size = 8192;
    dirty_attr.touch_mtime();
    dirty_attr.touch_ctime();
    vfs.state
        .namespace
        .dirty_attrs
        .lock()
        .insert(ino, dirty_attr.clone());

    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 8192;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release_fh barrier");
        assert!(
            outcome.closed_last_handle,
            "the only handle should own the last-handle release path"
        );
        assert!(
            outcome.flushed_local_dirty_state,
            "last-handle release should report draining dirty local state"
        );
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "default-config last-handle release must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 8192,
        "default-config last-handle release should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "default-config last-handle release should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "default-config last-handle release should preserve the committed local attr state"
    );
}

#[test]
fn release_fh_materializes_opportunistic_writer_stack_state() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-release-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-release-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"release-materialize";

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "last-handle release should materialize block-backed local unpublished slices: {spans:?}"
        );
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "last-handle release should enqueue writer-stack payloads for background upload after metadata commit"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "last-handle release should drain writer-stack dirty state"
        );
    });
}

#[test]
fn release_fh_invalidates_kernel_attrs_after_dirty_last_handle_close() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-invalidates-dirty-attrs");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-invalidates-dirty-attrs.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let invalidator = Arc::new(RecordingKernelInvalidator::default());
        vfs.set_kernel_invalidator(invalidator.clone());

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            b"release should drop stale kernel attrs",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        assert!(
            invalidator.attr_snapshot().is_empty(),
            "write should not invalidate kernel attrs while the handle is active"
        );

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(
            invalidator.attr_snapshot().contains(&ino),
            "last-handle release should evict stale kernel attrs after local dirty writes"
        );
    });
}

#[test]
fn release_fh_drains_tail_after_deferred_completed_block_materialization() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-301-release-deferred-tail");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-301-release-deferred-tail.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let tail_size = block_size / 3;
        let mut payload = vec![0xC3; block_size];
        payload.extend(std::iter::repeat_n(0x7E, tail_size));

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, &payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        vfs.materialize_writer_stack_completed_blocks_for_inode(ino)
            .await
            .unwrap();
        assert_eq!(
            vfs.total_pending_persisted_local_dirty_slice_bytes(),
            block_size,
            "setup should persist the completed block locally before release"
        );
        assert_eq!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino),
            tail_size,
            "setup should leave only the partial tail in the writer stack"
        );

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();

        assert!(outcome.closed_last_handle);
        assert!(
            outcome.flushed_local_dirty_state,
            "last-handle release must report the deferred tail drain"
        );
        assert_eq!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino),
            0,
            "last-handle release must not leave a writer-stack tail for a closed file"
        );
        assert_eq!(
            vfs.total_landed_write_drain_lag_bytes(),
            0,
            "closed file should have no landed-write drain lag after release"
        );
        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "release should commit materialized payload slices"
        );
        let pending_offsets = vfs
            .pending_uploads_for_inode(ino)
            .into_iter()
            .map(|(offset, _, _)| offset)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pending_offsets,
            BTreeSet::from([0, vfs.block_size as u64]),
            "release should queue every committed sparse writer-stack slice for upload"
        );
    });
}

#[test]
fn release_fh_materializes_sparse_cross_chunk_tail_before_size_extend() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sparse-cross-chunk-tail-before-extend");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sparse-cross-chunk-tail-before-extend.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let head = vec![0xA1; 233];
        let tail = vec![0xB2; 262];
        let tail_offset = u64::from(studiofs_meta::schema::DEFAULT_CHUNK_SIZE).saturating_sub(7);
        let logical_size = tail_offset + 262 + 1_048_444;

        let head_fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh(head_fh, ino, 0, &head).await.unwrap();
        vfs.release_fh_with_outcome(head_fh).await.unwrap();

        let tail_fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh(tail_fh, ino, tail_offset, &tail)
            .await
            .unwrap();
        vfs.release_fh_with_outcome(tail_fh).await.unwrap();

        let truncate_fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.setattr_with_handle(Some(truncate_fh), ino, SetattrArgs::size_only(logical_size))
            .expect("extend logical EOF after sparse tail release");
        vfs.release_fh_with_outcome(truncate_fh).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, logical_size)
            .expect("load sparse graph after separate write releases");
        let tail_range_start = u32::try_from(tail_offset).expect("tail offset fits in range start");
        let published_offsets = spans
            .iter()
            .filter(|span| span.payload.is_some())
            .map(|span| span.range.start)
            .collect::<BTreeSet<_>>();
        assert!(
            published_offsets.contains(&0),
            "prefix payload should remain in the sparse graph: {spans:?}"
        );
        assert!(
            published_offsets.contains(&tail_range_start),
            "cross-chunk sparse payload must be materialized before size-only extend: {spans:?}"
        );
    });
}

#[test]
fn deferred_materialization_linger_preserves_stream_coalescing() {
    assert_eq!(
        DEFERRED_MATERIALIZATION_LINGER_WAIT_MS, 200,
        "synchronous bridge callbacks need enough time to coalesce one payload object"
    );
}

#[test]
fn strict_flush_interrupts_deferred_materialization_linger() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) =
        make_unstarted_test_vfs(&tmp, "interrupt-deferred-materialization-linger");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"interrupt-deferred-materialization-linger.bin",
                0,
                0,
                0o644,
            )
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file");

        let payload = [0xA5; 8 * 1024];
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write small callback");
        tokio::time::timeout(Duration::from_millis(250), async {
            while !vfs.state.deferred_materialize_inflight.contains(ino) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deferred materializer should enter its coalescing window");

        tokio::time::timeout(Duration::from_millis(500), vfs.flush_fh(fh))
            .await
            .expect("strict flush must wake a lingering materializer")
            .expect("strict flush should persist the accepted write");
        assert_eq!(vfs.state.writer_stack.dirty_bytes_for_inode(ino), 0);
        let mut readback = vec![0; payload.len()];
        let read_len = vfs
            .read_into_fh(fh, ino, 0, &mut readback)
            .await
            .expect("read flushed payload");
        assert_eq!(read_len as usize, payload.len());
        assert_eq!(readback, payload);
        vfs.release_fh(fh).await.expect("release file handle");
    });
}

#[test]
fn last_handle_release_interrupts_deferred_materialization_linger() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "release-interrupts-linger");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-interrupts-linger.bin", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file");
        let payload = [0x6B; 8 * 1024];
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write small callback");

        tokio::time::timeout(Duration::from_millis(500), vfs.release_fh(fh))
            .await
            .expect("last-handle release must wake a lingering materializer")
            .expect("release accepted write");
        assert_eq!(vfs.open_count(ino), 0);
        assert_eq!(vfs.state.writer_stack.dirty_bytes_for_inode(ino), 0);

        let read_fh = vfs
            .open_file(ino, OpenFlags::RDONLY, false)
            .expect("reopen file");
        let mut readback = vec![0; payload.len()];
        let read_len = vfs
            .read_into_fh(read_fh, ino, 0, &mut readback)
            .await
            .expect("read released payload");
        assert_eq!(read_len as usize, payload.len());
        assert_eq!(readback, payload);
        vfs.release_fh(read_fh).await.expect("release read handle");
    });
}

#[test]
fn strict_flush_interrupts_clean_deferred_materialization_linger() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "interrupt-clean-linger");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"interrupt-clean-linger.bin", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file");
        let payload = vec![0x73; vfs.block_size as usize];
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write full block");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && vfs.state.deferred_materialize_inflight.contains(ino)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("materializer should enter the clean coalescing window");
        tokio::time::timeout(Duration::from_millis(500), vfs.flush_fh(fh))
            .await
            .expect("strict flush must wake the clean-linger branch")
            .expect("flush full block");
        vfs.release_fh(fh).await.expect("release file handle");
    });
}

#[test]
fn deferred_materialization_coalesces_synchronous_small_callbacks() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "coalesce-small-sync-callbacks");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"coalesce-small-sync-callbacks.bin", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file");
        let callback = [0x5A; 8 * 1024];
        let callback_count = 32usize;

        for index in 0..callback_count {
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                (index * callback.len()) as u64,
                &callback,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .expect("write synchronous-sized callback");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let expected_bytes = callback.len() * callback_count;
        assert_eq!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino),
            expected_bytes,
            "active small callbacks should remain in one coalescing writer buffer"
        );
        assert_eq!(
            vfs.total_pending_persisted_local_dirty_slice_bytes(),
            0,
            "the coalescing window must not emit one persisted object per callback"
        );

        tokio::time::timeout(Duration::from_millis(500), vfs.flush_fh(fh))
            .await
            .expect("strict flush must interrupt the coalescing window")
            .expect("flush coalesced callbacks");
        assert_eq!(
            vfs.pending_uploads_for_inode(ino).len(),
            1,
            "one partial block should produce one upload object after flush"
        );
        vfs.release_fh(fh).await.expect("release file handle");
    });
}

#[test]
fn deferred_materialization_drains_idle_tail_before_close() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-301-idle-tail-before-close");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-301-idle-tail-before-close.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let tail_size = block_size / 3;
        let mut payload = vec![0xA1; block_size];
        payload.extend(std::iter::repeat_n(0xB2, tail_size));

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let idle_tail_timeout = Duration::from_millis(
            DEFERRED_MATERIALIZATION_LINGER_WAIT_MS
                * DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES as u64
                * 3,
        )
        .max(Duration::from_secs(2));
        tokio::time::timeout(idle_tail_timeout, async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && vfs.total_pending_persisted_local_dirty_slice_bytes() == 0
                    && !vfs.pending_uploads_for_inode(ino).is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle deferred tail should materialize and enter upload before close");

        assert_eq!(
            vfs.open_count(ino),
            1,
            "setup keeps the writer handle open to exercise background idle-tail drain"
        );
        assert_eq!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino),
            0,
            "background materialization should not wait for last-handle release"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn deferred_materialization_drains_tail_after_write_handle_closes_with_reader_open() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-301-idle-tail-reader-open");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-301-idle-tail-reader-open.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let writer = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let reader = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let payload = vec![0xC3; vfs.block_size as usize / 3];

        vfs.write_fh_with_deferred_materialize(
            writer,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        vfs.release_fh(writer).await.unwrap();

        assert_eq!(vfs.open_count(ino), 1);
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && !vfs.pending_uploads_for_inode(ino).is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle tail should drain after the writer closes with a reader open");

        vfs.release_fh(reader).await.unwrap();
    });
}

#[test]
fn deferred_materialization_keeps_hot_open_writer_task_for_next_full_block() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-546-hot-writer-drain");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-546-hot-writer-drain.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let second_block_offset = block_size as u64;
        let first_block = vec![0x46; block_size];
        let second_block = vec![0x47; block_size];
        let hot_writer_timeout = Duration::from_millis(
            DEFERRED_MATERIALIZATION_LINGER_WAIT_MS
                * DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES as u64
                * 3,
        )
        .max(Duration::from_secs(2));

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &first_block,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        tokio::time::timeout(hot_writer_timeout, async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && vfs.state.deferred_materialize_inflight.contains(ino)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect(
            "hot open writer should keep one materializer alive after a clean full-block drain",
        );

        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            second_block_offset,
            &second_block,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        tokio::time::timeout(hot_writer_timeout, async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && (vfs.total_pending_persisted_local_dirty_slice_bytes() >= block_size
                        || vfs
                            .pending_uploads_for_inode(ino)
                            .iter()
                            .any(|(offset, _, _)| *offset == second_block_offset))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("same hot materializer should drain the next completed full block");

        let mut read = vec![0; block_size * 2];
        let read_len = vfs.read_into_fh(fh, ino, 0, &mut read).await.unwrap();
        assert_eq!(read_len as usize, read.len());
        assert_eq!(&read[..block_size], first_block.as_slice());
        assert_eq!(&read[block_size..], second_block.as_slice());

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn deferred_materialization_reschedules_write_that_arrives_while_inflight_finishes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-301-inflight-reschedule-tail");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-301-inflight-reschedule-tail.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let (entry, _guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve the deferred materialization gate");

        let payload = vec![0xD4; vfs.block_size as usize];
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "occupied in-flight slot should suppress immediate upload scheduling"
        );
        assert!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino) > 0,
            "write should remain buffered until the finishing materializer reschedules"
        );

        vfs.finish_deferred_materialization_task(entry, true);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if vfs.state.writer_stack.dirty_bytes_for_inode(ino) == 0
                    && !vfs.pending_uploads_for_inode(ino).is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("finishing materializer should reschedule dirty writer-stack state");

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn first_write_after_truncate_ignores_stale_pretruncate_dirty_attr() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "recovered-truncate-write-epoch");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"recovered-truncate-write-epoch.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        vfs.write(ino, 0, b"locally unpublished").await.unwrap();
        vfs.flush(ino).await.unwrap();
        let unpublished_attr = vfs.getattr(ino).unwrap();
        assert_eq!(unpublished_attr.committed_size, 0);
        assert_ne!(unpublished_attr.upload_state, MetaUploadState::Ready);

        let fh = vfs.open_file(ino, OpenFlags::RDWR, true).unwrap();
        let truncate_attr = vfs
            .handle_table
            .get(fh)
            .and_then(|open_file| open_file.open_attr())
            .expect("truncate open should retain its generation snapshot");
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, unpublished_attr);

        let rewritten = b"rewritten";
        vfs.write_fh_with_local_sync_policy(fh, ino, 0, rewritten, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        let rewritten_attr = vfs.getattr(ino).unwrap();
        assert_eq!(
            (rewritten_attr.size, rewritten_attr.content_epoch),
            (rewritten.len() as u64, truncate_attr.content_epoch + 1),
            "the truncate handle generation must win over a late pre-truncate dirty attr"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn deferred_inode_gate_waiter_does_not_hold_registry_lock_while_awaiting() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let gate = DeferredInodeGate::default();
        let ino = 42;
        let (entry, guard) = gate
            .try_register(ino)
            .expect("first registration should own the inode gate");

        let waiting_gate = gate.clone();
        let wait = tokio::spawn(async move {
            waiting_gate.wait(ino).await;
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !wait.is_finished(),
            "waiter should block on the async per-inode guard"
        );

        let replacement = gate.try_register(ino);
        assert!(
            replacement.is_none(),
            "waiter must not hold the synchronous registry lock while awaiting the per-inode guard"
        );

        drop(entry);
        drop(guard);
        wait.await.expect("wait task should not panic");
    });
}

#[test]
fn deferred_inode_gate_cleanup_only_removes_current_entry() {
    let gate = DeferredInodeGate::default();
    let ino = 43;
    let (older_entry, _older_guard) = gate
        .try_register(ino)
        .expect("first registration should own the inode gate");
    let (newer_entry, newer_guard) = gate.replace(ino);

    drop(older_entry);
    assert!(
        gate.contains(ino),
        "finishing a replaced entry must not remove the current inode gate"
    );

    drop(newer_guard);
    drop(newer_entry);
    assert!(
        !gate.contains(ino),
        "finishing the current entry should remove the inode gate"
    );
}

#[test]
fn deferred_inode_gate_entry_drop_removes_current_entry() {
    let gate = DeferredInodeGate::default();
    let ino = 44;
    let (entry, _guard) = gate
        .try_register(ino)
        .expect("first registration should own the inode gate");

    drop(entry);
    assert!(
        !gate.contains(ino),
        "dropping the current entry must remove the inode gate even before the async guard is released"
    );
}

#[test]
fn deferred_inode_gate_occupied_callback_blocks_current_entry_cleanup() {
    let gate = DeferredInodeGate::default();
    let ino = 46;
    let (entry, _guard) = gate
        .try_register(ino)
        .expect("first registration should own the inode gate");
    let (callback_entered_tx, callback_entered_rx) = std::sync::mpsc::channel();
    let (release_callback_tx, release_callback_rx) = std::sync::mpsc::channel();
    let cleanup_finished = Arc::new(AtomicBool::new(false));

    let occupied_gate = gate.clone();
    let occupied = std::thread::spawn(move || {
        let registered = occupied_gate.try_register_or_update_occupied(ino, || {
            callback_entered_tx
                .send(())
                .expect("signal occupied callback entry");
            release_callback_rx
                .recv()
                .expect("wait for cleanup contention check");
        });
        assert!(
            registered.is_none(),
            "occupied registration should not create a second gate entry"
        );
    });
    callback_entered_rx
        .recv()
        .expect("occupied callback should start");

    let cleanup_gate = gate.clone();
    let cleanup_finished_for_thread = Arc::clone(&cleanup_finished);
    let cleanup = std::thread::spawn(move || {
        let replacement = cleanup_gate.replace_if_current_or_remove(&entry, || false);
        assert!(
            replacement.is_none(),
            "cleanup without reschedule should remove the current entry"
        );
        cleanup_finished_for_thread.store(true, AtomicOrdering::SeqCst);
    });

    std::thread::sleep(Duration::from_millis(25));
    assert!(
        !cleanup_finished.load(AtomicOrdering::SeqCst),
        "current entry cleanup must wait until the occupied callback has recorded its reschedule marker"
    );

    release_callback_tx
        .send(())
        .expect("release occupied callback");
    occupied
        .join()
        .expect("occupied registration should finish");
    cleanup.join().expect("cleanup should finish");
    assert!(
        !gate.contains(ino),
        "cleanup should remove the current gate entry after the occupied callback completes"
    );
}

#[test]
fn deferred_inode_gate_wait_returns_when_guard_released_before_entry_cleanup() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let gate = DeferredInodeGate::default();
        let ino = 45;
        let (_entry, guard) = gate
            .try_register(ino)
            .expect("first registration should own the inode gate");

        drop(guard);
        tokio::time::timeout(Duration::from_millis(100), gate.wait(ino))
            .await
            .expect("wait should return when the current gate is unlocked but still registered");
    });
}

#[test]
fn strict_deferred_write_waits_for_inflight_materialization() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-301-strict-waits-inflight");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-301-strict-waits-inflight.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let (entry, guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve the deferred materialization gate");

        let strict_vfs = Arc::clone(&vfs);
        let strict_write = tokio::spawn(async move {
            strict_vfs
                .write_fh_with_deferred_materialize(
                    fh,
                    ino,
                    0,
                    b"strict",
                    LocalSyncPolicy::Strict,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !strict_write.is_finished(),
            "strict writes must wait until earlier deferred materialization leaves the inflight slot"
        );

        drop(entry);
        drop(guard);
        strict_write
            .await
            .expect("strict write task should not panic")
            .expect("strict write should complete after inflight materialization drains");
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn explicit_winfsp_flush_waits_for_deferred_last_handle_release_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-427-flush-waits-release");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-427-flush-waits-release.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let (entry, guard) = vfs
            .state
            .deferred_release_inflight
            .try_register(ino)
            .expect("test should reserve the deferred release gate");

        let flushing_vfs = Arc::clone(&vfs);
        let flush = tokio::spawn(async move {
            flushing_vfs
                .flush_volatile_writeback_with_outcome(ino, fh)
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !flush.is_finished(),
            "explicit WinFSP flush must wait for an in-flight deferred last-handle release barrier before reporting sync success"
        );

        drop(entry);
        drop(guard);
        flush
            .await
            .expect("flush task should not panic")
            .expect("flush should complete after deferred release barrier drains");
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn deferred_release_drains_many_tiny_closed_files_promptly() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "deferred-release-many-tiny");
    let _guard = rt.enter();
    rt.block_on(async {
        let payload = b"tiny close payload";
        let mut inos = Vec::new();
        for index in 0..32 {
            let name = format!("tiny-{index}.txt");
            let attr = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                0,
                payload,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
            let outcome = vfs.release_fh_with_deferred_last_handle_barrier(fh, rt.handle());
            assert!(outcome.closed_last_handle);
            inos.push(ino);
        }

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let all_drained = inos.iter().all(|ino| {
                    !vfs.state.writer_stack.has_dirty_writers(*ino)
                        && meta
                            .get_attr(*ino)
                            .is_ok_and(|attr| attr.size == payload.len() as u64)
                });
                if all_drained {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("deferred close should drain many tiny files promptly");
    });
}

#[test]
fn deferred_release_barrier_replaces_older_inflight_marker() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-427-release-replaces-marker");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-427-release-replaces-marker.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let (older, _older_guard) = vfs.register_deferred_release_barrier(ino);
        let (newer, newer_guard) = vfs.register_deferred_release_barrier(ino);

        drop(older);
        assert!(
            vfs.state.deferred_release_inflight.contains(ino),
            "dropping a replaced barrier must not remove the current marker"
        );

        let waiting_vfs = Arc::clone(&vfs);
        let wait = tokio::spawn(async move {
            waiting_vfs.wait_for_deferred_release_inflight(ino).await;
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !wait.is_finished(),
            "foreground sync must wait for the newest deferred release marker, not the replaced one"
        );

        drop(newer_guard);
        drop(newer);
        wait.await.expect("wait task should not panic");
        assert!(
            !vfs.state.deferred_release_inflight.contains(ino),
            "dropping the current barrier guard must remove its marker"
        );
    });
}

#[test]
fn deferred_release_ignored_entry_skips_release_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "ignored-release-skips-barrier");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"._DS_Store", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let outcome = vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &tokio::runtime::Handle::current());
        assert!(outcome.closed_last_handle);
        assert!(!outcome.flushed_local_dirty_state);
        assert!(
            !vfs.state.deferred_release_inflight.contains(ino),
            "ignored entries should not register a deferred release barrier"
        );
    });
}

#[test]
fn deferred_release_clean_entry_skips_release_barrier() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "clean-release-skips-barrier");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"clean-release-skips-barrier.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh(fh, ino, 0, b"clean release").await.unwrap();
        vfs.fsync(ino, false).await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "fsync should make the release action clean"
        );

        let outcome = vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &tokio::runtime::Handle::current());

        assert!(outcome.closed_last_handle);
        assert!(!outcome.flushed_local_dirty_state);
        assert!(
            !vfs.state.deferred_release_inflight.contains(ino),
            "clean last-handle release should not block follow-up reads on a no-op barrier"
        );
    });
}

#[test]
fn deferred_release_barrier_registration_holds_lock_before_marker_is_visible() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-427-release-atomic-register");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-427-release-atomic-register.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let (barrier, guard) = vfs.register_deferred_release_barrier(ino);
        let waiting_vfs = Arc::clone(&vfs);
        let wait = tokio::spawn(async move {
            waiting_vfs.wait_for_deferred_release_inflight(ino).await;
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !wait.is_finished(),
            "foreground sync must not be able to acquire a newly registered deferred release marker before the background release task owns it"
        );
        assert_eq!(
            barrier.interrupt_waiter_count(),
            0,
            "deferred-release waits must stay passive"
        );

        drop(guard);
        drop(barrier);
        wait.await.expect("wait task should not panic");
    });
}

#[test]
fn deferred_materialization_wait_tracks_rescheduled_marker() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "materialize-wait-reschedule");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"materialize-wait-reschedule.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let (older_entry, older_guard) = vfs
            .state
            .deferred_materialize_inflight
            .try_register(ino)
            .expect("test should reserve the deferred materialization gate");

        let waiting_vfs = Arc::clone(&vfs);
        let wait = tokio::spawn(async move {
            waiting_vfs
                .wait_for_deferred_materialization_inflight(ino)
                .await;
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !wait.is_finished(),
            "strict barriers must wait for the current materialization task"
        );

        let (newer_entry, newer_guard) = vfs
            .state
            .deferred_materialize_inflight
            .replace_if_current(&older_entry)
            .expect("current materialization gate should be replaceable");
        drop(older_guard);

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !wait.is_finished(),
            "strict barriers must continue waiting when materialization reschedules another task"
        );

        drop(newer_entry);
        drop(newer_guard);
        wait.await.expect("wait task should not panic");
    });
}

#[test]
fn deferred_materialization_interrupt_waiters_are_cancellation_safe_and_inode_scoped() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, _vfs, _meta) = make_unstarted_test_vfs(&tmp, "materialize-interrupt-waiters");
    let _guard = rt.enter();
    rt.block_on(async {
        let gate = DeferredInodeGate::default();
        let (entry_a, guard_a) = gate
            .try_register(101)
            .expect("register inode A materializer");
        let (entry_b, guard_b) = gate
            .try_register(202)
            .expect("register inode B materializer");

        let first_gate = gate.clone();
        let first_waiter = tokio::spawn(async move {
            first_gate.wait_interrupting(101).await;
        });
        let second_gate = gate.clone();
        let second_waiter = tokio::spawn(async move {
            second_gate.wait_interrupting(101).await;
        });
        tokio::time::timeout(Duration::from_millis(250), async {
            while entry_a.interrupt_waiter_count() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both strict waiters should register on the exact task generation");

        first_waiter.abort();
        let _ = first_waiter.await;
        assert_eq!(
            entry_a.interrupt_waiter_count(),
            1,
            "canceling one strict waiter must release only its own registration"
        );
        assert!(
            !entry_b
                .wait_linger_or_interrupt(Duration::from_millis(20))
                .await,
            "an inode A waiter must not interrupt inode B"
        );

        drop(entry_a);
        drop(guard_a);
        tokio::time::timeout(Duration::from_millis(250), second_waiter)
            .await
            .expect("remaining waiter should acquire the released gate")
            .expect("remaining waiter should not panic");
        drop(entry_b);
        drop(guard_b);
    });
}

#[test]
fn interrupted_materializer_does_not_orphan_dirty_tail_if_last_waiter_is_cancelled() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "cancelled-last-materialize-waiter");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"cancelled-last-materialize-waiter.bin", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file");
        let payload = [0x4C; 8 * 1024];
        vfs.write_fh_with_deferred_materialize(
            fh,
            ino,
            0,
            &payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write dirty tail");

        let waiting_vfs = Arc::clone(&vfs);
        let waiter = tokio::spawn(async move {
            waiting_vfs
                .wait_for_deferred_materialization_inflight(ino)
                .await;
            std::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_millis(500), async {
            while vfs.state.deferred_materialize_inflight.contains(ino) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("interrupted materializer should hand off its gate");
        waiter.abort();
        let _ = waiter.await;

        assert_eq!(
            vfs.state.writer_stack.dirty_bytes_for_inode(ino),
            0,
            "a cancelled last waiter must not leave the interrupted tail without a materializer"
        );
        let mut readback = vec![0; payload.len()];
        let read_len = vfs
            .read_into_fh(fh, ino, 0, &mut readback)
            .await
            .expect("read materialized tail");
        assert_eq!(read_len as usize, payload.len());
        assert_eq!(readback, payload);
        vfs.release_fh(fh).await.expect("release file handle");
    });
}

#[test]
fn namespace_create_records_one_complete_metadata_timing_sample() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "namespace-create-metadata-timing");
    let before = vfs.write_path_observability_snapshot();

    vfs.create(ROOT_INO, b"timed-create.txt", 0, 0, 0o644)
        .expect("create file");

    let after = vfs.write_path_observability_snapshot();
    let direct_metadata_engine = meta.metadata_write_observability();
    assert_eq!(
        after.metadata_engine.begin_write.calls, direct_metadata_engine.begin_write.calls,
        "the VFS snapshot must expose the active redb metadata engine metrics"
    );
    assert_eq!(
        after.metadata_commit.calls,
        before.metadata_commit.calls + 1,
        "one namespace create should record one metadata timing sample"
    );
    assert_eq!(
        after.metadata_commit_cadence.events,
        before.metadata_commit_cadence.events + 1,
        "a successful namespace create should record one completed metadata commit"
    );
    assert_eq!(
        after.namespace_create.inode_allocation.calls,
        before.namespace_create.inode_allocation.calls + 1,
        "one namespace create should expose its inode allocation transaction"
    );
    assert_eq!(
        after.namespace_create.namespace_mutation.calls,
        before.namespace_create.namespace_mutation.calls + 1,
        "one namespace create should expose its namespace mutation transaction"
    );
    assert_eq!(after.namespace_create.completed_entries, 1);
    assert_eq!(after.namespace_create.transactions_per_created_entry, 2.0);
    assert_eq!(
        after.metadata_engine.begin_write.calls,
        before.metadata_engine.begin_write.calls + 2,
        "inode allocation and namespace mutation should each begin one redb write transaction"
    );
    assert_eq!(
        after.metadata_engine.mutation.calls,
        before.metadata_engine.mutation.calls + 2,
        "each namespace-create transaction should record mutation time"
    );
    assert_eq!(
        after.metadata_engine.commit.calls,
        before.metadata_engine.commit.calls + 2,
        "each namespace-create transaction should record commit time"
    );
}

fn namespace_inode_allocation_calls(vfs: &StudioVfs) -> u64 {
    vfs.write_path_observability_snapshot()
        .namespace_create
        .inode_allocation
        .calls
}

#[test]
fn namespace_inode_reservation_refills_after_exhaustion() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "namespace-inode-refill");

    let first = vfs.reserve_namespace_ino().expect("reserve first inode");
    let (previous, contiguous) =
        (1..NAMESPACE_INODE_BATCH).fold((first, true), |(previous, contiguous), _| {
            let ino = vfs.reserve_namespace_ino().expect("reserve inode");
            (ino, contiguous && ino == previous + 1)
        });
    assert!(contiguous, "reserved inode range should be contiguous");
    assert_eq!(namespace_inode_allocation_calls(&vfs), 1);

    let refilled = vfs.reserve_namespace_ino().expect("refill inode range");
    assert_eq!(refilled, previous + 1);
    assert_eq!(namespace_inode_allocation_calls(&vfs), 2);
}

#[test]
fn namespace_inode_reservation_is_unique_under_concurrent_refill() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "namespace-inode-concurrent");
    let threads = (0..8)
        .map(|_| {
            let vfs = vfs.clone();
            thread::spawn(move || {
                (0..(NAMESPACE_INODE_BATCH / 4) as usize)
                    .map(|_| vfs.reserve_namespace_ino().expect("reserve inode"))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let inos = threads
        .into_iter()
        .flat_map(|thread| thread.join().expect("reservation thread"))
        .collect::<BTreeSet<_>>();

    assert_eq!(inos.len(), 2 * NAMESPACE_INODE_BATCH as usize);
    assert_eq!(namespace_inode_allocation_calls(&vfs), 2);
}

#[test]
fn namespace_inode_reservation_leaves_restart_gaps() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let first = {
        let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "namespace-inode-restart");
        let first = vfs.reserve_namespace_ino().expect("reserve first inode");
        drop(vfs);
        drop(meta);
        first
    };

    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "namespace-inode-restart");
    let after_restart = vfs
        .reserve_namespace_ino()
        .expect("reserve inode after restart");

    assert_eq!(after_restart, first + u64::from(NAMESPACE_INODE_BATCH));
}

#[test]
fn create_and_mkdir_share_namespace_inode_reservation() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "namespace-inode-create-mkdir");

    let file = vfs
        .create(ROOT_INO, b"reserved-file", 0, 0, 0o644)
        .expect("create file");
    let dir = vfs
        .mkdir(ROOT_INO, b"reserved-dir", 0, 0, 0o755)
        .expect("create directory");

    assert_eq!(dir.ino, file.ino + 1);
    assert_eq!(namespace_inode_allocation_calls(&vfs), 1);
}

#[test]
fn write_path_observability_tracks_burst_shape_and_metadata_commit_stalls() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "write-path-observability-bursts");
    let block_size = meta.get_format_info().expect("get format info").block_size as usize;

    rt.block_on(async {
        let attr = vfs
            .create(1, b"write-path-burst.bin", 0, 0, 0o644)
            .expect("create");
        let first = vec![0x5Au8; block_size];
        let second = vec![0xA5u8; block_size];

        vfs.write(attr.ino, 0, &first).await.expect("first write");
        vfs.flush(attr.ino).await.expect("first flush");

        tokio::time::sleep(Duration::from_millis(2)).await;

        vfs.write(attr.ino, block_size as u64, &second)
            .await
            .expect("second write");
        vfs.flush(attr.ino).await.expect("second flush");

        // Force a deterministic durability barrier so the sync metric is stable.
        vfs.fsync(attr.ino, false).await.expect("fsync barrier");
    });

    let snapshot = vfs.write_path_observability_snapshot();
    assert!(
        snapshot.payload_burst.bursts >= 2,
        "two write+flush rounds should yield at least two payload bursts"
    );
    assert!(
        snapshot.payload_burst.total_payloads >= 2,
        "burst accounting should include persisted full blocks"
    );
    assert!(
        snapshot.payload_burst.total_bytes >= (block_size as u64) * 2,
        "burst accounting should include persisted payload bytes"
    );
    assert!(
        snapshot.payload_persist.calls >= 2,
        "payload stage timing should be captured per persisted burst"
    );
    assert!(
        snapshot.metadata_commit.calls >= 2,
        "metadata commit timing should be captured per persisted burst"
    );
    assert!(
        snapshot.metadata_commit_cadence.events >= 2,
        "metadata commit cadence should be captured per completed commit"
    );
    assert!(
        snapshot
            .metadata_commit_cadence
            .interval_histogram
            .bucket_counts
            .iter()
            .copied()
            .sum::<u64>()
            >= 1,
        "metadata commit cadence should record at least one interval sample after multiple commits"
    );
    assert!(
        snapshot.metadata_commit_cadence.max_interval_ms > 0.0,
        "metadata commit cadence should expose a positive interval after multiple commits"
    );
    assert!(
        snapshot.payload_store_sync_data.calls >= 1,
        "sync barrier calls should be surfaced in write-path observability"
    );
    assert_eq!(
        snapshot.eager_flush.checks,
        snapshot.eager_flush.triggered
            + snapshot.eager_flush.skipped_empty
            + snapshot.eager_flush.skipped_below_threshold,
        "eager flush checks should be partitioned by outcome"
    );
}

#[test]
fn write_path_observability_reports_active_tuning_knobs() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "write-path-observability-tuning");
    let block_size = meta.get_format_info().expect("get format info").block_size as usize;

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.tuning.eager_flush_threshold_bytes, vfs.write_eager_flush_batch_bytes,
        "observability should report the active eager flush threshold"
    );
    assert!(
        snapshot.tuning.eager_flush_drain_chunk_bytes > 0,
        "observability should report a non-zero eager drain chunk"
    );
    assert!(
        snapshot.tuning.eager_flush_drain_chunk_bytes
            <= snapshot.tuning.eager_flush_threshold_bytes,
        "eager drain chunk should never exceed the trigger threshold"
    );
    assert!(
        snapshot.tuning.payload_put_concurrency >= 1,
        "payload put concurrency should be explicit in observability"
    );
    assert!(
        snapshot.tuning.payload_put_concurrency
            <= snapshot
                .tuning
                .eager_flush_drain_chunk_bytes
                .div_ceil(block_size)
                .max(1),
        "payload put concurrency should be bounded by the eager drain chunk"
    );
    assert_eq!(
        snapshot.tuning.sync_group_min_bytes, vfs.local_sync_group_min_bytes,
        "observability should report the active sync-data group minimum"
    );
}

#[test]
fn write_path_observability_reports_upload_reread_pressure() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-upload-reread");

    vfs.state
        .upload
        .upload_hot_payload_reads
        .store(3, Ordering::Relaxed);
    vfs.state
        .upload
        .upload_hot_payload_read_bytes
        .store(12 * 1024 * 1024, Ordering::Relaxed);
    vfs.state
        .upload
        .upload_staged_payload_reads
        .store(5, Ordering::Relaxed);
    vfs.state
        .upload
        .upload_staged_payload_read_bytes
        .store(20 * 1024 * 1024, Ordering::Relaxed);
    vfs.state
        .upload
        .upload_fallback_rereads
        .store(2, Ordering::Relaxed);
    vfs.state
        .upload
        .upload_fallback_reread_bytes
        .store(8 * 1024 * 1024, Ordering::Relaxed);
    vfs.state
        .upload
        .upload_cache_io_foreground_cost
        .store(7, Ordering::Relaxed);
    vfs.state
        .read
        .foreground_local_payload_reads
        .store(11, Ordering::Relaxed);

    let snapshot = vfs.write_path_observability_snapshot();

    assert_eq!(snapshot.foreground_reads.upload_hot_payload_reads, 3);
    assert_eq!(
        snapshot.foreground_reads.upload_staged_payload_read_bytes,
        20 * 1024 * 1024
    );
    assert_eq!(snapshot.foreground_reads.upload_fallback_rereads, 2);
    assert_eq!(snapshot.foreground_reads.upload_cache_io_foreground_cost, 7);
    assert_eq!(snapshot.foreground_reads.foreground_local_payload_reads, 11);

    let json = serde_json::to_value(&snapshot).expect("serialize write-path snapshot");
    assert_eq!(json["foreground_reads"]["upload_hot_payload_reads"], 3);
    assert_eq!(
        json["foreground_reads"]["upload_fallback_reread_bytes"],
        8 * 1024 * 1024
    );
}

#[test]
fn foreground_metrics_apply_bounded_upload_scheduler_penalty() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "upload-independent-foreground-metrics");

    vfs.upload_scheduler.set_adaptive_penalty(Duration::ZERO);
    vfs.adjust_upload_backpressure_from_metrics(
        Some(vfs.upload_fg_read_latency_target_ns.saturating_mul(8)),
        3,
    );

    let penalty_ms = vfs.upload_scheduler.adaptive_penalty_ms();
    assert!(
        penalty_ms > 0 && penalty_ms <= UPLOAD_SCHEDULER_PENALTY_MAX_MS,
        "foreground-read congestion should apply only the bounded adaptive penalty: {penalty_ms}"
    );
    let io_cost = vfs
        .state
        .upload
        .upload_cache_io_foreground_cost
        .load(Ordering::Relaxed);
    assert!(
        io_cost > vfs.upload_cache_io_foreground_cost_base,
        "foreground-read congestion should increase upload reread slot cost"
    );
}

#[test]
fn write_path_observability_reports_fetch_counters() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-fetch");

    vfs.fetch_scheduler
        .metrics
        .cache_hits
        .store(13, Ordering::Relaxed);
    vfs.fetch_scheduler
        .metrics
        .s3_fetches
        .store(5, Ordering::Relaxed);
    vfs.fetch_scheduler
        .metrics
        .singleflight_joins
        .store(3, Ordering::Relaxed);
    vfs.fetch_scheduler
        .metrics
        .bytes_downloaded
        .store(20 * 1024 * 1024, Ordering::Relaxed);
    vfs.state
        .metrics
        .readahead_scheduled_blocks
        .store(11, Ordering::Relaxed);
    vfs.state
        .metrics
        .readahead_dropped_blocks
        .store(2, Ordering::Relaxed);
    vfs.state
        .metrics
        .readahead_used_bytes
        .store(6 * 1024 * 1024, Ordering::Relaxed);

    let snapshot = vfs.write_path_observability_snapshot();

    assert_eq!(snapshot.fetch.cache_hits, 13);
    assert_eq!(snapshot.fetch.s3_fetches, 5);
    assert_eq!(snapshot.fetch.singleflight_joins, 3);
    assert_eq!(snapshot.fetch.bytes_downloaded, 20 * 1024 * 1024);
    assert_eq!(snapshot.fetch.readahead_scheduled_blocks, 11);
    assert_eq!(snapshot.fetch.readahead_dropped_blocks, 2);
    assert_eq!(snapshot.fetch.readahead_used_bytes, 6 * 1024 * 1024);
}

#[test]
fn write_path_observability_reports_fuse_callback_and_vfs_read_counters() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-fuse-callbacks");

    rt.block_on(async {
        vfs.payload_store_put_async(0xF5E491, Bytes::from_static(&[7; 8192]), false)
            .await
            .expect("payload store put");
        let mut out = [0_u8; 4096];
        let read = vfs
            .payload_store_read_into_async(0xF5E491, 0, &mut out)
            .await
            .expect("payload store read_into");
        assert_eq!(read, Some(4096));
    });
    vfs.note_vfs_read_call(4096);
    vfs.record_fuse_read_callback(4096, Duration::from_millis(3), false);
    vfs.record_fuse_write_callback(128 * 1024, Duration::from_millis(5), true);

    let snapshot = vfs.write_path_observability_snapshot();

    assert_eq!(snapshot.vfs_read_path.calls, 1);
    assert_eq!(snapshot.vfs_read_path.requested_bytes, 4096);
    assert_eq!(snapshot.vfs_read_path.foreground_local_payload_reads, 1);
    assert_eq!(
        snapshot.vfs_read_path.foreground_local_payload_read_bytes,
        4096
    );
    assert_eq!(snapshot.fuse_callbacks.read.calls, 1);
    assert_eq!(snapshot.fuse_callbacks.read.bytes, 4096);
    assert_eq!(snapshot.fuse_callbacks.read.errors, 0);
    assert_eq!(snapshot.fuse_callbacks.write.calls, 1);
    assert_eq!(snapshot.fuse_callbacks.write.bytes, 128 * 1024);
    assert_eq!(snapshot.fuse_callbacks.write.errors, 1);
    assert!(
        snapshot.fuse_callbacks.write.avg_ms >= snapshot.fuse_callbacks.read.avg_ms,
        "write callback latency should reflect the longer recorded duration"
    );
}

#[test]
fn write_path_observability_reports_fuse_async_write_snapshot() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-fuse-async-write");

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 1,
        enabled: true,
        linux_fuse_profile: "high-throughput".into(),
        fuse_max_write_bytes: 16 * 1024 * 1024,
        fuse_max_readahead_bytes: 128 * 1024,
        fuse_max_background: Some(128),
        fuse_congestion_threshold: Some(96),
        shared_buffer_budget_bytes: Some(300 * 1024 * 1024),
        prefetch_concurrency: Some(1),
        upload_concurrency: Some(20),
        download_concurrency: Some(200),
        kernel_writeback_cache: Some(false),
        max_pending_bytes: Some(128 * 1024 * 1024),
        max_pending_writes: Some(1024),
        per_handle_pending_bytes: Some(32 * 1024 * 1024),
        pending_writes: 3,
        pending_bytes: 256 * 1024,
        peak_pending_bytes: 512 * 1024,
        completed_writes: 17,
        failed_writes: 2,
        task_failed_writes: 1,
        admission_waits: 5,
        admission_wait_total_ms: 6.5,
        barrier_waits: 7,
        barrier_wait_total_ms: 8.5,
        ..Default::default()
    });

    let snapshot = vfs.write_path_observability_snapshot();

    assert!(snapshot.fuse_async_write.enabled);
    assert_eq!(
        snapshot.fuse_async_write.linux_fuse_profile,
        "high-throughput"
    );
    assert_eq!(snapshot.fuse_async_write.fuse_max_background, Some(128));
    assert_eq!(
        snapshot.fuse_async_write.max_pending_bytes,
        Some(128 * 1024 * 1024)
    );
    assert_eq!(snapshot.fuse_async_write.pending_writes, 3);
    assert_eq!(snapshot.fuse_async_write.pending_bytes, 256 * 1024);
    assert_eq!(snapshot.fuse_async_write.peak_pending_bytes, 512 * 1024);
    assert_eq!(snapshot.fuse_async_write.completed_writes, 17);
    assert_eq!(snapshot.fuse_async_write.failed_writes, 2);
    assert_eq!(snapshot.fuse_async_write.task_failed_writes, 1);
    assert_eq!(snapshot.fuse_async_write.admission_waits, 5);
    assert_eq!(snapshot.fuse_async_write.admission_wait_total_ms, 6.5);
    assert_eq!(snapshot.fuse_async_write.barrier_waits, 7);
    assert_eq!(snapshot.fuse_async_write.barrier_wait_total_ms, 8.5);
    assert_eq!(
        snapshot.fuse_async_write.shared_buffer_budget_bytes,
        Some(300 * 1024 * 1024)
    );
    assert_eq!(snapshot.fuse_async_write.upload_concurrency, Some(20));
    assert_eq!(
        snapshot.fuse_async_write.kernel_writeback_cache,
        Some(false)
    );
}

#[test]
fn write_path_observability_records_linux_fuse_open_cache_reasons() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "linux-fuse-open-cache-observability");

    vfs.record_linux_fuse_open_cache_decision(
        false,
        OsCacheDisposition::Reuse,
        OsCacheReason::StableGeneration,
        false,
    );
    vfs.record_linux_fuse_open_cache_decision(
        true,
        OsCacheDisposition::Replace,
        OsCacheReason::GenerationChanged,
        true,
    );

    let stats = vfs
        .write_path_observability_snapshot()
        .linux_fuse_open_cache;
    assert_eq!(stats.total, 2);
    assert_eq!(stats.direct_opens, 1);
    assert_eq!(stats.kernel_opens, 1);
    assert_eq!(stats.reuse, 1);
    assert_eq!(stats.replace, 1);
    assert_eq!(stats.memory_bypasses, 1);
    assert_eq!(stats.stable_generation, 1);
    assert_eq!(stats.generation_changed, 1);
}

#[test]
fn write_path_observability_reports_linux_fuse_worker_and_connection_snapshots() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "linux-fuse-worker-observability");

    vfs.record_fuse_invalidation_snapshot(VfsFuseInvalidationSnapshot {
        snapshot_generation: 4,
        queue_depth: 2,
        queue_peak: 8,
        coalesced: 5,
        skipped_unobserved: 3,
        notify_failures: 1,
        notify_latency_count: 7,
        notify_latency_total_ns: 700,
        notify_latency_max_ns: 250,
        shutdown_timeouts: 1,
        ..Default::default()
    });
    vfs.record_fuse_connection_snapshot(VfsFuseConnectionSnapshot {
        snapshot_generation: 2,
        cache_policy: "adaptive".into(),
        requested_max_read_bytes: 4 * 1024 * 1024,
        requested_max_readahead_bytes: 4 * 1024 * 1024,
        effective_max_read_bytes: 1024 * 1024,
        effective_max_readahead_bytes: 512 * 1024,
        effective_max_write_bytes: 2 * 1024 * 1024,
        effective_max_background: Some(128),
        effective_congestion_threshold: Some(96),
        capable_features: 0x31,
        enabled_features: 0x11,
    });

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(snapshot.fuse_invalidation.queue_depth, 2);
    assert_eq!(snapshot.fuse_invalidation.queue_peak, 8);
    assert_eq!(snapshot.fuse_invalidation.notify_latency_total_ns, 700);
    assert_eq!(snapshot.fuse_connection.cache_policy, "adaptive");
    assert_eq!(
        snapshot.fuse_connection.effective_max_readahead_bytes,
        512 * 1024
    );
    assert_eq!(snapshot.fuse_connection.enabled_features, 0x11);
}

#[test]
fn linux_fuse_observability_rejects_stale_replace_snapshots() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "linux-fuse-stale-observability");

    vfs.record_fuse_invalidation_snapshot(VfsFuseInvalidationSnapshot {
        snapshot_generation: 3,
        queue_peak: 9,
        ..Default::default()
    });
    vfs.record_fuse_invalidation_snapshot(VfsFuseInvalidationSnapshot {
        snapshot_generation: 2,
        queue_peak: 1,
        ..Default::default()
    });
    vfs.record_fuse_connection_snapshot(VfsFuseConnectionSnapshot {
        snapshot_generation: 5,
        cache_policy: "adaptive".into(),
        ..Default::default()
    });
    vfs.record_fuse_connection_snapshot(VfsFuseConnectionSnapshot {
        snapshot_generation: 4,
        cache_policy: "direct".into(),
        ..Default::default()
    });

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(snapshot.fuse_invalidation.queue_peak, 9);
    assert_eq!(snapshot.fuse_connection.cache_policy, "adaptive");
}

#[test]
fn runtime_stats_surface_reports_linux_fuse_numeric_metrics() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "runtime-stats-linux-fuse-observability");

    vfs.record_linux_fuse_open_cache_decision(
        false,
        OsCacheDisposition::Reuse,
        OsCacheReason::StableGeneration,
        false,
    );
    vfs.record_fuse_invalidation_snapshot(VfsFuseInvalidationSnapshot {
        queue_depth: 2,
        coalesced: 5,
        notify_latency_count: 3,
        notify_latency_total_ns: 90,
        ..Default::default()
    });
    vfs.record_fuse_connection_snapshot(VfsFuseConnectionSnapshot {
        cache_policy: "adaptive".into(),
        effective_max_read_bytes: 1024 * 1024,
        ..Default::default()
    });

    let snapshot = vfs.runtime_stats_snapshot();
    assert_eq!(snapshot.bridge.counters["linux_fuse_kernel_opens"].value, 1);
    assert_eq!(
        snapshot.bridge.counters["fuse_invalidation_coalesced"].value,
        5
    );
    assert_eq!(
        snapshot.bridge.gauges["fuse_invalidation_queue_depth"].value,
        2
    );
    assert_eq!(
        snapshot.bridge.gauges["fuse_effective_max_read_bytes"].value,
        1024 * 1024
    );
    assert_eq!(
        snapshot.bridge.latencies["fuse_invalidation_notify"].count,
        3
    );
}

#[test]
fn runtime_stats_surface_reports_fuse_profile_knobs() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "runtime-stats-fuse-profile-knobs");

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        linux_fuse_profile: "high-throughput".into(),
        fuse_max_write_bytes: 16 * 1024 * 1024,
        fuse_max_readahead_bytes: 128 * 1024,
        fuse_max_background: Some(128),
        fuse_congestion_threshold: Some(96),
        shared_buffer_budget_bytes: Some(300 * 1024 * 1024),
        prefetch_concurrency: Some(1),
        upload_concurrency: Some(20),
        download_concurrency: Some(200),
        kernel_writeback_cache: Some(false),
        ..Default::default()
    });

    let snapshot = vfs.runtime_stats_snapshot();

    assert_eq!(snapshot.bridge.gauges["fuse_max_background"].value, 128);
    assert_eq!(
        snapshot.bridge.gauges["fuse_congestion_threshold"].value,
        96
    );
    assert_eq!(
        snapshot.bridge.gauges["fuse_max_write_bytes"].value,
        16 * 1024 * 1024
    );
    assert_eq!(
        snapshot.bridge.gauges["fuse_max_readahead_bytes"].value,
        128 * 1024
    );
    assert_eq!(
        snapshot.bridge.gauges["fuse_shared_buffer_budget_bytes"].value,
        300 * 1024 * 1024
    );
    assert_eq!(snapshot.bridge.gauges["fuse_upload_concurrency"].value, 20);
    assert_eq!(
        snapshot.bridge.gauges["fuse_kernel_writeback_cache"].value,
        0
    );
    assert_eq!(
        snapshot.bridge.gauges["fuse_kernel_writeback_cache"].unit,
        "count"
    );
}

#[test]
fn runtime_stats_surface_skips_absent_fuse_profile_knobs() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "runtime-stats-absent-fuse-profile-knobs");

    let snapshot = vfs.runtime_stats_snapshot();

    assert!(
        !snapshot
            .bridge
            .gauges
            .contains_key("fuse_shared_buffer_budget_bytes")
    );
    assert!(
        !snapshot
            .bridge
            .gauges
            .contains_key("fuse_read_buffer_budget_bytes")
    );
    assert!(
        !snapshot
            .bridge
            .gauges
            .contains_key("fuse_upload_concurrency")
    );
    assert!(
        !snapshot
            .bridge
            .gauges
            .contains_key("fuse_kernel_writeback_cache")
    );
}

#[test]
fn fuse_async_write_snapshot_rejects_stale_gauges_and_keeps_monotonic_counters() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "fuse-async-write-stale-snapshot");

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 2,
        enabled: true,
        pending_writes: 0,
        pending_bytes: 0,
        peak_pending_bytes: 1024,
        completed_writes: 2,
        failed_writes: 1,
        task_failed_writes: 1,
        admission_waits: 3,
        admission_wait_total_ms: 4.0,
        barrier_waits: 5,
        barrier_wait_total_ms: 6.0,
        ..Default::default()
    });
    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 1,
        enabled: true,
        pending_writes: 7,
        pending_bytes: 8 * 1024,
        peak_pending_bytes: 2048,
        completed_writes: 3,
        failed_writes: 2,
        task_failed_writes: 2,
        admission_waits: 6,
        admission_wait_total_ms: 7.0,
        barrier_waits: 8,
        barrier_wait_total_ms: 9.0,
        ..Default::default()
    });

    let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
    assert_eq!(snapshot.snapshot_generation, 2);
    assert_eq!(snapshot.pending_writes, 0);
    assert_eq!(snapshot.pending_bytes, 0);
    assert_eq!(snapshot.peak_pending_bytes, 1024);
    assert_eq!(snapshot.completed_writes, 2);
    assert_eq!(snapshot.failed_writes, 1);
    assert_eq!(snapshot.task_failed_writes, 1);
    assert_eq!(snapshot.admission_waits, 3);
    assert_eq!(snapshot.admission_wait_total_ms, 4.0);
    assert_eq!(snapshot.barrier_waits, 5);
    assert_eq!(snapshot.barrier_wait_total_ms, 6.0);
}

#[test]
fn write_path_observability_reports_winfsp_write_callback_breakdown() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-winfsp-breakdown");

    vfs.note_winfsp_write_callback();
    vfs.record_winfsp_write_callback_observability(
        256 * 1024,
        LocalSyncPolicy::Strict,
        Duration::from_millis(6),
        Duration::from_millis(5),
        Duration::from_millis(1),
    );

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.winfsp_callbacks.write, 1,
        "callback count should still be reflected in the WinFSP callback breakdown"
    );
    assert_eq!(
        snapshot.winfsp_write_path.write_bytes,
        256 * 1024,
        "observability should surface cumulative write callback bytes"
    );
    assert!(
        snapshot.winfsp_write_path.avg_write_bytes >= 256.0 * 1024.0,
        "observability should expose average bytes per write callback"
    );
    assert_eq!(
        snapshot.winfsp_write_path.strict_calls, 1,
        "observability should attribute strict WinFSP callbacks"
    );
    assert_eq!(
        snapshot.winfsp_write_path.strict_bytes,
        256 * 1024,
        "observability should surface bytes written through the strict WinFSP path"
    );
    assert_eq!(
        snapshot.winfsp_write_path.opportunistic_calls, 0,
        "observability should keep opportunistic counts separate"
    );
    assert_eq!(
        snapshot.winfsp_write_path.callback.calls, 1,
        "callback latency stats should count WinFSP write callbacks"
    );
    assert_eq!(
        snapshot.winfsp_write_path.vfs_write.calls, 1,
        "VFS write latency stats should count WinFSP callback work"
    );
    assert_eq!(
        snapshot.winfsp_write_path.post_write_getattr.calls, 1,
        "post-write getattr latency stats should count the WinFSP tail work"
    );
    assert!(
        snapshot.winfsp_write_path.callback.total_ms
            >= snapshot.winfsp_write_path.vfs_write.total_ms,
        "full callback latency should be at least as large as the inner VFS write latency"
    );
}

#[test]
fn reset_diagnostics_observability_clears_winfsp_write_policy_breakdown() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "reset-winfsp-write-policy-breakdown");

    vfs.record_winfsp_write_callback_observability(
        11,
        LocalSyncPolicy::Strict,
        Duration::from_millis(3),
        Duration::from_millis(2),
        Duration::from_millis(1),
    );
    vfs.record_winfsp_write_callback_observability(
        13,
        LocalSyncPolicy::WriteThrough,
        Duration::from_millis(3),
        Duration::from_millis(2),
        Duration::from_millis(1),
    );
    vfs.record_winfsp_write_callback_observability(
        17,
        LocalSyncPolicy::Opportunistic,
        Duration::from_millis(3),
        Duration::from_millis(2),
        Duration::from_millis(1),
    );

    let before = vfs.write_path_observability_snapshot();
    assert_eq!(before.winfsp_write_path.strict_calls, 1);
    assert_eq!(before.winfsp_write_path.strict_bytes, 11);
    assert_eq!(before.winfsp_write_path.write_through_calls, 1);
    assert_eq!(before.winfsp_write_path.write_through_bytes, 13);
    assert_eq!(before.winfsp_write_path.opportunistic_calls, 1);
    assert_eq!(before.winfsp_write_path.opportunistic_bytes, 17);

    vfs.reset_diagnostics_observability();

    let after = vfs.write_path_observability_snapshot();
    assert_eq!(after.winfsp_write_path.strict_calls, 0);
    assert_eq!(after.winfsp_write_path.strict_bytes, 0);
    assert_eq!(after.winfsp_write_path.write_through_calls, 0);
    assert_eq!(after.winfsp_write_path.write_through_bytes, 0);
    assert_eq!(after.winfsp_write_path.opportunistic_calls, 0);
    assert_eq!(after.winfsp_write_path.opportunistic_bytes, 0);
}

#[test]
fn reset_diagnostics_observability_clears_fuse_async_write_snapshot() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "reset-fuse-async-write-snapshot");

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 7,
        enabled: true,
        pending_writes: 3,
        pending_bytes: 4096,
        peak_pending_bytes: 8192,
        completed_writes: 11,
        failed_writes: 2,
        task_failed_writes: 1,
        admission_waits: 4,
        admission_wait_total_ms: 5.0,
        barrier_waits: 6,
        barrier_wait_total_ms: 7.0,
        ..Default::default()
    });

    assert!(
        vfs.write_path_observability_snapshot()
            .fuse_async_write
            .enabled
    );

    vfs.reset_diagnostics_observability();

    let after = vfs.write_path_observability_snapshot().fuse_async_write;
    assert_eq!(after.snapshot_generation, 0);
    assert!(!after.enabled);
    assert_eq!(after.pending_writes, 0);
    assert_eq!(after.pending_bytes, 0);
    assert_eq!(after.peak_pending_bytes, 0);
    assert_eq!(after.completed_writes, 0);
    assert_eq!(after.failed_writes, 0);
    assert_eq!(after.task_failed_writes, 0);
    assert_eq!(after.admission_waits, 0);
    assert_eq!(after.barrier_waits, 0);
}

#[test]
fn fuse_async_write_snapshot_reports_values_relative_to_reset_baseline() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "reset-fuse-async-write-baseline");

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 7,
        enabled: true,
        pending_writes: 0,
        pending_bytes: 0,
        peak_pending_bytes: 8192,
        completed_writes: 11,
        failed_writes: 2,
        task_failed_writes: 1,
        admission_waits: 4,
        admission_wait_total_ms: 5.0,
        barrier_waits: 6,
        barrier_wait_total_ms: 7.0,
        ..Default::default()
    });
    vfs.reset_diagnostics_observability();

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 7,
        enabled: true,
        peak_pending_bytes: 8192,
        completed_writes: 11,
        failed_writes: 2,
        task_failed_writes: 1,
        admission_waits: 4,
        admission_wait_total_ms: 5.0,
        barrier_waits: 6,
        barrier_wait_total_ms: 7.0,
        ..Default::default()
    });
    let baseline = vfs.write_path_observability_snapshot().fuse_async_write;
    assert_eq!(baseline.snapshot_generation, 7);
    assert!(baseline.enabled);
    assert_eq!(baseline.peak_pending_bytes, 0);
    assert_eq!(baseline.completed_writes, 0);
    assert_eq!(baseline.failed_writes, 0);
    assert_eq!(baseline.task_failed_writes, 0);
    assert_eq!(baseline.admission_waits, 0);
    assert_eq!(baseline.barrier_waits, 0);

    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: 8,
        enabled: true,
        pending_writes: 2,
        pending_bytes: 4096,
        peak_pending_bytes: 8192,
        completed_writes: 13,
        failed_writes: 3,
        task_failed_writes: 1,
        admission_waits: 5,
        admission_wait_total_ms: 6.5,
        barrier_waits: 9,
        barrier_wait_total_ms: 11.0,
        ..Default::default()
    });

    let after = vfs.write_path_observability_snapshot().fuse_async_write;
    assert_eq!(after.snapshot_generation, 8);
    assert!(after.enabled);
    assert_eq!(after.pending_writes, 2);
    assert_eq!(after.pending_bytes, 4096);
    assert_eq!(after.peak_pending_bytes, 4096);
    assert_eq!(after.completed_writes, 2);
    assert_eq!(after.failed_writes, 1);
    assert_eq!(after.task_failed_writes, 0);
    assert_eq!(after.admission_waits, 1);
    assert_eq!(after.admission_wait_total_ms, 1.5);
    assert_eq!(after.barrier_waits, 3);
    assert_eq!(after.barrier_wait_total_ms, 4.0);
}

#[test]
fn write_path_observability_reports_vfs_write_stage_breakdown() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-vfs-breakdown");

    vfs.note_vfs_write_call(4 * 1024 * 1024);
    vfs.record_vfs_write_create(Duration::from_millis(8));
    vfs.record_vfs_write_mkdir(Duration::from_millis(9));
    vfs.record_vfs_write_open_file(Duration::from_millis(10));
    vfs.record_vfs_write_setattr(Duration::from_millis(11));
    vfs.record_vfs_write_pre_drain(Duration::from_millis(1));
    vfs.record_vfs_write_admission_throttle(Duration::from_millis(2));
    vfs.record_vfs_write_file_session_wait(Duration::from_millis(4));
    vfs.record_vfs_write_record_writer_stack_state(Duration::from_millis(3));
    vfs.record_vfs_write_getattr_async(Duration::from_millis(5));
    vfs.note_vfs_write_dirty_attr_hit();
    vfs.note_vfs_write_open_attr_hit();
    vfs.note_vfs_write_active_open_overlay_fallback();
    vfs.record_vfs_write_materialize_completed(Duration::from_millis(7));
    vfs.record_vfs_write_materialize_for_limits(Duration::from_millis(2));
    vfs.record_vfs_write_spawn_deferred_materialization(Duration::from_millis(6));

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(snapshot.vfs_write_path.calls, 1);
    assert_eq!(snapshot.vfs_write_path.write_bytes, 4 * 1024 * 1024);
    assert_eq!(snapshot.vfs_write_path.create.calls, 1);
    assert_eq!(snapshot.vfs_write_path.mkdir.calls, 1);
    assert_eq!(snapshot.vfs_write_path.open_file.calls, 1);
    assert_eq!(snapshot.vfs_write_path.setattr.calls, 1);
    assert_eq!(snapshot.vfs_write_path.pre_drain.calls, 1);
    assert_eq!(snapshot.vfs_write_path.admission_throttle.calls, 1);
    assert_eq!(snapshot.vfs_write_path.file_session_wait.calls, 1);
    assert_eq!(snapshot.vfs_write_path.record_writer_stack_state.calls, 1);
    assert_eq!(snapshot.vfs_write_path.getattr_async.calls, 1);
    assert_eq!(snapshot.vfs_write_path.dirty_attr_hits, 1);
    assert_eq!(snapshot.vfs_write_path.open_attr_hits, 1);
    assert_eq!(snapshot.vfs_write_path.active_open_overlay_fallbacks, 1);
    assert_eq!(snapshot.vfs_write_path.materialize_completed.calls, 1);
    assert_eq!(snapshot.vfs_write_path.materialize_strict.calls, 0);
    assert_eq!(snapshot.vfs_write_path.materialize_for_limits.calls, 1);
    assert_eq!(
        snapshot.vfs_write_path.spawn_deferred_materialization.calls,
        1
    );
    assert!(snapshot.vfs_write_path.avg_write_bytes >= 4.0 * 1024.0 * 1024.0);
}

#[test]
fn winfsp_observability_snapshot_tracks_callback_and_barrier_paths() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "winfsp-observability-snapshot");

    vfs.note_winfsp_write_callback();
    vfs.note_winfsp_overwrite_callback();
    vfs.note_winfsp_cleanup_callback();
    vfs.note_winfsp_cleanup_flush_fh_callback();
    vfs.note_winfsp_flush_callback();
    vfs.note_winfsp_close_callback();
    vfs.note_winfsp_cleanup_barrier_outcome(true);
    vfs.note_winfsp_cleanup_barrier_outcome(false);
    vfs.note_winfsp_flush_barrier_outcome(true);
    vfs.note_winfsp_flush_barrier_outcome(false);
    vfs.note_winfsp_close_barrier_outcome(false);

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(snapshot.winfsp_callbacks.write, 1);
    assert_eq!(snapshot.winfsp_callbacks.overwrite, 1);
    assert_eq!(snapshot.winfsp_callbacks.cleanup, 1);
    assert_eq!(snapshot.winfsp_callbacks.cleanup_flush_fh, 1);
    assert_eq!(snapshot.winfsp_callbacks.flush, 1);
    assert_eq!(snapshot.winfsp_callbacks.close, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.cleanup, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.cleanup_skipped_clean, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.flush, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.flush_skipped_clean, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.close, 0);
    assert_eq!(snapshot.winfsp_foreground_flushes.close_skipped_clean, 1);
}

#[test]
fn failed_payload_persist_attempts_do_not_count_as_completed_bursts() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-failed-burst");

    vfs.record_payload_persist_observability(4, 16 * 1024, Duration::from_millis(3), false);

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.payload_persist.calls, 1,
        "payload stage latency should still include failed attempts"
    );
    assert_eq!(
        snapshot.payload_burst.bursts, 0,
        "failed payload persists must not count as completed bursts"
    );
    assert_eq!(
        snapshot.payload_burst.total_payloads, 0,
        "failed payload persists must not add completed burst payload counts"
    );
    assert_eq!(
        snapshot.payload_burst.total_bytes, 0,
        "failed payload persists must not add completed burst bytes"
    );
}

#[test]
fn local_cache_io_stats_track_handoff_and_backend_time_separately() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-io-stats");
    let calls_before = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let handoff_before = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);
    let backend_before = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.payload_store_put_async(0xAA55, Bytes::from_static(b"payload"), false)
            .await
            .expect("payload store put");
        let payload = vfs
            .payload_store_get_async(0xAA55)
            .await
            .expect("payload store get")
            .expect("cached payload");
        assert_eq!(payload.as_ref(), b"payload");
    });

    let calls_after = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let handoff_after = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);
    let backend_after = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);

    assert!(
        calls_after >= calls_before + 2,
        "put + get should record at least two local-cache I/O calls"
    );
    assert!(
        handoff_after >= handoff_before,
        "blocking-isolated local-cache operations may accrue scheduler handoff time"
    );
    assert!(
        backend_after >= backend_before,
        "backend execution time should be tracked independently"
    );

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.local_cache_io.execution_model,
        "inline-runtime-worker-with-blocking-isolation"
    );
}

#[test]
fn local_cache_io_stats_track_direct_partial_read_into() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-stats");
    let calls_before = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let backend_before = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);
    let handoff_before = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.payload_store_put_async(0xBB66, Bytes::from_static(b"payload"), false)
            .await
            .expect("payload store put");
        let handoff_before_read = vfs
            .state
            .metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        let mut out = [0_u8; 3];
        let read = vfs
            .payload_store_read_into_async(0xBB66, 2, &mut out)
            .await
            .expect("payload store read_into");
        assert_eq!(read, Some(3));
        assert_eq!(&out, b"ylo");
        let handoff_after_read = vfs
            .state
            .metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        assert_eq!(
            handoff_after_read, handoff_before_read,
            "direct read_into should stay on the runtime worker and avoid blocking-pool handoff"
        );
    });

    let calls_after = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let backend_after = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);
    let handoff_after = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);

    assert!(
        calls_after >= calls_before + 2,
        "put + read_into should record local-cache I/O calls"
    );
    assert!(
        backend_after >= backend_before,
        "direct read_into should contribute backend execution timing"
    );
    assert!(
        handoff_after >= handoff_before,
        "overall local-cache handoff accounting must remain monotonic"
    );
}

#[test]
fn local_cache_read_into_returns_none_for_missing_block() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-missing");
    rt.block_on(async {
        let mut out = [0_u8; 8];
        let read = vfs
            .payload_store_read_into_async(0xDEADBEEF, 0, &mut out)
            .await
            .expect("read_into should not error on cache miss");
        assert_eq!(read, None);
    });
}

#[test]
fn payload_store_get_prefers_hot_upload_buffer_before_disk() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-get-hot-upload");
    rt.block_on(async {
        vfs.cache_upload_hot_batch(vec![studiofs_cache::local::LocalPayloadPut {
            object_id: 0xABCD,
            data: Bytes::from_static(b"hot-payload"),
            evictable: false,
        }]);

        let payload = vfs
            .payload_store_get_async(0xABCD)
            .await
            .expect("payload store get from hot upload buffer")
            .expect("hot upload payload");
        assert_eq!(payload.as_ref(), b"hot-payload");
    });
}

#[test]
fn payload_store_read_into_prefers_hot_upload_buffer_before_disk() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-hot-upload");
    rt.block_on(async {
        vfs.cache_upload_hot_batch(vec![studiofs_cache::local::LocalPayloadPut {
            object_id: 0xBCDE,
            data: Bytes::from_static(b"hot-payload"),
            evictable: false,
        }]);

        let mut out = [0_u8; 7];
        let read = vfs
            .payload_store_read_into_async(0xBCDE, 4, &mut out)
            .await
            .expect("payload store read_into from hot upload buffer");
        assert_eq!(read, Some(7));
        assert_eq!(&out, b"payload");
    });
}

#[test]
fn committed_copy_rejects_short_payload() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "committed-copy-short-payload");
    let block = PayloadRef::new_full(42, vfs.block_size);
    let mut dst = [0_u8; 8];
    let err = vfs
        .copy_committed_payload_into_range(b"abcd", &block, 0, &mut dst)
        .expect_err("short committed payload should error");
    let message = match err {
        VfsError::Other(message) => message,
        other => panic!("expected VfsError::Other, got {other}"),
    };
    assert!(
        message.contains("committed payload object too short"),
        "error message should highlight short committed payload"
    );
}

#[test]
fn histogram_percentile_marks_overflow_bucket_as_saturated() {
    let snapshot = studiofs_cache::metrics::LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: vec![1_000_000, 2_000_000],
        bucket_counts: vec![1, 1, 3],
    };
    let (value_ns, saturated) = StudioVfs::histogram_percentile_ns(&snapshot, 0.99);
    assert_eq!(value_ns, 2_000_000);
    assert!(saturated);
}

#[test]
fn histogram_percentile_non_overflow_bucket_is_not_saturated() {
    let snapshot = studiofs_cache::metrics::LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: vec![1_000_000, 2_000_000],
        bucket_counts: vec![1, 2, 0],
    };
    let (value_ns, saturated) = StudioVfs::histogram_percentile_ns(&snapshot, 0.95);
    assert_eq!(value_ns, 2_000_000);
    assert!(!saturated);
}

#[test]
fn build_cadence_stats_averages_over_completed_intervals() {
    let histogram = studiofs_cache::metrics::LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: vec![10_000_000, 20_000_000],
        bucket_counts: vec![1, 0, 0],
    };

    let stats = StudioVfs::build_cadence_stats(
        3,
        10_000_000,
        10_000_000,
        histogram,
        Duration::from_secs(1),
    );

    assert_eq!(
        stats.avg_interval_ms, 5.0,
        "cadence averages should divide by completed intervals (events - 1), not raw histogram sample count"
    );
}

fn foreign_exclusive_lock(ino: Ino) -> FileLockLease {
    FileLockLease {
        ino,
        kind: HardLockKind::Exclusive,
        generation: 1,
        lease_expires_at_unix_ms: i64::MAX,
        exclusive_owner: Some("client-slot:foreign".to_string()),
        shared_owners: BTreeSet::new(),
    }
}

#[test]
fn foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-open");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-open.txt", 0, 0, 0o644)
        .expect("create file");
    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    let blocked = vfs.open_file(attr.ino, OpenFlags::RDWR, false);
    assert!(
        matches!(blocked, Err(VfsError::WouldBlock)),
        "new write-open should block once a foreign hard lock is known"
    );

    let fh = vfs
        .open_file(attr.ino, OpenFlags::RDONLY, false)
        .expect("read-only open should still succeed under a foreign hard lock");
    let open_file = vfs.get_open_file(fh).expect("tracked open file");
    assert_eq!(open_file.flags.bits(), OpenFlags::RDONLY.bits());
}

#[test]
fn existing_writer_handle_remains_usable_after_foreign_hard_lock_becomes_known() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-grandfather");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-grandfather.txt", 0, 0, 0o644)
        .expect("create file");
    let fh = vfs
        .open_file(attr.ino, OpenFlags::RDWR, false)
        .expect("open writer before lock becomes known");

    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    rt.block_on(async {
        vfs.write_fh(fh, attr.ino, 0, b"abc")
            .await
            .expect("pre-lock writer handle should be allowed to finish");
    });

    assert_eq!(vfs.getattr(attr.ino).expect("getattr").size, 3);
}

#[test]
fn foreign_hard_lock_blocks_size_change_setattr_without_handle() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-setattr");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-setattr.txt", 0, 0, 0o644)
        .expect("create file");
    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    let result = vfs.setattr(attr.ino, SetattrArgs::size_only(1));
    assert!(
        matches!(result, Err(VfsError::WouldBlock)),
        "inode-scoped size changes should block when a foreign hard lock is known"
    );
}

#[test]
fn foreign_hard_lock_blocks_unlink_and_rename() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-namespace");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-namespace.txt", 0, 0, 0o644)
        .expect("create file");
    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    let unlink = vfs.unlink(ROOT_INO, b"foreign-lock-namespace.txt");
    assert!(
        matches!(unlink, Err(VfsError::WouldBlock)),
        "unlink should block when the target inode is hard-locked by a peer"
    );

    let rename = vfs.rename(
        ROOT_INO,
        b"foreign-lock-namespace.txt",
        ROOT_INO,
        b"foreign-lock-namespace-moved.txt",
    );
    assert!(
        matches!(rename, Err(VfsError::WouldBlock)),
        "rename should block when the source inode is hard-locked by a peer"
    );
}

#[test]
fn sfs623_read_only_allocator_blocks_new_mutations_but_keeps_reads_open() {
    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "offline-read-only-authority");
    let attr = vfs
        .create(ROOT_INO, b"offline-read-only-authority.txt", 0, 0, 0o644)
        .expect("create file");
    let fh = vfs
        .open_file(attr.ino, OpenFlags::RDWR, false)
        .expect("open writer before authority degrades");
    let payload = b"known-readable-before-read-only";

    rt.block_on(async {
        let written = vfs
            .write_fh(fh, attr.ino, 0, payload)
            .await
            .expect("write known readable payload before authority degrades");
        assert_eq!(written as usize, payload.len());
        vfs.flush_fh(fh)
            .await
            .expect("flush known readable payload before authority degrades");
    });

    vfs.set_metadata_authority_status_for_test(Some(MetadataAuthorityStatus::read_only()));

    let blocked_open = vfs.open_file(attr.ino, OpenFlags::RDWR, false);
    assert!(
        matches!(
            blocked_open,
            Err(VfsError::Meta(MetaError::AccessDenied {
                actual: AccessLevel::ReadOnly,
                ..
            }))
        ),
        "new write-capable opens should stop once metadata authority degrades to read-only"
    );

    let ro_fh = vfs
        .open_file(attr.ino, OpenFlags::RDONLY, false)
        .expect("read-only open should remain available after authority downgrade");
    let ro_open = vfs.get_open_file(ro_fh).expect("tracked read-only open");
    assert_eq!(ro_open.flags.bits(), OpenFlags::RDONLY.bits());

    rt.block_on(async {
        let readable = vfs
            .read(attr.ino, 0, payload.len() as u32)
            .await
            .expect("known readable state should stay readable after authority downgrade");
        assert_eq!(readable.as_ref(), payload);

        let write_error = vfs
            .write_fh(fh, attr.ino, 0, b"abc")
            .await
            .expect_err("existing writer handles should also become read-only");
        assert!(
            matches!(
                write_error,
                VfsError::Meta(MetaError::AccessDenied {
                    actual: AccessLevel::ReadOnly,
                    ..
                })
            ),
            "unexpected write error after authority downgrade: {write_error}"
        );
    });

    let create_error = vfs.create(ROOT_INO, b"blocked-after-downgrade.txt", 0, 0, 0o644);
    assert!(
        matches!(
            create_error,
            Err(VfsError::Meta(MetaError::AccessDenied {
                actual: AccessLevel::ReadOnly,
                ..
            }))
        ),
        "new namespace mutations should stop once metadata authority degrades to read-only"
    );
}
