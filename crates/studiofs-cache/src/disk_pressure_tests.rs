use super::*;

use crate::local::{LocalPayloadBackend, LocalPayloadLifecycleState, LocalPayloadOrigin};

const TOTAL_BYTES: u64 = 1_000;
const LOW_BYTES: u64 = 99;
const RECOVERED_BYTES: u64 = 110;
const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

fn backend() -> (tempfile::TempDir, LocalPayloadBackendRuntime) {
    let temp = studiofs_test_support::workspace_target_tempdir("disk-pressure-");
    let layout = LocalPayloadLayout::from_cache_root(temp.path());
    layout.provision().unwrap();
    let backend = LocalPayloadBackendRuntime::open_with_budget(
        LocalPayloadBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x87; 16],
        },
        Some(1 << 30),
    )
    .unwrap();
    (temp, backend)
}

fn space(available_bytes: u64, total_bytes: u64) -> platform::FilesystemSpace {
    platform::FilesystemSpace {
        available_bytes,
        total_bytes,
    }
}

fn pressure(backend: &LocalPayloadBackendRuntime, now: Instant) {
    backend
        .run_disk_pressure_maintenance(now, |_| Ok(space(LOW_BYTES, TOTAL_BYTES)))
        .unwrap();
}

#[test]
fn foreground_payload_operations_never_query_native_filesystem_space() {
    let (_temp, backend) = backend();
    platform::FILESYSTEM_SPACE_QUERIES.with(|calls| calls.set(0));

    backend.put(1, b"cached", true).unwrap();
    assert_eq!(backend.get(1).unwrap().as_ref(), b"cached");
    let _ = backend.storage_stats();
    backend.pin(1);
    backend.unpin(1);
    let _ = backend.physical_space_status();

    platform::FILESYSTEM_SPACE_QUERIES.with(|calls| assert_eq!(calls.get(), 0));
    backend.run_pressure_maintenance().unwrap();
    platform::FILESYSTEM_SPACE_QUERIES.with(|calls| assert!(calls.get() >= 1));
    let before = platform::FILESYSTEM_SPACE_QUERIES.with(|calls| calls.get());
    backend.run_pressure_maintenance().unwrap();
    backend.put(2, b"more", true).unwrap();
    let _ = backend.get(2);
    platform::FILESYSTEM_SPACE_QUERIES.with(|calls| assert_eq!(calls.get(), before));
}

#[cfg(any(windows, target_os = "macos"))]
#[test]
fn active_reader_finishes_after_physical_eviction_removes_its_cached_file() {
    let (_temp, backend) = backend();
    let backend = Arc::new(backend);
    let expected = vec![0x87; 4096];
    backend.put(1, &expected, true).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let reader_backend = Arc::clone(&backend);
    let reader = std::thread::spawn(move || {
        platform::PREAD_TEST_GATE.with(|gate| {
            *gate.borrow_mut() = Some((entered_tx, release_rx));
        });
        let mut out = vec![0; 4096];
        assert_eq!(reader_backend.read_into(1, 0, &mut out), Some(out.len()));
        out
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    pressure(&backend, Instant::now());
    assert!(!backend.contains(1));
    release_tx.send(()).unwrap();
    assert_eq!(reader.join().unwrap(), expected);
}

#[test]
fn invalid_samples_preserve_payloads_and_remain_rate_limited() {
    for invalid in [space(0, 0), space(2, 1)] {
        let (_temp, backend) = backend();
        backend.put(1, b"keep", true).unwrap();
        let now = Instant::now();
        backend
            .run_disk_pressure_maintenance(now, |_| Ok(invalid))
            .unwrap();
        backend
            .run_disk_pressure_maintenance(now, |_| panic!("invalid sample must cool down"))
            .unwrap();
        assert!(backend.contains(1));
        assert_eq!(backend.physical_space_status().outcome, "sampling_failed");
    }
    assert_eq!(percent_ceil(u64::MAX, 10), 1_844_674_407_370_955_162);
}

#[test]
fn physical_maintenance_yields_to_the_existing_eviction_owner() {
    let (_temp, backend) = backend();
    let owner = backend.eviction_pass_lock.lock();
    let now = Instant::now();
    backend
        .run_disk_pressure_maintenance(now, |_| panic!("another eviction owns the cache"))
        .unwrap();
    drop(owner);
    backend
        .run_disk_pressure_maintenance(now, |_| Ok(space(100, TOTAL_BYTES)))
        .unwrap();
    assert_eq!(backend.physical_space_status().outcome, "healthy");
}

#[test]
fn external_low_space_reclaims_downloaded_payload_and_stops_at_recovery_threshold() {
    let (_temp, backend) = backend();
    backend.put(1, &[1; 32], true).unwrap();
    let now = Instant::now();
    let mut samples = 0;

    backend
        .run_disk_pressure_maintenance(now, |_| {
            samples += 1;
            Ok(space(
                if samples == 1 {
                    LOW_BYTES
                } else {
                    RECOVERED_BYTES
                },
                TOTAL_BYTES,
            ))
        })
        .unwrap();

    assert_eq!(
        samples, 2,
        "reclaim must be followed by a physical-space resample"
    );
    assert!(!backend.contains(1));
    let status = backend.physical_space_status();
    assert_eq!(status.available_bytes, Some(RECOVERED_BYTES));
    assert_eq!(status.reserve_bytes, Some(100));
    assert_eq!(status.reclaimed_payload_bytes, 32);
    backend.put(2, b"new cached data", true).unwrap();
    backend
        .run_disk_pressure_maintenance(now + SAMPLE_INTERVAL, |_| Ok(space(105, TOTAL_BYTES)))
        .unwrap();
    assert!(
        backend.contains(2),
        "recovered cache must stop reclaiming above 10%"
    );
}

#[test]
fn post_reclaim_sampling_failure_retains_last_sample_and_cools_down() {
    let (_temp, backend) = backend();
    backend.put(1, b"cached", true).unwrap();
    let now = Instant::now();
    let mut calls = 0;
    backend
        .run_disk_pressure_maintenance(now, |_| {
            calls += 1;
            if calls == 1 {
                Ok(space(LOW_BYTES, TOTAL_BYTES))
            } else {
                Err(io::Error::other("resample failed"))
            }
        })
        .unwrap();
    backend
        .run_disk_pressure_maintenance(now, |_| panic!("must cool down after resample failure"))
        .unwrap();
    let status = backend.physical_space_status();
    assert_eq!(status.outcome, "sampling_failed");
    assert_eq!(status.available_bytes, Some(LOW_BYTES));
    assert_eq!(status.reclaimed_payload_bytes, 6);
    assert_eq!(calls, 2);
}

#[test]
fn failed_eviction_preserves_indexed_payload_and_cools_down() {
    let (_temp, backend) = backend();
    backend.put(1, b"cached", true).unwrap();
    backend.fail_next_index_write_for_test();
    let now = Instant::now();
    assert!(
        backend
            .run_disk_pressure_maintenance(now, |_| Ok(space(LOW_BYTES, TOTAL_BYTES)))
            .is_err()
    );
    assert_eq!(backend.physical_space_status().outcome, "eviction_failed");
    assert_eq!(backend.get(1).unwrap().as_ref(), b"cached");
    backend
        .run_disk_pressure_maintenance(now, |_| panic!("failed eviction must cool down"))
        .unwrap();
}

#[test]
fn ten_percent_is_healthy_and_space_below_it_enters_pressure() {
    let (_temp, backend) = backend();
    backend.put(1, b"keep", true).unwrap();
    let now = Instant::now();

    backend
        .run_disk_pressure_maintenance(now, |_| Ok(space(100, TOTAL_BYTES)))
        .unwrap();
    assert!(backend.contains(1));

    pressure(&backend, now + SAMPLE_INTERVAL);
    assert!(!backend.contains(1));
}

#[test]
fn dirty_uploading_local_origin_and_pinned_payloads_are_protected() {
    let (_temp, backend) = backend();
    backend.put(1, b"dirty", false).unwrap();
    backend.put(2, b"uploading", false).unwrap();
    backend.mark_uploading(2).unwrap();
    backend.put(3, b"local-uploaded", false).unwrap();
    backend.mark_uploaded(3).unwrap();
    backend.put(4, b"pinned", true).unwrap();
    backend.pin(4);
    backend.put(5, b"evictable", true).unwrap();

    pressure(&backend, Instant::now());

    assert!(backend.contains(1));
    assert!(backend.contains(2));
    assert!(backend.contains(3));
    assert!(backend.contains(4));
    assert!(!backend.contains(5));
    assert_eq!(
        backend.object_stats(3).unwrap().origin,
        LocalPayloadOrigin::Local
    );
    assert_eq!(
        backend.lifecycle_state(2),
        LocalPayloadLifecycleState::Uploading
    );
}

#[test]
fn failed_sample_is_reported_and_cooled_down_for_thirty_seconds() {
    let (_temp, backend) = backend();
    let now = Instant::now();
    let mut calls = 0;
    let mut fail = |_: &Path| {
        calls += 1;
        Err(io::Error::other("injected sampler failure"))
    };

    backend
        .run_disk_pressure_maintenance(now, &mut fail)
        .unwrap();
    backend
        .run_disk_pressure_maintenance(now + SAMPLE_INTERVAL - Duration::from_secs(1), &mut fail)
        .unwrap();

    assert_eq!(calls, 1);
    assert_eq!(backend.physical_space_status().outcome, "sampling_failed");
}

#[test]
fn scan_cursor_reaches_candidates_after_a_full_protected_page() {
    let (_temp, backend) = backend();
    let protected = (1..=512)
        .map(|object_id| CacheIndexUpsert {
            object_id,
            payload_key: object_id.to_le_bytes().to_vec(),
            payload_len: 1,
            lifecycle: LocalPayloadLifecycleState::Dirty,
            origin: LocalPayloadOrigin::Local,
            upload_state: LocalPayloadUploadState::Pending,
            evictable: false,
        })
        .collect::<Vec<_>>();
    backend.index.upsert_many(&protected).unwrap();
    backend.put(513, b"after protected page", true).unwrap();
    let first = Instant::now();

    pressure(&backend, first);
    assert!(backend.contains(513));

    pressure(&backend, first + SAMPLE_INTERVAL);
    assert!(!backend.contains(513));
}

#[test]
fn one_pass_removes_at_most_128_payloads() {
    let (_temp, backend) = backend();
    for object_id in 1..=129 {
        backend.put(object_id, b"x", true).unwrap();
    }
    let now = Instant::now();

    backend
        .run_disk_pressure_maintenance(now, |_| Ok(space(0, 1_u64 << 40)))
        .unwrap();

    assert_eq!(backend.object_ids().len(), 1);
    assert_eq!(backend.physical_space_status().reclaimed_payload_bytes, 128);
}

#[test]
fn one_pass_stops_near_64_mib_even_when_more_reclamation_is_needed() {
    let (_temp, backend) = backend();
    let payload_sizes = [40 * 1024 * 1024, 40 * 1024 * 1024];
    for (object_id, payload_bytes) in (1..=2).zip(payload_sizes) {
        backend.put(object_id, b"x", true).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(backend.layout.payload_object_path(object_id))
            .unwrap()
            .set_len(payload_bytes)
            .unwrap();
        backend
            .index
            .upsert(CacheIndexUpsert {
                object_id,
                payload_key: backend
                    .layout
                    .payload_object_relative_path(object_id)
                    .to_string_lossy()
                    .into_owned()
                    .into_bytes(),
                payload_len: payload_bytes,
                lifecycle: LocalPayloadLifecycleState::Uploaded,
                origin: LocalPayloadOrigin::Downloaded,
                upload_state: LocalPayloadUploadState::None,
                evictable: true,
            })
            .unwrap();
    }

    backend
        .run_disk_pressure_maintenance(Instant::now(), |_| Ok(space(0, 1_u64 << 40)))
        .unwrap();

    assert_eq!(backend.object_ids().len(), 1);
    assert_eq!(
        backend.physical_space_status().reclaimed_payload_bytes,
        payload_sizes[0]
    );
}

#[test]
fn each_cache_samples_the_shared_volume_independently() {
    let (_temp_a, backend_a) = backend();
    let (_temp_b, backend_b) = backend();
    backend_a.put(1, b"a", true).unwrap();
    backend_b.put(2, b"b", true).unwrap();
    let now = Instant::now();
    let mut samples = 0;
    let mut shared_volume = |_: &Path| {
        samples += 1;
        Ok(space(LOW_BYTES, TOTAL_BYTES))
    };

    backend_a
        .run_disk_pressure_maintenance(now, &mut shared_volume)
        .unwrap();
    backend_b
        .run_disk_pressure_maintenance(now, &mut shared_volume)
        .unwrap();

    assert_eq!(
        samples, 4,
        "each backend samples before and after its own reclaim"
    );
    assert!(!backend_a.contains(1));
    assert!(!backend_b.contains(2));
}

#[test]
fn repeated_pressure_passes_observe_space_without_assuming_unlink_freed_it() {
    let (_temp, backend) = backend();
    for object_id in 1..=4 {
        backend.put(object_id, &[object_id as u8; 8], true).unwrap();
    }
    let first = Instant::now();
    let mut calls = 0;
    let mut still_full = |_: &Path| {
        calls += 1;
        Ok(space(LOW_BYTES, TOTAL_BYTES))
    };

    backend
        .run_disk_pressure_maintenance(first, &mut still_full)
        .unwrap();
    let after_first_pass = backend.object_ids().len();
    assert!(after_first_pass < 4);
    assert_eq!(
        backend.physical_space_status().available_bytes,
        Some(LOW_BYTES)
    );

    backend
        .run_disk_pressure_maintenance(first + SAMPLE_INTERVAL, &mut still_full)
        .unwrap();
    assert!(backend.object_ids().len() < after_first_pass);
    assert_eq!(
        backend.physical_space_status().available_bytes,
        Some(LOW_BYTES)
    );
    assert!(
        calls >= 4,
        "each nonempty reclaim pass resamples the filesystem"
    );
}
