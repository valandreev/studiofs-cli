//! Opt-in native metadata measurements; no timing thresholds or cache policy changes.
#![cfg(windows)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use windows::Win32::Storage::FileSystem::GetLogicalDrives;

const CALLBACKS: [&str; 6] = [
    "get_security_by_name",
    "open",
    "get_file_info",
    "read_directory",
    "cleanup",
    "close",
];
const HOT_PATH_EVENTS: [&str; 7] = [
    "path_resolution",
    "metadata_lookup",
    "metadata_getattr",
    "acl_check",
    "readdir",
    "get_attrs_batch",
    "directory_buffer",
];
const WINFSP_HOT_PATH_EVENTS: [&str; 2] = ["path_resolution", "directory_buffer"];
const VFS_HOT_PATH_EVENTS: [&str; 5] = [
    "metadata_lookup",
    "metadata_getattr",
    "acl_check",
    "readdir",
    "get_attrs_batch",
];
const EVENT_COUNT: usize = CALLBACKS.len() + HOT_PATH_EVENTS.len();

#[derive(Clone, Default)]
struct CallbackCounter(Arc<[AtomicU64; EVENT_COUNT]>);

impl CallbackCounter {
    fn event_index(target: &str) -> Option<usize> {
        if let Some(name) = target.strip_prefix("studiofs_winfsp::metadata_baseline::") {
            if let Some(index) = CALLBACKS.iter().position(|callback| *callback == name) {
                return Some(index);
            }
            if WINFSP_HOT_PATH_EVENTS.contains(&name) {
                return HOT_PATH_EVENTS
                    .iter()
                    .position(|event| *event == name)
                    .map(|index| CALLBACKS.len() + index);
            }
            return None;
        }
        if let Some(name) = target.strip_prefix("studiofs_vfs::metadata_baseline::")
            && VFS_HOT_PATH_EVENTS.contains(&name)
        {
            return HOT_PATH_EVENTS
                .iter()
                .position(|event| *event == name)
                .map(|index| CALLBACKS.len() + index);
        }
        None
    }

    fn count_for(snapshot: &[u64; EVENT_COUNT], target: &str) -> u64 {
        Self::event_index(target).map_or(0, |index| snapshot[index])
    }

    fn snapshot(&self) -> [u64; EVENT_COUNT] {
        std::array::from_fn(|index| self.0[index].load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::CallbackCounter;

    #[test]
    fn callback_counter_classifies_vfs_hot_path_events() {
        assert_eq!(
            CallbackCounter::event_index(
                "studiofs_winfsp::metadata_baseline::get_security_by_name"
            ),
            Some(0)
        );
        assert_eq!(
            CallbackCounter::event_index("studiofs_winfsp::metadata_baseline::path_resolution"),
            Some(6)
        );
        assert_eq!(
            CallbackCounter::event_index("studiofs_vfs::metadata_baseline::metadata_lookup"),
            Some(7)
        );
        assert_eq!(
            CallbackCounter::event_index("studiofs_winfsp::metadata_baseline::directory_buffer"),
            Some(12)
        );
        assert_eq!(
            CallbackCounter::event_index("studiofs_vfs::metadata_baseline::directory_buffer"),
            None
        );
        assert_eq!(
            CallbackCounter::event_index("studiofs_winfsp::metadata_baseline::metadata_lookup"),
            None
        );
        assert_eq!(
            CallbackCounter::event_index("studiofs_vfs::metadata_baseline::unrelated"),
            None
        );
    }
}

impl tracing::Subscriber for CallbackCounter {
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if Self::event_index(metadata.target()).is_some() {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        Self::event_index(metadata.target()).is_some()
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if let Some(index) = Self::event_index(event.metadata().target()) {
            self.0[index].fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[test]
#[ignore = "requires opt-in and a free WinFSP drive; records native metadata baseline"]
fn winfsp_metadata_baseline() {
    let configured_value = |key: &str| {
        std::env::var(key)
            .ok()
            .or_else(|| super::workspace_dotenv_var(key))
    };
    if configured_value("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Some("1") {
        eprintln!(
            "Skipping WinFSP metadata baseline: set STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"
        );
        return;
    }
    let mount_point = match configured_value("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Some(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP metadata baseline: set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)"
            );
            return;
        }
    };
    let bytes = mount_point.as_bytes();
    assert!(
        bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':',
        "mount point must be a drive letter"
    );
    // GetLogicalDrives includes assigned but currently inaccessible drives.
    // SAFETY: The typed Win32 binding has no parameters and returns the process drive mask.
    let drives = unsafe { GetLogicalDrives() };
    assert_ne!(drives, 0, "GetLogicalDrives failed");
    assert_eq!(
        drives & (1 << (bytes[0].to_ascii_uppercase() - b'A')),
        0,
        "requested drive is already assigned"
    );

    let counter = CallbackCounter::default();
    // This integration binary invokes the baseline once per process. Keep the
    // installation fail-fast so an existing subscriber cannot hide zero counts.
    // Native callbacks run on dispatcher threads, so a thread-local subscriber
    // cannot observe the workload.
    tracing::subscriber::set_global_default(counter.clone()).expect("install callback counter");
    let tmp = studiofs_test_support::workspace_target_tempdir("metadata-baseline-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let format_info = super::test_format_info_without_loading_env("metadata_baseline");
    let vfs = super::make_test_vfs_with_format_info(&tmp, format_info, &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!("Local\\StudioFsMetadataBaseline-{}", std::process::id());
    let report = super::with_winfsp_mount(vfs, rt, mount_point, event_name, |drive| {
        let directory = std::path::Path::new(drive).join("baseline");
        std::fs::create_dir(&directory).expect("create baseline directory");
        for index in 0..64 {
            drop(
                std::fs::File::create(directory.join(format!("file-{index:02}")))
                    .expect("seed empty file"),
            );
        }
        let existing = directory.join("file-00");
        let missing = directory.join("missing");
        let expected_names: Vec<std::ffi::OsString> = (0..64)
            .map(|index| format!("file-{index:02}").into())
            .collect();
        let mut report = String::from(
            "pass,workload,operations,elapsed_ns,warmup,get_security_by_name,open,get_file_info,read_directory,cleanup,close,path_resolution,metadata_lookup,metadata_getattr,acl_check,readdir,get_attrs_batch,directory_buffer,dentry_cache_hits,dentry_cache_misses,path_shortcut_cache_hits,path_shortcut_cache_misses,attr_read_cache_hits,attr_read_cache_misses,native_cache_enabled_open,native_cache_disabled_open\n",
        );
        for pass in 1..=3 {
            for workload in ["stat", "open_close", "negative_lookup", "enumeration"] {
                let before = counter.snapshot();
                let before_cache = vfs_observer.cache_state_snapshot();
                let before_write = vfs_observer.write_path_observability_snapshot();
                let started = Instant::now();
                for _ in 0..1000 {
                    match workload {
                        "stat" => {
                            let metadata = std::fs::metadata(&existing).expect("stat existing");
                            assert!(metadata.is_file());
                            assert_eq!(metadata.len(), 0);
                        }
                        "open_close" => {
                            drop(std::fs::File::open(&existing).expect("open existing"))
                        }
                        "negative_lookup" => assert_eq!(
                            std::fs::metadata(&missing)
                                .expect_err("missing path")
                                .kind(),
                            std::io::ErrorKind::NotFound
                        ),
                        "enumeration" => {
                            let mut entries: Vec<_> = std::fs::read_dir(&directory)
                                .expect("enumerate")
                                .map(|entry| entry.expect("directory entry").file_name())
                                .collect();
                            entries.sort();
                            assert_eq!(entries, expected_names);
                        }
                        _ => unreachable!(),
                    }
                }
                let elapsed = started.elapsed().as_nanos();
                let after = counter.snapshot();
                let delta: [u64; EVENT_COUNT] =
                    std::array::from_fn(|index| after[index].saturating_sub(before[index]));
                let after_cache = vfs_observer.cache_state_snapshot();
                let after_write = vfs_observer.write_path_observability_snapshot();
                use std::fmt::Write;
                let mut row = vec![
                    pass.to_string(),
                    workload.to_string(),
                    "1000".to_string(),
                    elapsed.to_string(),
                    "false".to_string(),
                ];
                row.extend(delta.iter().map(|value| value.to_string()));
                row.extend(
                    [
                        after_cache
                            .dentry_cache
                            .hits
                            .saturating_sub(before_cache.dentry_cache.hits),
                        after_cache
                            .dentry_cache
                            .misses
                            .saturating_sub(before_cache.dentry_cache.misses),
                        after_cache
                            .path_shortcut_cache
                            .hits
                            .saturating_sub(before_cache.path_shortcut_cache.hits),
                        after_cache
                            .path_shortcut_cache
                            .misses
                            .saturating_sub(before_cache.path_shortcut_cache.misses),
                        after_cache
                            .attr_read_cache
                            .hits
                            .saturating_sub(before_cache.attr_read_cache.hits),
                        after_cache
                            .attr_read_cache
                            .misses
                            .saturating_sub(before_cache.attr_read_cache.misses),
                        after_write
                            .winfsp_callbacks
                            .os_cache_enabled_open
                            .saturating_sub(before_write.winfsp_callbacks.os_cache_enabled_open),
                        after_write
                            .winfsp_callbacks
                            .os_cache_disabled_open
                            .saturating_sub(before_write.winfsp_callbacks.os_cache_disabled_open),
                    ]
                    .iter()
                    .map(|value| value.to_string()),
                );
                writeln!(report, "{}", row.join(",")).expect("format row");
            }
        }
        let counts = counter.snapshot();
        let count = |target| CallbackCounter::count_for(&counts, target);
        assert!(
            count("studiofs_winfsp::metadata_baseline::get_security_by_name") > 0,
            "security-by-name instrumentation must be active"
        );
        assert!(
            count("studiofs_winfsp::metadata_baseline::open") >= 3000,
            "open instrumentation must observe repeated opens"
        );
        assert!(
            count("studiofs_winfsp::metadata_baseline::read_directory") > 0,
            "directory instrumentation must be active"
        );
        assert!(
            count("studiofs_winfsp::metadata_baseline::path_resolution") > 0,
            "path-resolution instrumentation must be active"
        );
        assert!(
            count("studiofs_vfs::metadata_baseline::metadata_lookup") > 0,
            "metadata-lookup instrumentation must be active"
        );
        assert!(
            count("studiofs_vfs::metadata_baseline::metadata_getattr") > 0,
            "metadata-getattr instrumentation must be active"
        );
        assert!(
            count("studiofs_vfs::metadata_baseline::acl_check") > 0,
            "ACL instrumentation must be active"
        );
        assert!(
            count("studiofs_vfs::metadata_baseline::readdir") > 0,
            "readdir instrumentation must be active"
        );
        assert!(
            count("studiofs_vfs::metadata_baseline::get_attrs_batch") > 0,
            "batched-attribute instrumentation must be active"
        );
        assert!(
            count("studiofs_winfsp::metadata_baseline::directory_buffer") > 0,
            "directory-buffer instrumentation must be active"
        );
        report
    });
    let directory = super::workspace_root().join("target/perf");
    std::fs::create_dir_all(&directory).expect("create report directory");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let path = directory.join(format!(
        "sfs-690-metadata-{timestamp}-{}.csv",
        std::process::id()
    ));
    std::fs::write(&path, &report).expect("retain raw baseline report");
    print!("{report}");
    println!("metadata_baseline_report={}", path.display());
}
