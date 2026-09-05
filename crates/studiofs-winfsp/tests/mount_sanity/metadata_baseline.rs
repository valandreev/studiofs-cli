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

#[derive(Clone, Default)]
struct CallbackCounter(Arc<[AtomicU64; 6]>);

impl CallbackCounter {
    fn snapshot(&self) -> [u64; 6] {
        std::array::from_fn(|index| self.0[index].load(Ordering::Relaxed))
    }
}

impl tracing::Subscriber for CallbackCounter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata
            .target()
            .starts_with("studiofs_winfsp::metadata_baseline::")
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if let Some(name) = event
            .metadata()
            .target()
            .strip_prefix("studiofs_winfsp::metadata_baseline::")
            && let Some(index) = CALLBACKS.iter().position(|callback| *callback == name)
        {
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
            "pass,workload,operations,elapsed_ns,get_security_by_name,open,get_file_info,read_directory,cleanup,close\n",
        );
        for pass in 1..=3 {
            for workload in ["stat", "open_close", "negative_lookup", "enumeration"] {
                let before = counter.snapshot();
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
                let delta: [u64; 6] = std::array::from_fn(|index| after[index] - before[index]);
                use std::fmt::Write;
                writeln!(
                    report,
                    "{pass},{workload},1000,{elapsed},{},{},{},{},{},{}",
                    delta[0], delta[1], delta[2], delta[3], delta[4], delta[5]
                )
                .expect("format row");
            }
        }
        let counts = counter.snapshot();
        assert!(
            counts[0] > 0,
            "security-by-name instrumentation must be active"
        );
        assert!(
            counts[1] >= 3000,
            "open instrumentation must observe repeated opens"
        );
        assert!(counts[3] > 0, "directory instrumentation must be active");
        report
    });
    let directory = super::workspace_root().join("target/perf");
    std::fs::create_dir_all(&directory).expect("create report directory");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let path = directory.join(format!(
        "sfs-675-metadata-{timestamp}-{}.csv",
        std::process::id()
    ));
    std::fs::write(&path, &report).expect("retain raw baseline report");
    print!("{report}");
    println!("metadata_baseline_report={}", path.display());
}
