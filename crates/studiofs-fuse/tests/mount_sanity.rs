//! Sanity-level FUSE test coverage for mount lifecycle and metadata edge cases.

#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::process::ExitStatus;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::native::dispatcher::NativeDispatcher;
use studiofs_fuse::native::lifecycle::{
    DirectMountOptions, FuseDevice, Fusermount3MountSystem, LibcMountSyscalls, LinuxMountSystem,
    NativeMountLifecycle,
};
use studiofs_fuse::native::protocol::FuseCapabilities;
use studiofs_fuse::native::session::{NativeFuseSession, NativeSessionConfig};
use studiofs_fuse::native::transport::{
    ClassicReadWriteTransport, HybridFuseTransport, IoUringBackend, LinuxNativeFuseTransport,
    LinuxNativeTransportSelection, io_uring_payload_len_for_session,
    linux_fuse_io_uring_queue_count,
};
use studiofs_fuse::native::vfs_adapter::{
    LinuxNativeBridge, NativeAsyncWriteConfig, StudioVfsNativeAdapter,
};
use studiofs_fuse::{StudioFuseFs, StudioFuseRuntimeOptions};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};

const MOUNT_TESTS_ENV: &str = "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS";
const DEFAULT_TEST_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_TEST_MEMORY_BYTES: u64 = 32 * 1024 * 1024;
const SFS509_CACHE_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;
const SFS509_DEFAULT_FIO_SIZE: &str = "16m";
const SFS509_DEFAULT_FIO_LARGE_SIZE: &str = "96m";
const SFS509_DEFAULT_FIO_RUNTIME_SECS: &str = "1";
const SFS509_DEFAULT_METADATA_FILES: &str = "50";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountedFuseMode {
    Sync,
    AsyncWrite,
    AsyncWriteDio,
    AsyncWritebackCache,
}

impl MountedFuseMode {
    fn name(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::AsyncWrite => "async-write",
            Self::AsyncWriteDio => "async-write+dio",
            Self::AsyncWritebackCache => "async-write+writeback-cache",
        }
    }

    fn async_write_enabled(self) -> bool {
        matches!(
            self,
            Self::AsyncWrite | Self::AsyncWriteDio | Self::AsyncWritebackCache
        )
    }

    fn async_dio_enabled(self) -> bool {
        matches!(self, Self::AsyncWriteDio)
    }

    fn writeback_cache_enabled(self) -> bool {
        matches!(self, Self::AsyncWritebackCache)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MountedScenarioOperation {
    SequentialWriteReadback,
    RandomWriteReadback,
    TruncateReadback,
    OverwriteReadback,
    WriteFlushReadback,
    WriteFsyncReadback,
    CloseReopenReadback,
    ErrorPropagation,
    MultipleWriters,
    SameHandleAppends,
    FioMultiJob,
    Sfs509FioMatrix,
    Sfs530EvidenceMatrix,
    WriteOnlyUntilUnmount,
    DirectoryLifecycle,
    NamespaceMutations,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountedBackend {
    Fuser,
    LinuxNativeBridge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MountedAsyncScenario {
    name: &'static str,
    backend: MountedBackend,
    mode: MountedFuseMode,
    operation: MountedScenarioOperation,
}

#[derive(Default)]
struct MountedOperationGuard {
    _open_files: Vec<std::fs::File>,
}

macro_rules! declare_mounted_scenarios {
    (
        $(
            $test_name:ident => {
                name: $name:literal,
                mode: $mode:expr,
                operation: $operation:expr
            }
        ),+ $(,)?
    ) => {
        fn mounted_async_scenarios() -> &'static [MountedAsyncScenario] {
            &[
                $(
                    MountedAsyncScenario {
                        name: $name,
                        backend: MountedBackend::Fuser,
                        mode: $mode,
                        operation: $operation,
                    },
                )+
            ]
        }

        $(
            #[test]
            #[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
            fn $test_name() {
                run_mounted_scenario(MountedAsyncScenario {
                    name: $name,
                    backend: MountedBackend::Fuser,
                    mode: $mode,
                    operation: $operation,
                });
            }
        )+
    };
}

fn native_mounted_scenarios() -> &'static [MountedAsyncScenario] {
    &[
        MountedAsyncScenario {
            name: "native-directory-lifecycle",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::Sync,
            operation: MountedScenarioOperation::DirectoryLifecycle,
        },
        MountedAsyncScenario {
            name: "native-namespace-mutations",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::Sync,
            operation: MountedScenarioOperation::NamespaceMutations,
        },
        MountedAsyncScenario {
            name: "native-sequential-write-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::Sync,
            operation: MountedScenarioOperation::SequentialWriteReadback,
        },
        MountedAsyncScenario {
            name: "native-async-sequential-write-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::SequentialWriteReadback,
        },
        MountedAsyncScenario {
            name: "native-async-random-write-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::RandomWriteReadback,
        },
        MountedAsyncScenario {
            name: "native-async-truncate-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::TruncateReadback,
        },
        MountedAsyncScenario {
            name: "native-async-overwrite-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::OverwriteReadback,
        },
        MountedAsyncScenario {
            name: "native-async-write-flush-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::WriteFlushReadback,
        },
        MountedAsyncScenario {
            name: "native-async-write-fsync-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::WriteFsyncReadback,
        },
        MountedAsyncScenario {
            name: "native-async-close-reopen-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::CloseReopenReadback,
        },
        MountedAsyncScenario {
            name: "native-async-error-propagation",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::ErrorPropagation,
        },
        MountedAsyncScenario {
            name: "native-async-writeback-cache-sequential-write-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWritebackCache,
            operation: MountedScenarioOperation::SequentialWriteReadback,
        },
        MountedAsyncScenario {
            name: "native-async-writeback-cache-write-fsync-readback",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWritebackCache,
            operation: MountedScenarioOperation::WriteFsyncReadback,
        },
        MountedAsyncScenario {
            name: "native-sfs530-async-evidence",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWrite,
            operation: MountedScenarioOperation::Sfs530EvidenceMatrix,
        },
        MountedAsyncScenario {
            name: "native-sfs530-writeback-cache-evidence",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWritebackCache,
            operation: MountedScenarioOperation::Sfs530EvidenceMatrix,
        },
        MountedAsyncScenario {
            name: "native-sfs532-classic-conservative-fio",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::Sync,
            operation: MountedScenarioOperation::Sfs509FioMatrix,
        },
        MountedAsyncScenario {
            name: "native-sfs532-classic-high-throughput-fio",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWritebackCache,
            operation: MountedScenarioOperation::Sfs509FioMatrix,
        },
        MountedAsyncScenario {
            name: "native-sfs532-io-uring-fio",
            backend: MountedBackend::LinuxNativeBridge,
            mode: MountedFuseMode::AsyncWritebackCache,
            operation: MountedScenarioOperation::Sfs509FioMatrix,
        },
    ]
}

fn native_mounted_scenario(name: &str) -> MountedAsyncScenario {
    native_mounted_scenarios()
        .iter()
        .copied()
        .find(|scenario| scenario.name == name)
        .expect("native mounted scenario should exist")
}

declare_mounted_scenarios! {
    async_mounted_sequential_write_readback => {
        name: "sequential-write-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    async_mounted_random_write_readback => {
        name: "random-write-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::RandomWriteReadback
    },
    async_mounted_truncate_readback => {
        name: "truncate-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::TruncateReadback
    },
    async_mounted_overwrite_readback => {
        name: "overwrite-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::OverwriteReadback
    },
    async_mounted_write_flush_readback => {
        name: "write-flush-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::WriteFlushReadback
    },
    async_mounted_write_fsync_readback => {
        name: "write-fsync-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::WriteFsyncReadback
    },
    async_mounted_close_reopen_readback => {
        name: "close-reopen-readback",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::CloseReopenReadback
    },
    async_mounted_error_propagation => {
        name: "error-propagation",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::ErrorPropagation
    },
    async_mounted_multiple_writers => {
        name: "multiple-writers",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::MultipleWriters
    },
    async_mounted_same_handle_appends => {
        name: "same-handle-appends",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::SameHandleAppends
    },
    async_mounted_unmount_after_async_write => {
        name: "unmount-after-async-write",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::WriteOnlyUntilUnmount
    },
    linux_async_mounted_default_sync => {
        name: "linux-default-sync",
        mode: MountedFuseMode::Sync,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    linux_async_mounted_async_opt_in => {
        name: "linux-async-opt-in",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    linux_async_mounted_async_dio_opt_in => {
        name: "linux-async-dio-opt-in",
        mode: MountedFuseMode::AsyncWriteDio,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    linux_async_mounted_fio_multi_job => {
        name: "linux-fio-multi-job",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::FioMultiJob
    },
    linux_sfs509_mounted_fio_default_sync => {
        name: "sfs-509-long-lived-fio-default-sync",
        mode: MountedFuseMode::Sync,
        operation: MountedScenarioOperation::Sfs509FioMatrix
    },
    linux_sfs509_mounted_fio_async_opt_in => {
        name: "sfs-509-long-lived-fio-async-opt-in",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::Sfs509FioMatrix
    },
    linux_sfs509_mounted_fio_async_dio_opt_in => {
        name: "sfs-509-long-lived-fio-async-dio-opt-in",
        mode: MountedFuseMode::AsyncWriteDio,
        operation: MountedScenarioOperation::Sfs509FioMatrix
    },
    linux_sfs509_mounted_fio_background_queue => {
        name: "sfs-509-long-lived-fio-background-queue",
        mode: MountedFuseMode::AsyncWrite,
        operation: MountedScenarioOperation::Sfs509FioMatrix
    },
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_mounted_directory_lifecycle() {
    run_native_mounted_scenario(native_mounted_scenarios()[0]);
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_mounted_namespace_mutations() {
    run_native_mounted_scenario(native_mounted_scenarios()[1]);
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_mounted_sequential_write_readback() {
    run_native_mounted_scenario(native_mounted_scenarios()[2]);
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_sequential_write_readback() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-async-sequential-write-readback",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_random_write_readback() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-async-random-write-readback",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_truncate_readback() {
    run_native_mounted_scenario(native_mounted_scenario("native-async-truncate-readback"));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_overwrite_readback() {
    run_native_mounted_scenario(native_mounted_scenario("native-async-overwrite-readback"));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_write_flush_readback() {
    run_native_mounted_scenario(native_mounted_scenario("native-async-write-flush-readback"));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_write_fsync_readback() {
    run_native_mounted_scenario(native_mounted_scenario("native-async-write-fsync-readback"));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_close_reopen_readback() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-async-close-reopen-readback",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_mounted_error_propagation() {
    run_native_mounted_scenario(native_mounted_scenario("native-async-error-propagation"));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_writeback_cache_mounted_sequential_write_readback() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-async-writeback-cache-sequential-write-readback",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_async_writeback_cache_mounted_write_fsync_readback() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-async-writeback-cache-write-fsync-readback",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, fio, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_sfs530_mounted_evidence_async_profile() {
    run_native_mounted_scenario(native_mounted_scenario("native-sfs530-async-evidence"));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, fio, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_sfs530_mounted_evidence_writeback_cache_profile() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-sfs530-writeback-cache-evidence",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, fio, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_sfs532_mounted_fio_classic_conservative_profile() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-sfs532-classic-conservative-fio",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, fio, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_sfs532_mounted_fio_classic_high_throughput_profile() {
    run_native_mounted_scenario(native_mounted_scenario(
        "native-sfs532-classic-high-throughput-fio",
    ));
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, fio, kernel FUSE-over-io_uring, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_sfs532_mounted_fio_io_uring_profile() {
    run_native_mounted_scenario(native_mounted_scenario("native-sfs532-io-uring-fio"));
}

#[test]
fn async_mounted_test_matrix_covers_correctness_and_platform_opt_in_scenarios() {
    let scenarios = mounted_async_scenarios();
    let names: BTreeSet<_> = scenarios.iter().map(|scenario| scenario.name).collect();
    let modes: BTreeSet<_> = scenarios
        .iter()
        .map(|scenario| scenario.mode.name())
        .collect();

    for expected in [
        "sequential-write-readback",
        "random-write-readback",
        "truncate-readback",
        "overwrite-readback",
        "write-flush-readback",
        "write-fsync-readback",
        "close-reopen-readback",
        "error-propagation",
        "multiple-writers",
        "same-handle-appends",
        "linux-default-sync",
        "linux-async-opt-in",
        "linux-async-dio-opt-in",
        "linux-fio-multi-job",
        "sfs-509-long-lived-fio-default-sync",
        "sfs-509-long-lived-fio-async-opt-in",
        "sfs-509-long-lived-fio-async-dio-opt-in",
        "sfs-509-long-lived-fio-background-queue",
        "unmount-after-async-write",
    ] {
        assert!(
            names.contains(expected),
            "mounted async FUSE scenario matrix is missing {expected}"
        );
    }

    for expected in ["sync", "async-write", "async-write+dio"] {
        assert!(
            modes.contains(expected),
            "mounted async FUSE scenario matrix is missing mode {expected}"
        );
    }
}

#[test]
fn native_mounted_matrix_includes_sfs530_async_write_correctness_profiles() {
    let scenarios = native_mounted_scenarios();
    let names: BTreeSet<_> = scenarios.iter().map(|scenario| scenario.name).collect();
    let operations: BTreeSet<_> = scenarios
        .iter()
        .map(|scenario| scenario.operation)
        .collect();
    let modes: BTreeSet<_> = scenarios
        .iter()
        .map(|scenario| scenario.mode.name())
        .collect();

    for expected in [
        "native-async-sequential-write-readback",
        "native-async-random-write-readback",
        "native-async-truncate-readback",
        "native-async-overwrite-readback",
        "native-async-write-flush-readback",
        "native-async-write-fsync-readback",
        "native-async-close-reopen-readback",
        "native-async-error-propagation",
        "native-async-writeback-cache-sequential-write-readback",
        "native-async-writeback-cache-write-fsync-readback",
    ] {
        assert!(
            names.contains(expected),
            "native SFS-530 mounted matrix is missing {expected}"
        );
    }

    for expected in [
        MountedScenarioOperation::SequentialWriteReadback,
        MountedScenarioOperation::RandomWriteReadback,
        MountedScenarioOperation::TruncateReadback,
        MountedScenarioOperation::OverwriteReadback,
        MountedScenarioOperation::WriteFlushReadback,
        MountedScenarioOperation::WriteFsyncReadback,
        MountedScenarioOperation::CloseReopenReadback,
        MountedScenarioOperation::ErrorPropagation,
    ] {
        assert!(
            operations.contains(&expected),
            "native SFS-530 mounted matrix is missing operation {expected:?}"
        );
    }

    for expected in ["async-write", "async-write+writeback-cache"] {
        assert!(
            modes.contains(expected),
            "native SFS-530 mounted matrix is missing mode {expected}"
        );
    }
}

#[test]
fn native_mounted_matrix_includes_sfs530_evidence_profiles() {
    let scenarios = native_mounted_scenarios();
    let names: BTreeSet<_> = scenarios.iter().map(|scenario| scenario.name).collect();
    let evidence_profiles: BTreeSet<_> = scenarios
        .iter()
        .filter(|scenario| scenario.operation == MountedScenarioOperation::Sfs530EvidenceMatrix)
        .map(|scenario| scenario.mode.name())
        .collect();

    for expected in [
        "native-sfs530-async-evidence",
        "native-sfs530-writeback-cache-evidence",
    ] {
        assert!(
            names.contains(expected),
            "native SFS-530 evidence matrix is missing {expected}"
        );
    }

    for expected in ["async-write", "async-write+writeback-cache"] {
        assert!(
            evidence_profiles.contains(expected),
            "native SFS-530 evidence matrix is missing profile {expected}"
        );
    }
}

#[test]
fn native_mounted_matrix_includes_sfs532_performance_gate_profiles() {
    let scenarios = native_mounted_scenarios();
    let names: BTreeSet<_> = scenarios.iter().map(|scenario| scenario.name).collect();

    for expected in [
        "native-sfs532-classic-conservative-fio",
        "native-sfs532-classic-high-throughput-fio",
        "native-sfs532-io-uring-fio",
    ] {
        assert!(
            names.contains(expected),
            "native SFS-532 performance matrix is missing {expected}"
        );
    }
}

#[test]
fn docs_list_sfs530_native_evidence_command() {
    let docs = include_str!("../../../docs/tests.md");

    assert!(
        docs.contains("native_sfs530_mounted_evidence"),
        "docs/tests.md must list the SFS-530 native mounted evidence command"
    );
    assert!(
        docs.contains("write-path-observability-after.json"),
        "docs/tests.md must list the SFS-530 write-path telemetry artifact"
    );
}

#[test]
fn docs_list_sfs533_native_io_uring_smoke_command() {
    let docs = include_str!("../../../docs/tests.md");

    assert!(
        docs.contains("native_fusermount3_io_uring_session_init_and_unmount_smoke"),
        "docs/tests.md must list the SFS-533 native io_uring smoke command"
    );
}

#[test]
fn sfs530_telemetry_snapshot_exports_native_local_cache_fallback_counters() {
    let source = include_str!("mount_sanity.rs");
    let writer = source
        .rsplit("fn write_sfs530_telemetry_snapshot")
        .next()
        .and_then(|rest| {
            rest.split("fn run_sfs530_evidence_matrix_if_available")
                .next()
        })
        .expect("SFS-530 telemetry writer should be present");

    assert!(writer.contains("local_cache_unavailable_fallbacks"));
    assert!(writer.contains("stale_epoch_fallbacks"));
    assert!(writer.contains("native_bridge.local_cache_unavailable_fallbacks"));
    assert!(writer.contains("native_bridge.stale_epoch_fallbacks"));
}

#[test]
fn mounted_tests_enabled_falls_back_to_workspace_dotenv_value() {
    assert!(mounted_tests_enabled_from_values(None, Some("1")));
}

#[test]
fn mounted_tests_enabled_prefers_process_env_value() {
    assert!(!mounted_tests_enabled_from_values(Some("0"), Some("1")));
}

#[test]
fn mounted_tests_enabled_treats_empty_process_env_as_unset() {
    assert!(mounted_tests_enabled_from_values(Some(""), Some("1")));
}

#[test]
fn parse_fio_size_bytes_accepts_binary_suffixes() {
    assert_eq!(parse_fio_size_bytes("2g"), Some(2 * 1024 * 1024 * 1024));
}

#[test]
fn parse_fio_size_bytes_avoids_lowercase_allocation() {
    let source = include_str!("mount_sanity.rs");
    let parser = source
        .rsplit("fn parse_fio_size_bytes(value: &str) -> Option<u64> {")
        .next()
        .and_then(|rest| rest.split("fn sfs509_cache_budget_bytes").next())
        .expect("fio size parser should be present");

    assert!(!parser.contains("to_ascii_lowercase"));
    assert!(parser.contains("eq_ignore_ascii_case"));
}

#[test]
fn sfs509_cache_budget_covers_large_file_workload() {
    let budget = sfs509_cache_budget_bytes("1536m");

    assert!(budget > 1536 * 1024 * 1024);
}

#[test]
fn sfs509_default_fio_size_is_bounded_for_mounted_smoke_runtime() {
    assert_eq!(SFS509_DEFAULT_FIO_SIZE, "16m");
}

#[test]
fn sfs509_multi_job_size_is_capped_for_mounted_smoke_runtime() {
    assert_eq!(sfs509_multi_job_size("512m"), "2m");
}

#[test]
fn sfs509_fsync_heavy_size_is_capped_for_mounted_smoke_runtime() {
    assert_eq!(sfs509_fsync_heavy_size("512m"), "2m");
}

#[test]
fn sfs509_default_large_file_size_is_bounded_for_mounted_smoke_runtime() {
    assert_eq!(SFS509_DEFAULT_FIO_LARGE_SIZE, "96m");
}

#[test]
fn sfs509_default_timed_runtime_is_bounded_for_mounted_smoke_runtime() {
    assert_eq!(SFS509_DEFAULT_FIO_RUNTIME_SECS, "1");
}

#[test]
fn sfs509_default_metadata_probe_is_bounded_for_mounted_smoke_runtime() {
    assert_eq!(SFS509_DEFAULT_METADATA_FILES, "50");
}

#[test]
fn native_sfs530_mounted_scenarios_are_exposed_as_opt_in_tests() {
    let source = include_str!("mount_sanity.rs");

    for expected in [
        "native_async_mounted_sequential_write_readback",
        "native_async_mounted_random_write_readback",
        "native_async_mounted_truncate_readback",
        "native_async_mounted_overwrite_readback",
        "native_async_mounted_write_flush_readback",
        "native_async_mounted_write_fsync_readback",
        "native_async_mounted_close_reopen_readback",
        "native_async_mounted_error_propagation",
        "native_async_writeback_cache_mounted_sequential_write_readback",
        "native_sfs530_mounted_evidence_async_profile",
        "native_sfs530_mounted_evidence_writeback_cache_profile",
    ] {
        assert!(
            source
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("fn {expected}()"))),
            "native SFS-530 scenario must be exposed as opt-in mounted test: {expected}"
        );
    }
}

#[test]
fn runtime_options_from_env_is_public_for_embedder_overrides() {
    let mut options = StudioFuseRuntimeOptions::from_env();

    options.async_write_enabled = true;

    assert!(options.async_write_enabled);
}

#[test]
fn async_mounted_matrix_includes_unmount_teardown_scenario() {
    let scenarios = mounted_async_scenarios();
    let unmount = scenarios
        .iter()
        .find(|scenario| scenario.name == "unmount-after-async-write")
        .expect("unmount-after-async-write scenario should be present");

    assert_eq!(
        unmount.operation,
        MountedScenarioOperation::WriteOnlyUntilUnmount,
        "unmount-after-async-write must leave write teardown to unmount"
    );
}

#[test]
fn async_mounted_matrix_includes_sfs509_long_lived_fio_scenario() {
    let scenarios = mounted_async_scenarios();
    let names: BTreeSet<_> = scenarios
        .iter()
        .filter(|scenario| scenario.operation == MountedScenarioOperation::Sfs509FioMatrix)
        .map(|scenario| scenario.name)
        .collect();

    for expected in [
        "sfs-509-long-lived-fio-default-sync",
        "sfs-509-long-lived-fio-async-opt-in",
        "sfs-509-long-lived-fio-async-dio-opt-in",
        "sfs-509-long-lived-fio-background-queue",
    ] {
        assert!(
            names.contains(expected),
            "SFS-509 long-lived fio matrix is missing {expected}"
        );
    }
}

#[test]
fn native_mount_smoke_is_available_as_sfs525_opt_in_gate() {
    let source = include_str!("mount_sanity.rs");

    assert!(
        source.contains("native_mount_session_mount_and_unmount_smoke"),
        "SFS-525 native mount smoke must stay available behind the opt-in mounted test gate"
    );
}

#[test]
fn native_io_uring_mount_smoke_is_available_as_sfs533_opt_in_gate() {
    let source = include_str!("mount_sanity.rs");

    assert!(
        source.contains("native_fusermount3_io_uring_session_init_and_unmount_smoke"),
        "SFS-533 native io_uring mount smoke must stay available behind the opt-in mounted test gate"
    );
}

#[test]
fn native_io_uring_mount_smoke_uses_drop_guard_for_panic_cleanup() {
    let source = include_str!("mount_sanity.rs");
    let smoke = source
        .rsplit("fn native_fusermount3_io_uring_session_init_and_unmount_smoke")
        .next()
        .and_then(|rest| rest.split("fn finish_mounted_fuse_test").next())
        .expect("SFS-533 native io_uring smoke should be present");

    assert!(smoke.contains("struct MountGuard"));
    assert!(smoke.contains("impl Drop for MountGuard"));
    assert!(smoke.contains("force_unmount_fuse_mount_path(mount_path)"));
    assert!(smoke.contains("mount_guard.disarm()"));
}

#[test]
fn native_mounted_session_uses_runtime_linux_native_transport_selection() {
    let source = include_str!("mount_sanity.rs");
    let native_mount_runner = source
        .split("fn with_mounted_native_fuse")
        .last()
        .and_then(|rest| rest.split("fn run_mounted_scenario").next())
        .expect("native mounted runner should be present");

    assert!(native_mount_runner.contains("runtime_options.linux_native_transport"));
    assert!(native_mount_runner.contains("LinuxNativeFuseTransport::Hybrid"));
    assert!(native_mount_runner.contains("LinuxNativeTransportSelection::IoUring"));
    assert!(native_mount_runner.contains("with_io_uring_transport"));
    assert!(native_mount_runner.contains("linux_fuse_io_uring_queue_count()"));
    assert!(native_mount_runner.contains("io_uring_payload_len_for_session"));
}

#[test]
fn native_mounted_matrix_runs_existing_correctness_operations_through_linux_native_bridge() {
    let scenarios = native_mounted_scenarios();
    let names: BTreeSet<_> = scenarios.iter().map(|scenario| scenario.name).collect();
    let operations: BTreeSet<_> = scenarios
        .iter()
        .map(|scenario| scenario.operation)
        .collect();

    assert!(
        scenarios
            .iter()
            .all(|scenario| scenario.backend == MountedBackend::LinuxNativeBridge),
        "native mounted scenarios must run through LinuxNativeBridge"
    );
    for expected in [
        "native-directory-lifecycle",
        "native-namespace-mutations",
        "native-sequential-write-readback",
    ] {
        assert!(
            names.contains(expected),
            "native mounted matrix is missing {expected}"
        );
    }
    for expected in [
        MountedScenarioOperation::DirectoryLifecycle,
        MountedScenarioOperation::NamespaceMutations,
        MountedScenarioOperation::SequentialWriteReadback,
    ] {
        assert!(
            operations.contains(&expected),
            "native mounted matrix is missing {expected:?}"
        );
    }
}

#[test]
fn command_path_is_available_only_when_executable() {
    let tmp = target_tempdir("command-availability-");
    let command_path = tmp.path().join("fio");
    std::fs::write(&command_path, b"not executable").expect("write fake command");

    std::fs::set_permissions(&command_path, std::fs::Permissions::from_mode(0o644))
        .expect("set non-executable permissions");
    let mode = std::fs::metadata(&command_path)
        .expect("fake command metadata")
        .permissions()
        .mode();
    if mode & 0o111 != 0 {
        eprintln!(
            "skipping command executable-bit check because the test filesystem reports mode {mode:o} after chmod 644"
        );
        return;
    }

    assert!(
        !command_path_is_executable(&command_path),
        "plain files in PATH must not be treated as executable commands"
    );

    std::fs::set_permissions(&command_path, std::fs::Permissions::from_mode(0o755))
        .expect("set executable permissions");
    assert!(command_path_is_executable(&command_path));
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn target_tempdir(prefix: &str) -> tempfile::TempDir {
    let temp_root = workspace_root()
        .join("target")
        .join("studiofs-fuse-mount-tests");
    std::fs::create_dir_all(&temp_root).expect("create target-backed test temp root");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(temp_root)
        .expect("target-backed tempdir")
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
        encryption: studiofs_meta::EncryptionConfig::new(
            [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        ),
        block_size: 4 * 1024 * 1024,
    }
}

fn make_test_vfs(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
) -> Arc<StudioVfs> {
    make_test_vfs_with_cache_budget(
        tmp,
        volume_suffix,
        rt,
        DEFAULT_TEST_CACHE_BYTES,
        DEFAULT_TEST_MEMORY_BYTES,
    )
}

fn make_test_vfs_with_cache_budget(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
    cache_capacity_bytes: u64,
    memory_capacity_bytes: u64,
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
        cache_capacity_bytes,
        memory_capacity_bytes,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs background workers");
    vfs
}

fn parse_fio_size_bytes(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let suffix_start = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(suffix_start);
    if digits.is_empty() {
        return None;
    }
    let amount = digits.parse::<u64>().ok()?;
    let suffix = suffix.trim();
    let multiplier = if suffix.is_empty() || suffix.eq_ignore_ascii_case("b") {
        1
    } else if suffix.eq_ignore_ascii_case("k")
        || suffix.eq_ignore_ascii_case("kb")
        || suffix.eq_ignore_ascii_case("kib")
    {
        1024
    } else if suffix.eq_ignore_ascii_case("m")
        || suffix.eq_ignore_ascii_case("mb")
        || suffix.eq_ignore_ascii_case("mib")
    {
        1024 * 1024
    } else if suffix.eq_ignore_ascii_case("g")
        || suffix.eq_ignore_ascii_case("gb")
        || suffix.eq_ignore_ascii_case("gib")
    {
        1024 * 1024 * 1024
    } else {
        return None;
    };

    amount.checked_mul(multiplier)
}

fn sfs509_cache_budget_bytes(large_size: &str) -> u64 {
    parse_fio_size_bytes(large_size)
        .and_then(|bytes| bytes.checked_add(SFS509_CACHE_HEADROOM_BYTES))
        .unwrap_or(DEFAULT_TEST_CACHE_BYTES)
        .max(DEFAULT_TEST_CACHE_BYTES)
}

fn mounted_scenario_cache_budget_bytes(suffix: &str) -> u64 {
    if suffix.contains("sfs-509-long-lived-fio") {
        let large_size = sfs509_env_value("STUDIOFS_SFS509_FIO_LARGE_SIZE", "2g");
        return sfs509_cache_budget_bytes(&large_size);
    }

    DEFAULT_TEST_CACHE_BYTES
}

fn sfs509_multi_job_size(base_size: &str) -> String {
    sfs509_capped_size(base_size, 2 * 1024 * 1024, "2m")
}

fn sfs509_fsync_heavy_size(base_size: &str) -> String {
    sfs509_capped_size(base_size, 2 * 1024 * 1024, "2m")
}

fn sfs509_capped_size(base_size: &str, max_bytes: u64, capped_size: &str) -> String {
    match parse_fio_size_bytes(base_size) {
        Some(bytes) if bytes <= max_bytes => base_size.to_string(),
        Some(_) => capped_size.to_string(),
        None => base_size.to_string(),
    }
}

fn mounted_tests_enabled() -> bool {
    load_test_env();
    mounted_tests_enabled_from_values(
        std::env::var(MOUNT_TESTS_ENV).ok().as_deref(),
        workspace_dotenv_var(MOUNT_TESTS_ENV).as_deref(),
    )
}

fn mounted_tests_enabled_from_values(process_env: Option<&str>, dotenv_env: Option<&str>) -> bool {
    process_env
        .filter(|value| !value.trim().is_empty())
        .or(dotenv_env)
        .is_some_and(|value| value == "1")
}

fn current_uid_gid() -> (libc::uid_t, libc::gid_t) {
    // SAFETY: `getuid` and `getgid` do not require preconditions and return
    // the process credentials.
    unsafe { (libc::getuid(), libc::getgid()) }
}

fn status_has_effective_capability(status: &str, capability: u32) -> bool {
    match status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|hex| u64::from_str_radix(hex.trim(), 16).ok())
    {
        Some(mask) => match 1_u64.checked_shl(capability) {
            Some(shift) => mask & shift != 0,
            None => false,
        },
        None => false,
    }
}

fn process_has_effective_cap_sys_admin() -> bool {
    std::fs::read_to_string("/proc/self/status")
        .map(|status| status_has_effective_capability(&status, 21))
        .unwrap_or(false)
}

fn mount_path_device(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).expect("mount path metadata").dev()
}

fn native_direct_mount_options() -> DirectMountOptions {
    DirectMountOptions {
        source: "studiofs".into(),
        fs_name: "studiofs-native-smoke".into(),
        subtype: Some("studiofs".into()),
        read_only: false,
        allow_other: false,
        default_permissions: false,
    }
}

#[test]
fn status_capability_parser_detects_effective_cap_sys_admin() {
    assert!(status_has_effective_capability(
        "Name:\ttest\nCapEff:\t0000000000200000\n",
        21
    ));
    assert!(!status_has_effective_capability(
        "Name:\ttest\nCapEff:\t0000000000000000\n",
        21
    ));
}

#[test]
fn status_capability_parser_rejects_out_of_range_capability_index() {
    assert!(!status_has_effective_capability(
        "Name:\ttest\nCapEff:\tffffffffffffffff\n",
        64
    ));
}

#[test]
#[ignore = "requires /dev/fuse, direct mount permissions, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_mount_session_mount_and_unmount_smoke() {
    if !mounted_tests_enabled() {
        eprintln!("skipping native mount smoke because {MOUNT_TESTS_ENV}=1 is not set");
        return;
    }
    if !process_has_effective_cap_sys_admin() {
        eprintln!(
            "skipping native direct mount smoke because this process lacks effective CAP_SYS_ADMIN"
        );
        return;
    }

    let tmp = target_tempdir("native-mount-smoke-");
    let mount_path = tmp.path().join("mnt");
    std::fs::create_dir(&mount_path).expect("create native mount point");
    let before_dev = mount_path_device(&mount_path);
    let fuse = FuseDevice::open_default().expect("open /dev/fuse for native smoke");
    let (uid, gid) = current_uid_gid();
    let options = native_direct_mount_options();
    let mut lifecycle = NativeMountLifecycle::new(LinuxMountSystem::new(LibcMountSyscalls));

    lifecycle
        .mount_direct(
            mount_path.to_str().expect("mount path should be utf-8"),
            std::os::fd::AsRawFd::as_raw_fd(&fuse),
            uid,
            gid,
            &options,
        )
        .expect("native direct FUSE mount should succeed");
    let after_dev = mount_path_device(&mount_path);
    assert_ne!(
        before_dev, after_dev,
        "native direct FUSE mount should change the mountpoint device"
    );

    lifecycle
        .unmount(mount_path.to_str().expect("mount path should be utf-8"))
        .expect("native direct FUSE unmount should succeed");
    assert!(
        wait_for_fuse_mount_path_to_clear(&mount_path, before_dev, Duration::from_secs(2)),
        "native direct FUSE mount path should clear after unmount"
    );
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_fusermount3_mount_and_unmount_smoke() {
    if !mounted_tests_enabled() {
        eprintln!("skipping native fusermount3 smoke because {MOUNT_TESTS_ENV}=1 is not set");
        return;
    }
    if Command::new("fusermount3").arg("-V").status().is_err() {
        eprintln!("skipping native fusermount3 smoke because fusermount3 is unavailable");
        return;
    }

    let tmp = target_tempdir("native-fusermount3-smoke-");
    let mount_path = tmp.path().join("mnt");
    std::fs::create_dir(&mount_path).expect("create native fusermount3 mount point");
    let options = native_direct_mount_options();
    let request = options
        .fusermount_request(mount_path.to_str().expect("mount path should be utf-8"))
        .expect("build native fusermount3 request");
    let system = Fusermount3MountSystem::default();

    let fuse = system
        .mount(&request)
        .expect("native fusermount3 FUSE mount should return a device fd");
    assert!(
        mountinfo_contains_path(&mount_path),
        "native fusermount3 FUSE mount should appear in mountinfo"
    );

    force_unmount_fuse_mount_path(&mount_path).expect("native fusermount3 unmount should succeed");
    drop(fuse);
    assert!(
        wait_for_mountinfo_path_to_clear(&mount_path, Duration::from_secs(2)),
        "native fusermount3 FUSE mount path should clear from mountinfo after unmount"
    );
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_fusermount3_classic_session_init_and_unmount_smoke() {
    if !mounted_tests_enabled() {
        eprintln!(
            "skipping native fusermount3 classic session smoke because {MOUNT_TESTS_ENV}=1 is not set"
        );
        return;
    }
    if Command::new("fusermount3").arg("-V").status().is_err() {
        eprintln!(
            "skipping native fusermount3 classic session smoke because fusermount3 is unavailable"
        );
        return;
    }

    let tmp = target_tempdir("native-fusermount3-session-smoke-");
    let vfs_tmp = target_tempdir("native-fusermount3-session-vfs-");
    let mount_path = tmp.path().join("mnt");
    std::fs::create_dir(&mount_path).expect("create native fusermount3 session mount point");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&vfs_tmp, "native-session-smoke", &rt);
    let options = native_direct_mount_options();
    let request = options
        .fusermount_request(mount_path.to_str().expect("mount path should be utf-8"))
        .expect("build native fusermount3 session request");
    let system = Fusermount3MountSystem::default();

    let fuse = system
        .mount(&request)
        .expect("native fusermount3 FUSE mount should return a device fd");
    assert!(
        mountinfo_contains_path(&mount_path),
        "native fusermount3 session mount should appear in mountinfo"
    );

    let (handshake_tx, handshake_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let adapter = StudioVfsNativeAdapter::new(Arc::clone(&vfs), rt.handle().clone());
    let session_thread = std::thread::spawn(move || {
        let transport = ClassicReadWriteTransport::new(fuse.into_file());
        let config = NativeSessionConfig::linux6_safe_from_env();
        let mut session = NativeFuseSession::new(transport, config);
        let handshake = session.handshake();
        let handshake_ok = handshake.is_ok();
        handshake_tx
            .send(handshake.map(|negotiated| negotiated.capabilities))
            .expect("send native handshake result");
        if !handshake_ok {
            return;
        }
        let transport = session.into_transport();
        let runtime_options = StudioFuseRuntimeOptions::from_env();
        let mut handler = LinuxNativeBridge::new(adapter);
        if let Some(config) = NativeAsyncWriteConfig::from_runtime_options(&runtime_options) {
            handler = handler.with_async_write_config(config);
        }
        let mut dispatcher = NativeDispatcher::new(transport, handler);
        let result = dispatcher.run_until_destroy();
        done_tx.send(result).expect("send native dispatcher result");
    });

    let negotiated_caps = handshake_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("native fusermount3 session should complete INIT handshake")
        .expect("native fusermount3 session INIT should succeed");
    assert!(
        negotiated_caps.contains(FuseCapabilities::ASYNC_READ),
        "native fusermount3 INIT should negotiate async read when offered by the kernel"
    );

    force_unmount_fuse_mount_path(&mount_path)
        .expect("native fusermount3 session unmount should succeed");
    let dispatch_result = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("native fusermount3 dispatcher should finish after unmount");
    dispatch_result.expect("native fusermount3 dispatcher should shut down cleanly");
    session_thread
        .join()
        .expect("native fusermount3 session thread should not panic");
    assert!(
        wait_for_mountinfo_path_to_clear(&mount_path, Duration::from_secs(2)),
        "native fusermount3 session mount path should clear from mountinfo after unmount"
    );
}

#[test]
#[ignore = "requires /dev/fuse, fusermount3, kernel FUSE-over-io_uring, and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn native_fusermount3_io_uring_session_init_and_unmount_smoke() {
    if !mounted_tests_enabled() {
        eprintln!(
            "skipping native fusermount3 io_uring smoke because {MOUNT_TESTS_ENV}=1 is not set"
        );
        return;
    }
    if Command::new("fusermount3").arg("-V").status().is_err() {
        eprintln!("skipping native fusermount3 io_uring smoke because fusermount3 is unavailable");
        return;
    }

    let tmp = target_tempdir("native-fusermount3-uring-session-smoke-");
    let vfs_tmp = target_tempdir("native-fusermount3-uring-session-vfs-");
    let mount_path = tmp.path().join("mnt");
    std::fs::create_dir(&mount_path).expect("create native io_uring session mount point");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&vfs_tmp, "native-uring-session-smoke", &rt);
    let options = native_direct_mount_options();
    let request = options
        .fusermount_request(mount_path.to_str().expect("mount path should be utf-8"))
        .expect("build native io_uring session request");
    let system = Fusermount3MountSystem::default();

    let fuse = system
        .mount(&request)
        .expect("native fusermount3 FUSE mount should return a device fd");
    struct MountGuard(Option<std::path::PathBuf>);
    impl MountGuard {
        fn disarm(&mut self) {
            self.0 = None;
        }
    }
    impl Drop for MountGuard {
        fn drop(&mut self) {
            if let Some(mount_path) = &self.0 {
                let _ = force_unmount_fuse_mount_path(mount_path);
            }
        }
    }
    let mut mount_guard = MountGuard(Some(mount_path.clone()));
    assert!(
        mountinfo_contains_path(&mount_path),
        "native fusermount3 io_uring session mount should appear in mountinfo"
    );

    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let adapter = StudioVfsNativeAdapter::new(Arc::clone(&vfs), rt.handle().clone());
    let session_thread = std::thread::spawn(move || {
        let fuse_file = fuse.into_file();
        let fuse_fd = std::os::fd::AsRawFd::as_raw_fd(&fuse_file);
        let transport = ClassicReadWriteTransport::new(fuse_file).with_transport_fd(fuse_fd);
        let config = NativeSessionConfig::linux6_safe_from_env().with_io_uring_transport();
        let session = NativeFuseSession::new(transport, config);
        let (negotiated, transport) = match session.handshake_into_transport() {
            Ok(result) => result,
            Err(error) => {
                ready_tx
                    .send(Err(format!("INIT handshake failed: {error}")))
                    .expect("send native io_uring handshake failure");
                return;
            }
        };
        if !negotiated
            .capabilities
            .contains(FuseCapabilities::OVER_IO_URING)
        {
            ready_tx
                .send(Ok(false))
                .expect("send native io_uring unsupported status");
            return;
        }
        let transport = match HybridFuseTransport::<_, IoUringBackend>::register_kernel_io_uring(
            transport,
            fuse_fd,
            linux_fuse_io_uring_queue_count(),
            io_uring_payload_len_for_session(negotiated.max_write, negotiated.max_pages),
        ) {
            Ok(transport) => transport,
            Err(error) => {
                ready_tx
                    .send(Err(format!("io_uring registration failed: {error}")))
                    .expect("send native io_uring registration failure");
                return;
            }
        };
        ready_tx
            .send(Ok(true))
            .expect("send native io_uring registration status");

        let runtime_options = StudioFuseRuntimeOptions::from_env();
        let mut handler = LinuxNativeBridge::new(adapter);
        if let Some(config) = NativeAsyncWriteConfig::from_runtime_options(&runtime_options) {
            handler = handler.with_async_write_config(config);
        }
        let mut dispatcher = NativeDispatcher::new(transport, handler);
        let result = dispatcher.run_until_destroy();
        done_tx
            .send(result)
            .expect("send native io_uring dispatcher result");
    });

    match ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("native io_uring session should report readiness")
    {
        Ok(true) => {}
        Ok(false) => {
            eprintln!("skipping native io_uring smoke because FUSE_CAP_OVER_IO_URING is missing");
            force_unmount_fuse_mount_path(&mount_path)
                .expect("native io_uring skip unmount should succeed");
            mount_guard.disarm();
            session_thread
                .join()
                .expect("native io_uring skipped session thread should not panic");
            return;
        }
        Err(error) => panic!("{error}"),
    }

    force_unmount_fuse_mount_path(&mount_path)
        .expect("native io_uring session unmount should succeed");
    mount_guard.disarm();
    let dispatch_result = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("native io_uring dispatcher should finish after unmount");
    dispatch_result.expect("native io_uring dispatcher should shut down cleanly");
    session_thread
        .join()
        .expect("native io_uring session thread should not panic");
    assert!(
        wait_for_mountinfo_path_to_clear(&mount_path, Duration::from_secs(2)),
        "native io_uring session mount path should clear from mountinfo after unmount"
    );
}

fn finish_mounted_fuse_test(
    test_result: std::thread::Result<MountedOperationGuard>,
    unmount_result: io::Result<()>,
    join_result: Option<std::thread::Result<io::Result<()>>>,
) {
    match test_result {
        Ok(_guard) => {
            if let Some(join_result) = join_result {
                match join_result {
                    Ok(run_result) => run_result.expect("FUSE session should run until unmount"),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            unmount_result.expect("FUSE unmount should succeed");
        }
        Err(panic) => {
            if let Err(error) = unmount_result {
                eprintln!("FUSE unmount failed after scenario panic: {error}");
            }
            if let Some(join_result) = join_result {
                match join_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        eprintln!("FUSE session failed after scenario panic: {error}");
                    }
                    Err(_) => {
                        eprintln!("FUSE session thread panicked after scenario panic");
                    }
                }
            }
            std::panic::resume_unwind(panic);
        }
    }
}

fn should_join_mounted_fuse_session(
    test_result: &std::thread::Result<MountedOperationGuard>,
    unmount_result: &io::Result<()>,
) -> bool {
    test_result.is_ok() && unmount_result.is_ok()
}

fn join_mounted_fuse_session_until(
    session_handle: std::thread::JoinHandle<io::Result<()>>,
    timeout: Duration,
) -> Option<std::thread::Result<io::Result<()>>> {
    let deadline = std::time::Instant::now() + timeout;
    while !session_handle.is_finished() {
        let now = std::time::Instant::now();
        if now >= deadline {
            eprintln!("FUSE session thread did not exit within {timeout:?}; skipping join");
            return None;
        }
        std::thread::sleep(join_poll_sleep_duration(deadline.duration_since(now)));
    }
    Some(session_handle.join())
}

fn join_poll_sleep_duration(remaining: Duration) -> Duration {
    remaining.min(Duration::from_millis(25))
}

fn fuse_mount_path_cleared(mount_path: &Path, pre_dev: u64) -> bool {
    std::fs::metadata(mount_path)
        .map(|metadata| {
            use std::os::unix::fs::MetadataExt;
            metadata.dev() == pre_dev
        })
        .unwrap_or_else(fuse_mount_metadata_error_is_cleared)
}

fn fuse_mount_metadata_error_is_cleared(error: io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
}

fn wait_for_fuse_mount_path_to_clear(mount_path: &Path, pre_dev: u64, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while !fuse_mount_path_cleared(mount_path, pre_dev) {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    true
}

fn mountinfo_contains_path(mount_path: &Path) -> bool {
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    let mount_path = mount_path.to_string_lossy();
    mountinfo.lines().any(|line| {
        line.split(" - ")
            .next()
            .and_then(|prefix| prefix.split_whitespace().nth(4))
            .is_some_and(|field| field == mount_path)
    })
}

fn wait_for_mountinfo_path_to_clear(mount_path: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while mountinfo_contains_path(mount_path) {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    true
}

fn force_unmount_fuse_mount_path(mount_path: &Path) -> io::Result<()> {
    force_unmount_fuse_mount_path_with(mount_path, |mount_path| {
        umount_fuse_mount_path_command(mount_path).status()
    })
}

fn umount_fuse_mount_path_command(mount_path: &Path) -> Command {
    let (program, args) = forced_fuse_unmount_command_parts(std::env::consts::OS);
    let mut command = Command::new(program);
    command.args(args).arg(mount_path);
    command
}

fn forced_fuse_unmount_command_parts(target_os: &str) -> (&'static str, &'static [&'static str]) {
    if target_os == "linux" {
        ("fusermount3", &["-u", "-z"])
    } else {
        ("umount", &["-f"])
    }
}

fn force_unmount_fuse_mount_path_with(
    mount_path: &Path,
    run_umount: impl FnOnce(&Path) -> io::Result<ExitStatus>,
) -> io::Result<()> {
    let umount_failure = match run_umount(mount_path) {
        Ok(status) if status.success() => return Ok(()),
        Ok(status) => format!("system umount exited with status {status}"),
        Err(error) => format!("system umount failed to execute: {error}"),
    };

    Err(io::Error::other(umount_failure))
}

fn reconcile_fuse_unmount_result(
    unmount_result: io::Result<()>,
    force_unmount: impl FnOnce() -> io::Result<()>,
    wait_for_clear: impl FnOnce() -> bool,
) -> io::Result<()> {
    if unmount_result.is_ok() {
        return force_unmount().and_then(|()| {
            if wait_for_clear() {
                Ok(())
            } else {
                Err(io::Error::other(
                    "FUSE mount remained active after system umount",
                ))
            }
        });
    }

    match force_unmount() {
        Ok(()) => {
            if let Some(message) = forced_unmount_clearance_warning(wait_for_clear()) {
                eprintln!("{message}");
            }
        }
        Err(error) => {
            eprintln!("system umount also failed after fuser unmount failure: {error}");
        }
    }
    unmount_result
}

fn forced_unmount_clearance_warning(cleared: bool) -> Option<&'static str> {
    if cleared {
        None
    } else {
        Some("FUSE mount remained active after forced system unmount reported success")
    }
}

#[test]
fn mounted_fuse_cleanup_preserves_original_scenario_panic() {
    let scenario_panic: std::thread::Result<MountedOperationGuard> =
        Err(Box::new("scenario failed") as Box<dyn std::any::Any + Send>);
    let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finish_mounted_fuse_test(
            scenario_panic,
            Err(io::Error::other("unmount failed")),
            None,
        );
    }))
    .expect_err("cleanup should resume the original scenario panic");

    assert_eq!(
        observed.downcast_ref::<&str>(),
        Some(&"scenario failed"),
        "cleanup errors must not replace the original scenario panic"
    );
}

#[test]
fn mounted_fuse_cleanup_reports_joined_session_panic() {
    let observed = std::panic::catch_unwind(|| {
        finish_mounted_fuse_test(
            Ok(MountedOperationGuard::default()),
            Ok(()),
            Some(Err(
                Box::new("session crashed") as Box<dyn std::any::Any + Send>
            )),
        );
    })
    .expect_err("session panic should be reported before unmount failure");

    assert_eq!(
        observed.downcast_ref::<&str>(),
        Some(&"session crashed"),
        "joined session thread panic must be reported"
    );
}

#[test]
fn mounted_fuse_cleanup_allows_skipping_join_after_cleanup_failure() {
    let observed = std::panic::catch_unwind(|| {
        finish_mounted_fuse_test(
            Ok(MountedOperationGuard::default()),
            Err(io::Error::other("unmount failed")),
            None,
        );
    })
    .expect_err("unmount failure should still be reported when join is skipped");

    let message = observed
        .downcast_ref::<String>()
        .expect("unmount failure panic should carry a string message");
    assert!(message.contains("FUSE unmount should succeed"));
    assert!(message.contains("unmount failed"));
}

#[test]
fn mounted_fuse_cleanup_skips_join_when_scenario_panicked_or_unmount_failed() {
    let scenario_panic: std::thread::Result<MountedOperationGuard> =
        Err(Box::new("scenario failed") as Box<dyn std::any::Any + Send>);
    let scenario_ok = Ok(MountedOperationGuard::default());
    let unmount_ok = Ok(());

    assert!(!should_join_mounted_fuse_session(
        &scenario_panic,
        &unmount_ok
    ));
    assert!(!should_join_mounted_fuse_session(
        &scenario_ok,
        &Err(io::Error::other("unmount failed"))
    ));
    assert!(should_join_mounted_fuse_session(&scenario_ok, &unmount_ok));
}

#[test]
fn mounted_fuse_cleanup_bounds_session_join_wait() {
    let session_handle = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(250));
        Ok(())
    });
    let started = std::time::Instant::now();

    let join_result = join_mounted_fuse_session_until(session_handle, Duration::from_millis(10));

    assert!(join_result.is_none());
    assert!(started.elapsed() < Duration::from_millis(200));
}

#[test]
fn mounted_fuse_cleanup_caps_join_poll_sleep_to_remaining_timeout() {
    assert_eq!(
        join_poll_sleep_duration(Duration::from_millis(40)),
        Duration::from_millis(25)
    );
    assert_eq!(
        join_poll_sleep_duration(Duration::from_millis(10)),
        Duration::from_millis(10)
    );
}

#[test]
fn mounted_fuse_cleanup_detects_mount_path_device_restoration() {
    let mount_dir = target_tempdir("mount-detection-");
    use std::os::unix::fs::MetadataExt;
    let original_dev = std::fs::metadata(mount_dir.path())
        .expect("mount detection tempdir metadata")
        .dev();

    assert!(fuse_mount_path_cleared(mount_dir.path(), original_dev));
    assert!(!fuse_mount_path_cleared(
        mount_dir.path(),
        original_dev.wrapping_add(1)
    ));
}

#[test]
fn mounted_fuse_cleanup_treats_only_not_found_metadata_errors_as_cleared() {
    assert!(fuse_mount_metadata_error_is_cleared(io::Error::new(
        io::ErrorKind::NotFound,
        "mount directory removed",
    )));
    assert!(!fuse_mount_metadata_error_is_cleared(io::Error::other(
        "transport endpoint is not connected",
    )));
}

#[test]
fn mounted_fuse_cleanup_uses_forced_system_unmount_commands() {
    let mount_path = Path::new("/tmp/studiofs-test-mount");
    let (expected_program, expected_args) = forced_fuse_unmount_command_parts(std::env::consts::OS);
    let umount_command = umount_fuse_mount_path_command(mount_path);
    let umount_args = umount_command
        .get_args()
        .map(OsString::from)
        .collect::<Vec<_>>();

    let expected_args = expected_args
        .iter()
        .map(OsString::from)
        .chain(std::iter::once(mount_path.as_os_str().to_os_string()))
        .collect::<Vec<_>>();

    assert_eq!(umount_command.get_program(), expected_program);
    assert_eq!(umount_args, expected_args);
}

#[test]
fn mounted_fuse_cleanup_uses_unprivileged_linux_fuse_unmount_command() {
    let (program, args) = forced_fuse_unmount_command_parts("linux");

    assert_eq!(program, "fusermount3");
    assert_eq!(args, &["-u", "-z"]);
}

#[test]
fn mounted_fuse_cleanup_waits_after_forced_unmount_on_failed_primary_unmount() {
    let waited = std::cell::Cell::new(false);

    let result = reconcile_fuse_unmount_result(
        Err(io::Error::other("primary unmount failed")),
        || Ok(()),
        || {
            waited.set(true);
            true
        },
    );

    assert!(result.is_err());
    assert!(waited.get());
}

#[test]
fn mounted_fuse_cleanup_warns_when_forced_unmount_reports_success_but_mount_remains() {
    assert_eq!(forced_unmount_clearance_warning(true), None);
    assert_eq!(
        forced_unmount_clearance_warning(false),
        Some("FUSE mount remained active after forced system unmount reported success")
    );
}

fn with_mounted_fuse(
    mode: MountedFuseMode,
    suffix: &str,
    test_fn: impl FnOnce(&Path) -> MountedOperationGuard,
) {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs_with_cache_budget(
        &tmp,
        suffix,
        &rt,
        mounted_scenario_cache_budget_bytes(suffix),
        DEFAULT_TEST_MEMORY_BYTES,
    );
    let fs = StudioFuseFs::new_with_runtime_options(
        vfs,
        rt.handle().clone(),
        StudioFuseRuntimeOptions {
            async_write_enabled: mode.async_write_enabled(),
            linux_async_dio_enabled: mode.async_dio_enabled(),
            ..StudioFuseRuntimeOptions::default()
        },
    );

    let options = vec![fuser::MountOption::FSName(format!("studiofs-{suffix}"))];

    let mount_path = mount_dir.path().to_path_buf();
    use std::os::unix::fs::MetadataExt;
    let pre_dev = std::fs::metadata(&mount_path)
        .expect("mount dir should exist before session creation")
        .dev();

    let mut session = fuser::Session::new(fs, &mount_path, &options).expect("create FUSE session");
    let mut unmount = session.unmount_callable();
    let session_handle = std::thread::spawn(move || session.run());

    let test_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if std::fs::metadata(&mount_path)
                .map(|m| m.dev() != pre_dev)
                .unwrap_or(false)
            {
                break;
            }
            assert!(
                !session_handle.is_finished(),
                "FUSE session thread exited early before mount became available"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "FUSE mount did not become available within 10 s"
            );
            std::thread::sleep(Duration::from_millis(200));
        }

        test_fn(&mount_path)
    }));
    let unmount_result = unmount.unmount();
    // The unmount call above must happen while scenario guards are still alive;
    // after that, release them so fallback cleanup is not blocked by test files.
    let test_result = test_result.map(|_guard| MountedOperationGuard::default());
    let unmount_result =
        if wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(2)) {
            unmount_result
        } else {
            eprintln!("FUSE mount remained active after fuser unmount; trying system umount");
            reconcile_fuse_unmount_result(
                unmount_result,
                || force_unmount_fuse_mount_path(&mount_path),
                || wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(2)),
            )
        };
    let join_result = if should_join_mounted_fuse_session(&test_result, &unmount_result) {
        join_mounted_fuse_session_until(session_handle, Duration::from_secs(5))
    } else {
        None
    };

    finish_mounted_fuse_test(test_result, unmount_result, join_result);
}

fn with_mounted_native_fuse(
    suffix: &str,
    mode: MountedFuseMode,
    test_fn: impl FnOnce(&Path, &StudioVfs) -> MountedOperationGuard,
) {
    if !mounted_tests_enabled() {
        return;
    }
    if Command::new("fusermount3").arg("-V").status().is_err() {
        eprintln!("skipping native mounted scenario because fusermount3 is unavailable");
        return;
    }

    let tmp = target_tempdir("native-vfs-");
    let mount_dir = target_tempdir("native-mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, suffix, &rt);

    let mount_path = mount_dir.path().to_path_buf();
    let pre_dev = mount_path_device(&mount_path);
    let options = native_direct_mount_options();
    let request = options
        .fusermount_request(mount_path.to_str().expect("mount path should be utf-8"))
        .expect("build native fusermount3 request");
    let system = Fusermount3MountSystem::default();
    let fuse = system
        .mount(&request)
        .expect("native fusermount3 FUSE mount should return a device fd");

    let adapter = StudioVfsNativeAdapter::new(Arc::clone(&vfs), rt.handle().clone());
    let (session_done_tx, session_done_rx) = std::sync::mpsc::channel();
    let session_handle = std::thread::spawn(move || -> io::Result<()> {
        let result = (|| {
            let fuse_file = fuse.into_file();
            let fuse_fd = std::os::fd::AsRawFd::as_raw_fd(&fuse_file);
            let transport = ClassicReadWriteTransport::new(fuse_file).with_transport_fd(fuse_fd);
            let mut runtime_options = StudioFuseRuntimeOptions::from_env();
            runtime_options.async_write_enabled = mode.async_write_enabled();
            let mut config = NativeSessionConfig::linux6_safe(mode.writeback_cache_enabled());
            if runtime_options.linux_native_transport != LinuxNativeTransportSelection::Classic {
                config = config.with_io_uring_transport();
            }
            let session = NativeFuseSession::new(transport, config);
            let (negotiated, transport) = session
                .handshake_into_transport()
                .map_err(|error| io::Error::other(error.to_string()))?;
            let transport = match runtime_options.linux_native_transport {
                LinuxNativeTransportSelection::Classic => {
                    LinuxNativeFuseTransport::Classic(transport)
                }
                LinuxNativeTransportSelection::IoUring => {
                    if !negotiated
                        .capabilities
                        .contains(FuseCapabilities::OVER_IO_URING)
                    {
                        return Err(io::Error::other(
                            "FUSE-over-io_uring requested but FUSE_CAP_OVER_IO_URING was not negotiated",
                        ));
                    }
                    LinuxNativeFuseTransport::Hybrid(
                        HybridFuseTransport::<_, IoUringBackend>::register_kernel_io_uring(
                            transport,
                            fuse_fd,
                            linux_fuse_io_uring_queue_count(),
                            io_uring_payload_len_for_session(
                                negotiated.max_write,
                                negotiated.max_pages,
                            ),
                        )
                        .map_err(|error| io::Error::other(error.to_string()))?,
                    )
                }
                LinuxNativeTransportSelection::Auto => {
                    if negotiated
                        .capabilities
                        .contains(FuseCapabilities::OVER_IO_URING)
                    {
                        match HybridFuseTransport::<_, IoUringBackend>::try_register_kernel_io_uring(
                            transport,
                            fuse_fd,
                            linux_fuse_io_uring_queue_count(),
                            io_uring_payload_len_for_session(
                                negotiated.max_write,
                                negotiated.max_pages,
                            ),
                        ) {
                            Ok(transport) => LinuxNativeFuseTransport::Hybrid(transport),
                            Err((transport, error)) => {
                                eprintln!(
                                    "falling back to classic native FUSE transport after io_uring registration failed: {error}"
                                );
                                LinuxNativeFuseTransport::Classic(transport)
                            }
                        }
                    } else {
                        LinuxNativeFuseTransport::Classic(transport)
                    }
                }
            };
            let mut handler = LinuxNativeBridge::new(adapter);
            if let Some(config) = NativeAsyncWriteConfig::from_runtime_options(&runtime_options) {
                handler = handler.with_async_write_config(config);
            }
            let mut dispatcher = NativeDispatcher::new(transport, handler);
            dispatcher
                .run_until_destroy()
                .map_err(|error| io::Error::other(error.to_string()))?;
            Ok(())
        })();
        if let Err(error) = &result {
            eprintln!("native FUSE session thread finished early: {error:?}");
        }
        let _ = session_done_tx.send(format!("{result:?}"));
        result
    });

    let test_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if std::fs::metadata(&mount_path)
                .map(|metadata| {
                    use std::os::unix::fs::MetadataExt;
                    metadata.dev() != pre_dev
                })
                .unwrap_or(false)
            {
                break;
            }
            assert!(
                !session_handle.is_finished(),
                "native FUSE session thread exited early before mount became available: {}",
                session_done_rx
                    .try_recv()
                    .unwrap_or_else(|_| "session result unavailable".to_string())
            );
            assert!(
                std::time::Instant::now() < deadline,
                "native FUSE mount did not become available within 10 s"
            );
            std::thread::sleep(Duration::from_millis(200));
        }

        test_fn(&mount_path, &vfs)
    }));

    let unmount_result = force_unmount_fuse_mount_path(&mount_path);
    let test_result = test_result.map(|_guard| MountedOperationGuard::default());
    let unmount_result =
        if wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(2)) {
            unmount_result
        } else {
            reconcile_fuse_unmount_result(
                unmount_result,
                || force_unmount_fuse_mount_path(&mount_path),
                || wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(2)),
            )
        };
    let join_result = if should_join_mounted_fuse_session(&test_result, &unmount_result) {
        join_mounted_fuse_session_until(session_handle, Duration::from_secs(5))
    } else {
        None
    };

    finish_mounted_fuse_test(test_result, unmount_result, join_result);
}

fn run_mounted_scenario(scenario: MountedAsyncScenario) {
    match scenario.backend {
        MountedBackend::Fuser => {
            with_mounted_fuse(scenario.mode, scenario.name, |mount_path| {
                run_mounted_operation(mount_path, scenario.operation, scenario.name)
            });
        }
        MountedBackend::LinuxNativeBridge => run_native_mounted_scenario(scenario),
    }
}

fn run_native_mounted_scenario(scenario: MountedAsyncScenario) {
    assert_eq!(
        scenario.backend,
        MountedBackend::LinuxNativeBridge,
        "native mounted scenario must use LinuxNativeBridge backend"
    );
    with_mounted_native_fuse(scenario.name, scenario.mode, |mount_path, vfs| {
        run_mounted_operation_with_vfs(mount_path, scenario.operation, scenario.name, Some(vfs))
    });
}

fn run_mounted_operation(
    mount_path: &Path,
    operation: MountedScenarioOperation,
    scenario_name: &str,
) -> MountedOperationGuard {
    run_mounted_operation_with_vfs(mount_path, operation, scenario_name, None)
}

fn run_mounted_operation_with_vfs(
    mount_path: &Path,
    operation: MountedScenarioOperation,
    scenario_name: &str,
    vfs: Option<&StudioVfs>,
) -> MountedOperationGuard {
    match operation {
        MountedScenarioOperation::SequentialWriteReadback => {
            let test_file = mount_path.join("sequential.txt");
            let payload = b"StudioFS async FUSE sequential payload";
            std::fs::write(&test_file, payload).expect("write through FUSE mount");
            let readback = std::fs::read(&test_file).expect("read through FUSE mount");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::RandomWriteReadback => {
            let test_file = mount_path.join("random.txt");
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&test_file)
                .expect("create random write file");
            file.set_len(64).expect("pre-size random write file");
            file.seek(SeekFrom::Start(32))
                .expect("seek first random chunk");
            file.write_all(b"middle").expect("write first random chunk");
            file.seek(SeekFrom::Start(4))
                .expect("seek second random chunk");
            file.write_all(b"head").expect("write second random chunk");
            file.seek(SeekFrom::Start(56))
                .expect("seek tail random chunk");
            file.write_all(b"tail").expect("write tail random chunk");
            file.sync_all().expect("sync random writes");
            drop(file);

            let readback = std::fs::read(&test_file).expect("read random write file");
            assert_eq!(&readback[4..8], b"head");
            assert_eq!(&readback[32..38], b"middle");
            assert_eq!(&readback[56..60], b"tail");
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::TruncateReadback => {
            let test_file = mount_path.join("truncate.txt");
            std::fs::write(&test_file, b"0123456789abcdef").expect("write truncate seed");
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&test_file)
                .expect("open file for truncate");
            file.set_len(6).expect("shrink through mounted file");
            drop(file);
            let readback = std::fs::read(&test_file).expect("read truncated file");
            assert_eq!(readback, b"012345");
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::OverwriteReadback => {
            let test_file = mount_path.join("overwrite.txt");
            std::fs::write(&test_file, b"abcdefgh").expect("write overwrite seed");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&test_file)
                .expect("open file for overwrite");
            file.seek(SeekFrom::Start(2))
                .expect("seek overwrite offset");
            file.write_all(b"XYZ").expect("overwrite mounted file");
            file.sync_all().expect("sync overwrite");
            drop(file);
            let readback = std::fs::read(&test_file).expect("read overwritten file");
            assert_eq!(readback, b"abXYZfgh");
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::WriteFlushReadback => {
            let test_file = mount_path.join("flush.txt");
            let payload = b"StudioFS async FUSE flush payload";
            {
                let mut file = std::fs::File::create(&test_file).expect("create file");
                file.write_all(payload).expect("write payload");
                file.flush().expect("flush file handle");
            }
            let readback = std::fs::read(&test_file).expect("read after flush and close");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::WriteFsyncReadback => {
            let test_file = mount_path.join("fsync.txt");
            let payload = b"StudioFS async FUSE fsync payload";
            let mut file = std::fs::File::create(&test_file).expect("create file");
            file.write_all(payload).expect("write payload");
            file.sync_all().expect("fsync file");
            drop(file);
            let readback = std::fs::read(&test_file).expect("read after fsync");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::CloseReopenReadback => {
            let test_file = mount_path.join("close-reopen.txt");
            let payload = b"StudioFS async FUSE close/reopen payload";
            {
                let mut file = std::fs::File::create(&test_file).expect("create file");
                file.write_all(payload).expect("write payload");
            }
            let readback = std::fs::read(&test_file).expect("read reopened file");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::ErrorPropagation => {
            let missing_parent = mount_path.join("missing-parent").join("child.txt");
            let error = std::fs::write(&missing_parent, b"must fail")
                .expect_err("write below missing parent should fail");
            assert_eq!(error.kind(), io::ErrorKind::NotFound);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::MultipleWriters => {
            let test_file = mount_path.join("multiple-writers.txt");
            let mut first = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&test_file)
                .expect("open first writer");
            let mut second = std::fs::OpenOptions::new()
                .append(true)
                .open(&test_file)
                .expect("open second writer");
            first.write_all(b"writer-one\n").expect("write first chunk");
            second
                .write_all(b"writer-two\n")
                .expect("write second chunk");
            first.sync_all().expect("sync first writer");
            drop(first);
            drop(second);
            let readback = std::fs::read_to_string(&test_file).expect("read combined writes");
            assert!(readback.contains("writer-one"));
            assert!(readback.contains("writer-two"));
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::SameHandleAppends => {
            let test_file = mount_path.join("same-handle.txt");
            let mut file = std::fs::File::create(&test_file).expect("create file");
            file.write_all(b"first\n").expect("write first chunk");
            file.write_all(b"second\n").expect("write second chunk");
            file.sync_all().expect("sync appended chunks");
            drop(file);
            let readback = std::fs::read_to_string(&test_file).expect("read appended file");
            assert_eq!(readback, "first\nsecond\n");
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::WriteOnlyUntilUnmount => {
            let test_file = mount_path.join("write-only-until-unmount.bin");
            let mut file = std::fs::File::create(&test_file).expect("create write-only file");
            for chunk in 0..64u8 {
                file.write_all(&[chunk; 16 * 1024])
                    .expect("write chunk that should drain during unmount");
            }
            MountedOperationGuard {
                _open_files: vec![file],
            }
        }
        MountedScenarioOperation::FioMultiJob => {
            run_fio_workload_if_available(mount_path);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::Sfs509FioMatrix => {
            run_sfs509_fio_matrix_if_available(mount_path, scenario_name);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::Sfs530EvidenceMatrix => {
            run_sfs530_evidence_matrix_if_available(mount_path, scenario_name, vfs);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::DirectoryLifecycle => {
            let dir = mount_path.join("dir-lifecycle");
            std::fs::create_dir(&dir).expect("mkdir through native FUSE mount");
            let file = dir.join("entry.txt");
            std::fs::write(&file, b"directory lifecycle payload")
                .expect("create file in opened directory");
            let entries: BTreeSet<_> = std::fs::read_dir(&dir)
                .expect("readdir through native FUSE mount")
                .map(|entry| entry.expect("directory entry").file_name())
                .collect();
            assert!(
                entries.contains(std::ffi::OsStr::new("entry.txt")),
                "readdir must include file created in directory, got {entries:?}"
            );
            std::fs::remove_file(&file).expect("unlink file from directory");
            std::fs::remove_dir(&dir).expect("released directory should be removable");
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::NamespaceMutations => {
            let dir = mount_path.join("namespace");
            std::fs::create_dir(&dir).expect("mkdir through native FUSE mount");
            let original = dir.join("original.txt");
            let renamed = dir.join("renamed.txt");
            std::fs::write(&original, b"0123456789abcdef")
                .expect("create file through native FUSE mount");
            std::fs::rename(&original, &renamed).expect("rename through native FUSE mount");
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&renamed)
                .expect("open renamed file for truncate");
            file.set_len(4).expect("truncate through native FUSE mount");
            drop(file);
            let metadata = std::fs::metadata(&renamed).expect("stat renamed file");
            assert_eq!(metadata.len(), 4);
            let statfs = statfs_mount_path(mount_path).expect("statfs native mount");
            assert!(statfs.f_blocks > 0, "statfs should report positive blocks");
            std::fs::remove_file(&renamed).expect("unlink through native FUSE mount");
            std::fs::remove_dir(&dir).expect("remove namespace directory");
            MountedOperationGuard::default()
        }
    }
}

fn statfs_mount_path(path: &Path) -> io::Result<libc::statvfs> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let mut statfs = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated C string and `statfs` points to
    // writable uninitialized storage for libc to fill.
    let result = unsafe { libc::statvfs(path.as_ptr(), statfs.as_mut_ptr()) };
    if result == 0 {
        // SAFETY: libc returned success, so it initialized the output struct.
        Ok(unsafe { statfs.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

fn command_path_is_executable(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn command_available(command: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|path| command_path_is_executable(&path.join(command)))
    })
}

fn run_fio_workload_if_available(mount_path: &Path) {
    if !command_available("fio") {
        eprintln!("skipping fio mounted workload because fio is not installed");
        return;
    }

    let mut directory_arg = OsString::from("--directory=");
    directory_arg.push(mount_path);
    let status = Command::new("fio")
        .arg("--name=studiofs-fuse-async")
        .arg(directory_arg)
        .arg("--filename=fio.bin")
        .arg("--rw=write")
        .arg("--bs=64k")
        .arg("--size=1m")
        .arg("--numjobs=2")
        .arg("--ioengine=sync")
        .arg("--fsync=1")
        .arg("--group_reporting")
        .status()
        .expect("run fio workload");
    assert!(status.success(), "fio workload should exit successfully");
}

fn sfs530_artifact_dir(scenario_name: &str) -> std::path::PathBuf {
    let dir = workspace_root()
        .join("target")
        .join("sfs-530-linux-native-async-write")
        .join(scenario_name);
    std::fs::create_dir_all(&dir).expect("create SFS-530 artifact directory");
    dir
}

fn run_sfs530_fio_json_case(
    artifact_dir: &Path,
    mount_path: &Path,
    case_name: &str,
    filename: &str,
    args: &[&str],
) -> bool {
    let output = artifact_dir.join(format!("{case_name}.json"));
    let log = artifact_dir.join(format!("{case_name}.log"));
    let mut directory_arg = OsString::from("--directory=");
    directory_arg.push(mount_path);
    let mut filename_arg = OsString::from("--filename=");
    filename_arg.push(filename);
    let mut output_arg = OsString::from("--output=");
    output_arg.push(&output);
    let timeout = sfs509_env_value("STUDIOFS_SFS530_FIO_TIMEOUT_SECS", "90");
    let timeout_arg = format!("{timeout}s");

    let mut command = if command_available("timeout") {
        let mut command = Command::new("timeout");
        command.arg("--kill-after=10s").arg(timeout_arg).arg("fio");
        command
    } else {
        Command::new("fio")
    };
    let result = command
        .arg(format!("--name=studiofs-sfs530-{case_name}"))
        .arg(directory_arg)
        .arg(filename_arg)
        .arg("--ioengine=sync")
        .arg("--group_reporting")
        .arg("--output-format=json")
        .arg(output_arg)
        .args(args)
        .output()
        .expect("run SFS-530 fio workload");
    let mut log_file = std::fs::File::create(&log).expect("create SFS-530 fio log");
    log_file
        .write_all(&result.stdout)
        .expect("write SFS-530 fio stdout");
    log_file
        .write_all(&result.stderr)
        .expect("write SFS-530 fio stderr");
    let status_line = format!("{case_name}\t{}\n", result.status);
    let mut status_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(artifact_dir.join("workload-status.tsv"))
        .expect("open SFS-530 workload status");
    status_file
        .write_all(status_line.as_bytes())
        .expect("write SFS-530 workload status");
    if !result.status.success() {
        eprintln!(
            "SFS-530 fio workload {case_name} exited with {}; see {}",
            result.status,
            log.display()
        );
    }
    result.status.success()
}

fn write_sfs530_telemetry_snapshot(artifact_dir: &Path, name: &str, vfs: Option<&StudioVfs>) {
    let Some(vfs) = vfs else {
        return;
    };
    let snapshot = vfs.write_path_observability_snapshot();
    let async_write = snapshot.fuse_async_write;
    let native_bridge = snapshot.native_bridge;
    let backpressure = snapshot.write_backpressure;
    let dirty_pressure = snapshot.dirty_pressure;
    let json = format!(
        concat!(
            "{{\n",
            "  \"fuse_async_write\": {{\n",
            "    \"snapshot_generation\": {snapshot_generation},\n",
            "    \"enabled\": {enabled},\n",
            "    \"platform_policy\": \"{platform_policy}\",\n",
            "    \"max_pending_bytes\": {max_pending_bytes},\n",
            "    \"max_pending_writes\": {max_pending_writes},\n",
            "    \"per_handle_pending_bytes\": {per_handle_pending_bytes},\n",
            "    \"pending_writes\": {pending_writes},\n",
            "    \"pending_bytes\": {pending_bytes},\n",
            "    \"peak_pending_bytes\": {peak_pending_bytes},\n",
            "    \"completed_writes\": {completed_writes},\n",
            "    \"failed_writes\": {failed_writes},\n",
            "    \"task_failed_writes\": {task_failed_writes},\n",
            "    \"admission_waits\": {admission_waits},\n",
            "    \"admission_wait_total_ms\": {admission_wait_total_ms:.3},\n",
            "    \"barrier_waits\": {barrier_waits},\n",
            "    \"barrier_wait_total_ms\": {barrier_wait_total_ms:.3}\n",
            "  }},\n",
            "  \"native_bridge\": {{\n",
            "    \"fuse_ops\": {native_fuse_ops},\n",
            "    \"metadata_ops\": {native_metadata_ops},\n",
            "    \"lookup_ops\": {native_lookup_ops},\n",
            "    \"getattr_ops\": {native_getattr_ops},\n",
            "    \"readdir_ops\": {native_readdir_ops},\n",
            "    \"readdirplus_ops\": {native_readdirplus_ops},\n",
            "    \"open_ops\": {native_open_ops},\n",
            "    \"release_ops\": {native_release_ops},\n",
            "    \"read_ops\": {native_read_ops},\n",
            "    \"write_ops\": {native_write_ops},\n",
            "    \"lookup_getattr_ratio_micros\": {native_lookup_getattr_ratio_micros},\n",
            "    \"open_release_ratio_micros\": {native_open_release_ratio_micros},\n",
            "    \"fuse_ops_per_read_write_micros\": {native_fuse_ops_per_read_write_micros},\n",
            "    \"metadata_ops_per_open_micros\": {native_metadata_ops_per_open_micros},\n",
            "    \"metadata_latency_total_ns\": {native_metadata_latency_total_ns},\n",
            "    \"lookup_p95_ns\": {native_lookup_p95_ns},\n",
            "    \"lookup_p99_ns\": {native_lookup_p99_ns},\n",
            "    \"getattr_p95_ns\": {native_getattr_p95_ns},\n",
            "    \"getattr_p99_ns\": {native_getattr_p99_ns},\n",
            "    \"readdir_p95_ns\": {native_readdir_p95_ns},\n",
            "    \"readdir_p99_ns\": {native_readdir_p99_ns},\n",
            "    \"read_p95_ns\": {native_read_p95_ns},\n",
            "    \"read_p99_ns\": {native_read_p99_ns},\n",
            "    \"write_p95_ns\": {native_write_p95_ns},\n",
            "    \"write_p99_ns\": {native_write_p99_ns},\n",
            "    \"local_cache_hits\": {native_local_cache_hits},\n",
            "    \"local_cache_unavailable_fallbacks\": {native_local_cache_unavailable_fallbacks},\n",
            "    \"stale_epoch_fallbacks\": {native_stale_epoch_fallbacks},\n",
            "    \"stale_generation_fallbacks\": {native_stale_generation_fallbacks},\n",
            "    \"bytes_fallbacks\": {native_bytes_fallbacks},\n",
            "    \"bytes_returned\": {native_bytes_returned},\n",
            "    \"read_ahead_prefetches\": {native_read_ahead_prefetches},\n",
            "    \"read_ahead_suppressed\": {native_read_ahead_suppressed},\n",
            "    \"read_ahead_amplification_bytes\": {native_read_ahead_amplification_bytes}\n",
            "  }},\n",
            "  \"write_backpressure\": {{\n",
            "    \"events\": {backpressure_events},\n",
            "    \"wait_total_ms\": {backpressure_wait_total_ms},\n",
            "    \"stall_max_ms\": {backpressure_stall_max_ms},\n",
            "    \"soft_throttle_events\": {backpressure_soft_throttle_events},\n",
            "    \"hard_events\": {backpressure_hard_events}\n",
            "  }},\n",
            "  \"dirty_pressure\": {{\n",
            "    \"pending_local_dirty_bytes\": {pending_local_dirty_bytes},\n",
            "    \"pending_persisted_slice_bytes\": {pending_persisted_slice_bytes},\n",
            "    \"writer_stack_dirty_bytes\": {writer_stack_dirty_bytes},\n",
            "    \"total_dirty_bytes\": {total_dirty_bytes},\n",
            "    \"dirty_limit_bytes\": {dirty_limit_bytes},\n",
            "    \"dirty_peak_bytes\": {dirty_peak_bytes},\n",
            "    \"staged_peak_bytes\": {staged_peak_bytes}\n",
            "  }}\n",
            "}}\n",
        ),
        snapshot_generation = async_write.snapshot_generation,
        enabled = async_write.enabled,
        platform_policy = async_write.platform_policy,
        max_pending_bytes = json_option_usize(async_write.max_pending_bytes),
        max_pending_writes = json_option_usize(async_write.max_pending_writes),
        per_handle_pending_bytes = json_option_usize(async_write.per_handle_pending_bytes),
        pending_writes = async_write.pending_writes,
        pending_bytes = async_write.pending_bytes,
        peak_pending_bytes = async_write.peak_pending_bytes,
        completed_writes = async_write.completed_writes,
        failed_writes = async_write.failed_writes,
        task_failed_writes = async_write.task_failed_writes,
        admission_waits = async_write.admission_waits,
        admission_wait_total_ms = async_write.admission_wait_total_ms,
        barrier_waits = async_write.barrier_waits,
        barrier_wait_total_ms = async_write.barrier_wait_total_ms,
        native_fuse_ops = native_bridge.fuse_ops,
        native_metadata_ops = native_bridge.metadata_ops,
        native_lookup_ops = native_bridge.lookup_ops,
        native_getattr_ops = native_bridge.getattr_ops,
        native_readdir_ops = native_bridge.readdir_ops,
        native_readdirplus_ops = native_bridge.readdirplus_ops,
        native_open_ops = native_bridge.open_ops,
        native_release_ops = native_bridge.release_ops,
        native_read_ops = native_bridge.read_ops,
        native_write_ops = native_bridge.write_ops,
        native_lookup_getattr_ratio_micros = native_bridge.lookup_getattr_ratio_micros,
        native_open_release_ratio_micros = native_bridge.open_release_ratio_micros,
        native_fuse_ops_per_read_write_micros = native_bridge.fuse_ops_per_read_write_micros,
        native_metadata_ops_per_open_micros = native_bridge.metadata_ops_per_open_micros,
        native_metadata_latency_total_ns = native_bridge.metadata_latency_total_ns,
        native_lookup_p95_ns = native_bridge.lookup_latency.p95_ns,
        native_lookup_p99_ns = native_bridge.lookup_latency.p99_ns,
        native_getattr_p95_ns = native_bridge.getattr_latency.p95_ns,
        native_getattr_p99_ns = native_bridge.getattr_latency.p99_ns,
        native_readdir_p95_ns = native_bridge.readdir_latency.p95_ns,
        native_readdir_p99_ns = native_bridge.readdir_latency.p99_ns,
        native_read_p95_ns = native_bridge.read_latency.p95_ns,
        native_read_p99_ns = native_bridge.read_latency.p99_ns,
        native_write_p95_ns = native_bridge.write_latency.p95_ns,
        native_write_p99_ns = native_bridge.write_latency.p99_ns,
        native_local_cache_hits = native_bridge.local_cache_hits,
        native_local_cache_unavailable_fallbacks = native_bridge.local_cache_unavailable_fallbacks,
        native_stale_epoch_fallbacks = native_bridge.stale_epoch_fallbacks,
        native_stale_generation_fallbacks = native_bridge.stale_generation_fallbacks,
        native_bytes_fallbacks = native_bridge.bytes_fallbacks,
        native_bytes_returned = native_bridge.bytes_returned,
        native_read_ahead_prefetches = native_bridge.read_ahead_prefetches,
        native_read_ahead_suppressed = native_bridge.read_ahead_suppressed,
        native_read_ahead_amplification_bytes = native_bridge.read_ahead_amplification_bytes,
        backpressure_events = backpressure.events,
        backpressure_wait_total_ms = backpressure.wait_total_ms,
        backpressure_stall_max_ms = backpressure.stall_max_ms,
        backpressure_soft_throttle_events = backpressure.soft_throttle_events,
        backpressure_hard_events = backpressure.hard_events,
        pending_local_dirty_bytes = dirty_pressure.pending_local_dirty_bytes,
        pending_persisted_slice_bytes = dirty_pressure.pending_persisted_slice_bytes,
        writer_stack_dirty_bytes = dirty_pressure.writer_stack_dirty_bytes,
        total_dirty_bytes = dirty_pressure.total_dirty_bytes,
        dirty_limit_bytes = dirty_pressure.dirty_limit_bytes,
        dirty_peak_bytes = dirty_pressure.dirty_peak_bytes,
        staged_peak_bytes = dirty_pressure.staged_peak_bytes,
    );
    std::fs::write(artifact_dir.join(name), json).expect("write SFS-530 telemetry snapshot");
}

fn json_option_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "null".to_string(), |value| value.to_string())
}

fn run_sfs530_evidence_matrix_if_available(
    mount_path: &Path,
    scenario_name: &str,
    vfs: Option<&StudioVfs>,
) {
    assert!(
        command_available("fio"),
        "SFS-530 native async write evidence matrix requires fio"
    );

    raise_nofile_limit_best_effort();
    let artifact_dir = sfs530_artifact_dir(scenario_name);
    for artifact in [
        "correctness-status.tsv",
        "workload-status.tsv",
        "seq-write.json",
        "seq-write.log",
        "rand-write.json",
        "rand-write.log",
        "multi-job.json",
        "multi-job.log",
        "fsync-heavy.json",
        "fsync-heavy.log",
        "write-path-observability-before.json",
        "write-path-observability-after.json",
    ] {
        let _ = std::fs::remove_file(artifact_dir.join(artifact));
    }
    write_sfs530_telemetry_snapshot(&artifact_dir, "write-path-observability-before.json", vfs);
    let full_correctness_ops = [
        MountedScenarioOperation::SequentialWriteReadback,
        MountedScenarioOperation::RandomWriteReadback,
        MountedScenarioOperation::TruncateReadback,
        MountedScenarioOperation::OverwriteReadback,
        MountedScenarioOperation::WriteFlushReadback,
        MountedScenarioOperation::WriteFsyncReadback,
        MountedScenarioOperation::CloseReopenReadback,
        MountedScenarioOperation::ErrorPropagation,
    ];
    for operation in full_correctness_ops {
        let _guard = run_mounted_operation(mount_path, operation, scenario_name);
        let mut status_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(artifact_dir.join("correctness-status.tsv"))
            .expect("open SFS-530 correctness status");
        let status_line = format!("{operation:?}\tpass\n");
        status_file
            .write_all(status_line.as_bytes())
            .expect("write SFS-530 correctness status");
    }

    let size = sfs509_env_value("STUDIOFS_SFS530_FIO_SIZE", "128m");
    let runtime = sfs509_env_value("STUDIOFS_SFS530_FIO_RUNTIME_SECS", "5");
    let size_arg = format!("--size={size}");
    let runtime_arg = format!("--runtime={runtime}");
    let offset_increment_arg = format!("--offset_increment={size}");
    let manifest = format!(
        concat!(
            "issue=SFS-530\n",
            "scenario={scenario_name}\n",
            "artifact_dir={artifact_dir}\n",
            "fio_size={size}\n",
            "fio_runtime_secs={runtime}\n",
            "correctness_status=correctness-status.tsv\n",
            "workload_status=workload-status.tsv\n",
            "telemetry_before=write-path-observability-before.json\n",
            "telemetry_after=write-path-observability-after.json\n",
        ),
        scenario_name = scenario_name,
        artifact_dir = artifact_dir.display(),
        size = size,
        runtime = runtime,
    );
    std::fs::write(artifact_dir.join("manifest.txt"), manifest)
        .expect("write SFS-530 evidence manifest");

    let mut all_passed = true;
    all_passed &= run_sfs530_fio_json_case(
        &artifact_dir,
        mount_path,
        "seq-write",
        "seq.bin",
        &[
            "--rw=write",
            "--bs=1m",
            &size_arg,
            "--numjobs=1",
            "--fsync_on_close=1",
        ],
    );
    all_passed &= run_sfs530_fio_json_case(
        &artifact_dir,
        mount_path,
        "rand-write",
        "rand.bin",
        &[
            "--rw=randwrite",
            "--bs=4k",
            &size_arg,
            "--numjobs=1",
            &runtime_arg,
            "--time_based",
            "--fsync=32",
        ],
    );
    all_passed &= run_sfs530_fio_json_case(
        &artifact_dir,
        mount_path,
        "multi-job",
        "multi.bin",
        &[
            "--rw=write",
            "--bs=64k",
            &size_arg,
            "--numjobs=4",
            &offset_increment_arg,
            &runtime_arg,
            "--time_based",
            "--fsync=1",
        ],
    );
    all_passed &= run_sfs530_fio_json_case(
        &artifact_dir,
        mount_path,
        "fsync-heavy",
        "fsync-heavy.bin",
        &[
            "--rw=write",
            "--bs=16k",
            &size_arg,
            "--numjobs=1",
            &runtime_arg,
            "--time_based",
            "--fsync=1",
        ],
    );

    write_sfs530_telemetry_snapshot(&artifact_dir, "write-path-observability-after.json", vfs);
    assert!(
        all_passed,
        "SFS-530 evidence matrix failed; see {}",
        artifact_dir.display()
    );
}

fn sfs509_env_value(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn sfs509_artifact_dir(scenario_name: &str) -> std::path::PathBuf {
    let dir = workspace_root()
        .join("target")
        .join("sfs-509-linux-fuse-ext4-throughput")
        .join("studiofs")
        .join("long-lived-fio")
        .join(scenario_name);
    std::fs::create_dir_all(&dir).expect("create SFS-509 artifact directory");
    dir
}

fn run_fio_json_case(
    artifact_dir: &Path,
    mount_path: &Path,
    case_name: &str,
    filename: &str,
    args: &[&str],
) -> bool {
    let output = artifact_dir.join(format!("{case_name}.json"));
    let log = artifact_dir.join(format!("{case_name}.log"));
    let mut directory_arg = OsString::from("--directory=");
    directory_arg.push(mount_path);
    let mut filename_arg = OsString::from("--filename=");
    filename_arg.push(filename);
    let mut output_arg = OsString::from("--output=");
    output_arg.push(&output);
    let timeout = sfs509_env_value("STUDIOFS_SFS509_FIO_TIMEOUT_SECS", "180");
    let timeout_arg = format!("{timeout}s");

    let mut command = if command_available("timeout") {
        let mut command = Command::new("timeout");
        command.arg("--kill-after=10s").arg(timeout_arg).arg("fio");
        command
    } else {
        Command::new("fio")
    };
    let result = command
        .arg(format!("--name=studiofs-sfs509-{case_name}"))
        .arg(directory_arg)
        .arg(filename_arg)
        .arg("--ioengine=sync")
        .arg("--group_reporting")
        .arg("--output-format=json")
        .arg(output_arg)
        .args(args)
        .output()
        .expect("run SFS-509 fio workload");
    let mut log_file = std::fs::File::create(&log).expect("create SFS-509 fio log");
    log_file
        .write_all(&result.stdout)
        .expect("write SFS-509 fio stdout");
    log_file
        .write_all(&result.stderr)
        .expect("write SFS-509 fio stderr");
    let status_line = format!("{case_name}\t{}\n", result.status);
    let mut status_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(artifact_dir.join("workload-status.tsv"))
        .expect("open SFS-509 workload status");
    status_file
        .write_all(status_line.as_bytes())
        .expect("write SFS-509 workload status");
    if !result.status.success() {
        eprintln!(
            "SFS-509 fio workload {case_name} exited with {}; see {}",
            result.status,
            log.display()
        );
    }
    result.status.success()
}

fn assert_sfs509_fio_json_case(
    artifact_dir: &Path,
    mount_path: &Path,
    case_name: &str,
    filename: &str,
    args: &[&str],
) {
    assert!(
        run_fio_json_case(artifact_dir, mount_path, case_name, filename, args),
        "SFS-509 fio workload {case_name} failed; see {}",
        artifact_dir.display()
    );
}

fn run_sfs509_fio_matrix_if_available(mount_path: &Path, scenario_name: &str) {
    assert!(
        command_available("fio"),
        "SFS-509 long-lived fio matrix requires fio"
    );

    raise_nofile_limit_best_effort();
    let artifact_dir = sfs509_artifact_dir(scenario_name);
    let _ = std::fs::remove_file(artifact_dir.join("workload-status.tsv"));
    let size = sfs509_env_value("STUDIOFS_SFS509_FIO_SIZE", SFS509_DEFAULT_FIO_SIZE);
    let large_size = sfs509_env_value(
        "STUDIOFS_SFS509_FIO_LARGE_SIZE",
        SFS509_DEFAULT_FIO_LARGE_SIZE,
    );
    let multi_job_default_size = sfs509_multi_job_size(&size);
    let multi_job_size = sfs509_env_value(
        "STUDIOFS_SFS509_FIO_MULTI_JOB_SIZE",
        &multi_job_default_size,
    );
    let fsync_heavy_default_size = sfs509_fsync_heavy_size(&size);
    let fsync_heavy_size = sfs509_env_value(
        "STUDIOFS_SFS509_FIO_FSYNC_HEAVY_SIZE",
        &fsync_heavy_default_size,
    );
    let runtime = sfs509_env_value(
        "STUDIOFS_SFS509_FIO_RUNTIME_SECS",
        SFS509_DEFAULT_FIO_RUNTIME_SECS,
    );
    let metadata_files = sfs509_env_value(
        "STUDIOFS_SFS509_METADATA_FILES",
        SFS509_DEFAULT_METADATA_FILES,
    )
    .parse::<usize>()
    .expect("STUDIOFS_SFS509_METADATA_FILES must be usize");
    let size_arg = format!("--size={size}");
    let large_size_arg = format!("--size={large_size}");
    let multi_job_size_arg = format!("--size={multi_job_size}");
    let fsync_heavy_size_arg = format!("--size={fsync_heavy_size}");
    let runtime_arg = format!("--runtime={runtime}");
    let offset_increment_arg = format!("--offset_increment={multi_job_size}");

    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "seq-write",
        "seq.bin",
        &[
            "--rw=write",
            "--bs=1m",
            &size_arg,
            "--numjobs=1",
            "--fsync_on_close=1",
        ],
    );
    drop_caches_best_effort(&artifact_dir);
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "cold-cache-read",
        "seq.bin",
        &["--rw=read", "--bs=1m", &size_arg, "--numjobs=1"],
    );
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "warm-cache-read",
        "seq.bin",
        &["--rw=read", "--bs=1m", &size_arg, "--numjobs=1"],
    );
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "rand-write",
        "rand.bin",
        &[
            "--rw=randwrite",
            "--bs=4k",
            &size_arg,
            "--numjobs=1",
            &runtime_arg,
            "--time_based",
            "--fsync=32",
        ],
    );
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "rand-read",
        "rand.bin",
        &[
            "--rw=randread",
            "--bs=4k",
            &size_arg,
            "--numjobs=1",
            &runtime_arg,
            "--time_based",
        ],
    );
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "mixed-rw",
        "mixed.bin",
        &[
            "--rw=randrw",
            "--rwmixread=70",
            "--bs=16k",
            &size_arg,
            "--numjobs=2",
            &runtime_arg,
            "--time_based",
            "--fsync=64",
        ],
    );
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "large-file",
        "large.bin",
        &[
            "--rw=write",
            "--bs=1m",
            &large_size_arg,
            "--numjobs=1",
            "--fsync_on_close=1",
        ],
    );
    run_sfs509_metadata_probe(mount_path, &artifact_dir, metadata_files);
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "multi-job",
        "multi.bin",
        &[
            "--rw=write",
            "--bs=64k",
            &multi_job_size_arg,
            "--numjobs=4",
            &offset_increment_arg,
        ],
    );
    assert_sfs509_fio_json_case(
        &artifact_dir,
        mount_path,
        "fsync-heavy",
        "fsync-heavy.bin",
        &[
            "--rw=write",
            "--bs=16k",
            &fsync_heavy_size_arg,
            "--numjobs=1",
            "--fsync=1",
        ],
    );
}

#[cfg(target_os = "linux")]
fn raise_nofile_limit_best_effort() {
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return;
        }
        let desired = 65_535;
        let raised = desired.min(limit.rlim_max);
        if raised > limit.rlim_cur {
            limit.rlim_cur = raised;
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
        }
    }
}

fn drop_caches_best_effort(artifact_dir: &Path) {
    let log = artifact_dir.join("drop-caches.log");
    let result = Command::new("sudo")
        .arg("-n")
        .arg("sh")
        .arg("-c")
        .arg("sync; echo 3 > /proc/sys/vm/drop_caches")
        .output();
    match result {
        Ok(output) if output.status.success() => {
            std::fs::write(log, b"drop_caches=success\n").expect("write drop_caches log");
        }
        Ok(output) => {
            let mut bytes = b"drop_caches=skipped\n".to_vec();
            bytes.extend_from_slice(&output.stderr);
            std::fs::write(log, bytes).expect("write drop_caches log");
        }
        Err(error) => {
            std::fs::write(log, format!("drop_caches=skipped\n{error}\n"))
                .expect("write drop_caches log");
        }
    }
}

fn run_sfs509_metadata_probe(mount_path: &Path, artifact_dir: &Path, file_count: usize) {
    let metadata_dir = mount_path.join("sfs509-metadata-heavy");
    let _ = std::fs::remove_dir_all(&metadata_dir);
    std::fs::create_dir_all(&metadata_dir).expect("create SFS-509 metadata directory");

    let create_started = std::time::Instant::now();
    for index in 0..file_count {
        let path = metadata_dir.join(format!("file-{index:06}.txt"));
        std::fs::write(path, format!("payload-{index:06}\n")).expect("write SFS-509 metadata file");
    }
    let create_elapsed = create_started.elapsed();

    let stat_started = std::time::Instant::now();
    let mut total_bytes = 0u64;
    let mut seen = 0usize;
    for entry in std::fs::read_dir(&metadata_dir).expect("read SFS-509 metadata directory") {
        let entry = entry.expect("read SFS-509 metadata entry");
        let metadata = entry.metadata().expect("stat SFS-509 metadata entry");
        total_bytes = total_bytes.saturating_add(metadata.len());
        seen = seen.saturating_add(1);
    }
    let stat_elapsed = stat_started.elapsed();
    assert_eq!(seen, file_count);

    let summary = format!(
        "files\tcreate_ms\tstat_ms\ttotal_bytes\n{}\t{}\t{}\t{}\n",
        file_count,
        create_elapsed.as_millis(),
        stat_elapsed.as_millis(),
        total_bytes
    );
    std::fs::write(artifact_dir.join("metadata-readdir-stat.tsv"), summary)
        .expect("write SFS-509 metadata summary");
}

/// Verifies that creating and dropping a FUSE session does not panic or leak.
///
/// This test is intentionally ignored unless `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1`
/// is set to avoid requiring a mounted FUSE environment in normal CI runs.
#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_session_mount_and_drop_smoke() {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());
    let options = vec![fuser::MountOption::FSName("studiofs-test".into())];

    let session = fuser::Session::new(fs, mount_dir.path(), &options).expect("create FUSE session");
    drop(session);
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
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_mount_write_read_unmount_smoke() {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());

    let options = vec![fuser::MountOption::FSName("studiofs-smoke".into())];

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
