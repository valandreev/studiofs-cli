//! CLI command modules and clap parser tree.

use clap::{Parser, Subcommand};

pub(crate) const WRITE_PATH_OBSERVABILITY_FILE: &str = "write_path_observability.json";

pub(crate) mod benchmark_engine;
pub mod cache_layout;
pub mod destroy;
pub mod discovery;
pub mod dump_load;
pub mod fsck;
pub mod init;
pub mod internal;
pub mod janitor;
pub mod mount;
#[cfg(unix)]
pub mod mount_profile;
#[cfg(not(unix))]
pub mod mount_profile_stub;
#[cfg(not(unix))]
pub use self::mount_profile_stub as mount_profile;
#[cfg(unix)]
pub mod benchmark_profile;
#[cfg(not(unix))]
pub mod benchmark_profile_stub;
pub mod mount_profile_types;
pub mod perf;
pub mod runtime_tuning;
pub mod settings;
pub mod snapshot;
pub mod status;
pub mod test;
pub mod test_cache;
#[cfg(not(unix))]
pub use self::benchmark_profile_stub as benchmark_profile;
pub mod benchmark_profile_common;
pub mod benchmark_profile_types;
pub mod umount;
pub mod utils;
#[cfg(windows)]
pub mod winfsp_bootstrap;

#[derive(Parser)]
#[command(name = "studiofs")]
#[command(version)]
#[command(about = "Mount S3-compatible storage as a local filesystem")]
#[command(long_about = "\
StudioFS — a high-performance, cross-platform virtual filesystem backed by \
S3-compatible cloud storage for file data and PostgreSQL for shared metadata. \
It combines the scalability of object storage with multi-tier caching (memory + \
disk) and a distributed metadata backend for multi-client mounts.

Supported platforms:
  • Windows  (via WinFSP)
  • Linux    (via FUSE / libfuse3)
  • macOS    (via macFUSE)

Supported S3 backends:
  AWS S3, MinIO, Google Cloud Storage, Wasabi, Backblaze B2, and any S3-compatible service.")]
#[command(after_help = "\
EXAMPLES:
  Initialize a new volume with RustFS + PostgreSQL:
    studiofs init myvolume --endpoint http://127.0.0.1:9000 \\
      --bucket mybucket --access-key studiofs --secret-key studiofs123 \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

  Discover the same volume on another machine/client:
    studiofs discover myvolume \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

  Mount on Windows (drive letter):
    studiofs mount myvolume Z:

  Mount in background (daemon mode):
    studiofs mount myvolume Z: -d

  Mount on Linux/macOS (directory):
    studiofs mount myvolume /mnt/studio --cache-size 500GB

  Unmount:
    studiofs umount Z:
    studiofs umount /mnt/studio

  Check volume status:
    studiofs status myvolume

  Run janitor in dry-run mode:
    studiofs janitor myvolume --dry-run

  Run metadata fsck:
    studiofs fsck myvolume
    studiofs fsck myvolume --repair

LOCAL-FIRST OPERATOR MODEL:
  Primary mount knobs:
    --cache-dir
    --cache-size
    --read-buffer
    --readahead
    --prefetch
  Metadata-cache internals stay on strong built-in defaults.
  Additional STUDIOFS_* env overrides are transitional/developer-oriented.
  Use `studiofs status <name>` to inspect runtime usage/dirty/evictable bytes,
  layout readiness, and reconcile signals.

  Enable debug logging:
    studiofs --log-level debug mount myvolume Z:
")]
pub(crate) struct Cli {
    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", global = true)]
    pub(crate) log_level: String,

    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        self.command.dispatch()
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init(init::InitArgs),
    Discover(discovery::DiscoverArgs),
    Mount(mount::MountArgs),
    Umount(umount::UmountArgs),
    Perf(perf::PerfArgs),
    Status(status::StatusArgs),
    Settings(settings::SettingsArgs),
    Janitor(janitor::JanitorArgs),
    Fsck(fsck::FsckArgs),
    Destroy(destroy::DestroyArgs),
    Dump(dump_load::DumpArgs),
    Load(dump_load::LoadArgs),
    Test(test::TestArgs),
    #[command(hide = true)]
    TestCache(test_cache::TestCacheArgs),
    Snapshot(snapshot::SnapshotArgs),
    Internal(internal::InternalArgs),
}

impl Commands {
    fn dispatch(self) -> anyhow::Result<()> {
        match self {
            Self::Init(args) => args.dispatch(),
            Self::Discover(args) => args.dispatch(),
            Self::Mount(args) => args.dispatch_with_auto_tray(),
            Self::Umount(args) => args.dispatch(),
            Self::Perf(args) => args.dispatch(),
            Self::Status(args) => args.dispatch(),
            Self::Settings(args) => args.dispatch(),
            Self::Janitor(args) => args.dispatch(),
            Self::Fsck(args) => args.dispatch(),
            Self::Destroy(args) => args.dispatch(),
            Self::Dump(args) => args.dispatch(),
            Self::Load(args) => args.dispatch(),
            Self::Test(args) => args.dispatch(),
            Self::TestCache(args) => args.dispatch(),
            Self::Snapshot(args) => args.dispatch(),
            Self::Internal(args) => args.dispatch(),
        }
    }
}

#[cfg(test)]
pub(crate) fn parse_for_test(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("studiofs").chain(args.iter().copied()))
}

#[cfg(test)]
fn assert_no_direct_io_in_command(cmd: &clap::Command) {
    for arg in cmd.get_arguments() {
        let name = arg.get_id().as_str();
        assert!(
            !name.starts_with("direct-io") && name != "direct_io",
            "command '{}' exposes a direct-I/O CLI flag '{}'; \
             this mode has been permanently removed (SFS-121)",
            cmd.get_name(),
            name
        );
    }
    for subcmd in cmd.get_subcommands() {
        assert_no_direct_io_in_command(subcmd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn log_level_propagates_to_mount() {
        let cli = parse_for_test(&["--log-level", "debug", "mount", "vol", "Z:"]).unwrap();
        assert_eq!(cli.log_level, "debug");
    }

    #[test]
    fn log_level_propagates_to_umount() {
        let cli = parse_for_test(&["--log-level", "trace", "umount", "Z:"]).unwrap();
        assert_eq!(cli.log_level, "trace");
    }

    #[test]
    fn test_command_parses_mount_and_report() {
        let cli =
            parse_for_test(&["test", "Z:", "--report", "reports\\studiofs-test.json"]).unwrap();
        match cli.command {
            Commands::Test(test::TestArgs { mount, report }) => {
                assert_eq!(mount, std::path::PathBuf::from("Z:"));
                assert_eq!(
                    report,
                    std::path::PathBuf::from("reports\\studiofs-test.json")
                );
            }
            other => panic!("expected Test command, got {other:?}"),
        }
    }

    #[test]
    fn perf_command_parses_sampling_and_output_flags() {
        let cli = parse_for_test(&[
            "perf",
            "Z:",
            "--interval",
            "250",
            "--count",
            "2",
            "--section",
            "bridge",
            "--section",
            "vfs-read",
            "--verbosity",
            "verbose",
            "--json-lines",
        ])
        .unwrap();

        match cli.command {
            Commands::Perf(perf::PerfArgs {
                mountpoint,
                interval,
                count,
                schema,
                sections,
                verbosity,
                json,
                json_lines,
            }) => {
                assert_eq!(mountpoint, Some(std::path::PathBuf::from("Z:")));
                assert_eq!(interval, 250);
                assert_eq!(count, Some(2));
                assert!(!schema);
                assert_eq!(sections, vec!["bridge", "vfs-read"]);
                assert_eq!(verbosity, perf::PerfVerbosity::Verbose);
                assert!(!json);
                assert!(json_lines);
            }
            other => panic!("expected Perf command, got {other:?}"),
        }
    }

    #[test]
    fn perf_command_parses_schema_and_json_flags() {
        let cli = parse_for_test(&["perf", "--schema", "--json"]).unwrap();

        match cli.command {
            Commands::Perf(perf::PerfArgs {
                mountpoint,
                schema,
                json,
                json_lines,
                ..
            }) => {
                assert_eq!(mountpoint, None);
                assert!(schema);
                assert!(json);
                assert!(!json_lines);
            }
            other => panic!("expected Perf command, got {other:?}"),
        }
    }

    #[test]
    fn help_hides_legacy_test_cache_command() {
        let command = Cli::command();
        let cmd = command
            .find_subcommand("test-cache")
            .expect("hidden test-cache parser should exist for internal reuse");

        assert!(
            cmd.is_hide_set(),
            "legacy test-cache command must stay out of the public help surface"
        );
    }

    #[test]
    fn legacy_test_cache_command_remains_parseable_for_internal_reuse() {
        let cli = parse_for_test(&["test-cache", "target/studiofs-cache-bench"]).unwrap();

        assert!(
            matches!(cli.command, Commands::TestCache(_)),
            "hidden test-cache parser should remain available until bench owns these internals"
        );
    }

    #[test]
    fn no_direct_io_flag_in_any_subcommand() {
        let cmd = Cli::command();
        assert_no_direct_io_in_command(&cmd);
    }
}
