//! Internal benchmark engine entry points shared by legacy test-cache and bench.

use anyhow::Result;
use std::path::Path;

use super::test_cache::TestCacheParams;
use super::test_cache::mounted::run_mounted_end_to_end;
use super::test_cache::result::BenchmarkReport;

pub(crate) struct MountedBenchmarkTarget<'a> {
    pub(crate) pid: u32,
    pub(crate) volume: &'a str,
    pub(crate) mount_point: &'a Path,
    pub(crate) work_root: &'a Path,
    pub(crate) cache_path: Option<&'a Path>,
}

pub(crate) struct MountedBenchmarkEngine;

impl MountedBenchmarkEngine {
    pub(crate) async fn run(
        params: &TestCacheParams,
        target: MountedBenchmarkTarget<'_>,
    ) -> Result<BenchmarkReport> {
        run_mounted_end_to_end(
            params,
            target.pid,
            target.volume,
            target.mount_point,
            target.work_root,
            target.cache_path,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::super::Cli;

    #[test]
    fn mounted_benchmark_engine_has_no_public_direct_shim_cli_surface() {
        let command = Cli::command();
        let visible_subcommands: Vec<_> = command
            .get_subcommands()
            .filter(|subcommand| !subcommand.is_hide_set())
            .map(clap::Command::get_name)
            .collect();

        assert!(
            !visible_subcommands.contains(&"test-cache"),
            "legacy direct-shim test-cache command must not be a public benchmark engine entry"
        );
    }
}
