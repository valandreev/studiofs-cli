use anyhow::Context;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use studiofs_vfs::{
    MemoryPressureController, MemoryPressureInput, MemoryPressureModel, MemoryPressureReadError,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(super) const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

pub(super) trait MemoryPressureProbe: Send + Sync {
    fn sample(&self) -> anyhow::Result<MemoryPressureInput>;
}

pub(super) struct LinuxMemoryPressureProbe {
    proc_root: PathBuf,
    cgroup_root: PathBuf,
}

impl LinuxMemoryPressureProbe {
    pub(super) fn system() -> Self {
        Self {
            proc_root: PathBuf::from("/proc"),
            cgroup_root: PathBuf::from("/sys/fs/cgroup"),
        }
    }

    #[cfg(test)]
    fn new(proc_root: PathBuf, cgroup_root: PathBuf) -> Self {
        Self {
            proc_root,
            cgroup_root,
        }
    }

    fn read_cgroup_memory(&self) -> anyhow::Result<(Option<u64>, Option<u64>)> {
        let cgroup = match fs::read_to_string(self.proc_root.join("self/cgroup")) {
            Ok(contents) => parse_unified_cgroup_path(&contents)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("read /proc/self/cgroup"),
        };
        let Some(cgroup) = cgroup else {
            return Ok((None, None));
        };
        let directory = join_cgroup_path(&self.cgroup_root, &cgroup)?;
        let limit = match fs::read_to_string(directory.join("memory.max")) {
            Ok(contents) => parse_cgroup_limit(&contents)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("read cgroup memory.max"),
        };
        let Some(limit) = limit else {
            return Ok((None, None));
        };
        let current_path = directory.join("memory.current");
        let current = match fs::read_to_string(&current_path) {
            Ok(contents) => parse_cgroup_counter(&contents)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(error).with_context(|| {
                    format!(
                        "read finite cgroup memory.current at {}",
                        current_path.display()
                    )
                });
            }
            Err(error) => return Err(error).context("read cgroup memory.current"),
        };
        Ok((Some(limit), Some(current)))
    }
}

impl MemoryPressureProbe for LinuxMemoryPressureProbe {
    fn sample(&self) -> anyhow::Result<MemoryPressureInput> {
        let meminfo =
            fs::read_to_string(self.proc_root.join("meminfo")).context("read /proc/meminfo")?;
        let (host_total_bytes, host_available_bytes) = parse_meminfo(&meminfo)?;
        let (cgroup_limit_bytes, cgroup_current_bytes) = self.read_cgroup_memory()?;
        Ok(MemoryPressureInput {
            host_total_bytes,
            host_available_bytes,
            cgroup_limit_bytes,
            cgroup_current_bytes,
        })
    }
}

pub(super) fn initialize_memory_pressure(
    probe: &dyn MemoryPressureProbe,
    mut model: MemoryPressureModel,
) -> (MemoryPressureController, MemoryPressureModel) {
    let input = probe.sample().map_err(|error| {
        tracing::warn!(error = %error, "failed to sample initial Linux memory headroom; using minimum limits");
        MemoryPressureReadError::ReadFailed
    });
    let snapshot = model.observe(input, 0);
    (MemoryPressureController::new(snapshot), model)
}

pub(super) struct MemoryPressureSampler {
    cancellation: CancellationToken,
    handle: JoinHandle<()>,
}

impl MemoryPressureSampler {
    pub(super) fn spawn(
        probe: Arc<dyn MemoryPressureProbe>,
        mut model: MemoryPressureModel,
        controller: MemoryPressureController,
        admitted_managed_bytes: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let handle = tokio::spawn(async move {
            let start = tokio::time::Instant::now() + SAMPLE_INTERVAL;
            let mut interval = tokio::time::interval_at(start, SAMPLE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = task_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let input = probe.sample().map_err(|error| {
                            tracing::warn!(error = %error, "failed to sample Linux memory headroom; retaining safe limits");
                            MemoryPressureReadError::ReadFailed
                        });
                        let snapshot = model.observe(input, admitted_managed_bytes());
                        controller.publish(snapshot);
                    }
                }
            }
        });
        Self {
            cancellation,
            handle,
        }
    }

    pub(super) async fn cancel_and_join(self) -> anyhow::Result<()> {
        self.cancellation.cancel();
        self.handle
            .await
            .context("Linux memory-pressure sampler task failed")
    }

    #[cfg(test)]
    pub(super) fn abort_for_test(&self) {
        self.handle.abort();
    }
}

pub(super) fn stop_memory_pressure_sampler(
    runtime: &tokio::runtime::Runtime,
    sampler: MemoryPressureSampler,
) -> anyhow::Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        let runtime_handle = runtime.handle().clone();
        return std::thread::Builder::new()
            .name("memory-pressure-shutdown".to_string())
            .spawn(move || runtime_handle.block_on(sampler.cancel_and_join()))
            .context("failed to spawn memory-pressure shutdown thread")?
            .join()
            .map_err(|_| anyhow::anyhow!("memory-pressure shutdown thread panicked"))?;
    }
    runtime.block_on(sampler.cancel_and_join())
}

fn parse_meminfo(contents: &str) -> anyhow::Result<(u64, u64)> {
    fn kibibytes(contents: &str, field: &str) -> anyhow::Result<u64> {
        let line = contents
            .lines()
            .find(|line| line.starts_with(field))
            .with_context(|| format!("missing {field} in /proc/meminfo"))?;
        let mut parts = line[field.len()..].split_whitespace();
        let value = parts
            .next()
            .with_context(|| format!("missing {field} value"))?
            .parse::<u64>()
            .with_context(|| format!("invalid {field} value"))?;
        if parts.next() != Some("kB") {
            anyhow::bail!("invalid {field} unit");
        }
        value
            .checked_mul(1024)
            .with_context(|| format!("{field} overflows bytes"))
    }

    Ok((
        kibibytes(contents, "MemTotal:")?,
        kibibytes(contents, "MemAvailable:")?,
    ))
}

fn parse_unified_cgroup_path(contents: &str) -> anyhow::Result<Option<PathBuf>> {
    let Some(path) = contents.lines().find_map(|line| line.strip_prefix("0::")) else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        anyhow::bail!("unified cgroup path must be absolute");
    }
    join_cgroup_path(Path::new("/"), &path)?;
    Ok(Some(path))
}

fn join_cgroup_path(root: &Path, cgroup: &Path) -> anyhow::Result<PathBuf> {
    let mut joined = root.to_path_buf();
    for component in cgroup.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => joined.push(part),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                anyhow::bail!("invalid unified cgroup path")
            }
        }
    }
    Ok(joined)
}

fn parse_cgroup_limit(contents: &str) -> anyhow::Result<Option<u64>> {
    let value = contents.trim();
    if value == "max" {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .context("invalid cgroup memory.max")
}

fn parse_cgroup_counter(contents: &str) -> anyhow::Result<u64> {
    contents
        .trim()
        .parse::<u64>()
        .context("invalid cgroup memory.current")
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use studiofs_vfs::{
        MemoryComponentLimits, MemoryPressureModel, MemoryPressureSource, MemoryPressureState,
    };

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    #[test]
    fn meminfo_parser_reads_total_and_available_kibibytes() {
        let parsed = parse_meminfo("MemTotal: 8388608 kB\nMemAvailable: 3145728 kB\n")
            .expect("valid meminfo");

        assert_eq!(parsed, (8 * GIB, 3 * GIB));
    }

    #[test]
    fn meminfo_parser_rejects_missing_required_field() {
        assert!(parse_meminfo("MemTotal: 8388608 kB\n").is_err());
    }

    #[test]
    fn meminfo_parser_rejects_malformed_required_value() {
        assert!(parse_meminfo("MemTotal: nope kB\nMemAvailable: 1 kB\n").is_err());
    }

    #[test]
    fn unified_cgroup_parser_accepts_root_and_nested_paths() {
        assert_eq!(
            parse_unified_cgroup_path("0::/\n").unwrap(),
            Some("/".into())
        );
        assert_eq!(
            parse_unified_cgroup_path("0::/user.slice/studiofs.scope\n").unwrap(),
            Some("/user.slice/studiofs.scope".into())
        );
    }

    #[test]
    fn unified_cgroup_parser_rejects_parent_traversal() {
        assert!(parse_unified_cgroup_path("0::/../escape\n").is_err());
    }

    #[test]
    fn unified_cgroup_parser_rejects_empty_v2_path() {
        assert!(parse_unified_cgroup_path("0::\n").is_err());
    }

    #[test]
    fn unified_cgroup_parser_rejects_relative_v2_path() {
        assert!(parse_unified_cgroup_path("0::relative\n").is_err());
    }

    #[test]
    fn cgroup_limit_parser_handles_finite_and_unlimited_values() {
        assert_eq!(parse_cgroup_limit("4294967296\n").unwrap(), Some(4 * GIB));
        assert_eq!(parse_cgroup_limit("max\n").unwrap(), None);
    }

    #[test]
    fn cgroup_counter_parser_rejects_malformed_values() {
        assert!(parse_cgroup_counter("unknown\n").is_err());
    }

    fn probe_root() -> tempfile::TempDir {
        studiofs_test_support::workspace_target_tempdir("memory-pressure")
    }

    fn write_probe_files(root: &Path, cgroup: &str) -> (PathBuf, PathBuf) {
        let proc_root = root.join("proc");
        let cgroup_root = root.join("cgroup");
        fs::create_dir_all(proc_root.join("self")).unwrap();
        fs::create_dir_all(cgroup_root.join("slice/test.scope")).unwrap();
        fs::write(
            proc_root.join("meminfo"),
            "MemTotal: 8388608 kB\nMemAvailable: 3145728 kB\n",
        )
        .unwrap();
        fs::write(proc_root.join("self/cgroup"), cgroup).unwrap();
        (proc_root, cgroup_root)
    }

    #[test]
    fn probe_reads_finite_nested_cgroup_memory() {
        let root = probe_root();
        let (proc_root, cgroup_root) = write_probe_files(root.path(), "0::/slice/test.scope\n");
        fs::write(
            cgroup_root.join("slice/test.scope/memory.max"),
            (4 * GIB).to_string(),
        )
        .unwrap();
        fs::write(
            cgroup_root.join("slice/test.scope/memory.current"),
            GIB.to_string(),
        )
        .unwrap();

        let sample = LinuxMemoryPressureProbe::new(proc_root, cgroup_root)
            .sample()
            .unwrap();

        assert_eq!(sample.cgroup_limit_bytes, Some(4 * GIB));
        assert_eq!(sample.cgroup_current_bytes, Some(GIB));
    }

    #[test]
    fn probe_treats_missing_cgroup_controller_as_host_only() {
        let root = probe_root();
        let (proc_root, cgroup_root) = write_probe_files(root.path(), "0::/slice/test.scope\n");

        let sample = LinuxMemoryPressureProbe::new(proc_root, cgroup_root)
            .sample()
            .unwrap();

        assert_eq!(sample.cgroup_limit_bytes, None);
        assert_eq!(sample.cgroup_current_bytes, None);
    }

    #[test]
    fn probe_rejects_finite_limit_without_current_counter() {
        let root = probe_root();
        let (proc_root, cgroup_root) = write_probe_files(root.path(), "0::/slice/test.scope\n");
        fs::write(
            cgroup_root.join("slice/test.scope/memory.max"),
            (4 * GIB).to_string(),
        )
        .unwrap();

        let error = LinuxMemoryPressureProbe::new(proc_root, cgroup_root)
            .sample()
            .expect_err("finite cgroup limit requires a current usage counter");

        assert!(error.to_string().contains("memory.current"));
    }

    #[test]
    fn probe_rejects_malformed_existing_cgroup_counter() {
        let root = probe_root();
        let (proc_root, cgroup_root) = write_probe_files(root.path(), "0::/slice/test.scope\n");
        fs::write(cgroup_root.join("slice/test.scope/memory.max"), "4096\n").unwrap();
        fs::write(
            cgroup_root.join("slice/test.scope/memory.current"),
            "invalid\n",
        )
        .unwrap();

        assert!(
            LinuxMemoryPressureProbe::new(proc_root, cgroup_root)
                .sample()
                .is_err()
        );
    }

    #[test]
    fn probe_reads_root_cgroup_and_ignores_unlimited_limit() {
        let root = probe_root();
        let (proc_root, cgroup_root) = write_probe_files(root.path(), "0::/\n");
        fs::write(cgroup_root.join("memory.max"), "max\n").unwrap();

        let sample = LinuxMemoryPressureProbe::new(proc_root, cgroup_root)
            .sample()
            .unwrap();

        assert_eq!(sample.cgroup_limit_bytes, None);
        assert_eq!(sample.cgroup_current_bytes, None);
    }

    #[test]
    fn probe_fails_when_required_meminfo_is_unreadable() {
        let root = probe_root();
        let proc_root = root.path().join("missing-proc");

        assert!(
            LinuxMemoryPressureProbe::new(proc_root, root.path().join("cgroup"))
                .sample()
                .is_err()
        );
    }

    struct FakeProbe {
        samples: AtomicUsize,
        results: Mutex<VecDeque<anyhow::Result<MemoryPressureInput>>>,
    }

    impl FakeProbe {
        fn new(results: impl IntoIterator<Item = anyhow::Result<MemoryPressureInput>>) -> Self {
            Self {
                samples: AtomicUsize::new(0),
                results: Mutex::new(results.into_iter().collect()),
            }
        }
    }

    impl MemoryPressureProbe for FakeProbe {
        fn sample(&self) -> anyhow::Result<MemoryPressureInput> {
            self.samples.fetch_add(1, Ordering::Relaxed);
            self.results
                .lock()
                .pop_front()
                .unwrap_or_else(|| anyhow::bail!("no fake sample"))
        }
    }

    fn model() -> MemoryPressureModel {
        MemoryPressureModel::new(
            MemoryComponentLimits {
                memory_cache_bytes: 2 * GIB,
                dirty_bytes: 800 * MIB,
                transit_bytes: 800 * MIB,
                async_write_bytes: 256 * MIB,
            },
            16 * MIB,
            MIB,
        )
    }

    fn host_input(available_bytes: u64) -> MemoryPressureInput {
        MemoryPressureInput {
            host_total_bytes: 8 * GIB,
            host_available_bytes: available_bytes,
            cgroup_limit_bytes: None,
            cgroup_current_bytes: None,
        }
    }

    #[test]
    fn initial_probe_failure_publishes_fallback_without_failing_mount() {
        let probe = FakeProbe::new([Err(anyhow::anyhow!("unreadable"))]);
        let (controller, _) = initialize_memory_pressure(&probe, model());

        let snapshot = controller.snapshot();
        assert_eq!(snapshot.source, MemoryPressureSource::Fallback);
        assert_eq!(snapshot.state, MemoryPressureState::Constrained);
        assert_eq!(probe.samples.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn initial_host_sample_publishes_eight_gib_three_gib_limits() {
        let probe = FakeProbe::new([Ok(host_input(3 * GIB))]);
        let (controller, _) = initialize_memory_pressure(&probe, model());

        let snapshot = controller.snapshot();
        assert_eq!(snapshot.source, MemoryPressureSource::Host);
        assert_eq!(snapshot.effective_limit_bytes, 8 * GIB);
        assert_eq!(snapshot.available_bytes, 3 * GIB);
        let reserve = 8 * GIB * 15 / 100;
        let envelope = (3 * GIB - reserve) * 75 / 100;
        assert_eq!(snapshot.envelope_bytes, envelope);
        assert_eq!(snapshot.limits.memory_cache_bytes, envelope * 40 / 100);
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_takes_one_sample_per_interval_and_stops_after_join() {
        let probe = Arc::new(FakeProbe::new([
            Ok(host_input(3 * GIB)),
            Ok(host_input(3 * GIB)),
        ]));
        let (controller, model) = initialize_memory_pressure(probe.as_ref(), model());
        let sampler =
            MemoryPressureSampler::spawn(probe.clone(), model, controller, Arc::new(|| 0));
        tokio::task::yield_now().await;

        tokio::time::advance(SAMPLE_INTERVAL - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(probe.samples.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(probe.samples.load(Ordering::Relaxed), 2);

        sampler.cancel_and_join().await.unwrap();
        tokio::time::advance(SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(probe.samples.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_error_retains_previous_snapshot() {
        let probe = Arc::new(FakeProbe::new([
            Ok(host_input(3 * GIB)),
            Err(anyhow::anyhow!("transient")),
        ]));
        let (controller, model) = initialize_memory_pressure(probe.as_ref(), model());
        let before = controller.snapshot();
        let sampler =
            MemoryPressureSampler::spawn(probe, model, controller.clone(), Arc::new(|| 0));
        tokio::task::yield_now().await;

        tokio::time::advance(SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;

        let after = controller.snapshot();
        assert_eq!(after.available_bytes, before.available_bytes);
        assert_eq!(after.sampling_failure_count, 1);
        sampler.cancel_and_join().await.unwrap();
    }

    #[test]
    fn sampler_shutdown_joins_before_runtime_is_dropped() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let probe = Arc::new(FakeProbe::new([Ok(host_input(3 * GIB))]));
        let (controller, model) = initialize_memory_pressure(probe.as_ref(), model());
        let retained = Arc::new(());
        let retained_weak = Arc::downgrade(&retained);
        let sampler = {
            let retained = retained.clone();
            let _guard = runtime.enter();
            MemoryPressureSampler::spawn(
                probe,
                model,
                controller,
                Arc::new(move || {
                    let _ = &retained;
                    0
                }),
            )
        };
        drop(retained);

        stop_memory_pressure_sampler(&runtime, sampler).unwrap();

        assert!(retained_weak.upgrade().is_none());
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
}
