//! Mounted single-client performance probe for large sequential I/O diagnostics.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::client::{
    ClientSpec, MultiClientController, SystemClientProcessExecutor, VolumeBootstrapOptions,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioDefinition, ScenarioError};
use studiofs_data::DataStoreConfig;
use studiofs_meta::{MetaFormat as _, PgMetaEngine, ROOT_INO};
use studiofs_vfs::{StudioVfs, VfsConfigRaw};

/// Mounted performance probe scenario identifier.
pub const MOUNTED_PERFORMANCE_PROBE: &str = "mounted-performance-probe";
/// Mounted FUSE write baseline scenario identifier for SFS-469.
pub const MOUNTED_FUSE_WRITE_BASELINE: &str = "mounted-fuse-write-baseline";

const PERFORMANCE_PROBE_CLIENT: &str = "client-a";
const PERFORMANCE_PROBE_FILE: &str = "performance-probe/probe-data.bin";
const PERFORMANCE_PROBE_SUMMARY_FILE: &str = "performance-probe-summary.json";
const PERFORMANCE_PROBE_VALIDATION_FILE: &str = "performance-probe-validation.json";
const PERFORMANCE_PROBE_SAMPLES_FILE: &str = "performance-probe-samples.ndjson";
const WRITE_PATH_OBSERVABILITY_FILE: &str = "write_path_observability.json";
const DEFAULT_FILE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const SEEDED_VFS_DIRTY_LIMIT_BYTES: u64 = 32 * 1024 * 1024;
const SEEDED_VFS_CACHE_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_READ_PASSES: u32 = 10;
const DEFAULT_SAMPLE_DELAY_MS: u64 = 1_200;
const DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECS: u64 = 600;
const DEFAULT_UPLOAD_DRAIN_POLL_MS: u64 = 1_000;
const METRICS_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const METRICS_WAIT_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct MountedPerformanceProbe;

impl Scenario for MountedPerformanceProbe {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_PERFORMANCE_PROBE,
            description: "mounted 4GiB write and repeated sequential read performance probe",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        run_mounted_performance_probe_scenario(context, MOUNTED_PERFORMANCE_PROBE)
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        validate_mounted_performance_probe(context, MOUNTED_PERFORMANCE_PROBE)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

/// SFS-469 mounted FUSE write baseline scenario backed by the shared performance probe.
#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct MountedFuseWriteBaseline;

impl Scenario for MountedFuseWriteBaseline {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_FUSE_WRITE_BASELINE,
            description: "SFS-469 macOS/Linux FUSE write baseline using the mounted performance probe",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        run_mounted_performance_probe_scenario(context, MOUNTED_FUSE_WRITE_BASELINE)
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        validate_mounted_performance_probe(context, MOUNTED_FUSE_WRITE_BASELINE)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn run_mounted_performance_probe_scenario(
    context: &ScenarioContext,
    scenario_name: &'static str,
) -> Result<(), ScenarioError> {
    let Some(config) = &context.client_controller_config else {
        return Err(ScenarioError::MissingComposeInfrastructure);
    };
    let mut controller = MultiClientController::new(config.clone(), SystemClientProcessExecutor);
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec![PERFORMANCE_PROBE_CLIENT.to_string()],
        clean_existing_local_metadata: true,
    })?;
    let config = PerformanceProbeConfig::from_env()?;
    if config.seed_mode == PerformanceProbeSeedMode::ReadOnlySeeded {
        let client = controller.client_at(0)?;
        seed_performance_probe_payload(client, &config)?;
        controller.set_client_env_overrides(
            PERFORMANCE_PROBE_CLIENT,
            read_only_bridge_mount_overrides(),
        )?;
        controller.set_client_mount_arg_overrides(
            PERFORMANCE_PROBE_CLIENT,
            read_only_bridge_mount_arg_overrides(),
        )?;
    }
    controller.mount(PERFORMANCE_PROBE_CLIENT)?;
    let client = controller.client_at(0)?;

    let run_result = run_mounted_performance_probe(context, client, scenario_name, config);
    let cleanup_result = controller.unmount_all();
    match (run_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(ScenarioError::Client(cleanup)),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::PerformanceProbeUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
    }
}

fn validate_mounted_performance_probe(
    context: &ScenarioContext,
    scenario_name: &'static str,
) -> Result<Vec<String>, ScenarioError> {
    let summary_path = context.scenario_dir.join(PERFORMANCE_PROBE_SUMMARY_FILE);
    let summary: PerformanceProbeSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
    if summary.status != "passed" {
        return Err(ScenarioError::PerformanceProbeValidationFailed {
            detail: format!("performance probe status is {}", summary.status),
        });
    }
    if summary.scenario != scenario_name {
        return Err(ScenarioError::PerformanceProbeValidationFailed {
            detail: format!(
                "performance probe summary scenario is {}, expected {scenario_name}",
                summary.scenario
            ),
        });
    }
    if let Some(upload_drain) = &summary.upload_drain
        && let Some(detail) = upload_drain_failure_detail(upload_drain)
    {
        return Err(ScenarioError::PerformanceProbeValidationFailed { detail });
    }

    let read_phases = summary
        .phases
        .iter()
        .filter(|phase| phase.kind == "read")
        .count();
    if read_phases != summary.config.read_passes as usize {
        return Err(ScenarioError::PerformanceProbeValidationFailed {
            detail: format!(
                "performance probe recorded {read_phases} read phases, expected {}",
                summary.config.read_passes
            ),
        });
    }

    let validation = json!({
        "scenario": scenario_name,
        "run_id": context.run_id,
        "seed": context.seed,
        "status": "passed",
        "file_bytes": summary.config.file_bytes,
        "read_passes": summary.config.read_passes,
        "read_observations": summary.read_observations,
        "write_observations": summary.write_observations,
        "upload_drain": summary.upload_drain,
    });
    fs::write(
        context.scenario_dir.join(PERFORMANCE_PROBE_VALIDATION_FILE),
        serde_json::to_vec_pretty(&validation)?,
    )?;
    Ok(vec![
        format!("artifacts/scenarios/{scenario_name}/{PERFORMANCE_PROBE_SUMMARY_FILE}"),
        format!("artifacts/scenarios/{scenario_name}/{PERFORMANCE_PROBE_SAMPLES_FILE}"),
        format!("artifacts/scenarios/{scenario_name}/{PERFORMANCE_PROBE_VALIDATION_FILE}"),
    ])
}

fn run_mounted_performance_probe(
    context: &ScenarioContext,
    client: &ClientSpec,
    scenario_name: &'static str,
    config: PerformanceProbeConfig,
) -> Result<(), ScenarioError> {
    let samples_path = context.scenario_dir.join(PERFORMANCE_PROBE_SAMPLES_FILE);
    let metrics_path = client.cache_dir.join(WRITE_PATH_OBSERVABILITY_FILE);
    let probe_path = client.mount_point.join(PERFORMANCE_PROBE_FILE);
    let mut phases = Vec::with_capacity(config.read_passes as usize + 2);

    let expected_checksum = match config.seed_mode {
        PerformanceProbeSeedMode::MountedWrite => {
            let before_write = sample_metrics(&metrics_path)?;
            append_sample(
                &samples_path,
                "before-write",
                before_write.as_ref(),
                &metrics_path,
            )?;
            let write_io = write_probe_file(&probe_path, config.file_bytes, config.chunk_bytes)?;
            let after_write = sample_metrics_after_delay(&metrics_path, config.sample_delay)?;
            append_sample(
                &samples_path,
                "after-write",
                after_write.as_ref(),
                &metrics_path,
            )?;
            let expected_checksum = write_io.checksum;
            phases.push(PerformanceProbePhase::new(
                "write".to_string(),
                "write".to_string(),
                None,
                write_io,
                &before_write,
                &after_write,
            ));
            expected_checksum
        }
        PerformanceProbeSeedMode::ReadOnlySeeded => {
            expected_probe_checksum(config.file_bytes, config.chunk_bytes)
        }
    };

    for pass in 1..=config.read_passes {
        let before = sample_metrics(&metrics_path)?;
        append_sample(
            &samples_path,
            &format!("before-read-pass-{pass}"),
            before.as_ref(),
            &metrics_path,
        )?;
        let read_io = read_probe_file(&probe_path, config.chunk_bytes, config.file_bytes)?;
        verify_probe_checksum(pass, read_io.checksum, expected_checksum)?;
        let after = sample_metrics_after_delay(&metrics_path, config.sample_delay)?;
        append_sample(
            &samples_path,
            &format!("after-read-pass-{pass}"),
            after.as_ref(),
            &metrics_path,
        )?;
        phases.push(PerformanceProbePhase::new(
            format!("read-pass-{pass}"),
            "read".to_string(),
            Some(pass),
            read_io,
            &before,
            &after,
        ));
    }

    let upload_drain = if config.wait_for_upload_drain {
        let drain = wait_for_upload_drain(
            &metrics_path,
            config.upload_drain_timeout,
            config.upload_drain_poll,
        )?;
        append_sample(
            &samples_path,
            "after-upload-drain-wait",
            drain.after.as_ref(),
            &metrics_path,
        )?;
        let before = drain.after.clone();
        let post_drain_io = read_probe_file(&probe_path, config.chunk_bytes, config.file_bytes)?;
        verify_probe_checksum(0, post_drain_io.checksum, expected_checksum)?;
        let after = sample_metrics_after_delay(&metrics_path, config.sample_delay)?;
        append_sample(
            &samples_path,
            "after-upload-drain-read",
            after.as_ref(),
            &metrics_path,
        )?;
        phases.push(PerformanceProbePhase::new(
            "read-after-upload-drain-wait".to_string(),
            "post-drain-read".to_string(),
            None,
            post_drain_io,
            &before,
            &after,
        ));
        Some(drain.into_observation())
    } else {
        None
    };

    let summary = PerformanceProbeSummary {
        scenario: scenario_name.to_string(),
        run_id: context.run_id.clone(),
        seed: context.seed,
        status: "passed".to_string(),
        client: client.name.clone(),
        mount_point: client.mount_point.display().to_string(),
        file_path: probe_path.display().to_string(),
        metrics_path: metrics_path.display().to_string(),
        samples_path: samples_path.display().to_string(),
        config: PerformanceProbeConfigSummary::from(config),
        bootstrap_volume: client.volume_name.clone(),
        phases,
        upload_drain,
        write_observations: PerformanceProbeWriteObservations::from_phases(&[]),
        read_observations: PerformanceProbeReadObservations::default(),
    };
    let summary = summary.with_observations();
    fs::write(
        context.scenario_dir.join(PERFORMANCE_PROBE_SUMMARY_FILE),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

fn seed_performance_probe_payload(
    client: &ClientSpec,
    config: &PerformanceProbeConfig,
) -> Result<(), ScenarioError> {
    let runtime = tokio::runtime::Runtime::new()?;
    let _guard = runtime.enter();
    let meta = PgMetaEngine::open(&client.local_metadata_db)
        .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?;
    let format_info = meta
        .get_format_info()
        .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?;
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = client.cache_dir.to_string_lossy().into_owned();
    let cache_budget_bytes = seeded_vfs_cache_budget_bytes(config.file_bytes);
    let vfs = StudioVfs::new(
        Arc::clone(&meta) as Arc<dyn studiofs_meta::MetaEngine>,
        &data_config,
        &cache_dir,
        cache_budget_bytes,
        SEEDED_VFS_DIRTY_LIMIT_BYTES,
        VfsConfigRaw::default()
            .validate()
            .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?,
    )
    .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?;
    vfs.start()
        .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?;
    let mut vfs_guard = SeedVfsShutdownGuard::new(&runtime, vfs);
    let vfs = vfs_guard.vfs();

    let dir_attr = match vfs.mkdir(ROOT_INO, b"performance-probe", 0, 0, 0o755) {
        Ok(attr) => attr,
        Err(studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryExists { .. })) => vfs
            .lookup(ROOT_INO, b"performance-probe")
            .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?,
        Err(error) => return Err(ScenarioError::Io(std::io::Error::other(error))),
    };
    let file_attr = match vfs.create(dir_attr.ino, b"probe-data.bin", 0, 0, 0o644) {
        Ok(attr) => attr,
        Err(studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryExists { .. })) => vfs
            .lookup(dir_attr.ino, b"probe-data.bin")
            .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?,
        Err(error) => return Err(ScenarioError::Io(std::io::Error::other(error))),
    };

    let mut buffer = vec![0u8; config.chunk_bytes];
    fill_probe_buffer(&mut buffer);
    let mut remaining = config.file_bytes;
    let mut offset = 0u64;
    while remaining > 0 {
        let len = remaining.min(buffer.len() as u64) as usize;
        runtime
            .block_on(vfs.write(file_attr.ino, offset, &buffer[..len]))
            .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?;
        offset = offset.saturating_add(len as u64);
        remaining -= len as u64;
    }
    runtime
        .block_on(vfs.flush(file_attr.ino))
        .map_err(|error| ScenarioError::Io(std::io::Error::other(error)))?;
    vfs_guard.shutdown();
    Ok(())
}

pub(crate) fn seeded_vfs_cache_budget_bytes(file_bytes: u64) -> u64 {
    file_bytes
        .saturating_add(SEEDED_VFS_DIRTY_LIMIT_BYTES)
        .saturating_add(SEEDED_VFS_CACHE_HEADROOM_BYTES)
}

struct SeedVfsShutdownGuard<'a> {
    runtime: &'a tokio::runtime::Runtime,
    vfs: Option<Arc<StudioVfs>>,
}

impl<'a> SeedVfsShutdownGuard<'a> {
    fn new(runtime: &'a tokio::runtime::Runtime, vfs: Arc<StudioVfs>) -> Self {
        Self {
            runtime,
            vfs: Some(vfs),
        }
    }

    fn vfs(&self) -> &StudioVfs {
        self.vfs
            .as_deref()
            .expect("seed VFS guard should hold a VFS before shutdown")
    }

    fn shutdown(&mut self) {
        if let Some(vfs) = self.vfs.take() {
            self.runtime.block_on(vfs.shutdown(Duration::from_secs(30)));
        }
    }
}

impl Drop for SeedVfsShutdownGuard<'_> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn write_probe_file(
    path: &Path,
    file_bytes: u64,
    chunk_bytes: usize,
) -> Result<PhaseIoResult, ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut buffer = vec![0u8; chunk_bytes];
    fill_probe_buffer(&mut buffer);
    let started = Instant::now();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let mut remaining = file_bytes;
    let mut written = 0u64;
    let mut checksum = 0u64;
    while remaining > 0 {
        let len = remaining.min(buffer.len() as u64) as usize;
        file.write_all(&buffer[..len])?;
        checksum = update_probe_checksum(checksum, &buffer[..len], written);
        written = written.saturating_add(len as u64);
        remaining -= len as u64;
    }
    file.flush()?;
    drop(file);
    Ok(PhaseIoResult::new(written, started.elapsed(), checksum))
}

fn read_probe_file(
    path: &Path,
    chunk_bytes: usize,
    expected_bytes: u64,
) -> Result<PhaseIoResult, ScenarioError> {
    let started = Instant::now();
    let mut file = File::open(path)?;
    let mut buffer = vec![0u8; chunk_bytes];
    let mut read_total = 0u64;
    let mut checksum = 0u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        checksum = update_probe_checksum(checksum, &buffer[..n], read_total);
        read_total = read_total.saturating_add(n as u64);
    }
    if read_total != expected_bytes {
        return Err(ScenarioError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("read {read_total} bytes, expected {expected_bytes}"),
        )));
    }
    Ok(PhaseIoResult::new(read_total, started.elapsed(), checksum))
}

fn fill_probe_buffer(buffer: &mut [u8]) {
    for (index, byte) in buffer.iter_mut().enumerate() {
        *byte = (index as u8).wrapping_mul(31).wrapping_add(17);
    }
}

fn update_probe_checksum(mut checksum: u64, bytes: &[u8], offset: u64) -> u64 {
    if bytes.is_empty() {
        return checksum;
    }
    checksum = checksum.rotate_left(7) ^ offset;
    checksum ^= bytes[0] as u64;
    checksum ^= (bytes[bytes.len() / 2] as u64) << 16;
    checksum ^= (bytes[bytes.len() - 1] as u64) << 32;
    checksum.wrapping_add(bytes.len() as u64)
}

fn verify_probe_checksum(pass: u32, actual: u64, expected: u64) -> Result<(), ScenarioError> {
    if actual == expected {
        return Ok(());
    }
    Err(ScenarioError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("read pass {pass} checksum {actual} did not match write checksum {expected}"),
    )))
}

fn expected_probe_checksum(file_bytes: u64, chunk_bytes: usize) -> u64 {
    let mut buffer = vec![0u8; chunk_bytes];
    fill_probe_buffer(&mut buffer);
    let mut remaining = file_bytes;
    let mut written = 0u64;
    let mut checksum = 0u64;
    while remaining > 0 {
        let len = remaining.min(buffer.len() as u64) as usize;
        checksum = update_probe_checksum(checksum, &buffer[..len], written);
        written = written.saturating_add(len as u64);
        remaining -= len as u64;
    }
    checksum
}

fn sample_metrics_after_delay(
    metrics_path: &Path,
    delay: Duration,
) -> Result<Option<Value>, ScenarioError> {
    if !delay.is_zero() {
        thread::sleep(delay);
    }
    sample_metrics(metrics_path)
}

fn sample_metrics(metrics_path: &Path) -> Result<Option<Value>, ScenarioError> {
    let deadline = Instant::now() + METRICS_WAIT_TIMEOUT;
    loop {
        let Some(resolved_metrics_path) = find_existing_metrics_path(metrics_path)? else {
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(METRICS_WAIT_POLL);
            continue;
        };
        match fs::read(&resolved_metrics_path) {
            Ok(bytes) => return Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                thread::sleep(METRICS_WAIT_POLL);
            }
            Err(error) => return Err(ScenarioError::Io(error)),
        }
    }
}

fn find_existing_metrics_path(metrics_path: &Path) -> Result<Option<PathBuf>, ScenarioError> {
    if metrics_path.exists() {
        return Ok(Some(metrics_path.to_path_buf()));
    }
    let Some(parent) = metrics_path.parent() else {
        return Ok(None);
    };
    let Some(file_name) = metrics_path.file_name() else {
        return Ok(None);
    };
    match fs::read_dir(parent) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let candidate = entry.path().join(file_name);
                if candidate.exists() {
                    return Ok(Some(candidate));
                }
            }
            Ok(None)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ScenarioError::Io(error)),
    }
}

fn wait_for_upload_drain(
    metrics_path: &Path,
    timeout: Duration,
    poll: Duration,
) -> Result<UploadDrainResult, ScenarioError> {
    let started = Instant::now();
    let before = sample_metrics(metrics_path)?;
    let mut after = before.clone();
    let mut samples = vec![UploadDrainSample {
        elapsed_ms: 0,
        snapshot: before.clone(),
    }];
    let poll = if poll.is_zero() {
        Duration::from_millis(1)
    } else {
        poll
    };
    let mut timed_out = false;
    loop {
        if after.as_ref().is_some_and(upload_path_is_drained) {
            break;
        }
        if started.elapsed() >= timeout {
            timed_out = true;
            break;
        }
        thread::sleep(poll);
        after = sample_metrics(metrics_path)?;
        samples.push(UploadDrainSample {
            elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            snapshot: after.clone(),
        });
    }

    Ok(UploadDrainResult {
        before,
        after,
        samples,
        elapsed: started.elapsed(),
        timed_out,
    })
}

fn classify_upload_drain(
    timed_out: bool,
    pending_uploads_after: u64,
    pending_upload_bytes_after: u64,
    max_active_uploads: u64,
    completed_uploads_delta: u64,
) -> String {
    if !timed_out {
        return "drained".to_string();
    }
    if pending_upload_bytes_after > 0 && max_active_uploads == 0 && completed_uploads_delta == 0 {
        return "pending-without-active-uploads".to_string();
    }
    if pending_uploads_after > 0 && completed_uploads_delta == 0 {
        return "pending-without-completions".to_string();
    }
    "drain-timeout".to_string()
}

fn upload_drain_failure_detail(observation: &UploadDrainObservation) -> Option<String> {
    if !observation.timed_out {
        return None;
    }
    Some(format!(
        "upload drain timed out; classification={}; pending_uploads_after={}; pending_upload_bytes_after={}; active_uploads_after={}; max_active_uploads={}; completed_uploads_delta={}; oldest_pending_upload_age_ms_after={}; samples={}",
        observation.classification,
        observation.pending_uploads_after,
        observation.pending_upload_bytes_after,
        observation.active_uploads_after,
        observation.max_active_uploads,
        observation.completed_uploads_delta,
        observation.oldest_pending_upload_age_ms_after,
        observation.sample_count,
    ))
}

fn upload_path_is_drained(snapshot: &Value) -> bool {
    metric_value(Some(snapshot), &["upload", "pending_uploads"]) == 0
        && metric_value(Some(snapshot), &["upload", "pending_upload_bytes"]) == 0
        && metric_value(Some(snapshot), &["dirty_pressure", "total_dirty_bytes"]) == 0
        && metric_value(Some(snapshot), &["drain_lag", "current_bytes"]) == 0
}

fn append_sample(
    samples_path: &Path,
    label: &str,
    snapshot: Option<&Value>,
    metrics_path: &Path,
) -> Result<(), ScenarioError> {
    if let Some(parent) = samples_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let record = json!({
        "label": label,
        "generated_unix_ms": unix_millis(),
        "metrics_path": metrics_path.display().to_string(),
        "resolved_metrics_path": find_existing_metrics_path(metrics_path)?
            .map(|path| path.display().to_string()),
        "snapshot": snapshot,
    });
    let mut record_bytes = serde_json::to_vec(&record)?;
    record_bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(samples_path)?;
    file.write_all(&record_bytes)?;
    Ok(())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PerformanceProbeConfig {
    pub(crate) file_bytes: u64,
    pub(crate) chunk_bytes: usize,
    pub(crate) read_passes: u32,
    pub(crate) sample_delay: Duration,
    pub(crate) wait_for_upload_drain: bool,
    pub(crate) upload_drain_timeout: Duration,
    pub(crate) upload_drain_poll: Duration,
    pub(crate) seed_mode: PerformanceProbeSeedMode,
}

impl PerformanceProbeConfig {
    fn from_env() -> Result<Self, ScenarioError> {
        Ok(Self {
            file_bytes: env_u64("STUDIOFS_E2E_PERF_FILE_BYTES", DEFAULT_FILE_BYTES)?,
            chunk_bytes: env_usize("STUDIOFS_E2E_PERF_CHUNK_BYTES", DEFAULT_CHUNK_BYTES)?,
            read_passes: env_u32("STUDIOFS_E2E_PERF_READ_PASSES", DEFAULT_READ_PASSES)?,
            sample_delay: Duration::from_millis(env_u64(
                "STUDIOFS_E2E_PERF_SAMPLE_AFTER_PHASE_MS",
                DEFAULT_SAMPLE_DELAY_MS,
            )?),
            wait_for_upload_drain: env_bool("STUDIOFS_E2E_PERF_WAIT_FOR_UPLOAD_DRAIN", true)?,
            upload_drain_timeout: Duration::from_secs(env_u64(
                "STUDIOFS_E2E_PERF_UPLOAD_DRAIN_TIMEOUT_SECS",
                DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECS,
            )?),
            upload_drain_poll: Duration::from_millis(env_u64(
                "STUDIOFS_E2E_PERF_UPLOAD_DRAIN_POLL_MS",
                DEFAULT_UPLOAD_DRAIN_POLL_MS,
            )?),
            seed_mode: PerformanceProbeSeedMode::from_env()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PerformanceProbeSeedMode {
    MountedWrite,
    ReadOnlySeeded,
}

impl PerformanceProbeSeedMode {
    fn from_env() -> Result<Self, ScenarioError> {
        match env::var("STUDIOFS_E2E_PERF_SEED_MODE") {
            Ok(value) if !value.trim().is_empty() => Self::parse(&value),
            _ => Ok(Self::platform_default()),
        }
    }

    fn platform_default() -> Self {
        if cfg!(target_os = "macos") {
            Self::ReadOnlySeeded
        } else {
            Self::MountedWrite
        }
    }

    fn parse(raw: &str) -> Result<Self, ScenarioError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "mounted-write" | "mounted_write" | "write" => Ok(Self::MountedWrite),
            "read-only-seeded" | "read_only_seeded" | "macfuse-read-only" | "macfuse" => {
                Ok(Self::ReadOnlySeeded)
            }
            other => Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("STUDIOFS_E2E_PERF_SEED_MODE has unsupported value {other:?}"),
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::MountedWrite => "mounted_write",
            Self::ReadOnlySeeded => "read_only_seeded",
        }
    }
}

fn read_only_bridge_mount_overrides() -> Vec<(String, String)> {
    let mut overrides = Vec::new();
    push_read_only_bridge_mount_overrides(&mut overrides);
    overrides
}

fn read_only_bridge_mount_arg_overrides() -> Vec<String> {
    let mut overrides = Vec::new();
    push_read_only_bridge_mount_arg_overrides(&mut overrides);
    overrides
}

pub(crate) fn push_read_only_bridge_mount_arg_overrides(overrides: &mut Vec<String>) {
    overrides.push("--read-only".to_string());
}

pub(crate) fn push_read_only_bridge_mount_overrides(overrides: &mut Vec<(String, String)>) {
    overrides.push(("STUDIOFS_MOUNT_BACKEND".to_string(), "macfuse".to_string()));
}

fn env_bool(key: &str, default: bool) -> Result<bool, ScenarioError> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{key} must be boolean, got {other:?}"),
            ))),
        },
        _ => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, ScenarioError> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => value.trim().parse::<u64>().map_err(|error| {
            ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{key} must be an unsigned integer: {error}"),
            ))
        }),
        _ => Ok(default),
    }
}

fn env_u32(key: &str, default: u32) -> Result<u32, ScenarioError> {
    let value = env_u64(key, u64::from(default))?;
    u32::try_from(value).map_err(|error| {
        ScenarioError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{key} is too large for u32: {error}"),
        ))
    })
}

fn env_usize(key: &str, default: usize) -> Result<usize, ScenarioError> {
    let value = env_u64(key, default as u64)?;
    usize::try_from(value).map_err(|error| {
        ScenarioError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{key} is too large for usize: {error}"),
        ))
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerformanceProbeSummary {
    scenario: String,
    run_id: String,
    seed: u64,
    status: String,
    client: String,
    mount_point: String,
    file_path: String,
    metrics_path: String,
    samples_path: String,
    config: PerformanceProbeConfigSummary,
    bootstrap_volume: String,
    phases: Vec<PerformanceProbePhase>,
    upload_drain: Option<UploadDrainObservation>,
    write_observations: PerformanceProbeWriteObservations,
    read_observations: PerformanceProbeReadObservations,
}

impl PerformanceProbeSummary {
    fn with_observations(mut self) -> Self {
        self.write_observations = PerformanceProbeWriteObservations::from_phases(&self.phases);
        self.read_observations = PerformanceProbeReadObservations::from_phases(&self.phases);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PerformanceProbeConfigSummary {
    pub(crate) file_bytes: u64,
    pub(crate) chunk_bytes: usize,
    pub(crate) read_passes: u32,
    pub(crate) sample_after_phase_ms: u64,
    pub(crate) wait_for_upload_drain: bool,
    pub(crate) upload_drain_timeout_ms: u64,
    pub(crate) upload_drain_poll_ms: u64,
    pub(crate) seed_mode: String,
}

impl From<PerformanceProbeConfig> for PerformanceProbeConfigSummary {
    fn from(config: PerformanceProbeConfig) -> Self {
        Self {
            file_bytes: config.file_bytes,
            chunk_bytes: config.chunk_bytes,
            read_passes: config.read_passes,
            sample_after_phase_ms: config.sample_delay.as_millis().min(u64::MAX as u128) as u64,
            wait_for_upload_drain: config.wait_for_upload_drain,
            upload_drain_timeout_ms: config
                .upload_drain_timeout
                .as_millis()
                .min(u64::MAX as u128) as u64,
            upload_drain_poll_ms: config.upload_drain_poll.as_millis().min(u64::MAX as u128) as u64,
            seed_mode: config.seed_mode.as_str().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct UploadDrainResult {
    before: Option<Value>,
    after: Option<Value>,
    samples: Vec<UploadDrainSample>,
    elapsed: Duration,
    timed_out: bool,
}

impl UploadDrainResult {
    fn into_observation(self) -> UploadDrainObservation {
        let samples: Vec<UploadDrainSampleObservation> = self
            .samples
            .iter()
            .map(UploadDrainSampleObservation::from_sample)
            .collect();
        let sample_count = samples.len();
        let max_active_uploads = samples
            .iter()
            .map(|sample| sample.active_uploads)
            .max()
            .unwrap_or(0);
        let completed_uploads_before =
            metric_value(self.before.as_ref(), &["upload", "completed_uploads"]);
        let completed_uploads_after =
            metric_value(self.after.as_ref(), &["upload", "completed_uploads"]);
        let completed_uploads_delta =
            completed_uploads_after.saturating_sub(completed_uploads_before);
        let pending_uploads_after =
            metric_value(self.after.as_ref(), &["upload", "pending_uploads"]);
        let pending_upload_bytes_after =
            metric_value(self.after.as_ref(), &["upload", "pending_upload_bytes"]);
        let oldest_pending_upload_age_ms_after = metric_value(
            self.after.as_ref(),
            &["upload", "oldest_pending_upload_age_ms"],
        );
        let stalled_pending_without_active_uploads = self.timed_out
            && pending_upload_bytes_after > 0
            && max_active_uploads == 0
            && completed_uploads_delta == 0;
        let classification = classify_upload_drain(
            self.timed_out,
            pending_uploads_after,
            pending_upload_bytes_after,
            max_active_uploads,
            completed_uploads_delta,
        );

        UploadDrainObservation {
            waited_ms: self.elapsed.as_millis().min(u64::MAX as u128) as u64,
            timed_out: self.timed_out,
            pending_uploads_before: metric_value(
                self.before.as_ref(),
                &["upload", "pending_uploads"],
            ),
            pending_uploads_after,
            pending_upload_bytes_before: metric_value(
                self.before.as_ref(),
                &["upload", "pending_upload_bytes"],
            ),
            pending_upload_bytes_after,
            dirty_total_bytes_before: metric_value(
                self.before.as_ref(),
                &["dirty_pressure", "total_dirty_bytes"],
            ),
            dirty_total_bytes_after: metric_value(
                self.after.as_ref(),
                &["dirty_pressure", "total_dirty_bytes"],
            ),
            drain_lag_bytes_before: metric_value(
                self.before.as_ref(),
                &["drain_lag", "current_bytes"],
            ),
            drain_lag_bytes_after: metric_value(
                self.after.as_ref(),
                &["drain_lag", "current_bytes"],
            ),
            active_uploads_after: metric_value(self.after.as_ref(), &["upload", "active_uploads"]),
            max_active_uploads,
            completed_uploads_before,
            completed_uploads_after,
            completed_uploads_delta,
            oldest_pending_upload_age_ms_after,
            stalled_pending_without_active_uploads,
            classification,
            sample_count,
            samples,
        }
    }
}

#[derive(Debug, Clone)]
struct UploadDrainSample {
    elapsed_ms: u64,
    snapshot: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UploadDrainSampleObservation {
    elapsed_ms: u64,
    metrics_seen: bool,
    pending_uploads: u64,
    pending_upload_bytes: u64,
    active_uploads: u64,
    completed_uploads: u64,
    oldest_pending_upload_age_ms: u64,
    dirty_total_bytes: u64,
    drain_lag_bytes: u64,
}

impl UploadDrainSampleObservation {
    fn from_sample(sample: &UploadDrainSample) -> Self {
        Self {
            elapsed_ms: sample.elapsed_ms,
            metrics_seen: sample.snapshot.is_some(),
            pending_uploads: metric_value(sample.snapshot.as_ref(), &["upload", "pending_uploads"]),
            pending_upload_bytes: metric_value(
                sample.snapshot.as_ref(),
                &["upload", "pending_upload_bytes"],
            ),
            active_uploads: metric_value(sample.snapshot.as_ref(), &["upload", "active_uploads"]),
            completed_uploads: metric_value(
                sample.snapshot.as_ref(),
                &["upload", "completed_uploads"],
            ),
            oldest_pending_upload_age_ms: metric_value(
                sample.snapshot.as_ref(),
                &["upload", "oldest_pending_upload_age_ms"],
            ),
            dirty_total_bytes: metric_value(
                sample.snapshot.as_ref(),
                &["dirty_pressure", "total_dirty_bytes"],
            ),
            drain_lag_bytes: metric_value(
                sample.snapshot.as_ref(),
                &["drain_lag", "current_bytes"],
            ),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UploadDrainObservation {
    waited_ms: u64,
    timed_out: bool,
    pending_uploads_before: u64,
    pending_uploads_after: u64,
    pending_upload_bytes_before: u64,
    pending_upload_bytes_after: u64,
    dirty_total_bytes_before: u64,
    dirty_total_bytes_after: u64,
    drain_lag_bytes_before: u64,
    drain_lag_bytes_after: u64,
    active_uploads_after: u64,
    max_active_uploads: u64,
    completed_uploads_before: u64,
    completed_uploads_after: u64,
    completed_uploads_delta: u64,
    oldest_pending_upload_age_ms_after: u64,
    stalled_pending_without_active_uploads: bool,
    classification: String,
    sample_count: usize,
    samples: Vec<UploadDrainSampleObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerformanceProbePhase {
    name: String,
    kind: String,
    pass: Option<u32>,
    bytes: u64,
    elapsed_ms: u64,
    mib_per_sec: f64,
    checksum: u64,
    metrics_before_seen: bool,
    metrics_after_seen: bool,
    delta: PerformanceProbeMetricDelta,
    interpretation: String,
}

impl PerformanceProbePhase {
    fn new(
        name: String,
        kind: String,
        pass: Option<u32>,
        io: PhaseIoResult,
        before: &Option<Value>,
        after: &Option<Value>,
    ) -> Self {
        let delta = PerformanceProbeMetricDelta::between(before.as_ref(), after.as_ref());
        let interpretation = interpret_phase(&kind, &delta);
        Self {
            name,
            kind,
            pass,
            bytes: io.bytes,
            elapsed_ms: io.elapsed.as_millis().min(u64::MAX as u128) as u64,
            mib_per_sec: io.mib_per_sec(),
            checksum: io.checksum,
            metrics_before_seen: before.is_some(),
            metrics_after_seen: after.is_some(),
            delta,
            interpretation,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PhaseIoResult {
    bytes: u64,
    elapsed: Duration,
    checksum: u64,
}

impl PhaseIoResult {
    fn new(bytes: u64, elapsed: Duration, checksum: u64) -> Self {
        Self {
            bytes,
            elapsed,
            checksum,
        }
    }

    fn mib_per_sec(self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        (self.bytes as f64 / (1024.0 * 1024.0)) / secs
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PerformanceProbeMetricDelta {
    fuse_read_calls: u64,
    fuse_read_bytes: u64,
    fuse_write_calls: u64,
    fuse_write_bytes: u64,
    vfs_read_calls: u64,
    vfs_read_requested_bytes: u64,
    vfs_write_calls: u64,
    vfs_write_bytes: u64,
    fetch_cache_hits: u64,
    fetch_s3_get_calls: u64,
    fetch_bytes_downloaded: u64,
    fetch_local_cache_io_calls: u64,
    fetch_total_calls: u64,
    foreground_local_payload_reads: u64,
    foreground_local_payload_read_bytes: u64,
    upload_completed_uploads: u64,
    upload_completed_upload_bytes: u64,
    upload_pending_snapshot_calls: u64,
    upload_pending_uploads_before: u64,
    upload_pending_uploads_after: u64,
    upload_pending_bytes_before: u64,
    upload_pending_bytes_after: u64,
    upload_oldest_pending_age_ms_after: u64,
    dirty_total_bytes_before: u64,
    dirty_total_bytes_after: u64,
    drain_lag_bytes_before: u64,
    drain_lag_bytes_after: u64,
    write_backpressure_events: u64,
    write_backpressure_wait_ms: u64,
}

impl PerformanceProbeMetricDelta {
    fn between(before: Option<&Value>, after: Option<&Value>) -> Self {
        Self {
            fuse_read_calls: metric_delta(before, after, &["fuse_callbacks", "read", "calls"]),
            fuse_read_bytes: metric_delta(before, after, &["fuse_callbacks", "read", "bytes"]),
            fuse_write_calls: metric_delta(before, after, &["fuse_callbacks", "write", "calls"]),
            fuse_write_bytes: metric_delta(before, after, &["fuse_callbacks", "write", "bytes"]),
            vfs_read_calls: metric_delta(before, after, &["vfs_read_path", "calls"]),
            vfs_read_requested_bytes: metric_delta(
                before,
                after,
                &["vfs_read_path", "requested_bytes"],
            ),
            vfs_write_calls: metric_delta(before, after, &["vfs_write_path", "calls"]),
            vfs_write_bytes: metric_delta(before, after, &["vfs_write_path", "write_bytes"]),
            fetch_cache_hits: metric_delta(before, after, &["fetch", "cache_hits"]),
            fetch_s3_get_calls: metric_delta(before, after, &["fetch", "s3_get_calls"]),
            fetch_bytes_downloaded: metric_delta(before, after, &["fetch", "bytes_downloaded"]),
            fetch_local_cache_io_calls: metric_delta(
                before,
                after,
                &["fetch", "local_cache_io_calls"],
            ),
            fetch_total_calls: metric_delta(before, after, &["fetch", "fetch_total_calls"]),
            foreground_local_payload_reads: metric_delta(
                before,
                after,
                &["vfs_read_path", "foreground_local_payload_reads"],
            ),
            foreground_local_payload_read_bytes: metric_delta(
                before,
                after,
                &["vfs_read_path", "foreground_local_payload_read_bytes"],
            ),
            upload_completed_uploads: metric_delta(before, after, &["upload", "completed_uploads"]),
            upload_completed_upload_bytes: metric_delta(
                before,
                after,
                &["upload", "completed_upload_bytes"],
            ),
            upload_pending_snapshot_calls: metric_delta(
                before,
                after,
                &["upload", "pending_upload_snapshot_calls"],
            ),
            upload_pending_uploads_before: metric_value(before, &["upload", "pending_uploads"]),
            upload_pending_uploads_after: metric_value(after, &["upload", "pending_uploads"]),
            upload_pending_bytes_before: metric_value(before, &["upload", "pending_upload_bytes"]),
            upload_pending_bytes_after: metric_value(after, &["upload", "pending_upload_bytes"]),
            upload_oldest_pending_age_ms_after: metric_value(
                after,
                &["upload", "oldest_pending_upload_age_ms"],
            ),
            dirty_total_bytes_before: metric_value(
                before,
                &["dirty_pressure", "total_dirty_bytes"],
            ),
            dirty_total_bytes_after: metric_value(after, &["dirty_pressure", "total_dirty_bytes"]),
            drain_lag_bytes_before: metric_value(before, &["drain_lag", "current_bytes"]),
            drain_lag_bytes_after: metric_value(after, &["drain_lag", "current_bytes"]),
            write_backpressure_events: metric_delta(
                before,
                after,
                &["write_backpressure", "events"],
            ),
            write_backpressure_wait_ms: metric_delta(
                before,
                after,
                &["write_backpressure", "wait_total_ms"],
            ),
        }
    }
}

fn metric_delta(before: Option<&Value>, after: Option<&Value>, path: &[&str]) -> u64 {
    metric_value(after, path).saturating_sub(metric_value(before, path))
}

fn metric_value(snapshot: Option<&Value>, path: &[&str]) -> u64 {
    let Some(mut value) = snapshot else {
        return 0;
    };
    for key in path {
        let Some(next) = value.get(*key) else {
            return 0;
        };
        value = next;
    }
    value
        .as_u64()
        .or_else(|| value.as_f64().map(|float| float.max(0.0) as u64))
        .unwrap_or(0)
}

fn interpret_phase(kind: &str, delta: &PerformanceProbeMetricDelta) -> String {
    if kind.contains("read") {
        if delta.fuse_read_calls == 0 && delta.vfs_read_calls == 0 {
            "os-cache-hit-no-fuse-callbacks".to_string()
        } else if delta.fetch_s3_get_calls > 0 {
            "read-reentered-fuse-and-fetched-from-s3".to_string()
        } else if (delta.upload_pending_bytes_before > 0
            || delta.upload_pending_bytes_after > 0
            || delta.dirty_total_bytes_before > 0
            || delta.drain_lag_bytes_before > 0)
            && (delta.fetch_cache_hits > 0 || delta.fetch_local_cache_io_calls > 0)
        {
            "read-used-local-cache-while-upload-backlog-active".to_string()
        } else if delta.fetch_cache_hits > 0 || delta.fetch_local_cache_io_calls > 0 {
            "read-reentered-fuse-and-used-local-cache".to_string()
        } else {
            "read-reentered-fuse-without-recorded-s3-fetch".to_string()
        }
    } else if delta.write_backpressure_events > 0 {
        "write-hit-vfs-backpressure".to_string()
    } else if delta.fuse_write_calls > 0 {
        "write-accepted-through-fuse".to_string()
    } else {
        "write-had-no-fuse-metrics".to_string()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PerformanceProbeWriteObservations {
    write_hit_backpressure: bool,
    write_pending_upload_bytes_after: u64,
    write_pending_uploads_after: u64,
    fuse_write_calls: u64,
    avg_fuse_write_bytes: f64,
}

impl PerformanceProbeWriteObservations {
    fn from_phases(phases: &[PerformanceProbePhase]) -> Self {
        let Some(write) = phases.iter().find(|phase| phase.kind == "write") else {
            return Self::default();
        };
        Self {
            write_hit_backpressure: write.delta.write_backpressure_events > 0,
            write_pending_upload_bytes_after: write.delta.upload_pending_bytes_after,
            write_pending_uploads_after: write.delta.upload_pending_uploads_after,
            fuse_write_calls: write.delta.fuse_write_calls,
            avg_fuse_write_bytes: if write.delta.fuse_write_calls == 0 {
                0.0
            } else {
                write.delta.fuse_write_bytes as f64 / write.delta.fuse_write_calls as f64
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PerformanceProbeReadObservations {
    os_cache_hit_passes: u32,
    fuse_reentered_passes: u32,
    s3_fetch_passes: u32,
    local_cache_passes: u32,
    read_passes_while_upload_backlog: u32,
    no_metrics_passes: u32,
    avg_read_mib_per_sec: f64,
    avg_read_mib_per_sec_while_upload_backlog: f64,
    avg_read_mib_per_sec_after_upload_drained: f64,
    fuse_read_calls: u64,
    fuse_read_bytes: u64,
    avg_fuse_read_bytes: f64,
    foreground_local_payload_reads: u64,
    foreground_local_payload_read_bytes: u64,
    avg_foreground_local_payload_read_bytes: f64,
    post_drain_read_mib_per_sec: Option<f64>,
    post_drain_read_interpretation: Option<String>,
    post_drain_fuse_read_calls: Option<u64>,
    post_drain_fuse_read_bytes: Option<u64>,
    post_drain_foreground_local_payload_reads: Option<u64>,
    post_drain_foreground_local_payload_read_bytes: Option<u64>,
}

impl PerformanceProbeReadObservations {
    fn from_phases(phases: &[PerformanceProbePhase]) -> Self {
        let mut observations = Self::default();
        let mut read_total = 0.0;
        let mut read_count = 0u32;
        let mut backlog_total = 0.0;
        let mut backlog_count = 0u32;
        let mut drained_total = 0.0;
        let mut drained_count = 0u32;
        for phase in phases.iter().filter(|phase| phase.kind == "read") {
            read_total += phase.mib_per_sec;
            read_count += 1;
            observations.fuse_read_calls = observations
                .fuse_read_calls
                .saturating_add(phase.delta.fuse_read_calls);
            observations.fuse_read_bytes = observations
                .fuse_read_bytes
                .saturating_add(phase.delta.fuse_read_bytes);
            observations.foreground_local_payload_reads = observations
                .foreground_local_payload_reads
                .saturating_add(phase.delta.foreground_local_payload_reads);
            observations.foreground_local_payload_read_bytes = observations
                .foreground_local_payload_read_bytes
                .saturating_add(phase.delta.foreground_local_payload_read_bytes);
            let upload_backlog_active = phase.delta.upload_pending_bytes_before > 0
                || phase.delta.upload_pending_bytes_after > 0
                || phase.delta.dirty_total_bytes_before > 0
                || phase.delta.drain_lag_bytes_before > 0;
            if upload_backlog_active {
                observations.read_passes_while_upload_backlog += 1;
                backlog_total += phase.mib_per_sec;
                backlog_count += 1;
            } else {
                drained_total += phase.mib_per_sec;
                drained_count += 1;
            }
            if !phase.metrics_after_seen {
                observations.no_metrics_passes += 1;
            } else if phase.delta.fuse_read_calls == 0 && phase.delta.vfs_read_calls == 0 {
                observations.os_cache_hit_passes += 1;
            } else {
                observations.fuse_reentered_passes += 1;
                if phase.delta.fetch_s3_get_calls > 0 {
                    observations.s3_fetch_passes += 1;
                } else if phase.delta.fetch_cache_hits > 0
                    || phase.delta.fetch_local_cache_io_calls > 0
                {
                    observations.local_cache_passes += 1;
                }
            }
        }
        observations.avg_read_mib_per_sec = average(read_total, read_count);
        observations.avg_read_mib_per_sec_while_upload_backlog =
            average(backlog_total, backlog_count);
        observations.avg_read_mib_per_sec_after_upload_drained =
            average(drained_total, drained_count);
        observations.avg_fuse_read_bytes =
            average_u64(observations.fuse_read_bytes, observations.fuse_read_calls);
        observations.avg_foreground_local_payload_read_bytes = average_u64(
            observations.foreground_local_payload_read_bytes,
            observations.foreground_local_payload_reads,
        );
        if let Some(post_drain) = phases.iter().find(|phase| phase.kind == "post-drain-read") {
            observations.post_drain_read_mib_per_sec = Some(post_drain.mib_per_sec);
            observations.post_drain_read_interpretation = Some(post_drain.interpretation.clone());
            observations.post_drain_fuse_read_calls = Some(post_drain.delta.fuse_read_calls);
            observations.post_drain_fuse_read_bytes = Some(post_drain.delta.fuse_read_bytes);
            observations.post_drain_foreground_local_payload_reads =
                Some(post_drain.delta.foreground_local_payload_reads);
            observations.post_drain_foreground_local_payload_read_bytes =
                Some(post_drain.delta.foreground_local_payload_read_bytes);
        }
        observations
    }
}

fn average(total: f64, count: u32) -> f64 {
    if count == 0 {
        0.0
    } else {
        total / f64::from(count)
    }
}

fn average_u64(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_observations_record_read_geometry_and_local_payload_io() {
        let phases = vec![
            PerformanceProbePhase::new(
                "read-pass-1".to_string(),
                "read".to_string(),
                Some(1),
                PhaseIoResult::new(2 * 1024 * 1024, Duration::from_millis(100), 17),
                &Some(json!({
                    "fuse_callbacks": { "read": { "calls": 0, "bytes": 0 } },
                    "vfs_read_path": {
                        "calls": 0,
                        "requested_bytes": 0,
                        "foreground_local_payload_reads": 0,
                        "foreground_local_payload_read_bytes": 0
                    }
                })),
                &Some(json!({
                    "fuse_callbacks": { "read": { "calls": 2, "bytes": 2 * 1024 * 1024 } },
                    "vfs_read_path": {
                        "calls": 2,
                        "requested_bytes": 2 * 1024 * 1024,
                        "foreground_local_payload_reads": 2,
                        "foreground_local_payload_read_bytes": 2 * 1024 * 1024
                    }
                })),
            ),
            PerformanceProbePhase::new(
                "read-pass-2".to_string(),
                "read".to_string(),
                Some(2),
                PhaseIoResult::new(2 * 1024 * 1024, Duration::from_millis(100), 17),
                &Some(json!({
                    "fuse_callbacks": { "read": { "calls": 2, "bytes": 2 * 1024 * 1024 } },
                    "vfs_read_path": {
                        "calls": 2,
                        "requested_bytes": 2 * 1024 * 1024,
                        "foreground_local_payload_reads": 2,
                        "foreground_local_payload_read_bytes": 2 * 1024 * 1024
                    }
                })),
                &Some(json!({
                    "fuse_callbacks": { "read": { "calls": 3, "bytes": 3 * 1024 * 1024 } },
                    "vfs_read_path": {
                        "calls": 3,
                        "requested_bytes": 3 * 1024 * 1024,
                        "foreground_local_payload_reads": 3,
                        "foreground_local_payload_read_bytes": 3 * 1024 * 1024
                    }
                })),
            ),
            PerformanceProbePhase::new(
                "read-after-upload-drain-wait".to_string(),
                "post-drain-read".to_string(),
                None,
                PhaseIoResult::new(2 * 1024 * 1024, Duration::from_millis(100), 17),
                &Some(json!({
                    "fuse_callbacks": { "read": { "calls": 3, "bytes": 3 * 1024 * 1024 } },
                    "vfs_read_path": {
                        "calls": 3,
                        "requested_bytes": 3 * 1024 * 1024,
                        "foreground_local_payload_reads": 3,
                        "foreground_local_payload_read_bytes": 3 * 1024 * 1024
                    }
                })),
                &Some(json!({
                    "fuse_callbacks": { "read": { "calls": 5, "bytes": 5 * 1024 * 1024 } },
                    "vfs_read_path": {
                        "calls": 5,
                        "requested_bytes": 5 * 1024 * 1024,
                        "foreground_local_payload_reads": 5,
                        "foreground_local_payload_read_bytes": 5 * 1024 * 1024
                    }
                })),
            ),
        ];

        let observations = PerformanceProbeReadObservations::from_phases(&phases);

        assert_eq!(observations.fuse_read_calls, 3);
        assert_eq!(observations.fuse_read_bytes, 3 * 1024 * 1024);
        assert_f64_eq(observations.avg_fuse_read_bytes, 1024.0 * 1024.0);
        assert_eq!(observations.foreground_local_payload_reads, 3);
        assert_eq!(
            observations.foreground_local_payload_read_bytes,
            3 * 1024 * 1024
        );
        assert_f64_eq(
            observations.avg_foreground_local_payload_read_bytes,
            1024.0 * 1024.0,
        );
        assert_eq!(observations.post_drain_fuse_read_calls, Some(2));
        assert_eq!(
            observations.post_drain_fuse_read_bytes,
            Some(2 * 1024 * 1024)
        );
        assert_eq!(
            observations.post_drain_foreground_local_payload_reads,
            Some(2)
        );
        assert_eq!(
            observations.post_drain_foreground_local_payload_read_bytes,
            Some(2 * 1024 * 1024)
        );
    }

    #[test]
    fn upload_drain_observation_flags_pending_uploads_without_active_uploads() {
        let result = UploadDrainResult {
            before: Some(json!({
                "upload": {
                    "pending_uploads": 64,
                    "pending_upload_bytes": 255 * 1024 * 1024u64,
                    "active_uploads": 0,
                    "completed_uploads": 17,
                    "oldest_pending_upload_age_ms": 45_000
                },
                "dirty_pressure": { "total_dirty_bytes": 0 },
                "drain_lag": { "current_bytes": 255 * 1024 * 1024u64 }
            })),
            after: Some(json!({
                "upload": {
                    "pending_uploads": 64,
                    "pending_upload_bytes": 253 * 1024 * 1024u64,
                    "active_uploads": 0,
                    "completed_uploads": 17,
                    "oldest_pending_upload_age_ms": 165_000
                },
                "dirty_pressure": { "total_dirty_bytes": 0 },
                "drain_lag": { "current_bytes": 253 * 1024 * 1024u64 }
            })),
            samples: vec![
                upload_drain_sample(
                    0,
                    json!({
                        "upload": {
                            "pending_uploads": 64,
                            "pending_upload_bytes": 255 * 1024 * 1024u64,
                            "active_uploads": 0,
                            "completed_uploads": 17,
                            "oldest_pending_upload_age_ms": 45_000
                        }
                    }),
                ),
                upload_drain_sample(
                    60_000,
                    json!({
                        "upload": {
                            "pending_uploads": 64,
                            "pending_upload_bytes": 254 * 1024 * 1024u64,
                            "active_uploads": 0,
                            "completed_uploads": 17,
                            "oldest_pending_upload_age_ms": 105_000
                        }
                    }),
                ),
                upload_drain_sample(
                    120_000,
                    json!({
                        "upload": {
                            "pending_uploads": 64,
                            "pending_upload_bytes": 253 * 1024 * 1024u64,
                            "active_uploads": 0,
                            "completed_uploads": 17,
                            "oldest_pending_upload_age_ms": 165_000
                        }
                    }),
                ),
            ],
            elapsed: Duration::from_secs(120),
            timed_out: true,
        };

        let observation = result.into_observation();

        assert!(observation.stalled_pending_without_active_uploads);
        assert_eq!(observation.classification, "pending-without-active-uploads");
        assert_eq!(observation.sample_count, 3);
        assert_eq!(observation.max_active_uploads, 0);
        assert_eq!(observation.completed_uploads_delta, 0);
        assert_eq!(observation.oldest_pending_upload_age_ms_after, 165_000);
    }

    #[test]
    fn upload_drain_failure_detail_names_stalled_scheduler_state() {
        let observation = UploadDrainObservation {
            timed_out: true,
            pending_uploads_after: 64,
            pending_upload_bytes_after: 253 * 1024 * 1024,
            active_uploads_after: 0,
            max_active_uploads: 0,
            completed_uploads_delta: 0,
            oldest_pending_upload_age_ms_after: 165_000,
            stalled_pending_without_active_uploads: true,
            classification: "pending-without-active-uploads".to_string(),
            ..UploadDrainObservation::default()
        };

        let detail = upload_drain_failure_detail(&observation).expect("stalled drain should fail");

        assert!(detail.contains("pending-without-active-uploads"));
        assert!(detail.contains("pending_upload_bytes_after=265289728"));
        assert!(detail.contains("max_active_uploads=0"));
        assert!(detail.contains("completed_uploads_delta=0"));
        assert!(detail.contains("oldest_pending_upload_age_ms_after=165000"));
    }

    fn upload_drain_sample(elapsed_ms: u64, snapshot: Value) -> UploadDrainSample {
        UploadDrainSample {
            elapsed_ms,
            snapshot: Some(snapshot),
        }
    }

    fn assert_f64_eq(actual: f64, expected: f64) {
        const FLOAT_TOLERANCE: f64 = 1e-6;
        assert!(
            (actual - expected).abs() <= FLOAT_TOLERANCE,
            "expected {actual} to equal {expected}"
        );
    }
}
