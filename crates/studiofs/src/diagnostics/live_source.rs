use super::*;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

#[derive(Debug, Clone)]
pub(crate) struct LiveMountRuntimeFingerprint {
    pub bridge: String,
    pub cache_size: Option<String>,
    pub endpoint_override: Option<String>,
    pub snapshot_id: Option<String>,
    pub vfs_config: serde_json::Value,
}

pub(crate) struct LiveMountDiagnosticsSource {
    identity: DiagnosticsSessionInfo,
    format_info: FormatInfo,
    base_fingerprint: EnvironmentFingerprint,
    vfs: Arc<StudioVfs>,
    process_sampler: Mutex<ProcessMetricsSampler>,
}

struct ProcessMetricsSampler {
    pid: Pid,
    system: System,
}

impl ProcessMetricsSampler {
    fn new(pid: u32) -> Self {
        Self {
            pid: Pid::from_u32(pid),
            system: System::new(),
        }
    }

    fn sample(&mut self) -> DiagnosticsProcessSnapshot {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        self.system
            .process(self.pid)
            .map(|process| DiagnosticsProcessSnapshot {
                cpu_percent: process.cpu_usage() as f64,
                rss_bytes: process.memory(),
            })
            .unwrap_or_default()
    }
}

impl LiveMountDiagnosticsSource {
    pub(crate) fn new(
        format_info: &FormatInfo,
        mount_point: &str,
        cache_path: &Path,
        pid: u32,
        read_only: bool,
        runtime_fingerprint: LiveMountRuntimeFingerprint,
        vfs: Arc<StudioVfs>,
    ) -> Self {
        let session_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{mount_point}:{pid}", format_info.uuid).as_bytes(),
        )
        .to_string();
        let identity = DiagnosticsSessionInfo {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            session_id,
            volume: format_info.name.clone(),
            volume_uuid: format_info.uuid.clone(),
            bridge: runtime_fingerprint.bridge.clone(),
            mount_point: mount_point.to_string(),
            pid,
            cache_path: cache_path.to_string_lossy().into_owned(),
            read_only,
            started_unix_ms: now_unix_millis(),
            controller: None,
        };
        let base_fingerprint = base_environment_fingerprint(&identity, &runtime_fingerprint);
        Self {
            identity,
            format_info: format_info.clone(),
            base_fingerprint,
            vfs,
            process_sampler: Mutex::new(ProcessMetricsSampler::new(pid)),
        }
    }
}

impl DiagnosticsDataSource for LiveMountDiagnosticsSource {
    fn session_info(&self) -> DiagnosticsSessionInfo {
        self.identity.clone()
    }

    fn snapshot_format_info(&self) -> Result<FormatInfo> {
        Ok(self.format_info.clone())
    }

    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        let generated_unix_ms = now_unix_millis();
        let synchronization = self.vfs.synchronization_state_snapshot();
        DiagnosticsHealthSnapshot {
            generated_unix_ms,
            ingress: self.vfs.ingress_stats(),
            upload: self.vfs.upload_stats(),
            file_sessions: synchronization.file_sessions.clone(),
            foreground_reads: synchronization.foreground_reads.clone(),
            coherence: synchronization.coherence,
            write_path: self.vfs.write_path_observability_snapshot(),
            synchronization: DiagnosticsSynchronizationSnapshot {
                state: synchronization,
            },
            sync_safety: self.vfs.sync_safety_snapshot(),
        }
    }

    fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot {
        let health = self.health_snapshot();
        // An unreadable quota is reported as absent, so leave a trace that
        // tells it apart from an unlimited volume.
        let logical_quota = self.vfs.logical_quota_status().unwrap_or_else(|error| {
            tracing::warn!(%error, "logical quota status unavailable for diagnostics");
            None
        });
        let scratch_status = self.vfs.scratch_status_snapshot().unwrap_or_else(|error| {
            tracing::warn!(%error, "scratch status unavailable for diagnostics");
            studiofs_vfs::VfsScratchStatusSnapshot::default()
        });
        DiagnosticsRuntimeSnapshot {
            cache_scrub: self.vfs.cache_scrub_snapshot(),
            scratch_auto_promote: Some(self.vfs.scratch_auto_promote_snapshot()),
            scratch_status: Some(scratch_status),
            logical_quota,
            schema_version: DIAGNOSTICS_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
            generated_unix_ms: health.generated_unix_ms,
            process: self.process_sampler.lock().sample(),
            ingress: health.ingress,
            write_path: health.write_path,
            upload: health.upload,
            zeroio: self.vfs.zeroio_runtime_snapshot(),
            cache: DiagnosticsCacheSnapshot {
                state: self.vfs.cache_state_snapshot(),
            },
            synchronization: health.synchronization,
            sync_safety: health.sync_safety,
            coherence: health.coherence,
        }
    }

    fn set_payload_limits(
        &self,
        upload_mbps: Option<u64>,
        download_mbps: Option<u64>,
    ) -> Result<(u64, u64)> {
        anyhow::ensure!(
            upload_mbps.is_none_or(|value| value <= 1_000_000)
                && download_mbps.is_none_or(|value| value <= 1_000_000),
            "payload limits must be between 0 and 1000000 Mbps"
        );
        self.vfs.set_payload_limits(upload_mbps, download_mbps);
        let (upload, download, _, _) = self.vfs.payload_limit_status();
        Ok((upload, download))
    }

    fn recent_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent> {
        self.vfs
            .recent_runtime_events(limit)
            .into_iter()
            .map(DiagnosticsRecentEvent::from)
            .collect()
    }

    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint {
        let mut fingerprint = self.base_fingerprint.clone();
        fingerprint.workload_identity = WorkloadIdentityFingerprint {
            case_workload: case_workload_identity.map(str::to_string),
            run_workload: run_workload_identity.map(str::to_string),
        };
        fingerprint
    }

    fn reset_observability_counters(&self) -> std::result::Result<(), DiagnosticsActionError> {
        self.vfs.reset_diagnostics_observability();
        Ok(())
    }

    fn promote_scratch_path(&self, path: &str) -> Result<studiofs_vfs::ScratchPromotePathOutcome> {
        if self.identity.read_only {
            anyhow::bail!("cannot promote scratch entries on a read-only mount");
        }
        crate::runtime_bridge::block_on(async {
            self.vfs
                .promote_scratch_path(path)
                .await
                .map_err(anyhow::Error::from)
        })
    }

    fn list_scratch_entries(
        &self,
        path: Option<&str>,
    ) -> Result<Vec<studiofs_vfs::ScratchListEntry>> {
        self.vfs
            .list_scratch_entries(path)
            .map_err(anyhow::Error::from)
    }

    fn list_scratch_orphans(
        &self,
        path: Option<&str>,
    ) -> Result<Vec<studiofs_vfs::ScratchOrphanSummary>> {
        self.vfs
            .list_scratch_orphan_entries(path)
            .map_err(anyhow::Error::from)
    }

    fn clean_scratch_entries(
        &self,
        path: Option<&str>,
        older_than_secs: Option<u64>,
    ) -> Result<studiofs_vfs::ScratchCleanOutcome> {
        if self.identity.read_only {
            anyhow::bail!("cannot clean scratch entries on a read-only mount");
        }
        crate::runtime_bridge::block_on(async {
            self.vfs
                .clean_scratch_entries(path, older_than_secs)
                .await
                .map_err(anyhow::Error::from)
        })
    }

    fn restore_scratch_orphan(
        &self,
        orphan: &str,
        target_path: Option<&str>,
    ) -> Result<studiofs_vfs::ScratchRestoreOutcome> {
        if self.identity.read_only {
            anyhow::bail!("cannot restore scratch orphans on a read-only mount");
        }
        self.vfs
            .restore_scratch_orphan_by_spec(orphan, target_path)
            .map_err(anyhow::Error::from)
    }
}

pub(super) fn base_environment_fingerprint(
    identity: &DiagnosticsSessionInfo,
    runtime_fingerprint: &LiveMountRuntimeFingerprint,
) -> EnvironmentFingerprint {
    EnvironmentFingerprint {
        platform: PlatformFingerprint {
            os: std::env::consts::OS.to_string(),
            family: std::env::consts::FAMILY.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        bridge: identity.bridge.clone(),
        build: BuildIdentity {
            package_name: env!("CARGO_PKG_NAME").to_string(),
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            executable_path: std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            debug_build: cfg!(debug_assertions),
        },
        runtime_configuration: RuntimeConfigurationFingerprint {
            volume: identity.volume.clone(),
            volume_uuid: identity.volume_uuid.clone(),
            mount_point: identity.mount_point.clone(),
            cache_path: identity.cache_path.clone(),
            read_only: identity.read_only,
            cache_size: runtime_fingerprint.cache_size.clone(),
            endpoint_override: runtime_fingerprint.endpoint_override.clone(),
            snapshot_id: runtime_fingerprint.snapshot_id.clone(),
            vfs_config: runtime_fingerprint.vfs_config.clone(),
        },
        workload_identity: WorkloadIdentityFingerprint {
            case_workload: None,
            run_workload: None,
        },
        machine: static_machine_fingerprint(),
        diagnostics: DiagnosticsSettingsFingerprint {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            default_lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
            max_lease_ttl_ms: MAX_LEASE_TTL_MS,
            max_connections: MAX_DIAGNOSTICS_CONNECTIONS,
        },
    }
}

pub(super) fn static_machine_fingerprint() -> MachineFingerprint {
    static MACHINE_FINGERPRINT: OnceLock<MachineFingerprint> = OnceLock::new();
    MACHINE_FINGERPRINT
        .get_or_init(|| {
            let mut system = sysinfo::System::new();
            system.refresh_memory();
            MachineFingerprint {
                host_name: sysinfo::System::host_name(),
                system_name: sysinfo::System::name(),
                kernel_version: sysinfo::System::kernel_version(),
                os_version: sysinfo::System::os_version(),
                total_memory_bytes: system.total_memory(),
                cpu_count: std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or_else(|_| system.cpus().len()),
            }
        })
        .clone()
}
