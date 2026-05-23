use super::*;

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
    base_fingerprint: EnvironmentFingerprint,
    vfs: Arc<StudioVfs>,
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
            base_fingerprint,
            vfs,
        }
    }
}

impl DiagnosticsDataSource for LiveMountDiagnosticsSource {
    fn session_info(&self) -> DiagnosticsSessionInfo {
        self.identity.clone()
    }

    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        DiagnosticsHealthSnapshot {
            generated_unix_ms: now_unix_millis(),
            ingress: self.vfs.ingress_stats(),
            upload: self.vfs.upload_stats(),
            file_sessions: self.vfs.file_session_stats(),
            foreground_reads: self.vfs.foreground_read_protection_stats(),
            write_path: self.vfs.write_path_observability_snapshot(),
            zeroio: self.vfs.zeroio_runtime_snapshot(),
        }
    }

    fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot {
        let health = self.health_snapshot();
        DiagnosticsRuntimeSnapshot {
            schema_version: DIAGNOSTICS_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
            generated_unix_ms: health.generated_unix_ms,
            ingress: health.ingress,
            write_path: health.write_path,
            upload: health.upload,
            zeroio: health.zeroio,
            cache: DiagnosticsCacheSnapshot {
                state: self.vfs.cache_state_snapshot(),
            },
            synchronization: DiagnosticsSynchronizationSnapshot {
                state: self.vfs.synchronization_state_snapshot(),
            },
        }
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
