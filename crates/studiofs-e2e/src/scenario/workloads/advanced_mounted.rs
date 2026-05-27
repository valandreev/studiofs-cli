//! Advanced mounted scenario families and helper entrypoints.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::scenario) struct ScenarioIsolation {
    pub(in crate::scenario) root: String,
    pub(in crate::scenario) payload_marker: String,
}

impl ScenarioIsolation {
    pub(in crate::scenario) fn for_advanced(
        context: &ScenarioContext,
        kind: AdvancedMountedKind,
        volume_name: &str,
    ) -> Self {
        let marker_input = format!(
            "{}:{}:{}:{}",
            context.run_id,
            context.seed,
            kind.name(),
            volume_name
        );
        let marker_hash = bytes_sha256(marker_input.as_bytes());
        let nonce = &marker_hash[..16];
        Self {
            root: format!("{}-{nonce}", kind.path_root()),
            payload_marker: format!("studiofs-e2e:{marker_input}:{nonce}"),
        }
    }

    pub(in crate::scenario) fn path(&self, relative_path: &str) -> String {
        let relative_path = relative_path.trim_matches('/');
        if relative_path.is_empty() {
            self.root.clone()
        } else {
            format!("{}/{}", self.root, relative_path)
        }
    }

    pub(in crate::scenario) fn payload(
        &self,
        label: &str,
        logical_path: &str,
        body: &str,
    ) -> Vec<u8> {
        format!(
            "{}\nlabel={label}\npath={logical_path}\n{body}",
            self.payload_marker
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::scenario) enum AdvancedMountedKind {
    CrashBeforeWriteAcceptance,
    CrashAfterPartialWriteAcceptance,
    CrashAfterFullWriteAcceptance,
    MountedChaos,
    HotspotChurn,
    FaultChaos,
    LwwReconnectChaos,
    OpenHandle,
    SparseRange,
    ColdObserverLongHistory,
    StaleRemountColdDiscoverCatchup,
    InvalidationDropoutFallback,
    SymlinkAttribute,
}

#[derive(Debug, Clone, Copy)]
struct AdvancedMountedProperties {
    name: &'static str,
    description: &'static str,
    path_root: &'static str,
    summary_file: &'static str,
    client_names: &'static [&'static str],
    convergence_timeout: Duration,
}

impl AdvancedMountedKind {
    fn properties(self) -> AdvancedMountedProperties {
        match self {
            Self::CrashBeforeWriteAcceptance => AdvancedMountedProperties {
                name: MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE,
                description: "mounted crash before write acceptance recovery",
                path_root: "crash-before-write-acceptance",
                summary_file: "write-acceptance-crash-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::CrashAfterPartialWriteAcceptance => AdvancedMountedProperties {
                name: MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE,
                description: "mounted crash after partial write acceptance recovery",
                path_root: "crash-after-partial-write-acceptance",
                summary_file: "write-acceptance-crash-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::CrashAfterFullWriteAcceptance => AdvancedMountedProperties {
                name: MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE,
                description: "mounted crash after full write acceptance recovery",
                path_root: "crash-after-full-write-acceptance",
                summary_file: "write-acceptance-crash-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::MountedChaos => AdvancedMountedProperties {
                name: MOUNTED_CHAOS_CONVERGENCE,
                description: "seeded mounted namespace chaos convergence",
                path_root: "chaos",
                summary_file: "mounted-chaos-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::HotspotChurn => AdvancedMountedProperties {
                name: MOUNTED_HOTSPOT_CHURN_CONVERGENCE,
                description: "mounted hot-spot churn convergence",
                path_root: "hotspot",
                summary_file: "mounted-hotspot-churn-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::FaultChaos => AdvancedMountedProperties {
                name: MOUNTED_FAULT_CHAOS_CONVERGENCE,
                description: "mounted fault chaos convergence",
                path_root: "fault-chaos",
                summary_file: "mounted-fault-chaos-summary.json",
                client_names: FAULT_CHAOS_CLIENT_NAMES,
                convergence_timeout: FAULT_CHAOS_CONVERGENCE_TIMEOUT,
            },
            Self::LwwReconnectChaos => AdvancedMountedProperties {
                name: MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE,
                description: "mounted LWW reconnect chaos convergence",
                path_root: "lww-chaos",
                summary_file: "mounted-lww-reconnect-chaos-summary.json",
                client_names: LWW_RECONNECT_CLIENT_NAMES,
                convergence_timeout: LWW_RECONNECT_CONVERGENCE_TIMEOUT,
            },
            Self::OpenHandle => AdvancedMountedProperties {
                name: MOUNTED_OPEN_HANDLE_CONVERGENCE,
                description: "mounted open-handle convergence",
                path_root: "open-handles",
                summary_file: "mounted-open-handle-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::SparseRange => AdvancedMountedProperties {
                name: MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE,
                description: "mounted sparse-file range convergence",
                path_root: "sparse",
                summary_file: "sparse-range-summary.json",
                client_names: ADVANCED_MOUNTED_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::ColdObserverLongHistory => AdvancedMountedProperties {
                name: MOUNTED_COLD_OBSERVER_LONG_HISTORY_CONVERGENCE,
                description: "mounted cold-observer long-history convergence",
                path_root: "cold-observer",
                summary_file: "cold-observer-long-history-summary.json",
                client_names: COLD_OBSERVER_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::StaleRemountColdDiscoverCatchup => AdvancedMountedProperties {
                name: MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE,
                description: "mounted stale-remount and cold-discover catch-up convergence",
                path_root: "stale-remount-cold-discover",
                summary_file: "stale-remount-cold-discover-summary.json",
                client_names: COLD_OBSERVER_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::InvalidationDropoutFallback => AdvancedMountedProperties {
                name: MOUNTED_INVALIDATION_DROPOUT_FALLBACK_CONVERGENCE,
                description: "mounted invalidation-dropout fallback-sync convergence",
                path_root: "invalidation-dropout",
                summary_file: "invalidation-dropout-summary.json",
                client_names: INVALIDATION_DROPOUT_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
            Self::SymlinkAttribute => AdvancedMountedProperties {
                name: MOUNTED_SYMLINK_ATTRIBUTE_CONVERGENCE,
                description: "mounted symlink and metadata-attribute convergence",
                path_root: "symlink-attribute",
                summary_file: "symlink-attribute-summary.json",
                client_names: SYMLINK_ATTRIBUTE_CLIENT_NAMES,
                convergence_timeout: ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
            },
        }
    }

    fn name(self) -> &'static str {
        self.properties().name
    }

    fn description(self) -> &'static str {
        self.properties().description
    }

    fn path_root(self) -> &'static str {
        self.properties().path_root
    }

    fn summary_file(self) -> &'static str {
        self.properties().summary_file
    }

    fn client_names(self) -> &'static [&'static str] {
        self.properties().client_names
    }

    fn convergence_timeout(self) -> Duration {
        self.properties().convergence_timeout
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct AdvancedMountedScenario {
    pub(in crate::scenario) kind: AdvancedMountedKind,
}

impl AdvancedMountedScenario {
    pub(in crate::scenario) fn new(kind: AdvancedMountedKind) -> Self {
        Self { kind }
    }
}

impl Scenario for AdvancedMountedScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: self.kind.name(),
            description: self.kind.description(),
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut controller =
                MultiClientController::new(config.clone(), SystemClientProcessExecutor);
            run_advanced_mounted_with_controller(context, self.kind, &mut controller)?;
        } else {
            write_advanced_mounted_plan_artifacts(context, self.kind)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context.scenario_dir.join(self.kind.summary_file());
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: AdvancedMountedSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let lww_oracle_matches = summary
            .lww_oracle
            .as_ref()
            .map(|oracle| oracle.matched)
            .unwrap_or(true);
        let long_history_matches = summary
            .long_history_validation
            .as_ref()
            .map(|validation| validation.matched)
            .unwrap_or_else(|| summary.long_history.is_none());
        let invalidation_dropout_matches = summary
            .invalidation_dropout
            .as_ref()
            .map(InvalidationDropoutSummary::matched)
            .unwrap_or(self.kind != AdvancedMountedKind::InvalidationDropoutFallback);
        let validation_passed = summary.final_validator.converged
            && summary.namespace_violations.is_empty()
            && lww_oracle_matches
            && long_history_matches
            && invalidation_dropout_matches
            && sparse_ranges_match(self.kind, summary.sparse_ranges.as_ref());
        let validation = json!({
            "scenario": self.kind.name(),
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if validation_passed { "passed" } else { "failed" },
            "final_validator": summary.final_validator,
            "namespace_violations": summary.namespace_violations,
            "operation_log": summary.operation_log,
            "lww_oracle": summary.lww_oracle,
            "sparse_ranges": summary.sparse_ranges,
            "cold_observer": summary.cold_observer,
            "long_history": summary.long_history,
            "long_history_validation": summary.long_history_validation,
            "catchup": summary.catchup,
            "invalidation_dropout": summary.invalidation_dropout,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        let mut artifacts = vec![
            format!(
                "artifacts/scenarios/{}/{}",
                self.kind.name(),
                self.kind.summary_file()
            ),
            format!("artifacts/scenarios/{}/validation.json", self.kind.name()),
        ];
        if self.kind == AdvancedMountedKind::LwwReconnectChaos {
            artifacts.push(format!(
                "artifacts/scenarios/{}/lww-oracle.json",
                self.kind.name()
            ));
        }
        if self.kind == AdvancedMountedKind::ColdObserverLongHistory {
            artifacts.push(format!(
                "artifacts/scenarios/{}/cold-observer-diagnostics.json",
                self.kind.name()
            ));
        }
        if self.kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
            artifacts.push(format!(
                "artifacts/scenarios/{}/catchup-diagnostics.json",
                self.kind.name()
            ));
        }
        if self.kind == AdvancedMountedKind::InvalidationDropoutFallback {
            artifacts.push(format!(
                "artifacts/scenarios/{}/invalidation-fallback-diagnostics.json",
                self.kind.name()
            ));
        }
        if !validation_passed {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} validation failed; see {}",
                self.kind.name(),
                context.scenario_dir.join("validation.json").display()
            ))));
        }
        Ok(artifacts)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct LwwReconnectOracle {
    pub(in crate::scenario) expected_payload_sha256: String,
    pub(in crate::scenario) actual_payload_sha256: String,
    pub(in crate::scenario) matched: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct SparseExpectedRange {
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) expected_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct SparseHoleProbe {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) all_zero: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct SparseReadProbe {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) actual_checksum_sha256: String,
    pub(in crate::scenario) matched: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct SparseRangeSummary {
    pub(in crate::scenario) relative_path: String,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) final_checksum_sha256: String,
    pub(in crate::scenario) expected_byte_ranges: Vec<SparseExpectedRange>,
    pub(in crate::scenario) zero_filled_holes: Vec<SparseHoleProbe>,
    pub(in crate::scenario) read_probes: Vec<SparseReadProbe>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct ColdObserverSummary {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) local_metadata_db: PathBuf,
    pub(in crate::scenario) cache_dir: PathBuf,
    pub(in crate::scenario) discovered_after_history: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct LongHistorySummary {
    pub(in crate::scenario) bounded_operation_count: usize,
    pub(in crate::scenario) expected_present_paths: Vec<String>,
    pub(in crate::scenario) expected_absent_paths: Vec<String>,
    pub(in crate::scenario) final_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct LongHistoryValidation {
    pub(in crate::scenario) matched: bool,
    pub(in crate::scenario) missing_present_paths: Vec<String>,
    pub(in crate::scenario) unexpected_present_paths: Vec<String>,
    pub(in crate::scenario) checksum_mismatches: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct CatchupSummary {
    pub(in crate::scenario) warm_writers: Vec<String>,
    pub(in crate::scenario) stale_remount_client: String,
    pub(in crate::scenario) cold_discover_client: String,
    pub(in crate::scenario) stale_remount_after_history: bool,
    pub(in crate::scenario) cold_discover_after_history: bool,
    pub(in crate::scenario) final_validation_result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct InvalidationDropoutSummary {
    pub(in crate::scenario) observer_client: String,
    pub(in crate::scenario) writer_clients: Vec<String>,
    pub(in crate::scenario) notify_suppressed: bool,
    pub(in crate::scenario) fallback_interval_ms: u64,
    pub(in crate::scenario) stale_window_ms: u64,
    pub(in crate::scenario) stale_window_observed: bool,
    pub(in crate::scenario) delete_stale_window_observed: bool,
    pub(in crate::scenario) fallback_resumed: bool,
    pub(in crate::scenario) final_observer_path: String,
    pub(in crate::scenario) final_observer_checksum_sha256: String,
    pub(in crate::scenario) final_validation_result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct SymlinkAttributeSummary {
    pub(in crate::scenario) symlink_supported: bool,
    pub(in crate::scenario) symlink_skip_reason: Option<String>,
    pub(in crate::scenario) readonly_attribute_supported: bool,
    pub(in crate::scenario) readonly_attribute_skip_reason: Option<String>,
    pub(in crate::scenario) final_symlink_path: String,
    pub(in crate::scenario) final_symlink_target: String,
    pub(in crate::scenario) readonly_path: String,
    pub(in crate::scenario) readonly_observed_clients: Vec<String>,
    pub(in crate::scenario) final_validation_result: String,
}

impl InvalidationDropoutSummary {
    fn matched(&self) -> bool {
        self.notify_suppressed
            && self.stale_window_observed
            && self.delete_stale_window_observed
            && self.fallback_resumed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct AdvancedMountedSummary {
    pub(in crate::scenario) scenario: String,
    pub(in crate::scenario) bootstrap: Option<VolumeBootstrapSummary>,
    pub(in crate::scenario) clients: usize,
    pub(in crate::scenario) operations: Vec<NamespaceCollisionOperation>,
    pub(in crate::scenario) operation_log: String,
    pub(in crate::scenario) final_validator: TreeComparison,
    pub(in crate::scenario) namespace_violations: Vec<NamespaceInvariantViolation>,
    pub(in crate::scenario) lww_oracle: Option<LwwReconnectOracle>,
    pub(in crate::scenario) sparse_ranges: Option<SparseRangeSummary>,
    #[serde(default)]
    pub(in crate::scenario) cold_observer: Option<ColdObserverSummary>,
    #[serde(default)]
    pub(in crate::scenario) long_history: Option<LongHistorySummary>,
    #[serde(default)]
    pub(in crate::scenario) long_history_validation: Option<LongHistoryValidation>,
    #[serde(default)]
    pub(in crate::scenario) catchup: Option<CatchupSummary>,
    #[serde(default)]
    pub(in crate::scenario) invalidation_dropout: Option<InvalidationDropoutSummary>,
    #[serde(default)]
    pub(in crate::scenario) symlink_attributes: Option<SymlinkAttributeSummary>,
}

pub(in crate::scenario) struct AdvancedMountedExecution {
    pub(in crate::scenario) operations: Vec<NamespaceCollisionOperation>,
    pub(in crate::scenario) lww_oracle: Option<LwwReconnectOracle>,
    pub(in crate::scenario) sparse_ranges: Option<SparseRangeSummary>,
    pub(in crate::scenario) cold_observer: Option<ColdObserverSummary>,
    pub(in crate::scenario) long_history: Option<LongHistorySummary>,
    pub(in crate::scenario) catchup: Option<CatchupSummary>,
    pub(in crate::scenario) invalidation_dropout: Option<InvalidationDropoutSummary>,
    pub(in crate::scenario) symlink_attributes: Option<SymlinkAttributeSummary>,
}

impl LongHistoryValidation {
    fn matched() -> Self {
        Self {
            matched: true,
            missing_present_paths: Vec::new(),
            unexpected_present_paths: Vec::new(),
            checksum_mismatches: Vec::new(),
        }
    }
}

pub(in crate::scenario) fn run_advanced_mounted_with_controller(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let result = run_advanced_mounted_body(context, kind, controller);
    if let Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) = result {
        if let Err(cleanup) = controller.emergency_kill_all_no_wait("advanced mounted timeout") {
            return Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
                scenario: kind.name().to_string(),
                primary: error.to_string(),
                cleanup: cleanup.to_string(),
            });
        }
        return Err(error);
    }

    let unmount_result = controller.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
            scenario: kind.name().to_string(),
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

pub(in crate::scenario) fn run_advanced_mounted_body(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    if kind == AdvancedMountedKind::ColdObserverLongHistory {
        return run_cold_observer_long_history_body(context, controller);
    }
    if kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
        return run_stale_remount_cold_discover_body(context, controller);
    }

    let bootstrap = controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: kind
            .client_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    if kind == AdvancedMountedKind::InvalidationDropoutFallback {
        controller.set_client_env_overrides(
            INVALIDATION_DROPOUT_OBSERVER_CLIENT,
            invalidation_dropout_observer_env_overrides(),
        )?;
    }
    controller.mount_all()?;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    if clients.len() < kind.client_names().len() {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: kind.client_names().len(),
            actual: clients.len(),
        });
    }

    let isolation = ScenarioIsolation::for_advanced(context, kind, &bootstrap.volume_name);
    let execution =
        execute_advanced_mounted_operations(context, kind, controller, &clients, &isolation)?;
    if kind != AdvancedMountedKind::SparseRange {
        write_namespace_collision_operations(context, &execution.operations)?;
    }
    let summary = finish_advanced_mounted(
        context,
        kind,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations: execution.operations,
            lww_oracle: execution.lww_oracle,
            sparse_ranges: execution.sparse_ranges,
            cold_observer: execution.cold_observer,
            long_history: execution.long_history,
            catchup: execution.catchup,
            invalidation_dropout: execution.invalidation_dropout,
            symlink_attributes: execution.symlink_attributes,
        },
    )?;
    Ok(summary)
}

pub(in crate::scenario) fn write_advanced_mounted_plan_artifacts(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let operations = planned_advanced_mounted_operations(kind, started);
    write_namespace_collision_operations(context, &operations)?;
    let lww_oracle = (kind == AdvancedMountedKind::LwwReconnectChaos).then(|| {
        let checksum = bytes_sha256(b"planned lww reconnect winner\n");
        LwwReconnectOracle {
            expected_payload_sha256: checksum.clone(),
            actual_payload_sha256: checksum,
            matched: true,
        }
    });
    let sparse_ranges =
        (kind == AdvancedMountedKind::SparseRange).then(planned_sparse_range_summary);
    if let Some(oracle) = &lww_oracle {
        write_lww_oracle(context, oracle)?;
    }
    if kind == AdvancedMountedKind::ColdObserverLongHistory {
        write_planned_cold_observer_diagnostics(context)?;
    }
    if kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
        write_planned_catchup_diagnostics(context)?;
    }
    if kind == AdvancedMountedKind::InvalidationDropoutFallback {
        write_planned_invalidation_dropout_diagnostics(
            context,
            &planned_invalidation_dropout_summary(),
        )?;
    }
    let compared_clients = kind
        .client_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let summary = AdvancedMountedSummary {
        scenario: kind.name().to_string(),
        bootstrap: None,
        clients: kind.client_names().len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: TreeComparison {
            converged: true,
            reference_client: compared_clients.first().cloned(),
            compared_clients,
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
        lww_oracle,
        sparse_ranges,
        cold_observer: (kind == AdvancedMountedKind::ColdObserverLongHistory)
            .then(planned_cold_observer_summary),
        long_history: match kind {
            AdvancedMountedKind::ColdObserverLongHistory => Some(planned_long_history_summary()),
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup => {
                Some(planned_stale_remount_long_history_summary())
            }
            _ => None,
        },
        long_history_validation: match kind {
            AdvancedMountedKind::ColdObserverLongHistory
            | AdvancedMountedKind::StaleRemountColdDiscoverCatchup => {
                Some(LongHistoryValidation::matched())
            }
            _ => None,
        },
        catchup: (kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup)
            .then(planned_catchup_summary),
        invalidation_dropout: (kind == AdvancedMountedKind::InvalidationDropoutFallback)
            .then(planned_invalidation_dropout_summary),
        symlink_attributes: (kind == AdvancedMountedKind::SymlinkAttribute)
            .then(planned_symlink_attribute_summary),
    };
    write_advanced_mounted_summary(context, kind, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn execute_advanced_mounted_operations(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<AdvancedMountedExecution, ScenarioError> {
    match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance
        | AdvancedMountedKind::CrashAfterPartialWriteAcceptance
        | AdvancedMountedKind::CrashAfterFullWriteAcceptance => Ok(AdvancedMountedExecution {
            operations: run_write_acceptance_crash_operations(
                kind, controller, clients, isolation,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::MountedChaos => Ok(AdvancedMountedExecution {
            operations: run_mounted_chaos_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::HotspotChurn => Ok(AdvancedMountedExecution {
            operations: run_hotspot_churn_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::FaultChaos => Ok(AdvancedMountedExecution {
            operations: run_fault_chaos_operations(context, controller, clients, isolation)?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::LwwReconnectChaos => {
            let (operations, oracle) =
                run_lww_reconnect_operations(context, controller, clients, isolation)?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: Some(oracle),
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::OpenHandle => Ok(AdvancedMountedExecution {
            operations: run_open_handle_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::SparseRange => {
            let (operations, summary) = run_sparse_range_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: Some(summary),
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::InvalidationDropoutFallback => {
            let (operations, summary) = run_invalidation_dropout_fallback_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: Some(summary),
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::SymlinkAttribute => {
            let (operations, summary) = run_symlink_attribute_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: Some(summary),
            })
        }
        AdvancedMountedKind::ColdObserverLongHistory => unreachable!(
            "cold-observer long-history uses delayed discovery and is handled before common bootstrap"
        ),
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup => unreachable!(
            "stale-remount cold-discover catch-up uses delayed remount/discovery and is handled before common bootstrap"
        ),
    }
}

pub(in crate::scenario) fn run_cold_observer_long_history_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(COLD_OBSERVER_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    for client in &COLD_OBSERVER_WARM_CLIENT_NAMES[1..] {
        controller.discover(client)?;
    }
    for client in COLD_OBSERVER_WARM_CLIENT_NAMES {
        controller.mount(client)?;
    }

    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| COLD_OBSERVER_WARM_CLIENT_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bootstrap = VolumeBootstrapSummary {
        volume_name: warm_clients
            .first()
            .map(|client| client.volume_name.clone())
            .unwrap_or_else(|| "projectfs".to_string()),
        init_client: COLD_OBSERVER_WARM_CLIENT_NAMES[0].to_string(),
        discovered_clients: COLD_OBSERVER_WARM_CLIENT_NAMES[1..]
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        clients: warm_clients
            .iter()
            .map(|client| crate::client::ClientBootstrapRecord {
                name: client.name.clone(),
                local_metadata_db: client.local_metadata_db.clone(),
                state: client.state,
            })
            .collect(),
    };
    write_bootstrap_summary(context, &bootstrap)?;

    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::ColdObserverLongHistory,
        &bootstrap.volume_name,
    );
    let started = Instant::now();
    let (mut operations, mut long_history) =
        run_cold_observer_warm_history(started, context, &warm_clients, &isolation)?;

    controller.discover("client-d")?;
    let cold_client = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-d")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold observer client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_discover_after_history",
        "client-d",
        Ok(()),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_mount_after_history",
        "client-d",
        controller.mount("client-d").map_err(ScenarioError::from),
    )?);
    long_history.bounded_operation_count = operations.len();
    write_namespace_collision_operations(context, &operations)?;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let cold = clients
        .iter()
        .find(|client| client.name == "client-d")
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold observer client-d")))?;
    let cold_observer = ColdObserverSummary {
        client: cold.name.clone(),
        local_metadata_db: cold.local_metadata_db.clone(),
        cache_dir: cold.cache_dir.clone(),
        discovered_after_history: true,
    };

    finish_advanced_mounted(
        context,
        AdvancedMountedKind::ColdObserverLongHistory,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: Some(cold_observer),
            long_history: Some(long_history),
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        },
    )
}

pub(in crate::scenario) fn run_stale_remount_cold_discover_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(COLD_OBSERVER_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    for client in &STALE_REMOUNT_INITIAL_CLIENT_NAMES[1..] {
        controller.discover(client)?;
    }
    for client in STALE_REMOUNT_INITIAL_CLIENT_NAMES {
        controller.mount(client)?;
    }

    let initial_clients = controller
        .clients()
        .into_iter()
        .filter(|client| STALE_REMOUNT_INITIAL_CLIENT_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bootstrap = VolumeBootstrapSummary {
        volume_name: initial_clients
            .first()
            .map(|client| client.volume_name.clone())
            .unwrap_or_else(|| "projectfs".to_string()),
        init_client: "client-a".to_string(),
        discovered_clients: vec!["client-b".to_string(), "client-c".to_string()],
        clients: initial_clients
            .iter()
            .map(|client| crate::client::ClientBootstrapRecord {
                name: client.name.clone(),
                local_metadata_db: client.local_metadata_db.clone(),
                state: client.state,
            })
            .collect(),
    };
    write_bootstrap_summary(context, &bootstrap)?;

    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        &bootstrap.volume_name,
    );
    let started = Instant::now();
    let mut operations = Vec::new();
    let stale_client = initial_clients
        .iter()
        .find(|client| client.name == "client-c")
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing stale remount client-c")))?;
    let baseline_path = isolation.path("baseline.txt");
    let baseline_payload = isolation.payload("initial_baseline", &baseline_path, "baseline");
    operations.push(timed_cold_observer_helper_fs_op(
        context,
        started,
        1,
        stale_client,
        "initial_baseline",
        "create",
        &baseline_path,
        &baseline_path,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&baseline_payload),
        None,
    )?);
    write_namespace_collision_operations(context, &operations)?;
    wait_for_mounted_initial_payloads_until(
        &initial_clients,
        &[(&baseline_path, baseline_payload.as_slice())],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "stale remount initial baseline",
    )?;
    operations.push(required_op(
        started,
        2,
        stale_client,
        "stale_client_unmount",
        "client-c",
        controller.unmount("client-c").map_err(ScenarioError::from),
    )?);
    write_namespace_collision_operations(context, &operations)?;

    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| STALE_REMOUNT_WARM_WRITER_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let (mut warm_history, long_history) = run_stale_remount_warm_history(
        context,
        started,
        &warm_clients,
        &isolation,
        operations.len() + 1,
        &operations,
    )?;
    operations.append(&mut warm_history);
    write_namespace_collision_operations(context, &operations)?;

    let stale_after_history = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-c")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing stale remount client-c")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &stale_after_history,
        "stale_remount_after_history",
        "client-c",
        controller.remount("client-c").map_err(ScenarioError::from),
    )?);
    controller.discover("client-d")?;
    let cold_after_discover = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-d")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold discover client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_after_discover,
        "cold_discover_after_history",
        "client-d",
        controller.mount("client-d").map_err(ScenarioError::from),
    )?);
    write_namespace_collision_operations(context, &operations)?;

    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let catchup = CatchupSummary {
        warm_writers: STALE_REMOUNT_WARM_WRITER_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        stale_remount_client: "client-c".to_string(),
        cold_discover_client: "client-d".to_string(),
        stale_remount_after_history: true,
        cold_discover_after_history: true,
        final_validation_result: "mounted_tree_convergence".to_string(),
    };
    write_catchup_diagnostics(context, &catchup)?;
    finish_advanced_mounted(
        context,
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: Some(long_history),
            catchup: Some(catchup),
            invalidation_dropout: None,
            symlink_attributes: None,
        },
    )
}

pub(in crate::scenario) fn run_stale_remount_warm_history(
    context: &ScenarioContext,
    started: Instant,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    first_order: usize,
    previously_recorded: &[NamespaceCollisionOperation],
) -> Result<(Vec<NamespaceCollisionOperation>, LongHistorySummary), ScenarioError> {
    let mut operations = Vec::new();
    let mut persisted_operations = previously_recorded.to_vec();
    let delete_target = isolation.path(COLD_OBSERVER_DELETE_TARGET);
    let rename_source = isolation.path(COLD_OBSERVER_RENAME_SOURCE);
    let rename_dest = isolation.path(COLD_OBSERVER_RENAME_DEST);
    let overwrite_target = isolation.path(COLD_OBSERVER_OVERWRITE_TARGET);
    let append_target = isolation.path(COLD_OBSERVER_APPEND_TARGET);
    let nested_file = isolation.path(COLD_OBSERVER_NESTED_FILE);
    let payload = |label: &str, path: &str| isolation.payload(label, path, label);
    let rename_payload = payload("rename_source", &rename_source);
    let append_payload_bytes = payload("append", &append_target);
    let nested_payload = payload("nested_child", &nested_file);

    for (offset, (client_index, operation, path, log_path, action)) in [
        (
            0,
            "create",
            delete_target.clone(),
            delete_target.clone(),
            Some(payload("delete_target", &delete_target)),
        ),
        (
            0,
            "delete",
            delete_target.clone(),
            delete_target.clone(),
            None,
        ),
        (
            0,
            "create",
            rename_source.clone(),
            rename_source.clone(),
            Some(rename_payload.clone()),
        ),
        (
            1,
            "rename",
            rename_dest.clone(),
            format!("{rename_source} -> {rename_dest}"),
            None,
        ),
        (
            0,
            "overwrite",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v2", &overwrite_target)),
        ),
        (
            1,
            "append",
            append_target.clone(),
            append_target.clone(),
            Some(append_payload_bytes.clone()),
        ),
        (
            0,
            "create",
            nested_file.clone(),
            nested_file.clone(),
            Some(nested_payload.clone()),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let client = &clients[client_index];
        let operation_path = if operation == "rename" {
            rename_source.as_str()
        } else {
            path.as_str()
        };
        let target = (operation == "rename").then_some(rename_dest.as_str());
        let helper_operation = if operation == "overwrite" {
            "overwrite-barrier"
        } else {
            operation
        };
        let recorded = timed_cold_observer_helper_fs_op(
            context,
            started,
            first_order + offset,
            client,
            operation,
            helper_operation,
            operation_path,
            &log_path,
            HOTSPOT_CHURN_OPERATION_TIMEOUT,
            action.as_deref(),
            target,
        )?;
        record_sparse_operation(context, &mut persisted_operations, recorded.clone())?;
        operations.push(recorded);
        if matches!(operation, "create" | "overwrite") && path != delete_target {
            wait_for_mounted_initial_payloads_until(
                clients,
                &[(&path, action.as_deref().unwrap())],
                Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                "stale remount warm history",
            )?;
        }
    }

    let final_payload = payload("overwrite_v2", &overwrite_target);
    let bounded_operation_count = first_order + operations.len() - 1;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&rename_dest, rename_payload.as_slice()),
            (&overwrite_target, final_payload.as_slice()),
            (&append_target, append_payload_bytes.as_slice()),
            (&nested_file, nested_payload.as_slice()),
        ],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "stale remount warm history",
    )?;
    Ok((
        operations,
        LongHistorySummary {
            bounded_operation_count,
            expected_present_paths: vec![
                rename_dest,
                overwrite_target.clone(),
                append_target,
                nested_file,
            ],
            expected_absent_paths: vec![delete_target, rename_source],
            final_checksum_sha256: bytes_sha256(&final_payload),
        },
    ))
}

pub(in crate::scenario) fn run_cold_observer_warm_history(
    started: Instant,
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<(Vec<NamespaceCollisionOperation>, LongHistorySummary), ScenarioError> {
    let mut operations = Vec::new();
    let nested_dir = isolation.path("nested");
    let delete_target = isolation.path(COLD_OBSERVER_DELETE_TARGET);
    let rename_source = isolation.path(COLD_OBSERVER_RENAME_SOURCE);
    let rename_dest = isolation.path(COLD_OBSERVER_RENAME_DEST);
    let overwrite_target = isolation.path(COLD_OBSERVER_OVERWRITE_TARGET);
    let append_target = isolation.path(COLD_OBSERVER_APPEND_TARGET);
    let nested_file = isolation.path(COLD_OBSERVER_NESTED_FILE);

    let payload = |label: &str, path: &str| isolation.payload(label, path, label);
    let nested_payload = payload("nested_child", &nested_file);
    for (order, (client_index, operation, path, log_path, action)) in (1..).zip([
        (0, "mkdir", nested_dir.clone(), nested_dir.clone(), None),
        (
            0,
            "create",
            delete_target.clone(),
            delete_target.clone(),
            Some(payload("delete_target", &delete_target)),
        ),
        (
            1,
            "delete",
            delete_target.clone(),
            delete_target.clone(),
            None,
        ),
        (
            0,
            "create",
            rename_source.clone(),
            rename_source.clone(),
            Some(payload("rename_source", &rename_source)),
        ),
        (
            1,
            "rename",
            rename_dest.clone(),
            format!("{rename_source} -> {rename_dest}"),
            None,
        ),
        (
            2,
            "create",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v1", &overwrite_target)),
        ),
        (
            0,
            "overwrite",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v2", &overwrite_target)),
        ),
        (
            1,
            "append",
            append_target.clone(),
            append_target.clone(),
            Some(payload("append", &append_target)),
        ),
        (
            2,
            "create",
            nested_file.clone(),
            nested_file.clone(),
            Some(nested_payload.clone()),
        ),
    ]) {
        let client = &clients[client_index];
        let operation_path = if operation == "rename" {
            rename_source.as_str()
        } else {
            path.as_str()
        };
        let target = (operation == "rename").then_some(rename_dest.as_str());
        let helper_operation = if operation == "overwrite" {
            "overwrite-barrier"
        } else {
            operation
        };
        let operation_record = timed_cold_observer_helper_fs_op(
            context,
            started,
            order,
            client,
            operation,
            helper_operation,
            operation_path,
            &log_path,
            HOTSPOT_CHURN_OPERATION_TIMEOUT,
            action.as_deref(),
            target,
        )?;
        operations.push(operation_record);
        match operation {
            "create" if path == delete_target || path == rename_source => {
                wait_for_mounted_initial_payloads_until(
                    clients,
                    &[(&path, action.as_deref().unwrap())],
                    Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                    ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                    "cold observer warm history",
                )?;
            }
            "create" | "overwrite" if path == overwrite_target => {
                wait_for_mounted_initial_payloads_until(
                    clients,
                    &[(&path, action.as_deref().unwrap())],
                    Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                    ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                    "cold observer warm history",
                )?;
            }
            _ => {}
        }
    }

    let final_payload = payload("overwrite_v2", &overwrite_target);
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&overwrite_target, final_payload.as_slice()),
            (&nested_file, nested_payload.as_slice()),
        ],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "cold observer warm history",
    )?;
    fs::write(
        context.scenario_dir.join("cold-observer-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_observer": "client-d",
            "warm_clients": COLD_OBSERVER_WARM_CLIENT_NAMES,
            "discovered_after_history": true,
        }))?,
    )?;

    let bounded_operation_count = operations.len();
    Ok((
        operations,
        LongHistorySummary {
            bounded_operation_count,
            expected_present_paths: vec![
                rename_dest,
                overwrite_target.clone(),
                append_target,
                nested_file,
            ],
            expected_absent_paths: vec![delete_target, rename_source],
            final_checksum_sha256: bytes_sha256(&final_payload),
        },
    ))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_cold_observer_helper_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    helper_operation: &str,
    path: &str,
    log_path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let label = format!("{} {} {}", client.name, operation, log_path);
    run_hotspot_helper_process(
        context,
        timeout,
        client,
        helper_operation,
        path,
        target,
        payload,
        label,
    )?;
    required_op(started, order, client, operation, log_path, Ok(()))
}

pub(in crate::scenario) fn run_write_acceptance_crash_operations(
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let mut operations = Vec::new();
    let phase = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => "attempt_create_before_crash",
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => "partial_write",
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => "full_write",
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    let file_name = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => "before-write.txt",
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => "partial-write.txt",
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => "full-write.txt",
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    let path = isolation.path(file_name);
    let payload = isolation.payload(
        phase,
        &path,
        &format!("write acceptance crash phase {phase}\n"),
    );
    let expected = expected_write_acceptance_payload(kind, &payload);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        phase,
        &path,
        match expected.as_deref() {
            Some(accepted_payload) => {
                write_acceptance_payload(writer.mount_point.join(&path), accepted_payload)
                    .map_err(|error| error.to_string())
            }
            None => Ok(()),
        },
    ))?);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "force_kill",
        &path,
        controller
            .force_kill(&writer.name)
            .map_err(|error| error.to_string()),
    ))?);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "remount",
        &path,
        controller
            .remount(&writer.name)
            .map_err(|error| error.to_string()),
    ))?);
    let recovered_path = writer.mount_point.join(&path);
    let recovered = bounded_mounted_read(
        recovered_path.clone(),
        format!(
            "write acceptance recovered read {}",
            recovered_path.display()
        ),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    )
    .map_err(scenario_error_to_io);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "readback",
        &path,
        match expected.as_deref() {
            Some(expected) => recovered
                .map_err(|error| error.to_string())
                .and_then(|bytes| {
                    if bytes == expected {
                        Ok(())
                    } else {
                        Err(format!(
                            "readback mismatch for {path}: expected checksum {}, got {}",
                            bytes_sha256(expected),
                            bytes_sha256(&bytes)
                        ))
                    }
                }),
            None => match recovered {
                Ok(bytes) => Err(format!(
                    "readback unexpectedly found {path}: {} bytes, checksum {}",
                    bytes.len(),
                    bytes_sha256(&bytes)
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.to_string()),
            },
        },
    ))?);
    operations.sort_by_key(|operation| operation.order);
    Ok(operations)
}

pub(in crate::scenario) fn expected_write_acceptance_payload(
    kind: AdvancedMountedKind,
    payload: &[u8],
) -> Option<Vec<u8>> {
    match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => None,
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => {
            Some(payload[..payload.len() / 2].to_vec())
        }
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => Some(payload.to_vec()),
        _ => unreachable!("write acceptance crash scenario expected"),
    }
}

pub(in crate::scenario) fn write_acceptance_payload(
    path: PathBuf,
    accepted_payload: &[u8],
) -> io::Result<()> {
    let payload = accepted_payload.to_vec();
    run_bounded_mounted_io(
        format!("write acceptance payload {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut options = OpenOptions::new();
            options.create(true).truncate(true).write(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;

                const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
                options.custom_flags(FILE_FLAG_WRITE_THROUGH);
            }
            let mut file = options.open(path)?;
            if !payload.is_empty() {
                file.write_all(&payload)?;
            }
            file.sync_all()
        },
    )
    .map_err(scenario_error_to_io)
}

pub(in crate::scenario) fn run_mounted_chaos_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let active_workers = Arc::new(AtomicUsize::new(0));
    let writer = &clients[0];
    seed_mounted_chaos_initial_tree(writer, isolation)?;
    wait_for_mounted_chaos_initial_tree(
        context,
        clients,
        isolation,
        initial_convergence_timeout,
        poll_interval,
    )?;
    for &(path, _) in MOUNTED_CHAOS_INITIAL_TREE {
        let path = isolation.path(path.strip_prefix("chaos/").unwrap_or(path));
        wait_for_hotspot_helper_stat_until(
            context,
            writer,
            &path,
            Instant::now() + HOTSPOT_CHURN_OPERATION_TIMEOUT,
            poll_interval,
            "mounted chaos initial path before conflicting operations",
            Arc::clone(&active_workers),
        )?;
    }
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        1,
        &clients[0],
        "rename",
        &isolation.path("rename-source.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        Some(&isolation.path("renamed-by-a.txt")),
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        2,
        &clients[1],
        "unlink",
        &isolation.path("unlink-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let append_payload = isolation.payload("append", "append-target.txt", "appended by chaos\n");
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        3,
        &clients[2],
        "append",
        &isolation.path("append-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&append_payload),
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let overwrite_payload =
        isolation.payload("overwrite", "read-target.txt", "new committed payload\n");
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        4,
        &clients[0],
        "overwrite",
        &isolation.path("read-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&overwrite_payload),
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        5,
        &clients[1],
        "mkdir",
        &isolation.path("dir"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        6,
        &clients[2],
        "readdir",
        &isolation.root,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    Ok(operations)
}

pub(in crate::scenario) fn seed_mounted_chaos_initial_tree(
    writer: &ClientSpec,
    isolation: &ScenarioIsolation,
) -> Result<(), ScenarioError> {
    for &(path, payload) in MOUNTED_CHAOS_INITIAL_TREE {
        let logical_path = path.strip_prefix("chaos/").unwrap_or(path);
        let mounted_path = isolation.path(logical_path);
        let payload =
            isolation.payload("initial", &mounted_path, &String::from_utf8_lossy(payload));
        write_mounted_initial_file(
            writer.mount_point.join(mounted_path),
            &payload,
            "mounted chaos initial tree",
        )?;
    }
    Ok(())
}

pub(in crate::scenario) fn write_mounted_initial_file(
    path: PathBuf,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    let write_path = path.clone();
    let write_payload = payload.to_vec();
    let write_label = label.to_string();
    run_bounded_mounted_io(
        format!("{label}: write {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || {
            if let Some(parent) = write_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut options = mounted_write_barrier_open_options();
            let mut file = options
                .create_new(true)
                .open(&write_path)
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("{write_label}: open {}: {error}", write_path.display()),
                    )
                })?;
            file.write_all(&write_payload)?;
            file.sync_all()
        },
    )?;
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

pub(in crate::scenario) fn scenario_error_to_io(error: ScenarioError) -> io::Error {
    match error {
        ScenarioError::Io(error) => error,
        error => io::Error::other(error),
    }
}

pub(in crate::scenario) fn wait_for_mounted_payload_readback(
    path: &Path,
    payload: &[u8],
    timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<u8>, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let mut last_readback = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Some(probe_timeout) = mounted_payload_readback_probe_timeout(remaining) else {
            return last_readback.ok_or_else(|| {
                ScenarioError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("mounted payload readback did not find {}", path.display()),
                ))
            });
        };
        match bounded_mounted_read(
            path.to_path_buf(),
            format!("readback {}", path.display()),
            probe_timeout,
        ) {
            Ok(readback) if readback == payload => return Ok(readback),
            Ok(readback) => last_readback = Some(readback),
            Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            return last_readback.ok_or_else(|| {
                ScenarioError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("mounted payload readback did not find {}", path.display()),
                ))
            });
        }
        thread::sleep(poll_interval.max(Duration::from_millis(1)));
    }
}

pub(in crate::scenario) fn mounted_payload_readback_probe_timeout(
    remaining: Duration,
) -> Option<Duration> {
    if remaining < Duration::from_millis(1) {
        return None;
    }
    Some(remaining.min(MOUNTED_WRITE_READBACK_TIMEOUT))
}

pub(in crate::scenario) fn mounted_write_barrier_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
        options.custom_flags(FILE_FLAG_WRITE_THROUGH);
    }
    options
}

pub(in crate::scenario) fn wait_for_mounted_chaos_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let required_paths = MOUNTED_CHAOS_INITIAL_TREE
        .iter()
        .map(|(path, payload)| {
            let logical_path = path.strip_prefix("chaos/").unwrap_or(path);
            let mounted_path = isolation.path(logical_path);
            let payload =
                isolation.payload("initial", &mounted_path, &String::from_utf8_lossy(payload));
            (mounted_path, payload)
        })
        .collect::<Vec<_>>();
    let required_paths = required_paths
        .iter()
        .map(|(path, payload)| (path.as_str(), payload.as_slice()))
        .collect::<Vec<_>>();
    wait_for_mounted_initial_payloads_until(
        clients,
        &required_paths,
        deadline,
        poll_interval,
        "mounted chaos initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "mounted chaos initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-chaos"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

pub(in crate::scenario) fn run_hotspot_churn_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    run_hotspot_churn_operations_with_timeout(
        context,
        clients,
        isolation,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        initial_convergence_timeout,
        poll_interval,
    )
}

pub(in crate::scenario) fn run_hotspot_churn_operations_with_timeout(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    operation_timeout: Duration,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let mkdir_client = clients[0].clone();
    run_hotspot_helper_process(
        context,
        operation_timeout,
        &mkdir_client,
        "mkdir",
        &isolation.root,
        None,
        None,
        format!("{} mkdir {}", mkdir_client.name, isolation.root),
    )?;
    wait_for_hotspot_initial_tree(
        context,
        clients,
        &isolation.root,
        initial_convergence_timeout,
        poll_interval,
    )?;
    for round in 0..10 {
        let slot = isolation.path(&format!("slot-{}.txt", round % 6));
        let renamed = hotspot_churn_rename_target(isolation, round);
        let write_path = slot.clone();
        let write_payload = isolation.payload(
            "overwrite",
            &write_path,
            &format!("hotspot round {round}\n"),
        );
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[0],
            "overwrite",
            &slot,
            operation_timeout,
            Some(&write_payload),
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        let append_path = slot.clone();
        let append_payload_bytes =
            isolation.payload("append", &append_path, &format!("append round {round}\n"));
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[1],
            "append",
            &slot,
            operation_timeout,
            Some(&append_payload_bytes),
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        wait_for_client_path_visible_until(
            &clients[0],
            &slot,
            Instant::now() + operation_timeout,
            poll_interval,
            "hotspot rename source",
        )?;
        let rename_target = renamed.clone();
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[0],
            "rename",
            &slot,
            operation_timeout,
            None,
            Some(&rename_target),
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[2],
            "readdir",
            &isolation.root,
            operation_timeout,
            None,
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
    }
    Ok(operations)
}

pub(in crate::scenario) fn hotspot_churn_rename_target(
    isolation: &ScenarioIsolation,
    round: usize,
) -> String {
    isolation.path(&format!("slot-{}-round-{round}-renamed.txt", round % 6))
}

pub(in crate::scenario) fn wait_for_hotspot_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    root: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    wait_for_required_paths_visible_until(
        clients,
        &[root],
        deadline,
        poll_interval,
        "hotspot initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "hotspot initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-hotspot"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_hotspot_helper_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    timed_hotspot_helper_fs_op_with_helper(
        context, started, order, client, operation, operation, path, timeout, payload, target,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_hotspot_helper_fs_op_with_helper(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    helper_operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let label = format!("{} {} {}", client.name, operation, path);
    match run_hotspot_helper_process(
        context,
        timeout,
        client,
        helper_operation,
        path,
        target,
        payload,
        label,
    ) {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) if hotspot_helper_error_is_fatal(&error) => Err(error),
        Err(error) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Err(error.to_string()),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_hotspot_mount_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
    active_workers: Arc<AtomicUsize>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let _ = active_workers;
    match run_hotspot_helper_process(
        context,
        timeout,
        client,
        operation,
        path,
        target,
        payload,
        format!("{} {} {}", client.name, operation, path),
    ) {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) if hotspot_helper_error_is_fatal(&error) => Err(error),
        Err(error) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Err(error.to_string()),
        )),
    }
}

pub(in crate::scenario) fn hotspot_helper_error_is_fatal(error: &ScenarioError) -> bool {
    matches!(
        error,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
            | ScenarioError::NamespaceCollisionUnmountAfterFailure { .. }
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_hotspot_mount_operation_with_timeout(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
    active_workers: Arc<AtomicUsize>,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;
    let mount_point = client.mount_point.clone();
    let path_owned = path.to_string();
    let target_owned = target.map(str::to_string);
    let operation_owned = operation.to_string();
    let pending_label = format!("{} {} {}", client.name, operation, path);
    let result = run_timed_hotspot_io_with_worker_limit(
        timeout,
        pending_label.clone(),
        move || {
            run_hotspot_op_helper(
                &operation_owned,
                mount_point,
                &path_owned,
                target_owned.as_deref(),
                payload_path.as_deref(),
                Some(&progress_path),
            )
            .map_err(io::Error::other)
        },
        active_workers,
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    );
    match result {
        Ok(()) => Ok(()),
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
            #[cfg(not(test))]
            let _ = terminate_blocked_hotspot_client_process(client, &pending_label);
            Err(error)
        }
        Err(error) => Err(ScenarioError::Io(io::Error::other(format!(
            "{pending_label}: {error}"
        )))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_hotspot_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    target: Option<&str>,
    payload: Option<&[u8]>,
    pending_label: String,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;

    #[cfg(test)]
    {
        let _ = timeout;
        let _ = pending_label;
        let result = run_hotspot_op_helper(
            operation,
            client.mount_point.clone(),
            path,
            target,
            payload_path.as_deref(),
            Some(&progress_path),
        );
        result.map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "hotspot helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("hotspot-op")
            .arg("--operation")
            .arg(operation)
            .arg("--sandbox")
            .arg(&context.sandbox)
            .arg("--mount-point")
            .arg(&client.mount_point)
            .arg("--path")
            .arg(path)
            .arg("--progress")
            .arg(&progress_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(target) = target {
            command.arg("--target").arg(target);
        }
        if let Some(payload_path) = &payload_path {
            command.arg("--payload").arg(payload_path);
        }
        command
            .arg("--timeout-ms")
            .arg(duration_millis_u64(hotspot_helper_internal_timeout(timeout)).to_string());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        write_hotspot_helper_process_metadata(
            context,
            child.id(),
            client,
            operation,
            path,
            target,
        )?;
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate hotspot helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                let stderr = child
                    .stderr
                    .take()
                    .and_then(|mut stderr| {
                        let mut output = String::new();
                        stderr.read_to_string(&mut output).ok().map(|_| output)
                    })
                    .unwrap_or_default();
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                if stderr.contains(HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER) {
                    return Err(namespace_collision_operation_timeout(
                        timeout,
                        vec![(1, format!("{pending_label}{progress_suffix}"))],
                    ));
                }
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "hotspot helper failed for {pending_label}: status={status}; stderr={}; progress={}",
                    stderr.trim(),
                    progress.as_deref().unwrap_or("<none>")
                ))));
            }
            if Instant::now() >= deadline {
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                eprintln!(
                    "studiofs-e2e: mounted helper operation timed out after {} ms; terminating helper process: {}{}",
                    timeout.as_millis(),
                    pending_label,
                    progress_suffix
                );
                let mut cleanup_error =
                    terminate_blocked_hotspot_client_process(client, &pending_label)
                        .err()
                        .map(|error| error.to_string());
                let _ = job.terminate();
                if should_wait_for_timed_out_hotspot_helper_child() {
                    if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                        let _ = job.force_kill();
                        if let Err(error) =
                            terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                        {
                            cleanup_error.get_or_insert_with(|| error.to_string());
                        }
                        let _ = child.kill();
                        let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
                    }
                } else {
                    let _ = job.force_kill();
                    if let Err(error) =
                        terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                    {
                        cleanup_error.get_or_insert_with(|| error.to_string());
                    }
                    let _ = child.kill();
                }
                if let Err(error) = terminate_blocked_hotspot_client_process(client, &pending_label)
                {
                    cleanup_error.get_or_insert_with(|| error.to_string());
                }
                let primary = namespace_collision_operation_timeout(
                    timeout,
                    vec![(1, format!("{pending_label}{progress_suffix}"))],
                );
                if let Some(cleanup) = cleanup_error {
                    return Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                        primary: primary.to_string(),
                        cleanup,
                    });
                }
                return Err(primary);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

pub(in crate::scenario) fn should_wait_for_timed_out_hotspot_helper_child() -> bool {
    cfg!(windows)
}

pub(in crate::scenario) fn hotspot_helper_internal_timeout(parent_timeout: Duration) -> Duration {
    let reserve = CHILD_TERMINATION_GRACE + HOTSPOT_HELPER_CANCEL_GRACE;
    parent_timeout
        .checked_sub(reserve)
        .unwrap_or(parent_timeout)
}

#[cfg(all(windows, not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_helper_process(
    pid: u32,
    pending_label: &str,
) -> io::Result<()> {
    eprintln!(
        "studiofs-e2e: force terminating hotspot helper process {pid} to release blocked I/O: {pending_label}"
    );
    match terminate_windows_pid_tree(pid, "blocked hotspot helper process", pending_label) {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to terminate blocked hotspot helper process {pid}: {error}"
            );
            Err(error)
        }
    }
}

#[cfg(all(not(windows), not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_helper_process(
    _pid: u32,
    _pending_label: &str,
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(windows, not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_client_process(
    client: &ClientSpec,
    pending_label: &str,
) -> io::Result<()> {
    let Some(pid) = client.process_id else {
        return Ok(());
    };
    eprintln!(
        "studiofs-e2e: terminating mounted client process {pid} to release blocked hotspot worker I/O: {pending_label}"
    );
    match terminate_windows_pid_tree(pid, "blocked mounted client process", pending_label) {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to terminate blocked mounted client process {pid}: {error}"
            );
            Err(error)
        }
    }
}

#[cfg(all(not(windows), not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_client_process(
    _client: &ClientSpec,
    _pending_label: &str,
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(windows, not(test)))]
pub(in crate::scenario) fn terminate_windows_pid_tree(
    pid: u32,
    label: &str,
    pending_label: &str,
) -> io::Result<()> {
    capture_process_diagnostics_best_effort(pid, &format!("{label}: {pending_label}"));
    match run_taskkill_with_timeout(pid, Duration::from_secs(5)) {
        Ok(output) if windows_taskkill_status_is_success(output.status.code()) => Ok(()),
        Ok(output) => {
            eprintln!(
                "studiofs-e2e: taskkill for {label} {pid} exited with {}; stdout={}; stderr={}",
                output.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
            Err(io::Error::other(format!(
                "taskkill for {label} {pid} exited with {}; {pending_label}",
                output.status.code().unwrap_or(-1)
            )))
        }
        Err(error) => {
            eprintln!("studiofs-e2e: taskkill for {label} {pid} failed: {error}");
            Err(error)
        }
    }
}

#[cfg(not(test))]
pub(in crate::scenario) fn prepare_hotspot_helper_exe(
    context: &ScenarioContext,
) -> io::Result<PathBuf> {
    let source = std::env::current_exe()?;
    let helper_dir = context.scenario_dir.join("hotspot-helper-bin");
    fs::create_dir_all(&helper_dir)?;
    let extension = source
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("exe");
    let helper = helper_dir.join(format!("studiofs-e2e-hotspot-helper.{extension}"));
    if !helper.exists() {
        fs::copy(source, &helper)?;
    }
    Ok(helper)
}

pub(in crate::scenario) fn write_helper_payload(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    payload: Option<&[u8]>,
) -> Result<Option<PathBuf>, ScenarioError> {
    match payload {
        Some(payload) => {
            let payload_dir = context.scenario_dir.join("hotspot-op-payloads");
            fs::create_dir_all(&payload_dir)?;
            let id = HOTSPOT_HELPER_PAYLOAD_ID.fetch_add(1, Ordering::SeqCst);
            let payload_path =
                payload_dir.join(format!("{}-{}-{}.payload", id, client.name, operation));
            fs::write(&payload_path, payload)?;
            Ok(Some(payload_path))
        }
        None => Ok(None),
    }
}

pub(in crate::scenario) fn helper_progress_path(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    path: &str,
) -> Result<PathBuf, ScenarioError> {
    let progress_dir = context.scenario_dir.join("hotspot-helper-progress");
    fs::create_dir_all(&progress_dir)?;
    let id = HOTSPOT_HELPER_PAYLOAD_ID.fetch_add(1, Ordering::SeqCst);
    Ok(progress_dir.join(format!(
        "{id:06}-{}-{}-{}.jsonl",
        client.name,
        sanitize_hotspot_progress_component(operation),
        sanitize_hotspot_progress_component(path)
    )))
}

#[cfg(not(test))]
pub(in crate::scenario) fn write_hotspot_helper_process_metadata(
    context: &ScenarioContext,
    pid: u32,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    target: Option<&str>,
) -> io::Result<()> {
    let process_dir = context.scenario_dir.join("hotspot-helper-processes");
    fs::create_dir_all(&process_dir)?;
    let metadata = json!({
        "process_id": pid,
        "process_name": "studiofs-e2e-hotspot-helper",
        "client": client.name,
        "operation": operation,
        "path": path,
        "target": target,
    });
    fs::write(
        process_dir.join(format!("{pid}.json")),
        serde_json::to_vec_pretty(&metadata).map_err(io::Error::other)?,
    )
}

#[cfg(not(test))]
pub(in crate::scenario) fn read_hotspot_helper_progress_tail(progress: &Path) -> Option<String> {
    let contents = fs::read_to_string(progress).ok()?;
    contents
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::to_string)
}

pub(in crate::scenario) fn run_timed_hotspot_io_with_worker_limit<T: Send + 'static>(
    timeout: Duration,
    pending_label: String,
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
    active_workers: Arc<AtomicUsize>,
    max_active_workers: usize,
) -> Result<T, ScenarioError> {
    let Some(permit) = HotspotIoWorkerPermit::try_acquire(active_workers, max_active_workers)
    else {
        let pending_label = format!(
            "hotspot worker limit reached; previous timed operation may still be blocked before {pending_label}"
        );
        eprintln!(
            "studiofs-e2e: {pending_label}; refusing to spawn another mounted hotspot worker"
        );
        return Err(namespace_collision_operation_timeout(
            timeout,
            vec![(1, pending_label)],
        ));
    };
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name("studiofs-e2e-hotspot-io".to_string())
        .spawn(move || {
            let result = f();
            drop(permit);
            let _ = tx.send(result);
        })?;
    match rx.recv_timeout(timeout) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into()),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            eprintln!(
                "studiofs-e2e: mounted hotspot operation timed out after {} ms; worker thread may remain blocked until process exit: {}",
                timeout.as_millis(),
                pending_label
            );
            Err(namespace_collision_operation_timeout(
                timeout,
                vec![(1, pending_label)],
            ))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(ScenarioError::Io(io::Error::other(
            "hotspot operation worker channel closed before completion",
        ))),
    }
}

pub(in crate::scenario) struct HotspotIoWorkerPermit {
    pub(in crate::scenario) active_workers: Arc<AtomicUsize>,
}

impl HotspotIoWorkerPermit {
    fn try_acquire(active_workers: Arc<AtomicUsize>, max_active_workers: usize) -> Option<Self> {
        let mut current = active_workers.load(Ordering::Acquire);
        loop {
            if current >= max_active_workers {
                return None;
            }
            match active_workers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self { active_workers }),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for HotspotIoWorkerPermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(in crate::scenario) fn run_lww_reconnect_operations(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<(Vec<NamespaceCollisionOperation>, LwwReconnectOracle), ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let path = isolation.path("winner.txt");
    let client_a_payload = isolation.payload("write_client_a", &path, "version from client-a\n");
    operations.push(required_record(fs_op(
        started,
        1,
        &clients[0],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[0].mount_point.join(&path),
                &client_a_payload,
                "lww reconnect client-a write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        2,
        &clients[2],
        "stale_writer_offline",
        clients[2].name.as_str(),
        controller
            .force_kill(&clients[2].name)
            .map_err(ScenarioError::from),
    )?);
    let client_b_payload = isolation.payload("write_client_b", &path, "version from client-b\n");
    operations.push(required_record(fs_op(
        started,
        3,
        &clients[1],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[1].mount_point.join(&path),
                &client_b_payload,
                "lww reconnect client-b write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    let client_d_payload = isolation.payload("write_client_d", &path, "version from client-d\n");
    operations.push(required_record(fs_op(
        started,
        4,
        &clients[3],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[3].mount_point.join(&path),
                &client_d_payload,
                "lww reconnect client-d write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        5,
        &clients[2],
        "stale_writer_reconnect",
        clients[2].name.as_str(),
        controller
            .remount(&clients[2].name)
            .map_err(ScenarioError::from),
    )?);
    let winner = isolation.payload(
        "write_lww_winner",
        &path,
        "explicit lww winner from reconnected client-c\n",
    );
    operations.push(required_record(fs_op(
        started,
        6,
        &clients[2],
        "write_lww_winner",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[2].mount_point.join(&path),
                &winner,
                "lww reconnect winner write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        7,
        &clients[2],
        "winner_crash_restart",
        clients[2].name.as_str(),
        controller
            .force_kill(&clients[2].name)
            .and_then(|()| controller.remount(&clients[2].name))
            .map_err(ScenarioError::from),
    )?);
    let actual = wait_for_mounted_payload_readback(
        &clients[2].mount_point.join(&path),
        &winner,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(scenario_error_to_io)?;
    let oracle = LwwReconnectOracle {
        expected_payload_sha256: bytes_sha256(&winner),
        actual_payload_sha256: bytes_sha256(&actual),
        matched: actual == winner,
    };
    write_lww_oracle(context, &oracle)?;
    Ok((operations, oracle))
}

pub(in crate::scenario) fn run_open_handle_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    seed_open_handle_initial_tree(context, clients, isolation, OPEN_HANDLE_OPERATION_TIMEOUT)?;
    wait_for_open_handle_initial_tree(
        context,
        clients,
        isolation,
        initial_convergence_timeout,
        poll_interval,
    )?;
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let rename_dest = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_DEST);
    let append_payload = isolation.payload(
        "append_old_handle",
        &unlink_target,
        "append through old handle\n",
    );
    run_open_handle_helper_process(
        context,
        OPEN_HANDLE_OPERATION_TIMEOUT,
        &clients[0],
        &clients[1],
        &unlink_target,
        &rename_source,
        &rename_dest,
        &append_payload,
    )?;
    operations.push(record_advanced_operation(
        started,
        1,
        &clients[0],
        "open",
        &unlink_target,
        Ok(()),
    ));
    operations.push(record_advanced_operation(
        started,
        2,
        &clients[1],
        "unlink",
        &unlink_target,
        Ok(()),
    ));
    operations.push(record_advanced_operation(
        started,
        3,
        &clients[1],
        "rename",
        &rename_source,
        Ok(()),
    ));
    operations.push(record_advanced_operation(
        started,
        4,
        &clients[0],
        "append_old_handle",
        &unlink_target,
        Ok(()),
    ));
    Ok(operations)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_open_handle_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client_a: &ClientSpec,
    client_b: &ClientSpec,
    unlink_path: &str,
    rename_source: &str,
    rename_dest: &str,
    append_payload: &[u8],
) -> Result<(), ScenarioError> {
    let payload_path =
        write_helper_payload(context, client_a, "open-handle", Some(append_payload))?
            .ok_or_else(|| ScenarioError::Io(io::Error::other("missing open handle payload")))?;
    let progress_path = helper_progress_path(context, client_a, "open-handle", unlink_path)?;
    let pending_label = format!("{} open-handle {}", client_a.name, unlink_path);

    #[cfg(test)]
    {
        let _ = timeout;
        run_open_handle_op_helper(
            client_a.mount_point.clone(),
            client_b.mount_point.clone(),
            unlink_path,
            rename_source,
            rename_dest,
            &payload_path,
            Some(&progress_path),
        )
        .map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "open handle helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("open-handle-op")
            .arg("--sandbox")
            .arg(&context.sandbox)
            .arg("--client-a-mount")
            .arg(&client_a.mount_point)
            .arg("--client-b-mount")
            .arg(&client_b.mount_point)
            .arg("--unlink-path")
            .arg(unlink_path)
            .arg("--rename-source")
            .arg(rename_source)
            .arg("--rename-dest")
            .arg(rename_dest)
            .arg("--append-payload")
            .arg(&payload_path)
            .arg("--progress")
            .arg(&progress_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        write_hotspot_helper_process_metadata(
            context,
            child.id(),
            client_a,
            "open-handle",
            unlink_path,
            Some(rename_dest),
        )?;
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate open handle helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                let stderr = child
                    .stderr
                    .take()
                    .and_then(|mut stderr| {
                        let mut output = String::new();
                        stderr.read_to_string(&mut output).ok().map(|_| output)
                    })
                    .unwrap_or_default();
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "open handle helper failed for {pending_label}: status={status}; stderr={}; progress={}",
                    stderr.trim(),
                    progress.as_deref().unwrap_or("<none>")
                ))));
            }
            if Instant::now() >= deadline {
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                eprintln!(
                    "studiofs-e2e: open handle helper timed out after {} ms; terminating helper process: {}{}",
                    timeout.as_millis(),
                    pending_label,
                    progress_suffix
                );
                let mut cleanup_error =
                    terminate_blocked_hotspot_client_process(client_a, &pending_label)
                        .err()
                        .map(|error| error.to_string());
                let _ = job.terminate();
                if should_wait_for_timed_out_hotspot_helper_child() {
                    if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                        let _ = job.force_kill();
                        if let Err(error) =
                            terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                        {
                            cleanup_error.get_or_insert_with(|| error.to_string());
                        }
                        let _ = child.kill();
                        let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
                    }
                } else {
                    let _ = job.force_kill();
                    if let Err(error) =
                        terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                    {
                        cleanup_error.get_or_insert_with(|| error.to_string());
                    }
                    let _ = child.kill();
                }
                if let Err(error) =
                    terminate_blocked_hotspot_client_process(client_a, &pending_label)
                {
                    cleanup_error.get_or_insert_with(|| error.to_string());
                }
                let primary = namespace_collision_operation_timeout(
                    timeout,
                    vec![(1, format!("{pending_label}{progress_suffix}"))],
                );
                if let Some(cleanup) = cleanup_error {
                    return Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                        primary: primary.to_string(),
                        cleanup,
                    });
                }
                return Err(primary);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_sparse_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    offset: Option<u64>,
    len: Option<u64>,
    payload: Option<&[u8]>,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;
    let pending_label = format!("{} {} {}", client.name, operation, path);

    #[cfg(test)]
    {
        let _ = timeout;
        run_sparse_op_helper(
            operation,
            client.mount_point.clone(),
            path,
            offset,
            len,
            payload_path.as_deref(),
            Some(&progress_path),
        )
        .map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "sparse helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("sparse-op")
            .arg("--operation")
            .arg(operation)
            .arg("--sandbox")
            .arg(&context.sandbox)
            .arg("--mount-point")
            .arg(&client.mount_point)
            .arg("--path")
            .arg(path)
            .arg("--progress")
            .arg(&progress_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(offset) = offset {
            command.arg("--offset").arg(offset.to_string());
        }
        if let Some(len) = len {
            command.arg("--len").arg(len.to_string());
        }
        if let Some(payload_path) = &payload_path {
            command.arg("--payload").arg(payload_path);
        }
        command
            .arg("--timeout-ms")
            .arg(duration_millis_u64(hotspot_helper_internal_timeout(timeout)).to_string());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        write_hotspot_helper_process_metadata(context, child.id(), client, operation, path, None)?;
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate sparse helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                let stderr = child
                    .stderr
                    .take()
                    .and_then(|mut stderr| {
                        let mut output = String::new();
                        stderr.read_to_string(&mut output).ok().map(|_| output)
                    })
                    .unwrap_or_default();
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "sparse helper failed for {pending_label}: status={status}; stderr={}; progress={}",
                    stderr.trim(),
                    progress.as_deref().unwrap_or("<none>")
                ))));
            }
            if Instant::now() >= deadline {
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                eprintln!(
                    "studiofs-e2e: sparse helper timed out after {} ms; terminating helper process: {}{}",
                    timeout.as_millis(),
                    pending_label,
                    progress_suffix
                );
                let mut cleanup_error =
                    terminate_blocked_hotspot_client_process(client, &pending_label)
                        .err()
                        .map(|error| error.to_string());
                let _ = job.terminate();
                if should_wait_for_timed_out_hotspot_helper_child() {
                    if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                        let _ = job.force_kill();
                        if let Err(error) =
                            terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                        {
                            cleanup_error.get_or_insert_with(|| error.to_string());
                        }
                        let _ = child.kill();
                        let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
                    }
                } else {
                    let _ = job.force_kill();
                    if let Err(error) =
                        terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                    {
                        cleanup_error.get_or_insert_with(|| error.to_string());
                    }
                    let _ = child.kill();
                }
                if let Err(error) = terminate_blocked_hotspot_client_process(client, &pending_label)
                {
                    cleanup_error.get_or_insert_with(|| error.to_string());
                }
                let primary = namespace_collision_operation_timeout(
                    timeout,
                    vec![(1, format!("{pending_label}{progress_suffix}"))],
                );
                if let Some(cleanup) = cleanup_error {
                    return Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                        primary: primary.to_string(),
                        cleanup,
                    });
                }
                return Err(primary);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

pub(in crate::scenario) fn seed_open_handle_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    operation_timeout: Duration,
) -> Result<(), ScenarioError> {
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let unlink_payload = isolation.payload(
        "initial_unlink_target",
        &unlink_target,
        &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
    );
    let rename_payload = isolation.payload(
        "initial_rename_source",
        &rename_source,
        &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
    );
    let active_workers = Arc::new(AtomicUsize::new(0));
    run_hotspot_mount_operation_with_timeout(
        context,
        &clients[0],
        "create",
        &unlink_target,
        operation_timeout,
        Some(&unlink_payload),
        None,
        Arc::clone(&active_workers),
    )
    .map_err(|error| ScenarioError::Io(io::Error::other(format!("open handle seed: {error}"))))?;
    run_hotspot_mount_operation_with_timeout(
        context,
        &clients[0],
        "create",
        &rename_source,
        operation_timeout,
        Some(&rename_payload),
        None,
        Arc::clone(&active_workers),
    )
    .map_err(|error| ScenarioError::Io(io::Error::other(format!("open handle seed: {error}"))))?;
    Ok(())
}

pub(in crate::scenario) fn wait_for_open_handle_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let started = Instant::now();
    let mut attempts = 0;
    let required_payloads = open_handle_initial_payloads(isolation);
    loop {
        attempts += 1;
        let missing = open_handle_initial_state_missing(context, clients, &required_payloads)?;
        if missing.is_empty() {
            return Ok(open_handle_initial_tree_outcome(clients, started, attempts));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "open handle initial tree did not become visible before timeout; missing: {}",
                    missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn open_handle_initial_payloads(
    isolation: &ScenarioIsolation,
) -> Vec<(String, Vec<u8>)> {
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    vec![
        (
            unlink_target.clone(),
            isolation.payload(
                "initial_unlink_target",
                &unlink_target,
                &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
            ),
        ),
        (
            rename_source.clone(),
            isolation.payload(
                "initial_rename_source",
                &rename_source,
                &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
            ),
        ),
    ]
}

pub(in crate::scenario) fn open_handle_initial_state_missing(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    required_payloads: &[(String, Vec<u8>)],
) -> Result<Vec<String>, ScenarioError> {
    let mut missing = Vec::new();
    for client in clients {
        for (relative_path, payload) in required_payloads {
            match run_hotspot_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                client,
                "read-exact",
                relative_path,
                None,
                Some(payload),
                format!(
                    "{} validate open handle initial {}",
                    client.name, relative_path
                ),
            ) {
                Ok(()) => {}
                Err(error) if hotspot_helper_error_is_fatal(&error) => return Err(error),
                Err(error) => {
                    missing.push(format!("{}:{} ({})", client.name, relative_path, error))
                }
            }
        }
    }
    Ok(missing)
}

pub(in crate::scenario) fn open_handle_initial_tree_outcome(
    clients: &[ClientSpec],
    started: Instant,
    attempts: usize,
) -> ConvergenceOutcome {
    let compared_clients = clients
        .iter()
        .map(|client| client.name.clone())
        .collect::<Vec<_>>();
    ConvergenceOutcome {
        attempts,
        elapsed_ms: started.elapsed().as_millis(),
        snapshots: Vec::new(),
        comparison: TreeComparison {
            converged: true,
            reference_client: compared_clients.first().cloned(),
            compared_clients,
            differences: Vec::new(),
        },
    }
}

pub(in crate::scenario) fn open_handle_isolated_path(
    isolation: &ScenarioIsolation,
    legacy_path: &str,
) -> String {
    isolation.path(
        legacy_path
            .strip_prefix("open-handles/")
            .unwrap_or(legacy_path),
    )
}

pub(in crate::scenario) fn wait_for_mounted_initial_payloads_until(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let last_missing =
            missing_mounted_initial_payloads(clients, required_payloads, &probe_limiter);
        if last_missing.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn wait_for_mounted_paths_absent_until(
    clients: &[ClientSpec],
    absent_paths: &[&str],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let present = present_mounted_paths(clients, absent_paths, &probe_limiter);
        if present.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; still present: {}",
                    present.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not disappear before timeout; still present: {}",
                    present.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn present_mounted_paths(
    clients: &[ClientSpec],
    absent_paths: &[&str],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    let mut present = Vec::new();
    for client in clients {
        for relative_path in absent_paths {
            let client = client.clone();
            let relative_path = (*relative_path).to_string();
            let client_name = client.name.clone();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(
                probe_limiter,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || present_mounted_path_probe(client, relative_path),
            ) {
                Ok(Some(present_path)) => present.push(present_path),
                Ok(None) => {}
                Err(error) => present.push(format!(
                    "{}:{} ({})",
                    client_name,
                    timeout_path,
                    error.detail()
                )),
            }
        }
    }
    present
}

pub(in crate::scenario) fn present_mounted_path_probe(
    client: ClientSpec,
    relative_path: String,
) -> Option<String> {
    let _ = refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path);
    match fs::metadata(client.mount_point.join(&relative_path)) {
        Ok(_) => Some(format!("{}:{relative_path}", client.name)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => Some(format!("{}:{relative_path} ({error})", client.name)),
    }
}

pub(in crate::scenario) fn missing_mounted_initial_payloads(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    missing_mounted_initial_payloads_with_probe(
        clients,
        required_payloads,
        probe_limiter,
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
        mounted_initial_payload_probe,
    )
}

pub(in crate::scenario) fn missing_mounted_initial_payloads_with_probe<F>(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    probe_limiter: &MountedProbeLimiter,
    probe_timeout: Duration,
    probe: F,
) -> Vec<String>
where
    F: Fn(ClientSpec, String, Vec<u8>) -> MountedPayloadProbeStatus + Copy + Send + 'static,
{
    let mut missing = Vec::new();
    for client in clients {
        for (relative_path, expected_payload) in required_payloads {
            let client = client.clone();
            let client_name = client.name.clone();
            let relative_path = (*relative_path).to_string();
            let expected_payload = expected_payload.to_vec();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(probe_limiter, probe_timeout, move || {
                probe(client, relative_path, expected_payload)
            }) {
                Ok(MountedPayloadProbeStatus::Present) => {}
                Ok(MountedPayloadProbeStatus::Missing(path)) => missing.push(path),
                Err(error) => {
                    missing.push(format!("{client_name}:{timeout_path} ({})", error.detail()));
                }
            }
        }
    }
    missing
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::scenario) enum MountedPayloadProbeStatus {
    Present,
    Missing(String),
}

pub(in crate::scenario) fn mounted_initial_payload_probe(
    client: ClientSpec,
    relative_path: String,
    expected_payload: Vec<u8>,
) -> MountedPayloadProbeStatus {
    let path = client.mount_point.join(&relative_path);
    let parent_contains_child =
        refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path);
    if !parent_contains_child {
        let metadata_detail = match fs::metadata(&path) {
            Ok(metadata) => format!("metadata_len={}", metadata.len()),
            Err(error) => format!("metadata_error={error}"),
        };
        return MountedPayloadProbeStatus::Missing(format!(
            "{}:{relative_path} (parent_contains_child=false; {metadata_detail})",
            client.name
        ));
    }
    match fs::read(&path) {
        Ok(payload) if payload == expected_payload => MountedPayloadProbeStatus::Present,
        Ok(payload) => MountedPayloadProbeStatus::Missing(format!(
            "{}:{relative_path} (parent_contains_child=true; expected {} bytes sha256={}, read {} bytes sha256={})",
            client.name,
            expected_payload.len(),
            bytes_sha256(&expected_payload),
            payload.len(),
            bytes_sha256(&payload)
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let metadata_detail = match fs::metadata(&path) {
                Ok(metadata) => format!("metadata_len={}", metadata.len()),
                Err(metadata_error) => format!("metadata_error={metadata_error}"),
            };
            MountedPayloadProbeStatus::Missing(format!(
                "{}:{relative_path} (parent_contains_child=true; read_error={error}; {metadata_detail})",
                client.name
            ))
        }
        Err(error) => {
            let metadata_detail = match fs::metadata(&path) {
                Ok(metadata) => format!("metadata_len={}", metadata.len()),
                Err(metadata_error) => format!("metadata_error={metadata_error}"),
            };
            MountedPayloadProbeStatus::Missing(format!(
                "{}:{relative_path} (parent_contains_child=true; read_error={error}; {metadata_detail})",
                client.name
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::scenario) enum MountedProbeFailure {
    TimedOut {
        timeout: Duration,
    },
    WorkerExited,
    WorkerLimitReached {
        active_workers: usize,
        max_active_workers: usize,
    },
}

impl MountedProbeFailure {
    fn detail(self) -> String {
        match self {
            Self::TimedOut { timeout } => {
                format!("probe timed out after {} ms", timeout.as_millis())
            }
            Self::WorkerExited => "probe worker exited without result".to_string(),
            Self::WorkerLimitReached {
                active_workers,
                max_active_workers,
            } => format!(
                "probe skipped because a previous mounted probe timed out; active probe workers {active_workers}/{max_active_workers}"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::scenario) struct MountedProbeLimiter {
    pub(in crate::scenario) active_workers: Arc<AtomicUsize>,
    pub(in crate::scenario) poisoned: Arc<AtomicBool>,
    pub(in crate::scenario) max_active_workers: usize,
}

impl MountedProbeLimiter {
    pub(in crate::scenario) fn new(max_active_workers: usize) -> Self {
        Self {
            active_workers: Arc::new(AtomicUsize::new(0)),
            poisoned: Arc::new(AtomicBool::new(false)),
            max_active_workers,
        }
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn mark_poisoned(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn try_acquire(&self) -> Result<MountedProbePermit, MountedProbeFailure> {
        if self.is_poisoned() {
            return Err(self.worker_limit_reached());
        }

        loop {
            let active_workers = self.active_workers.load(Ordering::Acquire);
            if active_workers >= self.max_active_workers {
                return Err(self.worker_limit_reached());
            }
            if self
                .active_workers
                .compare_exchange(
                    active_workers,
                    active_workers.saturating_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(MountedProbePermit {
                    active_workers: Arc::clone(&self.active_workers),
                });
            }
        }
    }

    fn worker_limit_reached(&self) -> MountedProbeFailure {
        MountedProbeFailure::WorkerLimitReached {
            active_workers: self.active_workers.load(Ordering::Acquire),
            max_active_workers: self.max_active_workers,
        }
    }
}

pub(in crate::scenario) struct MountedProbePermit {
    pub(in crate::scenario) active_workers: Arc<AtomicUsize>,
}

impl Drop for MountedProbePermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(in crate::scenario) fn run_mounted_probe_with_timeout<T, F>(
    probe_limiter: &MountedProbeLimiter,
    timeout: Duration,
    probe: F,
) -> Result<T, MountedProbeFailure>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = probe_limiter.try_acquire()?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = probe();
        drop(permit);
        let _ = tx.send(result);
    });
    match rx.recv_timeout(timeout) {
        Ok(result) => Ok(result),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            probe_limiter.mark_poisoned();
            Err(MountedProbeFailure::TimedOut { timeout })
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(MountedProbeFailure::WorkerExited),
    }
}

pub(in crate::scenario) fn refresh_relative_path_parent_contains_child(
    mount_point: &Path,
    relative_path: &str,
) -> bool {
    let relative = Path::new(relative_path);
    let Some(file_name) = relative.file_name() else {
        return true;
    };
    let mut current = mount_point.to_path_buf();
    let _ = drain_directory(&current);
    let Some(parent) = relative.parent() else {
        return directory_contains_child(&current, file_name);
    };
    let mut components = parent.components().peekable();
    while let Some(component) = components.next() {
        current.push(component.as_os_str());
        if components.peek().is_some() {
            if !drain_directory(&current) {
                return false;
            }
        } else {
            return directory_contains_child(&current, file_name);
        }
    }
    directory_contains_child(&current, file_name)
}

pub(in crate::scenario) fn directory_contains_child(
    parent: &Path,
    file_name: &std::ffi::OsStr,
) -> bool {
    let Ok(entries) = bounded_mounted_read_dir_names(
        parent.to_path_buf(),
        format!("directory contains child {}", parent.display()),
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
    ) else {
        return false;
    };
    for entry_name in entries {
        if entry_name == file_name {
            return true;
        }
    }
    false
}

pub(in crate::scenario) fn drain_directory(path: &Path) -> bool {
    bounded_mounted_read_dir_names(
        path.to_path_buf(),
        format!("drain directory {}", path.display()),
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
    )
    .is_ok()
}

pub(in crate::scenario) fn wait_for_required_paths_visible_until(
    clients: &[ClientSpec],
    required_paths: &[&str],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let last_missing = missing_required_paths(clients, required_paths, &probe_limiter);
        if last_missing.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn wait_for_client_path_visible_until(
    client: &ClientSpec,
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(1);
    loop {
        match client_path_missing(client, relative_path, &probe_limiter) {
            Ok(None) => return Ok(()),
            Ok(Some(missing)) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(ScenarioError::Io(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "{label} did not become visible before timeout; missing: {missing}"
                        ),
                    )));
                }
                thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
            }
            Err(error) => {
                return Err(ScenarioError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{label} mounted validation probe timed out for {}:{relative_path}: {}",
                        client.name,
                        error.detail()
                    ),
                )));
            }
        }
    }
}

pub(in crate::scenario) fn wait_for_hotspot_helper_stat_until(
    context: &ScenarioContext,
    client: &ClientSpec,
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
    active_workers: Arc<AtomicUsize>,
) -> Result<(), ScenarioError> {
    let mut last_error = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout for {}:{relative_path}; last error: {}",
                    client.name,
                    last_error.unwrap_or_else(|| "none".to_string())
                ),
            )));
        }

        let remaining = deadline.saturating_duration_since(now);
        match run_hotspot_mount_operation_with_timeout(
            context,
            client,
            "stat",
            relative_path,
            remaining,
            None,
            None,
            Arc::clone(&active_workers),
        ) {
            Ok(()) => return Ok(()),
            Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
                return Err(error);
            }
            Err(error) => {
                last_error = Some(error.to_string());
                thread::sleep(
                    poll_interval.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    }
}

pub(in crate::scenario) fn remaining_initial_validation_timeout(
    deadline: Instant,
    label: &str,
) -> Result<Duration, ScenarioError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{label} became visible, but no time remained for convergence validation"),
        )))
    } else {
        Ok(remaining)
    }
}

pub(in crate::scenario) fn missing_required_paths(
    clients: &[ClientSpec],
    required_paths: &[&str],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    let mut missing = Vec::new();
    for client in clients {
        for relative_path in required_paths {
            let client = client.clone();
            let relative_path = (*relative_path).to_string();
            let client_name = client.name.clone();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(
                probe_limiter,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || missing_required_path_probe(client, relative_path),
            ) {
                Ok(Some(missing_path)) => missing.push(missing_path),
                Ok(None) => {}
                Err(error) => missing.push(format!(
                    "{}:{} ({})",
                    client_name,
                    timeout_path,
                    error.detail()
                )),
            }
        }
    }
    missing
}

pub(in crate::scenario) fn client_path_missing(
    client: &ClientSpec,
    relative_path: &str,
    probe_limiter: &MountedProbeLimiter,
) -> Result<Option<String>, MountedProbeFailure> {
    let client = client.clone();
    let relative_path = relative_path.to_string();
    run_mounted_probe_with_timeout(probe_limiter, MOUNTED_VALIDATION_PROBE_TIMEOUT, move || {
        missing_required_path_probe(client, relative_path)
    })
}

pub(in crate::scenario) fn missing_required_path_probe(
    client: ClientSpec,
    relative_path: String,
) -> Option<String> {
    if !refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path) {
        return Some(format!("{}:{relative_path}", client.name));
    }
    let path = client.mount_point.join(&relative_path);
    match fs::metadata(&path) {
        Ok(_) => None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Some(format!("{}:{relative_path}", client.name))
        }
        Err(error) => Some(format!("{}:{relative_path} ({error})", client.name)),
    }
}

pub(in crate::scenario) struct AdvancedMountedFinishInput {
    pub(in crate::scenario) bootstrap: Option<VolumeBootstrapSummary>,
    pub(in crate::scenario) operations: Vec<NamespaceCollisionOperation>,
    pub(in crate::scenario) lww_oracle: Option<LwwReconnectOracle>,
    pub(in crate::scenario) sparse_ranges: Option<SparseRangeSummary>,
    pub(in crate::scenario) cold_observer: Option<ColdObserverSummary>,
    pub(in crate::scenario) long_history: Option<LongHistorySummary>,
    pub(in crate::scenario) catchup: Option<CatchupSummary>,
    pub(in crate::scenario) invalidation_dropout: Option<InvalidationDropoutSummary>,
    pub(in crate::scenario) symlink_attributes: Option<SymlinkAttributeSummary>,
}

pub(in crate::scenario) fn finish_advanced_mounted(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    isolation: Option<&ScenarioIsolation>,
    clients: &[ClientSpec],
    input: AdvancedMountedFinishInput,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let AdvancedMountedFinishInput {
        bootstrap,
        operations,
        lww_oracle,
        sparse_ranges,
        cold_observer,
        long_history,
        catchup,
        invalidation_dropout,
        symlink_attributes,
    } = input;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        kind.convergence_timeout(),
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
    )?;
    let expected_absent_paths = expected_absent_paths_for(kind, isolation);
    let expected_absent_paths = expected_absent_paths
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(
            snapshot,
            &expected_absent_paths,
        ));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::NamespaceInvariantViolations {
            violations: namespace_violations,
        });
    }
    let summary = AdvancedMountedSummary {
        scenario: kind.name().to_string(),
        bootstrap,
        clients: clients.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: convergence.comparison,
        namespace_violations,
        lww_oracle,
        sparse_ranges,
        cold_observer,
        long_history_validation: long_history
            .as_ref()
            .map(|history| validate_long_history_snapshots(&convergence.snapshots, history)),
        long_history,
        catchup,
        invalidation_dropout,
        symlink_attributes,
    };
    write_advanced_mounted_summary(context, kind, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn expected_absent_paths_for(
    kind: AdvancedMountedKind,
    isolation: Option<&ScenarioIsolation>,
) -> Vec<String> {
    match (kind, isolation) {
        (AdvancedMountedKind::OpenHandle, Some(isolation)) => {
            vec![open_handle_isolated_path(
                isolation,
                OPEN_HANDLE_UNLINK_TARGET,
            )]
        }
        (AdvancedMountedKind::OpenHandle, None) => vec![OPEN_HANDLE_UNLINK_TARGET.to_string()],
        (AdvancedMountedKind::ColdObserverLongHistory, Some(isolation)) => vec![
            isolation.path(COLD_OBSERVER_DELETE_TARGET),
            isolation.path(COLD_OBSERVER_RENAME_SOURCE),
        ],
        (AdvancedMountedKind::ColdObserverLongHistory, None) => vec![
            format!(
                "{}/{}",
                AdvancedMountedKind::ColdObserverLongHistory.path_root(),
                COLD_OBSERVER_DELETE_TARGET
            ),
            format!(
                "{}/{}",
                AdvancedMountedKind::ColdObserverLongHistory.path_root(),
                COLD_OBSERVER_RENAME_SOURCE
            ),
        ],
        (AdvancedMountedKind::StaleRemountColdDiscoverCatchup, Some(isolation)) => vec![
            isolation.path(COLD_OBSERVER_DELETE_TARGET),
            isolation.path(COLD_OBSERVER_RENAME_SOURCE),
        ],
        (AdvancedMountedKind::StaleRemountColdDiscoverCatchup, None) => vec![
            format!(
                "{}/{}",
                AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root(),
                COLD_OBSERVER_DELETE_TARGET
            ),
            format!(
                "{}/{}",
                AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root(),
                COLD_OBSERVER_RENAME_SOURCE
            ),
        ],
        (AdvancedMountedKind::InvalidationDropoutFallback, Some(isolation)) => {
            vec![isolation.path(INVALIDATION_DROPOUT_DELETE_TARGET)]
        }
        (AdvancedMountedKind::InvalidationDropoutFallback, None) => vec![format!(
            "{}/{}",
            AdvancedMountedKind::InvalidationDropoutFallback.path_root(),
            INVALIDATION_DROPOUT_DELETE_TARGET
        )],
        _ => Vec::new(),
    }
}

pub(in crate::scenario) fn validate_long_history_snapshots(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
    history: &LongHistorySummary,
) -> LongHistoryValidation {
    let mut missing_present_paths = Vec::new();
    let mut unexpected_present_paths = Vec::new();
    let mut checksum_mismatches = Vec::new();
    let checksum_path = long_history_checksum_path(history);

    for snapshot in snapshots {
        for expected_path in &history.expected_present_paths {
            let Some(entry) = snapshot
                .entries
                .iter()
                .find(|entry| entry.path == *expected_path)
            else {
                missing_present_paths.push(format!("{}:{expected_path}", snapshot.client));
                continue;
            };
            if checksum_path.as_deref() == Some(expected_path.as_str())
                && entry.checksum_sha256.as_deref() != Some(history.final_checksum_sha256.as_str())
            {
                checksum_mismatches.push(format!(
                    "{}:{expected_path}: expected {}, got {}",
                    snapshot.client,
                    history.final_checksum_sha256,
                    entry.checksum_sha256.as_deref().unwrap_or("<missing>")
                ));
            }
        }
        for absent_path in &history.expected_absent_paths {
            if snapshot
                .entries
                .iter()
                .any(|entry| entry.path == *absent_path)
            {
                unexpected_present_paths.push(format!("{}:{absent_path}", snapshot.client));
            }
        }
    }

    LongHistoryValidation {
        matched: missing_present_paths.is_empty()
            && unexpected_present_paths.is_empty()
            && checksum_mismatches.is_empty(),
        missing_present_paths,
        unexpected_present_paths,
        checksum_mismatches,
    }
}

pub(in crate::scenario) fn long_history_checksum_path(
    history: &LongHistorySummary,
) -> Option<String> {
    history
        .expected_present_paths
        .iter()
        .find(|path| path.ends_with(COLD_OBSERVER_OVERWRITE_TARGET))
        .cloned()
        .or_else(|| history.expected_present_paths.first().cloned())
}

pub(in crate::scenario) fn planned_advanced_mounted_operations(
    kind: AdvancedMountedKind,
    started: Instant,
) -> Vec<NamespaceCollisionOperation> {
    let spec = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => vec![(
            "client-a",
            "before_write",
            "crash-before-write-acceptance/before-write.txt",
        )],
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => vec![(
            "client-a",
            "partial_write",
            "crash-after-partial-write-acceptance/partial-write.txt",
        )],
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => vec![(
            "client-a",
            "full_write",
            "crash-after-full-write-acceptance/full-write.txt",
        )],
        AdvancedMountedKind::MountedChaos => vec![
            ("client-a", "rename", "chaos/rename-source.txt"),
            ("client-b", "unlink", "chaos/unlink-target.txt"),
            ("client-c", "append", "chaos/append-target.txt"),
        ],
        AdvancedMountedKind::HotspotChurn => vec![
            ("client-a", "overwrite", "hotspot/slot-0.txt"),
            ("client-b", "append", "hotspot/slot-0.txt"),
            ("client-c", "readdir", "hotspot"),
        ],
        AdvancedMountedKind::FaultChaos => vec![
            ("client-a", "stop_postgres", "compose/postgres"),
            (
                "client-a",
                "local_write_during_pg_outage",
                "fault-chaos/postgres-outage.txt",
            ),
            ("client-b", "stop_rustfs", "compose/rustfs"),
            ("client-d", "cold_observer_readdir", "fault-chaos"),
        ],
        AdvancedMountedKind::LwwReconnectChaos => vec![
            ("client-c", "stale_writer_offline", "client-c"),
            ("client-b", "write", "lww-chaos/winner.txt"),
            ("client-c", "write_lww_winner", "lww-chaos/winner.txt"),
            ("client-e", "late_observer_read", "lww-chaos/winner.txt"),
        ],
        AdvancedMountedKind::OpenHandle => vec![
            ("client-a", "open", "open-handles/unlink-target.txt"),
            ("client-b", "unlink", "open-handles/unlink-target.txt"),
            ("client-b", "rename", "open-handles/rename-source.txt"),
            (
                "client-a",
                "append_old_handle",
                "open-handles/unlink-target.txt",
            ),
        ],
        AdvancedMountedKind::SparseRange => vec![
            ("client-a", "write_range", "sparse/shared.bin@0"),
            ("client-a", "write_range", "sparse/shared.bin@1048569"),
            ("client-b", "read_probe", "sparse/shared.bin@65536"),
            ("client-b", "truncate_shrink", "sparse/shared.bin@1048833"),
            ("client-c", "truncate_extend", "sparse/shared.bin@3146061+"),
            ("client-c", "write_range", "sparse/shared.bin@2097147"),
            ("client-a", "read_probe", "sparse/shared.bin@3145900"),
        ],
        AdvancedMountedKind::ColdObserverLongHistory => vec![
            ("client-a", "mkdir", "cold-observer/nested"),
            ("client-a", "create", "cold-observer/delete-target.txt"),
            ("client-b", "delete", "cold-observer/delete-target.txt"),
            ("client-a", "create", "cold-observer/rename-source.txt"),
            (
                "client-b",
                "rename",
                "cold-observer/rename-source.txt -> cold-observer/renamed-final.txt",
            ),
            ("client-c", "create", "cold-observer/overwrite-target.txt"),
            (
                "client-a",
                "overwrite",
                "cold-observer/overwrite-target.txt",
            ),
            ("client-b", "append", "cold-observer/append-target.txt"),
            ("client-c", "create", "cold-observer/nested/child.txt"),
            ("client-d", "cold_discover_after_history", "client-d"),
            ("client-d", "cold_mount_after_history", "client-d"),
        ],
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup => vec![
            (
                "client-c",
                "initial_baseline",
                "stale-remount-cold-discover/baseline.txt",
            ),
            ("client-c", "stale_client_unmount", "client-c"),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/delete-target.txt",
            ),
            (
                "client-a",
                "delete",
                "stale-remount-cold-discover/delete-target.txt",
            ),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/rename-source.txt",
            ),
            (
                "client-b",
                "rename",
                "stale-remount-cold-discover/rename-source.txt -> stale-remount-cold-discover/renamed-final.txt",
            ),
            (
                "client-a",
                "overwrite",
                "stale-remount-cold-discover/overwrite-target.txt",
            ),
            (
                "client-b",
                "append",
                "stale-remount-cold-discover/append-target.txt",
            ),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/nested/child.txt",
            ),
            ("client-c", "stale_remount_after_history", "client-c"),
            ("client-d", "cold_discover_after_history", "client-d"),
        ],
        AdvancedMountedKind::InvalidationDropoutFallback => vec![
            ("client-c", "observer_dropout_start", "client-c"),
            (
                "client-a",
                "writer_create",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-b",
                "writer_overwrite",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-a",
                "writer_delete",
                "invalidation-dropout/delete-target.txt",
            ),
            (
                "client-c",
                "observer_stale_window",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-c",
                "observer_delete_stale_window",
                "invalidation-dropout/delete-target.txt",
            ),
            (
                "client-c",
                "fallback_sync_resume",
                "invalidation-dropout/fallback-target.txt",
            ),
        ],
        AdvancedMountedKind::SymlinkAttribute => vec![
            (
                "client-a",
                "symlink_target_write",
                "symlink-attribute/target-v1.txt",
            ),
            (
                "client-a",
                "symlink_create",
                "symlink-attribute/link-to-target -> target-v1.txt",
            ),
            (
                "client-b",
                "symlink_rename",
                "symlink-attribute/link-to-target -> symlink-attribute/renamed-link",
            ),
            (
                "client-b",
                "symlink_target_update",
                "symlink-attribute/renamed-link -> target-v2.txt",
            ),
            (
                "client-c",
                "readonly_attribute_set",
                "symlink-attribute/readonly-attribute.txt",
            ),
            (
                "client-a",
                "near_symlink_write",
                "symlink-attribute/near-symlink-write.txt",
            ),
        ],
    };
    spec.into_iter()
        .enumerate()
        .map(
            |(index, (client, operation, path))| NamespaceCollisionOperation {
                order: index + 1,
                client: client.to_string(),
                operation: operation.to_string(),
                path: path.to_string(),
                result: "planned".to_string(),
                timestamp_ms: elapsed_millis_u64(started),
                error: None,
            },
        )
        .collect()
}

pub(in crate::scenario) fn planned_symlink_attribute_summary() -> SymlinkAttributeSummary {
    SymlinkAttributeSummary {
        symlink_supported: true,
        symlink_skip_reason: None,
        readonly_attribute_supported: true,
        readonly_attribute_skip_reason: None,
        final_symlink_path: format!(
            "{}/{}",
            AdvancedMountedKind::SymlinkAttribute.path_root(),
            SYMLINK_ATTRIBUTE_RENAMED_LINK
        ),
        final_symlink_target: SYMLINK_ATTRIBUTE_TARGET_V2.to_string(),
        readonly_path: format!(
            "{}/{}",
            AdvancedMountedKind::SymlinkAttribute.path_root(),
            SYMLINK_ATTRIBUTE_READONLY_FILE
        ),
        readonly_observed_clients: SYMLINK_ATTRIBUTE_CLIENT_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

pub(in crate::scenario) struct SparseRangeSpec {
    pub(in crate::scenario) label: &'static str,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) payload: Vec<u8>,
}

pub(in crate::scenario) struct SparseHoleSpec {
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
}

pub(in crate::scenario) fn invalidation_dropout_observer_env_overrides() -> Vec<(String, String)> {
    vec![
        (
            "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL".to_string(),
            "1".to_string(),
        ),
        (
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS".to_string(),
            INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS.to_string(),
        ),
    ]
}

pub(in crate::scenario) fn run_invalidation_dropout_fallback_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, InvalidationDropoutSummary), ScenarioError> {
    let started = Instant::now();
    let writer_a = advanced_client_by_name(clients, "client-a")?;
    let writer_b = advanced_client_by_name(clients, "client-b")?;
    let observer = advanced_client_by_name(clients, INVALIDATION_DROPOUT_OBSERVER_CLIENT)?;
    let writer_clients = clients
        .iter()
        .filter(|client| INVALIDATION_DROPOUT_WRITER_CLIENTS.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut operations = Vec::new();

    let baseline_path = isolation.path(INVALIDATION_DROPOUT_BASELINE_FILE);
    let target_path = isolation.path(INVALIDATION_DROPOUT_TARGET_FILE);
    let delete_path = isolation.path(INVALIDATION_DROPOUT_DELETE_TARGET);
    let baseline_payload = isolation.payload("baseline", &baseline_path, "baseline visible");
    let create_payload = isolation.payload("writer_create", &target_path, "created before dropout");
    let final_payload = isolation.payload("writer_overwrite", &target_path, "fallback final");
    let delete_payload = isolation.payload("writer_delete", &delete_path, "delete before fallback");

    run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        writer_a,
        "create",
        &baseline_path,
        None,
        Some(&baseline_payload),
        format!(
            "{} invalidation dropout baseline {}",
            writer_a.name, baseline_path
        ),
    )?;
    run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        writer_a,
        "create",
        &delete_path,
        None,
        Some(&delete_payload),
        format!(
            "{} invalidation dropout delete baseline {}",
            writer_a.name, delete_path
        ),
    )?;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&baseline_path, baseline_payload.as_slice()),
            (&delete_path, delete_payload.as_slice()),
        ],
        Instant::now() + timeout,
        poll_interval,
        "invalidation dropout baseline",
    )?;

    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_dropout_start",
        INVALIDATION_DROPOUT_OBSERVER_CLIENT,
        Ok(()),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_a,
        "writer_create",
        &target_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer_a,
            "create",
            &target_path,
            None,
            Some(&create_payload),
            format!(
                "{} invalidation dropout writer create {}",
                writer_a.name, target_path
            ),
        ),
    )?);
    wait_for_mounted_initial_payloads_until(
        &writer_clients,
        &[(&target_path, create_payload.as_slice())],
        Instant::now() + timeout,
        poll_interval,
        "invalidation dropout writer create",
    )?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_b,
        "writer_overwrite",
        &target_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer_b,
            "overwrite-barrier",
            &target_path,
            None,
            Some(&final_payload),
            format!(
                "{} invalidation dropout writer overwrite {}",
                writer_b.name, target_path
            ),
        ),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_a,
        "writer_delete",
        &delete_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer_a,
            "unlink",
            &delete_path,
            None,
            None,
            format!(
                "{} invalidation dropout writer delete {}",
                writer_a.name, delete_path
            ),
        ),
    )?);

    let stale_window_observed = assert_observer_stays_stale_for_window(
        observer,
        &target_path,
        &final_payload,
        Duration::from_millis(INVALIDATION_DROPOUT_STALE_WINDOW_MS),
        poll_interval,
    )
    .is_ok();
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_stale_window",
        &target_path,
        stale_window_observed.then_some(()).ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "observer saw final payload before stale window elapsed",
            ))
        }),
    )?);
    let delete_stale_window_observed = assert_observer_keeps_payload_for_window(
        observer,
        &delete_path,
        &delete_payload,
        Duration::from_millis(INVALIDATION_DROPOUT_STALE_WINDOW_MS),
        poll_interval,
    )
    .is_ok();
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_delete_stale_window",
        &delete_path,
        delete_stale_window_observed.then_some(()).ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "observer lost deleted path before stale window elapsed",
            ))
        }),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "fallback_sync_resume",
        &target_path,
        wait_for_invalidation_fallback_resume(
            clients,
            &target_path,
            final_payload.as_slice(),
            &delete_path,
            timeout,
            poll_interval,
        ),
    )?);

    let summary = InvalidationDropoutSummary {
        observer_client: observer.name.clone(),
        writer_clients: INVALIDATION_DROPOUT_WRITER_CLIENTS
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        notify_suppressed: true,
        fallback_interval_ms: INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS,
        stale_window_ms: INVALIDATION_DROPOUT_STALE_WINDOW_MS,
        stale_window_observed,
        delete_stale_window_observed,
        fallback_resumed: true,
        final_observer_path: target_path,
        final_observer_checksum_sha256: bytes_sha256(&final_payload),
        final_validation_result: "mounted_tree_convergence".to_string(),
    };
    write_invalidation_dropout_diagnostics(context, &summary)?;
    Ok((operations, summary))
}

pub(in crate::scenario) fn advanced_client_by_name<'a>(
    clients: &'a [ClientSpec],
    name: &str,
) -> Result<&'a ClientSpec, ScenarioError> {
    clients
        .iter()
        .find(|client| client.name == name)
        .ok_or_else(|| ScenarioError::Io(io::Error::other(format!("missing client {name}"))))
}

pub(in crate::scenario) fn run_symlink_attribute_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, SymlinkAttributeSummary), ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let renamer = &clients[1];
    let attr_writer = &clients[2];
    let mut operations = Vec::new();

    let target_v1 = isolation.path(SYMLINK_ATTRIBUTE_TARGET_V1);
    let target_v2 = isolation.path(SYMLINK_ATTRIBUTE_TARGET_V2);
    let link = isolation.path(SYMLINK_ATTRIBUTE_LINK);
    let renamed_link = isolation.path(SYMLINK_ATTRIBUTE_RENAMED_LINK);
    let readonly_path = isolation.path(SYMLINK_ATTRIBUTE_READONLY_FILE);
    let nearby_path = isolation.path(SYMLINK_ATTRIBUTE_NEARBY_FILE);

    let target_v1_payload = isolation.payload("target_v1", &target_v1, "symlink target v1");
    let target_v2_payload = isolation.payload("target_v2", &target_v2, "symlink target v2");
    let readonly_payload = isolation.payload("readonly", &readonly_path, "readonly metadata attr");
    let nearby_payload = isolation.payload("nearby", &nearby_path, "write near symlink path");

    operations.push(required_op(
        started,
        operations.len() + 1,
        writer,
        "symlink_target_write",
        &target_v1,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer,
            "create",
            &target_v1,
            None,
            Some(&target_v1_payload),
            format!("{} symlink target v1 {}", writer.name, target_v1),
        ),
    )?);

    let link_result = run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        writer,
        "symlink-create",
        &link,
        Some(SYMLINK_ATTRIBUTE_TARGET_V1),
        None,
        format!(
            "{} symlink create {} -> {}",
            writer.name, link, SYMLINK_ATTRIBUTE_TARGET_V1
        ),
    );
    let (symlink_supported, symlink_skip_reason) = match link_result {
        Ok(()) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                writer,
                "symlink_create",
                &format!("{link} -> {}", SYMLINK_ATTRIBUTE_TARGET_V1),
                Ok(()),
            )?);
            wait_for_symlink_target_until(
                clients,
                &link,
                SYMLINK_ATTRIBUTE_TARGET_V1,
                Instant::now() + timeout,
                poll_interval,
            )?;
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_rename",
                &format!("{link} -> {renamed_link}"),
                run_hotspot_helper_process(
                    context,
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    renamer,
                    "rename",
                    &link,
                    Some(&renamed_link),
                    None,
                    format!(
                        "{} symlink rename {} -> {}",
                        renamer.name, link, renamed_link
                    ),
                ),
            )?);
            wait_for_symlink_target_until(
                clients,
                &renamed_link,
                SYMLINK_ATTRIBUTE_TARGET_V1,
                Instant::now() + timeout,
                poll_interval,
            )?;
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_target_replacement_write",
                &target_v2,
                run_hotspot_helper_process(
                    context,
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    renamer,
                    "create",
                    &target_v2,
                    None,
                    Some(&target_v2_payload),
                    format!("{} symlink target v2 {}", renamer.name, target_v2),
                ),
            )?);
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_target_update",
                &format!("{renamed_link} -> {}", SYMLINK_ATTRIBUTE_TARGET_V2),
                run_hotspot_helper_process(
                    context,
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    renamer,
                    "symlink-replace",
                    &renamed_link,
                    Some(SYMLINK_ATTRIBUTE_TARGET_V2),
                    None,
                    format!(
                        "{} symlink target update {} -> {}",
                        renamer.name, renamed_link, SYMLINK_ATTRIBUTE_TARGET_V2
                    ),
                ),
            )?);
            wait_for_symlink_target_until(
                clients,
                &renamed_link,
                SYMLINK_ATTRIBUTE_TARGET_V2,
                Instant::now() + timeout,
                poll_interval,
            )?;
            (true, None)
        }
        Err(error) if hotspot_helper_error_is_supported_symlink_skip(&error) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                writer,
                "symlink_create_skip",
                &link,
                Ok(()),
            )?);
            (false, Some(error.to_string()))
        }
        Err(error) => return Err(error),
    };

    operations.push(required_op(
        started,
        operations.len() + 1,
        attr_writer,
        "readonly_attribute_target_write",
        &readonly_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            attr_writer,
            "create",
            &readonly_path,
            None,
            Some(&readonly_payload),
            format!(
                "{} readonly attribute target {}",
                attr_writer.name, readonly_path
            ),
        ),
    )?);
    let readonly_result = run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        attr_writer,
        "set-readonly",
        &readonly_path,
        None,
        None,
        format!(
            "{} readonly attribute set {}",
            attr_writer.name, readonly_path
        ),
    );
    let (readonly_attribute_supported, readonly_attribute_skip_reason) = match readonly_result {
        Ok(()) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                attr_writer,
                "readonly_attribute_set",
                &readonly_path,
                Ok(()),
            )?);
            (true, None)
        }
        Err(error) if hotspot_helper_error_is_supported_symlink_skip(&error) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                attr_writer,
                "readonly_attribute_skip",
                &readonly_path,
                Ok(()),
            )?);
            (false, Some(error.to_string()))
        }
        Err(error) => return Err(error),
    };

    operations.push(required_op(
        started,
        operations.len() + 1,
        writer,
        "near_symlink_write",
        &nearby_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer,
            "create",
            &nearby_path,
            None,
            Some(&nearby_payload),
            format!("{} near symlink write {}", writer.name, nearby_path),
        ),
    )?);

    let readonly_observed_clients = if readonly_attribute_supported {
        wait_for_readonly_attribute_until(
            clients,
            &readonly_path,
            Instant::now() + timeout,
            poll_interval,
        )?
    } else {
        Vec::new()
    };

    Ok((
        operations,
        SymlinkAttributeSummary {
            symlink_supported,
            symlink_skip_reason,
            readonly_attribute_supported,
            readonly_attribute_skip_reason,
            final_symlink_path: renamed_link,
            final_symlink_target: SYMLINK_ATTRIBUTE_TARGET_V2.to_string(),
            readonly_path,
            readonly_observed_clients,
            final_validation_result: "mounted_tree_convergence".to_string(),
        },
    ))
}

pub(in crate::scenario) fn hotspot_helper_error_is_supported_symlink_skip(
    error: &ScenarioError,
) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("permission denied")
        || message.contains("unsupported")
        || message.contains("privilege")
        || message.contains("operation not permitted")
}

pub(in crate::scenario) fn sparse_expected_range_specs(
    isolation: Option<(&ScenarioIsolation, &str)>,
) -> Vec<SparseRangeSpec> {
    [
        SparseRangeSpec {
            label: "prefix",
            offset: 0,
            payload: sparse_range_payload(isolation, "prefix", SPARSE_PREFIX_PAYLOAD),
        },
        SparseRangeSpec {
            label: "first_chunk_boundary",
            offset: SPARSE_FIRST_BOUNDARY_OFFSET,
            payload: sparse_range_payload(
                isolation,
                "first_chunk_boundary",
                SPARSE_FIRST_BOUNDARY_PAYLOAD,
            ),
        },
        SparseRangeSpec {
            label: "second_chunk_boundary",
            offset: SPARSE_SECOND_BOUNDARY_OFFSET,
            payload: sparse_range_payload(
                isolation,
                "second_chunk_boundary",
                SPARSE_SECOND_BOUNDARY_PAYLOAD,
            ),
        },
        SparseRangeSpec {
            label: "final_tail",
            offset: SPARSE_FINAL_TAIL_OFFSET,
            payload: sparse_range_payload(isolation, "final_tail", SPARSE_FINAL_TAIL_PAYLOAD),
        },
    ]
    .into_iter()
    .collect()
}

pub(in crate::scenario) fn sparse_range_payload(
    isolation: Option<(&ScenarioIsolation, &str)>,
    label: &str,
    payload: &[u8],
) -> Vec<u8> {
    match isolation {
        Some((isolation, logical_path)) => {
            isolation.payload(label, logical_path, &String::from_utf8_lossy(payload))
        }
        None => payload.to_vec(),
    }
}

pub(in crate::scenario) fn sparse_hole_specs() -> &'static [SparseHoleSpec] {
    &[
        SparseHoleSpec {
            offset: 65_536,
            len: 4_096,
        },
        SparseHoleSpec {
            offset: 1_572_864,
            len: 4_096,
        },
    ]
}

pub(in crate::scenario) fn sparse_logical_size(
    base_logical_size: u64,
    ranges: &[SparseRangeSpec],
) -> u64 {
    ranges.iter().fold(base_logical_size, |size, range| {
        size.max(range.offset + range.payload.len() as u64)
    })
}

pub(in crate::scenario) fn sparse_final_logical_size(ranges: &[SparseRangeSpec]) -> u64 {
    sparse_logical_size(SPARSE_FINAL_LOGICAL_SIZE, ranges)
}

pub(in crate::scenario) fn run_sparse_range_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, SparseRangeSummary), ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let reader = &clients[1];
    let extender = &clients[2];
    let mut operations = Vec::new();
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let sparse_ranges = sparse_expected_range_specs(Some((isolation, &sparse_path)));
    let initial_logical_size =
        sparse_logical_size(SPARSE_INITIAL_LOGICAL_SIZE, &sparse_ranges[..2]);
    let shrunk_logical_size = sparse_logical_size(SPARSE_SHRUNK_LOGICAL_SIZE, &sparse_ranges[..2]);
    let final_logical_size = sparse_final_logical_size(&sparse_ranges);

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            1,
            writer,
            "write_range",
            &format!("{sparse_path}@0"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                writer,
                "write-range",
                &sparse_path,
                Some(0),
                None,
                Some(&sparse_ranges[0].payload),
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            2,
            writer,
            "write_range",
            &format!("{sparse_path}@1048569"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                writer,
                "write-range",
                &sparse_path,
                Some(SPARSE_FIRST_BOUNDARY_OFFSET),
                None,
                Some(&sparse_ranges[1].payload),
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            3,
            writer,
            "truncate_extend",
            &format!("{sparse_path}@{initial_logical_size}"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                writer,
                "truncate",
                &sparse_path,
                None,
                Some(initial_logical_size),
                None,
            ),
        )?,
    )?;
    wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: initial_logical_size,
        expected_ranges: &sparse_ranges[..2],
        hole_specs: &[],
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "initial sparse ranges",
    })?;

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            4,
            reader,
            "read_probe",
            &format!("{sparse_path}@65536"),
            run_sparse_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                reader,
                "read-range-zero",
                &sparse_path,
                Some(65_536),
                Some(4_096),
                None,
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            5,
            reader,
            "truncate_shrink",
            &format!("{sparse_path}@{shrunk_logical_size}"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                reader,
                "truncate",
                &sparse_path,
                None,
                Some(shrunk_logical_size),
                None,
            ),
        )?,
    )?;
    wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: shrunk_logical_size,
        expected_ranges: &sparse_ranges[..2],
        hole_specs: &[],
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "shrunk sparse ranges",
    })?;

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            6,
            extender,
            "truncate_extend",
            &format!("{sparse_path}@{final_logical_size}"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                extender,
                "truncate",
                &sparse_path,
                None,
                Some(final_logical_size),
                None,
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            7,
            extender,
            "write_range",
            &format!("{sparse_path}@2097147"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                extender,
                "write-range",
                &sparse_path,
                Some(SPARSE_SECOND_BOUNDARY_OFFSET),
                None,
                Some(&sparse_ranges[2].payload),
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            8,
            writer,
            "write_range",
            &format!("{sparse_path}@3145900"),
            run_sparse_helper_process(
                context,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                writer,
                "write-range",
                &sparse_path,
                Some(SPARSE_FINAL_TAIL_OFFSET),
                None,
                Some(&sparse_ranges[3].payload),
            ),
        )?,
    )?;

    let (read_probes, zero_filled_holes) = wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: final_logical_size,
        expected_ranges: &sparse_ranges,
        hole_specs: sparse_hole_specs(),
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "final sparse ranges",
    })?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            9,
            writer,
            "read_probe",
            &format!("{sparse_path}@3145900"),
            run_sparse_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                writer,
                "read-range-exact",
                &sparse_path,
                Some(SPARSE_FINAL_TAIL_OFFSET),
                None,
                Some(&sparse_ranges[3].payload),
            ),
        )?,
    )?;

    let expected_payload = expected_sparse_payload(&sparse_ranges, final_logical_size);
    let summary = SparseRangeSummary {
        relative_path: sparse_path,
        logical_size: final_logical_size,
        final_checksum_sha256: bytes_sha256(&expected_payload),
        expected_byte_ranges: sparse_expected_ranges(&sparse_ranges),
        zero_filled_holes,
        read_probes,
    };
    Ok((operations, summary))
}

pub(in crate::scenario) struct SparseExpectedWait<'a> {
    pub(in crate::scenario) context: &'a ScenarioContext,
    pub(in crate::scenario) clients: &'a [ClientSpec],
    pub(in crate::scenario) sparse_path: &'a str,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) expected_ranges: &'a [SparseRangeSpec],
    pub(in crate::scenario) hole_specs: &'a [SparseHoleSpec],
    pub(in crate::scenario) deadline: Instant,
    pub(in crate::scenario) poll_interval: Duration,
    pub(in crate::scenario) label: &'a str,
}

pub(in crate::scenario) fn wait_for_sparse_expected_until(
    wait: SparseExpectedWait<'_>,
) -> Result<(Vec<SparseReadProbe>, Vec<SparseHoleProbe>), ScenarioError> {
    let wait_started = Instant::now();
    let SparseExpectedWait {
        context,
        clients,
        sparse_path,
        logical_size,
        expected_ranges,
        hole_specs,
        deadline,
        poll_interval,
        label,
    } = wait;
    loop {
        let observation = observe_sparse_ranges(
            context,
            clients,
            sparse_path,
            logical_size,
            expected_ranges,
            hole_specs,
        )?;
        if observation.missing.is_empty() {
            return Ok((observation.read_probes, observation.zero_filled_holes));
        }

        let now = Instant::now();
        if now >= deadline {
            let artifact = write_sparse_timeout_diagnostics(
                context,
                SparseTimeoutDiagnosticsInput {
                    label,
                    sparse_path,
                    logical_size,
                    expected_ranges,
                    hole_specs,
                    poll_interval,
                    elapsed: wait_started.elapsed(),
                    observation: &observation,
                },
            );
            let artifact_note = match artifact {
                Ok(artifact) => format!(
                    "; diagnostics: artifacts/scenarios/{}/{}",
                    AdvancedMountedKind::SparseRange.name(),
                    artifact
                ),
                Err(error) => format!("; diagnostics write failed: {error}"),
            };
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}{}",
                    observation.missing.join(", "),
                    artifact_note
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseTimeoutDiagnostics {
    pub(in crate::scenario) schema_version: u32,
    pub(in crate::scenario) run_id: String,
    pub(in crate::scenario) scenario: String,
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) sparse_path: String,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) elapsed_ms: u64,
    pub(in crate::scenario) poll_interval_ms: u64,
    pub(in crate::scenario) missing: Vec<String>,
    pub(in crate::scenario) expected_ranges: Vec<SparseTimeoutRangeSpec>,
    pub(in crate::scenario) expected_holes: Vec<SparseTimeoutHoleSpec>,
    pub(in crate::scenario) environment: Vec<SparseEnvironmentValue>,
    pub(in crate::scenario) clients: Vec<SparseClientObservation>,
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseTimeoutRangeSpec {
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) expected_checksum_sha256: String,
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseTimeoutHoleSpec {
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseEnvironmentValue {
    pub(in crate::scenario) name: &'static str,
    pub(in crate::scenario) value: Option<String>,
}

pub(in crate::scenario) struct SparseTimeoutDiagnosticsInput<'a> {
    pub(in crate::scenario) label: &'a str,
    pub(in crate::scenario) sparse_path: &'a str,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) expected_ranges: &'a [SparseRangeSpec],
    pub(in crate::scenario) hole_specs: &'a [SparseHoleSpec],
    pub(in crate::scenario) poll_interval: Duration,
    pub(in crate::scenario) elapsed: Duration,
    pub(in crate::scenario) observation: &'a SparseObservation,
}

pub(in crate::scenario) struct SparseObservation {
    pub(in crate::scenario) read_probes: Vec<SparseReadProbe>,
    pub(in crate::scenario) zero_filled_holes: Vec<SparseHoleProbe>,
    pub(in crate::scenario) missing: Vec<String>,
    pub(in crate::scenario) clients: Vec<SparseClientObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseClientObservation {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) mount_point: String,
    pub(in crate::scenario) path: String,
    pub(in crate::scenario) parent_visible: bool,
    pub(in crate::scenario) metadata: SparseMetadataObservation,
    pub(in crate::scenario) open: SparseOpenObservation,
    pub(in crate::scenario) ranges: Vec<SparseRangeObservation>,
    pub(in crate::scenario) holes: Vec<SparseHoleObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseMetadataObservation {
    pub(in crate::scenario) ok: bool,
    pub(in crate::scenario) len: Option<u64>,
    pub(in crate::scenario) is_file: Option<bool>,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseOpenObservation {
    pub(in crate::scenario) ok: bool,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseRangeObservation {
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) expected_checksum_sha256: String,
    pub(in crate::scenario) actual_checksum_sha256: Option<String>,
    pub(in crate::scenario) matched: Option<bool>,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseHoleObservation {
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) all_zero: Option<bool>,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseIoErrorObservation {
    pub(in crate::scenario) kind: String,
    pub(in crate::scenario) message: String,
}

pub(in crate::scenario) fn observe_sparse_ranges(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    sparse_path: &str,
    logical_size: u64,
    expected_ranges: &[SparseRangeSpec],
    hole_specs: &[SparseHoleSpec],
) -> Result<SparseObservation, ScenarioError> {
    let mut read_probes = Vec::new();
    let mut hole_results = Vec::new();
    let mut missing = Vec::new();
    let mut client_observations = Vec::new();
    let expected_payload = expected_sparse_payload(expected_ranges, logical_size);

    for client in clients {
        let path = client.mount_point.join(sparse_path);
        let mut client_observation = SparseClientObservation {
            client: client.name.clone(),
            mount_point: client.mount_point.display().to_string(),
            path: path.display().to_string(),
            parent_visible: false,
            metadata: SparseMetadataObservation {
                ok: false,
                len: None,
                is_file: None,
                error: None,
            },
            open: SparseOpenObservation {
                ok: false,
                error: None,
            },
            ranges: Vec::new(),
            holes: Vec::new(),
        };

        match run_sparse_helper_process(
            context,
            MOUNTED_VALIDATION_PROBE_TIMEOUT,
            client,
            "read-all-exact",
            sparse_path,
            None,
            None,
            Some(&expected_payload),
        ) {
            Ok(()) => {
                client_observation.parent_visible = true;
                client_observation.metadata = SparseMetadataObservation {
                    ok: true,
                    len: Some(logical_size),
                    is_file: Some(true),
                    error: None,
                };
                client_observation.open = SparseOpenObservation {
                    ok: true,
                    error: None,
                };
                for range in expected_ranges {
                    let expected_checksum = bytes_sha256(&range.payload);
                    client_observation.ranges.push(SparseRangeObservation {
                        label: range.label.to_string(),
                        offset: range.offset,
                        len: range.payload.len(),
                        expected_checksum_sha256: expected_checksum.clone(),
                        actual_checksum_sha256: Some(expected_checksum.clone()),
                        matched: Some(true),
                        error: None,
                    });
                    read_probes.push(SparseReadProbe {
                        client: client.name.clone(),
                        offset: range.offset,
                        len: range.payload.len(),
                        actual_checksum_sha256: expected_checksum,
                        matched: true,
                    });
                }
                for hole in hole_specs {
                    client_observation.holes.push(SparseHoleObservation {
                        offset: hole.offset,
                        len: hole.len,
                        all_zero: Some(true),
                        error: None,
                    });
                    hole_results.push(SparseHoleProbe {
                        client: client.name.clone(),
                        offset: hole.offset,
                        len: hole.len,
                        all_zero: true,
                    });
                }
            }
            Err(error) if hotspot_helper_error_is_fatal(&error) => return Err(error),
            Err(error) => {
                let observation = sparse_scenario_error_observation(&error);
                client_observation.metadata.error = Some(observation.clone());
                client_observation.open.error = Some(observation);
                missing.push(format!("{}:{sparse_path} ({error})", client.name));
            }
        }
        client_observations.push(client_observation);
    }

    Ok(SparseObservation {
        read_probes,
        zero_filled_holes: hole_results,
        missing,
        clients: client_observations,
    })
}

pub(in crate::scenario) fn write_sparse_timeout_diagnostics(
    context: &ScenarioContext,
    input: SparseTimeoutDiagnosticsInput<'_>,
) -> Result<String, ScenarioError> {
    let artifact = sparse_timeout_diagnostics_file_name(input.label);
    let diagnostics = SparseTimeoutDiagnostics {
        schema_version: 1,
        run_id: context.run_id.clone(),
        scenario: AdvancedMountedKind::SparseRange.name().to_string(),
        label: input.label.to_string(),
        sparse_path: input.sparse_path.to_string(),
        logical_size: input.logical_size,
        elapsed_ms: duration_millis_u64(input.elapsed),
        poll_interval_ms: duration_millis_u64(input.poll_interval),
        missing: input.observation.missing.clone(),
        expected_ranges: input
            .expected_ranges
            .iter()
            .map(|range| SparseTimeoutRangeSpec {
                label: range.label.to_string(),
                offset: range.offset,
                len: range.payload.len(),
                expected_checksum_sha256: bytes_sha256(&range.payload),
            })
            .collect(),
        expected_holes: input
            .hole_specs
            .iter()
            .map(|hole| SparseTimeoutHoleSpec {
                offset: hole.offset,
                len: hole.len,
            })
            .collect(),
        environment: sparse_timeout_environment(),
        clients: input.observation.clients.clone(),
    };
    fs::write(
        context.scenario_dir.join(&artifact),
        serde_json::to_vec_pretty(&diagnostics)?,
    )?;
    append_sparse_timeout_diagnostics_event(context, input.label, &artifact, &diagnostics)?;
    Ok(artifact)
}

pub(in crate::scenario) fn append_sparse_timeout_diagnostics_event(
    context: &ScenarioContext,
    label: &str,
    artifact: &str,
    diagnostics: &SparseTimeoutDiagnostics,
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    serde_json::to_writer(
        &mut log,
        &json!({
            "run_id": context.run_id,
            "scenario": AdvancedMountedKind::SparseRange.name(),
            "operation": "sparse_timeout_diagnostics",
            "label": label,
            "path": diagnostics.sparse_path,
            "logical_size": diagnostics.logical_size,
            "artifact": format!(
                "artifacts/scenarios/{}/{}",
                AdvancedMountedKind::SparseRange.name(),
                artifact
            ),
            "missing": diagnostics.missing,
            "clients": diagnostics.clients,
        }),
    )?;
    log.write_all(b"\n")?;
    Ok(())
}

pub(in crate::scenario) fn sparse_timeout_diagnostics_file_name(label: &str) -> String {
    let slug = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    format!("sparse-{slug}-timeout-diagnostics.json")
}

pub(in crate::scenario) fn sparse_timeout_environment() -> Vec<SparseEnvironmentValue> {
    [
        "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS",
        "STUDIOFS_PG_SYNC_POLL_SECS",
        "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL",
    ]
    .into_iter()
    .map(|name| SparseEnvironmentValue {
        name,
        value: std::env::var(name).ok(),
    })
    .collect()
}

pub(in crate::scenario) fn sparse_io_error_observation(
    error: &io::Error,
) -> SparseIoErrorObservation {
    SparseIoErrorObservation {
        kind: format!("{:?}", error.kind()),
        message: error.to_string(),
    }
}

pub(in crate::scenario) fn sparse_scenario_error_observation(
    error: &ScenarioError,
) -> SparseIoErrorObservation {
    match error {
        ScenarioError::Io(error) => sparse_io_error_observation(error),
        error => SparseIoErrorObservation {
            kind: "ScenarioError".to_string(),
            message: error.to_string(),
        },
    }
}

pub(in crate::scenario) fn duration_millis_u64(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

pub(in crate::scenario) fn expected_sparse_payload(
    ranges: &[SparseRangeSpec],
    logical_size: u64,
) -> Vec<u8> {
    let mut payload = vec![0; logical_size as usize];
    for range in ranges {
        let start = range.offset as usize;
        let end = start + range.payload.len();
        payload[start..end].copy_from_slice(&range.payload);
    }
    payload
}

pub(in crate::scenario) fn sparse_expected_ranges(
    ranges: &[SparseRangeSpec],
) -> Vec<SparseExpectedRange> {
    ranges
        .iter()
        .map(|range| SparseExpectedRange {
            label: range.label.to_string(),
            offset: range.offset,
            len: range.payload.len(),
            expected_checksum_sha256: bytes_sha256(&range.payload),
        })
        .collect()
}

pub(in crate::scenario) fn sparse_hole_summaries(
    clients: &[&str],
    hole_specs: &[SparseHoleSpec],
    all_zero: bool,
) -> Vec<SparseHoleProbe> {
    clients
        .iter()
        .flat_map(|client| {
            hole_specs.iter().map(move |hole| SparseHoleProbe {
                client: (*client).to_string(),
                offset: hole.offset,
                len: hole.len,
                all_zero,
            })
        })
        .collect()
}

pub(in crate::scenario) fn planned_sparse_range_summary() -> SparseRangeSummary {
    let ranges = sparse_expected_range_specs(None);
    let final_logical_size = sparse_final_logical_size(&ranges);
    let expected_payload = expected_sparse_payload(&ranges, final_logical_size);
    SparseRangeSummary {
        relative_path: SPARSE_FILE_PATH.to_string(),
        logical_size: final_logical_size,
        final_checksum_sha256: bytes_sha256(&expected_payload),
        expected_byte_ranges: sparse_expected_ranges(&ranges),
        zero_filled_holes: sparse_hole_summaries(
            ADVANCED_MOUNTED_CLIENT_NAMES,
            sparse_hole_specs(),
            true,
        ),
        read_probes: ADVANCED_MOUNTED_CLIENT_NAMES
            .iter()
            .flat_map(|client| {
                ranges.iter().map(move |range| SparseReadProbe {
                    client: (*client).to_string(),
                    offset: range.offset,
                    len: range.payload.len(),
                    actual_checksum_sha256: bytes_sha256(&range.payload),
                    matched: true,
                })
            })
            .collect(),
    }
}

pub(in crate::scenario) fn planned_cold_observer_summary() -> ColdObserverSummary {
    ColdObserverSummary {
        client: "client-d".to_string(),
        local_metadata_db: PathBuf::from(
            "artifacts/clients/client-d/client-root/meta/projectfs/projectfs.db",
        ),
        cache_dir: PathBuf::from("artifacts/clients/client-d/cache"),
        discovered_after_history: true,
    }
}

pub(in crate::scenario) fn planned_catchup_summary() -> CatchupSummary {
    CatchupSummary {
        warm_writers: STALE_REMOUNT_WARM_WRITER_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        stale_remount_client: "client-c".to_string(),
        cold_discover_client: "client-d".to_string(),
        stale_remount_after_history: true,
        cold_discover_after_history: true,
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

pub(in crate::scenario) fn planned_invalidation_dropout_summary() -> InvalidationDropoutSummary {
    let root = AdvancedMountedKind::InvalidationDropoutFallback.path_root();
    let target_path = format!("{root}/{INVALIDATION_DROPOUT_TARGET_FILE}");
    let final_payload =
        format!("planned invalidation dropout\npath={target_path}\nbody=fallback final\n")
            .into_bytes();
    InvalidationDropoutSummary {
        observer_client: INVALIDATION_DROPOUT_OBSERVER_CLIENT.to_string(),
        writer_clients: INVALIDATION_DROPOUT_WRITER_CLIENTS
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        notify_suppressed: true,
        fallback_interval_ms: INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS,
        stale_window_ms: INVALIDATION_DROPOUT_STALE_WINDOW_MS,
        stale_window_observed: true,
        delete_stale_window_observed: true,
        fallback_resumed: true,
        final_observer_path: target_path,
        final_observer_checksum_sha256: bytes_sha256(&final_payload),
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

pub(in crate::scenario) fn write_planned_cold_observer_diagnostics(
    context: &ScenarioContext,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("cold-observer-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_observer": "client-d",
            "warm_clients": COLD_OBSERVER_WARM_CLIENT_NAMES,
            "discovered_after_history": true,
            "mode": "planned",
        }))?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_planned_catchup_diagnostics(
    context: &ScenarioContext,
) -> Result<(), ScenarioError> {
    write_catchup_diagnostics(context, &planned_catchup_summary())
}

pub(in crate::scenario) fn write_planned_invalidation_dropout_diagnostics(
    context: &ScenarioContext,
    summary: &InvalidationDropoutSummary,
) -> Result<(), ScenarioError> {
    write_invalidation_dropout_diagnostics(context, summary)
}

pub(in crate::scenario) fn write_catchup_diagnostics(
    context: &ScenarioContext,
    catchup: &CatchupSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("catchup-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "warm_writers": catchup.warm_writers,
            "stale_remount_client": catchup.stale_remount_client,
            "cold_discover_client": catchup.cold_discover_client,
            "stale_remount_after_history": catchup.stale_remount_after_history,
            "cold_discover_after_history": catchup.cold_discover_after_history,
            "final_validation_result": catchup.final_validation_result,
        }))?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_invalidation_dropout_diagnostics(
    context: &ScenarioContext,
    summary: &InvalidationDropoutSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("invalidation-fallback-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "observer_client": summary.observer_client,
            "writer_clients": summary.writer_clients,
            "notify_suppressed": summary.notify_suppressed,
            "fallback_interval_ms": summary.fallback_interval_ms,
            "stale_window_ms": summary.stale_window_ms,
            "stale_window_observed": summary.stale_window_observed,
            "delete_stale_window_observed": summary.delete_stale_window_observed,
            "fallback_resumed": summary.fallback_resumed,
            "final_observer_path": summary.final_observer_path,
            "final_observer_checksum_sha256": summary.final_observer_checksum_sha256,
            "final_validation_result": summary.final_validation_result,
        }))?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn planned_long_history_summary() -> LongHistorySummary {
    let overwrite_target = format!(
        "{}/{}",
        AdvancedMountedKind::ColdObserverLongHistory.path_root(),
        COLD_OBSERVER_OVERWRITE_TARGET
    );
    let final_payload = cold_observer_planned_payload("overwrite_v2", &overwrite_target);
    LongHistorySummary {
        bounded_operation_count: planned_advanced_mounted_operations(
            AdvancedMountedKind::ColdObserverLongHistory,
            Instant::now(),
        )
        .len(),
        expected_present_paths: vec![
            "cold-observer/renamed-final.txt".to_string(),
            "cold-observer/overwrite-target.txt".to_string(),
            "cold-observer/append-target.txt".to_string(),
            "cold-observer/nested/child.txt".to_string(),
        ],
        expected_absent_paths: vec![
            "cold-observer/delete-target.txt".to_string(),
            "cold-observer/rename-source.txt".to_string(),
        ],
        final_checksum_sha256: bytes_sha256(&final_payload),
    }
}

pub(in crate::scenario) fn planned_stale_remount_long_history_summary() -> LongHistorySummary {
    let root = AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root();
    let overwrite_target = format!("{root}/{COLD_OBSERVER_OVERWRITE_TARGET}");
    let final_payload = cold_observer_planned_payload("overwrite_v2", &overwrite_target);
    LongHistorySummary {
        bounded_operation_count: planned_advanced_mounted_operations(
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
            Instant::now(),
        )
        .len(),
        expected_present_paths: vec![
            format!("{root}/{COLD_OBSERVER_RENAME_DEST}"),
            format!("{root}/{COLD_OBSERVER_OVERWRITE_TARGET}"),
            format!("{root}/{COLD_OBSERVER_APPEND_TARGET}"),
            format!("{root}/{COLD_OBSERVER_NESTED_FILE}"),
        ],
        expected_absent_paths: vec![
            format!("{root}/{COLD_OBSERVER_DELETE_TARGET}"),
            format!("{root}/{COLD_OBSERVER_RENAME_SOURCE}"),
        ],
        final_checksum_sha256: bytes_sha256(&final_payload),
    }
}

pub(in crate::scenario) fn cold_observer_planned_payload(
    label: &str,
    logical_path: &str,
) -> Vec<u8> {
    format!("planned\nlabel={label}\npath={logical_path}\n{label}").into_bytes()
}

pub(in crate::scenario) fn sparse_ranges_match(
    kind: AdvancedMountedKind,
    summary: Option<&SparseRangeSummary>,
) -> bool {
    match (kind, summary) {
        (AdvancedMountedKind::SparseRange, Some(summary)) => {
            let final_logical_size = summary
                .expected_byte_ranges
                .iter()
                .fold(SPARSE_FINAL_LOGICAL_SIZE, |size, range| {
                    size.max(range.offset + range.len as u64)
                });
            summary.logical_size == final_logical_size
                && summary.read_probes.iter().all(|probe| probe.matched)
                && summary.zero_filled_holes.iter().all(|hole| hole.all_zero)
        }
        (AdvancedMountedKind::SparseRange, None) => false,
        (_, _) => true,
    }
}

pub(in crate::scenario) fn fs_op(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    f: impl FnOnce() -> io::Result<()>,
) -> NamespaceCollisionOperation {
    record_advanced_operation(
        started,
        order,
        client,
        operation,
        path,
        f().map_err(|error| error.to_string()),
    )
}

pub(in crate::scenario) fn required_op(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    result: Result<(), ScenarioError>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    match result {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) => {
            let operation_record = record_advanced_operation(
                started,
                order,
                client,
                operation,
                path,
                Err(error.to_string()),
            );
            Err(ScenarioError::Io(io::Error::other(format!(
                "{} failed for {}: {:?}",
                operation, path, operation_record.error
            ))))
        }
    }
}

pub(in crate::scenario) fn required_record(
    operation: NamespaceCollisionOperation,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    if let Some(error) = &operation.error {
        Err(ScenarioError::Io(io::Error::other(format!(
            "{} failed for {}: {}",
            operation.operation, operation.path, error
        ))))
    } else {
        Ok(operation)
    }
}

pub(in crate::scenario) fn record_advanced_operation(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    result: Result<(), String>,
) -> NamespaceCollisionOperation {
    match result {
        Ok(()) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            result: "accepted".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: None,
        },
        Err(error) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            result: "rejected".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: Some(error),
        },
    }
}

pub(in crate::scenario) fn write_advanced_mounted_summary(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    summary: &AdvancedMountedSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join(kind.summary_file()),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_lww_oracle(
    context: &ScenarioContext,
    oracle: &LwwReconnectOracle,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("lww-oracle.json"),
        serde_json::to_vec_pretty(oracle)?,
    )?;
    Ok(())
}
