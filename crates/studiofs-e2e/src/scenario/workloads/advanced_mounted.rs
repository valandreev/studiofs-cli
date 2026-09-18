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

    pub(in crate::scenario) fn aligned_payload(
        &self,
        label: &str,
        logical_path: &str,
        body: &str,
        len: usize,
    ) -> Vec<u8> {
        let mut payload = self.payload(label, logical_path, body);
        assert!(
            payload.len() <= len,
            "aligned scenario payload header must fit requested length"
        );
        let seed = payload.clone();
        while payload.len() < len {
            let remaining = len - payload.len();
            let take = remaining.min(seed.len());
            payload.extend_from_slice(&seed[..take]);
        }
        payload
    }
}

pub(in crate::scenario) fn mounted_chaos_payload(
    isolation: &ScenarioIsolation,
    label: &str,
    logical_path: &str,
    body: &str,
) -> Vec<u8> {
    if logical_path.ends_with("read-target.txt") {
        isolation.aligned_payload(
            label,
            logical_path,
            body,
            MOUNTED_CHAOS_DIRECT_IO_PAYLOAD_LEN,
        )
    } else {
        isolation.payload(label, logical_path, body)
    }
}

#[cfg(not(test))]
pub(in crate::scenario) fn drain_child_stderr(
    child: &mut std::process::Child,
) -> Option<std::thread::JoinHandle<String>> {
    child.stderr.take().map(|mut stderr| {
        std::thread::spawn(move || {
            let mut output = String::new();
            let _ = std::io::Read::read_to_string(&mut stderr, &mut output);
            output
        })
    })
}

#[cfg(not(test))]
pub(in crate::scenario) fn collect_child_stderr(
    stderr_reader: &mut Option<std::thread::JoinHandle<String>>,
) -> String {
    stderr_reader
        .take()
        .and_then(|reader| reader.join().ok())
        .unwrap_or_default()
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
    LargeHistoryColdBootstrap,
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
            Self::LargeHistoryColdBootstrap => AdvancedMountedProperties {
                name: MOUNTED_LARGE_HISTORY_COLD_BOOTSTRAP_CONVERGENCE,
                description: "opt-in mounted large-history cold-bootstrap catch-up convergence",
                path_root: "large-history-cold-bootstrap",
                summary_file: "large-history-cold-bootstrap-summary.json",
                client_names: COLD_OBSERVER_CLIENT_NAMES,
                convergence_timeout: LARGE_HISTORY_CONVERGENCE_TIMEOUT,
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
        let large_history_matches = summary
            .large_history
            .as_ref()
            .map(LargeHistorySummary::matched)
            .unwrap_or_else(|| self.kind != AdvancedMountedKind::LargeHistoryColdBootstrap);
        let invalidation_dropout_matches = summary
            .invalidation_dropout
            .as_ref()
            .map(InvalidationDropoutSummary::matched)
            .unwrap_or(self.kind != AdvancedMountedKind::InvalidationDropoutFallback);
        let validation_passed = summary.final_validator.converged
            && summary.namespace_violations.is_empty()
            && lww_oracle_matches
            && long_history_matches
            && large_history_matches
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
            "large_history": summary.large_history,
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
        if self.kind == AdvancedMountedKind::LargeHistoryColdBootstrap {
            artifacts.push(format!(
                "artifacts/scenarios/{}/large-history-status.ndjson",
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
    #[serde(default)]
    pub(in crate::scenario) large_history: Option<LargeHistorySummary>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct LargeHistorySummary {
    pub(in crate::scenario) mutation_count: usize,
    /// Expected workload count reported only after the post-writer marker and
    /// stable remote-head catch-up; status does not expose an exact operation counter.
    pub(in crate::scenario) remote_visible_mutations: usize,
    pub(in crate::scenario) initial_mutation_count: usize,
    pub(in crate::scenario) moving_head_mutation_count: usize,
    pub(in crate::scenario) cold_client: String,
    pub(in crate::scenario) checkpoint_version: Option<u64>,
    pub(in crate::scenario) startup_target_version: Option<u64>,
    pub(in crate::scenario) applied_version_progression: Vec<u64>,
    #[serde(default)]
    pub(in crate::scenario) applied_version_restart_index: Option<usize>,
    pub(in crate::scenario) observed_page_count: usize,
    pub(in crate::scenario) observed_event_rows: u64,
    pub(in crate::scenario) observed_event_bytes: u64,
    pub(in crate::scenario) saw_catching_up: bool,
    pub(in crate::scenario) restarted_during_catchup: bool,
    pub(in crate::scenario) moving_head_observed: bool,
    pub(in crate::scenario) reached_live: bool,
    pub(in crate::scenario) final_read_write_converged: bool,
    pub(in crate::scenario) final_marker_path: String,
    pub(in crate::scenario) wall_duration_ms: u64,
}

impl LargeHistorySummary {
    fn applied_version_progression_is_monotonic(&self) -> bool {
        let Some(restart_index) = self.applied_version_restart_index else {
            return self
                .applied_version_progression
                .windows(2)
                .all(|versions| versions[0] <= versions[1]);
        };
        self.applied_version_progression
            .get(..restart_index)
            .zip(self.applied_version_progression.get(restart_index..))
            .is_some_and(|(before_restart, after_restart)| {
                before_restart
                    .windows(2)
                    .all(|versions| versions[0] <= versions[1])
                    && after_restart
                        .windows(2)
                        .all(|versions| versions[0] <= versions[1])
            })
    }

    fn matched(&self) -> bool {
        self.mutation_count >= LARGE_HISTORY_MUTATION_COUNT
            && self.remote_visible_mutations >= LARGE_HISTORY_MUTATION_COUNT
            && self.cold_client == LARGE_HISTORY_COLD_CLIENT
            && self.checkpoint_version.is_some()
            && self
                .startup_target_version
                .zip(self.applied_version_progression.last().copied())
                .is_some_and(|(target, applied)| applied >= target)
            && self.applied_version_progression_is_monotonic()
            && self.observed_page_count > 1
            && self.observed_event_rows > 0
            && self.observed_event_bytes > 0
            && self.saw_catching_up
            && self.restarted_during_catchup
            && self.moving_head_observed
            && self.reached_live
            && self.final_read_write_converged
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_history_match_rejects_intra_session_regression() {
        let mut summary = planned_large_history_summary();
        summary.applied_version_progression = vec![30, 31, 29, 54];
        summary.applied_version_restart_index = Some(1);
        summary.startup_target_version = Some(53);

        assert!(!summary.matched());
    }

    #[test]
    fn large_history_match_accepts_restart_reset_between_monotonic_sessions() {
        let mut summary = planned_large_history_summary();
        summary.applied_version_progression = vec![30, 29, 50, 54];
        summary.applied_version_restart_index = Some(1);
        summary.startup_target_version = Some(53);

        assert!(summary.matched());
    }
}

mod finish;
mod hotspot;
mod invalidation_symlink;
mod open_handle;
mod planned;
mod probe;
mod probe_waits;
mod runner;
mod sparse;

pub(in crate::scenario) use finish::*;
pub(in crate::scenario) use hotspot::*;
pub(in crate::scenario) use invalidation_symlink::*;
pub(in crate::scenario) use open_handle::*;
pub(in crate::scenario) use planned::*;
pub(in crate::scenario) use probe::*;
pub(in crate::scenario) use probe_waits::*;
pub(in crate::scenario) use runner::*;
pub(in crate::scenario) use sparse::*;
