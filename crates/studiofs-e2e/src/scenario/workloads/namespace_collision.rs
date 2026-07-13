//! Namespace-collision scenario implementations and operation recording.

use super::*;

#[cfg(not(test))]
static NAMESPACE_COLLISION_ACTIVE_WORKER_BATCHES: AtomicBool = AtomicBool::new(false);

#[cfg(not(test))]
struct NamespaceCollisionWorkerBatchPermit;

#[cfg(not(test))]
impl NamespaceCollisionWorkerBatchPermit {
    fn try_acquire(timeout: Duration, pending: &[(usize, String)]) -> Result<Self, ScenarioError> {
        if NAMESPACE_COLLISION_ACTIVE_WORKER_BATCHES
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(Self);
        }

        let pending = pending
            .iter()
            .map(|(_, label)| label.clone())
            .chain(std::iter::once(
                "namespace collision worker limit reached; previous timed operation may still be blocked"
                    .to_string(),
            ))
            .collect();
        Err(ScenarioError::NamespaceCollisionOperationTimeout {
            timeout_ms: timeout.as_millis(),
            pending,
        })
    }
}

#[cfg(not(test))]
impl Drop for NamespaceCollisionWorkerBatchPermit {
    fn drop(&mut self) {
        NAMESPACE_COLLISION_ACTIVE_WORKER_BATCHES.store(false, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::scenario) enum NamespaceCollisionKind {
    RenameVsRename,
    RenameVsUnlink,
    UnlinkVsAppend,
    OverwriteVsRead,
    MkdirRmdirVsCreate,
    ReaddirStatDuringMutation,
}

impl NamespaceCollisionKind {
    fn name(self) -> &'static str {
        match self {
            Self::RenameVsRename => COLLISION_RENAME_VS_RENAME,
            Self::RenameVsUnlink => COLLISION_RENAME_VS_UNLINK,
            Self::UnlinkVsAppend => COLLISION_UNLINK_VS_APPEND,
            Self::OverwriteVsRead => COLLISION_OVERWRITE_VS_READ,
            Self::MkdirRmdirVsCreate => COLLISION_MKDIR_RMDIR_VS_CREATE,
            Self::ReaddirStatDuringMutation => COLLISION_READDIR_STAT_DURING_MUTATION,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::RenameVsRename => "mounted namespace collision: rename vs rename",
            Self::RenameVsUnlink => "mounted namespace collision: rename vs unlink",
            Self::UnlinkVsAppend => "mounted namespace collision: unlink vs append/write",
            Self::OverwriteVsRead => "mounted namespace collision: overwrite vs repeated reads",
            Self::MkdirRmdirVsCreate => "mounted namespace collision: mkdir/rmdir vs create",
            Self::ReaddirStatDuringMutation => {
                "mounted namespace collision: repeated readdir/stat during mutations"
            }
        }
    }

    fn initial_paths(self) -> Vec<(&'static str, &'static [u8])> {
        match self {
            Self::RenameVsRename | Self::RenameVsUnlink => {
                vec![("shared/file.txt", b"initial namespace payload\n")]
            }
            Self::UnlinkVsAppend | Self::OverwriteVsRead => {
                vec![("shared/data.txt", COLLISION_OLD_COMMITTED_PAYLOAD)]
            }
            Self::MkdirRmdirVsCreate => Vec::new(),
            Self::ReaddirStatDuringMutation => {
                vec![
                    ("scan/source-alpha.data", b"a\n"),
                    ("scan/source-b.txt", b"b\n"),
                ]
            }
        }
    }

    fn expected_absent_paths(self) -> &'static [&'static str] {
        match self {
            Self::RenameVsRename => &["shared/file.txt"],
            Self::RenameVsUnlink => &["shared/file.txt"],
            Self::UnlinkVsAppend => &[],
            Self::OverwriteVsRead => &[],
            Self::MkdirRmdirVsCreate => &[],
            Self::ReaddirStatDuringMutation => &["scan/source-alpha.data"],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct NamespaceCollisionScenario {
    pub(in crate::scenario) kind: NamespaceCollisionKind,
}

impl NamespaceCollisionScenario {
    pub(in crate::scenario) fn new(kind: NamespaceCollisionKind) -> Self {
        Self { kind }
    }
}

impl Scenario for NamespaceCollisionScenario {
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
            let mut driver = RealNamespaceCollisionDriver(MultiClientController::new(
                config.clone(),
                SystemClientProcessExecutor,
            ));
            run_namespace_collision_with_driver(
                context,
                self.kind,
                &mut driver,
                COLLISION_CONVERGENCE_TIMEOUT,
                COLLISION_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_namespace_collision_plan_artifacts(context, self.kind)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("namespace-collision-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: NamespaceCollisionSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": self.kind.name(),
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged
                && summary.namespace_violations.is_empty() { "passed" } else { "failed" },
            "final_validator": summary.final_validator,
            "namespace_violations": summary.namespace_violations,
            "operation_log": summary.operation_log,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{}/namespace-collision-summary.json",
                self.kind.name()
            ),
            format!("artifacts/scenarios/{}/validation.json", self.kind.name()),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

pub(in crate::scenario) trait NamespaceCollisionDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_conflicting_operations(
        &mut self,
        _kind: NamespaceCollisionKind,
        _clients: &[ClientSpec],
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn emergency_kill_all_no_wait(&mut self, context: &str) -> Result<(), ScenarioError>;
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

pub(in crate::scenario) struct RealNamespaceCollisionDriver(
    MultiClientController<SystemClientProcessExecutor>,
);

impl NamespaceCollisionDriver for RealNamespaceCollisionDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.0.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.0.clients().into_iter().cloned().collect()
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.unmount_all()?)
    }

    fn emergency_kill_all_no_wait(&mut self, context: &str) -> Result<(), ScenarioError> {
        Ok(self.0.emergency_kill_all_no_wait(context)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct NamespaceCollisionOperation {
    pub(in crate::scenario) order: usize,
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) operation: String,
    pub(in crate::scenario) path: String,
    pub(in crate::scenario) result: String,
    pub(in crate::scenario) timestamp_ms: u64,
    pub(in crate::scenario) error: Option<String>,
}

type NamespaceCollisionAction = Box<dyn FnOnce(&ClientSpec) -> io::Result<()> + Send + 'static>;

pub(in crate::scenario) struct NamespaceCollisionTask {
    pub(in crate::scenario) order: usize,
    pub(in crate::scenario) client: ClientSpec,
    pub(in crate::scenario) operation: &'static str,
    pub(in crate::scenario) path: &'static str,
    pub(in crate::scenario) action: NamespaceCollisionAction,
    pub(in crate::scenario) completion_grace: Duration,
}

impl NamespaceCollisionTask {
    pub(in crate::scenario) fn new(
        order: usize,
        client: ClientSpec,
        operation: &'static str,
        path: &'static str,
        action: impl FnOnce(&ClientSpec) -> io::Result<()> + Send + 'static,
    ) -> Self {
        Self {
            order,
            client,
            operation,
            path,
            action: Box::new(action),
            completion_grace: Duration::ZERO,
        }
    }

    #[cfg(test)]
    pub(in crate::scenario) fn with_completion_grace(mut self, completion_grace: Duration) -> Self {
        self.completion_grace = completion_grace;
        self
    }
}

pub(in crate::scenario) struct NamespaceCollisionHelperSpec {
    pub(in crate::scenario) order: usize,
    pub(in crate::scenario) client: ClientSpec,
    pub(in crate::scenario) operation: &'static str,
    pub(in crate::scenario) helper_operation: &'static str,
    pub(in crate::scenario) path: &'static str,
    pub(in crate::scenario) target: Option<&'static str>,
    pub(in crate::scenario) payload: Option<&'static [u8]>,
}

pub(in crate::scenario) fn namespace_collision_helper_task(
    context: &ScenarioContext,
    spec: NamespaceCollisionHelperSpec,
) -> NamespaceCollisionTask {
    let context = context.clone();
    NamespaceCollisionTask::new(
        spec.order,
        spec.client,
        spec.operation,
        spec.path,
        move |client| {
            let pending_label = match spec.target {
                Some(target) => {
                    format!(
                        "{} {} {} -> {target}",
                        client.name, spec.operation, spec.path
                    )
                }
                None => format!("{} {} {}", client.name, spec.operation, spec.path),
            };
            run_hotspot_helper_process(
                &context,
                COLLISION_OPERATION_TIMEOUT,
                client,
                spec.helper_operation,
                spec.path,
                spec.target,
                spec.payload,
                pending_label,
            )
            .map_err(scenario_error_to_io)
        },
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct NamespaceCollisionSummary {
    pub(in crate::scenario) scenario: String,
    pub(in crate::scenario) bootstrap: Option<VolumeBootstrapSummary>,
    pub(in crate::scenario) clients: usize,
    pub(in crate::scenario) operations: Vec<NamespaceCollisionOperation>,
    pub(in crate::scenario) operation_log: String,
    pub(in crate::scenario) final_validator: TreeComparison,
    pub(in crate::scenario) namespace_violations: Vec<NamespaceInvariantViolation>,
}

pub(in crate::scenario) fn run_namespace_collision_with_driver(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    driver: &mut dyn NamespaceCollisionDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    let result =
        run_namespace_collision_body(context, kind, driver, convergence_timeout, poll_interval);
    match result {
        Ok(summary) => cleanup_namespace_collision_driver(context, kind, driver).map(|()| summary),
        Err(error) if namespace_collision_error_requires_emergency_cleanup(&error) => {
            match driver.emergency_kill_all_no_wait("namespace collision timeout") {
                Ok(()) => Err(error),
                Err(cleanup) => Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                    primary: error.to_string(),
                    cleanup: cleanup.to_string(),
                }),
            }
        }
        Err(error) => match cleanup_namespace_collision_driver(context, kind, driver) {
            Ok(()) => Err(error),
            Err(cleanup) => Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                primary: error.to_string(),
                cleanup: cleanup.to_string(),
            }),
        },
    }
}

fn cleanup_namespace_collision_driver(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    driver: &mut dyn NamespaceCollisionDriver,
) -> Result<(), ScenarioError> {
    match driver.unmount_all() {
        Ok(()) => release_completed_hotspot_helpers_for_kind(context, kind),
        Err(unmount) => match driver.emergency_kill_all_no_wait("namespace collision unmount") {
            Ok(()) => Err(unmount),
            Err(emergency) => Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                primary: unmount.to_string(),
                cleanup: format!("emergency teardown failed: {emergency}"),
            }),
        },
    }
}

fn release_completed_hotspot_helpers_for_kind(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
) -> Result<(), ScenarioError> {
    if kind == NamespaceCollisionKind::UnlinkVsAppend {
        release_completed_hotspot_helpers(context).map_err(ScenarioError::Io)
    } else {
        Ok(())
    }
}

pub(in crate::scenario) fn run_namespace_collision_body(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    driver: &mut dyn NamespaceCollisionDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: COLLISION_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < COLLISION_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientCollisionClients {
            expected: COLLISION_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }
    let clients = clients
        .into_iter()
        .take(COLLISION_CLIENT_NAMES.len())
        .collect::<Vec<_>>();

    establish_collision_initial_tree(context, kind, &clients[0])?;
    driver.after_conflicting_operations(kind, &clients)?;
    let initial_validation_deadline = Instant::now() + convergence_timeout;
    wait_for_namespace_collision_initial_state_until(
        context,
        kind,
        &clients,
        initial_validation_deadline,
        poll_interval,
    )?;
    let operations = match perform_namespace_collision_operations(context, kind, &clients) {
        Ok(operations) => operations,
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
            return handle_namespace_collision_operation_timeout(error, driver);
        }
        Err(error) => return Err(error),
    };
    driver.after_conflicting_operations(kind, &clients)?;
    let summary = finish_namespace_collision(
        context,
        kind,
        Some(bootstrap),
        clients,
        operations,
        convergence_timeout,
        poll_interval,
    )?;
    Ok(summary)
}

pub(in crate::scenario) fn handle_namespace_collision_operation_timeout<T>(
    error: ScenarioError,
    driver: &mut dyn NamespaceCollisionDriver,
) -> Result<T, ScenarioError> {
    handle_namespace_collision_timeout_error(error, driver)
}

pub(in crate::scenario) fn namespace_collision_error_requires_emergency_cleanup(
    error: &ScenarioError,
) -> bool {
    matches!(
        error,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
            | ScenarioError::MountedIoTimeout { .. }
            | ScenarioError::MountedIoWorkerLimit { .. }
    ) || matches!(
        error,
        ScenarioError::MountedTree(source)
            if mounted_tree_error_requires_emergency_cleanup(source)
    )
}

fn mounted_tree_error_requires_emergency_cleanup(error: &MountedTreeError) -> bool {
    match error {
        MountedTreeError::SnapshotAttemptTimedOut { .. } => true,
        MountedTreeError::SnapshotUnavailableTimedOut { source, .. } => {
            mounted_tree_error_requires_emergency_cleanup(source)
        }
        _ => false,
    }
}

pub(in crate::scenario) fn handle_namespace_collision_timeout_error<T>(
    error: ScenarioError,
    driver: &mut dyn NamespaceCollisionDriver,
) -> Result<T, ScenarioError> {
    match driver.emergency_kill_all_no_wait("namespace collision timeout") {
        Ok(()) => Err(error),
        Err(cleanup) => Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
            primary: error.to_string(),
            cleanup: cleanup.to_string(),
        }),
    }
}

pub(in crate::scenario) fn write_namespace_collision_plan_artifacts(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let operations = planned_namespace_collision_operations(kind, started);
    write_namespace_collision_operations(context, &operations)?;
    let summary = NamespaceCollisionSummary {
        scenario: kind.name().to_string(),
        bootstrap: None,
        clients: COLLISION_CLIENT_NAMES.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some(COLLISION_CLIENT_NAMES[0].to_string()),
            compared_clients: COLLISION_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
    };
    write_namespace_collision_summary(context, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn establish_collision_initial_tree(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    if kind == NamespaceCollisionKind::MkdirRmdirVsCreate {
        run_hotspot_helper_process(
            context,
            COLLISION_OPERATION_TIMEOUT,
            client,
            "mkdir",
            "race",
            None,
            None,
            format!("{} seed mkdir race", client.name),
        )?;
    }
    for (relative_path, payload) in kind.initial_paths() {
        run_hotspot_helper_process(
            context,
            COLLISION_OPERATION_TIMEOUT,
            client,
            "create",
            relative_path,
            None,
            Some(payload),
            format!("{} seed create {}", client.name, relative_path),
        )?;
    }
    Ok(())
}

pub(in crate::scenario) fn wait_for_namespace_collision_initial_state_until(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    clients: &[ClientSpec],
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    loop {
        let missing = namespace_collision_initial_state_missing(context, kind, clients)?;
        if missing.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "namespace collision initial tree did not become visible before timeout; missing: {}",
                    missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn namespace_collision_initial_state_missing(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    clients: &[ClientSpec],
) -> Result<Vec<String>, ScenarioError> {
    let mut missing = Vec::new();
    let seed_client = clients.first().map(|client| client.name.as_str());
    for client in clients {
        for (relative_path, payload) in kind.initial_paths() {
            if Some(client.name.as_str()) == seed_client {
                continue;
            }
            match run_hotspot_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                client,
                "read-exact",
                relative_path,
                None,
                Some(payload),
                format!("{} validate initial payload {}", client.name, relative_path),
            ) {
                Ok(()) => {}
                Err(error) if namespace_collision_error_requires_emergency_cleanup(&error) => {
                    return Err(error);
                }
                Err(error) => {
                    missing.push(format!("{}:{} ({})", client.name, relative_path, error))
                }
            }
        }
        for relative_path in collision_required_initial_paths(kind) {
            match run_hotspot_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                client,
                "stat",
                relative_path,
                None,
                None,
                format!("{} validate initial path {}", client.name, relative_path),
            ) {
                Ok(()) => {}
                Err(error) if namespace_collision_error_requires_emergency_cleanup(&error) => {
                    return Err(error);
                }
                Err(error) => {
                    missing.push(format!("{}:{} ({})", client.name, relative_path, error))
                }
            }
        }
    }
    Ok(missing)
}

pub(in crate::scenario) fn collision_required_initial_paths(
    kind: NamespaceCollisionKind,
) -> Vec<&'static str> {
    if kind == NamespaceCollisionKind::MkdirRmdirVsCreate {
        vec!["race"]
    } else {
        Vec::new()
    }
}

pub(in crate::scenario) fn perform_namespace_collision_operations(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    clients: &[ClientSpec],
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let operations = match kind {
        NamespaceCollisionKind::RenameVsRename => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "shared/file.txt",
                        target: Some("shared/renamed-a.txt"),
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "shared/file.txt",
                        target: Some("shared/renamed-b.txt"),
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::RenameVsUnlink => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "shared/file.txt",
                        target: Some("shared/renamed.txt"),
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "unlink",
                        helper_operation: "delete",
                        path: "shared/file.txt",
                        target: None,
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::UnlinkVsAppend => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "unlink",
                        helper_operation: "delete",
                        path: "shared/data.txt",
                        target: None,
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "append",
                        helper_operation: "append",
                        path: "shared/data.txt",
                        target: None,
                        payload: Some(b"appended payload\n"),
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::OverwriteVsRead => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[1].clone(),
                    "read_loop",
                    "shared/data.txt",
                    |client| repeated_committed_read(client, "shared/data.txt"),
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[0].clone(),
                        operation: "overwrite",
                        helper_operation: "overwrite-barrier",
                        path: "shared/data.txt",
                        target: None,
                        payload: Some(COLLISION_NEW_COMMITTED_PAYLOAD),
                    },
                ),
                NamespaceCollisionTask::new(
                    3,
                    clients[2].clone(),
                    "read_loop",
                    "shared/data.txt",
                    |client| repeated_committed_read(client, "shared/data.txt"),
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::MkdirRmdirVsCreate => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "mkdir",
                        helper_operation: "mkdir",
                        path: "race/subdir",
                        target: None,
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "create",
                        helper_operation: "create",
                        path: "race/child.txt",
                        target: None,
                        payload: Some(b"child\n"),
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 3,
                        client: clients[2].clone(),
                        operation: "rmdir",
                        helper_operation: "rmdir",
                        path: "race",
                        target: None,
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::ReaddirStatDuringMutation => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[1].clone(),
                        operation: "readdir",
                        helper_operation: "readdir",
                        path: "scan",
                        target: None,
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[0].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "scan/source-alpha.data",
                        target: Some("scan/renamed-alpha.data"),
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 3,
                        client: clients[2].clone(),
                        operation: "stat",
                        helper_operation: "stat",
                        path: "scan/source-b.txt",
                        target: None,
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
    }?;
    write_namespace_collision_operations(context, &operations)?;
    Ok(operations)
}

pub(in crate::scenario) fn run_concurrent_namespace_operations(
    started: Instant,
    tasks: Vec<NamespaceCollisionTask>,
    timeout: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let barrier = Arc::new(Barrier::new(tasks.len()));
    let (tx, rx) = mpsc::channel();
    let mut pending = tasks
        .iter()
        .map(|task| (task.order, namespace_collision_task_label(task)))
        .collect::<Vec<_>>();
    #[cfg(not(test))]
    let batch_permit = Arc::new(NamespaceCollisionWorkerBatchPermit::try_acquire(
        timeout, &pending,
    )?);
    let expected_operations = pending.len();
    let completion_grace = tasks
        .iter()
        .map(|task| task.completion_grace)
        .max()
        .unwrap_or(Duration::ZERO);
    let mut handles = Vec::with_capacity(tasks.len());
    for task in tasks {
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        #[cfg(not(test))]
        let batch_permit = Arc::clone(&batch_permit);
        handles.push(thread::spawn(move || {
            #[cfg(not(test))]
            let _batch_permit = batch_permit;
            barrier.wait();
            let operation = record_fs_operation(
                started,
                task.order,
                &task.client,
                task.operation,
                task.path,
                || (task.action)(&task.client),
            );
            let _ = tx.send(operation);
        }));
    }
    drop(tx);

    let deadline = Instant::now() + timeout + completion_grace;
    let mut operations = Vec::with_capacity(expected_operations);
    while operations.len() < expected_operations {
        let now = Instant::now();
        if now >= deadline {
            return Err(namespace_collision_operation_timeout(timeout, pending));
        }
        match rx.recv_timeout(deadline.saturating_duration_since(now)) {
            Ok(operation) => {
                pending.retain(|(order, _)| *order != operation.order);
                operations.push(operation);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(namespace_collision_operation_timeout(timeout, pending));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(ScenarioError::Io(std::io::Error::other(
                    "namespace collision worker channel closed before all operations completed",
                )));
            }
        }
    }

    for handle in handles {
        handle
            .join()
            .expect("namespace collision worker thread should not panic");
    }
    operations.sort_by_key(|operation| operation.order);
    Ok(operations)
}

pub(in crate::scenario) fn namespace_collision_operation_timeout(
    timeout: Duration,
    pending: Vec<(usize, String)>,
) -> ScenarioError {
    ScenarioError::NamespaceCollisionOperationTimeout {
        timeout_ms: timeout.as_millis(),
        pending: pending.into_iter().map(|(_, label)| label).collect(),
    }
}

pub(in crate::scenario) fn namespace_collision_task_label(task: &NamespaceCollisionTask) -> String {
    format!("{} {} {}", task.client.name, task.operation, task.path)
}

pub(in crate::scenario) fn repeated_committed_read(
    client: &ClientSpec,
    relative_path: &str,
) -> io::Result<()> {
    for _ in 0..OVERWRITE_READ_ATTEMPTS {
        let path = client.mount_point.join(relative_path);
        let payload = bounded_mounted_read(
            path.clone(),
            format!("namespace collision repeated read {}", path.display()),
            COLLISION_OPERATION_TIMEOUT,
        )
        .map_err(scenario_error_to_io)?;
        if payload != COLLISION_OLD_COMMITTED_PAYLOAD && payload != COLLISION_NEW_COMMITTED_PAYLOAD
        {
            return Err(io::Error::other(format!(
                "read observed partial or unexpected payload for {relative_path}"
            )));
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

pub(in crate::scenario) fn planned_namespace_collision_operations(
    kind: NamespaceCollisionKind,
    started: Instant,
) -> Vec<NamespaceCollisionOperation> {
    let spec = match kind {
        NamespaceCollisionKind::RenameVsRename => vec![
            (
                "client-a",
                "rename",
                "shared/file.txt -> shared/renamed-a.txt",
            ),
            (
                "client-b",
                "rename",
                "shared/file.txt -> shared/renamed-b.txt",
            ),
        ],
        NamespaceCollisionKind::RenameVsUnlink => vec![
            (
                "client-a",
                "rename",
                "shared/file.txt -> shared/renamed.txt",
            ),
            ("client-b", "unlink", "shared/file.txt"),
        ],
        NamespaceCollisionKind::UnlinkVsAppend => vec![
            ("client-a", "unlink", "shared/data.txt"),
            ("client-b", "append", "shared/data.txt"),
        ],
        NamespaceCollisionKind::OverwriteVsRead => vec![
            ("client-b", "read_loop", "shared/data.txt"),
            ("client-a", "overwrite", "shared/data.txt"),
            ("client-c", "read_loop", "shared/data.txt"),
        ],
        NamespaceCollisionKind::MkdirRmdirVsCreate => vec![
            ("client-a", "mkdir", "race/subdir"),
            ("client-b", "create", "race/child.txt"),
            ("client-c", "rmdir", "race"),
        ],
        NamespaceCollisionKind::ReaddirStatDuringMutation => vec![
            ("client-b", "readdir", "scan"),
            (
                "client-a",
                "rename",
                "scan/source-alpha.data -> scan/renamed-alpha.data",
            ),
            ("client-c", "stat", "scan/source-b.txt"),
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

pub(in crate::scenario) fn record_fs_operation(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    f: impl FnOnce() -> std::io::Result<()>,
) -> NamespaceCollisionOperation {
    match f() {
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
            error: Some(error.to_string()),
        },
    }
}

pub(in crate::scenario) fn elapsed_millis_u64(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(in crate::scenario) fn elapsed_millis_nonzero(started: Instant) -> u128 {
    started.elapsed().as_millis().max(1)
}

pub(in crate::scenario) fn finish_namespace_collision(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    operations: Vec<NamespaceCollisionOperation>,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    if kind == NamespaceCollisionKind::ReaddirStatDuringMutation {
        return finish_readdir_stat_namespace_collision(
            context,
            kind,
            bootstrap,
            clients,
            operations,
            convergence_timeout,
            poll_interval,
        );
    }

    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(
            snapshot,
            kind.expected_absent_paths(),
        ));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::NamespaceInvariantViolations {
            violations: namespace_violations,
        });
    }
    let summary = NamespaceCollisionSummary {
        scenario: kind.name().to_string(),
        bootstrap,
        clients: clients.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: convergence.comparison,
        namespace_violations,
    };
    write_namespace_collision_summary(context, &summary)?;
    Ok(summary)
}

fn finish_readdir_stat_namespace_collision(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    operations: Vec<NamespaceCollisionOperation>,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    wait_for_readdir_stat_final_state_until(
        &clients,
        Instant::now() + convergence_timeout,
        poll_interval,
    )?;
    let compared_clients = clients
        .iter()
        .map(|client| client.name.clone())
        .collect::<Vec<_>>();
    let summary = NamespaceCollisionSummary {
        scenario: kind.name().to_string(),
        bootstrap,
        clients: clients.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: TreeComparison {
            converged: true,
            reference_client: clients.first().map(|client| client.name.clone()),
            compared_clients,
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
    };
    write_namespace_collision_summary(context, &summary)?;
    Ok(summary)
}

fn wait_for_readdir_stat_final_state_until(
    clients: &[ClientSpec],
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    loop {
        let missing = readdir_stat_final_state_missing(clients)?;
        if missing.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "namespace collision readdir/stat final state did not converge before timeout; missing: {}",
                    missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn readdir_stat_final_state_missing(clients: &[ClientSpec]) -> Result<Vec<String>, ScenarioError> {
    let mut missing = Vec::new();
    for client in clients {
        assert_readdir_stat_payload(client, "scan/renamed-alpha.data", b"a\n", &mut missing)?;
        assert_readdir_stat_payload(client, "scan/source-b.txt", b"b\n", &mut missing)?;
        assert_readdir_stat_absent(client, "scan/source-alpha.data", &mut missing)?;
    }
    Ok(missing)
}

fn assert_readdir_stat_payload(
    client: &ClientSpec,
    relative_path: &str,
    expected: &[u8],
    missing: &mut Vec<String>,
) -> Result<(), ScenarioError> {
    let path = client.mount_point.join(relative_path);
    let payload = match bounded_mounted_read(
        path,
        format!("{} validate final payload {}", client.name, relative_path),
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
    ) {
        Ok(payload) => payload,
        Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            missing.push(format!("{}:{} (missing)", client.name, relative_path));
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if payload != expected {
        missing.push(format!(
            "{}:{} ({})",
            client.name,
            relative_path,
            payload_mismatch_detail(expected, &payload)
        ));
    }
    Ok(())
}

fn assert_readdir_stat_absent(
    client: &ClientSpec,
    relative_path: &str,
    missing: &mut Vec<String>,
) -> Result<(), ScenarioError> {
    let path = client.mount_point.join(relative_path);
    match bounded_mounted_metadata(
        path,
        format!("{} validate final absence {}", client.name, relative_path),
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
    ) {
        Ok(_) => missing.push(format!("{}:{} (still present)", client.name, relative_path)),
        Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

pub(in crate::scenario) fn write_namespace_collision_operations(
    context: &ScenarioContext,
    operations: &[NamespaceCollisionOperation],
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(false)
        .truncate(true)
        .write(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    for operation in operations {
        serde_json::to_writer(&mut log, operation)?;
        log.write_all(b"\n")?;
    }
    Ok(())
}

pub(in crate::scenario) fn append_namespace_collision_operation(
    context: &ScenarioContext,
    operation: &NamespaceCollisionOperation,
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    serde_json::to_writer(&mut log, operation)?;
    log.write_all(b"\n")?;
    Ok(())
}

pub(in crate::scenario) fn record_sparse_operation(
    context: &ScenarioContext,
    operations: &mut Vec<NamespaceCollisionOperation>,
    operation: NamespaceCollisionOperation,
) -> Result<(), ScenarioError> {
    append_namespace_collision_operation(context, &operation)?;
    operations.push(operation);
    Ok(())
}

#[cfg(test)]
pub(in crate::scenario) fn record_required_sparse_operation(
    context: &ScenarioContext,
    operations: &mut Vec<NamespaceCollisionOperation>,
    operation: NamespaceCollisionOperation,
) -> Result<(), ScenarioError> {
    let required = required_record(operation.clone()).map(|_| ());
    record_sparse_operation(context, operations, operation)?;
    required
}

pub(in crate::scenario) fn write_namespace_collision_summary(
    context: &ScenarioContext,
    summary: &NamespaceCollisionSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("namespace-collision-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_local_durable_payload(
    path: PathBuf,
    payload: &[u8],
) -> Result<(), ScenarioError> {
    let write_payload = payload.to_vec();
    run_bounded_mounted_io(
        format!("write local durable payload {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
            file.write_all(&write_payload)?;
            file.sync_all()
        },
    )
}

pub(in crate::scenario) fn crash_restart_mode_slug(mode: CrashRestartMode) -> &'static str {
    match mode {
        CrashRestartMode::AfterLocalDurabilityBoundary => "after-local-durability-boundary",
        CrashRestartMode::BeforeRemotePublicationCompletes => "before-remote-publication-completes",
    }
}

pub(in crate::scenario) fn bytes_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}
