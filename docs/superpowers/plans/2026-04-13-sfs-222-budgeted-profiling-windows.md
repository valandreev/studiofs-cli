# SFS-222 Budgeted Profiling Windows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded profiling windows to live diagnostics runs so a controller can start and stop wall/blocking-first and CPU captures under explicit presets and budgets without restarting the daemon.

**Architecture:** Extend the existing diagnostics host with a controller-gated profiling coordinator that owns bounded capture windows and persists capture metadata into investigation runs. Reuse the current mount profiling backend as a lower-level capture primitive, but move session lifecycle control behind the diagnostics protocol so profiling becomes part of attach and run workflows instead of a mount-start side effect.

**Tech Stack:** Rust 2024, Tokio, serde, parking_lot, existing diagnostics IPC, investigation store persistence, Pyroscope/pprof backend on Unix, stubbed fail-open behavior on unsupported platforms.

---

### Task 1: Persist Profiling Window Metadata

**Files:**
- Modify: `crates/studiofs/src/investigation.rs`
- Test: `crates/studiofs/src/investigation.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn append_profiling_window_persists_capture_metadata() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = InvestigationStore::new(temp.path().join("store"));
    let case = store
        .create_case(NewInvestigationCase {
            title: "profiling".to_string(),
            problem_statement: None,
            workload_identity: None,
            target_environment: None,
        })
        .expect("create case");
    let run = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-1".to_string(),
            fingerprint: sample_environment_fingerprint(),
        })
        .expect("create run");

    let window = store
        .append_profiling_window(
            &run.id,
            NewProfilingWindow {
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                requested_duration_ms: 1_500,
                actual_duration_ms: Some(1_250),
                sample_rate_hz: Some(97),
                estimated_cost: ProfilingCostEstimate {
                    score: 2,
                    label: "balanced".to_string(),
                },
                status: ProfilingWindowStatus::Completed,
                artifact: Some(ProfileArtifactRecord {
                    kind: "pprof".to_string(),
                    path: "profiles/demo.pb.gz".to_string(),
                    bytes: 1024,
                }),
                error: None,
            },
        )
        .expect("append profiling window");

    assert_eq!(window.sequence, 1);
    let reloaded = store.load_run(&run.id).expect("reload run");
    assert_eq!(reloaded.profiling_windows.len(), 1);
    assert_eq!(
        reloaded.profiling_windows[0].capture_mode,
        ProfilingCaptureMode::WallBlocking
    );
}

#[test]
fn append_profiling_window_keeps_fail_open_failure_metadata() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = InvestigationStore::new(temp.path().join("store"));
    let case = store
        .create_case(NewInvestigationCase {
            title: "profiling".to_string(),
            problem_statement: None,
            workload_identity: None,
            target_environment: None,
        })
        .expect("create case");
    let run = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-1".to_string(),
            fingerprint: sample_environment_fingerprint(),
        })
        .expect("create run");

    let window = store
        .append_profiling_window(
            &run.id,
            NewProfilingWindow {
                capture_mode: ProfilingCaptureMode::Cpu,
                preset: DiagnosticsPreset::Minimal,
                requested_duration_ms: 5_000,
                actual_duration_ms: None,
                sample_rate_hz: Some(100),
                estimated_cost: ProfilingCostEstimate {
                    score: 1,
                    label: "minimal".to_string(),
                },
                status: ProfilingWindowStatus::Unsupported,
                artifact: None,
                error: Some("profiling backend unavailable".to_string()),
            },
        )
        .expect("append profiling window");

    assert_eq!(window.status, ProfilingWindowStatus::Unsupported);
    assert_eq!(
        window.error.as_deref(),
        Some("profiling backend unavailable")
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p studiofs append_profiling_window --lib`
Expected: FAIL with missing profiling window types or store methods.

- [ ] **Step 3: Write the minimal implementation**

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsPreset {
    Minimal,
    Balanced,
    Deep,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfilingCaptureMode {
    WallBlocking,
    Cpu,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfilingWindowStatus {
    Running,
    Completed,
    Failed,
    Unsupported,
}
```

```rust
pub(crate) struct NewProfilingWindow {
    pub capture_mode: ProfilingCaptureMode,
    pub preset: DiagnosticsPreset,
    pub requested_duration_ms: u64,
    pub actual_duration_ms: Option<u64>,
    pub sample_rate_hz: Option<u32>,
    pub estimated_cost: ProfilingCostEstimate,
    pub status: ProfilingWindowStatus,
    pub artifact: Option<ProfileArtifactRecord>,
    pub error: Option<String>,
}
```

```rust
pub(crate) fn append_profiling_window(
    &self,
    run_id: &str,
    input: NewProfilingWindow,
) -> Result<ProfilingWindowRecord> {
    let _guard = self.io_lock.lock();
    let _store_lock = self.lock_store()?;
    self.ensure_layout()?;

    let mut run = self.load_json::<InvestigationRun>(&self.run_path(run_id))?;
    let sequence = run
        .profiling_windows
        .last()
        .map(|window| window.sequence + 1)
        .unwrap_or(1);
    let record = ProfilingWindowRecord::from_new(sequence, input);
    run.profiling_windows.push(record.clone());
    self.save_json(&self.run_path(run_id), &run)?;
    Ok(record)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p studiofs append_profiling_window --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/investigation.rs
git commit -m "feat: persist profiling window metadata"
```

### Task 2: Add Diagnostics Protocol Coverage for Profiling Windows

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/diagnostics.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn diagnostics_client_can_start_and_stop_profiling_window() {
    let fixture = diagnostics_test_fixture().await;
    let lease = fixture
        .client
        .acquire_controller("test-client", Duration::from_secs(1))
        .await
        .expect("lease");

    let started = fixture
        .client
        .start_profiling_window(
            &lease.lease_id,
            &fixture.run_id,
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                budget: ProfilingBudget {
                    duration_ms: 500,
                    sample_rate_hz: Some(99),
                },
            },
        )
        .await
        .expect("start window");
    assert_eq!(started.status, ProfilingWindowStatus::Running);

    let stopped = fixture
        .client
        .stop_profiling_window(&lease.lease_id, &started.window_id)
        .await
        .expect("stop window");
    assert_eq!(stopped.status, ProfilingWindowStatus::Completed);
    assert!(stopped.actual_duration_ms.is_some());
}

#[tokio::test]
async fn diagnostics_rejects_profiling_budget_above_limit() {
    let fixture = diagnostics_test_fixture().await;
    let lease = fixture
        .client
        .acquire_controller("test-client", Duration::from_secs(1))
        .await
        .expect("lease");

    let err = fixture
        .client
        .start_profiling_window(
            &lease.lease_id,
            &fixture.run_id,
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::Cpu,
                preset: DiagnosticsPreset::Minimal,
                budget: ProfilingBudget {
                    duration_ms: MAX_PROFILE_WINDOW_MS + 1,
                    sample_rate_hz: Some(100),
                },
            },
        )
        .await
        .expect_err("budget should be rejected");

    assert!(err.to_string().contains("profiling budget exceeds limit"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p studiofs diagnostics_client_can_start_and_stop_profiling_window diagnostics_rejects_profiling_budget_above_limit --lib`
Expected: FAIL with missing diagnostics request variants or client methods.

- [ ] **Step 3: Write the minimal implementation**

```rust
enum DiagnosticsRequest {
    // ...
    StartProfilingWindow {
        auth_token: String,
        lease_id: String,
        run_id: String,
        request: StartProfilingWindowRequest,
    },
    StopProfilingWindow {
        auth_token: String,
        lease_id: String,
        window_id: String,
    },
    ProfilingStatus {
        auth_token: String,
    },
}
```

```rust
pub(crate) async fn start_profiling_window(
    &self,
    lease_id: &str,
    run_id: &str,
    request: StartProfilingWindowRequest,
) -> Result<ProfilingWindowRecord> { /* send request */ }
```

```rust
let profiling = Arc::new(DiagnosticsProfilingCoordinator::new(source.clone()));
match request {
    DiagnosticsRequest::StartProfilingWindow { lease_id, run_id, request, .. } => {
        require_mutating_request_prereqs(&auth_token, &lease_id, &lease_manager, &supplied_auth)?;
        let window = profiling.start_window(&run_id, request, investigation_store.as_deref())?;
        DiagnosticsResponse::ProfilingWindow { window: Box::new(window) }
    }
    DiagnosticsRequest::StopProfilingWindow { lease_id, window_id, .. } => {
        require_mutating_request_prereqs(&auth_token, &lease_id, &lease_manager, &supplied_auth)?;
        let window = profiling.stop_window(&window_id, investigation_store.as_deref())?;
        DiagnosticsResponse::ProfilingWindow { window: Box::new(window) }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p studiofs diagnostics_client_can_start_and_stop_profiling_window diagnostics_rejects_profiling_budget_above_limit --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/diagnostics.rs crates/studiofs/src/investigation.rs
git commit -m "feat: add diagnostics profiling window protocol"
```

### Task 3: Reuse Mount Profiling Backend Through a Runtime Coordinator

**Files:**
- Modify: `crates/studiofs/src/cli/mount_profile.rs`
- Modify: `crates/studiofs/src/cli/mount_profile_stub.rs`
- Modify: `crates/studiofs/src/cli/mount_profile_types.rs`
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/diagnostics.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn profiling_coordinator_uses_fail_open_result_when_backend_is_unsupported() {
    let coordinator = DiagnosticsProfilingCoordinator::new(
        Arc::new(StaticDiagnosticsSource::default()),
        Arc::new(FailingProfilingBackend::unsupported("missing backend")),
    );

    let window = coordinator
        .start_window(
            "run-1",
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::Cpu,
                preset: DiagnosticsPreset::Minimal,
                budget: ProfilingBudget {
                    duration_ms: 500,
                    sample_rate_hz: Some(100),
                },
            },
            None,
        )
        .expect("fail-open should still return metadata");

    assert_eq!(window.status, ProfilingWindowStatus::Unsupported);
    assert_eq!(window.error.as_deref(), Some("missing backend"));
}

#[test]
fn profiling_coordinator_enforces_single_active_window() {
    let coordinator = diagnostics_test_coordinator();
    let first = coordinator
        .start_window("run-1", balanced_wall_blocking_request(), None)
        .expect("start first");

    let err = coordinator
        .start_window("run-1", balanced_wall_blocking_request(), None)
        .expect_err("second concurrent window should be rejected");

    assert!(err.to_string().contains("profiling window already active"));
    let _ = coordinator.stop_window(&first.window_id, None).expect("stop first");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p studiofs profiling_coordinator --lib`
Expected: FAIL with missing coordinator or backend trait.

- [ ] **Step 3: Write the minimal implementation**

```rust
pub(crate) trait RuntimeProfilingBackend: Send + Sync + 'static {
    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult;
    fn stop_window(&self, active: ActiveProfilingWindow) -> ProfilingBackendStopResult;
}
```

```rust
pub(crate) struct DiagnosticsProfilingCoordinator {
    active_window: Mutex<Option<ActiveProfilingWindow>>,
    backend: Arc<dyn RuntimeProfilingBackend>,
}
```

```rust
impl RuntimeProfilingBackend for MountProfileRuntimeBackend {
    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        // Build a bounded profile session using the existing Pyroscope/pprof code.
    }

    fn stop_window(&self, active: ActiveProfilingWindow) -> ProfilingBackendStopResult {
        // Finish the profile, emit artifact metadata, and return fail-open warnings if export fails.
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p studiofs profiling_coordinator --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/cli/mount_profile.rs crates/studiofs/src/cli/mount_profile_stub.rs crates/studiofs/src/cli/mount_profile_types.rs crates/studiofs/src/diagnostics.rs
git commit -m "feat: reuse mount profiling backend for bounded windows"
```

### Task 4: Cover End-to-End Attach Flow and Investigation Metadata

**Files:**
- Modify: `crates/studiofs/src/cli/mount/session.rs`
- Test: `crates/studiofs/src/cli/mount/session.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn mount_session_attach_flow_records_budgeted_profiling_windows() {
    let _guard = DIAGNOSTICS_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock diagnostics test mutex");
    let temp = tempfile::tempdir().expect("tempdir");
    let store_dir = temp.path().join("investigations-store");
    set_test_store_base_dir(Some(&store_dir));

    let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
    drop(engine);
    let persisted_run_id = Arc::new(Mutex::new(None::<String>));
    let inspect: BackendInspect = Arc::new(move |launch| {
        let discovery = diagnostics::discover_local_sessions()
            .expect("discover local sessions")
            .into_iter()
            .find(|session| session.mount_point == launch.mount_point)
            .expect("live diagnostics session for mounted volume");
        let client = diagnostics::LocalDiagnosticsClient::new(discovery);
        let lease = launch
            .runtime_handle
            .block_on(client.acquire_controller("test-client", Duration::from_secs(1)))
            .expect("lease");
        let case = launch
            .runtime_handle
            .block_on(client.create_case(&lease.lease_id, "AJA copy slowdown", None, None, None))
            .expect("create case");
        let run = launch
            .runtime_handle
            .block_on(client.create_run(&lease.lease_id, &case.id, Some("copy-pass-1".to_string())))
            .expect("create run");
        let started = launch
            .runtime_handle
            .block_on(client.start_profiling_window(
                &lease.lease_id,
                &run.id,
                balanced_wall_blocking_request(),
            ))
            .expect("start window");
        let stopped = launch
            .runtime_handle
            .block_on(client.stop_profiling_window(&lease.lease_id, &started.window_id))
            .expect("stop window");
        assert_eq!(stopped.status, ProfilingWindowStatus::Completed);
        *persisted_run_id.lock().expect("lock persisted run id") = Some(run.id);
        Ok(())
    });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p studiofs mount_session_attach_flow_records_budgeted_profiling_windows --lib`
Expected: FAIL because attach flow does not yet expose profiling windows.

- [ ] **Step 3: Write the minimal implementation**

```rust
let profiling_backend = mount_profile::build_runtime_backend(
    command.profile,
    name,
    platform,
    bridge,
    mount_mode,
    cache_mode,
    cache_path,
);
let diagnostics_source: Arc<dyn DiagnosticsDataSource> = Arc::new(
    LiveMountDiagnosticsSource::new(/* existing args */)
        .with_runtime_profiling(profiling_backend),
);
```

```rust
let persisted_run = InvestigationStore::new(store_dir)
    .load_run(&run_id)
    .expect("load run after shutdown");
assert_eq!(persisted_run.profiling_windows.len(), 1);
assert_eq!(
    persisted_run.profiling_windows[0].preset,
    DiagnosticsPreset::Balanced
);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p studiofs mount_session_attach_flow_records_budgeted_profiling_windows --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/cli/mount/session.rs crates/studiofs/src/diagnostics.rs crates/studiofs/src/investigation.rs
git commit -m "feat: wire profiling windows into live attach flow"
```

### Task 5: Verify, Format, and Prepare Final Diff

**Files:**
- Modify: any files changed by previous tasks

- [ ] **Step 1: Run focused Rust tests**

Run: `cargo test -p studiofs append_profiling_window diagnostics_client_can_start_and_stop_profiling_window diagnostics_rejects_profiling_budget_above_limit profiling_coordinator mount_session_attach_flow_records_budgeted_profiling_windows --lib`
Expected: PASS

- [ ] **Step 2: Run formatting**

Run: `cargo fmt --all`
Expected: formatter completes without errors

- [ ] **Step 3: Re-run focused tests after formatting**

Run: `cargo test -p studiofs append_profiling_window diagnostics_client_can_start_and_stop_profiling_window diagnostics_rejects_profiling_budget_above_limit profiling_coordinator mount_session_attach_flow_records_budgeted_profiling_windows --lib`
Expected: PASS

- [ ] **Step 4: Review changed files**

Run: `git diff -- crates/studiofs/src/investigation.rs crates/studiofs/src/diagnostics.rs crates/studiofs/src/cli/mount_profile.rs crates/studiofs/src/cli/mount_profile_stub.rs crates/studiofs/src/cli/mount_profile_types.rs crates/studiofs/src/cli/mount/session.rs docs/superpowers/plans/2026-04-13-sfs-222-budgeted-profiling-windows.md`
Expected: diff only contains profiling-window design and implementation changes

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: add budgeted profiling windows for live investigations"
```
