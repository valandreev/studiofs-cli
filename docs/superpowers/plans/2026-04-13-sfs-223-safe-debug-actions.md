# SFS-223 Safe Debug Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add controller-safe diagnostics actions for bounded log verbosity changes, capture bundles, counter resets, and lightweight self-checks with loud-action timeline auditing and fail-open behavior.

**Architecture:** Extend the existing diagnostics IPC in `crates/studiofs/src/diagnostics.rs` with a typed safe-action request/response pair and execute actions inside the live diagnostics host so controller authorization, persistence, and fail-open handling stay centralized. Reuse the existing investigation store for audit entries, add a reloadable tracing filter in `crates/studiofs/src/main.rs`, and expose small VFS/cache reset helpers needed by representative loud actions.

**Tech Stack:** Rust 2024, Tokio, serde, tracing-subscriber reload filters, existing diagnostics IPC, investigation persistence, StudioVfs runtime snapshots/events, cargo test.

---

### Task 1: Define the safe-action protocol and timeline audit shape

**Files:**
- Modify: `crates/studiofs/src/investigation.rs`
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/investigation.rs`
- Test: `crates/studiofs/src/diagnostics.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn loud_safe_action_audit_entries_round_trip_with_action_kind() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let case = store
        .create_case(NewInvestigationCase {
            title: "safe action".to_string(),
            problem_statement: None,
            workload_identity: None,
            target_environment: None,
        })
        .expect("create case");
    let run = store
        .create_run(NewInvestigationRun {
            case_id: case.id,
            session_id: "session-1".to_string(),
            fingerprint: sample_fingerprint(),
        })
        .expect("create run");

    let entry = store
        .append_timeline_entry(
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Mcp,
                kind: TimelineEntryKind::Action,
                title: "reset_counters".to_string(),
                body: Some("reset upload/cache counters".to_string()),
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("baseline before rerun".to_string()),
            },
        )
        .expect("append action");

    assert_eq!(entry.kind, TimelineEntryKind::Action);
}

#[test]
fn mutating_safe_action_requests_require_current_controller_lease() {
    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: "lease-1".to_string(),
            run_id: "run-1".to_string(),
            action: SafeDebugActionRequest::ResetObservabilityCounters {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("baseline".to_string()),
            },
        },
        "secret",
        &static_source(),
        &ControllerLeaseManager::default(),
        None,
        None,
    );

    assert!(matches!(
        response,
        DiagnosticsResponse::Error { code, .. } if code == "lease_invalid"
    ));
}
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cargo test -p studiofs loud_safe_action_audit_entries_round_trip_with_action_kind mutating_safe_action_requests_require_current_controller_lease`

Expected: FAIL because `TimelineEntryKind::Action`, `DiagnosticsRequest::InvokeSafeAction`, and `SafeDebugActionRequest` do not exist yet.

- [ ] **Step 3: Add the minimal protocol and timeline types**

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TimelineEntryKind {
    Marker,
    Note,
    Action,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum SafeDebugActionRequest {
    SetLogVerbosity {
        level: SafeLogVerbosity,
    },
    CaptureBundle {
        actor: Option<String>,
        reason: Option<String>,
        recent_events_limit: Option<usize>,
    },
    ResetObservabilityCounters {
        actor: Option<String>,
        reason: Option<String>,
    },
    LightweightSelfCheck {
        actor: Option<String>,
        reason: Option<String>,
    },
}
```

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `cargo test -p studiofs loud_safe_action_audit_entries_round_trip_with_action_kind mutating_safe_action_requests_require_current_controller_lease`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/investigation.rs crates/studiofs/src/diagnostics.rs
git commit -m "feat: add safe debug action protocol scaffold (SFS-223)"
```

### Task 2: Add reloadable log verbosity control and client plumbing

**Files:**
- Modify: `crates/studiofs/src/main.rs`
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/diagnostics.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn silent_log_verbosity_action_returns_updated_level_without_timeline_entry() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let lease = acquire_test_lease(&source, &lease_manager, &store);
    let run = create_test_run(&source, &lease_manager, &store, &lease.lease_id);

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::SetLogVerbosity {
                level: SafeLogVerbosity::Debug,
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert_eq!(outcome.action, SafeDebugActionKind::SetLogVerbosity);
    assert_eq!(outcome.summary["level"], serde_json::json!("debug"));
    assert!(store.load_run(&run.id).expect("load run").timeline.is_empty());
}
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `cargo test -p studiofs silent_log_verbosity_action_returns_updated_level_without_timeline_entry`

Expected: FAIL because the safe-action execution path and reloadable tracing handle are not implemented yet.

- [ ] **Step 3: Implement reloadable tracing and silent action handling**

```rust
static DIAGNOSTICS_LOG_FILTER: OnceLock<
    reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>
> = OnceLock::new();

fn init_tracing(log_level: &str) -> anyhow::Result<Option<WorkerGuard>> {
    let filter = tracing_subscriber::EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let (filter_layer, reload_handle) = tracing_subscriber::reload::Layer::new(filter);
    let _ = DIAGNOSTICS_LOG_FILTER.set(reload_handle);
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();
    Ok(None)
}

fn set_diagnostics_log_verbosity(level: SafeLogVerbosity) -> Result<()> {
    let handle = DIAGNOSTICS_LOG_FILTER
        .get()
        .ok_or_else(|| anyhow!("diagnostics log filter reload handle is unavailable"))?;
    handle.reload(tracing_subscriber::EnvFilter::new(level.as_directive()))?;
    Ok(())
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `cargo test -p studiofs silent_log_verbosity_action_returns_updated_level_without_timeline_entry`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/main.rs crates/studiofs/src/diagnostics.rs
git commit -m "feat: add silent safe log verbosity control (SFS-223)"
```

### Task 3: Implement loud actions with fail-open outcomes and timeline persistence

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`
- Modify: `crates/studiofs/src/investigation.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-cache/src/metrics.rs`
- Test: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/investigation.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn loud_safe_action_appends_audited_timeline_entry() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let lease = acquire_test_lease(&source, &lease_manager, &store);
    let run = create_test_run(&source, &lease_manager, &store, &lease.lease_id);

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::LightweightSelfCheck {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("sanity check after attach".to_string()),
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert!(outcome.loud);
    let persisted = store.load_run(&run.id).expect("load run");
    assert_eq!(persisted.timeline.len(), 1);
    assert_eq!(persisted.timeline[0].kind, TimelineEntryKind::Action);
}

#[test]
fn loud_safe_action_failures_remain_audited_and_fail_open() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let lease = acquire_test_lease(&source, &lease_manager, &store);
    let run = create_test_run(&source, &lease_manager, &store, &lease.lease_id);

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::CaptureBundle {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("capture evidence".to_string()),
                recent_events_limit: Some(8),
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert_eq!(outcome.status, SafeDebugActionStatus::Failed);
    assert_eq!(store.load_run(&run.id).expect("load run").timeline.len(), 1);
}
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `cargo test -p studiofs loud_safe_action_appends_audited_timeline_entry loud_safe_action_failures_remain_audited_and_fail_open`

Expected: FAIL because loud action execution, fail-open outcomes, and audit appends are not implemented yet.

- [ ] **Step 3: Add the minimal loud-action implementation**

```rust
fn append_loud_action_audit(
    store: &InvestigationStore,
    run_id: &str,
    action: SafeDebugActionKind,
    actor: Option<String>,
    reason: Option<String>,
    outcome: &SafeDebugActionOutcome,
) -> Result<()> {
    store.append_timeline_entry(
        run_id,
        NewTimelineEntry {
            source: TimelineEntrySource::Mcp,
            kind: TimelineEntryKind::Action,
            title: action.as_str().to_string(),
            body: Some(outcome.summary.to_string()),
            actor,
            reason,
        },
    )?;
    Ok(())
}

pub fn reset_diagnostics_observability(&self) {
    self.state.metrics.runtime_events.lock().clear();
    self.cache_metrics.reset();
}
```

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `cargo test -p studiofs loud_safe_action_appends_audited_timeline_entry loud_safe_action_failures_remain_audited_and_fail_open`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/diagnostics.rs crates/studiofs/src/investigation.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-cache/src/metrics.rs
git commit -m "feat: add loud safe diagnostics actions with audit trail (SFS-223)"
```

### Task 4: Cover live attach integration and regression verification

**Files:**
- Modify: `crates/studiofs/src/cli/mount/session.rs`
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/cli/mount/session.rs`

- [ ] **Step 1: Write the failing integration assertion**

```rust
let action = launch
    .runtime_handle
    .block_on(competing_client.invoke_safe_action(
        &renewed.lease_id,
        &run.id,
        diagnostics::SafeDebugActionRequest::LightweightSelfCheck {
            actor: Some("studiofs-dev-mcp".to_string()),
            reason: Some("verify live action surface".to_string()),
        },
    ))
    .expect("invoke safe action");

assert!(action.loud);
assert_eq!(action.action, diagnostics::SafeDebugActionKind::LightweightSelfCheck);
```

- [ ] **Step 2: Run the focused integration test to verify it fails**

Run: `cargo test -p studiofs mount_session_launch_publishes_live_diagnostics_and_supports_attach -- --nocapture`

Expected: FAIL because the client helper and mounted-session action path are not wired end to end yet.

- [ ] **Step 3: Add the minimal client/helper wiring and update the integration**

```rust
pub(crate) async fn invoke_safe_action(
    &self,
    lease_id: &str,
    run_id: &str,
    action: SafeDebugActionRequest,
) -> Result<SafeDebugActionOutcome> {
    match self
        .send(DiagnosticsRequest::InvokeSafeAction {
            auth_token: self.discovery.auth_token.clone(),
            lease_id: lease_id.to_string(),
            run_id: run_id.to_string(),
            action,
        })
        .await?
    {
        DiagnosticsResponse::SafeDebugAction { outcome } => Ok(*outcome),
        DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
        other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
    }
}
```

- [ ] **Step 4: Run the focused verification set**

Run: `cargo test -p studiofs diagnostics::tests::silent_log_verbosity_action_returns_updated_level_without_timeline_entry diagnostics::tests::loud_safe_action_appends_audited_timeline_entry diagnostics::tests::loud_safe_action_failures_remain_audited_and_fail_open -- --nocapture`

Run: `cargo test -p studiofs mount_session_launch_publishes_live_diagnostics_and_supports_attach -- --nocapture`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/diagnostics.rs crates/studiofs/src/cli/mount/session.rs
git commit -m "test: cover live safe debug actions end to end (SFS-223)"
```
