# SFS-221 Runtime Snapshot Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add stable runtime snapshot and bounded recent-event diagnostics RPCs for live StudioFS attach sessions.

**Architecture:** Extend the existing local diagnostics host instead of creating a parallel protocol. Reuse `StudioVfs` snapshot producers for the structured schema, and feed a bounded daemon-side event buffer from the existing upload/cache/sync background summary worker so the first event surface stays small and low-overhead.

**Tech Stack:** Rust, Tokio, Serde, StudioFS diagnostics host, StudioVfs runtime metrics

---

### Task 1: Add Failing Diagnostics Contract Tests

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/diagnostics.rs`

- [ ] **Step 1: Write the failing runtime snapshot request test**

```rust
#[test]
fn runtime_snapshot_request_returns_structured_sections() {
    let source = static_source();
    let response = execute_request(
        DiagnosticsRequest::RuntimeSnapshot {
            auth_token: "secret".to_string(),
        },
        "secret",
        &source,
        &ControllerLeaseManager::default(),
        None,
    );

    let DiagnosticsResponse::RuntimeSnapshot { snapshot } = response else {
        panic!("expected runtime snapshot response");
    };

    assert_eq!(snapshot.schema_version, 1);
    assert_eq!(snapshot.upload.pending_uploads, source.health.upload.pending_uploads);
    assert_eq!(snapshot.synchronization.file_sessions.open_handles, 0);
}
```

- [ ] **Step 2: Run the targeted test and verify RED**

Run: `cargo test -p studiofs runtime_snapshot_request_returns_structured_sections`
Expected: FAIL because `RuntimeSnapshot` request/response types do not exist yet.

- [ ] **Step 3: Write the failing bounded recent-events test**

```rust
#[test]
fn recent_events_request_is_bounded_and_keeps_newest_entries() {
    let source = static_source_with_recent_events(vec![
        sample_event("older"),
        sample_event("middle"),
        sample_event("newest"),
    ]);

    let response = execute_request(
        DiagnosticsRequest::RecentEvents {
            auth_token: "secret".to_string(),
            limit: Some(2),
        },
        "secret",
        &source,
        &ControllerLeaseManager::default(),
        None,
    );

    let DiagnosticsResponse::RecentEvents { events } = response else {
        panic!("expected recent events response");
    };

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].title, "middle");
    assert_eq!(events[1].title, "newest");
}
```

- [ ] **Step 4: Run the targeted test and verify RED**

Run: `cargo test -p studiofs recent_events_request_is_bounded_and_keeps_newest_entries`
Expected: FAIL because recent-event support does not exist yet.

### Task 2: Implement Diagnostics Schema and Client Methods

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`

- [ ] **Step 1: Add runtime snapshot and recent-event structs**

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsRuntimeSnapshot {
    pub schema_version: u32,
    pub generated_unix_ms: u64,
    pub ingress: VfsIngressStats,
    pub write_path: VfsWritePathObservabilitySnapshot,
    pub upload: VfsUploadStats,
    pub cache: DiagnosticsCacheSnapshot,
    pub synchronization: DiagnosticsSynchronizationSnapshot,
}
```

- [ ] **Step 2: Extend the diagnostics data-source trait with snapshot and event reads**

```rust
pub(crate) trait DiagnosticsDataSource: Send + Sync + 'static {
    fn session_info(&self) -> DiagnosticsSessionInfo;
    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot;
    fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot;
    fn recent_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent>;
    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint;
}
```

- [ ] **Step 3: Add new request/response variants and wire `execute_request`**

```rust
DiagnosticsRequest::RuntimeSnapshot { auth_token: provided } => { /* auth + source.runtime_snapshot() */ }
DiagnosticsRequest::RecentEvents { auth_token: provided, limit } => { /* auth + source.recent_events(limit.unwrap_or(DEFAULT)) */ }
```

- [ ] **Step 4: Extend `LocalDiagnosticsClient` with async helpers**

```rust
pub(crate) async fn runtime_snapshot(&self) -> Result<DiagnosticsRuntimeSnapshot> { /* ... */ }
pub(crate) async fn recent_events(&self, limit: Option<usize>) -> Result<Vec<DiagnosticsRecentEvent>> { /* ... */ }
```

- [ ] **Step 5: Re-run the two targeted diagnostics tests and verify GREEN**

Run: `cargo test -p studiofs runtime_snapshot_request_returns_structured_sections recent_events_request_is_bounded_and_keeps_newest_entries`
Expected: PASS

### Task 3: Feed the Live Snapshot From `StudioVfs`

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`
- Modify: `crates/studiofs-vfs/src/vfs/metrics.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/file_session.rs`
- Modify: `crates/studiofs-cache/src/metrics.rs`

- [ ] **Step 1: Make the needed cache/session metric structs serializable**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePipelineMetricsSnapshot { /* existing fields */ }
```

- [ ] **Step 2: Export any private cache or namespace metric shapes needed by the runtime snapshot**

```rust
pub struct DiagnosticsNamespaceCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub occupancy_entries: usize,
    pub occupancy_bytes: usize,
}
```

- [ ] **Step 3: Add a `StudioVfs` helper that assembles the full diagnostics snapshot**

```rust
pub fn diagnostics_runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshotParts {
    DiagnosticsRuntimeSnapshotParts {
        ingress: self.ingress_stats(),
        write_path: self.write_path_observability_snapshot(),
        upload: self.upload_stats(),
        cache: /* queue, namespace caches, transit memory, dirty backend */,
        synchronization: /* file sessions, foreground reads, metadata backlog */,
    }
}
```

- [ ] **Step 4: Use that helper from `LiveMountDiagnosticsSource::runtime_snapshot()`**

```rust
fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot {
    let parts = self.vfs.diagnostics_runtime_snapshot();
    DiagnosticsRuntimeSnapshot {
        schema_version: 1,
        generated_unix_ms: now_unix_millis(),
        ingress: parts.ingress,
        write_path: parts.write_path,
        upload: parts.upload,
        cache: parts.cache,
        synchronization: parts.synchronization,
    }
}
```

- [ ] **Step 5: Run the focused diagnostics crate tests**

Run: `cargo test -p studiofs diagnostics`
Expected: PASS for touched diagnostics tests.

### Task 4: Add the Bounded Recent-Event Buffer

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/workers.rs`

- [ ] **Step 1: Add a bounded recent-event store to `StudioVfs` state**

```rust
const DEFAULT_RUNTIME_EVENT_CAPACITY: usize = 128;

pub struct DiagnosticsRecentEvent {
    pub sequence: u64,
    pub generated_unix_ms: u64,
    pub subsystem: String,
    pub kind: String,
    pub title: String,
    pub fields: serde_json::Value,
}
```

- [ ] **Step 2: Add `record_runtime_event` and `recent_runtime_events` helpers**

```rust
pub fn recent_runtime_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent> {
    let events = self.state.runtime_events.lock();
    let keep = limit.min(events.len());
    events.iter().skip(events.len().saturating_sub(keep)).cloned().collect()
}
```

- [ ] **Step 3: Record compact events from `background_upload_stats`**

```rust
if activity_started {
    self.record_runtime_event("upload_pipeline", "became_active", "upload pipeline active", json!({ /* counters */ }));
}
if should_log {
    self.record_runtime_event("upload_pipeline", "sample", "upload pipeline sample", json!({ /* counters */ }));
}
if !is_active && upload_pipeline_summary_was_active {
    self.record_runtime_event("upload_pipeline", "became_idle", "upload pipeline idle", json!({ /* counters */ }));
}
```

- [ ] **Step 4: Serve those events through `LiveMountDiagnosticsSource::recent_events()`**

```rust
fn recent_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent> {
    self.vfs.recent_runtime_events(limit)
}
```

- [ ] **Step 5: Run the recent-event tests and one focused VFS test target**

Run: `cargo test -p studiofs recent_events`
Expected: PASS

Run: `cargo test -p studiofs-vfs upload_pipeline_summary_logs_only_when_pending_work_exists`
Expected: PASS

### Task 5: Extend Live Attach Coverage

**Files:**
- Modify: `crates/studiofs/src/cli/internal.rs`
- Modify: `crates/studiofs/src/cli/mount/session.rs`

- [ ] **Step 1: Extend the internal attach command output to include the new surfaces**

```rust
let runtime_snapshot = client.runtime_snapshot().await?;
let recent_events = client.recent_events(Some(16)).await?;
```

- [ ] **Step 2: Extend the live mount attach integration test**

```rust
let runtime_snapshot = launch.runtime_handle.block_on(client.runtime_snapshot())?;
assert_eq!(runtime_snapshot.schema_version, 1);

let recent_events = launch.runtime_handle.block_on(client.recent_events(Some(8)))?;
assert!(recent_events.len() <= 8);
```

- [ ] **Step 3: Run the focused mount-session test and verify GREEN**

Run: `cargo test -p studiofs mount_session_launch_publishes_live_diagnostics_and_supports_attach`
Expected: PASS

### Task 6: Final Verification

**Files:**
- Modify: `docs/superpowers/specs/2026-04-13-sfs-221-runtime-snapshot-surface-design.md`
- Modify: `docs/superpowers/plans/2026-04-13-sfs-221-runtime-snapshot-surface.md`

- [ ] **Step 1: Format the touched Rust crates**

Run: `cargo fmt --all`
Expected: exit 0

- [ ] **Step 2: Run the focused verification set**

Run: `cargo test -p studiofs diagnostics`
Expected: PASS

Run: `cargo test -p studiofs mount_session_launch_publishes_live_diagnostics_and_supports_attach`
Expected: PASS

Run: `cargo test -p studiofs-vfs upload_pipeline_summary_logs_only_when_pending_work_exists`
Expected: PASS

- [ ] **Step 3: Run a focused lint/build check for touched code**

Run: `cargo clippy -p studiofs -p studiofs-vfs -p studiofs-cache -- -D warnings`
Expected: exit 0

- [ ] **Step 4: Commit after verification**

```bash
git add docs/superpowers/specs/2026-04-13-sfs-221-runtime-snapshot-surface-design.md docs/superpowers/plans/2026-04-13-sfs-221-runtime-snapshot-surface.md crates/studiofs/src/diagnostics.rs crates/studiofs/src/cli/internal.rs crates/studiofs/src/cli/mount/session.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/workers.rs crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs/file_session.rs crates/studiofs-cache/src/metrics.rs
git commit -m "feat: add runtime diagnostics snapshot surface (SFS-221)"
```
