# SFS-261 Local Payload Runtime Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove superseded `legacy runtime tag` runtime surface and duplicate module tree so the repository exposes one canonical local payload runtime while preserving the current `v3/` disk layout as an implementation detail.

**Architecture:** Keep `crates/studiofs-cache/src/runtime.rs` plus `payload_*` and `startup_recovery.rs` as the only active runtime implementation. Rename public/runtime-facing `legacy runtime symbols` surface to `LocalPayload*`, migrate CLI and VFS callers to the neutral runtime vocabulary, then delete `crates/studiofs-cache/src/v3/**` and rewrite active docs/tests to match.

**Tech Stack:** Rust 2024 workspace, `cargo test`, `cargo fmt`, `studiofs-cache`, `studiofs`, `studiofs-vfs`, Markdown docs.

---

### Task 1: Canonicalize the `studiofs-cache` runtime API

**Files:**
- Modify: `crates/studiofs-cache/src/lib.rs`
- Modify: `crates/studiofs-cache/src/runtime.rs`
- Modify: `crates/studiofs-cache/src/payload_layout.rs`
- Modify: `crates/studiofs-cache/src/payload_store.rs`
- Modify: `crates/studiofs-cache/src/startup_recovery.rs`
- Test: `crates/studiofs-cache/src/lib.rs`
- Test: `crates/studiofs-cache/src/runtime.rs`

- [ ] **Step 1: Write the failing tests for the renamed runtime surface**

```rust
#[test]
fn prepare_local_payload_runtime_creates_contract_paths() {
    let temp = tempfile::tempdir().unwrap();

    let prepared = prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

    assert_eq!(prepared.layout, LocalPayloadLayout::from_cache_root(temp.path()));
    assert!(prepared.detection.has_runtime_layout());
}

#[test]
fn local_payload_runtime_root_opens_local_payload_backend() {
    let temp = tempfile::tempdir().unwrap();
    let prepared =
        prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

    let opened = open_prepared_local_payload_backend_with_budget_and_sync_mode(
        prepared,
        "volume-uuid",
        64 * 1024 * 1024,
        LocalPayloadSyncMode::Durable,
    )
    .expect("open local payload backend");

    assert_eq!(opened.backend_kind, LocalPayloadBackendKind::Current);
    assert!(opened.detection.has_runtime_layout());
}
```

- [ ] **Step 2: Run the focused cache tests to verify they fail for the expected symbol/name mismatch**

Run: `cargo test -p studiofs-cache prepare_local_payload_runtime_creates_contract_paths local_payload_runtime_root_opens_local_payload_backend`

Expected: FAIL because the new `LocalPayload*` names and neutral detection helpers do not exist yet.

- [ ] **Step 3: Rename the canonical runtime types, helpers, and detection predicates**

```rust
pub use runtime::{
    CacheBackendDetection as LocalPayloadRuntimeDetection,
    LocalPayloadBackendKind,
    LocalPayloadBackendConfig,
    LocalPayloadBackendOpenError,
    LocalPayloadBackendRuntime as PersistentLocalPayloadBackend,
    LocalPayloadLayout,
    LocalPayloadSyncMode,
};

#[derive(Debug, Clone)]
pub struct PreparedLocalPayloadRuntime {
    pub layout: runtime::LocalPayloadLayout,
    pub detection: runtime::LocalPayloadRuntimeDetection,
}

pub fn prepare_local_payload_runtime(
    cache_root: impl AsRef<Path>,
) -> Result<PreparedLocalPayloadRuntime, PrepareLocalPayloadRuntimeError> {
    // existing provisioning logic, but using neutral names
}
```

- [ ] **Step 4: Keep the on-disk `v3/` layout constants as implementation-detail aliases, not the primary vocabulary**

```rust
pub const LOCAL_PAYLOAD_SUBTREE: &str = "v3";
pub const LOCAL_PAYLOAD_LAYOUT_MARKER_FILE: &str = "LAYOUT_V1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPayloadLayout {
    pub cache_root: PathBuf,
    pub v3_root: PathBuf,
    pub layout_marker: PathBuf,
    // ...
}

pub type LegacyRuntimeLayout = LocalPayloadLayout;
```

- [ ] **Step 5: Run the focused cache tests to verify the renamed API passes**

Run: `cargo test -p studiofs-cache prepare_local_payload_runtime_creates_contract_paths local_payload_runtime_root_opens_local_payload_backend`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-cache/src/lib.rs crates/studiofs-cache/src/runtime.rs crates/studiofs-cache/src/payload_layout.rs crates/studiofs-cache/src/payload_store.rs crates/studiofs-cache/src/startup_recovery.rs
git commit -m "refactor: rename canonical local payload runtime surface (SFS-261)"
```

### Task 2: Migrate CLI and VFS callers to the neutral runtime vocabulary

**Files:**
- Modify: `crates/studiofs/src/cli/status.rs`
- Modify: `crates/studiofs/src/cli/janitor.rs`
- Modify: `crates/studiofs/src/cli/destroy.rs`
- Modify: `crates/studiofs/src/cli/cache_layout.rs`
- Modify: `crates/studiofs/src/cli/mount/session.rs`
- Modify: `crates/studiofs-vfs/src/vfs/bootstrap.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Test: `crates/studiofs/src/cli/status.rs`
- Test: `crates/studiofs/src/cli/janitor.rs`
- Test: `crates/studiofs/src/cli/mount/session.rs`
- Test: `crates/studiofs-vfs/src/vfs/bootstrap.rs`

- [ ] **Step 1: Write the failing CLI/VFS regression tests around neutral runtime naming**

```rust
#[test]
fn scan_pending_uploads_supports_current_local_payload_runtime() {
    let tmp = tempfile::tempdir().unwrap();
    let layout = studiofs_cache::runtime::LocalPayloadLayout::from_cache_root(tmp.path());
    std::fs::create_dir_all(&layout.v3_root).unwrap();
    std::fs::write(&layout.layout_marker, b"v1").unwrap();

    let detection = detect_backends(tmp.path()).unwrap();
    assert_eq!(detection.runtime_backend(), Some(LocalPayloadBackendKind::Current));
}

#[test]
fn prepare_volume_cache_runtime_scopes_layout_by_volume_uuid() {
    let temp = tempfile::tempdir().unwrap();
    let prepared = prepare_volume_cache_runtime(temp.path(), "volume-uuid").unwrap();

    assert_eq!(
        prepared.cache_runtime.layout,
        LocalPayloadLayout::from_cache_root(&prepared.cache_path)
    );
    assert!(prepared.cache_runtime.detection.has_runtime_layout());
}
```

- [ ] **Step 2: Run the narrow CLI/VFS tests to verify they fail on old `legacy runtime symbols` names**

Run: `cargo test -p studiofs scan_pending_uploads_supports_current_local_payload_runtime prepare_volume_cache_runtime_scopes_layout_by_volume_uuid`

Expected: FAIL because the code still imports `legacy runtime symbols` names and old detection helpers.

- [ ] **Step 3: Update status, janitor, mount, and bootstrap code to use the neutral API and wording**

```rust
match cache_backend_detection {
    Some(detection) if detection.runtime_backend() == Some(LocalPayloadBackendKind::Current) => {
        scan_runtime_cache(detection).map(Some)
    }
    _ => Ok(None),
}

println!("  Runtime:             current local payload runtime");
println!("  Disk subtree:        {}", detection.layout.v3_root.display());
```

- [ ] **Step 4: Rename observability tags and test fixture strings away from `legacy runtime tag`**

```rust
let snapshot = VfsWritePathObservabilitySnapshot {
    backend_tag: "local-payload-runtime".to_string(),
    // ...
};

assert_eq!(loaded.1.backend_tag, "local-payload-runtime");
```

- [ ] **Step 5: Run the narrow CLI/VFS tests to verify the caller migration passes**

Run: `cargo test -p studiofs scan_pending_uploads_supports_current_local_payload_runtime prepare_volume_cache_runtime_scopes_layout_by_volume_uuid`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs/src/cli/status.rs crates/studiofs/src/cli/janitor.rs crates/studiofs/src/cli/destroy.rs crates/studiofs/src/cli/cache_layout.rs crates/studiofs/src/cli/mount/session.rs crates/studiofs-vfs/src/vfs/bootstrap.rs crates/studiofs-vfs/src/vfs.rs
git commit -m "refactor: move cli and vfs onto local payload runtime naming (SFS-261)"
```

### Task 3: Remove the duplicated `src/v3/**` module tree

**Files:**
- Modify: `crates/studiofs-cache/src/lib.rs`
- Delete: `crates/studiofs-cache/src/v3/cache_index.rs`
- Delete: `crates/studiofs-cache/src/v3/eviction.rs`
- Delete: `crates/studiofs-cache/src/v3/file_payload_store.rs`
- Delete: `crates/studiofs-cache/src/v3/layout.rs`
- Delete: `crates/studiofs-cache/src/v3/mod.rs`
- Delete: `crates/studiofs-cache/src/v3/platform.rs`
- Delete: `crates/studiofs-cache/src/v3/reconcile.rs`
- Test: `crates/studiofs-cache/src/runtime.rs`
- Test: `crates/studiofs-vfs/tests/vfs_test.rs`

- [ ] **Step 1: Write the failing regression by removing the last `studiofs_cache::v3` caller in tests first**

```rust
#[test]
fn studio_vfs_new_selects_current_local_payload_runtime_for_layout_only_root() {
    let layout = studiofs_cache::runtime::LocalPayloadLayout::from_cache_root(&cache_dir);
    std::fs::create_dir_all(&layout.v3_root).unwrap();
    std::fs::write(&layout.layout_marker, b"v1").unwrap();

    assert!(
        vfs.local_payload_backend()
            .as_any()
            .is::<studiofs_cache::runtime::LocalPayloadBackendRuntime>()
    );
}
```

- [ ] **Step 2: Run the targeted VFS/cache tests to verify the stale `studiofs_cache::v3` imports fail**

Run: `cargo test -p studiofs-vfs studio_vfs_new_selects_current_local_payload_runtime_for_layout_only_root`

Expected: FAIL until all `studiofs_cache::v3` references are removed.

- [ ] **Step 3: Delete `pub mod v3`, remove the duplicated files, and keep all callers on `runtime` or top-level re-exports**

```rust
pub mod benchmark_cache;
pub mod cached_store;
pub mod local;
pub mod metrics;
pub mod runtime;
pub mod upload_queue;

pub use runtime::{
    LocalPayloadBackendConfig,
    LocalPayloadBackendKind,
    LocalPayloadBackendRuntime,
    LocalPayloadLayout,
    LocalPayloadSyncMode,
};
```

- [ ] **Step 4: Run the targeted cache/VFS tests again to verify the repo no longer depends on `src/v3/**`**

Run: `cargo test -p studiofs-cache`

Expected: PASS for the touched `studiofs-cache` runtime tests

Run: `cargo test -p studiofs-vfs studio_vfs_new_selects_current_local_payload_runtime_for_layout_only_root`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-cache/src/lib.rs crates/studiofs-cache/src/runtime.rs crates/studiofs-vfs/tests/vfs_test.rs crates/studiofs-vfs/src/vfs/bootstrap.rs
git rm crates/studiofs-cache/src/v3/cache_index.rs crates/studiofs-cache/src/v3/eviction.rs crates/studiofs-cache/src/v3/file_payload_store.rs crates/studiofs-cache/src/v3/layout.rs crates/studiofs-cache/src/v3/mod.rs crates/studiofs-cache/src/v3/platform.rs crates/studiofs-cache/src/v3/reconcile.rs
git commit -m "refactor: delete duplicated cache v3 module tree (SFS-261)"
```

### Task 4: Rewrite docs and vocabulary guards around the cleaned-up runtime

**Files:**
- Modify: `docs/architecture.md`
- Modify or replace: `docs/local-payload-runtime-layout.md`
- Modify: `docs/juicefs-local-first-rust-contract.md`
- Modify: `crates/studiofs/tests/architecture_vocabulary_test.rs`
- Modify: `crates/studiofs/src/cli/test_cache_profile_common.rs`
- Modify: `crates/studiofs/src/cli/test_cache_profile_stub.rs`
- Test: `crates/studiofs/tests/architecture_vocabulary_test.rs`

- [ ] **Step 1: Write the failing vocabulary/doc tests for the new runtime wording**

```rust
#[test]
fn active_docs_drop_superseded_runtime_identity_wording() {
    let forbidden_terms = ["legacy cache runtime", "superseded runtime", "superseded backend"];
    // same helper pattern, but allow implementation-detail mentions for `v3/` layout
}

#[test]
fn observability_surface_uses_local_payload_runtime_tag() {
    let hits = collect_term_hits(
        "crates/studiofs/src/cli/test_cache_profile_common.rs",
        &["legacy runtime tag"],
    );
    assert!(hits.is_empty());
}
```

- [ ] **Step 2: Run the documentation/vocabulary tests to verify they fail before the doc rewrite**

Run: `cargo test -p studiofs --test architecture_vocabulary_test`

Expected: FAIL because docs and fixtures still encode the superseded runtime identity.

- [ ] **Step 3: Rewrite the active docs so they describe one local payload runtime with a retained `v3/` disk-layout detail**

```md
## Local Cache Runtime

- StudioFS uses one current local payload runtime for mounted operation.
- The present on-disk layout still lives under `<cache-root>/v3/**`.
- The `v3/` subtree is an implementation detail, not a separate product/runtime generation.
```

- [ ] **Step 4: Update fixtures and vocabulary tests to accept the retained disk-layout detail but reject the old runtime identity**

```rust
let snapshot = VfsWritePathObservabilitySnapshot {
    backend_tag: "local-payload-runtime".to_string(),
    // ...
};
```

- [ ] **Step 5: Run the documentation/vocabulary tests to verify the cleanup passes**

Run: `cargo test -p studiofs --test architecture_vocabulary_test`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add docs/architecture.md docs/local-payload-runtime-layout.md docs/juicefs-local-first-rust-contract.md crates/studiofs/tests/architecture_vocabulary_test.rs crates/studiofs/src/cli/test_cache_profile_common.rs crates/studiofs/src/cli/test_cache_profile_stub.rs
git commit -m "docs: describe one canonical local payload runtime (SFS-261)"
```

### Task 5: Final verification and finish

**Files:**
- Modify: any files touched by compile or test fallout
- Verify: `crates/studiofs-cache/**`
- Verify: `crates/studiofs/**`
- Verify: `crates/studiofs-vfs/**`

- [ ] **Step 1: Run formatting**

Run: `cargo fmt`

Expected: exits successfully with formatted files.

- [ ] **Step 2: Run targeted cache verification**

Run: `cargo test -p studiofs-cache`

Expected: PASS

- [ ] **Step 3: Run targeted CLI verification**

Run: `cargo test -p studiofs --test architecture_vocabulary_test`

Expected: PASS

Run: `cargo test -p studiofs scan_pending_uploads_supports_current_local_payload_runtime prepare_volume_cache_runtime_scopes_layout_by_volume_uuid`

Expected: PASS

- [ ] **Step 4: Run targeted VFS verification**

Run: `cargo test -p studiofs-vfs studio_vfs_new_selects_current_local_payload_runtime_for_layout_only_root`

Expected: PASS

- [ ] **Step 5: Inspect the remaining `legacy runtime tag` hits and confirm they are only the retained disk-layout details**

Run: `rg -n "legacy runtime tag|LegacyRuntime" crates docs`

Expected: only implementation-detail aliases or compatibility remnants that are intentionally retained.

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "refactor: finish local payload runtime cleanup (SFS-261)"
```
