# SFS-232 Unpublished Epoch Uploader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the background uploader so unpublished epochs are uploaded from local durable state, transition to explicit `PublishReady`, and publish exactly once only after the full epoch is complete.

**Architecture:** Keep the current parallel block upload worker pool, but move epoch completion truth into durable metadata instead of inferring readiness from pending queue emptiness. Upload success will advance overlay block lifecycle state for the active epoch; a separate finalizer will publish only when the inode lifecycle says the current epoch is `PublishReady` and still current.

**Tech Stack:** Rust 2024, Tokio, StudioFS VFS, Redb/PG metadata engines, focused Rust tests.

---

### Task 1: Lock the failing behavior in tests

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs`
- Modify: `crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs`

- [ ] Add a metadata test that proves uploaded overlay slices transition inode lifecycle to `PublishReady` without publishing the inode yet.
- [ ] Run only the new metadata test and verify it fails for the expected reason.
- [ ] Add an uploader worker test that proves successful upload records durable epoch progress instead of only clearing queue state.
- [ ] Run only the new uploader worker test and verify it fails for the expected reason.

### Task 2: Add durable per-block uploaded bookkeeping

**Files:**
- Modify: `crates/studiofs-meta/src/engine.rs`
- Modify: `crates/studiofs-vfs/src/runtime.rs`
- Modify: `crates/studiofs-meta/src/redb_engine/payload_objects.rs`
- Modify: `crates/studiofs-meta/src/pg_engine.rs`

- [ ] Add a metadata API that marks the current overlay block for `(ino, block_index, expected_epoch, expected_block)` as uploaded without publishing base visibility.
- [ ] Implement the Redb mutation so it updates overlay slice state to `Uploaded`, preserves stale-epoch rejection, and rebuilds inode lifecycle.
- [ ] Forward the same API through the runtime and PG layers.
- [ ] Run focused metadata tests for the new mutation and stale-epoch rejection.

### Task 3: Make upload success advance epoch state

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs`

- [ ] Update the upload worker success path to mark metadata block state as uploaded before local post-upload cleanup.
- [ ] Keep retry, delete-cancel, and stale-epoch short-circuit behavior unchanged for local readability.
- [ ] Refresh VFS incarnation state after durable upload-state transition.
- [ ] Run focused uploader worker tests to verify success, retry, and stale suppression behavior.

### Task 4: Replace queue-emptiness publication gating with explicit publish barrier

**Files:**
- Modify: `crates/studiofs-vfs/src/commit.rs`
- Modify: `crates/studiofs-vfs/src/vfs/workers.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`

- [ ] Change finalization so the current epoch publishes only when metadata says the inode lifecycle is `PublishReady`.
- [ ] Preserve stale-epoch suppression by re-reading current attr/lifecycle before publish and skipping superseded epochs.
- [ ] Preserve exactly-once behavior by keeping conditional publish calls and treating already-superseded epochs as skipped rather than published.
- [ ] Run focused VFS tests around epoch finalization and metadata-only publish-ready recovery.

### Task 5: Cover restart-safe and visibility semantics with focused tests

**Files:**
- Modify: `crates/studiofs-vfs/tests/vfs_test.rs`

- [ ] Add or adapt focused tests for successful upload to publish-ready to publish, retry/requeue without local read regression, and stale-epoch suppression when a newer epoch supersedes an older one.
- [ ] Keep test runs scoped to the changed tests only; do not run the full workspace suite.
- [ ] Run the focused test commands and record any remaining gaps for follow-up issues rather than guessing.

### Task 6: Refresh docs to match the new uploader contract

**Files:**
- Modify: `docs/vfs.md`
- Modify: `docs/metadata.md`

- [ ] Update docs so the uploader is described as data-plane only, with durable epoch progress stored in metadata and `PublishReady` as the explicit publish barrier.
- [ ] Mention that the same local payload objects stay in one cache and only lifecycle/retention metadata changes across upload and publish.
- [ ] Run the focused tests again after doc-adjacent code changes if any behavior changed while documenting.
