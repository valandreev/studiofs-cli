# SFS-267 PG Offline Headroom Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement canonical PostgreSQL-offline write continuation with finite preallocated headroom, read-only degradation on exhaustion, preserved known-lock enforcement, and reconnect replay/coalescing.

**Architecture:** Extend `PgMetaEngine` so inode/object allocation uses explicit persisted active/next preallocated ranges plus a narrow metadata-authority status surface. Reuse the existing local mutation + pending replication pipeline, then gate new VFS mutations and writable opens when PG is unavailable and the persisted headroom is exhausted, while keeping reads and local durability barriers intact.

**Tech Stack:** Rust 2024, `studiofs-meta`, `studiofs-vfs`, PostgreSQL-backed ignored tests, focused `cargo test` slices, existing PG event-log batching/projection.

---

### Task 1: Lock the finite-headroom contract in focused metadata tests

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-meta/src/engine.rs`

- [ ] Add focused tests in `pg_engine.rs` for persisted active/next allocation ranges, explicit exhaustion, and status transitions from `writable` to `read-only`.
- [ ] Add a small test-only configuration path if needed so the engine can run with tiny inode/block ranges and deterministic reconnect timing.
- [ ] Add a failing test proving local mutations still succeed while PG is unavailable and reserved headroom remains.
- [ ] Run only the targeted metadata slice for the new tests and verify the failure is for the missing range/status implementation, not for unrelated setup issues.

### Task 2: Implement persisted inode/block ranges and metadata-authority status

**Files:**
- Modify: `crates/studiofs-meta/src/engine.rs`
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-vfs/src/runtime.rs`

- [ ] Add the narrow metadata-authority status model to `MetaReplication`, exposing PG reachability, writable headroom, and exhaustion-driven read-only degradation without turning raw storage errors into policy.
- [ ] Replace the current implicit slot-derived “large local ID space” behavior in `PgMetaEngine` with explicit persisted active/next inode and block ranges plus deterministic low-water reservation of the next range.
- [ ] Make `next_inode()` and `next_object_ids()` consume only from persisted local ranges, switching into a previously reserved next range when available and surfacing true exhaustion when no safe range remains.
- [ ] Keep local mutations accepted while PG is down but headroom remains, and make the authority status flip to read-only only when the real allocation boundary is reached.
- [ ] Re-run the targeted metadata tests and verify the new range/status behavior passes.

### Task 3: Gate new VFS mutations while preserving reads and local durability barriers

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/io_ops.rs`
- Modify: `crates/studiofs-vfs/src/vfs/namespace_ops.rs`

- [ ] Add one shared VFS helper that denies new mutations and writable opens when metadata authority reports “offline read-only because headroom is exhausted”.
- [ ] Wire that helper into `open_file(...)` for write-intent opens and into the mutating entrypoints in `io_ops.rs` and `namespace_ops.rs`: `write`, `create`, `mkdir`, `symlink`, `link`, `rename`, `unlink`, `rmdir`, and size-changing `setattr`.
- [ ] Keep plain reads, lookup/getattr-style access, `flush`, `fsync`, and last-handle release behavior unchanged so already accepted local work can still cross the required local-durability barriers.
- [ ] Preserve the existing known foreign hard-lock checks so disconnect behavior still enforces the last-known lock cache from `SFS-264`/`SFS-265`.
- [ ] Run focused VFS tests for writable-open denial vs read-only-open allowance and verify the new gate passes without regressing lock enforcement.

### Task 4: Lock reconnect replay and coalescing behavior in metadata/VFS tests

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-vfs/tests/vfs_test.rs`

- [ ] Add or extend PG-backed ignored tests that simulate: local mutations during PG outage, reconnect, and peer observation of the net externally visible result.
- [ ] Cover at least one realistic microstate-heavy sequence such as create-temp / repeated local updates / rename-final, and assert reconnect does not need to replay meaningless internal states to peers.
- [ ] Reuse and, if necessary, strengthen the existing PG event projection path instead of introducing a new replay engine in VFS.
- [ ] Add a focused VFS test proving already known published reads still work while PG is down when local cache or S3 can satisfy the read.
- [ ] Run the smallest relevant ignored PG-backed test slices plus any local fast tests needed to verify the reconnect path.

### Task 5: Focused verification and cleanup

**Files:**
- Modify: any touched Rust source or tests

- [ ] Run `cargo fmt` on the workspace.
- [ ] Run only the focused Rust test slices for touched fast tests and the smallest relevant ignored PG-backed tests.
- [ ] Review the diff for accidental scope creep into `SFS-268`, `SFS-269`, or `SFS-270`, especially around namespace visibility and broad diagnostics/doc churn.
- [ ] Summarize the exact verification commands that passed and leave the full-suite command for the user to run manually.
