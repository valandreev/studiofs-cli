# SFS-264 PG Lock Leases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace object-store-backed distributed file locks with PostgreSQL-backed hard-lock leases and wire VFS `flock` to the new authority.

**Architecture:** Add a lock-lease authority to `studiofs-meta` centered in `PgMetaEngine`, including lease rows, typed acquire/release/renew/read APIs, known-state caching, and PG `NOTIFY` wakeups. Keep `studiofs-vfs` responsible only for per-mount owner/refcount bookkeeping and call into the metadata authority for distributed coordination.

**Tech Stack:** Rust, `tokio-postgres`, PostgreSQL `NOTIFY`, existing `PgMetaEngine` runtime helpers, VFS unit tests, PG-backed ignored tests.

---

### Task 1: Add failing metadata tests for PG hard-lock leases

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-meta/src/engine.rs`
- Modify: `crates/studiofs-meta/src/types.rs`

- [ ] **Step 1: Write failing tests for lease acquire/release/expiry visibility**
- [ ] **Step 2: Run targeted metadata tests to verify they fail for the expected missing API**
- [ ] **Step 3: Add typed lock-lease domain models and metadata trait surface**
- [ ] **Step 4: Re-run targeted tests and keep them failing on missing PG implementation details**
- [ ] **Step 5: Commit if the task boundary is clean**

### Task 2: Implement PG lock-lease storage and authority APIs

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-meta/src/engine.rs`
- Modify: `crates/studiofs-meta/src/lib.rs`
- Modify: `crates/studiofs-meta/src/types.rs`

- [ ] **Step 1: Add the PostgreSQL table/bootstrap path for file-lock leases**
- [ ] **Step 2: Implement acquire/release/read-known-state operations with typed results**
- [ ] **Step 3: Add lease renewal and generation/version handling**
- [ ] **Step 4: Run the targeted metadata tests and verify they pass**
- [ ] **Step 5: Commit if the task boundary is clean**

### Task 3: Add notification-driven known-state refresh

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-meta/src/engine.rs`

- [ ] **Step 1: Write/extend failing tests for peer notification refresh**
- [ ] **Step 2: Run the targeted metadata tests to verify correct red failure**
- [ ] **Step 3: Implement a dedicated notification listener plus cache refresh path**
- [ ] **Step 4: Re-run the targeted metadata tests and verify they pass**
- [ ] **Step 5: Commit if the task boundary is clean**

### Task 4: Rewire VFS `flock` away from object-store lock records

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/locking.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/bootstrap.rs`
- Modify: `crates/studiofs-vfs/src/runtime.rs`

- [ ] **Step 1: Write failing VFS tests proving `flock` uses metadata authority semantics**
- [ ] **Step 2: Run targeted VFS tests to verify they fail for the expected reason**
- [ ] **Step 3: Remove object-store remote lock code and call the metadata lease authority instead**
- [ ] **Step 4: Re-run the targeted VFS tests and verify they pass**
- [ ] **Step 5: Commit if the task boundary is clean**

### Task 5: Final focused verification and cleanup

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-vfs/src/vfs/locking.rs`
- Modify: any touched file requiring formatting/import cleanup

- [ ] **Step 1: Run `cargo fmt`**
- [ ] **Step 2: Run focused tests for touched metadata and VFS lock paths**
- [ ] **Step 3: Review diff for accidental scope creep into `SFS-265`**
- [ ] **Step 4: Stage all files and commit the implementation**
- [ ] **Step 5: Report remaining gaps, especially full-suite validation left for the user**
