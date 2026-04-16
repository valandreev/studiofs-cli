# SFS-237 Ingress Legacy Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the remaining ingress/spill/checkpoint runtime surface without weakening ZeroIO shutdown or replay behavior.

**Architecture:** Replace the legacy ingress runtime directory with a dedicated ZeroIO runtime state directory, then delete dead ingress runtime metrics/config/status plumbing that is currently zeroed or test-only. Keep validation focused on layout, graceful shutdown snapshot persistence, and active Cache 3.0 payload behavior.

**Tech Stack:** Rust workspace, `studiofs-vfs`, `studiofs`, Cache 3.0 local backend, focused `cargo test`

---

### Task 1: Lock in the new runtime layout with tests

**Files:**
- Modify: `crates/studiofs-vfs/tests/vfs_test.rs`

- [ ] Add a focused test that exercises graceful shutdown and asserts the persisted ZeroIO shutdown snapshot lives outside `ingress_spill`.
- [ ] Run the focused test and verify it fails for the current layout.
- [ ] Update runtime path code with the minimal change needed to satisfy the new expectation.
- [ ] Re-run the focused test and verify it passes.

### Task 2: Remove runtime ingress directory/bootstrap dependencies

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/bootstrap.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Delete or reduce: `crates/studiofs-vfs/src/ingress_wal.rs`

- [ ] Remove runtime bootstrap creation and cleanup of `ingress_spill`.
- [ ] Replace ingress-bound snapshot path helpers with dedicated ZeroIO runtime state helpers.
- [ ] Delete runtime-only ingress helpers that are no longer part of the active path.
- [ ] Run focused VFS tests for shutdown/replay/layout coverage.

### Task 3: Remove dead ingress metrics, config, and status plumbing

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/metrics.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/config.rs`
- Modify: `crates/studiofs/src/cli/status.rs`
- Modify: any directly failing tests/docs touched by the removals

- [ ] Remove zero-valued spill/checkpoint fields that no longer describe real runtime state.
- [ ] Remove ingress checkpoint tuning config that no longer affects production behavior.
- [ ] Remove status scanning of `ingress_spill` and any output derived from it.
- [ ] Re-run focused tests and compile checks for the touched crates.

### Task 4: Verify and summarize residual gaps

**Files:**
- Modify: tests/docs only if verification exposes drift

- [ ] Run the final focused test set for `studiofs-vfs` and any affected `studiofs` status tests.
- [ ] Run a targeted build/test check that proves the touched crates compile cleanly.
- [ ] Summarize any remaining SFS-237 follow-up gaps explicitly instead of leaving them implicit.
