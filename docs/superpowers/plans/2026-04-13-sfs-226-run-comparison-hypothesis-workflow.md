# SFS-226 Run Comparison And Hypothesis Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add controller-gated run comparison across equivalent marker spans and durable case-level hypotheses with evidence-linked status transitions.

**Architecture:** Extend the investigation store with marker-boundary snapshots, span comparison artifacts, and hypothesis records while keeping mutations inside the existing diagnostics protocol. Reuse the current evidence-view and report logic for span-scoped summaries, and compute durable before-and-after deltas from marker snapshots captured at timeline boundaries.

**Tech Stack:** Rust 2024, serde, Tokio, existing diagnostics IPC, investigation persistence, focused `cargo test`.

---

### Task 1: Extend investigation persistence for marker snapshots, comparisons, and hypotheses

**Files:**
- Modify: `crates/studiofs/src/investigation.rs`
- Test: `crates/studiofs/src/investigation.rs`

- [ ] Add failing tests for:
  - marker snapshot persistence on a run
  - comparison artifact persistence and linkage from a case
  - hypothesis creation plus status-history updates with evidence references
- [ ] Run focused tests and verify they fail for missing types and store methods.
- [ ] Implement the minimal structs and store methods to make those tests pass.
- [ ] Re-run the focused tests and keep them green.

### Task 2: Add diagnostics comparison and hypothesis workflows

**Files:**
- Modify: `crates/studiofs/src/diagnostics.rs`
- Test: `crates/studiofs/src/diagnostics.rs`

- [ ] Add failing tests for:
  - matching equivalent marker spans by marker title and valid ordering
  - comparison summaries including metric, anomaly, and hotspot deltas
  - hypothesis lifecycle updates that preserve evidence-linked status history
- [ ] Run focused tests and verify they fail for missing request/response handling.
- [ ] Implement the minimal diagnostics protocol, client helpers, and controller-gated execution path.
- [ ] Re-run the focused tests and keep them green.

### Task 3: Add hidden CLI helpers for local workflows

**Files:**
- Modify: `crates/studiofs/src/cli/internal.rs`
- Test: `crates/studiofs/src/cli/internal.rs`

- [ ] Add failing parse tests for hidden diagnostics commands covering compare-run-span and hypothesis management.
- [ ] Run focused tests and verify they fail for missing subcommands.
- [ ] Implement the minimal CLI glue over the diagnostics client and store readers.
- [ ] Re-run the focused tests and keep them green.

### Task 4: Verify and finish

**Files:**
- Modify: `crates/studiofs/src/investigation.rs`
- Modify: `crates/studiofs/src/diagnostics.rs`
- Modify: `crates/studiofs/src/cli/internal.rs`

- [ ] Run `cargo test -p studiofs` with targeted test names for the new comparison and hypothesis coverage.
- [ ] Run `cargo fmt`.
- [ ] Run `cargo clippy -p studiofs -- -D warnings`.
- [ ] Review the diff for schema consistency and accidental API drift.
