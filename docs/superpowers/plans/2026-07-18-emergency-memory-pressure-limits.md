# Emergency Memory-pressure Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make StudioFS reduce managed-memory limits below the performance minima when those minima do not fit, while preserving correctness floors and normal behavior.

**Architecture:** Keep the decision inside `MemoryPressureModel::limits_for`: retain the current performance profile, then fall back to a budget-fitting emergency profile only when necessary. Reuse the existing 40/25/15/10 weights, state transitions, bounded recovery, and `overcommitted_minimum` calculation.

**Tech Stack:** Rust 2024, Cargo unit tests, Python `unittest`, Linux/systemd pressure collector.

---

### Task 1: Reproduce and fix the pressure-envelope overcommit

**Files:**
- Modify: `crates/studiofs-vfs/src/memory_pressure.rs`

- [x] **Step 1: Write the failing regression test**

Add a test that creates a finite 3 GiB cgroup with 1 GiB + 168 MiB headroom, 4 MiB blocks, and a 1 MiB negotiated write. Admit more than the resulting 126 MiB envelope and assert that the snapshot is constrained, not overcommitted, within its 90-percent budget, and above all four 4 MiB correctness floors.

- [x] **Step 2: Verify the regression test is red**

Run: `cargo test -p studiofs-vfs --locked emergency_limits_fit_finite_cgroup_budget -- --nocapture`

Expected: FAIL because the current 64/64/32/32 MiB performance minima total 192 MiB and set `overcommitted_minimum=true`.

- [x] **Step 3: Implement the minimum emergency-profile calculation**

In `MemoryPressureModel::limits_for`, calculate the existing performance profile first and return it when it fits `percent(envelope_bytes, 90)`. Otherwise calculate floors of one block for cache, dirty, and transit, plus `max(block_size, negotiated_max_write)` for async writes. If the floors fit, add each component's rounded share of the remaining budget using weights 40/25/15/10 over total weight 90, capped by `max(configured ceiling, floor)`; if they do not fit, return the floors unchanged. Use `u128` arithmetic in a small private weighted-share helper to avoid overflow.

- [x] **Step 4: Verify the regression test is green**

Run: `cargo test -p studiofs-vfs --locked emergency_limits_fit_finite_cgroup_budget -- --nocapture`

Expected: PASS.

### Task 2: Preserve impossible-budget and normal-profile behavior

**Files:**
- Modify: `crates/studiofs-vfs/src/memory_pressure.rs`

- [x] **Step 1: Strengthen the impossible-budget test**

Update the zero-envelope minimum test to assert the exact correctness floors and retain `overcommitted_minimum=true`.

- [x] **Step 2: Run the focused memory-pressure tests**

Run: `cargo test -p studiofs-vfs --locked memory_pressure -- --nocapture`

Expected: all memory-pressure tests PASS, including normal shares, hysteresis, bounded recovery, cgroup selection, and sampling failures.

- [x] **Step 3: Format and lint the changed crate**

Run: `cargo fmt --check`

Run: `cargo clippy -p studiofs-vfs --all-targets --all-features --locked -- -D warnings`

Expected: both commands exit 0 without warnings.

- [ ] **Step 4: Commit the Rust change**

```bash
git add crates/studiofs-vfs/src/memory_pressure.rs docs/superpowers/plans/2026-07-18-emergency-memory-pressure-limits.md
git commit -m "fix(vfs): fit emergency limits to memory envelope"
```

### Task 3: Revalidate the Linux pressure workflow

**Files:**
- Verify: `tools/perf/tests/`
- Generate ignored artifacts under: `target/perf/`

- [x] **Step 1: Run the deterministic gate suite**

Run: `python3 -m unittest tools.perf.test_linux_cache_gate tools.perf.test_sfs598_linux_parity_gate`

Expected: 108 or more tests PASS.

- [x] **Step 2: Build the release binary**

Run: `cargo build --release --locked`

Expected: exit 0 and a fresh `target/release/studiofs`.

- [ ] **Step 3: Run a fresh HEAD-bound correctness pressure collector**

Run the existing Linux cache collector from a new ignored `target/perf/` artifact root, with the orchestrator inside `systemd-run --user --scope`, so holder, collector, and cgroup operations share an eligible cgroup subtree.

Expected: runtime samples remain within the calculated envelope, `overcommitted_minimum=false`, correctness gates pass, and cleanup leaves no mount, process, or owned cgroup behind.

- [ ] **Step 4: Record evidence or the next concrete blocker**

If the collector passes, continue the four required runs from the completion plan. If it fails, preserve the new artifact path and report the first failing gate without relaxing validation.

### Self-review

- Spec coverage: performance profile, emergency floors, weighted remainder, impossible budget, hysteresis, sampling failures, and collector validation are covered.
- Placeholder scan: no deferred implementation steps or unspecified error handling.
- Type consistency: all changes remain on existing `u64`, `MemoryComponentLimits`, and `MemoryPressureModel::observe` seams.
