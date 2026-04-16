# SFS-249 Simple Local-First CLI Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the broad env-driven operator story with a small Local-First CLI surface for data-cache and IO tuning while keeping metadata-cache internals off the standard UX.

**Architecture:** Extend the existing `studiofs mount`/`settings` layering instead of inventing a second configuration path. Add a small set of explicit IO flags that map onto existing `VfsConfigRaw` fields, preserve transitional env compatibility in `overlay_vfs_env`, and rewrite active help/docs so operators are taught the new small surface.

**Tech Stack:** Rust 2024, `clap` derive API, existing `StudioFsConfig` + `VfsConfigRaw` layering, Markdown docs.

---

### Task 1: Add the small first-class IO flag surface

**Files:**
- Modify: `crates/studiofs/src/cli/mount/mod.rs`
- Modify: `crates/studiofs/src/cli/mount/session.rs`
- Modify: `crates/studiofs/src/cli/settings.rs`
- Test: `crates/studiofs/src/cli/mount/session.rs`
- Test: `crates/studiofs/src/cli/settings.rs`

- [ ] **Step 1: Write failing CLI parse tests for the new flags**

Add tests that parse:

```rust
crate::cli::parse_for_test(&[
    "mount",
    "demo",
    "/mnt/demo",
    "--read-buffer",
    "4GB",
    "--readahead",
    "256",
    "--prefetch",
    "192",
    "--writeback-delay",
    "1500",
])
```

and:

```rust
crate::cli::parse_for_test(&[
    "settings",
    "--read-buffer",
    "4GB",
    "--readahead",
    "256",
    "--prefetch",
    "192",
    "--writeback-delay",
    "1500",
])
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run:

```powershell
cargo test -p studiofs settings_parses_cache_options -- --nocapture
cargo test -p studiofs resolve_mount_startup_prefers_cli_runtime_settings_over_defaults -- --nocapture
```

Expected: at least one failure because the new flags are not wired yet.

- [ ] **Step 3: Implement minimal flag plumbing**

Wire new mount/settings fields to existing validated config fields:

- `--read-buffer` -> `VfsConfigRaw.read_buffer_mb`
- `--readahead` -> `VfsConfigRaw.readahead_width`
- `--prefetch` -> `VfsConfigRaw.prefetch_concurrency`
- `--writeback-delay` -> `VfsConfigRaw.upload_delay_ms`

Preserve precedence:

1. CLI flags
2. env overrides
3. persisted `settings`
4. built-in defaults

- [ ] **Step 4: Run focused tests to verify the plumbing passes**

Run:

```powershell
cargo test -p studiofs settings_ -- --nocapture
cargo test -p studiofs resolve_mount_startup_ -- --nocapture
```

Expected: the new parse/startup tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/cli/mount/mod.rs crates/studiofs/src/cli/mount/session.rs crates/studiofs/src/cli/settings.rs
git commit -m "feat: add small local-first mount tuning flags (SFS-249)"
```

### Task 2: Demote internal env knobs in help text and command UX

**Files:**
- Modify: `crates/studiofs/src/cli/mod.rs`
- Modify: `crates/studiofs/src/cli/mount/mod.rs`
- Test: `crates/studiofs/src/cli/mod.rs`

- [ ] **Step 1: Write or tighten help-text expectations**

Add/update tests so help output mentions the new small operator surface and no
longer presents the large env matrix as the primary tuning model.

- [ ] **Step 2: Run the targeted help tests to verify the old wording fails**

Run:

```powershell
cargo test -p studiofs parse_for_test -- --nocapture
```

Expected: help-text assertions fail until wording is updated.

- [ ] **Step 3: Rewrite the help text**

Update root and mount help to say:

- use `--cache-dir` / `--cache-size` for cache placement and budget
- use the new IO flags for read/write tuning
- env vars are internal or transitional unless explicitly documented

- [ ] **Step 4: Re-run the targeted help verification**

Run:

```powershell
cargo test -p studiofs --lib -- --nocapture
```

Expected: the relevant CLI tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs/src/cli/mod.rs crates/studiofs/src/cli/mount/mod.rs
git commit -m "docs: simplify local-first CLI help surface (SFS-249)"
```

### Task 3: Rewrite active docs around the simplified operator model

**Files:**
- Modify: `docs/configuration.md`
- Modify: `docs/examples.md`
- Modify: `docs/juicefs-local-first-rust-contract.md` (only if wording needs alignment)

- [ ] **Step 1: Identify the active operator-facing sections that still teach the wide env matrix**

Check:

```text
docs/configuration.md
docs/examples.md
crates/studiofs/src/cli/mod.rs
crates/studiofs/src/cli/mount/mod.rs
```

- [ ] **Step 2: Rewrite docs to match the approved model**

Make the docs teach:

- the small first-class CLI surface
- metadata-cache internals stay internal
- transitional env overrides are for migration/debugging
- developers can change internal metadata defaults in code

- [ ] **Step 3: Run formatting and a final focused verification pass**

Run:

```powershell
cargo fmt --all
cargo test -p studiofs settings_ -- --nocapture
cargo test -p studiofs resolve_mount_startup_ -- --nocapture
```

Expected: formatter is clean and the focused CLI tests pass.

- [ ] **Step 4: Commit**

```bash
git add docs/configuration.md docs/examples.md docs/juicefs-local-first-rust-contract.md
git commit -m "docs: teach the simplified local-first operator model (SFS-249)"
```
