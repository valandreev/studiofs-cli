# SFS-648 Configuration Contract Reconciliation

> **For agentic workers:** Follow this living ExecPlan with the TDD workflow. Keep the scope narrow: reconcile executable defaults and remove only configuration knobs proven to have no runtime reader. Maintain this file in accordance with `.agent/PLANS.md`.

## Purpose

Make the documented cross-platform configuration contract agree with the current executable behavior and remove zombie configuration surface without redesigning `VfsConfig`, `VfsConfigRaw`, or `VfsBootstrapPlan`. After the change, `studiofs --help`, the platform guides, CI instructions, and the executable source all describe Linux/libfuse3, macOS/macFUSE, and Windows/WinFSP consistently. The five removed public configuration fields are an intentional source-level API removal because their end-to-end removal is part of SFS-648 acceptance.

## Progress

- [x] (2026-08-07 21:35 +03:00) Read SFS-648, related Linear history, current HEAD, and repository guidance.
- [x] (2026-08-07 21:35 +03:00) Confirm macFUSE is the current macOS default and WinFSP cleanup flush/purge defaults to disabled.
- [x] (2026-08-07 21:35 +03:00) Audit the candidate knobs for runtime readers and preserve `upload_delay_ms`, which is still consumed by internal VFS upload scheduling.
- [x] (2026-08-07 21:35 +03:00) Add red contract tests for executable defaults, selectors, target dependencies, cleanup overrides, and live platform documentation.
- [x] (2026-08-07 21:35 +03:00) Reconcile documentation and remove proven-dead knobs end-to-end.
- [x] (2026-08-07 21:35 +03:00) Run focused tests, lint/build checks, and record platform gaps.
- [x] (2026-08-07 21:35 +03:00) Address review findings for exact validation commands, public API accuracy, and self-contained execution steps.

## Surprises & Discoveries

- `crates/studiofs/src/cli/mount/backend.rs` already routes macOS `None` and empty selectors to `MacFuseMountBackend`; the stale opt-in wording is documentation drift, not an executable-default bug.
- `crates/studiofs-winfsp/src/lib.rs` already defines `DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP` as `false`; `docs/platform-bridges.md` and `docs/configuration.md` agree, while `docs/agents-coding-guide.md` is stale.
- `VfsBootstrapPlan` does not carry the candidate dead knobs, and the remaining configuration duplication is intentionally retained for this issue because a measured nested grouping has not been established.
- The SFS-485 readiness document referenced the removed `studiofs-fuse` package. The current Linux bridge package is `studiofs-linux-fuse`, and its native mounted tests are selected with the `mounted_` filter.
- The executable source and target dependency wiring already cover all three supported bridge crates; the missing contract was documentation and host-independent test coverage, not a runtime routing change.

## Decision Log

- Decision: Preserve the macFUSE default and keep `STUDIOFS_MOUNT_BACKEND=macfuse` as a compatibility selector. Rationale: this is the executable behavior and existing scripts depend on the selector. Date/Author: 2026-08-07 / Codex.
- Decision: Preserve WinFSP cleanup flush/purge disabled by default and keep the explicit environment override. Rationale: the override remains a targeted stale-cache experiment surface. Date/Author: 2026-08-07 / Codex.
- Decision: Remove only `readahead_seq_step`, `readahead_random_penalty`, `prefetch_max_per_cycle`, `max_drain_batch`, and `slab_flush_interval_ms`. Rationale: their references are limited to config/default/overlay/test or legacy environment plumbing and no runtime consumer reads them. Date/Author: 2026-08-07 / Codex.
- Decision: Do not remove `upload_delay_ms`. Rationale: it remains part of the internal `VfsBootstrapPlan` and `StudioVfs` upload lifecycle even though public mount/settings input deliberately strips it. Date/Author: 2026-08-07 / Codex.
- Decision: Document removal of the five public fields as a source-level API break instead of retaining deprecated zombie fields. Rationale: SFS-648 requires unused knobs removed end-to-end, and no runtime behavior or replacement setting exists for them. Date/Author: 2026-08-07 / Codex.

## Outcomes & Retrospective

The executable and documented defaults now agree for Linux/libfuse3, macFUSE,
and WinFSP. The five proven-dead knobs have no active source, environment,
test-helper, or configuration-documentation references. The shared
configuration types remain flat; no nested grouping was introduced without a
measured benefit. The public `VfsConfig` and `VfsConfigRaw` fields named in the
decision log are intentionally absent from the resulting API.

The two SFS-648 contract tests, the updated SFS-485 vocabulary test, three VFS
config validation tests, the macOS default selector test, formatting, focused
`-D warnings` checks for `studiofs`/`studiofs-vfs` and `studiofs-winfsp`, and
`git diff --check` are the required local proof. Native Windows behavior and
mounted FUSE/macFUSE validation remain runner-dependent gaps when those hosts
are unavailable.

Observed local evidence on 2026-08-07: each focused test reported one passed
test and zero failures, `cargo fmt --all -- --check` produced no output, the warning-deny
`cargo check -p studiofs -p studiofs-vfs -p studiofs-winfsp` finished
successfully, and `git diff --check` produced no output. The Linux mounted
command was not run on this macOS host because `/dev/fuse` and `fusermount3`
are Linux runner requirements.

## Context

The issue acceptance surface is cross-platform:

- shared core: configuration types, environment overlays, and documentation contract tests;
- macOS/macFUSE: default backend selection and legacy selector compatibility;
- Linux/FUSE: preserve the existing Linux default and shared config behavior;
- Windows/WinFSP: cleanup flush/purge default and explicit override semantics.

The shared VFS types and environment overlays live in `crates/studiofs-vfs/src/vfs/{config.rs,vfs.rs,tests.rs}` and `crates/studiofs/src/cli/mount/session.rs`. Platform routing lives in `crates/studiofs/src/cli/mount/backend.rs`, target dependencies in `crates/studiofs/Cargo.toml`, and WinFSP cleanup parsing in `crates/studiofs-winfsp/src/lib.rs`. User-facing platform help and validation vocabulary live in `crates/studiofs/src/cli/mod.rs`, `docs/configuration.md`, `docs/tests.md`, `docs/platform-bridges.md`, `docs/agents-coding-guide.md`, `docs/macos-native-bridge-split.md`, `docs/perf/sfs-485-macos-native-bridge-readiness.md`, and `.github/workflows/rust.yml`. The architecture contract test is `crates/studiofs/tests/architecture_vocabulary_test.rs`.

## Plan of Work

The first milestone adds source-backed contract rows for each platform's executable route, selector compatibility, target dependency, cleanup default, cleanup override, and live operator documentation. The test must fail before the stale CLI help, platform guide, and workflow wording are corrected; this proves that the new rows detect the actual drift rather than merely exercising already-correct source markers.

The second milestone updates only the live documentation and validation instructions named by those rows. It changes the Linux package/test command to the current `studiofs-linux-fuse` crate, records macFUSE as the default in user-facing help and CI comments, and keeps historical performance reports outside this narrow reconciliation unless they describe current instructions.

The third milestone records the resulting public interface accurately: the five ignored fields are removed from `VfsConfig` and `VfsConfigRaw`, while backend selectors, WinFSP override variables, target dependencies, and platform routing remain. The final milestone reruns the exact focused tests and records native-runner gaps without treating them as passing proof.

## Milestones

The contract milestone is complete when the new architecture test fails on the pre-fix live documentation and passes after the exact current strings, dependencies, selectors, and cleanup wiring are present. Its proof is one filtered Cargo test with one passing test and no ignored or failed cases.

The reconciliation milestone is complete when the SFS-485 vocabulary test, the removed-knob absence test, the three VFS config tests, and the available platform selector test pass. Formatting, warning-deny checks, and `git diff --check` must also pass; native Windows and mounted bridge commands are recorded as unavailable when their required host or driver is absent.

## Concrete Steps

### 1. Red tests

From the repository root, `/Volumes/Storage/dev/studiofs` in this checkout, run the new contract test before correcting the live surfaces:

    cargo test -p studiofs --test architecture_vocabulary_test configuration_contracts_match_executable_defaults -- --nocapture

The expected red result is one failed test listing the missing `CLI Linux platform help`, `platform test guide Linux bridge`, and `workflow macOS default` contracts. The source-routing, target-dependency, selector, and WinFSP override rows should already be present; a failure in those rows indicates a test expectation or source inspection error that must be corrected before proceeding.

### 2. Contract and cleanup implementation

Use `apply_patch` for the minimal edits. Update the Linux line in `crates/studiofs/src/cli/mod.rs`, the platform-gated test guidance in `docs/tests.md`, the macOS workflow comment in `.github/workflows/rust.yml`, and the Linux command in `docs/perf/sfs-485-macos-native-bridge-readiness.md`. Update the matching SFS-485 architecture-test term. The existing VFS deletion remains limited to the five proven-dead fields, defaults, normalization, conversions, environment mappings, test helper parsing, and active spec/config references; do not add a replacement abstraction or retain ignored public knobs.

### 3. Validation

Run each command separately from `/Volumes/Storage/dev/studiofs`. A successful Cargo test prints `test ... ok` and `test result: ok`; a command requiring a native host is a validation gap, not a pass, when that host is unavailable.

    cargo test -p studiofs --test architecture_vocabulary_test configuration_contracts_match_executable_defaults -- --nocapture
    cargo test -p studiofs --test architecture_vocabulary_test removed_configuration_knobs_have_no_active_surface -- --nocapture
    cargo test -p studiofs --test architecture_vocabulary_test macos_native_bridge_readiness_report_covers_sfs485_decision_gate -- --nocapture
    cargo test -p studiofs-vfs --lib vfs::config::tests::vfs_config_default_starts_uploads_without_delay -- --exact
    cargo test -p studiofs-vfs --lib vfs::config::tests::vfs_config_raw_validate_applies_defaults -- --exact
    cargo test -p studiofs-vfs --lib vfs::config::tests::vfs_config_raw_validate_overlays_explicit_values -- --exact
    cargo test -p studiofs --bin studiofs cli::mount::backend::tests::macos_default_selector_routes_to_macfuse -- --exact
    cargo fmt --all -- --check
    RUSTFLAGS='-D warnings' cargo check -p studiofs -p studiofs-vfs -p studiofs-winfsp
    git diff --check

On a Linux host with `/dev/fuse` and `fusermount3`, run:

    STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-linux-fuse mounted_ -- --ignored --nocapture --test-threads=1

On a configured Windows host, run the WinFSP unit and mounted checks documented in `docs/tests.md`. On a configured macOS host, run the macFUSE library and mounted checks documented in `docs/perf/sfs-485-macos-native-bridge-readiness.md`. Windows/WinFSP and mounted FUSE/macFUSE tests are only reported as passing when their runners and drivers are actually available.

## Validation and Acceptance

Acceptance is behavioral and observable: `studiofs --help` identifies Linux as libfuse3, current platform guides name the same bridge crates as `Cargo.toml`, macOS is described as the default while retaining the compatibility selector, and WinFSP documents disabled cleanup flush/purge with its explicit override. The two SFS-648 contract rows and the SFS-485 vocabulary test fail if these statements or the source wiring drift. The removed-knob test stays green only when the five names have no active references in the audited source, environment, test, or configuration files. The public API note in this plan must continue to state that the five fields are intentionally removed.

## Idempotence

The edits are deterministic and safe to reapply by checking the exact field/env names and documentation sentences first. Do not broaden deletion to similarly named settings without a fresh runtime-reader audit.

## Artifacts

The artifacts are this living ExecPlan, the focused architecture contract tests, the reconciled configuration/platform documentation, and the source diff removing only the five proven-dead knobs.

## Interfaces and Dependencies

`VfsConfig` and `VfsConfigRaw` remain public and re-exported by `studiofs-vfs`, but `readahead_seq_step`, `readahead_random_penalty`, `prefetch_max_per_cycle`, `max_drain_batch`, and `slab_flush_interval_ms` are intentionally removed. This is a source-level breaking change for consumers that construct or access those structs; no replacement fields are introduced. Existing `STUDIOFS_MOUNT_BACKEND=macfuse` compatibility selection, WinFSP cleanup override environment variables, shared configuration validation, and platform bridge routing remain behaviorally compatible.

The target dependency contract remains explicit in `crates/studiofs/Cargo.toml`: `studiofs-macfuse` under macOS, `studiofs-linux-fuse` under Linux, and `studiofs-winfsp` under Windows. The architecture test checks these target sections together with the executable routing markers and WinFSP cleanup parser so a host-specific test run cannot silently omit one supported platform.

## Revision Note

2026-08-07 21:39 +03:00 — Updated this living plan after review to add exact working directories and commands, expected outcomes, native-runner gap handling, cross-platform dependency/selector/override coverage, live documentation surfaces, and the intentional public API-removal decision.
