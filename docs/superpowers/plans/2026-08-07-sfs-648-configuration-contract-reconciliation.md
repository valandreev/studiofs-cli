# SFS-648 Configuration Contract Reconciliation

> **For agentic workers:** Follow this living ExecPlan with the TDD workflow. Keep the scope narrow: reconcile executable defaults and remove only configuration knobs proven to have no runtime reader.

## Purpose

Make the documented cross-platform configuration contract agree with the current executable behavior and remove zombie configuration surface without redesigning `VfsConfig`, `VfsConfigRaw`, or `VfsBootstrapPlan`.

## Progress

- [x] Read SFS-648, related Linear history, current HEAD, and repository guidance.
- [x] Confirm macFUSE is the current macOS default and WinFSP cleanup flush/purge defaults to disabled.
- [x] Audit the candidate knobs for runtime readers and preserve `upload_delay_ms`, which is still consumed by internal VFS upload scheduling.
- [x] Add red contract tests.
- [x] Reconcile documentation and remove proven-dead knobs end-to-end.
- [x] Run focused tests, lint/build checks, and record platform gaps.

## Surprises & Discoveries

- `crates/studiofs/src/cli/mount/backend.rs` already routes macOS `None` and empty selectors to `MacFuseMountBackend`; the stale opt-in wording is documentation drift, not an executable-default bug.
- `crates/studiofs-winfsp/src/lib.rs` already defines `DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP` as `false`; `docs/platform-bridges.md` and `docs/configuration.md` agree, while `docs/agents-coding-guide.md` is stale.
- `VfsBootstrapPlan` does not carry the candidate dead knobs, and the remaining configuration duplication is intentionally retained for this issue because a measured nested grouping has not been established.

## Decision Log

- Preserve the macFUSE default and keep `STUDIOFS_MOUNT_BACKEND=macfuse` as a compatibility selector.
- Preserve WinFSP cleanup flush/purge disabled by default; keep the explicit environment override for targeted stale-cache experiments.
- Remove only `readahead_seq_step`, `readahead_random_penalty`, `prefetch_max_per_cycle`, `max_drain_batch`, and `slab_flush_interval_ms`, because their current references are limited to config/default/overlay/test or legacy environment plumbing and no runtime consumer reads them.
- Do not remove `upload_delay_ms`: it remains part of the internal `VfsBootstrapPlan` and `StudioVfs` upload lifecycle even though public mount/settings input deliberately strips it.

## Outcomes & Retrospective

The executable and documented defaults now agree for Linux/libfuse3, macFUSE,
and WinFSP. The five proven-dead knobs have no active source, environment,
test-helper, or configuration-documentation references. The shared
configuration types remain flat; no nested grouping was introduced without a
measured benefit.

Validation passed for the two new SFS-648 contract tests, the updated SFS-485
vocabulary test, three VFS config validation tests, the macOS default selector
test, formatting, focused `-D warnings` checks for `studiofs`/`studiofs-vfs`
and `studiofs-winfsp`, and `git diff --check`. Native Windows behavior and
mounted FUSE/macFUSE validation were not available in this environment.

## Context

The issue acceptance surface is cross-platform:

- shared core: configuration types, environment overlays, and documentation contract tests;
- macOS/macFUSE: default backend selection and legacy selector compatibility;
- Linux/FUSE: preserve the existing Linux default and shared config behavior;
- Windows/WinFSP: cleanup flush/purge default and explicit override semantics.

Relevant files are `crates/studiofs/src/cli/mount/backend.rs`, `crates/studiofs/src/cli/mount/session.rs`, `crates/studiofs-vfs/src/vfs/{config.rs,vfs.rs,tests.rs}`, `crates/studiofs-vfs/tests/vfs_test.rs`, `crates/studiofs-winfsp/src/lib.rs`, `crates/studiofs/tests/architecture_vocabulary_test.rs`, and the affected configuration/platform documents.

## Plan

1. Add failing architecture tests that assert source defaults match their documented contract and that the five removed knobs have no active runtime/configuration surface.
2. Update stale macOS and WinFSP documentation/test vocabulary.
3. Delete the five proven-dead fields, defaults, normalization, conversions, environment mappings, test helper parsing, and dangling active documentation references.
4. Run focused red/green tests, format, warning-deny checks, and diff hygiene. Record unavailable native platform validation honestly.

## Concrete Steps

### 1. Red tests

Add the smallest table-driven contract test to `architecture_vocabulary_test.rs`. It must inspect the executable source markers and current documentation for the macOS default and WinFSP `false` default. Add a focused absence test over active source/configuration files for the five removed knobs. Run each new filtered test before implementation and retain the expected failure evidence in the execution notes.

### 2. Contract and cleanup implementation

Use `apply_patch` for the minimal edits. Update `docs/configuration.md`, `docs/macos-native-bridge-split.md`, `docs/perf/sfs-485-macos-native-bridge-readiness.md`, and `docs/agents-coding-guide.md`; update the existing SFS-485 architecture test expectation. Remove the five fields and all corresponding default/overlay/normalization/conversion entries, CLI/VFS environment parsing, test environment setup, and active spec/config references. Leave shared config grouping unchanged.

### 3. Validation

Run focused commands one at a time:

- `cargo test -p studiofs --test architecture_vocabulary_test configuration_contracts_match_executable_defaults -- --nocapture`
- `cargo test -p studiofs --test architecture_vocabulary_test removed_configuration_knobs_have_no_active_surface -- --nocapture`
- relevant `studiofs-vfs` config validation tests;
- macOS backend selector tests when the current target supports them;
- `cargo fmt --all -- --check`;
- `RUSTFLAGS='-D warnings' cargo check -p studiofs -p studiofs-vfs`;
- `git diff --check`.

Windows/WinFSP native tests and mounted FUSE/macFUSE tests are only reported as passing when their runners are actually available.

## Idempotence

The edits are deterministic and safe to reapply by checking the exact field/env names and documentation sentences first. Do not broaden deletion to similarly named settings without a fresh runtime-reader audit.

## Artifacts

- this living ExecPlan;
- the focused architecture contract tests;
- the reconciled configuration/platform documentation;
- the source diff removing only the five proven-dead knobs.

## Interfaces

No public API or dependency changes are planned. Existing backend selectors, WinFSP override environment variables, shared configuration validation, and platform bridge routing remain behaviorally compatible except for removal of ignored/zombie knobs that had no runtime effect.
