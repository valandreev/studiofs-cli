# SFS-249 Simple Local-First CLI Surface Design

## Status

Approved for implementation on 2026-04-16.

## Goal

Replace the current operator-facing `STUDIOFS_*` tuning sprawl with a small
Local-First CLI surface that focuses on data-cache and high-value IO controls,
while keeping metadata-cache internals and most runtime heuristics off the
standard operator path.

## Problem

The repository already moved toward a simpler mount UX:

- `studiofs mount` publicly exposes `--cache-dir` and `--cache-size`
- the canonical rewrite contract says metadata caches should stay internal
- active docs/help still present a wide `STUDIOFS_*` matrix as if it were the
  normal operator model

This creates a mismatch between the rewrite contract and the actual operator
story. The code still supports many env overrides through `VfsConfigRaw` and
`overlay_vfs_env`, but that does not mean all of them should be treated as
first-class public controls.

## Decision

`SFS-249` will be implemented as a narrow operator-surface cleanup, not as a
new broad runtime tuning feature.

### Public surface

Keep the existing core mount inputs:

- `--cache-dir`
- `--cache-size`
- `--principal`
- `--profile`
- `--snapshot-id`
- `--endpoint`

Add a small first-class IO tuning group for operator-relevant behavior:

- `--read-buffer`
- `--readahead`
- `--prefetch`
- `--writeback-delay`

These flags map onto already-existing validated runtime config fields and give
operators a coherent way to control read/write behavior without exposing the
entire internal runtime model.

### Internal by default

The following stay internal and are not promoted to normal CLI flags:

- dentry cache sizing
- path shortcut cache sizing
- attr read cache sizing
- readdir attr hydration knobs
- metadata invalidation polling cadence
- ACL cache capacity
- most QoS/AIMD worker and slot counters

These remain code-level or expert-only overrides because they tune internal
implementation details rather than the primary Local-First operator contract.

### Transitional compatibility

Existing `STUDIOFS_*` env overrides remain supported only where needed for a
safe migration. Active docs/help should stop teaching them as the preferred
operator model.

The simplified messaging is:

- operators use mount flags and `studiofs settings`
- developers can still use internal/transitional env overrides when debugging
  or changing defaults
- metadata-cache defaults are documented with code locations, not surfaced as a
  standard tuning matrix

## Files In Scope

- `crates/studiofs/src/cli/mount/mod.rs`
- `crates/studiofs/src/cli/mount/session.rs`
- `crates/studiofs/src/cli/settings.rs`
- `crates/studiofs/src/cli/mod.rs`
- `docs/configuration.md`
- `docs/examples.md`
- `docs/juicefs-local-first-rust-contract.md` if wording needs alignment

## Explicit Non-Goals

- implementing bridge-specific tuning from `SFS-250`
- redesigning janitor/recovery knobs from `SFS-251`
- deleting every historical term across the repo; broad cleanup remains for
  `SFS-252`
- turning `settings.vfs` into a large public user-facing config product

## Testing Strategy

Use focused verification only:

- CLI parse tests for new flags and precedence
- mount startup tests for CLI-over-config behavior
- help text checks where present
- formatting

Do not run the full workspace suite in this task.
