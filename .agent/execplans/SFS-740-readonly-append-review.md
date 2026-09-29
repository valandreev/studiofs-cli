# Align append flags with writable access modes

This ExecPlan is a living record for the SFS-740 review follow-up and follows
`.agent/PLANS.md`.

## Purpose / Big Picture

After this change, opening a file with read-only access plus `O_APPEND` behaves
as a read-only open on Linux and macOS, and it is classified as read-only by the
shared VFS. It will not reserve a writer slot, activate a writer overlay, or
disable the read-only cache path. Write-only, read-write, and truncating opens
remain rejected on read-only mounts.

## Progress

- [x] (2026-09-29) Confirm the platform flag mapping, VFS writer accounting,
  adapter gates, and existing append-only VFS compatibility tests.
- [x] (2026-09-29) Add VFS regressions first and confirm they fail because
  `RDONLY | APPEND` is currently classified as `SequentialWriter`, increments
  `write_open_count`, and registers a writer overlay.
- [x] (2026-09-29) Add the shared `OpenFlags::write_access_requested` contract and apply it
  to VFS classification and Linux, macFUSE, and WinFSP read-only gates.
- [x] (2026-09-29) Add or update focused tests for read-only append admission, writer
  accounting, and read-only mount rejection.
- [x] (2026-09-29) Run focused crate tests, formatting, Clippy, and diff checks; record
  platform-gated validation limits.
- [x] (2026-09-29) Preserve the existing initial `RDWR` access-class behavior
  after review identified the broader branch change.

## Surprises & Discoveries

- `OpenFlags::allows_write` currently treats every `APPEND` bit as write access,
  including `RDONLY | APPEND`. The POSIX mapper preserves both bits, so this
  combination reaches the VFS and all bridge gates.
- The VFS has an existing tested shorthand where bare `OpenFlags::APPEND` is a
  writable internal handle. Preserve that case while making `RDONLY | APPEND`
  read-only.
- The macFUSE test named `adapter_append_handle_fsync_survives_unlink_of_open_file`
  opens `O_RDONLY | O_APPEND` and then writes through it. Update that test to
  request `O_WRONLY | O_APPEND`, which is the valid append-writer case.
- Red test evidence: `cargo test -p studiofs-vfs --lib read_only_append --
  --nocapture` failed in three expected assertions; `test_open_flags_bitwise`
  failed because `RDONLY | APPEND` still allowed writes.

## Decision Log

- Decision: Define write access as `WRONLY`, `RDWR`, or bare `APPEND` when
  `RDONLY` is absent. Rationale: a read-only access mode must dominate the
  append modifier, while preserving the VFS's existing bare-append shorthand.
  Date/Author: 2026-09-29, Codex.
- Decision: Keep `posix_flags_to_open_flags`'s representation unchanged and
  centralize the interpretation in `OpenFlags`. Rationale: the adapter and VFS
  can share one rule without discarding the original append modifier.
  Date/Author: 2026-09-29, Codex.

## Outcomes & Retrospective

The shared flag contract now treats explicit `RDONLY` as dominant over
`APPEND`, while preserving bare `APPEND` as the existing internal writer
shorthand. VFS writer counts, access classification, overlays, hard-lock
checks, and all three platform read-only gates follow the same rule. Linux
tests now pin `EROFS` for write-only, read-write, and truncating opens.

A follow-up review caught that using `write_access_requested()` for the
non-append classifier branch also changed `RDWR` handles from their established
initial `RandomInteractiveRead` class to `RandomWriter`. The branch now remains
limited to `WRONLY`; a regression test first reproduced the change and then
passed after the correction.

Focused VFS and WinFSP tests, VFS and WinFSP Clippy, formatting, and
`git diff --check` passed. Linux-target compilation could not reach project
source because the Windows host lacks `x86_64-linux-gnu-gcc`, required while
building `ring`; native Linux FUSE and macOS FUSE tests remain unverified here.
No full or ignored test suite was run.

## Context and Orientation

`crates/studiofs-bridge/src/posix.rs` maps POSIX access modes and `O_APPEND` to
`studiofs_vfs::OpenFlags`. `crates/studiofs-vfs/src/vfs/file_session.rs` defines
those flags. `allows_write` is used by VFS handle accounting, access checks,
writer-cache setup, and platform cache decisions. The VFS also has a separate
`write_access_requested` helper for opening and write-lock checks. Linux FUSE,
macFUSE, and WinFSP each reject write-capable opens on read-only mounts at their
adapter boundary.

`O_RDONLY | O_APPEND` includes an explicit read-only access mode. The append bit
does not grant permission to write in that mode. Bare `OpenFlags::APPEND`,
however, is an existing VFS-internal append-writer shorthand and remains
write-capable.

## Plan of Work

In `crates/studiofs-vfs/src/vfs/file_session.rs`, add a documented
`OpenFlags::write_access_requested` accessor. It returns true for `WRONLY` or
`RDWR`, and for `APPEND` only when `RDONLY` is absent. Make `allows_write` use
that shared rule so existing handle counts, writer admission, cache leases, and
write checks agree. Update access classification in `crates/studiofs-vfs/src/access.rs`
and the open/overlay classifier in `crates/studiofs-vfs/src/vfs.rs` to use the
accessor directly.

Use the same accessor in the read-only open gates in
`crates/studiofs-linux-fuse/src/low_level.rs`,
`crates/studiofs-macfuse/src/adapter.rs`, and
`crates/studiofs-winfsp/src/context.rs`. Apply it to macFUSE's runtime-stats
open gate as well. Add Linux FUSE tests for accepted `O_RDONLY | O_APPEND` and
`EROFS` for `O_WRONLY`, `O_RDWR`, and `O_RDONLY | O_TRUNC`. Extend VFS tests to
prove `RDONLY | APPEND` does not count as a writer or activate an overlay, while
bare `APPEND` remains a writer. Adjust the macFUSE append-writer test to use
`O_WRONLY | O_APPEND` and pin read-only append acceptance and cache behavior.

## Concrete Steps

Run from `E:\dev\studiofs-cli`:

    cargo fmt --all -- --check
    cargo test -p studiofs-vfs --lib test_open_flags_bitwise -- --nocapture
    cargo test -p studiofs-vfs --lib active_open_overlay_tracks_only_mutating_handles -- --nocapture
    cargo test -p studiofs-vfs --lib read_only_append -- --nocapture
    cargo test -p studiofs-vfs --lib foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open -- --nocapture
    cargo test -p studiofs-winfsp --lib read_only_context_rejects_write_data_and_append_data_opens -- --nocapture
    cargo clippy -p studiofs-vfs --all-targets -- -D warnings
    cargo clippy -p studiofs-winfsp --all-targets -- -D warnings
    git diff --check

Run named macFUSE and Linux FUSE unit tests on their native/CI targets. A test
selection reporting zero tests is not evidence. Do not run full workspace or
ignored mount suites for this review fix.

## Validation and Acceptance

The VFS flag tests must show that `RDONLY | APPEND` does not request write
access, while `WRONLY`, `RDWR`, and bare `APPEND` still do. The writer-count and
overlay tests must show that read-only append opens do not retain writer state.
Linux FUSE and macFUSE must accept `O_RDONLY | O_APPEND` and reject write or
truncate modes on read-only mounts with `EROFS`. WinFSP must continue rejecting
write-data and append-data grants on a read-only mount.

Formatting, focused tests, Clippy, and `git diff --check` must pass. Unit tests
do not prove live FUSE or WinFSP mount behavior; report those platform boundaries
separately.

## Idempotence and Recovery

The source and tests can be edited or validated repeatedly without changing
runtime state. Preserve unrelated worktree changes. If a native target is not
available, retain the focused source/test change and state the missing target or
toolchain rather than substituting a zero-test host run.

## Artifacts and Notes

The review evidence is in the Rust source and focused test output. No external
service, mount, or `.env` value is needed.

## Interfaces and Dependencies

The shared interface is `studiofs_vfs::OpenFlags::write_access_requested(self)
-> bool`. It owns the distinction between a write access mode and an append
modifier. `OpenFlags::allows_write` remains the existing compatibility-facing
method and delegates to this accessor; no dependency or POSIX mapping changes
are required.

## Revision note

2026-09-29: Completed the review follow-up by aligning `RDONLY | APPEND` with
read-only access across VFS and adapter gates, adding focused regression
coverage, and recording the unavailable native Linux/macOS validation.
2026-09-29: Kept non-append `RDWR` handles on their original initial access
class after adding a regression for the review-discovered behavior change.
