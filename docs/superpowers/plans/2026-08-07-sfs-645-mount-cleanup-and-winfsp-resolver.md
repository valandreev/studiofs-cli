# SFS-645 Mount Cleanup and Single-Owner WinFSP Resolution

This ExecPlan is living documentation for SFS-645 and must stay aligned with
the implementation. The repository rules in `.agent/PLANS.md` apply.

## Purpose / Big Picture

After this change, a foreground `studiofs mount` that exits through a normal
backend return or an early failure will release its runtime resources, stop
diagnostics and profiling helpers, remove its persisted mount record, release
the lifecycle lock, and remove a temporary snapshot database. If more than one
teardown operation fails, the first mount error remains the returned error and
later teardown is still attempted; cleanup failures are added as context.

On Windows, the CLI and `studiofs-winfsp` will call the same architecture-aware
WinFSP runtime loader. The hard-coded x64 registry lookup in the CLI will no
longer be a second resolver. The behavior is observable through focused mount
session tests and the existing x86/x64/arm64 resolver mapping test.

## Progress

- [x] (2026-08-07) Read Linear issue SFS-645, confirmed it is assigned and `In Progress`.
- [x] (2026-08-07) Traced `MountSession::launch` → `PreparedMount::launch` → `launch_foreground` with codebase-memory MCP.
- [x] (2026-08-07) Confirmed the existing `studiofs-winfsp` resolver maps x86, x86_64, and aarch64 DLL stems, while `studiofs/src/main.rs` still hard-codes x64 registry paths.
- [x] (2026-08-07) Checked Tokio `Runtime::shutdown_timeout` semantics with Context7; it bounds waiting for blocking work but does not cancel already-running blocking tasks.
- [x] (2026-08-07) Added cleanup-order/error-preservation regressions and routed foreground launch through `MountRuntimeGuard`.
- [x] (2026-08-07) Made foreground teardown continue after individual cleanup errors and preserve VFS-owner drop ordering.
- [x] (2026-08-07) Routed CLI WinFSP preload through `studiofs-winfsp` and removed the duplicate x64 registry loader.
- [x] (2026-08-07) Ran focused tests, checks, Clippy, formatting, and diff hygiene validation.
- [x] (2026-08-07) Recorded final evidence and remaining unavailable validation in this plan.

## Surprises & Discoveries

- The current backend-failure tests already prove that the backend error is
  returned and that the PID/snapshot artifacts are removed when VFS shutdown
  succeeds. They do not cover a failure from an earlier `?` in teardown or
  prove that a later cleanup step runs after a cleanup error.
- `PreparedMount::launch_foreground` starts cleanup only after
  `backend.serve` returns, but `run_mount_vfs_shutdown(...) ?` and
  `stop_mount_diagnostics_host(...) ?` can exit before PID removal, lock release,
  snapshot removal, and runtime shutdown.
- `studiofs-winfsp` already owns the real DLL search and architecture mapping;
  its `ensure_winfsp_runtime_loaded` function is only `pub(crate)`, which is
  the smallest seam to expose to the CLI.
- Cleanup must stop the diagnostics host before VFS shutdown so a VFS shutdown
  error cannot skip diagnostics teardown; the guard then retains the runtime
  long enough to attempt its own bounded shutdown.
- Tokio documents that `shutdown_timeout` can leave already-running blocking
  tasks executing after the timeout. This plan keeps the existing configured
  timeout and treats it as a bounded wait, not cancellation.

## Decision Log

- Decision: Keep the cleanup guard in `crates/studiofs/src/cli/mount/session.rs`
  instead of adding a new cross-crate lifecycle abstraction.
  Rationale: all foreground resources and teardown helpers already meet in that
  module; a local owner is the smallest change that covers macFUSE, Linux/FUSE,
  and WinFSP through the shared `MountBackend` trait.
  Date/Author: 2026-08-07 / Codex.
- Decision: Stop the publisher and diagnostics host first, then attempt VFS
  shutdown, finish profiling, release VFS-retaining owners, shut down Tokio,
  and finally remove persisted mount artifacts.
  Rationale: diagnostics teardown must not be skipped when VFS shutdown fails;
  VFS-retaining owners must still be dropped before runtime shutdown, and PID,
  lock, and snapshot cleanup must run after every fallible step.
  Date/Author: 2026-08-07 / Codex.
- Decision: Make the existing library loader public and call it from `main`.
  Rationale: the library already selects `winfsp-x64.dll`, `winfsp-x86.dll`, or
  `winfsp-a64.dll` from the target architecture and searches executable and
  Program Files locations; moving that logic would duplicate it again.
  Date/Author: 2026-08-07 / Codex.

## Outcomes & Retrospective

Implemented and validated on the Windows host. Foreground mount teardown now
uses one local guard that attempts diagnostics, VFS, runtime, PID, lock, and
snapshot cleanup even when an earlier step fails. The first mount error is
preserved and later cleanup errors are attached or logged. The CLI now calls
the public architecture-aware `studiofs-winfsp` loader; the duplicate registry
lookup is gone.

Passed checks:

- `cargo test -p studiofs cleanup_ -- --nocapture`: 11 passed.
- `cargo test -p studiofs prepared_mount_launch -- --nocapture`: 5 passed.
- `cargo test -p studiofs foreground_shutdown_releases_vfs_handles_before_runtime_shutdown -- --nocapture`: 1 passed.
- `cargo test -p studiofs-winfsp winfsp_dll_stem_matches_supported_architectures --lib -- --nocapture`: 1 passed.
- `cargo check -p studiofs`, `cargo check -p studiofs-winfsp`, and
  `cargo clippy -p studiofs --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed. The latter
  reports only the checkout's expected LF-to-CRLF warnings.

Not run: a real mounted macFUSE, Linux/FUSE, or WinFSP driver smoke test and
the workspace-wide test suites. The WinFSP unit mapping test does not require
the driver. Tokio's existing timeout limitation remains: running blocking
tasks are not cancelled when the bounded shutdown wait expires.

## Context and Orientation

`crates/studiofs/src/cli/mount/session.rs` prepares a mount plan, opens local
metadata, creates the local VFS and Tokio runtime, writes the JSON mount record,
and invokes the selected platform bridge through `MountBackend::serve`. A
foreground mount owns a `MountLockGuard`, an optional temporary snapshot redb
database, a VFS `Arc`, a Tokio `Runtime`, a diagnostics host, profiling state,
and a write-path observability publisher.

The current `PreparedMount::launch_foreground` implementation calls the
backend, stops the publisher, shuts down VFS, stops diagnostics, drops owners,
removes the PID file, unlocks, removes the snapshot database, and shuts down
Tokio. Several of those calls use `?`, so an early failure skips the remaining
steps. `MountLockGuard::drop` only unlocks the lock file; it does not remove the
JSON record.

`crates/studiofs-winfsp/src/lib.rs` is compiled only on Windows. Its private
loader derives the DLL name from `std::env::consts::ARCH`, searches the current
executable directory and WinFSP Program Files directories, and caches the
result in a `OnceLock`. `crates/studiofs/src/main.rs` currently performs a
separate registry lookup and always asks for `winfsp-x64.dll`. The CLI already
depends on `studiofs-winfsp` on Windows, so no new dependency is needed.

The platform bridges are unchanged in this issue: Linux uses
`studiofs-linux-fuse`, macOS uses `studiofs-macfuse`, and Windows uses
`studiofs-winfsp`; all three enter the common foreground cleanup path through
`MountBackend`.

## Plan of Work

First add the smallest regression coverage around cleanup error collection. The
test will invoke the same helper used by the guard with two failing cleanup
steps and a side-effect list, then assert that the first error is retained and
the second step still ran. Keep the existing successful backend, failed
backend, snapshot, diagnostics, and VFS-owner tests intact; extend the failed
backend assertions if the guard changes their observable error chain.

Next introduce `MountRuntimeGuard` in `session.rs`. It will own the prepared
foreground state, mount plan metadata, optional snapshot path, optional mount
lock, whether the JSON mount record was persisted, and the optional diagnostics
host. `PreparedMount::launch` will transfer the foreground mount into this
guard before `write_pid_file` and `print_ready`, so failures in either method
also leave the guard responsible for cleanup.

The guard will run the existing teardown helpers without `?` short-circuiting.
It will stop the observability publisher and diagnostics host, attempt VFS
shutdown, finish profiling, drop all `StudioVfs`-retaining owners, shut down
Tokio, remove the PID record, release the lock, and remove the snapshot
database. A small `record_cleanup_error` helper will keep the first cleanup
error and log later cleanup errors. The final result will return the original
mount/backend error when one exists; otherwise it will return the first cleanup
error with the cleanup step named. Any helper whose current return type loses
the Tokio runtime when VFS shutdown fails will be adjusted only enough to keep
the runtime available for the later shutdown step.

Finally make `studiofs_winfsp::ensure_winfsp_runtime_loaded` public with a
short API comment, delete `init_winfsp_dll_path` and its unsafe registry/FFI
code from `main.rs`, and call the public resolver once under `cfg(windows)`.
Keep the CLI's current warning behavior on preload failure so the later mount
operation remains the authority for returning the actual mount error. Retain
the existing architecture mapping test and add only a missing candidate-path
assertion if the focused test exposes a gap.

## Concrete Steps

1. From `E:\dev\studiofs-cli`, add the cleanup error-preservation regression in
   `crates/studiofs/src/cli/mount/session.rs`, adjacent to the existing mount
   launch tests. Run the filtered `studiofs` tests and observe the new test
   fail if it is written against behavior not yet implemented.

2. Implement `MountRuntimeGuard` and route foreground launch through it. Keep
   daemon launch behavior unchanged: daemon PID guarding and child spawning do
   not own the foreground VFS runtime. Preserve the current backend error text
   (`FakeBackend error: boom`) and the current VFS-before-runtime drop
   invariant.

3. Change `crates/studiofs-winfsp/src/lib.rs` so the cached preload function
   is callable by the Windows CLI. Remove only the duplicate preload function
   and its call from `crates/studiofs/src/main.rs`; do not remove
   `cli/winfsp_bootstrap.rs`, whose registry access configures
   `MountUseMountmgrFromFSD` and is a separate operation.

4. Run the focused validation from `E:\dev\studiofs-cli`:

       cargo test -p studiofs prepared_mount_launch -- --nocapture
       cargo test -p studiofs foreground_shutdown_releases_vfs_handles_before_runtime_shutdown -- --nocapture
       cargo test -p studiofs-winfsp winfsp_dll_stem_matches_supported_architectures --lib -- --nocapture
       cargo check -p studiofs
       cargo check -p studiofs-winfsp
       cargo clippy -p studiofs --all-targets -- -D warnings
       cargo fmt --all -- --check
       git diff --check

   Do not run the workspace-wide test suites in this task; the repository
   instructions reserve those for the user/operator. If a Windows-only test is
   unavailable because WinFSP is not installed, report that exact prerequisite
   instead of calling it an integration pass.

5. Update this plan's Progress, Surprises, Outcomes, and Artifacts sections
   with the actual test names and command results. Review `git diff` to ensure
   only SFS-645 code, focused tests, and this plan are included.

## Validation and Acceptance

The cleanup acceptance test must show that a backend failure still returns
`FakeBackend error: boom`, the JSON PID record is gone, and the snapshot DB is
gone. The cleanup-order regression must show that the first cleanup error is
the returned cleanup error and that later cleanup steps executed after it.
The existing `foreground_shutdown_releases_vfs_handles_before_runtime_shutdown`
test must continue to pass, proving all VFS `Arc` owners are dropped before the
Tokio runtime shutdown boundary.

The WinFSP unit test must continue to map `x86_64` to `winfsp-x64`, `x86` to
`winfsp-x86`, and `aarch64` to `winfsp-a64`, with unsupported architectures
rejected. `cargo check` must prove the CLI can call the public loader without
reintroducing a second registry implementation. Formatting and
`git diff --check` must be clean.

This task does not claim a real mounted FUSE/WinFSP smoke pass unless the host
has the required driver/runtime and the focused smoke command is explicitly
run. Full workspace suites remain intentionally unrun.

## Idempotence and Recovery

All changes are source and test changes; rerunning focused Cargo commands is
safe. Test artifacts must remain under the existing `target/`-backed test
support temporary directories. If a focused test leaves a process or lock,
stop that test process and rerun the single test; do not delete a broad cache or
workspace directory. If implementation review reveals unrelated dirty files,
leave them untouched and scope the final diff to the SFS-645 changes.

## Artifacts and Notes

Expected implementation artifacts:

- `crates/studiofs/src/cli/mount/session.rs`: guard-owned foreground cleanup,
  error collection, and focused regression coverage.
- `crates/studiofs/src/main.rs`: no duplicate WinFSP registry/DLL resolver;
  one call to the library loader under `cfg(windows)`.
- `crates/studiofs-winfsp/src/lib.rs`: public cached preload entry point and
  architecture mapping tests retained or narrowly extended.
- `docs/superpowers/plans/2026-08-07-sfs-645-mount-cleanup-and-winfsp-resolver.md`:
  this living plan with final evidence.

Validation evidence from 2026-08-07:

    cargo test -p studiofs prepared_mount_launch -- --nocapture
    test result: ok. 5 passed; 0 failed

    cargo test -p studiofs cleanup_ -- --nocapture
    test result: ok. 11 passed; 0 failed

    cargo test -p studiofs foreground_shutdown_releases_vfs_handles_before_runtime_shutdown -- --nocapture
    test result: ok. 1 passed; 0 failed

    cargo test -p studiofs-winfsp winfsp_dll_stem_matches_supported_architectures --lib -- --nocapture
    test result: ok. 1 passed; 0 failed

    cargo check -p studiofs
    cargo check -p studiofs-winfsp
    cargo clippy -p studiofs --all-targets -- -D warnings
    Finished `dev` profile; no diagnostics

    cargo fmt --all -- --check
    git diff --check
    passed; `git diff --check` emitted only LF-to-CRLF checkout warnings

## Interfaces and Dependencies

The common mount interface remains `MountBackend::serve(BackendLaunch) ->
io::Result<()>` in `crates/studiofs/src/cli/mount/backend.rs`. The new local
guard must not add a trait with one implementation or a new dependency.

The existing cleanup helpers remain the platform-neutral boundaries:
`run_mount_vfs_shutdown`, `stop_mount_diagnostics_host`, and
`shutdown_mount_runtime` in `session.rs`. The Windows adapter boundary remains
`studiofs_winfsp::ensure_winfsp_runtime_loaded() -> std::io::Result<()>`, now
public so the CLI can use the same cached resolver as `mount_and_serve`.

The plan was created after reading the current SFS-645 issue, the codebase
knowledge graph, `.agent/PLANS.md`, the project bridge/test contracts, and the
current Tokio runtime documentation.
