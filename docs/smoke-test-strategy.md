# Backend Smoke-Test Strategy

This document defines the smoke-test strategy for real mount validation of
StudioFS on Linux, macOS, and Windows. It covers execution models, minimum
scenarios, backend constraints, and CI integration.

---

## Goals

Smoke tests validate that mount backends are *operationally alive*, not just
syntactically valid. They catch regressions in:

- mount startup failures
- shutdown / unmount hangs
- broken mount options or platform-specific assumptions
- missing runtime dependencies (FUSE driver, WinFSP DLL)
- file I/O correctness through the mounted filesystem

Smoke tests complement — not duplicate — the unit/compile coverage in the
main CI pipeline (`rust.yml`).

---

## Minimum Smoke Scenario

Each backend must exercise the following lifecycle:

1. **Mount** — start the filesystem backend with a real mount point
2. **Write** — create a small file and write known payload through the OS
3. **Read** — read the file back through the mount and verify contents
4. **List** — `readdir` the mount root and verify the created file appears
5. **Delete** — remove the test file
6. **Unmount** — clean shutdown with no hangs or crashes

All smoke tests use an in-memory VFS backed by a temporary Redb metadata store.
A live S3/PostgreSQL backend is **not** required — the VFS dirty buffer serves
reads for recently written data without an upstream data store.

---

## Execution Model Per OS

### Linux — `ubuntu-latest` (GitHub-hosted)

| Aspect | Detail |
|--------|--------|
| **Runner** | `ubuntu-latest` |
| **Driver** | `/dev/fuse` (present on GitHub-hosted Ubuntu runners) |
| **Mount mode** | StudioFS raw `/dev/fuse`, no `libfuse3` link |
| **Mount options** | raw JuiceFS-like profile (no `AllowOther` — avoids needing `user_allow_other` in `/etc/fuse.conf`) |
| **Feasibility** | **High** — `/dev/fuse` exists on hosted runners and raw FUSE can mount without libfuse |
| **Gating** | Manual dispatch only |

### macOS — `macos-latest` (GitHub-hosted)

| Aspect | Detail |
|--------|--------|
| **Runner** | `macos-latest` |
| **Driver** | macFUSE (`brew install --cask macfuse`) |
| **Mount mode** | StudioFS native macFUSE bridge |
| **Feasibility** | **Low on hosted runners** — macFUSE installs a system extension that typically requires a reboot or Security & Privacy approval before the kernel extension loads. `brew install` alone does not guarantee the kext is active. |
| **Gating** | `continue-on-error: true` on hosted runners; self-hosted runners are the reliable path |

**Self-hosted contract:** a macOS self-hosted runner with macFUSE pre-installed
and the system extension approved (via MDM profile or manual approval). The
runner must expose `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1`.

### Windows — `windows-latest` (GitHub-hosted)

| Aspect | Detail |
|--------|--------|
| **Runner** | `windows-latest` |
| **Driver** | WinFSP (`choco install winfsp -y`) |
| **Mount mode** | WinFSP kernel driver (delayed-load DLL) |
| **Mount point** | Free drive letter, default `Z:` |
| **Feasibility** | **Medium–High** — `choco install` runs with admin privileges and installs the WinFSP kernel driver. Most GitHub Windows runners allow kernel driver loading. Mount Manager paths (`\\.\Z:`) require Administrator. |
| **Gating** | Manual dispatch only |

---

## CI Integration

### Workflow: `.github/workflows/smoke.yml`

Smoke tests run in a **separate workflow** from the main `rust.yml` pipeline.
This is deliberate:

- Smoke tests are slower and may be flaky on hosted runners
- They should not block standard PR merges
- They provide best-effort signal on hosted infrastructure

#### Trigger conditions

| Trigger | Behavior |
|---------|----------|
| `workflow_dispatch` | Manual run, all platforms |

#### Job structure

```
linux-fuse
macos-fuse    (continue-on-error)
windows-winfsp
```

Manual dispatch runs all smoke jobs unconditionally.

### Artifact collection

On failure, each smoke job uploads test output and temporary file artifacts
with 7-day retention. This enables post-mortem debugging without SSH access to
the runner.

### Step timeouts

Each smoke step has a 5-minute timeout. If mount or unmount hangs, the step
is killed cleanly. The job-level timeout is 15 minutes.

---

## Hosted vs Self-Hosted Responsibilities

| Platform | Hosted runner | Self-hosted runner |
|----------|---------------|--------------------|
| **Linux** | Full smoke coverage expected | Not required unless hosted runners regress |
| **macOS** | Best-effort (`continue-on-error`) | Required for reliable mount validation |
| **Windows** | Full smoke coverage expected | Fallback if hosted driver loading regresses |

### Self-hosted runner contract

If self-hosted runners are provisioned:

1. Pre-install the required kernel driver (macFUSE / WinFSP)
2. Ensure the driver is loaded and active (not just installed)
3. Set `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1` in the runner environment
4. For Windows: set `STUDIOFS_WINFSP_TEST_MOUNT_POINT` to a free drive letter
5. Tag the runner with `self-hosted` + platform label
6. Document the runner in this file and in `docs/contributing.md`

---

## Test Architecture

### FUSE smoke tests (`crates/studiofs-fuse/tests/mount_sanity.rs`)

- **Session control**: uses the raw `/dev/fuse` session loop in a background
  thread.
- **Unmount**: the main thread calls OS FUSE tooling after the smoke read.
- **Cleanup**: the raw mount guard detaches the mount during session teardown.

### WinFSP smoke tests (`crates/studiofs-winfsp/tests/mount_sanity.rs`)

- **Session control**: uses `mount_and_serve()` in a background thread with a
  named Win32 event for external shutdown.
- **Unmount**: the main thread signals the named event after I/O completes,
  which causes `mount_and_serve` to return cleanly.
- **Cleanup**: the test harness now always signals the named event and joins
  the mount thread even if the test body panics, so one failure does not leave
  the drive letter mounted for later cases.
- **Online-only coverage**: the post-upload WinFSP cases run only when the
  resolved S3 endpoint is reachable. On hosted CI without a live backend they
  self-skip and the job still validates the basic mount/unmount and I/O smoke
  path.
- **Throughput coverage**: hosted Windows uses a conservative AJA sequential
  writer floor as a path-health check. Treat sustained performance validation
  as self-hosted hardware coverage, not a GitHub-hosted runner SLA.

### VFS setup (shared)

Both platforms use the `make_test_vfs` helper:

1. Create temporary Redb metadata store
2. Initialize root inode
3. Set `FormatInfo` with a dummy S3 endpoint (`http://127.0.0.1:9000`)
4. Create `StudioVfs` with default config, small cache budgets
5. Call `vfs.start()` to spawn background workers

The dummy S3 endpoint means background upload workers will fail — this is
expected for the baseline smoke path. Smoke test I/O hits the dirty buffer
(memory-backed) which serves reads for recently written data without a live
data store. WinFSP post-upload/finalization cases automatically upgrade to
online coverage when a reachable S3 endpoint is configured.

---

## Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS` | Yes | — | Gate: must be `1` to enable smoke tests |
| `STUDIOFS_WINFSP_TEST_MOUNT_POINT` | Windows only | — | Drive letter for WinFSP mount (e.g. `Z:`) |
| `STUDIOFS_WINFSP_AJA_MIN_THROUGHPUT_MIB_PER_SEC` | Windows only | `64` in test code, `16` in hosted CI | Minimum AJA sequential write throughput floor; legacy alias: `STUDIOFS_WINFSP_WRITE_MIN_THROUGHPUT_MIB_PER_SEC` |

---

## Running Smoke Tests Manually

### Linux / macOS

```bash
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 \
  cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1
```

### Windows (PowerShell)

```powershell
$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS = '1'
$env:STUDIOFS_WINFSP_TEST_MOUNT_POINT = 'Z:'
cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1
```

---

## Tracking

| Item | Status |
|------|--------|
| Strategy document | ✅ This file |
| FUSE mount+I/O smoke test | ✅ `fuse_mount_write_read_unmount_smoke` |
| WinFSP mount+I/O smoke test | ✅ `winfsp_mount_write_read_unmount_smoke` |
| CI workflow (`smoke.yml`) | ✅ `.github/workflows/smoke.yml` |
| Self-hosted runner provisioning | Deferred — documented contract above |
| Linear issue | SFS-133 |
