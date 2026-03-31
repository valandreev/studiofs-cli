# Contributing

This document describes the engineering contract for contributing to StudioFS:
how the verification model works, what is expected when platform-specific code
is added or changed, and who owns each part of CI.

---

## Verification Matrix

StudioFS is a cross-platform filesystem product. It targets Linux, macOS, and
Windows. The CI model reflects that — a green signal from Ubuntu alone is not
sufficient.

### What is verified automatically on each push / PR

| Check | Linux (`ubuntu-latest`) | macOS (`macos-latest`) | Windows (`windows-latest`) |
|-------|------------------------|----------------------|----------------------------|
| `cargo fmt` | ✅ (Quality Gates job) | — | — |
| `cargo clippy` (workspace) | ✅ (Quality Gates job) | ✅ | ✅ |
| `cargo clippy` (all features) | ✅ (Quality Gates job) | ✅ | ✅ |
| `cargo doc` | ✅ (Quality Gates job) | — | — |
| `cargo audit` ¹ | ✅ (Quality Gates job) | — | — |
| Workspace unit + compile tests | ✅ | ✅ | ✅ |
| SFS-121 offline VFS fast suite | ✅ | — | — |
| `studiofs-fuse` Linux unit tests | ✅ | — | — |
| `studiofs-fuse` macOS unit + non-mount integration tests | — | ✅ | — |
| `studiofs-winfsp` unit tests | — | — | ✅ |

### What is NOT verified automatically (manual / self-hosted only)

| Check | Notes |
|-------|-------|
| Platform bridge smoke tests (mount/unmount) | Require `/dev/fuse` or WinFSP kernel driver; marked `#[ignore]`. See [smoke workflow](#smoke-test-workflow) |
| S3 / PostgreSQL integration tests | Require live RustFS + PostgreSQL; all marked `#[ignore]` |
| Cross-client convergence tests | Require multi-process infra setup; all marked `#[ignore]` |

<a name="footnote-audit"></a>
¹ `cargo audit` reads the single workspace `Cargo.lock` and reports
vulnerabilities regardless of target platform. Running it once on Linux covers
the full dependency graph, including platform-specific crates such as
`studiofs-winfsp`. Platform-conditional vulnerabilities (e.g. a Windows-only
advisory on a `windows-sys` crate) are surfaced in the same audit run.

**Compile/unit/smoke distinctions:**

- **Portable compile + clippy** — `cargo clippy` runs in Quality Gates (Linux), macOS, and Windows jobs. The Linux job does not repeat clippy; compile coverage on Linux comes from the workspace `cargo test` run. Together the three jobs catch API and cfg-gated breakage on all supported targets.
- **Portable unit tests** — the workspace `cargo test` run on each platform. Covers all crates except the platform-specific backend crate, which has a dedicated step.
- **Platform unit tests** — the named backend step in each platform job (`studiofs-fuse` on Linux and macOS, `studiofs-winfsp` on Windows). This is the primary signal for platform-specific code paths.
- **Platform smoke tests** — `mount_sanity` integration tests marked `#[ignore]`. These include both minimal lifecycle tests (session create/drop) and full I/O tests (mount → write → read → readdir → delete → unmount). Run via the `smoke.yml` workflow or manually on self-hosted runners with the full kernel driver available. Not a PR gate.
- **Infra integration tests** — all `#[ignore]` tests requiring live S3 + PostgreSQL. Not a PR gate.

---

## CI Ownership

### Required PR gates

All four jobs are intended to be required for a PR to merge. This is enforced
via **GitHub Branch Protection Rules** (Settings → Branches → require status
checks) — it is not a property of the workflow YAML itself. If you administer
the repository, verify that all four checks are listed as required. The
dependency graph is:

```
quality  →  linux
         →  macos
         →  windows
```

A `quality` failure short-circuits before consuming macOS or Windows runner minutes.

| Job | Runner | Gate status |
|-----|--------|-------------|
| **Quality Gates** | `ubuntu-latest` | Required — blocks all platform jobs |
| **Linux** | `ubuntu-latest` | Required |
| **macOS** | `macos-latest` | Required |
| **Windows** | `windows-latest` | Required |

### Reading CI failures

- **Quality Gates failed** → fix formatting, clippy, doc, or audit issue. All platform
  jobs are blocked and have not started; no platform signal to interpret yet.
- **Linux failed, platform step** → a regression in a `cfg(target_os = "linux")` path
  inside `studiofs-fuse` or in portable workspace code. The named step
  `cargo test (studiofs-fuse — Linux paths)` will identify the failing binary.
- **macOS failed, platform step** → a regression in a `cfg(target_os = "macos")` path
  inside `studiofs-fuse`. The named step
  `cargo test (studiofs-fuse — macOS paths)` will identify it.
- **Windows failed, platform step** → a regression in `studiofs-winfsp` (the entire
  crate is `#![cfg(windows)]`). The named step `cargo test (studiofs-winfsp)` will
  identify it.
- **Platform job failed, workspace step** → a portable regression that only triggers on
  that OS's toolchain or standard library. Investigate the full workspace run step output.

### When self-hosted infrastructure is expected

Self-hosted runners are not currently provisioned. Tests that require a live
kernel driver (FUSE, WinFSP) or external services (S3, PostgreSQL) must be run
manually on the developer's own machine before merging platform-bridge changes.
See [tests.md — Platform Bridge Smoke Tests](tests.md#platform-bridge-smoke-tests)
for the exact commands.

### Smoke test workflow

`.github/workflows/smoke.yml` provides automated backend smoke tests that
exercise real mount/unmount + file I/O through the platform backends:

| Trigger | Behavior |
|---------|----------|
| `workflow_dispatch` | Manual run — all platforms |
| Weekly schedule (Mon 4 AM UTC) | Automated regression detection |
| PR with `smoke-test` label | On-demand PR validation for bridge changes |

The smoke workflow is **not** a required PR gate. It provides best-effort signal
on GitHub-hosted runners and reliable signal on self-hosted runners when
available. macOS smoke tests run with `continue-on-error: true` because the
macFUSE system extension typically requires a reboot to load on hosted runners.

See [docs/smoke-test-strategy.md](smoke-test-strategy.md) for the full strategy
covering hosted vs self-hosted responsibilities and self-hosted runner contract.

If self-hosted runners are added in the future, they must be documented here,
in the workflow file comment block, and in
[docs/smoke-test-strategy.md](smoke-test-strategy.md).

---

## Rules for Platform-Specific Code

These rules apply any time a PR introduces or expands code guarded by
`cfg(windows)`, `cfg(target_os = "macos")`, `cfg(target_os = "linux")`, or
any other host-OS conditional that routes to platform-specific behavior.

### 1. Explain how the platform path is verified

A PR that adds or meaningfully changes a `cfg`-guarded branch **must** include
one of the following in the PR description:

- "CI covers this — the macOS job runs `cargo test -p studiofs-fuse --lib --tests` which exercises this path."
- "This path requires a live FUSE mount. I validated locally with `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored`."
- "This path is Windows-only. CI covers it via the `studiofs-winfsp` unit step on `windows-latest`."

Do not say "CI will catch it" without identifying *which* CI step exercises the new path.

### 2. Use the narrowest `cfg` guard

Prefer the narrowest target that reflects the actual constraint:

```rust
// ✅ correct — fusermount3 is a Linux concept
#[cfg(target_os = "linux")]
fn unmount_linux() { ... }

// ✅ correct — macOS-specific mount option
#[cfg(target_os = "macos")]
fn set_volname() { ... }

// ✅ correct — WinFSP is Windows-only
#[cfg(windows)]
fn drive_letter_from_path() { ... }

// ✅ acceptable — generic POSIX interface shared by Linux and macOS
// Use this when the behavior is identical on both platforms.
#[cfg(unix)]
fn close_fd(fd: RawFd) { ... }

// ❌ avoid when the behavior is OS-specific — use target_os instead
// cfg(unix) also fires on FreeBSD, OpenBSD, and other platforms where
// the behavior may differ or the crate does not target them.
#[cfg(unix)]
fn unmount_via_fusermount() { ... }  // fusermount3 is Linux-only
```

Rule of thumb: use `#[cfg(unix)]` for standard POSIX APIs that behave the same
on both Linux and macOS (file descriptors, signals, `libc` calls). Use
`#[cfg(target_os = "linux")]` or `#[cfg(target_os = "macos")]` when the
behavior, the system call, or the dependency is specific to one OS.

For tests, the same rule applies. See [tests.md — Rules for writing platform-gated unit tests](tests.md#rules-for-writing-platform-gated-unit-tests) for the full guidance.

### 3. Document backend constraints that CI cannot cover

If a change adds or modifies behavior that only manifests with a kernel driver
(FUSE, WinFSP) or a real mount point, add a comment in the relevant test or
source file:

```rust
// NOTE: full mount/unmount behavior requires a live FUSE or WinFSP driver.
// The CI unit step does not exercise this code path end-to-end.
// Validate manually: STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test ...
```

Do not hide this constraint. Surfacing it clearly is the correct outcome;
silence is not.

### 4. Place new tests so CI selection stays clear

| Test type | Where to put it |
|-----------|-----------------|
| Platform unit test (no OS driver needed, no mounts) | `src/` with `#[cfg(target_os = "...")]` guard |
| Platform test requiring external crate or heavy setup | `tests/` with `#[cfg(target_os = "...")]` guard |
| Platform test requiring FUSE or WinFSP driver | `tests/mount_sanity.rs` with `#[ignore]` |
| Portable test | `src/` or `tests/` without platform guard |

The CI coverage table in [tests.md](tests.md#ci-coverage-per-platform) maps
each crate step to its runner. A new test placed in the wrong layer can silently
go uncovered — use the table to verify which CI step exercises your new test.

### 5. Do not use `#[ignore]` as a substitute for `cfg` guards

`#[ignore]` skips a test at runtime regardless of platform. A test that is only
meaningful on Windows should use `#[cfg(windows)]` so it does not compile at all
on Linux or macOS. Combining both is acceptable for mount-requiring tests that
also need a kernel driver:

```rust
#[cfg(target_os = "macos")]
#[test]
#[ignore] // requires macfuse driver; run with STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1
fn mount_sanity_macos() { ... }
```

For crates that only support specific platforms, use `compile_error!` in the
crate root to make unsupported builds fail loudly rather than silently
producing non-functional code:

```rust
// In studiofs-winfsp/src/lib.rs (already guarded by #![cfg(windows)])
// — the cfg guard at the crate root is sufficient; compile_error! is only
// needed if a portable crate gains non-portable code without a crate-root
// guard.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
compile_error!("studiofs only supports Linux, macOS, and Windows");
```

---

## Commit and PR Guidelines

### Commit message format

```
<type>: <subject>

[optional body]
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `ci`

### PR checklist

- [ ] `cargo fmt --all` applied
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace --all-targets -- --nocapture` passes
- [ ] New or changed platform-specific code explains how it is verified (see rules above)
- [ ] New public items have `///` doc comments
- [ ] `docs/tests.md` regression list updated if new named regressions are added

---

## Related Docs

- [Development](development.md) — build, local setup, CI matrix summary
- [Tests Guide](tests.md) — full test command matrix, platform-gated test rules, CI coverage table
- [Architecture Overview](architecture.md)
- [Configuration Reference](configuration.md)

