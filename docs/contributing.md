# Contributing

This document describes the engineering contract for contributing to StudioFS:
how the verification model works, what is expected when platform-specific code
is added or changed, and who owns each part of CI.

---

## Licensing and provenance

Submit original StudioFS contributions under **GPL-3.0-only**, the project's
license in [LICENSE](../LICENSE). Confirm that you have authority to contribute
the work, including any required employer or client permission.

Identify copied, adapted, translated, or generated third-party material in your
contribution, with its source, license, and attribution requirements. Preserve
existing copyright and license notices; the project license does not replace
upstream grants. Flag uncertain rights before submitting code, documentation,
fixtures, or artwork. See the [source provenance audit](legal/source-provenance.md).

Changes to dependencies or packaging must also preserve the applicable
third-party notices and follow the [dependency audit](legal/rust-dependency-audit.md)
and [native platform distribution policy](legal/platform-distribution.md).
Use the [license compliance commands](legal/license-compliance.md) to generate
the notice review bundle and check release evidence; audit inventories alone
are not distribution notices.

---

## Verification Matrix

StudioFS is a cross-platform filesystem product. It targets Linux, macOS, and
Windows. The CI model reflects that — a green signal from Ubuntu alone is not
sufficient. Linux PR verification runs automatically in GitHub Actions
(`.github/workflows/linux.yml`) on Ubuntu and Rocky Linux. Hosted macOS and
Windows checks are manual-only while Windows work is primarily validated locally.

### What is verified automatically vs manually

| Check | Linux (Ubuntu + Rocky) | macOS (`macos-latest`) | Windows (`windows-latest`) |
|-------|----------------------------|----------------------|----------------------------|
| `cargo fmt` | Auto (Ubuntu quality) | — | — |
| `cargo clippy` (workspace) | Auto (Ubuntu quality) | Manual GitHub Action | Manual GitHub Action |
| `cargo clippy` (all features) | Auto (Ubuntu quality) | Manual GitHub Action | Manual GitHub Action |
| `cargo doc` | Auto (Ubuntu quality) | — | — |
| `cargo audit` ¹ | Auto (Ubuntu) | — | — |
| Workspace unit + compile tests | Auto (Ubuntu + Rocky) | Manual GitHub Action | Manual GitHub Action |
| Local-first VFS fast checks | Auto (explicit Ubuntu step; covered by Rocky workspace run) | — | — |
| PostgreSQL metadata + CLI regressions | Auto (Ubuntu; PostgreSQL 18) | — | — |
| Linux mount bridge | Manual release gate (`studiofs-linux-fuse` ignored mounted tests) | — | — |
| `studiofs-winfsp` unit tests | — | — | Manual GitHub Action |

### What is NOT verified automatically (manual / self-hosted only)

| Check | Notes |
|-------|-------|
| Platform bridge smoke tests (mount/unmount) | Require `/dev/fuse` or WinFSP kernel driver; marked `#[ignore]`. See [smoke workflow](#smoke-test-workflow) |
| S3-backed integration tests | Require live RustFS; the PostgreSQL-only metadata and CLI gate does not cover them |
| Mounted multi-client convergence tests | Require multi-process infrastructure and mounted filesystem setup |
| Real mounted E2E smoke/full run | Manual GitHub Action via `.github/workflows/linux-e2e.yml`; requires Docker plus `/dev/fuse` and uploads runner artifacts |

<a name="footnote-audit"></a>
¹ `cargo audit` reads the single workspace `Cargo.lock` and reports
vulnerabilities regardless of target platform. Running it once in Linux CI covers
the full dependency graph, including platform-specific crates such as
`studiofs-winfsp`. Platform-conditional vulnerabilities (e.g. a Windows-only
advisory on a `windows-sys` crate) are surfaced in the same audit run.
Accepted exceptions live in `.cargo/audit.toml`, each with its rationale and
removal issue; v0.2.0 ignores only the `quick-xml` advisories RUSTSEC-2026-0194
and RUSTSEC-2026-0195 (SFS-883). Unmaintained, unsound and yanked warnings are
printed but do not fail the gate.

**Compile/unit/smoke distinctions:**

- **Portable compile + clippy** — `cargo clippy` runs automatically in the Ubuntu quality gate. Hosted macOS and Windows clippy runs are available as manual GitHub Actions. Linux compile coverage comes from Ubuntu and Rocky workspace `cargo test` runs.
- **Portable unit tests** — the workspace `cargo test` run on each platform. Covers all crates except the platform-specific backend crate, which has a dedicated step.
- **PostgreSQL metadata and CLI regressions** — the Ubuntu gate starts an ephemeral PostgreSQL 18 service and runs the ignored metadata `pg_engine` and CLI binary tests serially, excluding the SFS-720 scalability benchmark. It prints the selected count and Cargo's pass/failure totals; a missing service or zero-test selection fails the gate.
- **Platform unit tests** — the named backend step in each platform lane (`studiofs-macfuse` on manual macOS, `studiofs-winfsp` on manual Windows). This is the primary signal for platform-specific code paths.
- **Platform smoke tests** — `mount_sanity` integration tests marked `#[ignore]`. These include both minimal lifecycle tests (session create/drop) and full I/O tests (mount → write → read → readdir → delete → unmount). Run via the `smoke.yml` workflow or manually on self-hosted runners with the full kernel driver available. Not a PR gate.
- **Other infra integration tests** — S3-backed, mounted, and multi-process tests still need their respective services or platform support and remain outside this gate.

---

## Workspace Lint Policy

StudioFS uses Cargo workspace lint inheritance. The root `Cargo.toml` owns the
shared lint baseline under `[workspace.lints]`; each member crate must opt in
with a root-level manifest table:

```toml
[lints]
workspace = true
```

Do not use a `[package] lints.workspace = true` entry. Cargo's supported member
syntax is the separate `[lints]` table above.

The baseline is intentionally conservative. New lints should start as warnings
only when they are low-noise across the workspace. Large hygiene migrations
such as broad Clippy groups, `unwrap_used`, or `expect_used` need their own
issue and focused cleanup plan rather than being folded into unrelated PRs.

---

## CI Ownership

### PR verification

Linux CI runs automatically on every pull request and `main` push. Mounted
E2E, macOS, and Windows verification are manual-only. Required checks, if
enabled, are enforced via **GitHub Branch Protection Rules** (Settings →
Branches → require status checks) — it is not a property of the workflow YAML
itself. The high-level execution model is:

```
PR / main push (linux.yml)  →  Quality gates, cargo audit
                            →  Ubuntu workspace tests, S3 and PostgreSQL tests
                            →  Rocky Linux workspace tests

Manual GitHub Actions  →  Linux mounted E2E (linux-e2e.yml)
                       →  macOS
                       →  Windows
```

| Job | Runner | Gate status |
|-----|--------|-------------|
| **Linux** | `ubuntu-24.04` + `rockylinux:9` container | Automatic |
| **Real mounted E2E** | `ubuntu-24.04` with Docker + `/dev/fuse` | Manual only |
| **macOS** | `macos-latest` | Manual only |
| **Windows** | `windows-latest` | Manual only |

### Reading CI failures

- **Quality gates / cargo audit failed** → fix formatting, clippy, doc, or audit issue.
- **Ubuntu tests failed** → a regression in portable Linux behavior, Ubuntu package/runtime compatibility, or the explicit local-first VFS fast checks.
- **Rocky tests failed** → a regression in Rocky/RHEL-family package/runtime compatibility or a Linux backend compile/unit path that differs from Ubuntu.
- **Linux failed, mounted backend** → inspect the
  `studiofs-linux-fuse`/libfuse3 callback, invalidation, async-write, and
  unmount logs; confirm the runner exposed `/dev/fuse` and `fusermount3`.
- **Manual Windows failed, platform step** → a regression in `studiofs-winfsp` (the entire
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

`.github/workflows/smoke.yml` provides manual backend smoke tests that exercise
real mount/unmount + file I/O through the platform backends:

| Trigger | Behavior |
|---------|----------|
| `workflow_dispatch` | Manual run — all platforms |

The smoke workflow is **not** a required PR gate and never starts
automatically. It provides best-effort signal on GitHub-hosted runners and
reliable signal on self-hosted runners when available. macOS smoke tests run
with `continue-on-error: true` because the macFUSE system extension typically
requires a reboot to load on hosted runners.

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

- "Manual GitHub Actions macOS verification covers this — the macOS job runs `cargo test -p studiofs-macfuse --lib` which exercises this path."
- "This path requires a live WinFSP mount. I validated locally with `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-winfsp --test mount_sanity -- --ignored`."
- "This path is Windows-only. I validated it locally, or by manually running the GitHub Actions Windows job, via the `studiofs-winfsp` unit step on `windows-latest`."

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
| Platform test requiring WinFSP driver | `tests/mount_sanity.rs` with `#[ignore]` |
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
- [ ] New crates inherit workspace lints with `[lints] workspace = true`
- [ ] `cargo machete --with-metadata` reviewed when direct dependencies change; false positives belong in `[package.metadata.cargo-machete].ignored` with a short justification comment near the manifest entry
- [ ] New or changed platform-specific code explains how it is verified (see rules above)
- [ ] New public items have `///` doc comments
- [ ] `docs/tests.md` regression list updated if new named regressions are added

---

## Related Docs

- [Development](development.md) — build, local setup, CI matrix summary
- [Tests Guide](tests.md) — full test command matrix, platform-gated test rules, CI coverage table
- [Architecture Overview](architecture.md)
- [Configuration Reference](configuration.md)
