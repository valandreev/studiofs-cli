# Migrate the Linux tray to KSNI for 0.2.1

This living ExecPlan follows `.agent/PLANS.md`. The specification is Linear
SFS-884, https://linear.app/valandreev/issue/SFS-884. Work starts in
`/home/val/dev/studiofs-cli`, on
`feature/sfs-884-021-migrate-linux-tray-to-the-gtk-free-ksni-backend`, with a clean
working tree.

## Purpose / Big Picture

Remove the vulnerable GTK3/GLib tray dependency path while retaining the Linux
tray indicator. KSNI implements StatusNotifierItem, the desktop tray protocol
over a user's session D-Bus. A compatible desktop host displays the icon; its
absence must never prevent a mount from running. Windows and macOS retain their
native implementations. Synchronization safety, diagnostics polling, animation,
and mount result propagation remain unchanged.

## Progress

- [x] Inspect issue, current branch, contracts, tray callers and upstream APIs.
- [x] Run baseline tray tests and demonstrate missing-watcher regression.
- [x] Select upstream KSNI on Linux and verify backend startup/update/drop.
- [x] Update CI prerequisites, runtime guides, release notes and license evidence.
- [x] Run focused tests/check/Clippy, strict notices, graph/linkage checks and review.
- [ ] Release acceptance on real KDE/GNOME and mounted Linux, native Windows/macOS,
  and the Rust 1.95 release toolchain (unavailable in this local environment).

## Surprises & Discoveries

`tray-icon 0.26.1` requires Rust 1.90, below the workspace's 1.95 minimum.
Its `ksni` feature enables `muda-snapshot`, without GTK. The default feature is
AppIndicator and must stay disabled. The 0.26 watcher fix uses
`assume_sni_available(true)` to permit late watcher registration. Its destructor
joins the menu watcher and waits for KSNI shutdown. Builder, RGBA icon, tooltip,
and update APIs used by StudioFS remain compatible.

The Linear shared-notes tool is advertised but returned `Tool get_agent_context
not found`; the issue and repository are the available sources of context.

The old backend failed the isolated-bus regression by panicking when its
AppIndicator shared library was absent. With KSNI the same test passes. An
invalid session bus returns an error rather than panicking. The two new tests
run individually with one passing test each; the ordinary focused run has
25 passing tests and skips these two environment-specific probes.

The resolved KSNI version is 0.3.7 (Rust 1.80); `muda 0.21.2` and `tray-icon
0.26.1` require 1.90, while `zbus 5.19.0`/`zvariant 5.15.1` require 1.87.
No package manifest in the resolved metadata declares a minimum above 1.95.
Actual local compilation uses Rust 1.99.1; Rust 1.95 execution remains a release
toolchain check, not a claimed local pass.

The renewal adds/replaces 36 registry identities and removes 44, leaving
649 registry and 12 workspace packages. All changed archives and primary
notice evidence were verified. `ksni`'s standalone `Unlicense` requires an
exact-version policy entry after reviewing its packaged dedication/disclaimer.
Cargo-deny rejected it before that entry and passes afterward. Policy probes
still accept MIT and reject AGPL/missing declarations. Strict notices report
zero unresolved evidence and reproduce the additional futures-lite/parking
third-party attributions.

An isolated D-Bus harness under `target/sfs-884/` uses the exact compiled
`tray-icon` library, a mock watcher and observable registration callbacks.
It passed late watcher startup, icon pixel/tooltip reads for four states,
watcher name release/reacquisition, and backend bus-name disappearance after
drop. There are no fixed coordination sleeps. This checks protocol behavior;
it does not prove a real desktop or StudioFS mount. Sandbox restrictions require
the local D-Bus fixture to run with approved socket access. While Cargo check
held its build lock, two wrapper commands timed out before executing tests;
rerunning the already built test executable directly passed both tests.

Independent review found a stray Rocky package token left by a text replacement;
it was removed. YAML parsing and `bash -n` on the affected setup/linkage scripts
pass. A synthetic check of the actual release linkage expression caught missing
AppIndicator suffixes; the corrected expression rejects both `*appindicator3`
names and accepts the retained libfuse3/libc dependencies.

## Decision Log

Keep the existing winit event loop, ticker, diagnostics monitor and mount thread.
They already own shutdown and propagate the mount result. Headless event-loop
failure already falls back to direct mount dispatch before spawning a mount
thread. No custom D-Bus implementation, menu, retry policy or status model is
needed. Drop the tray after helper shutdown, before waiting for a still-running
mount, so backend resources are released when the UI exits.

Use `tray-icon 0.26.1` with disabled defaults for Windows/macOS and a separate
Linux declaration with only `ksni`. Review its pinned sources and the resolved
transitive dependency licenses/checksums before intentionally renewing inventory.
Follow the repository's focused validation rule over skills requesting a full
workspace suite; this task does not authorize that suite.

## Context and Orientation

`crates/studiofs/Cargo.toml` owns target dependencies. `src/tray.rs` dispatches
foreground mounts through `dispatch_mount_with_auto_tray`, while detached mounts
bypass it. `TrayApp` observes local health and updates tinted images. Its
Booting/Active/Idle/Unavailable classifier fails closed on stale telemetry.
The only caller is `crates/studiofs/src/cli/mount/mod.rs`.
`docs/legal/generate-rust-dependency-inventory.py` records all lockfile packages;
`generate-third-party-notices.py --release` validates the reviewed baseline and
requires complete notice evidence. `.github/workflows/{linux,linux-e2e,release}.yml`
previously installed obsolete GTK/AppIndicator/libxdo prerequisites; those
packages are removed by this change. Runtime instructions
are in `docs/releases.md`, `docs/tray.md`, and both Linux installation guides.

## Plan of Work

First run the existing focused tray tests. Add opt-in Linux tests in the existing
tray test module: absent watcher on an isolated session bus must permit creation,
updates and destruction; an invalid session bus must return an initialization
error. Run them with external bounded timeouts, never mutate the parent test
process environment, and demonstrate the absent-watcher test failing on the old
backend. Upgrade the target dependencies, renew Cargo.lock normally, and rerun.

Then save Linux/Windows/macOS product dependency trees beneath `target/sfs-884/`.
Assert that Linux selects KSNI and contains no GTK3, old GLib, AppIndicator or
libxdo packages; verify Windows/macOS select native backends. Remove obsolete CI
and installation packages only after that evidence. Document session bus/host
requirements, GNOME extension and tooltip limitations, and unchanged headless
fallback in English and Ukrainian. Add the 0.2.1 release entry.

Finally compare the old and new package inventories, review changed upstream
license files and archive checksums, intentionally regenerate the baseline, and
run strict notice generation. Keep historical 0.2.0 evidence labeled as such.
Build the product, inspect its Linux dynamic dependencies and run focused checks.

## Concrete Steps and Validation

Run commands from the repository root and retain output under `target/sfs-884/`:

    cargo test --locked -p studiofs --bin studiofs tray::supported::tests:: -- --nocapture
    cargo tree --locked -p studiofs --target x86_64-unknown-linux-gnu -e normal,build,features
    cargo check --locked -p studiofs --all-targets
    cargo clippy --locked -p studiofs --all-targets -- -D warnings
    cargo build --locked -p studiofs
    readelf -d target/debug/studiofs
    python3 docs/legal/generate-third-party-notices.py --self-test
    python3 docs/legal/generate-third-party-notices.py --release --output-dir target/sfs-884/notices
    cargo fmt --all -- --check
    git diff --check

Run the backend tests with `timeout 20s dbus-run-session -- ...` for the
absent-watcher test and `timeout 20s env DBUS_SESSION_BUS_ADDRESS=unix:path=/nonexistent/studiofs-sfs-884 ...`
for missing-bus initialization. Successful tests must have a nonzero count.
Inspect actual dynamic imports for libfuse3 and absence of the removed GTK stack.
No local compile claim substitutes for native Windows/macOS checks or real Linux
KDE/GNOME/mounted smoke tests. Record unavailable prerequisites explicitly.

## Idempotence and Recovery

Commands are repeatable; generated diagnostics stay under ignored `target/`.
No secrets or `.env` are required for focused tests. Retain unrelated work and
never rewrite existing commits. Regenerate inventory only after license review.

## Artifacts and Notes

Dependency trees, backend output, notices and linkage evidence belong in
`target/sfs-884/`. This plan records concise conclusions and outstanding release
gates. Source review uses the pinned upstream release, rather than the moving
development branch.

Available validation has passed: `cargo check --locked -p studiofs --all-targets`,
`cargo clippy --locked -p studiofs --all-targets -- -D warnings`, the focused
tray tests, both direct backend probes, the D-Bus restart harness, inventory and
notice generator self-tests, cargo-deny 0.20.2 license checks/policy probes,
strict release notices, and four release-package tests. The downloaded official
cargo-deny tool and archive checksum were verified and retained under `target/`;
no global install was performed. Native dependency trees are scope evidence only.

## Outcomes & Retrospective

The 0.2.1 migration is implemented and independently reviewed with no remaining
actionable findings. Linux explicitly selects KSNI; Windows/macOS retain native
backends. The complete lockfile and all three product graphs no longer contain
the old GTK3/GLib/AppIndicator chain. User guides, CI prerequisites, release notes,
reviewed inventory and license policy/notices agree with that resolution.

`cargo build --locked -p studiofs` passed, and the fresh executable reports
`studiofs 0.2.1`. Its local x86-64 debug `NEEDED` entries are `libfuse3.so.4`,
`libgcc_s.so.1`, `libm.so.6`, `libc.so.6`, and the ELF loader; none of the removed
tray libraries remain. This host provides libfuse 3.18.2 with SONAME `.4`.
The Ubuntu 24.04 release gate still expects its system `.3`; this local debug
binary is not release-artifact evidence for that builder.

With `DISPLAY`/`WAYLAND_DISPLAY`/`WAYLAND_SOCKET` removed, an invalid session bus
and an isolated `STUDIOFS_CLIENT_ROOT`, fresh-binary `mount sfs-884-uninitialized
<target mountpoint> --foreground` and `--detach` both return the expected missing
format error with exit code 1 within a 15-second bound, without panic. This proves
headless startup/error return only, not successful mounted operation or the GUI
dispatcher mount-thread lifecycle.

All locally available gates passed. Full desktop rendering/tooltip acceptance,
real mounted lifecycle, native Windows/macOS checks and Rust 1.95 execution remain
release handoffs. This host has no graphical session or `/dev/fuse`; no credentials
or `.env` were needed or loaded. Logs and exact commands are retained in
`target/sfs-884/validation.md`. The issue stays In Progress pending those native
acceptance checks; no PR or release publication is implied.
