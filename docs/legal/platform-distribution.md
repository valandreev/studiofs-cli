# Platform Native Distribution Terms

Status: engineering license review for SFS-850 (Release 0.2.0), checked
2026-10-22. This is not legal advice. It records the native components each
StudioFS build loads, the upstream terms that apply to them, and the packaging
decisions that keep StudioFS distributable as GPL-3.0-only. Items marked
**Escalation** need a legal or upstream answer before the related decision may
change.

## Summary

| Platform | Native component | How StudioFS uses it | Upstream terms | Packaging decision |
| --- | --- | --- | --- | --- |
| Windows | WinFsp DLL `winfsp-<arch>.dll` + driver | Delay-loaded at runtime from the installed WinFsp | GPLv3 with a FLOSS exception | **Separate install.** Release archives contain no WinFsp DLL or installer. |
| Windows | `winfsp` / `winfsp-sys` crates 0.12 (`+winfsp-2.1`) | Rust bindings; import library linked into `studiofs.exe` | GPL-3.0 | Ships inside `studiofs.exe` under GPLv3; StudioFS source is published. |
| Linux | libfuse3 `libfuse.so.3` (≥ 3.2) | Dynamically linked via the C shim in `studiofs-linux-fuse` | LGPL-2.1 (`lib/`, `include/`); GPL-2.0 utilities | **System package.** Never vendored or statically linked (ADR 0008). |
| Linux | GTK 3, GLib, libxdo, Ayatana AppIndicator | Tray UI; dynamically linked or `dlopen`ed | LGPL-2.1+ / LGPL-2.1-or-3 / BSD-3-Clause | **System packages.** Never bundled. |
| macOS | macFUSE `libfuse.2.dylib` + kernel extension | Dynamically linked; kernel extension reached through `mount_macfuse` | macFUSE license (BSD-3-Clause + no commercial bundling); libfuse LGPL | **Separate install by the user.** Never bundled, downloaded, or installed automatically. |
| macOS | macFUSE `MFMount.framework` (FSKit backend) | Linked only when present at build time; experimental | macFUSE package license; component terms not separately published | **Excluded from 0.2.0 release binaries.** Opt-in developer builds only. |

StudioFS release artifacts for 0.2.0 are standalone `studiofs` executables
(plus StudioFS license and notice files). They contain no FUSE, macFUSE, or
WinFsp binaries; each user installs the platform driver from its upstream.

## StudioFS license position

- StudioFS declares `GPL-3.0-only` in the workspace manifest; internal crates
  inherit it. SFS-852 added the complete GPLv3 text in [LICENSE](../../LICENSE)
  on 2026-10-23. Every release archive must contain that text.
- Git history records project-author aliases and separate branding uploads;
  it does not prove sole copyright ownership or authority to grant an
  additional GPLv3 §7 permission (see the macOS escalation). Confirm the
  affected rightsholders before granting any exception. The
  [source provenance audit](source-provenance.md) records unresolved source,
  contribution and asset evidence; no such exception exists today.
- GPLv3 §1 "System Libraries" covers OS components and the compiler runtime
  (Windows system DLLs, the MSVC runtime, glibc, Apple system frameworks). It
  does **not** cover third-party drivers such as WinFsp, libfuse3 shipped by a
  distribution, or macFUSE, so each is assessed below.

## Windows: WinFsp

### What is linked

- `crates/studiofs-winfsp` depends on `winfsp` 0.12.4 (`+winfsp-2.1`) and
  `winfsp-sys` 0.12.1, both `GPL-3.0` from
  <https://github.com/SnowflakePowered/winfsp-rs>.
- `winfsp-sys` vendors WinFsp headers, import libraries
  (`winfsp/lib/winfsp-*.lib`), and the WinFsp DLLs
  (`winfsp/bin/winfsp-*.dll`) inside the crate. The import library is linked
  into `studiofs.exe`. The DLLs are copied out only when the
  `WINFSP_DLL_OUTPUT_PATH` build variable is set; StudioFS does not set it.
- `crates/studiofs/build.rs` adds `/DELAYLOAD:winfsp-<arch>.dll`, and
  `studiofs-winfsp` preloads the DLL at runtime from, in order: the
  executable's directory, `%ProgramFiles(x86)%\WinFsp\bin`, and
  `%ProgramFiles%\WinFsp\bin`.

### Upstream terms

WinFsp (<https://github.com/winfsp/winfsp/blob/master/License.txt>) is GPLv3
with a special exception from Bill Zissimopoulos for FLOSS software. The
exception permits linking with a platform DLL (`winfsp-a64.dll`,
`winfsp-x64.dll`, `winfsp-x86.dll`, `winfsp-msil.dll`) and distributing
**unmodified** WinFsp installers without the linking software being GPLv3. It
applies only when the software (1) is under an FSD- or OSD-compliant license,
(2) shows the WinFsp copyright notice and repository link in its UI and
user-facing documentation, and (3) is not linked or distributed with
proprietary software. Commercial licenses are available from the author.

### Decision

- StudioFS is GPL-3.0-only, so it uses WinFsp and `winfsp-rs` under plain
  GPLv3; the combined `studiofs.exe` is conveyed under GPLv3 with the StudioFS
  source. The FLOSS exception is not required.
- StudioFS still meets the exception's attribution condition (notice below),
  so the exception remains available if a future package ever bundles the
  unmodified WinFsp installer.
- **Separate install.** Users install WinFsp from
  <https://github.com/winfsp/winfsp/releases>. Release archives must not
  include any `winfsp-*.dll`: a copy beside `studiofs.exe` would be loaded
  first, would create a GPLv3 corresponding-source obligation for that DLL,
  and is useless without the WinFsp driver anyway.
- Bundling the WinFsp installer in a future StudioFS installer is allowed only
  unmodified, as released by WinFsp, and only if nothing proprietary is
  distributed with it. That change requires updating this document first.
- Rust's default MSVC target uses the dynamic MSVC runtime; do not
  redistribute the Visual C++ Redistributable inside StudioFS archives.

## Linux: libfuse3 and desktop libraries

### What is linked

- `crates/studiofs-linux-fuse/build.rs` probes `fuse3` with `pkg-config`,
  requires libfuse3 ≥ 3.2, and compiles `csrc/low_level_ops.c` against the
  system headers; `src/low_level.rs` declares `#[link(name = "fuse3")]`
  (dynamic). ADR 0008 forbids vendoring, bundling, or building libfuse3.
- The tray UI (`tray-icon` → `muda`, `gtk`) dynamically links GTK 3, GLib,
  GDK, Pango, Cairo, ATK, and libxdo; `libappindicator-sys` `dlopen`s
  `libayatana-appindicator3.so.1` or `libappindicator3.so.1` at runtime.
- `fusermount3` (GPL-2.0, part of the `fuse3` package) is executed by libfuse
  as a separate program; StudioFS does not distribute it.

### Upstream terms

libfuse (<https://github.com/libfuse/libfuse/blob/master/LICENSE>) licenses
`lib/`, `include/`, and `meson.build` under LGPL-2.1; everything else
(including `fusermount3` and examples) is GPL-2.0. LGPL-2.1 is compatible with
a GPL-3.0-only program. GTK 3 and GLib are LGPL-2.1-or-later (with
permissive parts), the Ayatana AppIndicator library is LGPL-2.1 or LGPL-3,
and libxdo is BSD-3-Clause.

### Decision

- **System packages only.** Release tarballs ship the `studiofs` binary and
  StudioFS license/notice files; they must not contain `libfuse3.so*`, GTK, or
  other shared libraries. Users install `fuse3`/`libfuse3-3` (Ubuntu 24.04) or
  `fuse3`/`fuse3-libs` (Rocky Linux 9.4) from their distribution.
- Dynamic linking against the copy already present on the user's system
  satisfies LGPL-2.1 §6(b). StudioFS's own GPLv3 terms already permit
  modification and reverse engineering. The notice file still names libfuse3
  and its LGPL-2.1 terms (LGPL-2.1 §6 notice).
- **Static linking of libfuse3 or any LGPL library is not allowed** without
  first adding object-file relinking material and revising this document.
- A future `.deb`/`.rpm` must declare runtime dependencies (`fuse3`,
  `libfuse3-3` / `fuse3-libs`, GTK 3, libxdo, Ayatana AppIndicator) instead of
  bundling them, and install the license and notice files under
  `/usr/share/doc/studiofs/` (Debian `copyright` format for `.deb`).

### Linkage evidence

Observed on 2026-10-22 with `cargo build --locked -p studiofs --release` in an
`ubuntu:24.04` container (Ubuntu 24.04.5 LTS, aarch64, Rust 1.95.0,
`pkg-config --modversion fuse3` = 3.14.0, packages `libfuse3-dev`,
`libgtk-3-dev`, `libxdo-dev`, `libayatana-appindicator3-dev`):

- `readelf -d` `NEEDED`: `libfuse3.so.3`, `libgtk-3.so.0`, `libgdk-3.so.0`,
  `libpango-1.0.so.0`, `libcairo-gobject.so.2`, `libcairo.so.2`,
  `libgdk_pixbuf-2.0.so.0`, `libgio-2.0.so.0`, `libgobject-2.0.so.0`,
  `libglib-2.0.so.0`, `libgcc_s.so.1`, `libm.so.6`, `libc.so.6`,
  `ld-linux-aarch64.so.1`. libxdo is not a `NEEDED` entry in this build;
  Ayatana AppIndicator is loaded only through `dlopen`.
- libfuse is linked dynamically: `nm` finds no defined `fuse_session_new`,
  and 26 `fuse_*` symbols are undefined dynamic imports.
- Debian `copyright` files on that image: libfuse3 `lib/*` is `LGPL-2.1`
  (utilities `GPL-2`/`GPL-2+`); GTK 3 `Files: *` is
  `LGPL-2+ and LGPL-2.1+ and Expat`; libayatana-appindicator's library
  sources are `LGPL-2.1 or LGPL-3` (some build and test files are `GPL-3`);
  libxdo is `BSD-3-clause`. All are compatible with GPL-3.0-only when
  dynamically linked from the system.
- Not checked: an x86_64 build and Rocky Linux 9.4 packaging. The release
  gate below repeats the `NEEDED` check on the actual artifact.

## macOS: macFUSE and MFMount.framework

### What is linked

- Legacy (default) backend: `crates/studiofs-macfuse/build.rs` compiles
  `csrc/low_level_ops.c` against macFUSE's `fuse` headers (FUSE API 26) and
  `src/low_level.rs` links `#[link(name = "fuse")]`, which resolves to
  macFUSE's `/usr/local/lib/libfuse.2.dylib`. Mounting goes through macFUSE's
  closed-source kernel extension and `mount_macfuse` helper, which StudioFS
  does not link or distribute.
- Experimental FSKit backend (`STUDIOFS_MOUNT_BACKEND=fskit`): when
  `MFMount.framework` exists under `STUDIOFS_MFMOUNT_FRAMEWORK_DIR` (default
  `/Library/Filesystems/macfuse.fs/Contents/Frameworks`) at build time, the
  build script sets `cfg(studiofs_fskit)` and `src/fskit.rs` links
  `#[link(name = "MFMount", kind = "framework")]` as a load-time dependency.
  Without it, the backend compiles to an `Unsupported` error.
- Observed on macOS with macFUSE 5.5.0 installed: a default release build
  (`target/release/studiofs`) links both
  `/usr/local/lib/libfuse.2.dylib` and
  `/Library/Filesystems/macfuse.fs/Contents/Frameworks/MFMount.framework/Versions/A/MFMount`.
  A build with `STUDIOFS_MFMOUNT_FRAMEWORK_DIR` pointed at an empty directory
  links only `/usr/local/lib/libfuse.2.dylib` (`otool -L`).

### Upstream terms

- The license shipped in macFUSE 5.5.0
  (`/Library/Filesystems/macfuse.fs/Contents/Resources/License.rtf`, also
  <https://github.com/macfuse/macfuse/blob/master/LICENSE.txt>) is a
  BSD-3-Clause-style license, copyright Benjamin Fleischer, with a fourth
  clause: binary redistribution bundled with commercial software is not
  allowed without prior written permission, and this includes automated
  download or installation of macFUSE in the context of commercial software.
  It also lists BSD, APSL (`mount_macfuse`), Apple sample-code, and LGPL
  (libfuse user-space library) components.
- macFUSE is not fully open source: since 3.9 some components, including the
  kernel extension, are closed source
  (<https://github.com/macfuse/macfuse/wiki/Open-Source-Status>). macFUSE
  5.2.0 release notes describe the MFMount mount API as open source, but no
  separate license for `MFMount.framework` or the FSKit extension is
  published.
- "Free software" status does not waive clause 4: whether a given StudioFS
  distribution counts as "commercial software" depends on the distributor,
  not on StudioFS's license.

### Decision

- **Separate install by the user.** Users install macFUSE themselves from
  <https://macfuse.github.io/> or `brew install --cask macfuse`. StudioFS
  release archives, installers, and setup scripts must not contain, download,
  or install macFUSE. CI workflows that run `brew install --cask macfuse` are
  test infrastructure, not distribution.
- **Legacy backend ships.** The released binary links only macFUSE's
  `libfuse.2.dylib`, which macFUSE distributes under the LGPL; LGPL-2.1
  dynamic linking is compatible with GPL-3.0-only. The notice file names
  macFUSE and libfuse.
- **FSKit/MFMount is excluded from 0.2.0 release binaries** (confirmed by
  the maintainer on 2026-10-22). Release builds
  must set `STUDIOFS_MFMOUNT_FRAMEWORK_DIR` to a directory without
  `MFMount.framework`, and the release gate below must show no `MFMount`
  load command. Developers may still build FSKit locally.
- **Escalation (blocks shipping FSKit, not 0.2.0):** linking a GPL-3.0-only
  binary to `MFMount.framework` needs either (a) upstream confirmation that
  `MFMount.framework` is under a GPL-compatible license without clause 4, or
  (b) a GPLv3 §7 additional permission from the StudioFS copyright holder to
  link with macFUSE components, which changes the SPDX expression to
  `GPL-3.0-only WITH <exception>` and needs a new tracked issue to update the
  workspace `license` field, LICENSE/notice files and README. Ask
  the macFUSE maintainer, as the Open Source Status page directs for
  licensing questions, before choosing.

## Statically linked third-party native code

These are compiled into `studiofs` on the listed targets and need notices,
but no packaging change. SFS-853 owns generating the full notice file.

| Crate | Native code | Targets | License |
| --- | --- | --- | --- |
| `libmimalloc-sys` 0.1.46 | mimalloc (C) | all | MIT |
| `ring` 0.17.14 | BoringSSL-derived C/assembly | all | Apache-2.0 AND ISC |
| `tikv-jemalloc-sys` 0.6.1 | jemalloc 5.3 (C), via `pyroscope` | Linux, macOS | crate MIT/Apache-2.0; jemalloc BSD-2-Clause |
| `winfsp-sys` 0.12.1 | WinFsp import library | Windows | GPLv3 (WinFsp) |

## Required notices

Include this text in the release notice file, in user-facing installation
documentation, and in `studiofs --version` or an equivalent About surface
(WinFsp requires the notice and link in the user interface as well as the
documentation):

```text
StudioFS is free software distributed under the GNU General Public License,
version 3 only (GPL-3.0-only). Source: https://github.com/valandreev/studiofs-cli

Windows: StudioFS uses WinFsp - Windows File System Proxy,
Copyright (C) Bill Zissimopoulos. https://github.com/winfsp/winfsp
WinFsp is licensed under GPLv3 with a FLOSS exception and is installed
separately; StudioFS does not redistribute it.

Linux: StudioFS dynamically links libfuse3 (https://github.com/libfuse/libfuse),
licensed under the GNU Lesser General Public License version 2.1, and uses
the system GTK 3, GLib, libxdo, and AppIndicator libraries. These are
installed from your distribution and are not redistributed by StudioFS.

macOS: StudioFS requires macFUSE (https://macfuse.github.io/),
Copyright (c) 2011-2026 Benjamin Fleischer, which is installed separately
and is not redistributed by StudioFS. StudioFS dynamically links the macFUSE
FUSE user-space library, licensed under the GNU Lesser General Public License.
```

## Release gate checks

Run these on the release artifacts before publishing (SFS-854):

```bash
# Windows (Developer PowerShell): no WinFsp payload in the archive, delay-load only
Get-ChildItem -Recurse <release-dir> -Filter 'winfsp-*.dll'   # must be empty
dumpbin /dependents studiofs.exe                               # winfsp-<arch>.dll under delay load

# Linux: libfuse3 dynamic, no bundled shared libraries
readelf -d studiofs | grep NEEDED                              # contains libfuse3.so.3
tar -tf studiofs-*.tar.* | grep -E '\.so(\.|$)'                # must be empty

# macOS: libfuse only, no MFMount, no macFUSE payload
otool -L studiofs | grep -E 'fuse|MFMount'                     # only libfuse.2.dylib
```

## Open items

- Generate the third-party notice bundle, including the native table above,
  using the [license compliance commands](license-compliance.md). Resolve its
  reported evidence gaps before release packaging.
- SFS-854: run the release gate checks above on the actual artifacts; the
  Windows `dumpbin` check is not yet run on a release build.
- FSKit escalation above, before `STUDIOFS_MOUNT_BACKEND=fskit` ships.

## Sources (retrieved 2026-10-22)

- WinFsp license and FLOSS exception:
  <https://github.com/winfsp/winfsp/blob/master/License.txt>
- winfsp-rs: <https://github.com/SnowflakePowered/winfsp-rs>
- libfuse license: <https://github.com/libfuse/libfuse/blob/master/LICENSE>
- macFUSE license: <https://github.com/macfuse/macfuse/blob/master/LICENSE.txt>
  and the copy installed with macFUSE 5.5.0
- macFUSE open-source status:
  <https://github.com/macfuse/macfuse/wiki/Open-Source-Status>
- macFUSE 5.2.0 release notes (MFMount API), as mirrored at
  <https://newreleases.io/project/github/macfuse/macfuse/release/macfuse-5.2.0>
- GPLv3 System Libraries definition: <https://www.gnu.org/licenses/gpl-3.0.html>
