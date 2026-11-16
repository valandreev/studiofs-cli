# StudioFS release downloads and builds

Published releases are
[v0.2.2](https://github.com/valandreev/studiofs-cli/releases/tag/v0.2.2)
(19 assets, published 2026-11-12),
[v0.2.1](https://github.com/valandreev/studiofs-cli/releases/tag/v0.2.1)
(19 assets, published 2026-11-08), and
[v0.2.0](https://github.com/valandreev/studiofs-cli/releases/tag/v0.2.0).
The current development workspace version is `0.2.3`; the platform instructions
below describe the published v0.2.2 packages.

StudioFS 0.2.2 targets these five native platforms:

| v0.2.2 download suffix | Suffix from v0.2.3 | Rust target triple | System | GitHub builder |
| --- | --- | --- | --- | --- |
| `x86_64-pc-windows-msvc.zip` | `x86_64-pc-windows.zip` | `x86_64-pc-windows-msvc` | Windows x86-64 | `windows-2025` |
| `aarch64-pc-windows-msvc.zip` | `aarch64-pc-windows.zip` | `aarch64-pc-windows-msvc` | Windows ARM64 | `windows-11-arm` |
| `x86_64-unknown-linux-gnu.tar.gz` | `x86_64-linux-gnu.tar.gz` | `x86_64-unknown-linux-gnu` | Linux x86-64 | `ubuntu-24.04` |
| `aarch64-unknown-linux-gnu.tar.gz` | `aarch64-linux-gnu.tar.gz` | `aarch64-unknown-linux-gnu` | Linux ARM64 | `ubuntu-24.04-arm` |
| `aarch64-apple-darwin.tar.gz` | `aarch64-apple-darwin.tar.gz` | `aarch64-apple-darwin` | macOS Apple Silicon | `macos-15` |

Releases up to v0.2.2 name archives `studiofs-<version>-<full target triple>`.
Starting with the next release (workspace `0.2.3`), `tools/release/package.py`
drops the redundant `-msvc` suffix and `-unknown-` vendor part from archive and
folder names; `BUILD-INFO.json` still records the full target triple.

Download the archive for your machine from
[GitHub Releases](https://github.com/valandreev/studiofs-cli/releases), verify
its `.sha256` file, and extract it. Archives include the executable, GPLv3
`LICENSE`, `THIRD-PARTY-NOTICES.txt`, dependency inventory, this installation
guide, `PLATFORM-DISTRIBUTION.md`, and `BUILD-INFO.json`.

`BUILD-INFO.json` records the source commit, version, target, Rust compiler,
build command, executable hash and lockfile hash. These builds use default
features and release optimization. They are unsigned; macOS notarization and
Windows code signing are not provided by this workflow. Linux builds use
Ubuntu 24.04's system libraries and are not portable to every older distribution
or to musl/Alpine Linux. No 32-bit x86 or Intel macOS package is produced.

## Install the platform requirements

Windows users install [WinFsp](https://github.com/winfsp/winfsp/releases)
separately, matching their machine architecture. The MSVC runtime must also be
available; use Microsoft's supported Visual C++ Redistributable if needed.

On Ubuntu 24.04, install the runtime packages from your distribution:

```bash
sudo apt-get update
sudo apt-get install fuse3 libfuse3-3
```

Other distributions use their corresponding packages. StudioFS dynamically
links libfuse3; it does not ship system shared libraries. Since 0.2.1 the Linux
tray uses KSNI over session D-Bus, without GTK3/GLib/libxdo/AppIndicator libraries.
A StatusNotifierItem host such as KDE Plasma is required for an icon. GNOME needs
a compatible AppIndicator/KStatusNotifierItem extension and may not display
tooltips. Without a compatible host/session bus, mounts continue without the
tray. Linux isolates the tray in an owned helper; an unresponsive D-Bus cannot
hold mount completion beyond the helper's two-second shutdown grace and
500 ms post-kill reap window. A watcher appearing or restarting on an existing bus is handled by the
upstream backend. See [tray details](tray.md). Older 0.2.0 binaries still need
the GTK/AppIndicator packages listed in that release's installation instructions.

macOS users install [macFUSE](https://macfuse.github.io/) themselves and follow
its platform setup requirements. The v0.2.0 release uses its legacy libfuse
backend. Experimental FSKit/MFMount is excluded. The archive does not install
or download macFUSE, WinFsp, or Linux system libraries. The complete native
distribution contract is in
[platform-distribution.md](legal/platform-distribution.md).

Run `studiofs --version` (or `studiofs.exe --version`) after extraction. See
the [user guide](user-guide/en/README.md) for configuration and mounting.

## Reproduce a binary

Use the exact tag or `source_commit` from `BUILD-INFO.json`. Install Rust
1.95.0 and the native build tools: Visual Studio C++ tools on Windows, Xcode
command-line tools on macOS, or a C compiler, make and pkg-config on Linux.
Ubuntu builders also need:

```bash
sudo apt-get install pkg-config libfuse3-dev
```

macOS builders need the headers from the separately installed macFUSE package.
Disable MFMount discovery before building, even when the framework is installed:

```bash
mkdir -p target/no-mfmount
export STUDIOFS_MFMOUNT_FRAMEWORK_DIR="$PWD/target/no-mfmount"
```

On a native machine matching the chosen download, replace `<target>` with the
table's Rust target triple (not the download suffix):

```bash
rustup toolchain install 1.95.0 --profile minimal
rustup target add --toolchain 1.95.0 <target>
cargo +1.95.0 build -p studiofs --release --locked --target <target>
```

The output is `target/<target>/release/studiofs` (`studiofs.exe` on Windows).
This is the product equivalent of `cargo build --release`; selecting `-p studiofs`
avoids building unrelated workspace test/scenario tools. Exact byte reproduction
also depends on the native toolchain and library versions; this workflow records
identity and linkage evidence, not a promise of bit-for-bit reproducible builds.

The release's `studiofs-v0.2.2-source.tar.gz` contains the exact tracked project
source and build scripts. GitHub also supplies source archives for the tag.
`studiofs-v0.2.2-dependency-source.tar.gz` contains `cargo vendor --locked`
output for every locked Rust dependency, plus `Cargo.lock` and the Cargo source
replacement configuration needed to build offline from it. Separately installed
platform components (WinFsp, macFUSE, libfuse3 and desktop libraries) are not
redistributed. The [publication gate](legal/v0.2.0-publication-gate.md) records
the approval of this delivery method.

## Prepare the next release on GitHub

The current development workspace version is 0.2.3. Tag the exact reviewed commit with
the matching version after the release workflow has reached the default branch:

The [Release builds workflow](../.github/workflows/release.yml) runs on a pushed
version tag or manually from Actions. The tag must match the workspace version;
for the reviewed 0.2.3 workspace, use `v0.2.3`. Push the reviewed source branch first. After
it is merged, the maintainer can tag that exact approved commit:

```bash
git tag -a v0.2.3 <approved-commit> -m "StudioFS v0.2.3"
git push origin v0.2.3
```

Do not retag a published version. Manual runs on a branch create workflow
artifacts only; manual runs selecting an existing version tag can retry its
draft. A new workflow must reach the default branch before the Actions UI
offers its manual trigger.

The workflow first checks the license policy and generates release notices.
Missing evidence fails before the five paid/native build jobs start. When all
five builds, linkage checks and packages pass, the tag run creates a **draft**
GitHub Release and uploads all archives, checksum files, linkage logs and the
project source archive. A retry may update that draft; it refuses to overwrite
an already published release. No personal access token is required.

After the new release's checklist records approval for those exact artifacts,
publish the draft using GitHub's **Publish release** button. Then readers can
download from Releases when the repository is public. Standard hosted ARM
runners are available for private repositories too; private builds consume the
account's Actions minutes and can incur runner charges. This workflow does not
change repository visibility.

GitHub references: [hosted runner labels](https://docs.github.com/en/actions/reference/runners/github-hosted-runners),
[release creation](https://cli.github.com/manual/gh_release_create),
[managing releases](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository).
