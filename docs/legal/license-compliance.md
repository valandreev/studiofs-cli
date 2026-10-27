# Dependency license checks and release notices

Run these commands from the repository root with Cargo and Python 3.11+:

```powershell
cargo install cargo-deny --version 0.20.2 --locked
cargo deny --locked check licenses
python docs/legal/check-license-policy.py
python docs/legal/generate-third-party-notices.py --self-test
python docs/legal/generate-third-party-notices.py --output-dir target/license-compliance
```

`deny.toml` permits the SPDX terms reviewed in the
[dependency audit](rust-dependency-audit.md). It checks all platforms and all
features, including build and development dependencies. The two exact WinFsp
versions have documented exceptions for their deprecated `GPL-3.0` identifier;
this means GPLv3 only and does not waive any notice obligation. SFS-884 also
permits `Unlicense` for exactly `ksni 0.3.7` after reviewing its packaged
dedication and disclaimer; see the [KSNI review](sfs-884-ksni-license-review.md).
Unknown or unapproved licenses fail. `AND` and `WITH` requirements retain their meaning.
The probe uses the real policy: MIT must pass, while AGPL-3.0-only and missing
license declarations must fail specifically on licensing. Existing `cargo audit`
security checks remain independent and enabled.

The generator requires external package identities, checksums and original
manifest expressions to match the reviewed
[inventory](rust-dependency-inventory.json). A dependency update therefore needs
a renewed audit, even when its SPDX identifiers are already allowed. Review the
affected terms and component notices, update the audit decisions, then
intentionally regenerate the baseline using its documented commands. Do not
refresh that baseline automatically to make CI pass. Workspace packages are
excluded from this historical baseline comparison because SFS-852 subsequently
set their license to GPL-3.0-only.

The generated directory contains `THIRD-PARTY-NOTICES.txt`,
`rust-dependency-inventory.json`, and the StudioFS `LICENSE`. The inventory covers
every entry of the release lockfile, a conservative superset of a binary's
dependencies. It records authors, original license expressions, source identity,
checksums and reproduced license/notice evidence, including nested native code.
It is not a link map. Text comes from Cargo sources checked against the pinned
crate archive; missing archives and mismatches fail. Output ordering and
line endings are deterministic and exclude timestamps and local source paths.

Native platform acknowledgements and statically linked component information
are included from [platform-distribution.md](platform-distribution.md). Cargo
metadata alone cannot discover the terms of separately installed WinFsp,
libfuse3 or macFUSE, or of the desktop services used by the KSNI tray.
The 0.2.1 Linux tray no longer requires GTK/GLib/libxdo/AppIndicator libraries.
That document remains the packaging contract: no bundled drivers or system shared libraries, and no
MFMount/FSKit in v0.2.0 release binaries. Ring, mimalloc and jemalloc also require
their embedded native notices, independently of wrapper manifest licenses.

## CI and release staging

The dedicated GitHub Actions workflow runs the policy, negative probes,
generator self-check and generation. It runs on relevant pull
requests/main pushes and offers the directory as a downloadable **review**
artifact. A successful review run does not clear a binary for distribution.

Before creating any release archive, run:

```powershell
python docs/legal/generate-third-party-notices.py --release --output-dir target/license-compliance
if ($LASTEXITCODE -ne 0) { throw 'Release notice evidence is incomplete' }
```

Only after this passes, put all three generated files beside `studiofs.exe` or
`studiofs` in **each** Windows/macOS/Linux archive. Keep the inventory so recipients
can identify exact versions and obtain covered source. Corresponding source and
build material must accompany GPL distribution through the release's chosen
compliant source-delivery method; a repository URL alone is not a substitute
for that release decision. Run the native linkage/archive checks documented in
the platform policy before publication. Packaging automation must make these
commands an explicit prerequisite and copy the complete directory. The GitHub
`release: published` check is post-publication verification, not a publication
lock or an attachment to an existing binary archive.

SFS-854 adds the separate [Release builds workflow](../../.github/workflows/release.yml)
and [archive helper](../../tools/release/package.py). That workflow runs the strict
command above before starting five native product builds, includes the complete
notice directory in every archive, checks native linkage, and attaches successful
tag builds to a draft Release. The older published-event check remains a
post-publication verification. See [release instructions](../releases.md) for
the target matrix and the explicit final publication step.

The generated notices also include the curated
[source-material notices](source-material-notices.txt) for the MIT documentation
and plan templates identified by the source provenance audit. These supplements
do not waive any unresolved registry-package evidence.

The SFS-849 audit recorded **65 third-party packages without named primary
notice evidence**; after the SFS-882 renewal, strict generation reports zero
unresolved entries. The generator also searches `LICENSES/` directories; consult
its inventory for the current gap count. Crates that ship no license file are
covered by verbatim upstream texts in `supplemental-licenses/` (see its README). Review generation records these gaps;
`--release` returns failure while any remain.
Resolve the exact upstream notice/copyright evidence and review any required
supplements before distribution. Do not substitute a generic SPDX text for
missing author notices. Finding a primary file is an automated minimum: it does
not prove every per-file, generated-data, or embedded-component obligation is
complete. The source provenance and actual native artifact reviews remain part
of the final release gate.
