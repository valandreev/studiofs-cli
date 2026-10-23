# Source, documentation and asset provenance

SFS-851 audit, 2026-10-23. **Publication decision: blocked pending the
evidence below.** This is an engineering provenance inventory, not a grant of
rights or legal clearance. It records what the repository demonstrates and
what a release owner must still establish.

## Audited snapshot and method

The source snapshot is `c7be848af03c6bebaa5e890406e3d0ec8c0d729b`, with 718
tracked files and a clean worktree on
`feature/sfs-851-audit-source-documentation-and-asset-provenance-before`.
[The inventory](source-provenance-inventory.tsv) lists every path in that
snapshot, its Git blob ID and its review category. Blob IDs identify the
audited bytes; a category is not a per-file permission certificate. This report,
the inventory and subsequent edits are additions to that baseline, not entries
in its 718-file count.

The audit enumerated the complete tracked tree, scanned attribution/source
indicators, inspected the native boundaries, asset set, fixture documentation
and relevant introduction commits, and checked contributor history. The checkout
is not shallow; the baseline (`c7be848a`) has 5,156 reachable commits, beginning at `da2d97c2`
(2026-02-15). Other local refs contain checkpoints and unrelated branch work;
they are not the source release set. History inspection was targeted, not a
review of every historical blob. Making an existing Git repository public also
exposes reachable historical content, so removing a file from HEAD alone does
not resolve that publication question.

Codebase Memory identified native/header and documentation paths. Exact-path
coverage checks were supplemented by direct reads for excluded assets and the
macFUSE shim's parse gap at line 158. Git and direct text reads supplied history,
configuration and notice evidence. A follow-up review pass read all 209
documentation files in full, checked their external links, compared the macFUSE
fallback with upstream headers and ran a tree-wide JuiceFS similarity scan; its
results are folded into the sections and register below. No existing notices were removed. Dependency
licenses and asset permissions are not changed by this audit. A follow-up
remediation on the same branch removed the dead macFUSE fallback (P2), added MIT
notices for copied templates (P6) and replaced developer-local paths (P5); the
inventory still describes the unmodified baseline. The `.antigravitycli` symlink was
also untracked.

## Complete source-tree inventory

Categories are disjoint; the TSV supplies the exact paths rather than relying
on the examples in this table. Project-authored below means repository-local
development evidence, **not** independently verified ownership or a clean-room
claim.

| Category | Files | Evidence and disposition |
| --- | ---: | --- |
| Rust source | 345 | Project implementation and tests under `crates/`, excluding build scripts. Targeted source/history review found architectural references, not an identified copied implementation body. Rights confirmation remains P1; native API boundaries are discussed below. |
| Build scripts | 4 | `crates/{studiofs,studiofs-linux-fuse,studiofs-macfuse,studiofs-winfsp}/build.rs`: local glue, installed-library discovery, Git metadata and delay loading. No vendored implementation is introduced by these scripts. |
| Native source | 3 | Two `csrc/low_level_ops.c` shims and macFUSE `fuse_lowlevel_compat.h`. Header provenance is unresolved (P2); the header was later removed from the tree. |
| Branding | 22 | Six SVGs, fourteen PNGs and two HTML boards, all below `docs/branding/studiofs-logo-final/`. Source/rights evidence is missing (P3). |
| Database fixtures | 4 | Two metadata and two cache-index `.redb` files. Generation is partly documented; baseline origin remains unresolved (P4). |
| Documentation | 209 | README/CONTEXT and `docs/` excluding branding/legal. Includes guides, ADRs, plans, examples and benchmark reports. All 209 files were read in full; prose is project-authored apart from the MIT agent-workflow templates in P6, short attributed quotations and functional configuration snippets. Exact-phrase web checks covered suspicious passages only, not every sentence (P1). |
| Agent plans | 68 | `.agent/PLANS.md` and tracked execution plans. `.agent/PLANS.md` reproduces the OpenAI Cookbook ExecPlan template (P6). Internal development material with source snippets and issue context; covered by project rights confirmation and final publication hygiene (P1/P5). |
| Tools and automation | 30 | `tools/`, `.github/`, `.rwx/`, `docker/`, and crate scripts. Project benchmark/test/CI glue; some run or refer to external programs. Those programs are not copied into this source tree. |
| Cargo metadata | 14 | Workspace plus twelve crate manifests and `Cargo.lock`. Declared GPL intent; fetched package rights are covered separately by SFS-849. |
| Existing legal audits | 4 | Dependency audit, generator, JSON ledger and platform review. Their dependency/native conclusions retain their own stated evidence limits. |
| Repository controls | 15 | AGENTS files, formatting/editor/configuration files, the metadata fixture README and hidden metadata. The machine-local symlink was later untracked (P5). |
| **Total** | **718** | All baseline paths accounted for; no submodule entries in the baseline tree. |

There is no tracked vendored dependency-source tree in this snapshot. Cargo's
downloaded registry code, installed native SDKs, local benchmark reference trees,
`target/`, ignored files and externally supplied workloads are not part of this
718-file inventory. Binary distributions can include third-party compiled code
even when its source is not tracked here: use the
[Rust dependency audit](rust-dependency-audit.md) and
[platform distribution review](platform-distribution.md), then inspect the
actual archives under SFS-854.

## Contributor and rightsholder evidence

The baseline's Git author names are Valentine Andreev (4,842 commits), Valentine (308),
valandreev (4), and DDK1805 (2). The first three are apparent account/name aliases;
there is no `.mailmap` that formally resolves them. Other local refs also contain
T3 Code author metadata; those refs do not add an independent rights grant to
HEAD. Author names, commit counts and AI/tool attribution are development
evidence, not proof of an individual's authority to license employer, client or
third-party material.

DDK1805's two commits add `cat_logo.svg` (`02f902cb`) and `png/cat_logo.png`
(`e44a94c4`), both titled “Add files via upload”. The history does not record
the illustration's creator, original source or permission terms. Do not
attribute all code or assets to a sole rightsholder based on this history.

No tracked LICENSE/COPYING/NOTICE file was found. `Cargo.toml` declares
`GPL-3.0` and README repeats it; these establish licensing intent, not missing
third-party grants. SFS-852 owns adding the GPLv3 text and the intended
`GPL-3.0-only` declaration. It must not silently relicense upstream material.

## JuiceFS: reference material versus implementation

README's Credits identify JuiceFS Community Edition as an architectural
inspiration. The [mounted-IO contract](../juicefs-local-first-rust-contract.md)
pins the reference to `668828b61b216ccd289a1e0e10515a41c5f4f4ee`.
Its StudioFS introduction (`504278ed`, SFS-243) describes a JuiceFS-inspired
rewrite. The SFS-595 design/plan explicitly names `pkg/vfs/vfs.go`,
`pkg/chunk/cached_store.go` and `pkg/chunk/disk_cache.go` as references.

StudioFS's Rust workspace, local redb authority, PostgreSQL replication and
platform callback glue are maintained as StudioFS implementations. Shared
writeback, chunk/slice and cache concepts do not by themselves demonstrate
copied source. Conversely, a language rewrite or missing attribution marker
does not prove independent authorship. Targeted inspection did not identify a
specific copied JuiceFS implementation body; this is not an exhaustive
similarity analysis or a clean-room certificate. Project confirmation must
cover translated implementation and documentation as well as verbatim copying.

The pinned JuiceFS Git objects were available locally and read directly, avoiding
dependence on that reference checkout's working-tree contents. The bounded
comparison covered its `pkg/vfs/vfs.go`, `pkg/chunk/cached_store.go` and
`pkg/chunk/disk_cache.go` against StudioFS `studiofs-cache/src/{cached_store,
payload_store}.rs` and `studiofs-vfs/src/vfs/{io_ops,read_path}.rs` under
`crates/`. Their buffering/staging/cache bodies differ materially; searches for
distinctive upstream comments also found no matching text in `crates/` or
`docs/`. Conventional block loops and sparse zero-fill were not classified as
copied expression.

A wider mechanical scan compared the comments and prose of all tracked `.rs`,
Markdown, script and configuration files under `crates/`, `docs/`, `tools/` and
README (600 files) with upstream `pkg/`, `cmd/` and `docs/{en,zh_cn}` at the pinned
commit (448 files). Shared normalized word 8-grams were limited to S3 key
placeholders and AWS IAM policy schema. Paragraph-level and sentence-level
comparisons of topically paired documents (architecture, cache, internals,
benchmarks, Windows) found no paraphrase or translation match. Shared literals
are platform/protocol names. `devbench` deliberately mirrors the `juicefs bench`
flag names and defaults (1 GB big file, 128 KB small files, count 100), and the
300 MiB buffer default is JuiceFS-aligned; these are compatibility parameters,
with differing help text, validation and reporting. Chunk 64 MiB and block 4 MiB
are shared conventional values; object/metadata key layouts, slice overlap
resolution, read-ahead, eviction and writeback decomposition differ. A
Chinese-only passage without an English counterpart, upstream `sdk/`,
`integration/`, `hack/`, `rfcs/` and historical upstream versions remain outside
the comparison. The upstream commit has no NOTICE file.

A local, gitignored `docs/done/juicefs-winfsp.md` holds a verbatim copy of a
Juicedata blog post. It has never been committed and is not part of this
inventory, but it must stay out of any commit and release archive.

JuiceFS's [upstream LICENSE](https://github.com/juicedata/juicefs/blob/668828b61b216ccd289a1e0e10515a41c5f4f4ee/LICENSE)
is Apache-2.0. If a file is confirmed derived, record its exact upstream path
and revision, preserve applicable upstream notices, describe modifications and
include the required license/NOTICE material. README's thank-you is not a
substitute for that file-specific evidence. No Apache notice is fabricated for
code whose derivation has not been established.

## Native source and upstream declarations

The Linux C shim was introduced by `339a4f1e`; the macOS low-level boundary
began at `44a03e1e`. The inspected shim bodies assemble StudioFS callback tables
and small accessors. Linux includes installed libfuse3 headers; macOS uses
installed macFUSE headers through the compatibility include. WinFSP integration
uses the `winfsp` and `winfsp-sys` crates, not a tracked copy of the SDK.
Git history and those shapes support local glue development; no copied native
implementation body was identified in this bounded inspection.

`crates/studiofs-macfuse/csrc/fuse_lowlevel_compat.h`, introduced in
`70bd8984`, is different: its fallback repeats FUSE structs, callback signatures,
Apple xattr position arguments and FUSE_SET_ATTR values, without an upstream
source/version or license notice. The
[macFUSE low-level header](https://github.com/macfuse/library/blob/master/include/fuse_lowlevel.h)
is a strong source candidate and identifies Miklos Szeredi, Amit Singh/Google
and Benjamin Fleischer with an LGPLv2 notice. The
[libfuse license map](https://github.com/libfuse/libfuse/blob/master/LICENSE)
also distinguishes library/include terms from utility/example terms. Those
upstream pages are comparison leads, not proof of this file's exact origin.

A direct comparison with macFUSE `include/fuse_lowlevel.h` and `fuse_common.h`
(`master`, fetched 2026-10-23) shows that `setattr_x` repeats macFUSE's
Apple-only struct field for field, `fuse_conn_info` repeats the first five
libfuse 2.x fields, and `fuse_file_info` follows the non-Apple libfuse 2.9
layout (`padding : 27`, without macFUSE's `purge_attr`/`purge_ubc`). The
`fuse_lowlevel_ops` subset is reordered, so it is not ABI-compatible with either
upstream. `FUSE_SET_ATTR_MODE` through `MTIME_NOW` match upstream, but the
fallback's `CRTIME`/`CHGTIME`/`BKUPTIME`/`FLAGS` use bits 9–12 where macFUSE uses
28–31. The fallback is therefore a mixed, partly divergent declaration subset,
not a verbatim copy. `build.rs` always defines
`STUDIOFS_REAL_MACFUSE_HEADERS_REQUIRED`, so the fallback branch is never
compiled; removing it is the smallest P2 resolution.

The fallback also lacks the `forget` member that `low_level_ops.c` assigns, so
it could not have compiled the shim. The remediation deleted
`fuse_lowlevel_compat.h`, made `low_level_ops.c` include the installed
`<fuse_lowlevel.h>` directly and dropped the now-unused build define, so a source
release from the remediated tree no longer carries those declarations. The file
remains in reachable history at `70bd8984` and later baseline commits. Keep
SDK/package linking questions separate from rights to the declarations.

## Non-code assets, examples and fixtures

The original mark, variants, size exports, contact sheet and HTML boards entered
at `ee30a6de` (“Basic brand-system + icon”). The cat SVG/PNG are separate uploads
described above. `cat_logo_shine.png` and all other exported variants are also
included in P3; a raster export does not acquire rights independently of its
source image. README embeds the cat PNG, so exclusion would require updating
README and any remaining consumers as well as the source release set.

No tracked font files were found. The HTML boards request Inter, JetBrains Mono,
IBM Plex Sans, Manrope and Space Grotesk through Google Fonts. These are remote
references, not bundled font payloads; any future offline font inclusion needs
its own pinned license/notice evidence. The SVGs/PNGs have no accompanying
creation/permission record. Do not assume that upload, export or AI generation
is a sufficient rights record.

The [fixture README](../../crates/studiofs-meta/tests/fixtures/README.md)
calls the redb 4.1 databases “original” baselines, introduced at `71742ba3`, but
does not say who generated them, from which data, or under what permission.
It documents how the 4.3 metadata fixture updates a 4.1 database and how the
4.3 cache fixture is created/compacted; it also records their SHA-256 values.
That is useful generation evidence, but it does not clear the inherited
metadata baseline's contents. P4 covers all four fixtures until origin/data
review resolves them individually. Regeneration must preserve the old-format
compatibility scenarios; simply creating a current-format empty database
would invalidate the tests.

`docs/examples.md` uses sample CLI/configuration values. Benchmark scripts
generate workloads or take operator-supplied paths: SFS-612 uses an external
Curvine tree, and the Linux parity script uses a separately installed/local
JuiceFS reference. No Curvine/production-media dataset is bundled in the
tracked assets. Linked third-party vendor documentation and benchmark reference
values are not declared to be StudioFS-owned prose. No complete sentence-level
text comparison or binary database content audit was performed.

## Release-blocking evidence register

These are explicit **open** gates. Flagging them completes the audit record;
it does not satisfy the acceptance requirement that every third-party file
already has proven redistribution rights. The release owner must attach the
resolution evidence here and have SFS-854 verify the exact public commit and
any reachable history. Do not mark the repository cleared while rows remain.

| ID | Scope / missing evidence | Required resolution | Owner / gate |
| --- | --- | --- | --- |
| P1 | Project source, prose, scripts and contribution authority are inferred from local development history. Root license text is missing. | Maintainer records author/alias and employer/client authority, identifies copied/translated contributions with exact upstream terms, or excludes unresolved content. Add intended project license text without overriding upstream grants. | Maintainer confirmation; SFS-852 license formalization; SFS-854 publication gate. |
| P2 | macFUSE fallback header's upstream origin and notices are unresolved. **Tree remediated:** the never-compiled fallback was deleted; only reachable history remains. | Verify a macOS build with installed macFUSE headers (not run on the Windows remediation host). History: accepted by the maintainer decision below. | SFS-851 finding; maintainer remediation; SFS-853 notices; SFS-854 gate. |
| P3 | All 22 branding files, especially the separately uploaded cat artwork, lack a source/rights record. | Obtain creator/source and redistribution grant covering SVGs, PNG derivatives and HTML artwork; record required attribution and asset-specific terms. Otherwise replace/exclude assets and update consumers. | Maintainer and asset contributor; SFS-854 gate. |
| P4 | Four redb fixtures: incomplete baseline origin/content provenance. | Establish synthetic generation and right to publish data, document original redb version/schema/input or regenerate compatible synthetic baselines and derived fixtures with focused fixture tests. | Maintainer/fixture author; SFS-854 gate. |
| P5 | `.antigravitycli/9d7276d9-1579-4c0b-8fa1-49bdc747c598.json` is a tracked symlink (mode `120000`) introduced at `c3d34463`; its blob is an absolute user-home path, not JSON content. Windows checkouts without symlink support materialize it as a small plain file. The documentation review also found developer-local paths (`/home/val/…`, `E:\dev\studiofs-cli`, `/Volumes/Storage/dev/studiofs`) across plans/specs, a machine hostname in `docs/perf/sfs-473-linux-async-fuse-benchmark.md:8`, and an absolute checkout path in `docs/tests.md:1239`; no secrets were found. **Docs remediated:** tracked Markdown now uses `<repo-root>`, `<juicefs-checkout>` and `<host>` placeholders, and `tools/perf/linux-juicefs-parity-gate.sh` defaults to a sibling `../juicefs` checkout instead of `/home/val/dev/juicefs`. | **Tree remediated:** the symlink was untracked (it was already in `.gitignore`). History: accepted by the maintainer decision below. Complete the separate secret/private-data scan. | SFS-854 publication hygiene. |
| P6 | MIT-licensed agent-workflow templates are reproduced without their notices: `.agent/PLANS.md` (OpenAI Cookbook `articles/codex_exec_plans.md`, © 2025 OpenAI; full-text match) and the ExecPlan preamble/section skeleton copied into `docs/plans/` and `docs/superpowers/plans/`; `docs/agents/domain.md` and `docs/agents/triage-labels.md` (mattpocock/skills `setup-matt-pocock-skills`, © 2026 Matt Pocock); the "REQUIRED SUB-SKILL" plan header (obra/superpowers `writing-plans`, © 2025 Jesse Vincent). **Remediated:** `.agent/PLANS.md`, `docs/agents/domain.md` and `docs/agents/triage-labels.md` now open with the source, copyright line and full MIT permission notice in an HTML comment; the 16 superpowers headers were reworded to point at `.agent/PLANS.md`. | SFS-853 should list these MIT sources in the generated third-party notices; SFS-854 confirms no other copies. History: accepted by the maintainer decision below. The short ExecPlan preamble in individual plans derives from `.agent/PLANS.md`, which carries the notice. | SFS-853 notices; SFS-854 gate. |

SFS-849 dependency evidence gaps, SFS-850 native packaging conditions and
SFS-853 generated notices remain separate release gates. This source audit
does not supersede or close them. No permission requests have been sent to
contributors/upstreams and no new legal exception has been granted.

**Maintainer decision on reachable history (2026-10-23):** the repository will be
published with its full Git history, without rewriting or a squashed public
snapshot. Older commits therefore keep the developer-local paths and username,
the machine hostname, the `.antigravitycli` symlink, the removed macFUSE fallback
header and the template copies that predate their MIT notices. The maintainer
accepts that exposure; these are not secrets, and the username already appears
in many other public places. The SFS-854 secret scan still covers the full history.

External links in the documentation and this report returned HTTP 200 on
2026-10-23, except `github.com/valandreev/studiofs-cli` pages (releases, issues,
pull requests). They return 404 only because the repository is private until
release; recheck them after it is made public. Links to the gitignored
`docs/code-reviews/` files from a few plans do not resolve in the public tree.

## Reproduction and validation

From the repository root, use the immutable baseline when reproducing the
inventory, so subsequent report commits do not change its scope:

```powershell
git ls-tree -r c7be848af03c6bebaa5e890406e3d0ec8c0d729b
git rev-list --count c7be848af03c6bebaa5e890406e3d0ec8c0d729b
git shortlog -sn c7be848af03c6bebaa5e890406e3d0ec8c0d729b
git log --follow -- crates/studiofs-macfuse/csrc/fuse_lowlevel_compat.h
git log -- docs/branding/studiofs-logo-final
git log -- crates/studiofs-meta/tests/fixtures crates/studiofs-cache/tests/fixtures
git grep -n -I -i -E 'copyright|SPDX|copied from|adapted from|derived from|JuiceFS' c7be848af03c6bebaa5e890406e3d0ec8c0d729b -- crates docs README.md
git diff --check
```

The inventory integrity check compares every path/blob pair with the baseline
tree (no output from `Compare-Object` means an exact match), then reports
duplicate paths (expected `0`) and category totals:

```powershell
$base = 'c7be848af03c6bebaa5e890406e3d0ec8c0d729b'
$tree = git ls-tree -r $base | ForEach-Object { $meta, $path = $_ -split "`t", 2; "$path`t$(($meta -split ' ')[2])" }
$rows = Get-Content docs/legal/source-provenance-inventory.tsv | Select-Object -Skip 1 | ForEach-Object { , ($_ -split "`t") }
Compare-Object $tree ($rows | ForEach-Object { "$($_[0])`t$($_[1])" })
($rows | Group-Object { $_[0] } | Where-Object Count -gt 1).Count
$rows | Group-Object { $_[2] } | Sort-Object Name | Format-Table Count, Name
```

The TSV has no file-mode column; `git ls-tree` shows the one symlink (P5) and the
one executable script, `tools/perf/linux-juicefs-parity-gate.sh`. The delivery
checks also verify report-local links and the
fixture hashes recorded upstream in this repository, and inspect the final
documentation diff. These checks verify inventory integrity, not ownership.
No Rust/full suites, native builds, release archives or exhaustive historical
secret/similarity scans are part of this documentation-only audit.
