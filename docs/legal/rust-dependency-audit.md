# Rust dependency license audit for v0.2.0

## Security updates, 2026-10-24

The v0.2.1 lockfile intentionally adds and updates eight registry package
identities for the approved security dependency updates. The renewed inventory
contains 657 third-party packages and 12 workspace packages; the LF-normalized
lockfile SHA-256 is
`037ddc054286eab15a43c6732db4f6aaa19a2de9e3400a3479caba0521eae0d5`.
The LF-normalized reviewed inventory SHA-256 is
`8cd008549c29eb7158a32b3321ebde1c754f23c571a3a738be324b9362d365f9`.
The [package archive review](security-update-license-review-2026-10-24.md)
records the exact checksums and license evidence. All changed/new declarations
remain `Apache-2.0` or `MIT OR Apache-2.0`; no license-policy exception or
notice supplement was added. Strict release notice generation passed with zero
unresolved third-party evidence; the existing AWS-LC and Fiat texts and CCADB
attribution were present in its output. This review does not establish a binary
link set.

## PR #851 renewed baseline, 2026-10-24

The committed inventory is renewed for `xxhash-rust 0.8.15 -> 0.8.16` after
reviewing both published crate archives. The new archive SHA-256 is
`4d93c89cdc2d3a63c3ec48ffe926931bdc069eafa8e4402fe6d8f790c9d1e576`.
Both manifests declare `BSL-1.0`; their `LICENSE` files are byte-identical
(SHA-256 `c9bff75738922193e67fa726fa225535870d2aa1059f91452c411736284ad566`).
No changed copyright or additional embedded-component license terms were found.
Retain the existing Boost license text; no policy exception is needed.

Reproduction used `cargo metadata --locked --format-version 1` saved as
`target/pr851/metadata.json`, then
`python docs/legal/generate-rust-dependency-inventory.py --metadata target/pr851/metadata.json --output target/pr851/inventory-candidate.json`.
The lockfile also reassigns incoming dependency
edges among already reviewed `windows-core` and `windows-sys` versions; the
inventory records those edges and target features without changing their
archive evidence. Counts remain 667 lock entries, including 655 registry and
12 workspace packages, with unchanged package counts for all three target scopes.
The LF-normalized lockfile SHA-256 is
`54b0005c383f4f84d2ed8084444a29c9cb826c1b6cc3c37243ef808a6a2fe55c`;
the renewed inventory SHA-256 is
`c5530f695a3a965d119613e20a811c547de0a53fb2986dfe61af4cced929477a`.
Earlier audit and release-artifact hashes below remain historical evidence.

## SFS-882 renewed baseline, 2026-10-24

The committed inventory is intentionally renewed for the compatible security
updates in SFS-882 after reviewing all **26 added or updated crate archives**.
The [per-package review](sfs-882-dependency-license-review.md) records their
exact archive checksums, license expressions, copyright/notice decisions and
the expanded AWS-LC native notices. Eleven old versions were replaced and
15 additional versions were introduced; unchanged external identities retain
their prior checksums and license expressions. No license-policy exception or
baseline-validation relaxation was added.

The current ledger contains **667 lock entries: 655 registry packages and
12 workspace packages**, all workspace packages at 0.2.0. Its LF-normalized
lockfile SHA-256 is
`dded12951cd1c32fce190f9410fdc5db0176fc7d2118eb6c7a4af1f4a08b4017`.
Reviewed inventory SHA-256 is
`be36ad31ec34e4ed38d2958f684c71e975858db66266705def33310e84bddcb0`.
The inventory, expression counts and scope tables below describe the original
SFS-849 snapshot; use the current JSON ledger and SFS-882 review for this delta.
The existing distribution-profile decisions still apply: select MIT for the
new dual MIT/Apache RustCrypto packages, and preserve every mandatory term
of AWS-LC's compound expression, including ISC, Apache-2.0, MIT and BSD-3-Clause.

Reproduction used `cargo metadata --locked --format-version 1` saved as
`target/sfs-882/metadata.json`, then
`python3 docs/legal/generate-rust-dependency-inventory.py --metadata target/sfs-882/metadata.json --output target/sfs-882/inventory-candidate.json`.
The candidate was promoted to the committed inventory only after archive review.
The generator retains its historical `metadata_snapshot.path` label; the actual
input for this renewal is the explicit metadata path above. Target scope graphs
were regenerated on macOS ARM64 and remain dependency candidates, not native
build/link evidence.

Pinned cargo-deny 0.20.2 license checks, policy probes, inventory/notice self-tests,
and strict release notice generation passed. The generated notices reproduce
the full AWS-LC and Fiat texts and retain the CCADB attribution. Current hashes
and publication limits are in the [publication gate](v0.2.0-publication-gate.md).
No actual release archive or native linkage approval is implied.

## Original SFS-849 audit (historical)

Engineering audit for SFS-849,
checked 2026-10-23. **Inventory complete; public release remains conditional on
the blockers below.** The selected manifest-level licenses have no identified
GPL-3.0-only conflict; missing notice evidence and embedded components are
explicit exceptions, not blanket approvals. This is not a legal opinion or a
claim that future binaries are cleared for distribution.

## Pinned inputs and deliverables

The baseline is commit `778070e39253d3744f027799d9aed9cbde6d4cfe`, on
`feature/sfs-849-audit-rust-dependency-licenses-for-v020`. `Cargo.lock` SHA-256 is
`5e4aeef99a7b65931c365dba7a7eb07d8017a14bad330ff08ccd0fe61cb36946`
(LF-normalized committed bytes; workspace manifests are hashed the same way, so
the value does not depend on a checkout's `core.autocrlf`).
The workspace version at this baseline is **0.1.1**; this audit prepares 0.2.0
and must be regenerated for the actual release lockfile.

SFS-852 update, 2026-10-23: the current workspace declares `GPL-3.0-only`
and includes [LICENSE](../../LICENSE). The pinned inventory below retains its
original manifest values and evidence gaps; it is not regenerated by this update.

[rust-dependency-inventory.json](rust-dependency-inventory.json) is the per-package
ledger. It accounts for **652 lock entries: 640 third-party registry packages
and 12 first-party workspace packages**. All third-party sources are crates.io
registry entries; there are no Git or third-party path packages in this lockfile.
Every registry package supplies a manifest `license` value; none supplies a
Cargo `license-file` value. These observations do not prove that every source
file or embedded asset has the same license as its package manifest.

Each ledger row records name/version/source/checksum, original license expression,
repository, hashed manifest and discovered license/notice files, and the target
roles/features described below. Rows reference the distribution profile table by
expression ID. Original expressions, including deprecated slash syntax, remain
unchanged. License files are evidence, rather than generated third-party notices.

## Scope and reproduction

The full lockfile includes transitive, normal, build, dev, workspace-tool, and
platform-specific dependencies. Product scope is `cargo build -p studiofs
--release --locked` with default features. Root `cargo build --release` can also
build the other workspace members, including E2E/scenario tools; do not label
their entire dependency closure as the product binary.

The generator records representative graphs for `x86_64-pc-windows-msvc`,
`x86_64-unknown-linux-gnu`, and `aarch64-apple-darwin`. These are **Cargo dependency
graph candidates observed from Windows**, not native builds, link maps, an SBOM
of executable bytes, or evidence for every architecture/feature combination.
The exact Cargo commands and role definitions are in the generator script;
the inventory records each target's selection source.
Cargo itself describes `tree -e normal,build` as an approximation of a build,
rather than a guarantee of exact compiled contents.
([Cargo tree](https://doc.rust-lang.org/cargo/commands/cargo-tree.html),
[metadata](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html))

A runtime role means reachable normal dependencies after excluding proc-macro
tools. A host-build role means build dependencies or proc-macro implementation
dependencies, and can coexist with runtime use. **Build does not mean absent
from distribution**: native C/assembly produced by build scripts and tokens/data
generated by procedural macros can enter the executable. Packages outside the
three product graphs remain in the ledger; they are workspace/dev/other-target
scope, not omitted or proven irrelevant to a source/vendor bundle. Vendoring or
shipping test tools changes the distribution set and requires their notices too.

From the repository root, reproduce using installed Python 3.11+ and Cargo:

```powershell
New-Item -ItemType Directory -Force target/sfs-849 | Out-Null
cargo metadata --locked --format-version 1 > target/sfs-849/metadata.json
python docs/legal/generate-rust-dependency-inventory.py --self-test
python docs/legal/generate-rust-dependency-inventory.py
```

The final generator command and validation results are recorded below. No
credentials, `.env`, database, S3 access, build, or full Rust test suite is needed.
The first metadata command can fetch missing sources; graph regeneration uses
the pinned local cache. Metadata contains machine-specific paths, so its raw hash
is specific to the captured input, while the lock checksum pins the dependency
universe. Recheck any mismatch instead of silently refreshing the accepted report.

## Distribution profiles

The table below chooses licenses for this snapshot, not new licenses for upstream
code. `OR` permits a choice; `AND` retains every required component; `WITH` retains
its specified exception. For legacy `/` values, use the upstream dual-license
grant and retain the original expression in the inventory. Do not apply a generic
slash-to-OR transformation to unknown future licenses.
([Cargo manifest](https://doc.rust-lang.org/cargo/reference/manifest.html#the-license-and-license-file-fields))

Profile obligations apply to each package selecting that profile, with additional
per-file/component notices preserved. The selected terms permit GPLv3 combination
subject to their conditions; they do not erase upstream licenses. The FSF confirms
Apache-2.0 compatibility with GPLv3 and explains the MPL larger-work path.
([FSF license list](https://www.gnu.org/licenses/license-list.html))

| Profile | Required distribution treatment |
| --- | --- |
| MIT | Preserve each copyright and permission notice; include its license text in notices/source. |
| Apache | Include Apache-2.0 text, retain applicable copyright/patent/trademark/attribution notices, mark changed files, and reproduce applicable upstream NOTICE content when supplied. Patent-termination terms remain. |
| ISC | Preserve copyright and permission notices. |
| BSD2 / BSD3 | Preserve copyright, conditions, and disclaimer in source; reproduce them in binary documentation/materials. BSD3 also forbids endorsement using contributor names without permission. |
| Zlib | Do not misrepresent origin, mark altered source, and retain the notice in source distributions. Keep attribution in the release notice set as well. |
| Boost | BSL-1.0 is the **Boost Software License**, not Business Source License. Retain its notices/text for source and derivatives; it expressly excepts copies solely in executable object-code form. |
| CC0 | Preserve provenance and the CC0 dedication/fallback text in the notice archive; CC0 itself does not impose attribution. No patent/trademark grant is inferred. |
| Unicode | Preserve the Unicode copyright and permission notice with redistributed software/data. This is Unicode-3.0, not an assumed license for every Unicode publication. |
| MPL | Retain MPL-2.0, make the covered source (including unmodified covered files) available to executable recipients, and tell them how to obtain it. Modifications to covered files remain under MPL; GPL larger-work distribution follows MPL §3.3. |
| CDLA | Include the CDLA-Permissive-2.0 agreement with shared data, including the embedded root dataset. Record the CCADB provenance/attribution noted below. |
| LLVM | Retain Apache-2.0 **and** LLVM exception text; apply the actual exception conditions to embedded portions. Do not treat WITH as a second optional license. |
| GPL | GPL-3.0's deprecated SPDX identifier means GPL-3.0-only here. Preserve GPLv3 text/notices and provide corresponding source and necessary build material by a compliant distribution method. See the WinFsp evidence exception below. |

The Apache profile follows [Apache §4](https://www.apache.org/licenses/LICENSE-2.0).
Other obligation sources are the exact packaged texts indexed in the ledger and
the primary references in the exception reviews below. Combined profiles retain
all named obligations. The complete expression decisions appear at the end of
this report.

## Explicit dependency reviews

| Pinned dependency | Decision and additional evidence |
| --- | --- |
| `winfsp 0.12.4+winfsp-2.1`; `winfsp-sys 0.12.1+winfsp-2.1` | Both manifest expressions are GPL-3.0. Compatible with intended GPL-3.0-only StudioFS without relying on WinFsp's custom FLOSS exception. Neither cached crate includes a standalone license file. **Notice/provenance blocker:** verify the exact binding/native terms and retain the full GPL text, copyright and any applicable notices. The sys crate contains WinFsp headers, import libraries and DLLs; its build script copies DLLs only when `WINFSP_DLL_OUTPUT_PATH` is set. Separate user-installed WinFsp remains the packaging decision. Do not infer future archive contents from that script. |
| `opendal 0.55.0` | Apache-2.0. Preserve packaged LICENSE and source headers. The packaged crate has no NOTICE file; do not invent one or omit notices from its transitive dependencies. S3/memory features are from the workspace declaration; full enabled feature sets are in the ledger. |
| `ring 0.17.14` | Apache-2.0 **AND** ISC; retain both. Packaged LICENSE, LICENSE-BoringSSL, LICENSE-other-bits, `third_party/fiat/LICENSE` and polyfill license files cover Rust/C/assembly components. File-level notices remain required; selecting one license for the whole package is incorrect. |
| `mimalloc 0.1.49`; `libmimalloc-sys 0.1.46` | MIT wrapper plus separately copyrighted MIT native allocator. Retain wrapper LICENSE.txt and the bundled `c_src/mimalloc/v2/LICENSE` and `v3/LICENSE` for a source/vendor bundle; actual compiled version must be determined from the selected build. |
| `tikv-jemalloc-sys 0.6.1+5.3.0-1-ge13ca993e8ccb9ba9847cc330696e02839f328f7` | Select MIT for the wrapper, **plus BSD-2-Clause for vendored jemalloc**. Packaged `jemalloc/COPYING` SHA-256 is `94aa2caa98c25d942f58b956c71dba6a99ff98fc3a31cbc669fe2a4cd0268b53`. It requires source/binary notice retention and allows file-specific exceptions. The wrapper manifest alone is incomplete; SFS-853 must retain the C allocator's notice too. |
| `symbolic-demangle 12.17.2`; `pprof_util 0.8.2` | Unix profiling dependencies. Symbolic includes Swift C++ source with Apache and LLVM/NCSA license files; current Linux/macOS product graphs enable `cpp,rust` and their Rust dependencies, **not swift**. A source/vendor bundle still needs the packaged Swift notices. Enabling Swift requires a new compiled-component review. `pprof_util` is Apache-2.0 Rust code; its missing standalone text stays in the evidence-gap list. |
| `aws-lc-rs 1.16.2` | `ISC AND (Apache-2.0 OR ISC)`; choose ISC for the disjunction, retaining all ISC notices. |
| `aws-lc-sys 0.39.1` | Keep mandatory ISC, Apache-2.0, MIT, BSD-3-Clause. Choose ISC for all three OR subexpressions. Retain wrapper LICENSE, vendored `aws-lc/LICENSE`, Fiat LICENSE, and applicable per-file notices. The compound expression is not a license menu for the entire crate. |
| GTK family | `gtk 0.18.2`, `gtk-sys 0.18.2`, `gtk3-macros 0.18.2` and the ATK/GLib/GDK/GIO/Pango/Cairo Rust wrappers declare MIT. Linux production selects these through tray/menu dependencies. Their separate native libraries have their own LGPL/permissive terms; wrapper MIT declarations do not license native GTK. |

AWS-LC is activated by `aws-sdk-s3` and the explicit `rustls-aws-lc` feature of
`aws-smithy-http-client` in the **dev-dependencies** of `studiofs-data` and
`studiofs-vfs`. Product metadata uses Rustls with `default-features = false` and
`ring`. Its broad workspace presence is not evidence that the default product
ships AWS-LC. The per-target product graphs provide the narrower reachability
evidence, while AWS-LC stays in the full lock inventory.

Native-platform packaging decisions and previous platform linkage evidence are
in [platform-distribution.md](platform-distribution.md). Those observations were
not rerun here. In particular, macFUSE/WinFsp remain separate installs, Linux
libraries remain system dependencies, and MFMount/FSKit is excluded from 0.2.0
artifacts. This report neither changes nor clears those native distribution gates.

Primary evidence: exact registry package contents and hashes in the inventory;
[WinFsp license](https://github.com/winfsp/winfsp/blob/master/License.txt),
[winfsp-rs](https://github.com/SnowflakePowered/winfsp-rs),
[ring 0.17.14 LICENSE](https://docs.rs/crate/ring/0.17.14/source/LICENSE),
[aws-lc-sys 0.39.1 LICENSE](https://docs.rs/crate/aws-lc-sys/0.39.1/source/LICENSE),
[libmimalloc-sys 0.1.46](https://docs.rs/crate/libmimalloc-sys/0.1.46/source/),
[OpenDAL 0.55.0 LICENSE](https://docs.rs/crate/opendal/0.55.0/source/LICENSE).
Mutable upstream URLs explain terms; the hashed package snapshot is the version
evidence and does not silently follow upstream changes.

The WinFsp sys crate's `winfsp/.gitrev` pins native revision
`5251dcbce93a334cc2b69565eb43de42eeaa2a64`; the missing upstream text can be
sourced from that revision's
[License.txt](https://github.com/winfsp/winfsp/blob/5251dcbce93a334cc2b69565eb43de42eeaa2a64/License.txt).
Its content was not successfully fetched in this pass, so this is a precise
follow-up pointer, not a verified license-text hash. The binding `.cargo_vcs_info`
revisions are `b26647554eebbaa38897c4c50689807127ee3224` (`winfsp`) and
`5ae65f6ae65837bf368887672c5baca7542c3cc7` (`winfsp-sys`).
The jemalloc wrapper's revision is
`df5934f67d7b462b431f7fe9658952178c2bf2f4`; its actual packaged COPYING is hashed
in the ledger regardless of mutable upstream labels.

## Other exceptions and less common terms

`option-ext 0.2.0` is MPL-2.0, used through the directory helpers. Its source and
license need to remain available to binary recipients under MPL terms; simply
shipping the GPL text is insufficient. GPL combination uses the MPL §3.3 route.
The inspected package has no attached Exhibit B / incompatible-secondary-license
notice outside the license template. Its generic Exhibit B template does not
itself mark the package incompatible. ([MPL text](https://www.mozilla.org/en-US/MPL/2.0/),
[Mozilla FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/))

`webpki-roots 1.0.6` carries **CDLA-Permissive-2.0** for root-certificate data.
Its LICENSE is the agreement and its README identifies CCADB provenance. Include
the agreement and preserve CCADB attribution in the release notices, with no
implied endorsement. Treat this as a data-terms review, not an MIT Rust wrapper.
The extra CCADB terms/provenance must be reviewed in SFS-853's actual notice set
before final distribution approval.
([CDLA](https://cdla.dev/permissive-2-0/),
[CCADB terms](https://www.ccadb.org/rootstores/usage))

The 18 Unicode-3.0-only packages are ICU4X and supporting collections/provider
crates (all names/versions are in the ledger). `unicode-ident 1.0.23` additionally
requires Unicode-3.0 **and** the selected MIT branch. Preserve both; OR MIT does
not remove the final AND Unicode component.
([Unicode licensing](https://unicode.org/faq/unicode_license.html))

`encoding_rs 0.8.35` similarly selects **MIT AND BSD-3-Clause**.
`dpi 0.1.2` requires **Apache-2.0 AND MIT**. The package-level choices never
override separately licensed embedded source/data.

`target-lexicon 0.12.16` has only Apache-2.0 WITH LLVM-exception; retain the
exception. The 17 packages offering that license OR Apache-2.0 OR MIT select MIT.
`r-efi 5.3.0` selects MIT instead of its LGPL alternative. `dunce 1.0.5` selects
Apache-2.0 instead of CC0/MIT-0. `ryu 1.0.23` selects Apache-2.0 instead of Boost.
`xxhash-rust 0.8.15` has Boost-only terms. `tiny-keccak 2.0.2` has CC0-only terms.
The packaged `dunce` LICENSE is CC0 text even though its manifest also grants
Apache-2.0: supply the selected Apache text and applicable notice rather than
treating a discovered filename as proof that the selected text is already there.
([LLVM policy](https://www.llvm.org/docs/DeveloperPolicy.html#new-llvm-project-license-framework),
[Boost](https://www.boost.org/LICENSE_1_0.txt),
[CC0](https://creativecommons.org/publicdomain/zero/1.0/legalcode.en))

The ledger preserves the legacy expressions `MIT/Apache-2.0`, `Apache-2.0/MIT`,
`Apache-2.0 / MIT`, and `Unlicense/MIT`. They represent this ecosystem's historical
dual-license convention; choose MIT, retain both original evidence and the selected
notice, and use modern OR syntax in SFS-853 tooling. `same-file` and `walkdir`
COPYING files explicitly grant either Unlicense or MIT. `fnv` includes both license
texts. Do not rewrite third-party manifests or the lockfile to normalize labels.

## Release blockers and ownership

| Gate | Resolution required before public distribution | Owner |
| --- | --- | --- |
| Missing packaged license/notice evidence | Resolve each package listed in the final evidence-gap section against its exact source revision, or exclude it from the distributed set. A manifest label is not a replacement for the copyright/permission notice. Verify vendored/native per-file supplements. | SFS-853; SFS-854 validates artifacts |
| StudioFS license text and spelling | Resolved by SFS-852: root LICENSE contains the GPLv3 text and workspace metadata declares GPL-3.0-only. The pinned inventory preserves the pre-correction evidence. | SFS-852 |
| Complete corresponding source and notices | Provide GPL-compliant corresponding source/build material, MPL source access, selected permissive texts/attributions and native supplements. Verify WinFsp pinned evidence and CDLA/CCADB notice treatment. Generated JSON alone satisfies none of these packaging obligations. | SFS-853 / SFS-854 |
| Exact target, architecture and feature set | Reproduce on native release builders, inspect actual executable links/generated content and archive contents, and account for any target/features different from these three graphs. Host build tools are not automatic binary inclusions or exclusions. | SFS-854 |
| Native platform conditions | Follow SFS-850's documented external-driver/system-library policy and MFMount exclusion; check actual artifacts. No native licensing exception is newly granted by this report. | SFS-850 decisions; SFS-854 enforcement |
| Dependency drift | Regenerate at the exact 0.2.0 tag/lockfile; review changed expressions and source evidence. Unknown/custom/new licenses must fail review rather than inherit a blanket allowance. | SFS-853 / SFS-854 |

There is **no identified manifest-selected incompatible license** in the current
lockfile. Evidence gaps above remain unresolved release conditions. The conclusion
is conditional on the recorded selections, valid upstream grants, correct
provenance, and fulfillment of the obligations; it is not approval to publish.

## Validation and evidence gaps

The captured registry archives match **all 640 Cargo.lock checksums**. The
generator compares discovered manifest/license evidence with the corresponding
archive members; **1,703 registry evidence files match**. Thus the report is tied
to the pinned package contents, rather than merely trusting an unpacked cache or
current upstream README. This verifies the inspected evidence bytes, not every
source file's legal grant.

| Representative target | All product-graph packages, including workspace | Third-party runtime candidates | Third-party host-build candidates |
| --- | ---: | ---: | ---: |
| Windows x86_64 MSVC | 322 | 270 | 86 |
| Linux x86_64 GNU | 409 | 344 | 72 |
| macOS aarch64 | 353 | 309 | 42 |

Runtime/build columns overlap. There are **161 registry packages outside all
three product scopes**, plus three first-party workspace tools. The ledger keeps
incoming resolved dependency edges with their normal/dev/build kinds and target
predicates, so these packages can be traced without pretending all are dev-only.
Feature names come from the product's Cargo tree; candidate roles propagate
through target-filtered metadata restricted to that tree's membership. Cargo
metadata can unify workspace features, so these roles are a conservative graph
description, not an exact Cargo compilation-unit model.

The filename-based evidence scan finds a primary LICENSE/COPYING/NOTICE/COPYRIGHT/
UNLICENSE file for **575 rows**. Primary means a non-source file at the package
root or in a root `LICENSES/` directory; nested vendor notices (for example
`symbolic-demangle`'s `vendor/swift/LICENSE.txt`) and source files such as
`objc2-foundation`'s `src/copying.rs` are listed as evidence rows but do not
satisfy the status. **65 registry packages** lack such a named file;
the other **12** are first-party workspace packages whose root LICENSE was
missing at the audited baseline (added by SFS-852). This is a notice-evidence
gap in that snapshot, not a claim that the crates have no license grant. Some have notices in README or source headers: for example,
`wasite 1.0.2` README names “The Wasite Contributors” and its license alternatives.
The exact source text still needs to enter the selected-license notice artifact.

Every registry gap is listed below. W/L/M identify membership in the three
representative product graphs; an empty scope requires review if redistributed
as source/vendor/test tools or selected on another target. For each listed
package, SFS-853 must retrieve the exact revision's selected license/copyright
notice (or verified README/header grant), record provenance, and include it when
distributed; SFS-854 must reject any shipped unresolved entry. The ledger's
`license_evidence_status` and package checksum identify each item mechanically.

| Product graph scope | Packages missing a named primary license/notice file |
| --- | --- |
| L | `libappindicator-sys 0.9.0` |
| L,M | `mappings 0.7.2`; `pprof_util 0.8.2`; `symbolic-common 12.17.2`; `symbolic-demangle 12.17.2` |
| M | `block2 0.5.1`; `block2 0.6.2`; `dispatch 0.2.0`; `objc-sys 0.3.5`; `objc2 0.5.2`; `objc2 0.6.4`; `objc2-app-kit 0.2.2`; `objc2-app-kit 0.3.2`; `objc2-cloud-kit 0.3.2`; `objc2-core-data 0.3.2`; `objc2-core-foundation 0.3.2`; `objc2-core-graphics 0.3.2`; `objc2-core-image 0.3.2`; `objc2-core-text 0.3.2`; `objc2-core-video 0.3.2`; `objc2-encode 4.1.0`; `objc2-foundation 0.2.2`; `objc2-foundation 0.3.2`; `objc2-io-kit 0.3.2`; `objc2-quartz-core 0.3.2`; `objc2-system-configuration 0.3.2` |
| Outside W/L/M | `base64-simd 0.8.0`; `crc-catalog 2.4.0`; `dispatch2 0.3.1`; `gloo-timers 0.3.0`; `jni 0.22.4`; `jni-macros 0.22.4`; `jni-sys-macros 0.4.1`; `ndk 0.9.0`; `ndk-context 0.1.1`; `ndk-sys 0.6.0+11769913`; `objc2-cloud-kit 0.2.2`; `objc2-contacts 0.2.2`; `objc2-core-data 0.2.2`; `objc2-core-image 0.2.2`; `objc2-core-location 0.2.2`; `objc2-io-surface 0.3.2`; `objc2-link-presentation 0.2.2`; `objc2-metal 0.2.2`; `objc2-quartz-core 0.2.2`; `objc2-symbols 0.2.2`; `objc2-ui-kit 0.2.2`; `objc2-uniform-type-identifiers 0.2.2`; `objc2-user-notifications 0.2.2`; `r-efi 5.3.0`; `valuable 0.1.1`; `vsimd 0.8.0`; `wasip2 1.0.2+wasi-0.2.9`; `wasip3 0.4.0+wasi-0.3.0-rc-2026-01-06`; `wasite 1.0.2`; `wasm-encoder 0.244.0`; `wasm-metadata 0.244.0`; `wasmparser 0.244.0`; `winapi-i686-pc-windows-gnu 0.4.0`; `winapi-x86_64-pc-windows-gnu 0.4.0`; `wit-component 0.244.0`; `wit-parser 0.244.0` |
| W | `winfsp 0.12.4+winfsp-2.1`; `winfsp-sys 0.12.1+winfsp-2.1` |
| W,L,M | `crc32c 0.6.8` |

The generator self-test covers tree parsing, normal/build/proc-macro propagation,
shared roles, development-edge exclusion, cycle termination, rejection of unknown
tree lines, tampered checksum maps, and missing archive members. On 2026-10-23,
the self-test passed and a fresh generator run to
`target/sfs-849/verified-inventory.json` was **byte-identical** to the committed
snapshot. Independent assertions verified the 652 lock identities, all 640
archive checksums and all 1,703 evidence members. Inventory SHA-256:
`b396e84001ef1fd5d7cc71b19c1050445984d339cf052ef8695b59f762ef9e49`.
Tools: Python 3.13.7, Cargo 1.99.0 (`5f94df478`), rustc 1.99.0 (`b940084d7`).
Checksum-file-only fallback is explicitly unresolved until archive verification;
the current snapshot uses actual archives for all registry packages. Review
dispositions and final whitespace validation are recorded in the execution plan.
Rust builds/full tests, native release links, archive assembly,
and a complete per-source-file copyright analysis were not run. Final packaging
must resolve generated code/data and additional per-file grants; the scanner
finds named evidence files rather than proving all embedded notices were found.

## Complete manifest expression decisions

The per-package ledger's expression ID is `spdx:` plus the first 16 hexadecimal
SHA-256 characters of the exact original license string. Counts below concern
the 640 third-party packages; first-party workspace rows stay separately marked.

| Original expression / decision ID | Packages | Selected terms | Profiles |
| --- | ---: | --- | --- |
| `(Apache-2.0 OR MIT) AND BSD-3-Clause`<br>`spdx:cbfb06ff0df16a04` | 1 | `MIT AND BSD-3-Clause` | MIT + BSD3 |
| `(MIT OR Apache-2.0) AND Unicode-3.0`<br>`spdx:cc107737a812052a` | 1 | `MIT AND Unicode-3.0` | MIT + Unicode |
| `0BSD OR MIT OR Apache-2.0`<br>`spdx:30ca2a9fb1bc534a` | 1 | `MIT` | MIT |
| `Apache-2.0`<br>`spdx:2af71558e438db0b` | 34 | `Apache-2.0` | Apache |
| `Apache-2.0 / MIT`<br>`spdx:7dcb5226ace66964` | 1 | `MIT` | MIT |
| `Apache-2.0 AND ISC`<br>`spdx:2057641fc66432f3` | 1 | `Apache-2.0 AND ISC` | Apache + ISC |
| `Apache-2.0 AND MIT`<br>`spdx:38e0d1ca69c066e1` | 1 | `Apache-2.0 AND MIT` | Apache + MIT |
| `Apache-2.0 OR BSL-1.0`<br>`spdx:46aa28e6a83c89ca` | 1 | `Apache-2.0` | Apache |
| `Apache-2.0 OR BSL-1.0 OR MIT`<br>`spdx:152ff209273c124a` | 2 | `MIT` | MIT |
| `Apache-2.0 OR ISC OR MIT`<br>`spdx:4f3687dfffe45524` | 3 | `MIT` | MIT |
| `Apache-2.0 OR MIT`<br>`spdx:12c4662dfa8e784a` | 33 | `MIT` | MIT |
| `Apache-2.0 WITH LLVM-exception`<br>`spdx:3d495bd9cc536aeb` | 1 | `Apache-2.0 WITH LLVM-exception` | LLVM |
| `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`<br>`spdx:0ba40231f7ee15be` | 17 | `MIT` | MIT |
| `Apache-2.0/MIT`<br>`spdx:a7af1b0aa267b3f1` | 3 | `MIT` | MIT |
| `BSD-2-Clause`<br>`spdx:248dd895a2f89e28` | 1 | `BSD-2-Clause` | BSD2 |
| `BSD-2-Clause OR Apache-2.0 OR MIT`<br>`spdx:4c74218e368f4fc9` | 2 | `MIT` | MIT |
| `BSD-3-Clause`<br>`spdx:118dbcd2d5c9f9b2` | 5 | `BSD-3-Clause` | BSD3 |
| `BSD-3-Clause OR Apache-2.0`<br>`spdx:85d778b95d9ab3dd` | 2 | `Apache-2.0` | Apache |
| `BSD-3-Clause OR MIT OR Apache-2.0`<br>`spdx:7577f3745d732c4d` | 2 | `MIT` | MIT |
| `BSL-1.0`<br>`spdx:955a202342b3b672` | 1 | `BSL-1.0` | Boost |
| `CC0-1.0`<br>`spdx:6e237c55b0583cb7` | 1 | `CC0-1.0` | CC0 |
| `CC0-1.0 OR MIT-0 OR Apache-2.0`<br>`spdx:590c808905767d61` | 1 | `Apache-2.0` | Apache |
| `CDLA-Permissive-2.0`<br>`spdx:cb277f172e98a156` | 1 | `CDLA-Permissive-2.0` | CDLA |
| `GPL-3.0`<br>`spdx:3207cff3bf6b01bb` | 2 | `GPL-3.0-only` | GPL |
| `ISC`<br>`spdx:d8d62d58d661d5dd` | 4 | `ISC` | ISC |
| `ISC AND (Apache-2.0 OR ISC)`<br>`spdx:f152bfd226674693` | 1 | `ISC` | ISC |
| `ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0)`<br>`spdx:3d3f4adc8c1e1c4c` | 1 | `ISC AND Apache-2.0 AND MIT AND BSD-3-Clause` | ISC + Apache + MIT + BSD3 |
| `MIT`<br>`spdx:e5dcffe836b6ec8a` | 141 | `MIT` | MIT |
| `MIT OR Apache-2.0`<br>`spdx:db11fec9946737df` | 291 | `MIT` | MIT |
| `MIT OR Apache-2.0 OR LGPL-2.1-or-later`<br>`spdx:feaf9d5ed80fd081` | 1 | `MIT` | MIT |
| `MIT OR Apache-2.0 OR Zlib`<br>`spdx:c0fb98161c4fbc6e` | 5 | `MIT` | MIT |
| `MIT OR Zlib OR Apache-2.0`<br>`spdx:7604adf7326d513a` | 1 | `MIT` | MIT |
| `MIT/Apache-2.0`<br>`spdx:ec5c82d846feaa2a` | 29 | `MIT` | MIT |
| `MPL-2.0`<br>`spdx:09962c1dc23fac80` | 1 | `MPL-2.0` | MPL |
| `Unicode-3.0`<br>`spdx:2163353ae6271625` | 18 | `Unicode-3.0` | Unicode |
| `Unlicense OR MIT`<br>`spdx:2f9f1b1a9e44ec2d` | 9 | `MIT` | MIT |
| `Unlicense/MIT`<br>`spdx:30d4520d7b2a847c` | 2 | `MIT` | MIT |
| `Zlib`<br>`spdx:dc65a73a784e2954` | 3 | `Zlib` | Zlib |
| `Zlib OR Apache-2.0 OR MIT`<br>`spdx:1deaa5a03ae93f5b` | 15 | `MIT` | MIT |
