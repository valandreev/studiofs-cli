# SFS-882 dependency license review

Reviewed 2026-10-24 (Europe/Kyiv) for the 26 newly added/updated registry
archives in `target/sfs-882/delta.json`. The candidate inventory describes
655 third-party registry packages and 12 workspace packages. The LF-normalized
`Cargo.lock` SHA-256 is
`dded12951cd1c32fce190f9410fdc5db0176fc7d2118eb6c7a4af1f4a08b4017`.

For every row below, the `.crate` archive was read from the local Cargo registry
cache, its SHA-256 matched the lockfile checksum, and the candidate inventory's
license-evidence files matched archive members. Decisions below are notice
reviews of the package archives; they do not establish which native code entered
a product binary.

| Package | Manifest expression | Reviewed archive SHA-256 | Archive evidence and decision |
| --- | --- | --- | --- |
| `aws-lc-rs 1.18.2` | `ISC AND (Apache-2.0 OR ISC)` | `faac5829c2b74c28f830747e7818ccfb684261b5f48a1118b1e2a13d36dfab13` | `LICENSE` (SHA-256 `b50b376e7d24a0598488b730c3034ffcd0b58dd36c0913a24b9903a3cfd04bf7`) contains the ISC grant and Apache/ISC text. Retain the mandatory ISC notice; choosing ISC for the disjunction is consistent with the existing audit decision. |
| `aws-lc-sys 0.46.0` | `ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0)` | `f1622d8446a2d4b2ce0c7eefc73dd43a99779028d5ee5c2dd8073a658ba8a2bc` | Inspected root `LICENSE` (`728536b4160e051f86d7c9c388f704866b3d512cd7df97ac3516c65279523c4e`), vendored `aws-lc/LICENSE` (`977c541bd25ffe36975dde25c028c0cd15ddf3d08243983b58e8a6a3d9447523`), and `aws-lc/third_party/fiat/LICENSE` (`43e358d7b6eb109d0f51f7b3a090fd82607965767c25fadee39e922475de2061`). The AWS-LC text identifies BoringSSL/Google ISC and Apache notices, OpenSSL Apache notices, mlkem-native/mldsa-native, Fiat MIT, s2n-bignum, jitterentropy BSD-3-Clause, CC0 contributions, and supplies full Apache-2.0, ISC, MIT, BSD-3-Clause, and MIT-0 text. Choose ISC for each OR arm while retaining mandatory ISC, Apache-2.0, MIT, and BSD-3-Clause obligations; ship the complete AWS-LC license text and Fiat notice. The archive contains s2n-bignum and jitterentropy native sources, but not the component LICENSE files named by the AWS-LC text (`third_party/s2n-bignum/.../LICENSE` and `third_party/jitterentropy/.../LICENSE`). Their copyright/SPDX statements and full selected license texts are reproduced in `aws-lc/LICENSE`; no additional text supplement is required if that exact file is included in the notice set. Per-file native source notices and the actual compiled AWS-LC source set still require artifact-level review. |
| `block-buffer 0.12.1` | `MIT OR Apache-2.0` | `d2f6c7dbe95a6ed67ad9f18e57daf93a2f034c524b99fd2b76d18fdfeb6660aa` | `LICENSE-MIT` and `LICENSE-APACHE` are present with RustCrypto Project Developers' copyright. Keep a selected grant and the packaged license texts. |
| `chacha20 0.10.2` | `MIT OR Apache-2.0` | `65c35e4b699c7e15ccbe7ee35c005e4fc0a278d22238a2857e6ce2dadeda1b06` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT notice names RustCrypto Project Developers. Preserve both supplied texts. |
| `cmov 0.5.4` | `Apache-2.0 OR MIT` | `0c9ea0ac24bc397ab3c98583a3c9ba74fa56b09a4449bbe172b9b1ddb016027a` | `LICENSE-APACHE` and `LICENSE-MIT`; RustCrypto Project Developers' copyright is present. Preserve the selected grant and packaged notices. |
| `const-oid 0.10.2` | `Apache-2.0 OR MIT` | `a6ef517f0926dd24a1582492c791b6a4818a4d94e789a334894aa15b0d12f55c` | `LICENSE-APACHE` and `LICENSE-MIT`; RustCrypto Project Developers' copyright is present. Preserve both supplied texts. |
| `cpufeatures 0.3.1` | `MIT OR Apache-2.0` | `5ca28b0ae3115b884660db4118d803791fd6756b6e88f39c0f3f7859060d7566` | `LICENSE-MIT` and `LICENSE-APACHE`; copyright is present. Preserve both supplied texts. |
| `crypto-common 0.2.2` | `MIT OR Apache-2.0` | `ce6e4c961d6cd6c9a86db418387425e8bdeaf05b3c8bc1411e6dca4c252f1453` | `LICENSE-MIT` and `LICENSE-APACHE`; RustCrypto Developers' copyright is present. Preserve both supplied texts. |
| `ctutils 0.4.5` | `Apache-2.0 OR MIT` | `38d28fb6aa7dd680b3ffd1a3e64e115c177bdba63c0ae7f9cd48129ff05c4fa0` | `LICENSE-APACHE` and `LICENSE-MIT`; RustCrypto Project Developers' copyright is present. Preserve both supplied texts. |
| `digest 0.11.3` | `MIT OR Apache-2.0` | `f1dd6dbb5841937940781866fa1281a1ff7bd3bf827091440879f9994983d5c2` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text also credits Artyom Pavlov. Preserve the exact packaged notices. |
| `h2 0.4.20` | `MIT` | `7d29020232d6aa3fb1daca64c1127cf662cf97f254ae16c18c05b8ab635fc118` | Root `LICENSE` carries the MIT permission and disclaimer with `h2 authors` copyright. Retain it. |
| `hmac 0.13.0` | `MIT OR Apache-2.0` | `6303bc9732ae41b04cb554b844a762b4115a61bfaa81e3e83050991eeb56863f` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text names Artyom Pavlov. Preserve both supplied texts. |
| `hybrid-array 0.4.10` | `MIT OR Apache-2.0` | `3944cf8cf766b40e2a1a333ee5e9b563f854d5fa49d6a8ca2764e97c6eddb214` | `LICENSE-MIT` and `LICENSE-APACHE`; RustCrypto Project Developers' copyright is present. Preserve both supplied texts. |
| `md-5 0.11.0` | `MIT OR Apache-2.0` | `69b6441f590336821bb897fb28fc622898ccceb1d6cea3fde5ea86b090c4de98` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text also credits Artyom Pavlov and Mozilla Foundation. Preserve exact notices. |
| `postgres-protocol 0.6.12` | `MIT OR Apache-2.0` | `08808e3c483c46e999108051c78334f473d5adb59d78bb80a1268c7e6aa6c514` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text names Steven Fackler. Preserve both. |
| `postgres-types 0.2.14` | `MIT OR Apache-2.0` | `851ca9db4932932d69f3ea811b1abe63087a0f740a47692619dd40d4899b68be` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text names Steven Fackler. Preserve both. |
| `quinn-proto 0.11.19` | `MIT OR Apache-2.0` | `0e750cca55fe4f0439a15d0bb529da9651e79993e8e72c61a899a36d462befbe` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text names Quinn Developers. Preserve both. |
| `rand 0.10.3` | `MIT OR Apache-2.0` | `65c9fb96cbc91e3478eaae79a69fcd3f1ae4ad052e471fe6732fff548984b4af` | `COPYRIGHT`, `LICENSE-MIT`, and `LICENSE-APACHE` are present. Preserve contributor attribution and selected license text. |
| `rand_core 0.10.1` | `MIT OR Apache-2.0` | `63b8176103e19a2643978565ca18b50549f6101881c443590420e4dc998a3c69` | `COPYRIGHT`, `LICENSE-MIT`, and `LICENSE-APACHE` are present. Preserve contributor attribution and selected license text. |
| `rand_pcg 0.10.2` | `MIT OR Apache-2.0` | `caa0f4137e1c0a72f4c651489402276c8e8e1cf081f3b0ba156d2cbeef09e86a` | `COPYRIGHT`, `LICENSE-MIT`, and `LICENSE-APACHE` are present. MIT notice also credits Melissa O'Neill and PCG Project contributors. Preserve all attributions. |
| `rustls 0.23.46` | `Apache-2.0 OR ISC OR MIT` | `48e13bd8c0e9365c43cfa5c9e8f9ad49d3c8444926c9aac819e0e4dc503c8fdf` | `LICENSE-APACHE`, `LICENSE-ISC`, and `LICENSE-MIT` are present; ISC/MIT texts name Joseph Birr-Pixton. Retain a selected grant and the packaged notice set. |
| `rustls-webpki 0.103.16` | `ISC` | `4443b1b6d39d7c663139942cc4558262562d2c84123bb6282ef177009ca8b281` | Root `LICENSE` supplies ISC-style terms and Brian Smith's copyright. It mentions `third-party/chromium/LICENSE`, but the exact crate archive has no `third-party/chromium` directory or Chromium data; no separate Chromium text is evidenced in this archive. Retain the complete root license and recheck only if a source/vendor bundle adds that absent material. |
| `sha2 0.11.1` | `MIT OR Apache-2.0` | `47d7069beb7d6ac7b9acd1039986e73443f24234f41074da099d6f994ac9ad19` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text also credits Artyom Pavlov and Mozilla Foundation. Preserve exact notices. |
| `tokio 1.54.1` | `MIT` | `b52fd3255258d26fac7264ab5a76b53c13367136dc011d3d6ad4f8ed070396a3` | Root `LICENSE` carries the MIT grant and Tokio Contributors' copyright. Retain it. |
| `tokio-postgres 0.7.18` | `MIT OR Apache-2.0` | `a528f7d280f6d5b9cd149635c8705b0dd049754bc67d81d31fa25169a93809d3` | `LICENSE-MIT` and `LICENSE-APACHE`; MIT text names Steven Fackler. Preserve both. |
| `webpki-roots 1.0.11` | `CDLA-Permissive-2.0` | `f5f280bbb2b287bc8e7de7e3852945a0ff50d1bbcb5ed42b6c7a568c546df171` | Root `LICENSE` contains the full CDLA-Permissive-2.0 agreement. The packaged README says its derived Mozilla root data comes from CCADB. Preserve the agreement and the existing CCADB provenance/attribution notice; the generated notice supplement must continue to include it. |

## Review limits and release follow-up

The 26 package identities and archive checksums were verified; their manifest
expressions, evidence paths, and archive-matched license/copyright texts were
reviewed. No changed package declares a newly observed license identifier.
This review did not run cargo-deny or the notice generator, inspect every
source-file header, or establish a product build's native link set. In
particular, AWS-LC remains a full-lockfile dependency with expanded
embedded-native notices; neither its presence in the lockfile nor this license
review proves it is linked into any release executable.

After the baseline inventory is intentionally renewed, confirm that generated
notices contain the full `aws-lc-sys 0.46.0` `aws-lc/LICENSE`, its nested Fiat
MIT text, and the CCADB attribution for `webpki-roots`. Then record new notice
receipt hashes. Release approval still requires strict notice generation and
native target builds/linkage and archive/source checks under the existing
publication gate.
