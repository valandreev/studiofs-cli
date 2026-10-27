# SFS-884 KSNI dependency license review

Reviewed 2026-10-27 for StudioFS 0.2.1. The dependency update adds or replaces
36 registry package identities and removes 44 old identities. The candidate
contains 649 registry packages and 12 workspace packages. Every changed crate
archive SHA-256 matches Cargo.lock; packaged manifest/license/notice evidence
matches the archive members. Exact evidence hashes and target roles are in the
[reviewed inventory](rust-dependency-inventory.json).

The updated `tray-icon 0.26.1` and `muda 0.21.2` keep their MIT/Apache grants
and Tauri attribution. Choose MIT for dual MIT/Apache components and preserve
both packaged texts in generated notices. The MIT-only D-Bus crates retain their
Zeeshan Ali Khan/contributor notices; `uds_windows` retains Microsoft's notice.
`futures-lite` also carries Alex Crichton/Tokio notices in `LICENSE-THIRD-PARTY`;
`parking` carries Rust Project Developers attribution in that file. Preserve
these additional files, rather than treating the wrapper manifest as the only
obligation. `task-local` retains Bugen Zhao's packaged MIT/Apache texts.

`ksni 0.3.7` introduces the standalone `Unlicense` expression. Its packaged
`UNLICENSE` contains the public-domain dedication and warranty disclaimer.
Retain that exact text and provenance. This is a reviewed grant, not a missing
license: `deny.toml` permits it only for `ksni@=0.3.7`; no general Unlicense
allowance or unknown-license exception is added. The real cargo-deny policy
rejected this dependency before that scoped entry, then passed afterward.

| Package | Manifest expression | Archive SHA-256 | Packaged notice evidence |
| --- | --- | --- | --- |
| `arc-swap 1.9.3` | `MIT OR Apache-2.0` | `380962b3d93b99642e50b9febf72c071e97e8b32d86eae9cf4ef582bb960ae6e` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-broadcast 0.7.2` | `MIT OR Apache-2.0` | `435a87a52755b8f27fcf321ac4f04b2802e337c8c4872923137471ec39c37532` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-channel 2.5.0` | `Apache-2.0 OR MIT` | `924ed96dd52d1b75e9c1a3e6275715fd320f5f9439fb5a4a11fa51f4221158d2` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-executor 1.14.0` | `Apache-2.0 OR MIT` | `c96bf972d85afc50bf5ab8fe2d54d1586b4e0b46c97c50a0c9e71e2f7bcd812a` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-io 2.6.0` | `Apache-2.0 OR MIT` | `456b8a8feb6f42d237746d4b3e9a178494627745c3c56c6ea55d92ba50d026fc` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-lock 3.4.2` | `Apache-2.0 OR MIT` | `290f7f2596bd5b78a9fec8088ccd89180d7f9f55b94b0576823bbbdc72ee8311` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-process 2.5.0` | `Apache-2.0 OR MIT` | `fc50921ec0055cdd8a16de48773bfeec5c972598674347252c0399676be7da75` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-recursion 1.2.0` | `MIT OR Apache-2.0` | `4f8abc12baad266b1c8cec146854c195b5864b4221d4b2ca7296a7ae82d9e451` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-signal 0.2.14` | `Apache-2.0 OR MIT` | `52b5aaafa020cf5053a01f2a60e8ff5dccf550f0f77ec54a4e47285ac2bab485` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `async-task 4.7.1` | `Apache-2.0 OR MIT` | `8b75356056920673b02621b35afd0f7dda9306d03c79a30f5c56c44cf256e3de` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `blocking 1.7.0` | `Apache-2.0 OR MIT` | `a70e4329df6cb94385eed412ec92375c3cdd8a6e502493d1229b6414e4036dfa` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `crossbeam-queue 0.3.14` | `MIT OR Apache-2.0` | `03e8bd762f7479489c70ed6c768ddca99d7296857de437a68dcb2a94365b3fae` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `endi 1.1.1` | `MIT` | `66b7e2430c6dff6a955451e2cfc438f09cea1965a9d6f87f7e3b90decc014099` | `LICENSE-MIT` |
| `enumflags2 0.7.12` | `MIT OR Apache-2.0` | `1027f7680c853e056ebcec683615fb6fbbc07dbaa13b4d5d9442b146ded4ecef` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `enumflags2_derive 0.7.12` | `MIT OR Apache-2.0` | `67c78a4d8fdf9953a5c9d458f9efe940fd97a0cab0941c075a813ac594733827` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `event-listener 5.4.2` | `Apache-2.0 OR MIT` | `5a23add41df1562121a9393cb065eab5146a1242410f23a644851e90cfd669d2` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `event-listener-strategy 0.5.4` | `Apache-2.0 OR MIT` | `8be9f3dfaaffdae2972880079a491a1a8bb7cbed0b8dd7a347f668b4150a3b93` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `futures-lite 2.6.1` | `Apache-2.0 OR MIT` | `f78e10609fe0e0b3f4157ffab1876319b5b0db102a2c60dc4626306dc46b44ad` | `LICENSE-APACHE`, `LICENSE-MIT`, `LICENSE-THIRD-PARTY` |
| `keyboard-types 0.8.3` | `MIT OR Apache-2.0` | `0fbe853b403ae61a04233030ae8a79d94975281ed9770a1f9e246732b534b28d` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `ksni 0.3.7` | `Unlicense` | `1bcc1cb43797140f74b711f1c4a49f48a7a7429f211c5fb78264e7bcd1e99caf` | `UNLICENSE` |
| `muda 0.21.2` | `Apache-2.0 OR MIT` | `4340e89c178916e62879c5c36b7f699019369be413e496178a6983ce91b8bc03` | `LICENSE-APACHE`, `LICENSE-MIT`, `LICENSE.spdx` |
| `ordered-stream 0.2.0` | `MIT OR Apache-2.0` | `9aa2b01e1d916879f73a53d01d1d6cee68adbb31d6d9177a8cfce093cced1d50` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `parking 2.2.1` | `Apache-2.0 OR MIT` | `f38d5652c16fde515bb1ecef450ab0f6a219d619a7274976324d5e377f7dceba` | `LICENSE-APACHE`, `LICENSE-MIT`, `LICENSE-THIRD-PARTY` |
| `pastey 0.2.3` | `MIT OR Apache-2.0` | `2ee67f1008b1ba2321834326597b8e186293b049a023cdef258527550b9935b4` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `piper 0.2.5` | `MIT OR Apache-2.0` | `c835479a4443ded371d6c535cbfd8d31ad92c5d23ae9770a61bc155e4992a3c1` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `serde_repr 0.1.21` | `MIT OR Apache-2.0` | `8d3b1629de253c70a0508c3899572da79ca359fdab27c7920ff00406df418906` | `LICENSE-APACHE`, `LICENSE-MIT` |
| `task-local 0.1.1` | `MIT OR Apache-2.0` | `2972044a9e5e448a506a7ff6f0d03b566d8ef4cd6918a58fc59835a0f8666626` | `LICENSE`, `LICENSE-APACHE`, `LICENSE-MIT` |
| `tray-icon 0.26.1` | `MIT OR Apache-2.0` | `4f5cc5858ecf9574ddd8f0032a2ff7ffd6f7724d2b81e48b9374984b3fd6ddc9` | `LICENSE-APACHE`, `LICENSE-MIT`, `LICENSE.spdx` |
| `uds_windows 1.2.1` | `MIT` | `f2f6fb2847f6742cd76af783a2a2c49e9375d0a111c7bef6f71cd9e738c72d6e` | `LICENSE` |
| `zbus 5.19.0` | `MIT` | `5db4be7c075cb421e4b7ee645541604239bd243ba7c357511f4ff3a74b555907` | `LICENSE` |
| `zbus_macros 5.19.0` | `MIT` | `2990635d09ade6df1868f72f8cac69a876a90981e8bd3c40b1be413f8dc88f40` | `LICENSE` |
| `zbus_names 4.3.5` | `MIT` | `812a12be4ead7a59e18ea8f047d08add0066b73bc62fc1467066a2472ae088dd` | `LICENSE` |
| `zcheapstr 1.1.0` | `MIT` | `d1afec51604565183aeb5c54c20aeab286120d4e4460f7f76e3e8bb8c0d99473` | `LICENSE` |
| `zvariant 5.15.1` | `MIT` | `80bf6d6cd6a4b601acb64cfb8284cdc643fcfa75ef26ae3c46ec8aa4e8a1fbb7` | `LICENSE` |
| `zvariant_derive 5.15.1` | `MIT` | `ef2672456ba1cb54b5eaec136e63135b3c7495fb938e53b28a62e633496d486d` | `LICENSE` |
| `zvariant_utils 4.2.0` | `MIT` | `bad0294361a320b694a328460dc73add56c306150f5cb6bfafc44446120008a3` | `LICENSE` |

The 613 unchanged registry identities keep their existing checksums and license
expressions. GTK3/GLib/AppIndicator and their orphaned bindings are removed from
the entire lockfile. Historical supplemental license files remain available for
older release evidence; the notice generator includes only resolved packages.

Reproduction from the repository root:

```bash
cargo metadata --locked --format-version 1 > target/sfs-884/metadata.json
python3 docs/legal/generate-rust-dependency-inventory.py \
  --metadata target/sfs-884/metadata.json \
  --output target/sfs-884/inventory-candidate.json
```

The reviewed candidate was deliberately promoted to the committed inventory.
The generator's historical `metadata_snapshot.path` label remains unchanged; the
actual input for this renewal is the explicit path above. Dependency graphs
were saved for Linux x86-64, Windows x86-64 and macOS ARM64. Linux selects
`ksni`, while Windows/macOS use their native backends. These graphs establish
package scope, not native platform execution or binary linkage.

Validation passed: cargo-deny 0.20.2 license checks (existing WinFsp deprecated
identifier warnings remain), real policy probes, inventory/notice self-tests,
and strict release notices with zero unresolved entries. The generated notices
retain the KSNI dedication, futures-lite/Tokio and parking/Rust attributions.
No new supplement or general license allowance was needed.

LF-normalized lockfile SHA-256: `8ab1c3aeebd3fec4de66bf8b91f9952d082f186622ba70c192f16808113f62a4`.
