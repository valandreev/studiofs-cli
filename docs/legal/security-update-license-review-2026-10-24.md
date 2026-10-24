# Security update dependency license review

Reviewed 2026-10-24 (Europe/Kyiv) for the eight added or updated registry
package identities in the v0.2.1 lockfile. The renewed candidate inventory
contains 657 registry packages and 12 workspace packages. Its LF-normalized
`Cargo.lock` SHA-256 is
`037ddc054286eab15a43c6732db4f6aaa19a2de9e3400a3479caba0521eae0d5`.

For each entry, the inventory generator verified that the cached `.crate`
archive SHA-256 matches the lockfile checksum and that each listed license or
copyright file matches its archive member.

| Package change | Manifest expression | Archive SHA-256 | Archive evidence and decision |
| --- | --- | --- | --- |
| `aws-smithy-async 1.2.13 -> 1.2.14` | `Apache-2.0` | `2ffcaf626bdda484571968400c326a244598634dc75fd451325a54ad1a59acfc` | Root `LICENSE`, SHA-256 `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b`. Expression and license text are unchanged; retain the included Apache-2.0 text. |
| `aws-smithy-json 0.62.4 -> 0.62.7` | `Apache-2.0` | `701a947f4797e52a911e114a898667c746c39feea467bbd1abd7b3721f702ffa` | Root `LICENSE`, SHA-256 `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b`. Expression and license text are unchanged; retain the included Apache-2.0 text. |
| `aws-smithy-runtime-api 1.11.5 -> 1.12.3` | `Apache-2.0` | `9db177daa6ba8afb9ee1aefcf548c907abcf52065e394ee11a92780057fe0e8c` | Root `LICENSE`, SHA-256 `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b`. Expression and license text are unchanged; retain the included Apache-2.0 text. |
| `aws-smithy-runtime-api-macros (new) 1.0.0` | `Apache-2.0` | `8d7396fd9500589e62e460e987ecb671bad374934e55ec3b5f498cc7a8a8a7b7` | Root `LICENSE`, SHA-256 `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b`. Preserve the included Apache-2.0 text. |
| `aws-smithy-schema (new) 0.1.0` | `Apache-2.0` | `7442cb268338f0eb8278140a107c046756aa01093d8ef5e99628d34ae09c94f5` | Root `LICENSE`, SHA-256 `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b`. Preserve the included Apache-2.0 text. |
| `aws-smithy-types 1.4.5 -> 1.4.9` | `Apache-2.0` | `53f93074121a1be41317b9aa607143ae17900631f7f59a99f2b905d519d6783b` | Root `LICENSE`, SHA-256 `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b`. Expression and license text are unchanged; retain the included Apache-2.0 text. |
| `rand 0.8.5 -> 0.8.6` | `MIT OR Apache-2.0` | `5ca0ecfa931c29007047d1bc58e623ab12e5590e8c7cc53200d5202b69266d8a` | `COPYRIGHT` SHA-256 `90eb64f0279b0d9432accfa6023ff803bc4965212383697eee27a0f426d5f8d5`; `LICENSE-APACHE` `35242e7a83f69875e6edeff02291e688c97caafe2f8902e4e19b49d3e78b4cab`; `LICENSE-MIT` `209fbbe0ad52d9235e37badf9cadfe4dbdc87203179c0899e738b39ade42177b`. All match the prior package evidence; retain the contributor attribution and both packaged license texts. |
| `rand 0.9.2 -> 0.9.3` | `MIT OR Apache-2.0` | `7ec095654a25171c2124e9e3393a930bddbffdc939556c914957a4c3e0a87166` | `COPYRIGHT` SHA-256 `90eb64f0279b0d9432accfa6023ff803bc4965212383697eee27a0f426d5f8d5`; `LICENSE-APACHE` `35242e7a83f69875e6edeff02291e688c97caafe2f8902e4e19b49d3e78b4cab`; `LICENSE-MIT` `209fbbe0ad52d9235e37badf9cadfe4dbdc87203179c0899e738b39ade42177b`. All match the prior package evidence; retain the contributor attribution and both packaged license texts. |

No changed package introduces a new license expression or evidence gap. The 649
unchanged third-party identities retain their prior checksums, expressions,
manifests, and license evidence. No
supplemental license text is needed for this delta. Strict release notice
generation passed with 0 unresolved third-party evidence. Its output retained
the full AWS-LC and Fiat texts and CCADB attribution. The focused inventory
self-test passed, and the notice-generator self-test passed from a clean staged
copy of `docs/legal`; the checkout's retained release-source archives under
`target/` make the unmodified test fixture scan unrelated `LICENSES` folders
and fail before its intended checksum assertions. `cargo deny --locked check
licenses` was not run because `cargo deny` is not installed; no global install
was performed.

This archive review does not establish which dependencies or native code are
linked into release binaries.
