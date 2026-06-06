# SFS-487 fuse3 macOS Candidate Proof Gate

SFS-487 validates `Sherlock-Holo/fuse3` as a possible macOS bridge candidate for
StudioFS. The current decision: reference only.

`fuse3` is no longer a simple Linux-only non-starter. `Sherlock-Holo/fuse3`
merged macOS support in https://github.com/Sherlock-Holo/fuse3/pull/108 and a
macOS documentation follow-up in https://github.com/Sherlock-Holo/fuse3/pull/109.
The current upstream `mount_options.rs` has a macOS-specific `build()` path and
supports `custom_options(...)`, so macFUSE option pass-through is plausible.

That does not make it a viable migration candidate yet. StudioFS already has a
custom low-level macFUSE bridge with explicit callback ownership, xattr/setattr
coverage, kernel invalidation, cache policy, and large read behavior. A library
that can mount on macOS still has to match those contracts before it can replace
`studiofs-macfuse`.

## Decision Outcomes

SFS-487 must use one of these outcomes:

- viable migration candidate: the proof mounts successfully and shows no
  contract blocker against the current macOS bridge surfaces.
- reference only: the upstream support is useful for design comparison, but no
  StudioFS routing or dependency change is justified yet.
- rejected for StudioFS macOS: the proof finds an incompatibility that blocks
  the candidate for the current bridge contract.

The current decision is `reference only` because no mounted StudioFS-compatible
proof has run yet.

## Current StudioFS Anchors

- `crates/studiofs/src/cli/mount/backend.rs` routes macOS to the native
  `MacFuseMountBackend`.
- `crates/studiofs-macfuse/src/lib.rs` owns `mount_with_request(...)`, session
  lifecycle, signal handling, callback-state lifetime, and kernel invalidator
  registration.
- `crates/studiofs-macfuse/src/low_level.rs` owns low-level reply ownership,
  connection sizing, callback tables, and macFUSE request/reply wrappers.
- `crates/studiofs-macfuse/src/adapter.rs` owns StudioVfs callback translation,
  including `read`, `write`, `open`, `setattr`, `getxattr`, `setxattr`,
  `listxattr`, and `removexattr`.
- `crates/studiofs-fuse/Cargo.toml` keeps the existing `fuser` dependency
  Linux-only; `fuse3` is not a production dependency.

## Proof Scope

Build any temporary candidate under `target/sfs-487-fuse3-proof`. Do not add it
to the workspace, and do not reuse it as production code.

The proof must check:

- a minimal read-only macOS mount with the current `Sherlock-Holo/fuse3`;
- pass-through of macOS mount options through `custom_options(...)`;
- large read callback behavior and max-read/iosize compatibility;
- low-level reply ownership parity or a clear gap;
- xattr behavior, including Finder/resource-fork paths;
- setattr behavior, including macOS metadata bits and size changes;
- kernel invalidation or the lack of an equivalent to the current
  `studiofs-macfuse` invalidator path;
- lifecycle behavior for foreground runs, signal handling, and unmount cleanup.

No production dependency or routing change is allowed by this proof. A future
migration issue may change that only after mounted evidence exists.

## Operator Commands

Focused validation commands:

```bash
cargo test -p studiofs --test architecture_vocabulary_test fuse3_macos_candidate_report_covers_sfs487_proof_gate -- --nocapture
cargo test -p studiofs-macfuse -- --nocapture
```

Operator-only mounted proof commands should live under `target/sfs-487-fuse3-proof`
and record their output next to the prototype. The required comparison target is
the current `studiofs-macfuse` mounted smoke/read path, not only whether the
temporary `fuse3` binary compiles.

## Notes From External Verification

As of 2026-06-06, the upstream repository describes `fuse3` as an async Rust
FUSE library, with latest release `0.9.0` published on 2026-03-12. Context7 did
not have `Sherlock-Holo/fuse3` documentation, so the SFS-487 check used the
upstream GitHub repository, the two linked PRs, and raw `mount_options.rs` as
the source of truth.
