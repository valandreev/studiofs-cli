# Removing `vendor/fuser`

## Recommendation

Do not remove `vendor/fuser` yet.

Right now the vendored crate contains at least one macOS fix that is required for
correct behavior:

- `vendor/fuser/build.rs`
  - removes automatic `macfuse-4-compat` enablement when probing macFUSE 4.x
  - this was the root cause of broken directory rename parsing on macOS

Without that patch, the old bug is likely to return.

## What Is Actually Needed Today

The changes in `vendor/fuser` are not equally important.

Required to keep macOS working:

- `vendor/fuser/build.rs`
  - keep the `macfuse-4-compat` line removed

Probably useful, but not proven as the root fix:

- `vendor/fuser/src/lib.rs`
  - reduced advertised macOS init flags to the subset we actually implement

Debugging / cleanup only:

- `vendor/fuser/src/request.rs`
  - macOS dispatch debug logging
- `vendor/fuser/src/ll/fuse_abi.rs`
  - `#![allow(dead_code)]` to silence vendored warnings

## Safe Cleanup Option

If we want to shrink the vendored diff without removing the vendored crate:

1. Revert `vendor/fuser/src/request.rs` to upstream.
2. Revert `vendor/fuser/src/ll/fuse_abi.rs` to upstream.
3. Keep `vendor/fuser/build.rs`.
4. Decide separately whether to keep or revert `vendor/fuser/src/lib.rs`.

This keeps the critical macOS fix while removing temporary diagnostics.

## What Must Happen Before Removing `vendor/fuser`

One of these needs to be true first:

1. Upstream `fuser` ships a release with the macOS `macfuse-4-compat` fix.
2. We switch from a vendored copy to a maintained fork via git dependency.
3. We carry the fix in another reproducible patching mechanism during build.

Only after that can we safely remove the vendored crate from the repo.

## Steps To Remove It

When the fix exists outside the repo:

1. Update the root dependency override in `Cargo.toml`.
   - remove:
     - `[patch.crates-io]`
     - `fuser = { path = "vendor/fuser" }`
2. Point `fuser` to the upstream release or a maintained fork.
3. Delete `vendor/fuser`.
4. Rebuild and rerun macOS regressions.

## Required Regression Checks

Before deleting the vendored crate, verify all of these on macOS:

1. `cargo build --release`
2. create / rename / delete directory in Finder
3. copy directory in Finder
4. cancel Finder copy
5. `cp -a <src> <mount>/`
6. copy into an existing directory
7. read and write regular files
8. unmount / remount sanity check

Also verify Windows still behaves the same:

1. build WinFSP target
2. create / rename / delete directory
3. copy directory and regular files

## Bottom Line

`vendor/fuser` is still justified.

The right next cleanup is not "delete it now", but either:

1. keep it as-is until the fix is upstreamed, or
2. reduce it to the smallest necessary diff and keep only the required macOS patch.
