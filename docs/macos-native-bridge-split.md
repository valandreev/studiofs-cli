# macOS Native Bridge Split

Date: 2026-06-02

Tracking issue: [SFS-475](https://linear.app/valandreev/issue/SFS-475/define-macos-native-bridge-split-from-main)

Historical note: this SFS-475 split predates SFS-600. References below to
`studiofs-fuse` describe the removed legacy Linux raw FUSE path and are kept as
the original macOS split rationale, not as current Linux mount guidance.

## Decision

StudioFS should split the macOS mount implementation out of the current
cross-platform FUSE crate. The implementation starts from `main`, not from an
old experimental branch.

The acceptance boundary is that studiofs-fuse becomes Linux-only; it remains
the owner for Linux FUSE3 callback translation, Linux mount options, Linux
kernel cache invalidation, and Linux opt-in benchmark controls such as async
write replies and `STUDIOFS_FUSE_ASYNC_DIO=1`.

The new macOS bridge crate is `studiofs-macfuse`. Its public mount entry point
is:

```rust
pub async fn mount_and_serve(request: MacFuseMountRequest) -> Result<(), MacFuseMountError>
```

The request type should carry the same shared inputs as the existing bridge
adapters: `StudioVfs`, runtime handle or runtime integration, mount point,
volume label, read-only policy, foreground behavior, and graceful shutdown
controls. The bridge should translate macFUSE-native callbacks into `StudioVfs`
operations; shared filesystem semantics stay in VFS and metadata crates.
`MacFuseMountError` should be a `thiserror`-derived public error enum so
configuration, mount lifecycle, and macOS-specific bridge failures keep their
domain context before the CLI backend router converts them into user-facing
diagnostics.

## SFS-474 Evidence

[SFS-474](https://linear.app/valandreev/issue/SFS-474/probe-fuser-notifierstore-for-macos-repeated-read-cache-behavior)
tested whether the current `studiofs-fuse` backend could use
`fuser::Notifier::store` to populate macOS hot file pages after stable local
bytes were available.

The probe result was negative: `fuser::Notifier::store` failed with `EINVAL`
and did not produce macOS kernel-cache hits. Repeated reads still re-entered the
FUSE read path instead of being served from the macOS kernel cache. That makes
the notifier-store path unsuitable as the main repeated-read cache fix on
macOS.

## Boundary

Responsibilities that stay in `studiofs-fuse`:

- Linux FUSE3 session creation and callback translation.
- Linux-only mount options and capability negotiation.
- Linux cache policy, including `FOPEN_DIRECT_IO` behavior and explicit kernel
  invalidation.
- Linux async write and async DIO experiment controls.
- Linux mounted smoke tests.

Responsibilities that move into `studiofs-macfuse`:

- macOS/macFUSE session creation and callback translation.
- macOS UBC/vnode cache policy, including native repeated-read cache goals.
- Finder metadata behavior, AppleDouble avoidance, and root metadata filtering
  that must be enforced at the macOS bridge boundary.
- Native xattr and `com.apple.ResourceFork` behavior.
- Unicode normalization handling for Finder/macFUSE lookups and directory
  listings.
- macOS mount options such as `local`, `volname`, `iosize`, and future cache
  controls.
- macOS mounted smoke tests.

Responsibilities that remain shared:

- Namespace, inode, file-handle, xattr, and payload semantics in `StudioVfs`.
- Metadata storage, object upload, local payload cache, and durability rules.
- CLI configuration parsing until backend routing is split.
- Cross-platform validation contracts in docs and focused architecture tests.

## CLI/Backend Routing

The current CLI has a `MountBackend` trait and uses `default_mount_backend()` to
pick the platform bridge. The split should preserve that shape while changing
the Unix routing into explicit per-OS backends:

- Linux: route to `studiofs_fuse`.
- macOS: route to `studiofs_macfuse`.
- Windows: keep routing to `studiofs_winfsp`.

The first routing change should be compile-time gated with `cfg(target_os =
"linux")`, `cfg(target_os = "macos")`, and `cfg(windows)`. Runtime mount
arguments should keep their current public CLI shape until the macOS bridge has
its own validated options.

The initial CLI routing keeps the macOS native bridge behind
`STUDIOFS_MOUNT_BACKEND=macfuse`. Without that selector, macOS uses the explicit
unsupported backend so the incomplete native bridge does not become the
unconditional default.

## Fallback Behavior

Before `studiofs-macfuse` is production-ready, macOS should not silently fall
back to the Linux FUSE backend. A missing or disabled native bridge should fail
with an explicit diagnostic that points to the macOS bridge issue chain and
states that the old fuser notifier-store path was rejected by SFS-474.

Temporary developer escape hatches, if needed, must be opt-in, named as
experimental, and excluded from the default mount path. They should not change
Linux or Windows behavior.

## Validation Sequence

1. Keep this SFS-475 design note and focused docs test green.
2. Add the `studiofs-macfuse` crate shell with no default routing change.
3. Add the public `MacFuseMountRequest` and `pub async fn mount_and_serve`
   entry point with unit-level request validation.
4. Port the minimal mount lifecycle and callback adapter path behind macOS-only
   compilation.
5. Add fast non-mounted tests for macOS-specific name, xattr, and resource-fork
   behavior at the bridge boundary.
6. Add opt-in mounted smoke tests for mount, write, read, readdir, xattr, and
   unmount.
7. Switch CLI/backend routing on macOS only after the native bridge passes the
   focused validation gate.

## First Implementation Issues

1. Create `studiofs-macfuse` crate shell and workspace wiring.
2. Define `MacFuseMountRequest` and `pub async fn mount_and_serve`.
3. Implement minimal macFUSE mount lifecycle without changing default routing.
4. Port read-only callback translation from the shared VFS surface.
5. Add native xattr, resource-fork, Finder metadata, and Unicode boundary
   tests.
6. Add mounted macOS smoke tests.
7. Switch macOS CLI/backend routing to `studiofs-macfuse` after validation.

## Non-goals

- Do not implement the bridge in SFS-475.
- Do not change default mount behavior in SFS-475.
- Do not reuse an old experimental branch as the base.
- Do not make `fuser::Notifier::store` a macOS production cache strategy.
