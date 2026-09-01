# SFS-669: Experimental macFUSE FSKit backend path

## Context

SFS-668 fixed the macFUSE FSKit contract and selected the legacy `macfuse`
path as the compatibility baseline. SFS-669 adds an explicit experimental
selector for the native `MFMount.framework` path while reusing the existing
`StudioVfsAdapter`; it does not add a second filesystem core or the SFS-671
zero-copy data path.

## Plan

1. Add explicit macOS selectors: `legacy`/`macfuse` for the current path and
   `fskit` for the experimental path. Keep the default legacy path.
2. Add an isolated FSKit transport around `MFChannelCreate` and `MFMount`.
   Validate and copy message bodies at the transport boundary, retain each
   message until its reply is sent, and release channel/message ownership only
   after workers have stopped.
3. Route the basic mounted protocol through the existing adapter: init,
   lookup, getattr, open, read, release, directory enumeration, and shutdown.
   Preserve read-only enforcement in the adapter and register/clear the
   explicit FSKit kernel-invalidator policy during the same lifecycle window.
4. Add focused parser, lifecycle, cancellation, selector, and gated real
   macOS mounted smoke coverage. Document the selector and the supported
   experimental limitations.

## Invariants and decisions

- The legacy backend remains the default and keeps all legacy libfuse calls.
- The FSKit path never calls `fuse_mount`, `fuse_session_loop`,
  `fuse_session_add_chan`, or `fuse_reply_*`.
- A received message is either replied exactly once or explicitly consumed as
  a no-reply operation; malformed requests receive an error when a unique id
  is available.
- A signal or explicit unmount interrupts the blocked channel receive, then
  joins request workers before channel and callback state teardown.
- FSKit notifications are not available in this slice, so the transport uses
  `novncache` plus direct I/O and keeps invalidator registration explicit. The
  optimization and notification work remains outside this ticket.
- The mounted smoke is macOS- and installed-macFUSE-dependent; unavailable
  Windows validation is reported rather than inferred.

## Progress

- [x] Confirm SFS-668 contract, current selector, callback adapter, and local
  macFUSE installation.
- [x] Add selector and backend routing.
- [x] Add FSKit transport and basic callback routing.
- [x] Add focused lifecycle and mounted smoke coverage.
- [x] Update docs and record verification in SFS-669.

## Validation

- `cargo test -p studiofs ...` for selector and lifecycle tests.
- `cargo test -p studiofs-macfuse ...` for transport and callback tests.
- `cargo fmt --all -- --check` and `git diff --check`.
- On macOS with macFUSE installed, run
  `STUDIOFS_RUN_FSKIT_MOUNT_TESTS=1 cargo test -p studiofs-macfuse fskit_mounted_lookup_open_read_unmount_smoke_is_gated -- --nocapture`
  for the mounted lookup/open/read/unmount smoke. Keep it separate from the
  general platform bridge gate because FSKit activation depends on the host's
  installed and approved macFUSE extension.
