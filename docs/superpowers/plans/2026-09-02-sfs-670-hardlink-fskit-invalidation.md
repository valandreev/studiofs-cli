# SFS-670 Hardlink and FSKit invalidation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

## Goal

Deliver the first production slice of SFS-670: expose the existing shared-VFS
hardlink semantics through both macFUSE bridges and replace the FSKit
backend's no-op kernel invalidator with serialized FUSE notification frames.
This establishes the missing bridge contract before the remaining mounted
parity sweep.

## Architecture

- `StudioVfs` remains the single source of filesystem semantics.
- `StudioVfsAdapter` remains the shared bridge adapter; the legacy callback and
  FSKit dispatcher only translate their native request shapes into it.
- The legacy macFUSE low-level operation table gets `.link` only in its
  writable table. The read-only table stays unchanged.
- FSKit remains opt-in through `STUDIOFS_MOUNT_BACKEND=fskit`; the legacy
  bridge remains the default.
- FSKit invalidations use the existing `FsKitControl::send` channel and FUSE
  notification wire layouts. No new registry, dependency, async raw-pointer
  lifetime, or duplicate VFS logic is introduced.

## Tech Stack

Rust 2024, `cargo`, macFUSE low-level C ABI, MFMount FSKit channel, existing
`StudioVfs`, `StudioVfsAdapter`, and the repository's contract fixtures.

## Source of truth

Design: `docs/superpowers/specs/2026-09-02-sfs-670-macfuse-fskit-semantic-parity-design.md`

## Global Constraints

- Use test-driven development: add one focused failing test before each
  production behavior change, then run that test green.
- Do not touch Windows bridge code or claim Windows validation from this Mac.
- Do not change the default mount backend or remove the legacy bridge.
- Reuse the existing VFS and adapter contracts; do not weaken durability or
  add a second semantic implementation.
- Do not implement SFS-671 zero-copy work in this change.
- Treat names as checked byte slices at the request boundary and preserve
  existing errno normalization.
- Keep notification buffers alive through the synchronous
  `MFChannelSendMessage` call, serialize sends through the existing control
  lock, and tolerate late sends after control close.
- Keep commits and repository-facing text in English.

## File map

- `crates/studiofs-macfuse/src/adapter.rs`: shared adapter hardlink method and
  legacy C callback.
- `crates/studiofs-macfuse/src/low_level.rs`: test-only ABI capability helper.
- `crates/studiofs-macfuse/csrc/low_level_ops.c`: writable `.link` callback
  declaration/table entry and capability helper.
- `crates/studiofs-macfuse/csrc/fuse_lowlevel_compat.h`: fallback operation
  layout field.
- `crates/studiofs-macfuse/src/fskit.rs`: link request decoding/dispatch and
  concrete invalidation frame encoding/sending.
- `crates/studiofs-macfuse/src/lib.rs`: adapter, ABI, protocol, and gated
  mounted-surface tests.
- `docs/platform-bridges.md`: document the implemented FSKit notification
  transport and the validation boundary.

## Implementation tasks

### Task 1: Expose shared hardlink semantics through `StudioVfsAdapter`

Files: `crates/studiofs-macfuse/src/adapter.rs`,
`crates/studiofs-macfuse/src/lib.rs`.

Test first in the existing contract-fixture test module:

    #[test]
    fn adapter_link_preserves_inode_and_increments_link_count() {
        let fixture = ContractVfsFixture::new("macfuse-adapter-link")
            .expect("fixture should initialize");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let source = vfs
            .create(root, b"link-source.txt", 501, 20, 0o644)
            .expect("source should be created");
        let before = vfs.getattr(source.ino).expect("source should exist");
        let adapter = StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let linked = adapter
            .link(source.ino, root, b"link-alias.txt")
            .expect("hardlink should succeed");

        assert_eq!(linked.ino, source.ino);
        assert_eq!(linked.nlink, before.nlink + 1);
        assert_eq!(
            vfs.lookup(root, b"link-alias.txt")
                .expect("alias should exist")
                .ino,
            source.ino
        );

        let read_only = StudioVfsAdapter::new_with_read_only(
            vfs,
            fixture.runtime().handle().clone(),
            true,
        );
        assert_eq!(
            read_only.link(source.ino, root, b"read-only-alias.txt"),
            Err(libc::EROFS)
        );
    }

Run:

    cargo test -p studiofs-macfuse adapter_link_preserves_inode_and_increments_link_count -- --nocapture

Expected RED state: the test does not compile because the adapter has no
`link` method. Implement only the adapter method, routing to
`StudioVfs::link`, observing the returned inode for existing kernel liveness,
and mapping errors through the existing adapter mapper. Re-run the same
filter; it must pass.

### Task 2: Wire the legacy macFUSE link callback

Files: `crates/studiofs-macfuse/src/adapter.rs`,
`crates/studiofs-macfuse/src/low_level.rs`,
`crates/studiofs-macfuse/csrc/low_level_ops.c`,
`crates/studiofs-macfuse/csrc/fuse_lowlevel_compat.h`,
`crates/studiofs-macfuse/src/lib.rs`.

Add the ABI regression test before adding the helper:

    #[test]
    fn writable_studio_vfs_low_level_ops_install_link_callback() {
        assert!(LowLevelOperations::writable().has_link_callback());
        assert!(!LowLevelOperations::read_only().has_link_callback());
    }

Run:

    cargo test -p studiofs-macfuse writable_studio_vfs_low_level_ops_install_link_callback -- --nocapture

Expected RED state: the capability method or C symbol is missing. Add the
test-only Rust declaration/method, the C capability helper, and `.link` only
to `STUDIOFS_LOW_LEVEL_OPS`. Add the fallback header field in the same
position as the real low-level operation layout. Implement
`studiofs_macfuse_link` beside the existing rename/unlink callbacks: reject a
null name, convert the checked C string, call `state.adapter.link(ino,
newparent, name)`, and reply with the existing `Entry` wrapper or errno.
Re-run the ABI test and Task 1's adapter filter.

### Task 3: Dispatch `FUSE_LINK` through the FSKit backend

File: `crates/studiofs-macfuse/src/fskit.rs` and its existing test module.

Add a protocol-layout test before implementation:

    #[test]
    fn fskit_link_request_uses_the_fuse_wire_layout() {
        assert_eq!(FUSE_LINK, 13);
        assert_eq!(size_of::<FuseLinkIn>(), size_of::<u64>());

        let mut bytes = vec![0u8; size_of::<FuseInHeader>()];
        bytes.extend_from_slice(&77u64.to_ne_bytes());
        bytes.extend_from_slice(b"alias\0");
        let len = u32::try_from(bytes.len()).expect("test message fits");
        bytes[..size_of::<u32>()].copy_from_slice(&len.to_ne_bytes());

        let body = parse_message_body(&bytes).expect("link message should parse");
        let input = read_copy::<FuseLinkIn>(
            &body.bytes,
            size_of::<FuseInHeader>(),
        )
        .expect("link input should decode");

        assert_eq!(input.oldnodeid, 77);
        assert_eq!(
            parse_c_string(&body.bytes[size_of::<FuseInHeader>() + size_of::<FuseLinkIn>()..])
                .expect("name should decode"),
            b"alias"
        );
    }

Run:

    cargo test -p studiofs-macfuse fskit_link_request_uses_the_fuse_wire_layout -- --nocapture

Expected RED state: `FUSE_LINK` and `FuseLinkIn` are not defined. Add the
constant and `#[repr(C)]` one-field input with `oldnodeid: u64`, then add a
dispatcher branch. Decode the old inode from the body, use the request header
node ID as the new parent, validate the trailing NUL-terminated name with the
existing checked parser, call `StudioVfsAdapter::link`, and reply with one
existing `FuseEntryOut`. Malformed input returns the existing `EINVAL` reply
path. Re-run the protocol test and the smallest relevant FSKit dispatcher
tests.

### Task 4: Replace the FSKit no-op invalidator with FUSE notification frames

File: `crates/studiofs-macfuse/src/fskit.rs` and its existing test module.

Add a pure frame-layout test before changing the invalidator:

    #[test]
    fn fskit_invalidation_frames_match_fuse_wire_layout() {
        let inode = invalidation_inode_bytes(42, -1, 0);
        let header = read_copy::<FuseOutHeader>(&inode, 0)
            .expect("inode invalidation header should decode");
        assert_eq!(header.len as usize, inode.len());
        assert_eq!(header.error, FUSE_NOTIFY_INVAL_INODE);
        assert_eq!(header.unique, 0);
        let payload = read_copy::<FuseNotifyInvalInodeOut>(
            &inode,
            size_of::<FuseOutHeader>(),
        )
        .expect("inode invalidation payload should decode");
        assert_eq!((payload.ino, payload.off, payload.len), (42, -1, 0));

        let entry = invalidation_entry_bytes(7, b"alias").expect("entry fits");
        let entry_header = read_copy::<FuseOutHeader>(&entry, 0)
            .expect("entry invalidation header should decode");
        assert_eq!(entry_header.len as usize, entry.len());
        assert_eq!(entry_header.error, FUSE_NOTIFY_INVAL_ENTRY);
        let entry_payload = read_copy::<FuseNotifyInvalEntryOut>(
            &entry,
            size_of::<FuseOutHeader>(),
        )
        .expect("entry invalidation payload should decode");
        assert_eq!((entry_payload.parent, entry_payload.namelen), (7, 5));
        assert_eq!(
            &entry[size_of::<FuseOutHeader>() + size_of::<FuseNotifyInvalEntryOut>()..],
            b"alias"
        );
    }

Run:

    cargo test -p studiofs-macfuse fskit_invalidation_frames_match_fuse_wire_layout -- --nocapture

Expected RED state: the concrete frame builders and payload types are absent.
Define the existing FUSE notification header reuse plus the two exact payload
layouts. Encode inode invalidations with `off = -1, len = 0` for attributes
and `off = 0, len = 0` for data; encode entry invalidations with the raw name
bytes after the fixed payload. Use checked `u32` length conversion. Make
`FsKitKernelInvalidator` hold `Arc<FsKitControl>`, serialize each complete
frame through `FsKitControl::send`, and log a closed-channel error without
panicking. Install it from `mount_with_request` before `session.run`; clear
the VFS invalidator after the session, then retain the existing reply-task
wait and control close ordering.

Re-run the frame-layout test and the existing control-lock/cancellation tests.

### Task 5: Prove the bridge seam and document the validation boundary

Files: `crates/studiofs-macfuse/src/lib.rs`,
`docs/platform-bridges.md`.

Extend the existing opt-in FSKit mounted smoke coverage with one gated
writable scenario that creates a file, creates a hardlink, verifies both
names resolve to the same inode, and exercises the invalidation path after a
local mutation. Keep the existing environment gate and unmount cleanup. Run
the focused mounted test with the required explicit environment; if this Mac
cannot provide a real FSKit mount, record it as unavailable rather than
turning the gate into a pass.

Run the focused suite:

    cargo test -p studiofs-macfuse adapter_link_preserves_inode_and_increments_link_count -- --nocapture
    cargo test -p studiofs-macfuse writable_studio_vfs_low_level_ops_install_link_callback -- --nocapture
    cargo test -p studiofs-macfuse fskit_link_request_uses_the_fuse_wire_layout -- --nocapture
    cargo test -p studiofs-macfuse fskit_invalidation_frames_match_fuse_wire_layout -- --nocapture
    cargo test -p studiofs-macfuse fskit_mounted_hardlink_and_invalidation_smoke_is_gated -- --nocapture

Update `docs/platform-bridges.md` to describe the implemented frame transport,
the unchanged legacy default, and the exact distinction between unit/protocol
proof and a native mounted FSKit run. Do not state that Windows was validated.

### Task 6: Final verification and handoff

- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `git diff --check`.
- [ ] Run `cargo test -p studiofs-macfuse` once all focused tests are green.
- [ ] Rebuild the exact release binary only if a mounted scenario is run with
      `--studiofs-bin`; otherwise do not claim mounted release validation.
- [ ] Inspect `git diff`, confirm no Windows bridge files changed, and update
      the SFS-670 Linear issue in English with results and unavailable native
      validation boundaries.

## Plan self-review

- The tasks cover the approved first milestone: shared hardlink semantics,
  both bridge entry points, FSKit dispatch, concrete invalidation, lifecycle
  ordering, and a mounted proof boundary.
- Every production slice has a named RED test, an exact focused command, and a
  minimal implementation location.
- The plan intentionally leaves the remaining full parity matrix—xattrs,
  Finder metadata/provenance, open-unlink, remote-change mounted scenarios,
  and broader read-only/shutdown sweeps—for the next evidence-driven slice;
  it does not weaken those requirements.
- No new dependency, platform-wide abstraction, Windows edit, or SFS-671
  zero-copy work is planned.
