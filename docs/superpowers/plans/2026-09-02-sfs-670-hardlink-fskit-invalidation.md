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
- `crates/studiofs-macfuse/src/lib.rs`: public mount-request API and
  adapter/ABI/protocol tests.
- `crates/studiofs-macfuse/tests/fskit_mount_sanity.rs`: ignored native FSKit
  mount, cancellation, hardlink, and bounded-cleanup tests.
- `docs/platform-bridges.md`: document the implemented FSKit notification
  transport and the validation boundary.

## Implementation tasks

### Task 1: Expose shared hardlink semantics through `StudioVfsAdapter`

- [x] Add the contract-fixture RED test.
- [x] Route the adapter method to `StudioVfs::link` with read-only and liveness
      behavior.
- [x] Run the focused adapter test green.

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

- [x] Add the writable/read-only ABI capability test.
- [x] Add the C table/helper, fallback layout field, Rust helper, and callback.
- [x] Run the focused ABI test green.

Files: `crates/studiofs-macfuse/src/adapter.rs`,
`crates/studiofs-macfuse/src/low_level.rs`,
`crates/studiofs-macfuse/csrc/low_level_ops.c`,
`crates/studiofs-macfuse/csrc/fuse_lowlevel_compat.h`,
`crates/studiofs-macfuse/src/lib.rs`.

Add the ABI regression test before adding the helper:

    #[test]
    fn writable_studio_vfs_low_level_ops_install_link_callback() {
        assert!(LowLevelOperations::studio_vfs().has_link_callback());
        assert!(!LowLevelOperations::read_only_studio_vfs().has_link_callback());
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

- [x] Add the wire-layout RED test.
- [x] Add `FUSE_LINK`, `FuseLinkIn`, dispatch, checked name parsing, and entry
      reply.
- [x] Run the focused protocol test green.

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

- [x] Add the frame-layout RED test and closed-control lifecycle test.
- [x] Add a live-channel delivery regression for inode, attribute, and entry
      invalidation frames.
- [x] Encode inode/entry notifications and send complete frames through the
      retained, serialized FSKit control.
- [x] Install the invalidator and preserve teardown ordering.

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

        let entry = invalidation_entry_bytes(7, b"\xC3\xA9").expect("entry fits");
        let entry_header = read_copy::<FuseOutHeader>(&entry, 0)
            .expect("entry invalidation header should decode");
        assert_eq!(entry_header.len as usize, entry.len());
        assert_eq!(entry_header.error, FUSE_NOTIFY_INVAL_ENTRY);
        let entry_payload = read_copy::<FuseNotifyInvalEntryOut>(
            &entry,
            size_of::<FuseOutHeader>(),
        )
        .expect("entry invalidation payload should decode");
        assert_eq!((entry_payload.parent, entry_payload.namelen), (7, 2));
        assert_eq!(
            &entry[size_of::<FuseOutHeader>() + size_of::<FuseNotifyInvalEntryOut>()..],
            b"\xC3\xA9"
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

- [x] Add the ignored writable hardlink/unlink mounted scenario.
- [x] Complete a real native FSKit mounted run on an internal volume with a
      responsive, approved provider.
- [x] Update the bridge documentation with the implementation and validation
      boundary.

Files: `crates/studiofs-macfuse/src/lib.rs`,
`crates/studiofs-macfuse/tests/fskit_mount_sanity.rs`,
`docs/platform-bridges.md`.

Keep the native FSKit mounted scenarios in a dedicated ignored integration
test. The writable scenario creates a file, creates a hardlink, verifies both
names resolve to the same inode, removes one name, checks `ENOENT` for the
removed path, and keeps the remaining hardlink readable. The test uses the
public mount-request builder, a PID-scoped mountpoint, and bounded cleanup;
there is no environment-only test gate.

Run the focused suite:

    cargo test -p studiofs-macfuse adapter_link_preserves_inode_and_increments_link_count -- --nocapture
    cargo test -p studiofs-macfuse writable_studio_vfs_low_level_ops_install_link_callback -- --nocapture
    cargo test -p studiofs-macfuse fskit_link_request_uses_the_fuse_wire_layout -- --nocapture
    cargo test -p studiofs-macfuse fskit_invalidation_frames_match_fuse_wire_layout -- --nocapture
    STUDIOFS_FSKIT_MOUNT_ROOT=/Volumes/studiofs/target/sfs-670-validation cargo test -p studiofs-macfuse --test fskit_mount_sanity fskit_ -- --ignored --nocapture --test-threads=1

Update `docs/platform-bridges.md` to describe the implemented frame transport,
the unchanged legacy default, and the exact distinction between unit/protocol
proof and a native mounted FSKit run. Do not state that Windows was validated.

Historical native validation result (2026-09-03, before the internal-volume
rerun): the explicit mounted command reached the MFMount helper, but the
helper stayed blocked before readiness for more than 60 seconds. The test was
stopped to avoid leaving a live mount attempt; the target mount directory and
child processes were clean afterward. The review-blocker cleanup below records
the later successful internal-volume run.

### Task 6: First-milestone verification and handoff

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `git diff --check`.
- [x] Run `cargo test -p studiofs-macfuse` once all focused tests are green.
- [ ] Rebuild the exact release binary only if a mounted scenario is run with
      `--studiofs-bin`; otherwise do not claim mounted release validation.
- [x] Inspect `git diff`, confirm no Windows bridge files changed, and update
      the SFS-670 Linear issue in English with results and unavailable native
      validation boundaries.

The strict `RUSTFLAGS="-D warnings" cargo test -p studiofs-macfuse` attempt was
stopped after `rustc` remained inactive for more than three minutes during a
fresh dependency rebuild. Strict `cargo clippy --all-targets --all-features
--locked -- -D warnings` passed; the ordinary full test suite and formatting/
diff checks passed.

### Follow-up parity slice (2026-09-03)

- [x] Align FSKit `getxattr` with the legacy adapter helpers, preserving size
      queries, positioned remaining-value reads, past-end reads, macOS
      `ENOATTR`, and `ERANGE` handling.
- [x] Route FSKit `rmdir` and `symlink` through the existing tracked blocking
      reply-task lifecycle so the dispatcher loop is not held by VFS waits.
- [x] Add focused RED/green coverage for the xattr response contract and the
      blocking-task dispatch seam.
- [x] Refresh the FSKit direct-I/O/unsupported-operation comments and platform
      bridge documentation; the legacy backend and Windows bridge remain
      untouched.
- [ ] Run a native mounted FSKit proof for this slice when an approved,
      responsive provider is available; the current Mac still cannot provide
      that evidence.

Focused validation:

    cargo test -p studiofs-macfuse fskit_getxattr_preserves_legacy_position_contract -- --nocapture
    cargo test -p studiofs-macfuse fskit_missing_xattr_uses_macos_attribute_errno -- --nocapture
    cargo test -p studiofs-macfuse fskit_blocking_namespace_work_runs_off_dispatch_thread -- --nocapture

### Follow-up FSKit startup and cancellation repair (2026-09-04)

The earlier mount-result gate was based on the MFMount documentation's
sequential example. A minimal C probe against the installed macFUSE 5.3.3
on macOS 26.6.2 disproved that ordering for local mounts: it received
`FUSE_INIT` (opcode 26) before `MFMount()` returned. Returning a diagnostic
`ENOSYS` reply made `MFMount()` return an error in 1.582 seconds; leaving the
request unread kept the call blocked despite a successful XPC connection.
The Rust gate therefore introduced a circular wait and has been removed.

The earlier send-port error was not sufficient evidence of a broken host:
the three-second smoke deadline requested cancellation before the cold
framework/helper startup finished. `request_exit` closed the endpoint while
`MFMount()` was still using it. Cancellation now records the stop request,
continues startup dispatch, and performs a normal unmount when mount completes.
Sequentially consistent flag operations prevent completion and cancellation
from both missing each other. A failed mount still closes the channel to
wake the receiver and propagate the mount error.

- [x] Replace the incorrect ordering test with a socket-backed MFChannel test
      that sends a real INIT frame through `FsKitSession::run` while the mount
      result is pending, checks the reply, and checks mount-error propagation.
- [x] Observe RED: `INIT was blocked by pending mount`.
- [x] Add cancellation coverage and observe RED:
      `cancellation destroyed the pending mount endpoint`.
- [x] Restore concurrent startup dispatch and defer early cancellation cleanup.
- [x] Run library regressions, native read/unmount and hardlink/invalidation
      smokes, formatting, and warnings-as-errors.

The first successful native read exposed a second protocol defect: `DESTROY`
was incorrectly classified as one-way. macFUSE's `do_destroy` sends an empty
success reply. Without it, FSKit remained in `deactivateVolume` and `unmount(2)`
waited indefinitely. The transport regression observed RED (`UnexpectedEof`)
before the reply was implemented. DESTROY now replies and closes the transport;
interrupting from the dispatch thread itself could lose the wakeup before its
next receive. Shutdown calls normal `unmount(2)` while dispatch is still alive,
serializes racing cancellation/completion requests, and propagates unmount
errors. A pre-cancelled native mount also verifies that no volume is left behind.

Two independent test-host effects were isolated without increasing deadlines:

- Mounting below the external `/Volumes/Storage` failed with an explicit
  `SystemPolicyRemovableVolumes` / `file-mount` denial for `fskitd` (EPERM).
  Native validation used a separate `target` directory on the internal volume,
  under `/Volumes/studiofs`. No TCC permissions or security settings were changed.
- `MFMount` calls `NSBundle`, which scanned the large Cargo `target/debug/deps`
  directory. A sample captured `CFBundleGetMainBundle` / `__getdirentries64`;
  a run took 130.20 seconds and hit the unchanged three-second smoke deadline.
  Copying the same executable into an isolated directory removed this delay.

The pre-cleanup native command, run from that isolated internal-volume
directory using a copy of the executable printed by `cargo test -p
studiofs-macfuse --lib --no-run`, was:

    STUDIOFS_RUN_FSKIT_MOUNT_TESTS=1 ./macfuse-tests-final tests::fskit_ --test-threads=1 --nocapture

Result: 16 passed, 0 failed in 5.46 seconds, including real pre-cancelled mount,
mount/lookup/open/read/unmount, and hardlink/write/read-alias/unmount tests.
The native startup deadlines remain three and five seconds. The warnings gate
`cargo clippy -p studiofs-macfuse --all-targets --all-features --locked -- -D warnings`
also passed. This proves these mounted scenarios, not the entire parity matrix
or mounting beneath a TCC-protected external volume.

Final library run: 175 passed, 0 failed in 4.56 seconds (native gates unset;
native evidence is the separate run above). `cargo fmt --all -- --check` and
`git diff --check` passed. No diagnostic mounts remained after validation;
temporary probe sources, samples, executable copies, and empty test mount
directories from this investigation were removed.

Native results below must come from explicitly requested ignored runs. Windows remains
outside this Mac's validation scope. The legacy backend remains the default.

## Review-blocker cleanup (2026-09-04)

- [x] Move the three native FSKit scenarios to
      `tests/fskit_mount_sanity.rs` behind
      `cfg(all(target_os = "macos", studiofs_fskit))` and `#[ignore]`; remove
      the old `STUDIOFS_RUN_FSKIT_MOUNT_TESTS` gate.
- [x] Add `MacFuseMountRequest::with_unmount_signal` so integration tests use
      the public mount-request API.
- [x] Share one readiness loop and bounded worker polling path. A readiness or
      cleanup timeout requests unmount, waits two seconds, and terminates the
      isolated native test process with a diagnostic if the worker remains
      alive; no FSKit worker is detached.
- [x] Add a live-channel invalidator regression covering inode, attribute, and
      entry notification frames over
      `MFChannelCreateWithDeviceFileDescriptor`. The RED check was observed
      with a temporary no-op invalidator and the implementation was restored
      before verification.
- [x] Rename the mounted hardlink test to reflect local hardlink/unlink
      coverage and check the removed name is `ENOENT` while the alias remains
      readable.
- [x] Document the internal-volume launch command and the TCC/Windows
      validation boundary in `docs/tests.md` and `docs/platform-bridges.md`.

Native validation command:

    STUDIOFS_FSKIT_MOUNT_ROOT=/Volumes/studiofs/target/sfs-670-validation cargo test -p studiofs-macfuse --test fskit_mount_sanity fskit_ -- --ignored --nocapture --test-threads=1

Result on macOS 26.6.2 with macFUSE 5.3.3: 3 passed, 0 failed in 34.13s.
The run used the internal `/Volumes/studiofs` volume, and a read-only check
found no remaining test mountpoints or FSKit mounts. The first sandboxed
attempt failed before FSKit because the execution sandbox could not create the
internal mountpoint; the successful result used the approved native host
permission. This is mounted evidence for these three scenarios only. A
removable `/Volumes/Storage` root remains a TCC-sensitive validation boundary,
Windows remains unvalidated on this Mac, and the full foreign
create/update/rename/unlink mounted parity matrix remains the next SFS-670
slice.

SFS-670 remains open for that parity matrix and code review; no Linear status,
commit, or push was changed by this implementation.

### Mounted parity root configuration (2026-09-04)

The shared `studiofs-e2e` mounted contour now reads the host mount root from
the repository `.env` key `STUDIOFS_E2E_MOUNT_ROOT`. A process environment
value is retained as a one-run override. Each client owns a child path below
the configured root, while reports and client metadata remain in the normal
`target/e2e-runs/<run-id>` sandbox. The stand's internal root is:

    STUDIOFS_E2E_MOUNT_ROOT=/Volumes/studiofs

The full legacy-vs-FSKit scenario sweep is still pending a clean native host
after the previous FSKit kernel handoff left uninterruptible stale processes;
the current implementation does not count that contaminated run as parity
evidence.

### Bounded macOS stale-mount probe (2026-09-04)

The first post-reboot FSKit parity batch reproduced a second lifecycle defect:
the PostgreSQL outage hook force-killed a client and then blocked forever in
`fs::metadata()` while probing its stale FSKit mountpoint. A short macOS sample
confirmed the runner was stuck in the kernel `stat` path. The runner now reads
the `/sbin/mount` table through the existing bounded command helper for macOS
readiness and stale-mount cleanup, with a five-second probe limit. The focused
mount-table regression passes; the affected native run must be repeated after
another reboot because the old runner and its FSKit workers remain in kernel
state.

### FSKit graceful teardown and current host blocker (2026-09-05)

The FSKit session now uses macFUSE 5.3.3's `MFChannelInterrupt()` to wake a
blocked receive without entering native unmount from the signal watcher. The
regular session path performs the unmount while the dispatcher is still alive,
and the focused regression proves that cancellation does not wait on the reply
send lock. The macFUSE library suite and warnings-as-errors check pass with
this change.

A fresh native `studiofs-e2e` PostgreSQL-outage run, started after a clean
reboot from the internal `/Volumes/studiofs` root, reached the mounted phase
and force-killed the client, but failed during stale-mount cleanup. The bounded
attempts to read `/sbin/mount` and to run `umount -f`, `umount`, and
`diskutil unmount force` all timed out; the resulting `fskit_agent` and
uninterruptible helper processes required another reboot to clear. The
retained report is:

    target/e2e-runs/sfs-670-fskit-postgres-outage-20260905-postreboot3/report.json

This is an external macFUSE 5.3.3 crash/handoff blocker, not evidence that the
FSKit operation dispatcher lacks the requested operation mappings. It matches
the stale-mount-after-termination failure reported upstream in
[macFUSE issue #1180](https://github.com/macfuse/macfuse/issues/1180). Full
FSKit mounted parity, including crash/restart and the remaining remote
create/update/rename/unlink matrix, remains open until a clean host can run the
same contour to completion. SFS-670 remains open and no Linear status was
changed.

## Plan self-review

- The tasks cover the approved first milestone: shared hardlink semantics,
  both bridge entry points, FSKit dispatch, concrete invalidation, lifecycle
  ordering, and a mounted proof boundary.
- Every production slice has a named RED test, an exact focused command, and a
  minimal implementation location.
- The plan intentionally leaves the remaining full parity matrix—Finder
  metadata/provenance, open-unlink, remote-change mounted scenarios, and
  broader read-only/shutdown sweeps—for later evidence-driven slices; it does
  not weaken those requirements.
- No new dependency, platform-wide abstraction, Windows edit, or SFS-671
  zero-copy work is planned.
