# SFS-670 macFUSE FSKit semantic parity

This document is a living ExecPlan for SFS-670. It follows .agent/PLANS.md and
must be kept current as milestones complete. It is also the design record
approved for implementation: the legacy macFUSE bridge stays the default, the
FSKit bridge remains opt-in, and Windows code is outside the change surface.

## Purpose / Big Picture

Users selecting the experimental macFUSE FSKit backend should be able to use
the same filesystem behavior already provided by the production macFUSE
bridge. That includes namespace operations, local-first I/O, metadata and
xattrs, cache invalidation, read-only behavior, and safe unmount. A user can
see the result by running the existing mounted scenarios once against the
legacy backend and once against FSKit; the same operations must produce the
same visible contents, attributes, errors, and remote-change convergence.

The work is staged. The first implementation milestone closes the missing
hardlink callback and replaces the FSKit invalidation no-op with serialized
FUSE notification messages sent through the existing MFMount channel. Later
milestones use the same mounted scenarios to close the remaining callback,
metadata, lifetime, and shutdown differences. SFS-671 zero-copy work and
SFS-672 performance comparison remain separate issues.

## Progress

- [x] (2026-09-02) Read SFS-670, its dependencies, the current branch, and the
      SFS-668/SFS-669 contracts.
- [x] (2026-09-02) Confirmed the branch is
      feature/sfs-670-reach-full-filesystem-semantic-parity-on-the-macfuse-fskit
      and Linear state is In Progress.
- [x] (2026-09-02) Established the clean baseline with
      cargo test -p studiofs-macfuse: 162 tests passed.
- [x] (2026-09-02) Confirmed the staged design and first milestone with the
      user.
- [x] Write, self-review, and commit this design document (`6fae39d2`).
- [x] Write the implementation plan in
      docs/superpowers/plans/2026-09-02-sfs-670-hardlink-fskit-invalidation.md.
- [x] (2026-09-03) Add RED tests for the first hardlink and invalidation
      milestone.
- [x] (2026-09-03) Implement the smallest production change that makes those
      tests pass.
- [ ] Sweep the remaining SFS-670 operations through shared mounted scenarios.
- [x] (2026-09-03) Run focused validation and update the bridge documentation;
      native FSKit mount evidence remains unavailable on this host.
- [ ] Record the final first-milestone evidence in Linear after the working
      tree is committed.

## Surprises & Discoveries

- The shared VFS already exposes
  crates/studiofs-vfs/src/vfs/namespace_ops.rs::StudioVfs::link, but the
  macOS StudioVfsAdapter has no link method and the legacy C operation tables
  omit the installed fuse_lowlevel_ops.link callback. The FSKit dispatcher
  also has no FUSE_LINK opcode branch. Hardlink parity is therefore a real
  missing transport/adapter path, not a missing filesystem-core feature.
- FsKitKernelInvalidator currently implements all three KernelInvalidator
  methods as debug-only no-ops. SFS-669 intentionally documented this as an
  initial limitation and compensated with novncache plus direct I/O. That
  fallback cannot prove remote namespace visibility or remove stale directory
  entries, so it does not satisfy SFS-670.
- The installed MFMount.framework header does not expose a named invalidation
  function, but MFChannelSendMessage accepts one complete serialized FUSE
  message body and only borrows the iovec storage for the synchronous call.
  This permits the FUSE notification protocol to reuse the existing
  FsKitControl.send lifetime and send lock. The mounted smoke remains the
  proof that the FSKit channel accepts and delivers those frames; a unit test
  alone is not enough.
- The macOS, Windows, and Linux bridges have separate transport code. The
  planned files are macOS-specific except for the already-existing shared VFS
  link operation. No Windows source change is expected, and a macOS run is not
  presented as a native Windows mount validation.
- The focused bridge suite passes with 168 tests. An explicit
  `STUDIOFS_RUN_FSKIT_MOUNT_TESTS=1` run reached the MFMount helper but did not
  become ready; the test was stopped after the helper remained blocked for
  more than 60 seconds, and the generated target mount directory was clean
  afterward. This is unavailable native FSKit validation, not a passing mount
  result.

## Decision Log

- Decision: Use a staged parity implementation with hardlink and invalidation
  as the first milestone.
  Rationale: This addresses two concrete gaps that block semantic parity while
  reusing the existing macOS adapter, KernelInvalidator contract, MFMount
  channel, and shared VFS. A full bridge rewrite would add risk without
  improving the user-visible contract.
  Date/Author: 2026-09-02 / Codex with user approval.

- Decision: Send FSKit invalidations as complete FUSE notification frames via
  FsKitControl.send and MFChannelSendMessage.
  Rationale: The installed MFMount API provides a message-body send primitive,
  and the existing control already serializes sends, retains the channel for
  the call, detects partial sends, and rejects sends after close. This keeps
  notification lifetime in the existing lifecycle instead of adding a second
  channel or a new abstraction.
  Date/Author: 2026-09-02 / Codex with user approval.

- Decision: Keep StudioVfs::link unchanged unless a RED test demonstrates a
  shared-core defect.
  Rationale: The operation and its hardlink invariants already exist in the
  shared VFS and have tests. The missing behavior is at the macOS bridge
  boundary; duplicating namespace logic in a bridge would violate the
  dependency direction and increase cross-platform risk.
  Date/Author: 2026-09-02 / Codex with user approval.

- Decision: Do not make FSKit the default and do not include zero-copy changes
  in this work.
  Rationale: The existing selector is an explicit rollback path and SFS-671
  owns MFMount buffer negotiation and copy accounting. Mixing those changes
  would make correctness failures harder to isolate.
  Date/Author: 2026-09-02 / Codex with user approval.

## Outcomes & Retrospective

Initial state: no production code has been changed for this plan. The
macFUSE package baseline is green with 162 passing tests. The implementation
outcome and any remaining parity gaps will be recorded here after each
milestone; Windows behavior will be described as untouched unless a shared
contract change makes that statement false.

## Context and Orientation

StudioFS has a shared filesystem core and platform-specific transport
bridges. A bridge translates kernel requests into the shared core and turns
the result back into a kernel reply. The relevant macOS path is:

    MFMount.framework channel
      -> crates/studiofs-macfuse/src/fskit.rs
      -> StudioVfsAdapter in crates/studiofs-macfuse/src/adapter.rs
      -> StudioVfs and metadata/cache services
      -> FUSE reply or invalidation message

The production legacy path uses libfuse/macFUSE callbacks instead:

    macFUSE low-level callback
      -> crates/studiofs-macfuse/csrc/low_level_ops.c
      -> CallbackState and StudioVfsAdapter
      -> the same StudioVfs and metadata/cache services

The legacy session, invalidation queue, and shutdown ordering are in
crates/studiofs-macfuse/src/low_level.rs. Backend selection and mount lifetime
are in crates/studiofs-macfuse/src/lib.rs. The fallback C declarations are in
crates/studiofs-macfuse/csrc/fuse_lowlevel_compat.h; the file has a recorded
parser issue at line 4, so its relevant ranges must be checked directly if it
is edited.

StudioVfs::link already performs the shared namespace operation and returns
the resulting InodeAttr, including the inode identity and link count. The
adapter is responsible for macOS read-only checks, inode liveness tracking,
error conversion, and kernel-facing attribute conversion. The legacy and
FSKit transports must call that adapter rather than implement a second link
operation.

KernelInvalidator is the shared callback used when local or remote metadata
changes make kernel-side inode or directory-entry cache state stale. The
legacy implementation calls libfuse notification functions through
SessionInvalidationQueue. The FSKit implementation will use the same logical
events but serialize them as outbound FUSE messages because MFMount exposes a
message channel rather than the libfuse notification functions.

## Selected Design

The design has three layers.

First, close the hardlink operation at the macOS semantic boundary. Add an
adapter method with the existing macOS style, equivalent to

    StudioVfsAdapter::link(
        &self,
        ino: u64,
        newparent: u64,
        newname: &[u8],
    ) -> Result<InodeAttr, i32>

It must reject read-only mutation with the established macOS errno, call
StudioVfs::link, preserve the returned inode identity and nlink, and update
the existing kernel-inode liveness state in the same way as an entry-producing
operation. It must not add retry policy or duplicate namespace logic.

Add the matching legacy callback in low_level_ops.c and wire it into the
writable operation table using the installed fuse_lowlevel_ops.link layout.
Preserve the established restricted read-only table policy and characterize
its visible error behavior instead of silently changing all read-only
callbacks. Add FUSE_LINK parsing to the FSKit dispatcher with the exact FUSE
link request layout, checked name parsing, one entry reply on success, and the
existing errno reply path on failure. The reply must increment the kernel
lookup reference for the new directory entry without inventing a new liveness
counter.

Second, replace the FSKit invalidator no-op with a small transport adapter that
holds the existing Arc<FsKitControl>. It will construct the two notification
messages already used by the legacy bridge:

    - inode data/attribute invalidation: a fuse_out_header with unique zero,
      the installed FUSE notification code, and the exact inode payload;
    - directory-entry invalidation: the same outbound header followed by the
      parent/name payload with validated byte length.

The inode attribute path will use the established negative-offset convention;
the data path will invalidate the whole file range. Exact integer widths,
field order, header length, and error-code direction must be covered by pure
serialization tests against the installed Darwin FUSE wire contract. The
transport will call FsKitControl.send, which already holds send_lock, retains
the channel until MFChannelSendMessage returns, detects a closed channel, and
rejects partial sends. It will log an expected closed-channel result during
teardown without panicking or waiting on VFS locks. The existing adapter
wrappers continue to decide whether an inode was observed and whether active
reads defer data invalidation.

Install this invalidator before FsKitSession::run, clear it immediately after
the session returns, wait for reply tasks, and close the control channel only
after the invalidator can no longer be reached by the shared VFS. This keeps
the existing terminal order and prevents a late worker from sending through a
destroyed MFMount channel.

Third, run a parity sweep over the Linear scope using the same adapter and
the same mounted scenarios for both macOS backends. The sweep will verify,
then correct only the observed differences in lookup/getattr/setattr,
create/open/read/write/flush/fsync/release, directory handles and enumeration,
rename/unlink/rmdir/mkdir/mknod, symlinks, xattrs and ResourceFork,
statfs/access, read-only mutations, Finder provenance metadata, open-unlink,
foreign create/update/rename/unlink, and shutdown while I/O is active. Any
operation that is already routed through the adapter stays there. No new
filesystem core or platform-neutral registry is introduced.

## Error Handling

Malformed FSKit messages continue to use the existing checked length and
errno reply helpers. A malformed hardlink request is rejected before VFS work;
an operation error uses the adapter's existing VFS-to-errno mapping. A
notification send failure is observable in logs and test counters, is ignored
only after the channel has entered teardown, and never causes a process panic
or a retry loop that could deadlock a filesystem callback.

Hardlink success must produce exactly one entry reply containing the existing
inode and updated link count. A failed reply must not leave a prepared open or
lookup state behind. FSKit cancellation and shutdown retain their current
exactly-once reply/no-reply rules.

## Plan of Work

Milestone 1 is the hardlink and invalidation foundation. Begin with one RED
test at a time: an adapter hardlink test using a real StudioVfs, a legacy
operation-table assertion, an FSKit request/reply or dispatch assertion, and
pure byte-level notification serialization tests. Run each focused test and
confirm it fails for the missing behavior before production edits. Then add
the adapter method, legacy callback/table entry, FSKit opcode and wire
builders, keeping the patch limited to existing files and helpers. Run the
same tests green, then refactor only duplicated test setup or encoding code.

Milestone 2 is mounted coherence and lifecycle proof. Extend the existing
gated FSKit mounted smoke and the corresponding legacy scenarios to exercise
same-mount accepted-write visibility, hardlink identity and unlink behavior,
remote namespace changes, xattrs/ResourceFork, read-only mutations,
open-unlink, and unmount during active I/O. Use bounded polling and existing
test synchronization; do not make sleeps or global timeouts larger to hide a
race. If a test exposes a mismatch, trace the first differing request and
state transition before editing that path.

Milestone 3 is the remaining parity sweep and documentation. Compare the
legacy callback table, FSKit dispatcher, adapter methods, reply layouts, and
mounted observations for every operation listed in SFS-670. Fix backend-only
differences at the macOS adapter/transport seam. Update
docs/platform-bridges.md and docs/macos-native-bridge-split.md, and amend the
SFS-668/SFS-669 limitation text so it no longer claims that invalidation is
unavailable once this milestone is proven. Leave SFS-671 and SFS-672 as
cross-references, not partial implementations.

## Concrete Steps

All commands run from /Volumes/Storage/dev/studiofs.

The recorded baseline command is:

    cargo test -p studiofs-macfuse

Expected baseline: 162 unit tests pass and no test fails. The doc-test target
currently has no tests; that is not used as validation evidence.

After each RED test is added, run its single filter, for example:

    cargo test -p studiofs-macfuse adapter_link_preserves_inode_and_link_count -- --nocapture

Use one cargo test filter per command. A command reporting running 0 tests is
not validation; correct the filter before continuing.

For the native mounted checks, use the existing gates separately:

    STUDIOFS_RUN_FSKIT_MOUNT_TESTS=1 cargo test -p studiofs-macfuse fskit_mounted_lookup_open_read_unmount_smoke_is_gated -- --nocapture

    STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-macfuse low_level_session_mount_unmount_smoke_is_gated -- --nocapture

The final local checks are:

    cargo test -p studiofs-macfuse
    cargo fmt --all -- --check
    cargo clippy -p studiofs-macfuse --all-targets --all-features --locked -- -D warnings
    git diff --check

Run cargo test -p studiofs-vfs when shared VFS files or shared tests change.
Run broader workspace and integration commands only when a failure or actual
blast radius justifies them. Do not claim a Windows mount test from these
commands: no native Windows/WinFSP runner is available in this macOS
environment, and the intended diff does not touch the Windows bridge.

## Validation and Acceptance

The first milestone is accepted only when all of the following are true:

- A writable legacy mount and a writable FSKit mount can create a hardlink;
  lookup of both names returns one inode, nlink increases, and removing one
  name leaves the other readable.
- Read-only hardlink behavior is characterized and matches the established
  macOS bridge policy; no mutation is silently accepted.
- The FSKit dispatcher no longer reports FUSE_LINK as unsupported and malformed
  link payloads are rejected without VFS side effects.
- Pure tests prove the exact inode and entry invalidation frames, including
  header length, unique zero, notification code, field widths, and names with
  non-ASCII bytes.
- A gated mounted FSKit scenario observes an accepted local write immediately,
  sees a foreign create/update/rename/unlink after the corresponding
  invalidation, and does not retain a stale directory entry after teardown.
- Active read/write tasks cannot send through a closed FSKit channel, and
  unmount completes after the existing reply/task drain ordering.

The full SFS-670 acceptance gate additionally requires every operation in the
Linear description to be either exercised successfully through FSKit and
legacy with matching behavior or documented as genuinely unnecessary with
evidence from the mounted scenario. Flush, fsync, and release must retain
local-first durability and must not add S3/PostgreSQL waits. Finder metadata,
ResourceFork, xattrs, read-only behavior, and remote convergence must not
regress. The legacy backend remains the default. No Windows source change is
expected and no native Windows validation is claimed here.

## Idempotence and Recovery

The design and unit tests are safe to rerun. Keep all temporary artifacts under
target/. Use a new isolated mounted test directory for each gated run and
unmount only the exact path created by that run. If a mounted test exits
unexpectedly, inspect the retained process output and verify the exact mount
is gone before retrying; do not delete a broad workspace directory.

If a native FSKit notification experiment fails, preserve the failing frame,
errno, macFUSE version, and first stale-state observation in this plan. Do not
restore the no-op invalidator or broaden direct-I/O fallback and call the
feature complete. If code changes must be abandoned before commit, use the
normal working-tree diff for recovery; do not reset unrelated user changes.

## Artifacts and Notes

Initial evidence:

    HEAD: c84abf54 (merge of SFS-669 / PR #661)
    Linear: SFS-670, In Progress, branch feature/sfs-670-reach-full-filesystem-semantic-parity-on-the-macfuse-fskit
    Baseline: cargo test -p studiofs-macfuse -> 162 passed, 0 failed
    FSKit install: /Library/Filesystems/macfuse.fs/Contents/Frameworks/MFMount.framework

First milestone evidence (2026-09-03):

    cargo test -p studiofs-macfuse -> 168 passed, 0 failed
    adapter_link_preserves_inode_and_increments_link_count -> 1 passed
    writable_studio_vfs_low_level_ops_install_link_callback -> 1 passed
    fskit_link_request_uses_the_fuse_wire_layout -> 1 passed
    fskit_invalidation_frames_match_fuse_wire_layout -> 1 passed
    fskit_invalidator_tolerates_a_closed_control -> 1 passed
    STUDIOFS_RUN_FSKIT_MOUNT_TESTS=1 ... fskit_mounted_hardlink_and_invalidation_smoke_is_gated
      unavailable: MFMount helper stayed blocked before readiness; target cleanup verified

The code knowledge graph is ready for the current commit. The planned Rust
files have no recorded coverage issue; the fallback C header has a
parse-partial marker at line 4 and must be read directly if edited. The
installed MFMount header documents that MFChannelSendMessage copies one
complete message body synchronously and does not retain caller buffers.

At each stopping point, append the focused command, its result, the first
failure or acceptance observation, and any changed decision to this section
and to Progress. Do not replace a phase-specific mounted measurement with a
later aggregate result.

## Interfaces and Dependencies

Reuse these existing interfaces:

    studiofs_vfs::KernelInvalidator
    StudioVfs::link(ino: Ino, new_parent: Ino, new_name: &[u8]) -> VfsResult<InodeAttr>
    FsKitControl::send(buffers: &[iovec]) -> io::Result<()>
    FsKitControl::close()
    CallbackState::kernel_invalidator(inner: Arc<dyn KernelInvalidator>)
    StudioVfsAdapter::attr_to_stat(attr: &InodeAttr)

Add only the macOS bridge seam required by the first milestone:

    StudioVfsAdapter::link(
        &self,
        ino: u64,
        newparent: u64,
        newname: &[u8],
    ) -> Result<InodeAttr, i32>

    FsKitDispatcher::link(&self, request: FsKitRequest)

The FSKit wire structs and notification encoders must use the existing
repr(C), checked-slice, encode, request-reply, and iovec patterns in fskit.rs.
Do not add a dependency or a new cross-platform operation registry. The
legacy C callback must use the installed macFUSE fuse_lowlevel_ops.link ABI;
the fallback header is edited only if its declarations are required for a
supported build and its ABI order is verified directly.

The main dependencies are the pinned macFUSE 5.3.3 MFMount.framework and the
existing Tokio/runtime, StudioVfs, metadata, cache, and payload services. The
FSKit backend must continue to avoid fuse_mount, fuse_session_loop,
fuse_session_add_chan, and raw fuse_reply_* calls. SFS-671 owns negotiated
FUSE_DARWIN_REPLY_BUF/FUSE_DARWIN_PAYLOAD_BUF handling; this plan keeps the
current owned read/write path until that issue lands.

Revision note: created 2026-09-02 after user approval of the staged parity
design. The first milestone deliberately targets the two confirmed missing
contracts before the broader mounted parity sweep.
