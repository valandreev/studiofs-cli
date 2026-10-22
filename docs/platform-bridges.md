# Platform Bridges

StudioFS supports Windows, macOS, and Linux through thin bridge crates that
translate OS-specific filesystem callbacks into `StudioVfs` method calls.

## Overview

The [shared metadata-cache coherence contract](metadata-cache-coherence.md)
defines invalidation intents, health and policy semantics for the Windows-first
cache work. Its [Windows baseline](perf/sfs-675-windows-metadata-baseline.md)
records current behavior and the separate promotion requirements.

```
┌──────────────────────┐     ┌──────────────────────┐
│   studiofs-winfsp    │     │   studiofs-macfuse    │
│   (Windows only)     │     │   (macOS only)        │
├──────────────────────┤     ├──────────────────────┤
│  winfsp crate (0.12) │     │  low-level + FSKit   │
│  FileSystemContext    │     │  callback transports │
├──────────────────────┤     ├──────────────────────┤
│     StudioVfs        │     │     StudioVfs         │
└──────────────────────┘     └──────────────────────┘
```

Both bridges follow the same pattern:
1. Receive a filesystem callback from the OS
2. Convert OS-specific arguments to VFS types
3. Call `StudioVfs` (via `rt.block_on()`)
4. Convert the result back to OS-specific response

Local payload capacity exhaustion and OS disk-full errors use the shared VFS
`StorageFull` category, including errors retained through local payload and
writer materialization sources. POSIX adapters return `ENOSPC`; WinFSP returns
`STATUS_DISK_FULL`. Resource pressure already classified as
`InsufficientResources` keeps `ENOMEM` / `STATUS_INSUFFICIENT_RESOURCES`,
and unrelated failures keep their existing mappings.
Source inspection for disk-full is limited to VFS I/O, local payload, and
upload-stage I/O errors; nested causes do not override object-corruption or
remote-fetch categories. Writer failures retain the operation label as well as
the original error source.

Shutdown snapshots retain the disk-full category before discarding error
sources, so repeated shutdown callers receive the same disk-full response.
After failed writer materialization, a strict local barrier retries the retained
slices using their original object IDs; it reports exhaustion until persistence
succeeds, even when no new writer bytes or object IDs are needed.

This disk-full mapping does not add quota handling: `FilesystemQuotaExceeded`
(`EDQUOT` on POSIX) keeps its existing fallback mapping and must remain distinct
from `ENOSPC` if quota reporting is added. Metadata failures represented by
`MetaError::Storage(String)` also keep `GenericFailure`; the redb I/O cause is
converted to text before reaching this bridge. Metadata disk-full reporting
requires retaining that typed cause, rather than interpreting storage-error text.

Path walks are centralized in VFS `NamespaceResolver`:
- WinFSP uses `NamespacePathStyle::Windows` for full-path and parent resolution.
- macFUSE uses VFS component lookup (`lookup`) which is routed through the same resolver logic.
- Bridge-local namespace traversal should not bypass VFS runtime filtering.

WinFSP rename forwards `ReplaceIfExists` to the local metadata write
transaction. With replacement disabled, an existing file or directory returns
`STATUS_OBJECT_NAME_COLLISION` before either entry changes, including when the
destination was created after path resolution. Replacement-enabled callbacks
retain the existing replacement and nonempty-directory behavior. Filtered
metadata and destructive trash renames do not use that transaction: no-replace
requests reject an existing target as a collision and an absent target as
unsupported, preserving the source in both cases.

## macOS backend selection

The default macOS backend is the existing low-level macFUSE bridge. Select it
explicitly with `STUDIOFS_MOUNT_BACKEND=legacy` or
`STUDIOFS_MOUNT_BACKEND=macfuse`. The experimental native FSKit transport is
opt-in:

```bash
STUDIOFS_MOUNT_BACKEND=fskit studiofs mount <volume> /Volumes/<name>
```

The legacy backend uses libfuse2's `fuse_session_loop_mt`: macFUSE manages
concurrent native callback workers, so a blocked local callback allows unrelated
requests to progress. The loop joins those workers before returning. StudioFS
then drains replies queued on its runtime before destroying the session and
boxed callback state. Each request retains a single reply owner. The libfuse2
API has no worker-count configuration argument.
Legacy request callbacks contain Rust unwinding panics before returning through
the C ABI. A request that has not been handed to a reply owner is completed with
`EIO` on failure; `forget` instead releases its request with `fuse_reply_none`.
Requests already handed to a worker retain that worker's reply ownership.
Borrowing or consuming an already replied request returns a controlled error
before calling macFUSE, and interrupt callbacks are unregistered before request
release. This containment does not recover aborting panics or invalid C pointers.
macFUSE grows the pool when workers are occupied and retires idle workers;
StudioFS does not configure an additional worker limit. Write callbacks await
the VFS local durability path, without synchronous S3 or PostgreSQL publication.

Native concurrency and shutdown acceptance holds one real write callback,
requires a write to a different open file to finish within three seconds, and
checks that the loop waits for the held callback after exit is requested:

    cargo test -p studiofs-macfuse --lib session_tests -- --ignored --nocapture --test-threads=1

This requires a native legacy macFUSE mount; an ignored result does not prove
callback concurrency.

The FSKit path uses macFUSE `MFMount.framework` and does not open `/dev/fuse` or
call the legacy libfuse session/reply APIs. It reuses the existing
`StudioVfsAdapter`, keeps the received message alive until its reply is sent,
interrupts the channel for signals and explicit unmount, and drains callback
workers before releasing the channel. The selected backend name is included in
mount diagnostics/status as `macFUSE-FSKit`.

This path is intentionally opt-in: mount points must be below `/Volumes`, macOS
15.4+ and macFUSE 5.3.3+ are required, and the framework must be installed
under `/Library/Filesystems/macfuse.fs/Contents/Frameworks` or the parent path
supplied through `STUDIOFS_MFMOUNT_FRAMEWORK_DIR`; if it is not available, the
package still builds the legacy bridge and rejects the FSKit selector as
unsupported. The bridge now installs a concrete kernel invalidator that sends
FUSE `INVAL_INODE` and `INVAL_ENTRY` frames through the existing MFMount
channel, including separate full-inode, attribute-only, and directory-entry
payloads. The existing `novncache` and per-handle direct-I/O choices remain
explicit cache safeguards; FSKit invalidation is no longer a logging-only
fallback. Hardlink requests are translated through the shared
`StudioVfsAdapter` in both macFUSE operation transports. Positioned FSKit
`getxattr` reads preserve the legacy `position`, size-query, `ENOATTR`, and
`ERANGE` contract. Potentially waiting `rmdir` callbacks run as tracked blocking
reply tasks. Symlink replies use tracked async tasks: each local metadata attempt
runs on the blocking pool, while a deferred parent is awaited through VFS
notification with a five-second deadline and request/shutdown cancellation. The
wait ends when any deferred operation on the parent inode settles, so a parent
create that commits ahead of queued attribute updates retries immediately.
Neither wait occupies the FSKit dispatcher loop. Each mount's signal watcher
blocks on its own wake stream for stop and unmount requests, and also observes
the process-lifetime stream written by the async-signal-safe shutdown handler;
signal bytes stay readable while shutdown is requested, so concurrent watchers
cannot consume one another's wakeups. The zero-copy negotiation and
the remaining full semantic parity matrix
remain separate follow-up work.

Native FSKit mount sanity is kept out of the default library gate as an
ignored, opt-in integration test. Run it only with the `fskit-mount-sanity`
Cargo feature, an approved provider, and an internal volume mount root, for
example:

```bash
STUDIOFS_FSKIT_MOUNT_ROOT=/Volumes/studiofs/target/sfs-670-validation \
cargo test -p studiofs-macfuse --features fskit-mount-sanity \
  --test fskit_mount_sanity fskit_ -- \
  --ignored --nocapture --test-threads=1
```

The test binary owns only PID-scoped mountpoints and polls the FSKit worker
after requesting unmount. A worker that misses the fixed cleanup window makes
the isolated native test process fail with a diagnostic; it is never detached.
The library live-channel regression proves that the concrete invalidator sends
inode, attribute, and entry frames, while the ignored tests are the separate
proof of mounted behavior. Removable roots such as `/Volumes/Storage` may be
blocked by macOS TCC for `fskitd`; such a run is not a passing FSKit result.
Windows validation is outside this macOS test boundary, and the remaining
foreign create/update/rename/unlink parity matrix remains a follow-up.

Mounted multi-client parity runs use the existing `studiofs-e2e` contour. Set
the host-specific root in the repository `.env`:

```dotenv
STUDIOFS_E2E_MOUNT_ROOT=/Volumes/studiofs
```

The runner mounts each client below `<root>/<client>/<volume>` and keeps
artifacts under `target/e2e-runs/<run-id>`. `STUDIOFS_E2E_MOUNT_ROOT` in the
process environment may override `.env` for a one-off run. Run the same
scenario selection once with `STUDIOFS_MOUNT_BACKEND=legacy` and once with
`STUDIOFS_MOUNT_BACKEND=fskit`; the backend selector is intentionally separate
from the mount-root setting.

On macOS, the mounted runner uses a bounded `/sbin/mount` table probe for
readiness and stale-mount cleanup. It does not `stat` an FSKit mountpoint after
worker termination, because a stale vnode can block that call indefinitely.

Graceful FSKit teardown now interrupts the MFMount receive wait and performs
the native unmount on the regular session path. This does not make abrupt
process termination safe: on macFUSE 5.3.3 a killed FSKit client can leave a
stale mount handoff in the kernel and make `/sbin/mount`, `umount`, or
`diskutil unmount` block until reboot. Such a crash/restart run is a failed
native validation, not a skipped or passing scenario. The full mounted parity
matrix must therefore be rerun from a clean host after the macFUSE handoff is
fixed or upgraded.

## WinFSP Bridge (`studiofs-winfsp`)

### Requirements

- [WinFSP](https://winfsp.dev/) installed with the architecture-matching
  runtime DLL available to StudioFS.
- The CLI and WinFSP bridge share one startup resolver. It preloads
  `winfsp-x64.dll` on x86_64, `winfsp-x86.dll` on x86, or `winfsp-a64.dll` on
  ARM64 from the executable directory or the standard WinFSP `bin` directory
  under `%ProgramFiles(x86)%` / `%ProgramFiles%`.

### Mount Configuration

| Parameter | Value |
|-----------|-------|
| Mount type | Local disk device (drive-letter mount, no network prefix) |
| Sector size | `4096` bytes by default. Override: `STUDIOFS_WINFSP_SECTOR_SIZE` |
| Allocation unit size | `1048576` bytes by default. Override: `STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE` |
| Max component length | 255 |
| Metadata cache policy | `adaptive` by default. Override: `STUDIOFS_WINFSP_METADATA_CACHE_POLICY=conservative`; unknown values fail safe to `conservative` |
| File info timeout | WinFSP cache-manager mode (`-1`/`max`) by default. Override: `STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS` |
| Dir info timeout | `60000 ms` by default in adaptive mode, `0 ms` in conservative mode. Override: `STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS` (`-1`/`max` supported) |
| Volume info timeout | `60000 ms` by default in adaptive mode, `0 ms` in conservative mode. Override: `STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS` (`-1`/`max` supported) |
| Security/name timeout | `60000 ms` by default in adaptive mode, `0 ms` in conservative mode. Override: `STUDIOFS_WINFSP_SECURITY_TIMEOUT_MS` |
| Stream info timeout | `60000 ms` by default in adaptive mode, `0 ms` in conservative mode. Override: `STUDIOFS_WINFSP_STREAM_INFO_TIMEOUT_MS` |
| Extended attribute timeout | `60000 ms` by default in adaptive mode, `0 ms` in conservative mode. Override: `STUDIOFS_WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_MS` |
| Allow open in kernel mode | Disabled by default to preserve same-inode overwrite coherence. Override: `STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE=1` |
| Flush/purge on cleanup | Disabled by default to preserve read-cache pages across close/reopen cycles. Override: `STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP=1` |
| Case sensitive | Yes |
| Unicode on disk | Yes |
| Persistent ACLs | No (always disabled) |
| Reparse points | Yes |
| Volume serial | `0x5354_4644` ("STFD") |
| Filesystem name | "StudioFS" |

Windows per-file security descriptors are not persisted: the descriptor passed
to `Create` is ignored, security queries use a context-wide descriptor, and
`SetSecurity` is unsupported. A true value for
`STUDIOFS_WINFSP_PERSISTENT_ACLS` now rejects mount startup; the capability is
always disabled. VFS per-inode ACL enforcement for the configured mount
principal is independent and unchanged. Full Windows per-file ACL persistence
is future work scoped separately.

`STUDIOFS_WINFSP_SECTOR_SIZE` and `STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE` are the primary WinFSP bridge throughput geometry controls for local payload I/O.
The metadata timeout fields are mount-time WinFSP parameters. SFS-677 health
gates new read-only file-cache admission through `Create.Opened.DisableCache`;
it does not mutate those kernel metadata TTLs live. On an unsafe transition,
StudioFS retires its known cache generations and enqueues the root and known
inodes for WinFSP invalidation; native delivery remains bounded and observable.
Write-capable handles always bypass cache-manager writeback.
StudioFS keeps WinFSP kernel-mode opens disabled by default: a kernel-mode open
can reuse a stale cache-manager file object after a same-inode published
overwrite and bypass the user-mode `Open` callback that resolves the current
content identity. `STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE=1` is therefore
an explicit performance/debug override with weaker overwrite-coherence
guarantees.
StudioFS disables WinFSP `FlushAndPurgeOnCleanup` by default so read-only
handles can preserve cache-manager pages across close/reopen cycles.
Write-capable file handles opt out of the WinFSP/Windows file cache at
`Create`/`Open` time, while read-only handles keep the WinFSP cache-manager /
fast-I/O path by default. Regular-file `FileInfo.IndexNumber` includes the
committed content epoch, so remote same-inode overwrites get a new cache-manager
identity when the open is resolved by user mode. Use a bounded
`STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS` value only for targeted multi-client
invalidation experiments. Set `STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP=1`
only for targeted stale-cache investigations.

Windows mount-point handling:
- StudioFS requests Mount Manager drive syntax (`\\.\X:`) internally for drive-letter mounts.
- If Mount Manager access is denied, mount fails with an explicit error (instead of silently falling back to `DefineDosDevice` semantics).
- Typical requirement: run with elevated privileges, or configure WinFSP `MountUseMountmgrFromFSD=1`.

### Implementation: `StudioFsContext`

Implements `winfsp::filesystem::FileSystemContext` trait.

**Per-file context (`StudioFileHandle`):**
- `ino` — inode number
- `is_dir` — directory flag
- `fh` — VFS file handle
- `open_flags` — VFS open flags stored with the handle
- `dir_buffer` — WinFSP directory buffer for readdir

**Key mappings:**

| WinFSP Callback | VFS Method |
|-----------------|------------|
| `get_volume_info` | `statfs()` |
| `get_security_by_name` | `NamespaceResolver::resolve_path()` |
| `open` | `NamespaceResolver::resolve_path()` + `open()` |
| `close` | `release()` |
| `read` | `read_into_fh()` |
| `write` | `write_fh_with_deferred_materialize()` |
| `cleanup` | observe callback; delete finalizes namespace mutation, non-delete file-handle cleanup defers barrier to `close` / `release_fh()` |
| `flush` | `flush()` |
| `get_file_info` | `getattr()` |
| `set_basic_info` | `setattr()` |
| `set_file_size` | `setattr()` (truncate) |
| `read_directory` | `readdir()` |
| `create` | `NamespaceResolver::resolve_parent()` + `create_file()` / `mkdir()` |
| `rename` | `NamespaceResolver::resolve_parent()` + `rename()` |
| `set_delete` | _(mark for delete)_ |
| `cleanup` + delete flag | `unlink()` / `rmdir()` |

I/O buffer behavior on Windows callbacks:
- `read`: WinFSP-provided output buffers are forwarded directly to the VFS `read_into_fh()` data-reader path; `StudioFsContext` does not allocate an intermediate payload buffer for file data reads.
- Mounted `read`, `write`, and `read_directory` use WinFSP async completion (`STATUS_PENDING` + later response) through `AsyncFileSystemContext`, so dispatcher threads are not parked while Tokio/VFS work is in progress.
- `write`: WinFSP-provided input buffers are forwarded directly to `write_fh_with_deferred_materialize()`; `StudioFsContext` does not allocate an intermediate payload buffer before the VFS writer accepts the bytes.
- Handles opened with Windows `FILE_WRITE_THROUGH` or `FILE_NO_INTERMEDIATE_BUFFERING`
  are mapped to the explicit `WriteThrough` local sync policy. Ordinary handles
  remain opportunistic writeback; write-through handles force the submitted bytes
  through the local payload and metadata durability barrier before the callback
  returns, without changing remote upload or peer-publication semantics.
- `read_directory`: per-entry UTF-16 conversion reuses a single `Vec<u16>` buffer and only falls back to lossy replacement semantics for invalid UTF-8 names.

WinFSP callback barrier semantics:
- Explicit `flush(context=Some(..))` is WinFSP's `FlushFileBuffers` callback.
  It admits the handle through `flush_volatile_writeback_with_outcome()` and
  may return before the bounded background local-dirty commit. The explicit
  `fsync` API owns local payload and redb durability barriers; neither operation
  waits for remote upload or peer publication.
- The synchronous `flush` wait is bounded by
  `STUDIOFS_WINFSP_FLUSH_CALLBACK_TIMEOUT_MS` (default 30000 ms, 30 s). On expiry the
  callback fails with `STATUS_IO_TIMEOUT`, keeps any deferred cleanup hint for
  the last-handle `close` barrier, and lets the admitted task finish in the
  background. Synchronous `read`/`write` borrow WinFSP buffers and stay
  unbounded; the mounted IO path uses their async callbacks.
- `cleanup` with delete finalizes `unlink()` / `rmdir()` in the cleanup callback itself.
- WinFSP `FlushAndPurgeOnCleanup` is disabled at the driver layer by default so
  read-only file caching keeps WinFSP cache-manager behavior across close/reopen
  cycles. Write-capable handles set `Create.Opened.DisableCache` so writes
  bypass the OS file cache. Committed content epoch is folded into regular-file
  `FileInfo.IndexNumber` so remote same-inode content changes do not reuse stale
  cached file data. Set `STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP=1` only for
  targeted stale-cache investigations.
- Non-delete `cleanup` callback handling in StudioFS is observational only; for real file handles it records that the callback happened, but any remaining writer state is deferred to a later explicit `flush` or the last-handle `close` / `release_fh()` path.
- `fh == 0` contexts, such as directories, do not have a `release_fh()` fallback; they stay observational unless an explicit `flush` callback makes local state visible.
- `close` is the release-path hook for WinFSP. It admits remaining local writer
  state for the last handle to the same volatile worker, without waiting for
  redb, PostgreSQL replication, object upload completion, or peer publication.
  `fsync` remains the explicit caller-requested strict local-redb sync API.
- `overwrite` and EOF-changing `set_file_size` commit the new local EOF before the callback returns; they are not backgroundable flush barriers.

**Attribute mapping:**
- `InodeAttr.win_attrs` → Win32 `FILE_ATTRIBUTE_*` flags
- Directories always have `FILE_ATTRIBUTE_DIRECTORY`
- Empty `win_attrs` on regular files defaults to `FILE_ATTRIBUTE_NORMAL`

**NTSTATUS mappings:**
- `InodeNotFound` → `STATUS_OBJECT_NAME_NOT_FOUND`
- `EntryNotFound` → `STATUS_OBJECT_NAME_NOT_FOUND`
- `EntryExists` → `STATUS_OBJECT_NAME_COLLISION`
- `DirNotEmpty` → `STATUS_DIRECTORY_NOT_EMPTY`
- `IsADirectory` → `STATUS_FILE_IS_A_DIRECTORY`
- `RepairRequired` → `STATUS_DISK_CORRUPT_ERROR`
- `RemoteUnavailable` → `STATUS_BAD_NETWORK_PATH`
- `Timeout` → `STATUS_IO_TIMEOUT`
- `MetadataPersistenceDegraded` → `STATUS_IO_DEVICE_ERROR`
- Other errors → `STATUS_UNEXPECTED_IO_ERROR`

### Mount Lifecycle

```
mount_and_serve(vfs, rt, mount_point, volume_label, foreground, read_only, shutdown_event_name, profiling)
  │
  ├─ Configure VolumeParams
  ├─ Create FileSystemHost
  ├─ Mount filesystem
  ├─ Block on Ctrl+C signal
  │
  └─ Shutdown:
       ├─ host.stop()
       ├─ host.unmount()
       └─ vfs.shutdown() — bounded upload drain, cancel in-flight uploader I/O, persist checkpoint
```

Current signature includes additional policy controls:
- `read_only` toggles read-only mount behavior.
- `shutdown_event_name` allows external graceful shutdown signal integration.
- WinFSP forwards caller buffers into the shared buffered VFS read/write paths; there is no Windows-specific alternate data path.

## Linux Bridge (`studiofs-linux-fuse`)

Linux mount starts through a thin libfuse3 low-level bridge. The bridge uses
the system libfuse3 development/runtime packages and `fusermount3`; StudioFS
does not vendor, bundle, or build libfuse3.

Linux directory replies call `fuse_add_direntry` once per entry with the remaining
buffer capacity. Its [public contract](https://libfuse.github.io/doxygen/fuse__lowlevel_8h.html)
returns the required size without writing when the entry does not fit. Libfuse
owns size, alignment, and encoding; StudioFS does not duplicate the dirent ABI.
An entry that does not fit ends the reply and is returned first by the next
`readdir`. Names with embedded NUL bytes are rejected with `EINVAL`.

Directory handles capture a compact immutable snapshot of names, inode numbers,
and file types on the first read that reaches child entries. Opening a directory
or a reply with too little space for the smallest child dirent does not
enumerate its children. Before snapshot capture, the bridge queries libfuse
for that minimum size using a one-byte name; it does not hardcode the ABI.
A longer actual child name may still fail to fit after capture.
Chunked reads and backward seeks to a nonzero offset keep the same offsets after
namespace mutations. A read at offset 0 (the first read, or `rewinddir`) drops
the handle's snapshot so the next child read observes the current namespace,
matching WinFSP's marker-less `read_directory`. Because capture happens on first
child read, `ENOENT` for a directory removed before then is returned by
`readdir`, not `opendir`.

As on WinFSP, snapshots are shared through a session cache keyed by inode and
validated by `StudioVfs::directory_enumeration_observation`. The cache keeps at
most 64 snapshots and 4 MiB of entry/name bytes with LRU eviction, clears on a
newer namespace epoch, and does not admit a build whose observation changed
during enumeration. Unlike WinFSP, the Linux key ignores the attribute
generation because dirents carry only inode and file type. A cache hit skips VFS
enumeration entirely; an existing handle's snapshot is never replaced.
Full child attributes are not retained because ordinary libfuse dirents use only
inode and file type. A captured snapshot still holds O(N) names, like WinFSP's
per-handle `DirBuffer`, rather than paging metadata. A 1024-entry fixture with
nine-byte names retains 50,176 entry/name bytes, compared with 156,672 for the
previous attribute-bearing snapshot (68% less); these are allocation-layout
measurements, not mounted RSS measurements.

Reply storage is pooled per session after each synchronous reply, including
error replies. The pool retains at most 16 buffers, each with at most 128 KiB of
aligned reply storage and 4 KiB of scratch-name storage. Larger requests still
work, but their buffers are discarded after use. A reused buffer honours the
current requested size and exposes only bytes initialized for that reply.

Current Linux callbacks:

| libfuse3 Callback | VFS Method |
|-------------------|------------|
| `lookup` | `lookup()` |
| `getattr` | `getattr()` |
| `setattr` | `setattr_with_handle()` |
| `create` | deferred namespace admission + `open_created_file_with_known_attr()` |
| `mkdir` | `mkdir()` |
| `mknod` | `create()` for regular files; special files return `ENOTSUP` |
| `symlink` | `symlink()` |
| `unlink` | `unlink()` |
| `rmdir` | `rmdir()` |
| `rename` | `rename()`; non-zero libfuse3 rename flags return `EINVAL` |
| `link` | `link()` |
| `open` | `open_file_with_known_attr()` |
| `read` | `read_into_fh()` |
| `readlink` | `readlink()` |
| `write` | `write_fh_with_deferred_materialize()` |
| `flush` | async-write inode watermark + `flush_volatile_writeback_with_outcome()` |
| `fsync` | `fsync_fh()` when handle-backed, otherwise `fsync()` |
| `release` | close handle admission, wait its write watermark, then `release_fh_with_deferred_last_handle_barrier()` |
| `forget` | decrement successful entry/create lookup references |
| `statfs` | `statfs()` |
| `access(2)` | Native kernel permission check (`default_permissions`); no userspace access callback |
| `opendir` | `getattr()` directory check |
| `readdir` | lazy per-handle `readdir()` snapshot |
| `releasedir` | success reply |
| `getxattr` | `get_xattr()`; missing attributes return `ENODATA` |
| `setxattr` | `set_xattr()` |
| `listxattr` | `list_xattrs()` as NUL-separated names |
| `removexattr` | `remove_xattr()`; missing attributes return `ENODATA` |

Linux ordinary `flush` and final `release` admit state to the shared volatile
metadata worker; the batch age is bounded to one second. An async-write ledger
uses sequence-number watermarks, so a barrier waits for earlier writes to its
own inode or handle, never unrelated inodes or later writes.

Async-write capacity is granted in registration order. Each admission predicate
evaluation is O(1); advancing the queue on reservation or completion is O(log N)
in the pending-request tree. The shared condition variable wakes all waiters,
including inode and handle barriers, after admission or completion.

Linux `flush`, `fsync`, `release`, and `link` limit this async-write wait to 30 seconds,
including time that matching writes spend in the global admission queue. A slow
healthy disk can exhaust this callback budget. On timeout the callback logs its
scope and pending work, increments `fuse_async_write_barrier_timeouts`, and returns
`EIO` without entering the VFS barrier. The timeout fails only that invocation:
idle and unrelated files continue to flush, fsync, link, close, and admit writes.
A read-only close can still report an inode-scoped flush timeout if another
handle on the same inode has pending writes. Later barriers can succeed once
those writes finish; actual write errors retain their normal sticky delivery.

Pending tasks and reservations remain owned because VFS writes cannot safely be
cancelled. A timed-out release retains its VFS handle and inode liveness pin until
all of that handle's writes finish, then automatically closes the handle through
the VFS-owned deferred last-handle barrier and removes the ledger close state.
The pin remains live through the kernel release reply and late handle close.
Session teardown includes this cleanup in its bounded write drain and retains
callback state if it cannot drain. Permanently stuck writes therefore retain
only their own resources; a barrier timeout does not require remount to restore
the write path. Admission remains subject to the existing global pending-byte
and pending-write limits.

The `link` wait preserves sticky write errors for the next `flush`, `fsync`, or
`release`; a timed-out link returns `EIO` without changing the namespace.

Linux `fsync` maps to the shared strict metadata sync boundary after any
handle-local payload state is drained. The adapter only translates resulting
errors to errno; queue draining and the redb barrier remain in shared VFS and
metadata code.

Linux regular-file opens use the adaptive cache policy by default. Clean
read-only handles use the kernel page cache when they fit available memory;
write-capable, dirty, actively written, unknown, and larger-than-memory files
use direct I/O. A stable content generation enables `keep_cache`; a first or
changed generation replaces cached pages. Set
`STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct` and remount for the operational
rollback.

Adaptive sessions sample available memory in an owned background thread. Open
callbacks read an atomic snapshot; samples expire two seconds after measurement
starts, with a one-second sampling interval. Missing, failed, or expired samples
select direct I/O, including for stable generations, and count in
`linux_fuse_memory_bypasses`; a host without memory telemetry therefore loses
page-cache throughput visibly. Session teardown stops and joins the sampler. Inode liveness and regular-file handle bookkeeping use 32
independent lock segments while
preserving lookup/reply and deferred-release lifetimes.

The bridge installs a bounded, coalescing invalidation worker through libfuse3
low-level inode notifications. Successful entry/create replies and `forget`
maintain lookup liveness, while open handles independently keep an inode live.
Linux entry and allocated open/create replies reuse their original attributes
when the VFS observation captured before the read remains valid after reply
liveness and access validation. Per-inode retirement stamps detect local changes
before native notification delivery; parent-only changes preserve a created
child's snapshot. Matching cached snapshots retain their source deadline.
Retired, expired, or uncached observations, reset boundaries, truncating opens, and
synthetic high-range inodes use the local getattr refresh. Pending lookup replies
retain post-reply DATA invalidation when a notification crosses the reservation.
Fresh-create handle allocation can itself retire the original snapshot.
`mkdir`, `mknod`, and `symlink` do not admit their returned attributes to the
attribute read cache, so their uncached snapshots conservatively refresh.
Missing or expired source deadlines skip reuse validation and go directly to
the normal local getattr access and freshness checks.
Directory replies intentionally leave libfuse3's `cache_readdir` disabled:
remote namespace changes currently arrive through inode invalidation rather than
individual dirent notifications, so caching a kernel-side directory snapshot
could hide a peer-created entry. This does not disable the separate regular-file
`keep_cache` decisions described above.
Callback dispatch runs through libfuse3's multithreaded low-level loop while
the connection init callback caps the kernel background queue
(`max_background=128`, `congestion_threshold=96`) so heavy callback workloads
apply backpressure instead of growing unbounded request memory. The Linux
bridge request can override those queue bounds before mounting. StudioFS uses
the libfuse3 `fuse_loop_config` multithreaded loop API:
libfuse 3.12+ builds use the opaque config setters with `clone_fd=0` and a
worker-pool limit of `min(max(16, 2 * available_parallelism), max_background, 128)`.
The 16-worker floor keeps small hosts from serializing blocking I/O callbacks;
larger hosts keep the previous `2 * available_parallelism` count.
Set `STUDIOFS_LINUX_FUSE_WORKER_THREADS` to a positive integer and remount
to override it; bridge callers can set `LinuxFuseConnectionConfig.worker_threads`,
which takes precedence over the environment. Overrides are capped by
`max_background` and an absolute ceiling of 128 workers. Invalid environment
values fail before mounting. Workers grow on demand; idle workers remain
available for later bursts. Startup tracing records requested/selected workers,
queue bounds, and `hard_worker_limit`; the connection-init event also records
effective queue and congestion values. "Effective" is the value libfuse sends
in the init reply; the kernel may still lower it for unprivileged mounts
(`max_user_bgreq`), which userspace cannot observe.

Older supported 3.x headers keep the public pre-3.12 `struct fuse_loop_config`
fallback with `clone_fd=0` and the previous CPU-based idle retention
(`min(2 * available_parallelism, max_background)`). That API bounds idle
retention, **not active concurrency**. Explicit worker overrides fail before
mounting with an unsupported error on those builds; the startup diagnostic
reports `hard_worker_limit=false` for the default. Use libfuse 3.12+ for an
enforced callback-worker cap. The kernel background queue remains configured
on both paths.

First validated target: Ubuntu 24.04+. Required release target: Rocky Linux
9.4+ (system `fuse3-devel` is currently 3.10.x, so it uses the pre-3.12
fallback). Rocky Linux 8.6+ remains best-effort.

### Requirements

- Linux: libfuse3 development/runtime packages and `fusermount3`
- macOS: [macFUSE](https://osxfuse.github.io/) or [FUSE-T](https://www.fuse-t.org/)

### Mount Options

| Option | Value |
|--------|-------|
| Linux `fsname` | volume name |
| Linux `subtype` | `"studiofs"` |
| Linux `auto_unmount` | Yes |
| Linux cache policy | `adaptive`; `direct` rollback; explicit `kernel` mode |
| Linux requested `max_read` / `max_readahead` | up to 4 MiB; negotiated values are recorded |
| macOS `fsname` | `"studiofs"` |
| macOS `auto_unmount` | Yes |
| `allow_other` | macOS: yes |
| Kernel `max_write` | macOS: `131072` bytes by default |
| Kernel `max_read` | macOS mount option: `4194304` bytes |
| Kernel `max_readahead` | macOS: `1048576` bytes by default |
| Kernel `max_background` | macOS: `50` by default |
| Kernel `congestion_threshold` | macOS: `40` by default |
| macOS: `volname` | `"StudioFS"` |
| macOS: `local` | Yes |
| macOS: cache policy | UBC for fitting read-only files with a clean content generation; direct I/O for write-capable, oversized, or locally dirty/unknown read-only handles |
| macOS: validated macFUSE | 5.2.0 |

### macOS cache and I/O policy

The legacy bridge disables vnode name caching with `novncache` and does not
opt into `negative_vncache`. Entry notifications are therefore disabled at the
legacy transport boundary because the mount options disable name caching.
On macFUSE 5.4.0, even notifications for names already resolved by lookup and
readdir return `ENOENT` with this policy, and MFMount logs each failed send.
Inode data/attribute notifications remain enabled. StudioFS-local dentry/cache
retirement and namespace/ACL epochs are unchanged, and fresh directory handles
still enumerate the current local view. The experimental FSKit notification
path is unchanged. Changing the legacy name-cache policy requires revalidating
entry notification behavior and mounted namespace convergence.

macOS FUSE keeps the Unified Buffer Cache and kernel readahead enabled for
read-only files with known attributes that fit in memory available for file
caching. `direct_io` is forced by unknown attributes, by files larger than that
estimate, and by a missing fresh memory sample. This avoids cycling the same
bytes through both StudioFS's local payload cache and UBC. `direct_io` does not
bypass the StudioFS cache: the VFS still reads cached payloads locally and
fetches from object storage only on a payload-cache miss. Memory-driven
`direct_io` keeps the handle's OS-cache participation and `keep_cache` decision,
so it does not clear the observed generation that other handles may reuse.

Read-only handles also use `direct_io` unless the VFS cache decision observed a
content generation: a first observation, a changed generation, or a stable
generation. An observed generation covers both published content and locally
stable content. Locally dirty, actively written, and unknown-state files bypass
UBC and kernel readahead until their state settles, because the kernel's cached
size and pages cannot track unpublished local state. The file-size and
available-memory check still applies to every read-only handle.

The estimate is `free + speculative + inactive` pages from
`host_statistics64`. Unlike `sysinfo::available_memory`, it does not subtract
compressor pages. That subtraction saturates to zero on ordinary Macs with an
active compressor and would disable UBC for every file. Each adapter owns a
sampler that publishes one measurement synchronously before the mount serves
callbacks. A background worker then refreshes it once per second. Publication
is lock-free: two atomics written bytes-then-timestamp with release/acquire
ordering. Opens never wait for the worker. Missing or zero samples, and samples
at least two seconds old, select direct I/O. Sample age starts before the OS
call, so a slow refresh cannot make an old measurement fresh. The one-second
cadence accounts for time spent sampling; an overrun starts the next sample
immediately and skips missed ticks without a catch-up burst. A panicking or
failing measurement is contained and logged once per health transition. The
`macfuse_open_cache` counters `read_only_memory_unavailable_direct_io` and
`read_only_exceeds_memory_direct_io` make memory-driven `direct_io` visible.
Teardown drains callbacks, then `CallbackState::stop_background_work` stops and
joins the sampler; dropping the adapter also joins it.

`novncache` keeps namespace lookups coherent. The mount does not pass the
global `direct_io`, `nolocalcaches`, or `auto_cache` options; data and attribute
coherence instead use per-handle `keep_cache` decisions plus scoped low-level
inode invalidations. The mount also passes `iosize=4194304`, `max_read=4194304`,
and `default_permissions`; the 4 MiB geometry stays aligned with regular-file
`st_blksize` so macFUSE does not split
large sequential media callbacks into smaller requests. StudioFS does not pass
`noappledouble`: macFUSE handles that option before userspace and can hide
`.DS_Store`, which Finder/copyfile requires for folder copies.
Write-capable handles use per-handle `direct_io`: macFUSE's buffered Apple
copy path can otherwise replace valid large-write buffers before the low-level
write callback. Read-only handles retain the adaptive UBC policy above.

A read-only handle that gets `direct_io` has no UBC or kernel read-ahead.
An `F_NOCACHE` reader such as AJA System Test or NLE playback bypasses UBC
even on a cached handle and issues callbacks one at a time. The adapter cannot
observe that per-descriptor hint, so it opts all read-only handles into VFS
sequential RAM read-ahead (`StudioVfs::enable_sequential_ram_readahead`).
After two forward reads whose ranges touch or overlap and extend the stream,
the VFS loads 4 MiB-aligned chunks ahead in background tasks. Its idle lead is
`sequential_ram_readahead_bytes` (default 32 MiB on macOS and Windows,
`STUDIOFS_SEQUENTIAL_RAM_READAHEAD_BYTES`, `0` disables it). Both read-ahead
environment variables accept byte counts or size suffixes such as `8MB`, with
surrounding whitespace allowed. Nonzero configured depths round up to 4 MiB
chunks and have a minimum lead of two chunks (8 MiB). These depths bound the
per-stream lead; the 256 MiB mount-wide budget limits total chunk memory, so
the actual lead can be smaller near EOF or under budget pressure. A callback
whose range is covered by loaded chunks is served by a copy; it waits for an
in-flight chunk before it takes its own file-session `Read` permit, so it
cannot deadlock against a queued writer. A chunk task
reads through the normal foreground read path under a `Read` permit and
installs its bytes only if no invalidation of the inode happened since it was
scheduled; any invalidation, seek, or switch to kernel read-ahead discards all
chunks of the handle. Chunks ending at or before the next callback's start
are released, while its overlapping tail remains available for reuse.
A lead never ends inside the next request: when a callback is larger than the
configured depth, the lead extends to the request size plus one chunk.
WinFSP opts in read-only handles that run without the cache manager
(`FILE_FLAG_NO_BUFFERING`, as AJA System Test opens files, or the
conservative cache policy); cached WinFSP handles keep cache-manager
read-ahead. Write-capable handles and the Linux bridge do not use RAM read-ahead.

When pending/active uploads, deferred materialization/release, or dirty local
payload objects compete for the cache disk, read-ahead uses
`sequential_ram_readahead_shared_bytes` (default 12 MiB on macOS and Windows,
`STUDIOFS_SEQUENTIAL_RAM_READAHEAD_SHARED_BYTES`). It holds this shared depth
for two seconds after the last busy observation. Shared depth is capped at
the idle depth; zero disables new speculative loads while busy. As with idle
depth, the configured shared depth rounds up to 4 MiB chunks with a minimum of
two chunks when nonzero. Reducing depth discards queued slots beyond the new
lead.

Read-ahead keeps several reads queued on the cache disk, and the deeper the
queue, the more of the disk bandwidth a sequential stream takes from write-back
of a just-closed writer's backlog and from upload rereads. On the reference
USB disk (976 MB/s raw), a 16 GB `F_NOCACHE` read while an upload ran measured:

| Depth | Read MB/s | Upload MB/s |
|---|---:|---:|
| off | 560 | 86 |
| 8 MiB | 669 | 106 |
| 12 MiB (shared default) | 863 | 58 |
| 16 MiB | 929 | 39 |
| 32 MiB | 927 | 19 |

Read right after the write, 12 MiB read at 823-837 MB/s and delayed the
upload start from about 6 s to 9-13 s after the writer closed; 32 MiB read at
1151 MB/s but deferred the upload until the read ended.
One chunk of depth is not useful: requests that straddle chunk boundaries
miss, so the minimum is two chunks.

The macFUSE read-reply buffer pool lends storage per request. A buffer is reused
only for a request in the same power-of-two size class and only when it is large
enough. A miss allocates the whole class, capped at 8 MiB; the reply is limited
to the requested length. Requests above 8 MiB get an exact-size buffer that is
never retained. Idle storage is capped at 32 buffers and 32 MiB of capacity.
Under count or byte pressure the oldest idle buffer is evicted first, so the
buffer just returned survives and the pool converges on the current working set.
Evicted buffers are freed after the pool lock is released. The hit and miss
counters and the retained-byte gauge are built and stored as one snapshot under
an adapter mutex, so the exported counters never move backwards.

The native macOS bridge queues read replies asynchronously. Write callbacks are
currently acknowledged after the shared VFS accepts the write, while the VFS
materializes and uploads accepted data in the background. The shared writer
keeps a 200 ms coalescing window for synchronous small callbacks; `fsync`
wakes that materializer and drains the accepted tail immediately instead of
waiting through the full idle window. Ordinary flush and last-handle release
remain volatile. The
bridge applies the `50/40` background queue policy, keeps macFUSE-safe
`max_write=131072`, and
reports the preferred I/O size through regular file attributes.
macFUSE `flush` and final `release` admit state to the shared volatile metadata
worker; `fsync` enters the shared strict metadata sync boundary. The adapter retains
errno translation only and owns no durability timer, queue drain, or redb
transaction.
On macOS, StudioFS handles `getxattr` / `setxattr` / `listxattr` /
`removexattr` natively (including `com.apple.ResourceFork`) so Finder metadata
and resource-fork writes stay in xattrs rather than relying on AppleDouble
fallback.

### Trash and recycle-bin behavior

Desktop delete may arrive at the filesystem as a rename into a trash directory
instead of a direct `unlink`; common forms include Freedesktop-style
`.Trash-<uid>/files`, `.Trash/<uid>/files`, macOS `.Trashes`, and Windows
`$RECYCLE.BIN`.

StudioFS does not provide recoverable trash/recycle-bin semantics. Renames into
trash-like roots are treated as immediate deletes: the source file or directory
subtree is removed through the ordinary delete path, no trash destination entry
is retained, and local payload cache cleanup follows normal unlink behavior.
Existing trash-like roots are hidden from root directory listings so they are
not exposed as user-visible folders in mounted trees.

### macOS metadata file filtering

Finder and related macOS services may also try to create metadata sidecar files
such as AppleDouble `._*` and root-level system metadata names like
`.Spotlight-V100`, and `.fseventsd`. StudioFS stores `.DS_Store` as an ordinary
small file because Finder and `copyfile` require it to survive post-close
metadata operations during folder copies.

StudioFS drops these names at the VFS layer:
- `create` / `open` flows succeed locally so Finder does not surface errors
- ignored files stay ephemeral to the mounting client only
- root metadata names are absorbed into ephemeral ignored entries; `mkdir`
  yields an ephemeral directory inode and file/symlink-style creates are also
  kept local-only
- any nested activity underneath an ignored metadata directory is blackholed
- no inode, xattr, cache, or object-store state is persisted for these names
- these root metadata directories are filtered from `readdir`

StudioFS also enforces a size limit on xattr values to avoid unbounded metadata
in-memory pressure from very large native `com.apple.*` writes:
- `com.apple.ResourceFork` and all xattrs are validated against `STUDIOFS_XATTR_MAX_BYTES`
- writes larger than the configured limit fail predictably (`xattr` set error) without partial persistence
- default limit is `4MB`; raise or lower it with environment configuration if needed
- `com.apple.ResourceFork` values are stored with a resource-fork-aware metadata codec:
  compress-on-benefit (RLE + frame), otherwise plain bytes
- callers still observe plain bytes on `getxattr`; existing plain persisted values remain readable

This keeps shared directories free of macOS-only garbage while preserving normal
Finder interaction.

### macOS file flags

Darwin `st_flags` (`chflags`, Finder hidden state) are a mount-local overlay in
the macFUSE adapter, not persisted metadata. `.DS_Store` gets `UF_HIDDEN` on
lookup/create/rename. `setattr`, `setattr_x`, and FSKit `SETATTR` record the
requested flags only after the VFS update (or `getattr` for flag-only requests)
succeeds; a request without the FLAGS bit, including an empty mask, leaves the
overlay unchanged. Flag changes return `EROFS` on read-only mounts. Overlays
are dropped on final `unlink`, `rmdir`, rename overwrite, and last `forget`,
and are never recorded for open-unlinked inodes.

### macOS unsupported ioctls

During mount/unmount lifecycle macOS may probe some filesystem control operations
that are not implemented by StudioFS. These `ioctl` requests are answered with
`ENOTTY` and do not indicate filesystem corruption; they are handled as explicit,
expected no-op control-plane responses.

### macOS Unicode normalization (NFC/NFD)

StudioFS keeps namespace bytes unchanged in the shared VFS/metadata layers, but
applies Unicode normalization handling at the macOS FUSE boundary to match
Finder/macFUSE expectations.

Current policy:
- `readdir` / `readdirplus` return names in decomposed form (NFD) for Finder compatibility
- `lookup` and `rename` resolve names by canonical Unicode equivalence (NFD key),
  so NFC/NFD variants of the same visible name map to the same directory entry when unique
- when multiple byte-distinct entries would normalize to the same key, StudioFS
  does not guess; it falls back to byte-exact behavior
- Windows and Linux behavior is unchanged; macOS applies the normalization
  compatibility layer at the FUSE boundary

This scope keeps macOS interoperability predictable without broad
cross-platform name-semantics changes.

## Platform Differences

| Feature | Windows (WinFSP) | macOS (macFUSE) |
|---------|-------------------|---------------------|
| Mount point | Drive letter (`Z:`) only | Directory |
| Case sensitivity | Configurable (default: sensitive) | Always sensitive |
| Windows attrs | `win_attrs` field in `InodeAttr` | Ignored |
| Hardlinks | Limited by NTFS | Full support |
| Symlinks | Supported via reparse points | Full support |
| Extended attrs | Via `xattrs` table | Via FUSE xattr callbacks |
| File locking | Via WinFSP mechanisms | `flock()` support |

`mounted-symlink-attribute-convergence` validates only behavior exposed through
the mounted filesystem. It treats symlink creation and read-only file attributes
as capability-gated operations: unsupported, permission-denied, or WinFSP
`ERROR_GEN_FAILURE` symlink-create responses after the bounded mount-readiness
retry are recorded as explicit scenario skips, while supported operations must
converge through public mountpoint reads and metadata calls.

### POSIX caller permissions

Linux libfuse3, legacy macFUSE and the experimental FSKit mount use
`default_permissions`. The native kernel selects owner, primary or
supplementary group, or other mode bits using the caller's credentials and
the attributes returned by StudioFS. Directory search requires execute
permission; owner/group checks do not fall through to a more permissive
class. Root and capability behavior follow the native platform. Existing
legacy uid/gid zero translation to the mount user remains in effect.
FSKit rejects unexpected userspace access requests with `EACCES`.

Native acceptance is a shared 17-row access(2) table, run as a non-root
caller with at least one supplementary group:

    cargo test -p studiofs-macfuse --test posix_access -- --ignored --nocapture --test-threads=1
    cargo test -p studiofs-linux-fuse --test posix_access -- --ignored --nocapture --test-threads=1

Run each command on its native platform with the FUSE driver available.
An ignored or zero-test result does not prove the permission contract.

Read-only Linux sessions additionally pass `ro` to libfuse. Legacy macFUSE
passes `rdonly` to the mount channel; mount-only options must not be passed
to its low-level session parser. A separate native test requires `R_OK` and
`F_OK` to succeed and `W_OK` to fail with `EROFS` on a writable inode.
The shared test uses unique directories under the configured workspace target
root and preserves the initial assertion failure after joining the mount worker.

The MFMount path has an opt-in native permission test:

    cargo test -p studiofs-macfuse --features fskit-mount-sanity --test posix_access mounted_mfmount_access -- --ignored --nocapture --test-threads=1

This check requires successful native MFMount/provider startup; a startup
failure is not evidence about permission decisions.

MFMount/experimental FSKit receives the same read-only policy through
`rdonly` in its mount options. Native MFMount acceptance remains gated on
successful provider/channel startup; option tests alone do not prove access(2).
