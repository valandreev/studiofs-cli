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

Path walks are centralized in VFS `NamespaceResolver`:
- WinFSP uses `NamespacePathStyle::Windows` for full-path and parent resolution.
- macFUSE uses VFS component lookup (`lookup`) which is routed through the same resolver logic.
- Bridge-local namespace traversal should not bypass VFS runtime filtering.

## macOS backend selection

The default macOS backend is the existing low-level macFUSE bridge. Select it
explicitly with `STUDIOFS_MOUNT_BACKEND=legacy` or
`STUDIOFS_MOUNT_BACKEND=macfuse`. The experimental native FSKit transport is
opt-in:

```bash
STUDIOFS_MOUNT_BACKEND=fskit studiofs mount <volume> /Volumes/<name>
```

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
`ERANGE` contract, while potentially waiting `rmdir` and `symlink` callbacks
run as tracked blocking reply tasks instead of occupying the FSKit dispatcher
loop. The zero-copy negotiation and the remaining full semantic parity matrix
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
| Persistent ACLs | Yes |
| Reparse points | Yes |
| Volume serial | `0x5354_4644` ("STFD") |
| Filesystem name | "StudioFS" |

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
| `readdir` | `readdir_with_attrs()` |
| `releasedir` | success reply |
| `getxattr` | `get_xattr()`; missing attributes return `ENODATA` |
| `setxattr` | `set_xattr()` |
| `listxattr` | `list_xattrs()` as NUL-separated names |
| `removexattr` | `remove_xattr()`; missing attributes return `ENODATA` |

Linux ordinary `flush` and final `release` admit state to the shared volatile
metadata worker; the batch age is bounded to one second. An async-write ledger
uses sequence-number watermarks, so a barrier waits for earlier writes to its
own inode or handle, never unrelated inodes or later writes.
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

The bridge installs a bounded, coalescing invalidation worker through libfuse3
low-level inode notifications. Successful entry/create replies and `forget`
maintain lookup liveness, while open handles independently keep an inode live.
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
libfuse 3.12+ builds use the opaque config setters with `clone_fd=0` and an
adaptive worker-pool limit based on available CPU parallelism, capped by the
configured `max_background` queue. Older supported 3.x headers use the public
pre-3.12 `struct fuse_loop_config` fallback with the same `clone_fd=0` and
adaptive `max_idle_threads` cap.

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
| macOS: cache policy | UBC for fitting read-only files; direct I/O for write-capable or oversized read-only handles |
| macOS: validated macFUSE | 5.2.0 |

### macOS cache and I/O policy

macOS FUSE keeps the Unified Buffer Cache and kernel readahead enabled for
read-only files that fit in currently available RAM. When a file is larger than
available RAM, its read-only handle uses `direct_io` to avoid cycling the same
bytes through both StudioFS's local payload cache and UBC. This does not bypass
the StudioFS cache: the VFS still reads cached payloads locally and fetches from
object storage only on a payload-cache miss. `novncache` keeps namespace lookups
coherent. The mount does not pass the global `direct_io`, `nolocalcaches`, or
`auto_cache` options; data and attribute coherence instead use per-handle
`keep_cache` decisions plus scoped low-level inode invalidations. The mount also
passes
`iosize=4194304`, `max_read=4194304`, and `default_permissions`; the 4 MiB
geometry stays aligned with regular-file `st_blksize` so macFUSE does not split
large sequential media callbacks into smaller requests. StudioFS does not pass
`noappledouble`: macFUSE handles that option before userspace and can hide
`.DS_Store`, which Finder/copyfile requires for folder copies.
Write-capable handles use per-handle `direct_io`: macFUSE's buffered Apple
copy path can otherwise replace valid large-write buffers before the low-level
write callback. Read-only handles retain the adaptive UBC policy above.
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
