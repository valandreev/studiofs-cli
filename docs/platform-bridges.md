# Platform Bridges

StudioFS supports two platforms through thin bridge crates that translate OS-specific filesystem callbacks into `StudioVfs` method calls.

## Overview

```
┌──────────────────────┐     ┌──────────────────────┐
│   studiofs-winfsp    │     │   studiofs-fuse       │
│   (Windows only)     │     │   (Linux / macOS)     │
├──────────────────────┤     ├──────────────────────┤
│  winfsp crate (0.12) │     │  fuser crate (0.16)  │
│  FileSystemContext    │     │  Filesystem trait     │
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
- FUSE uses VFS component lookup (`lookup`) which is routed through the same resolver logic.
- Bridge-local namespace traversal should not bypass VFS runtime filtering.

## WinFSP Bridge (`studiofs-winfsp`)

### Requirements

- [WinFSP](https://winfsp.dev/) installed (detected via Windows registry)
- `winfsp-x64.dll` loaded at startup by the CLI

### Mount Configuration

| Parameter | Value |
|-----------|-------|
| Mount type | Local disk device (drive-letter mount, no network prefix) |
| Sector size | `4096` bytes by default. Override: `STUDIOFS_WINFSP_SECTOR_SIZE` |
| Allocation unit size | `1048576` bytes by default. Override: `STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE` |
| Max component length | 255 |
| File info timeout | WinFSP cache-manager mode (`-1`/`max`) by default. Override: `STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS` |
| Dir info timeout | `1000 ms` by default. Override: `STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS` (`-1`/`max` supported) |
| Volume info timeout | `1000 ms` by default. Override: `STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS` (`-1`/`max` supported) |
| Allow open in kernel mode | Enabled by default. Override: `STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE` |
| Flush/purge on cleanup | Disabled by default to preserve read-cache pages across close/reopen cycles. Override: `STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP=1` |
| Case sensitive | Yes |
| Unicode on disk | Yes |
| Persistent ACLs | Yes |
| Reparse points | Yes |
| Volume serial | `0x5354_4644` ("STFD") |
| Filesystem name | "StudioFS" |

`STUDIOFS_WINFSP_SECTOR_SIZE` and `STUDIOFS_WINFSP_ALLOCATION_UNIT_SIZE` are the primary WinFSP bridge throughput geometry controls for local payload I/O.
StudioFS disables WinFSP `FlushAndPurgeOnCleanup` by default so read-only
handles can preserve cache-manager pages across close/reopen cycles.
Write-capable file handles opt out of the WinFSP/Windows file cache at
`Create`/`Open` time, while read-only handles keep the WinFSP cache-manager /
fast-I/O path by default. Regular-file `FileInfo.IndexNumber` includes the
committed content epoch, so remote same-inode overwrites get a new cache-manager
identity without globally disabling read caching. Use a bounded
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
- Explicit `flush(context=Some(..))` freezes/finishes dirty writer slices for
  the handle or inode through the cached-store pipeline.
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
- `close` is the release-path hook for WinFSP. It drains remaining local writer
  state for the last handle, but it must not force synchronous PostgreSQL
  replication, object upload completion, or peer publication. `fsync` remains
  the explicit caller-requested strict sync API.
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

## FUSE Bridge (`studiofs-fuse`)

Linux has two selectable FUSE paths:
- `STUDIOFS_MOUNT_BACKEND=fuser` or unset: legacy `fuser` backend, kept as the fallback/control path.
- `STUDIOFS_MOUNT_BACKEND=raw`: experimental JuiceFS-like raw `/dev/fuse` backend. It opens `/dev/fuse` directly, mounts with `mount(2)`, and owns INIT negotiation without libfuse high-level callbacks.

### Requirements

- Linux: `libfuse3-dev` (or `fuse3`)
- macOS: [macFUSE](https://osxfuse.github.io/) or [FUSE-T](https://www.fuse-t.org/)

### Mount Options

| Option | Value |
|--------|-------|
| `fsname` | `"studiofs"` |
| `auto_unmount` | Yes |
| `allow_other` | macOS: yes. Linux: opt-in via `STUDIOFS_FUSE_ALLOW_OTHER=1` |
| Kernel `max_write` | macOS and Linux conservative profile: `131072` bytes. Linux high-throughput profile: up to `16777216` bytes, clamped by `/proc/sys/fs/fuse/max_pages_limit`. Override: `STUDIOFS_FUSE_MAX_WRITE_BYTES` in `Filesystem::init()` |
| Kernel `max_readahead` | `131072` bytes on Linux, `1048576` bytes on macOS by default. Override: `STUDIOFS_FUSE_MAX_READAHEAD_BYTES` in `Filesystem::init()` |
| Kernel `max_background` | macOS: `50` by default. Linux conservative profile: kernel/fuser default. Linux high-throughput profile: `128`. Override: `STUDIOFS_FUSE_MAX_BACKGROUND` in `Filesystem::init()` |
| Kernel `congestion_threshold` | macOS: `40` by default. Linux conservative profile: kernel/fuser default. Linux high-throughput profile: `96`. Override: `STUDIOFS_FUSE_CONGESTION_THRESHOLD` in `Filesystem::init()` |
| FUSE async write replies | macOS: enabled by default. Linux: enabled by default; disable with `STUDIOFS_FUSE_ASYNC_WRITE=0` |
| FUSE async read replies | Linux: direct reads by default; opt in with `STUDIOFS_FUSE_ASYNC_READ=1` |
| Linux: `writeback_cache` capability | Disabled by default. Opt in with `STUDIOFS_FUSE_WRITEBACK_CACHE=1`; requested only when the kernel advertises `FUSE_WRITEBACK_CACHE` |
| Linux: `async_dio` capability | Disabled by default. Opt in with `STUDIOFS_FUSE_ASYNC_DIO=1`; requested only when the kernel advertises `FUSE_ASYNC_DIO` |
| macOS: `volname` | `"StudioFS"` |
| macOS: `local` | Yes |
| macOS: cache policy | `direct` by default |
| macOS: validated macFUSE | 5.2.0 |

`STUDIOFS_LINUX_FUSE_PROFILE=high-throughput` selects the permanent Linux
high-throughput tuning profile. It requests `max_write=16 MiB`, keeps
`max_readahead` at the Linux default 128 KiB, and sets `max_background=128`
plus `congestion_threshold=96`. This is the Linux default.

`STUDIOFS_LINUX_FUSE_PROFILE=media` selects a macOS-like media profile for
Linux A/B runs. It requests `max_write=4 MiB`, `max_readahead=4 MiB`, enables
async read replies and Linux async DIO, keeps kernel cache policy enabled for
read-only opens, keeps writeback cache disabled, and uses the macOS-style
`50/40` background queue policy.

`STUDIOFS_FUSE_MAX_WRITE_BYTES`, `STUDIOFS_FUSE_MAX_READAHEAD_BYTES`,
`STUDIOFS_FUSE_MAX_BACKGROUND`, and `STUDIOFS_FUSE_CONGESTION_THRESHOLD` are
the primary FUSE bridge throughput controls for local payload I/O. Explicit env
vars override the selected Linux profile. Linux `max_write` is also capped by
the kernel FUSE page limit: `max_pages_limit * page_size`. On typical 4 KiB
pages, `max_pages_limit=256` means a 1 MiB ceiling; use `1024` for 4 MiB if the
stand allows changing `/proc/sys/fs/fuse/max_pages_limit`. Set either
background queue env var to an empty string to leave that value at the
kernel/fuser default; `0` has the same opt-out meaning for these two background
queue controls. StudioFS clamps congestion thresholds above an explicitly
configured `max_background`; when only `STUDIOFS_FUSE_CONGESTION_THRESHOLD` is
set, fuser may still clamp it against its own default `max_background` during
the FUSE init reply.
On Linux, `allow_other` also requires `user_allow_other` to be enabled in
`/etc/fuse.conf`; otherwise `fusermount` rejects the mount request.

### Linux cache and I/O policy

Linux FUSE keeps buffered I/O as the default. Read-only regular file opens
return `FOPEN_KEEP_CACHE`, while write-capable, append, and truncate opens leave
the flag clear so the kernel does not keep potentially stale cached pages across
write-capable handles. Opens do not request `FOPEN_DIRECT_IO`.
Regular file attributes report a 4 MiB preferred I/O size to match the bridge
read/write sizing policy instead of advertising a 512-byte block size.

The VFS invalidator remains responsible for dropping kernel pages after unsafe
transitions such as local writes, truncation, sparse size changes, and remote
metadata invalidation. POSIX `O_SYNC` / `O_DSYNC` write handles map to
`LocalSyncPolicy::WriteThrough`; ordinary writes remain opportunistic.

`STUDIOFS_FUSE_ASYNC_DIO=1` is a Linux-only benchmark/control knob. It requests
`FUSE_ASYNC_DIO` during FUSE init when supported by the kernel, but it does not
turn on global direct I/O and does not change the buffered read cache policy.

`STUDIOFS_FUSE_WRITEBACK_CACHE=1` requests Linux writeback cache for A/B runs;
the default is off.

`STUDIOFS_FUSE_ASYNC_WRITE=0` disables the Linux bridge queue for targeted A/B
runs. The default Linux write callback copies caller bytes into owned buffers,
admits writes on runtime tasks, and keeps staging, barriers, and upload
lifecycle in the shared VFS writer stack.

`STUDIOFS_FUSE_ASYNC_READ=1` enables the Linux bridge read reply path. It moves
regular file reads from the FUSE callback stack onto the mount runtime while
keeping runtime stats file reads synchronous.

### macOS cache and I/O policy

macOS FUSE defaults to the direct cache policy. The mount keeps `local`,
`sparse`, and `volname`, and adds `direct_io`, `nolocalcaches`, and `auto_cache`
so AJA-style media workloads do not fight macOS UBC while StudioFS is already
buffering and materializing writes locally. The mount also passes
`iosize=4194304`, `max_read=4194304`, and `defer_permissions`; the 4 MiB
geometry stays aligned with regular-file `st_blksize` so macFUSE does not split
large sequential media callbacks into smaller requests. StudioFS does not pass
`noappledouble`: macFUSE handles that option before userspace and can hide
`.DS_Store`, which Finder/copyfile requires for folder copies.
The default macOS asyncFUSE profile also enables async write replies, applies
the `50/40` background queue policy, keeps macFUSE-safe `max_write=131072`, and
reports the preferred I/O size through regular file attributes. Set
`STUDIOFS_FUSE_ASYNC_WRITE=0` to restore synchronous write callbacks without
changing the cache policy.
The mounted performance probe records SFS-491 read-geometry evidence in
`performance-probe-summary.json`: `read_observations.fuse_read_calls`,
`read_observations.fuse_read_bytes`, and `read_observations.avg_fuse_read_bytes`
capture observed macFUSE callback sizing, while
`read_observations.foreground_local_payload_reads`,
`read_observations.foreground_local_payload_read_bytes`, and
`read_observations.avg_foreground_local_payload_read_bytes` show whether those
callbacks caused matching local payload reads or extra local read amplification.
When the probe waits for upload drain, the matching
`read_observations.post_drain_fuse_read_*` and
`read_observations.post_drain_foreground_local_payload_read_*` fields record the
same geometry for the post-drain verification read.
Use that report before adding any daemon-side hot read window.

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
- Linux and Windows behavior is unchanged

This scope keeps macOS interoperability predictable without broad
cross-platform name-semantics changes.

### Implementation: `StudioFuseFs`

Implements `fuser::Filesystem` trait.

**Key mappings:**

| FUSE Callback | VFS Method |
|---------------|------------|
| `lookup` | `lookup()` |
| `getattr` | `getattr()` |
| `setattr` | `setattr()` |
| `open` | `open()` |
| `release` | `release()` |
| `read` | `read_into_fh()` |
| `write` | `write_fh_with_deferred_materialize()` |
| `flush` | `flush()` |
| `readdir` | `readdir()` |
| `mkdir` | `mkdir()` |
| `create` | `create_file()` |
| `unlink` | `unlink()` |
| `rmdir` | `rmdir()` |
| `rename` | `rename()` |
| `link` | `link()` |
| `symlink` | `symlink()` |
| `readlink` | `readlink()` |
| `statfs` | `statfs()` |

`unlink()` and cache-only rename cleanup still present a synchronous bridge API,
but the internal payload-object eviction work is batched and routed through the
background local-I/O pool.

FUSE callback barrier semantics:
- `flush` is a handle-backed explicit local-durability barrier and routes through the shared handle-drain barrier path.
- `fsync` is inode-scoped. It persists local unpublished state for the inode, including dirty writer-stack state, but does not force remote publication.
- `release` becomes the fallback strict local crash-safe durability barrier for the last handle when dirty unpublished state remains, but it must not force synchronous PG/S3 replication or upload completion. `fsync` remains the explicit caller-requested strict sync API even though it is no longer the only callback path that reaches local crash-safe durability.
- Size-changing `setattr` / truncate paths commit the new local EOF before the callback returns.

FUSE data-buffer behavior:
- `write`: the kernel-provided `data: &[u8]` slice is forwarded directly to the shared deferred VFS writer-stack path; the bridge does not allocate a second full payload buffer before VFS acceptance.
- `read`: `fuser::ReplyData` exposes `reply.data(&[u8])` rather than a mutable caller buffer, so `StudioFuseFs` must allocate one reply-sized `Vec<u8>` for the callback, fill it through `read_into_fh()`, and pass the populated slice to `ReplyData`. This is the bridge-level exception to direct caller-buffer filling and is limited to the API boundary.

**UID/GID remapping:** root-owned inodes (uid=0) are remapped to the mounting user's UID/GID.

**errno mappings:**
- `InodeNotFound` → `ENOENT`
- `EntryNotFound` → `ENOENT`
- `EntryExists` → `EEXIST`
- `NotADirectory` → `ENOTDIR`
- `DirNotEmpty` → `ENOTEMPTY`
- `IsADirectory` → `EISDIR`
- Other → `EIO`

### Mount Lifecycle

```
mount_and_serve(vfs, rt, mount_point, volume_name, foreground, read_only)
  │
  ├─ Build mount options
  ├─ Configure kernel `max_write` / `max_readahead` and optional background queue policy in `Filesystem::init()`
  ├─ `fuser::Session::new(...).run()` — blocks until unmount
  │
  └─ Shutdown:
       ├─ Attempt bounded drain of pending uploads (`STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS`)
       ├─ Cancel in-flight uploader network I/O (CancellationToken)
       └─ Persist cache checkpoint
```

Signal ownership is explicit. The reusable FUSE library path defaults to
`SignalHandling::Disabled` and does not install process-global `SIGINT` /
`SIGTERM` handlers. Library embedders can pass a `FuseUnmountSignal` in
`FuseMountRequest` and trigger it from their own lifecycle code to request
graceful in-process unmount. The mount lifecycle watcher also remains active
without process signal handlers so critical VFS runtime failures can request an
emergency unmount. The StudioFS CLI opts into `SignalHandling::InstallForCli`
so Ctrl+C/SIGTERM still trigger graceful in-process unmount; that CLI-only path
restores the previously installed signal dispositions during cleanup instead of
resetting them to `SIG_DFL`. Because Unix signal disposition is process-global,
only one active FUSE mount in a process may use `InstallForCli` at a time.
The watcher uses a simple 100 ms polling loop to bridge both process signals
and explicit unmount requests into `fuser::SessionUnmounter`.

## Platform Differences

| Feature | Windows (WinFSP) | Linux/macOS (FUSE) |
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
