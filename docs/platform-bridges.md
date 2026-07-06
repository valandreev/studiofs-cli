# Platform Bridges

StudioFS supports Windows, macOS, and Linux through thin bridge crates that
translate OS-specific filesystem callbacks into `StudioVfs` method calls.

## Overview

```
┌──────────────────────┐     ┌──────────────────────┐
│   studiofs-winfsp    │     │   studiofs-macfuse    │
│   (Windows only)     │     │   (macOS only)        │
├──────────────────────┤     ├──────────────────────┤
│  winfsp crate (0.12) │     │  macFUSE low-level   │
│  FileSystemContext    │     │  callback API        │
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
| `create` | `create()` + `open_created_file_with_known_attr()` |
| `mkdir` | `mkdir()` |
| `mknod` | `create()` for regular files; special files return `ENOTSUP` |
| `unlink` | `unlink()` |
| `rmdir` | `rmdir()` |
| `rename` | `rename()`; non-zero libfuse3 rename flags return `EINVAL` |
| `open` | `open_file_with_known_attr()` |
| `read` | `read_into_fh()` |
| `write` | `write_fh_with_deferred_materialize()` |
| `flush` | `flush_fh()` |
| `fsync` | `flush_fh()` when handle-backed, otherwise `fsync()` |
| `release` | `release_fh()` |
| `statfs` | `statfs()` |
| `access` | `getattr()` + POSIX mode check |
| `opendir` | `getattr()` directory check |
| `readdir` | `readdir_with_attrs()` |
| `releasedir` | success reply |

Linux regular-file opens use `direct_io` by default and keep kernel writeback
caching out of the default path. Symlink/readlink/link, xattrs, kernel
invalidation, and bounded multithreaded callback dispatch remain explicit
follow-up compatibility gaps. Those operations are tracked by subsequent Linux
bridge slices and should not be treated as silent omissions.

First validated target: Ubuntu 24.04+. Required release target: Rocky Linux
9.4+. Rocky Linux 8.6+ remains best-effort.

### Requirements

- Linux: libfuse3 development/runtime packages and `fusermount3`
- macOS: [macFUSE](https://osxfuse.github.io/) or [FUSE-T](https://www.fuse-t.org/)

### Mount Options

| Option | Value |
|--------|-------|
| Linux `fsname` | volume name |
| Linux `subtype` | `"studiofs"` |
| Linux `auto_unmount` | Yes |
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
| macOS: cache policy | `direct` by default |
| macOS: validated macFUSE | 5.2.0 |

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
reports the preferred I/O size through regular file attributes.
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
- Windows behavior is unchanged; Linux has no mounted bridge

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
