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
| Sector size | 512 bytes |
| Allocation unit size | 16384 bytes |
| Max component length | 255 |
| File info timeout | 1000 ms |
| Dir info timeout | 1000 ms |
| Volume info timeout | 1000 ms |
| Case sensitive | Yes |
| Unicode on disk | Yes |
| Persistent ACLs | Yes |
| Reparse points | Yes |
| Volume serial | `0x5354_4644` ("STFD") |
| Filesystem name | "StudioFS" |

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
- `open_flags` — VFS open flags derived from WinFSP `CreateOptions`
- `dir_buffer` — WinFSP directory buffer for readdir

**Key mappings:**

| WinFSP Callback | VFS Method |
|-----------------|------------|
| `get_volume_info` | `statfs()` |
| `get_security_by_name` | `NamespaceResolver::resolve_path()` |
| `open` | `NamespaceResolver::resolve_path()` + `open()` (records `DIRECT_IO` / `WRITE_THROUGH` intent) |
| `close` | `release()` |
| `read` | `read_into_fh()` (per-handle unbuffered path when requested) |
| `write` | `write_fh()` (unbuffered backend drain deferred to flush/close) |
| `cleanup` | `flush()` |
| `flush` | `flush()` |
| `get_file_info` | `getattr()` |
| `set_basic_info` | `setattr()` |
| `set_file_size` | `setattr()` (truncate) |
| `read_directory` | `readdir()` |
| `create` | `NamespaceResolver::resolve_parent()` + `create_file()` / `mkdir()` |
| `rename` | `NamespaceResolver::resolve_parent()` + `rename()` |
| `set_delete` | _(mark for delete)_ |
| `cleanup` + delete flag | `unlink()` / `rmdir()` |

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
mount_and_serve(vfs, rt, mount_point, volume_label, foreground, read_only, direct_io, shutdown_event_name, profiling)
  │
  ├─ Configure VolumeParams
  ├─ Create FileSystemHost
  ├─ Mount filesystem
  ├─ Block on Ctrl+C signal
  │
  └─ Shutdown:
       ├─ host.stop()
       ├─ host.unmount()
       └─ vfs.shutdown() — drain uploads, persist checkpoint
```

Current signature includes additional policy controls:
- `read_only` toggles read-only mount behavior.
- `direct_io` enables per-handle unbuffered I/O path on Windows (write-through drain moved to flush/close).
- `shutdown_event_name` allows external graceful shutdown signal integration.

## FUSE Bridge (`studiofs-fuse`)

### Requirements

- Linux: `libfuse3-dev` (or `fuse3`)
- macOS: [macFUSE](https://osxfuse.github.io/) or [FUSE-T](https://www.fuse-t.org/)

### Mount Options

| Option | Value |
|--------|-------|
| `fsname` | `"studiofs"` |
| `auto_unmount` | Yes |
| `allow_other` | Yes |
| macOS: `volname` | `"StudioFS"` |
| macOS: `local` | Yes |

On macOS, StudioFS handles `getxattr` / `setxattr` / `listxattr` /
`removexattr` natively (including `com.apple.ResourceFork`) so Finder metadata
and resource-fork writes stay in xattrs rather than relying on AppleDouble
fallback.

### macOS Trash behavior

On macOS, Finder-style delete may arrive at the filesystem as a rename into a
Trash directory (`.Trash`, `.Trashes`, `.Trash-<uid>`) instead of a direct
`unlink`.

StudioFS mounts the volume as a local drive (via `local`) and allows
`.Trashes` to be materialized as a real directory. Finder then performs native
Trash semantics, so deletes go through ordinary rename/restore flow instead of
`unlink` fallback:
- the file remains available in Trash
- inode metadata is preserved
- local L1/L2 cache blocks for the file are evicted promptly to release disk
  space

This keeps Trash recovery semantics intact while preventing local cache growth
from Finder-driven deletes.

### macOS metadata file filtering

Finder and related macOS services may also try to create metadata sidecar files
such as `.DS_Store`, AppleDouble `._*`, and root-level system metadata names like
`.Spotlight-V100`, and `.fseventsd`.

StudioFS drops these names at the VFS layer:
- `create` / `open` flows succeed locally so Finder does not surface errors
- ignored files stay ephemeral to the mounting client only
- root metadata names are absorbed into ephemeral ignored entries; `mkdir`
  yields an ephemeral directory inode and file/symlink-style creates are also
  kept local-only
- any nested activity underneath an ignored metadata directory is blackholed
- no inode, xattr, cache, or object-store state is persisted for these names
- these root metadata directories are filtered from `readdir`, even if a legacy
  metadata entry already exists

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
| `read` | `read()` |
| `write` | `write()` |
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
mount_and_serve(vfs, rt, mount_point, foreground)
  │
  ├─ Build mount options
  ├─ `fuser::Session::new(...).run()` — blocks until unmount
  │
  └─ Shutdown:
       ├─ Drain pending uploads
       └─ Persist cache checkpoint
```

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
