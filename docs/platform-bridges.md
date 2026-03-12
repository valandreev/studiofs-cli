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
| Sector size | 512 bytes |
| Max component length | 255 |
| File info timeout | 1000 ms |
| Dir info timeout | 1000 ms |
| Case sensitive | Yes |
| Unicode on disk | Yes |
| Persistent ACLs | Yes |
| Reparse points | Yes |
| Volume serial | `0x5354_4644` ("STFD") |
| Filesystem name | "StudioFS" |

### Implementation: `StudioFsContext`

Implements `winfsp::filesystem::FileSystemContext` trait.

**Per-file context (`StudioFileHandle`):**
- `ino` — inode number
- `is_dir` — directory flag
- `fh` — VFS file handle
- `dir_buffer` — WinFSP directory buffer for readdir

**Key mappings:**

| WinFSP Callback | VFS Method |
|-----------------|------------|
| `get_volume_info` | `statfs()` |
| `get_security_by_name` | `NamespaceResolver::resolve_path()` |
| `open` | `NamespaceResolver::resolve_path()` + `open()` |
| `close` | `release()` |
| `read` | `read()` |
| `write` | `write()` |
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
mount_and_serve(vfs, rt, mount_point, volume_label, foreground)
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
| macOS: `noappledouble` | Suppress `._*` files |
| macOS: `volname` | `"StudioFS"` |

### macOS Trash behavior

On macOS, Finder-style delete may arrive at the filesystem as a rename into a
Trash directory (`.Trash`, `.Trashes`, `.Trash-<uid>`) instead of a direct
`unlink`.

StudioFS handles this as a namespace-preserving trash move plus **local
cache-only eviction**:
- the file remains available in Trash
- inode metadata is preserved
- local L1/L2 cache blocks for the file are evicted promptly to release disk
  space

This keeps Trash recovery semantics intact while preventing local cache growth
from Finder-driven deletes.

### macOS metadata file filtering

Finder and related macOS services may also try to create metadata sidecar files
such as `.DS_Store`, AppleDouble `._*`, and root-level system metadata names like
`.Spotlight-V100`, `.Trashes`, and `.fseventsd`.

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

This keeps shared directories free of macOS-only garbage while preserving normal
Finder interaction.

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
  ├─ fuser::mount2() — blocks until unmount
  │
  └─ Shutdown:
       ├─ Drain pending uploads
       └─ Persist cache checkpoint
```

## Platform Differences

| Feature | Windows (WinFSP) | Linux/macOS (FUSE) |
|---------|-------------------|---------------------|
| Mount point | Drive letter (`Z:`) or directory | Directory |
| Case sensitivity | Configurable (default: sensitive) | Always sensitive |
| Windows attrs | `win_attrs` field in `InodeAttr` | Ignored |
| Hardlinks | Limited by NTFS | Full support |
| Symlinks | Supported via reparse points | Full support |
| Extended attrs | Via `xattrs` table | Via FUSE xattr callbacks |
| File locking | Via WinFSP mechanisms | `flock()` support |
