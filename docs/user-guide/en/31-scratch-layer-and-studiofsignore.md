# 31. Local Scratch Layer and `.studiofsignore`

DCC applications and operating systems constantly write temporary files: autosaves, `.tmp` staging files, `.blend1` backups, thumbnail caches, render scratch directories, and macOS AppleDouble (`._*`) sidecars. Uploading and replicating those files across PostgreSQL and S3 wastes bandwidth and object-store space for data no other workstation needs.

StudioFS solves this with a **local-only scratch layer** driven by `.studiofsignore` files.

## How the scratch layer works

- **Shared rules, local data**: `.studiofsignore` files live at the volume root or in any directory. They are ordinary tracked files and replicate to every client, while the files they match stay strictly local to the workstation that created them.
- **Gitignore syntax**: Rules follow `.gitignore` semantics — `#` comments, `!` negation, trailing `/` for directories, leading `/` anchoring, and `**` globs. Deeper `.studiofsignore` files override shallower ones.
- **Built-in `._*` rule**: macOS AppleDouble `._*` sidecars are treated as scratch by default at lowest precedence. Add `!._*` if you ever need to track them.
- **Rule files are always tracked**: `.studiofsignore` itself is never ignored. If a pattern would match `.studiofsignore`, StudioFS ignores that pattern for the rule file and warns you in `studiofs status`.
- **Sticky classification**: An entry's class (tracked or scratch) is decided when it is created. Adding a rule does not remove existing tracked files (they are reported in `studiofs status`). Removing a rule automatically promotes matching scratch entries whose parent is tracked.
- **Atomic saves without copying**: Renaming a scratch file (such as `shot.exr.tmp`) to a tracked name (`shot.exr`) promotes it in place without copying its payload.
- **Crash-safe and excluded from snapshots**: Scratch data survives unmounts, remounts, and crashes on the creating workstation, is treated as live by local payload GC, never counts as dirty or pending upload, and is excluded from volume snapshots and S3 `janitor`.

## Sample `.studiofsignore` for DCC workflows

Place this file at the root of your StudioFS volume (or customize per-show / per-department subdirectories):

```gitignore
# ==============================================================================
# StudioFS sample .studiofsignore for DCC pipelines
# ==============================================================================

# --- General OS & temporary files ---
*.tmp
*.temp
*.bak
*.swp
*.swo
*~
.DS_Store
Thumbs.db
desktop.ini

# --- Autodesk Maya ---
.mayaSwatches/
*.ma.swatches
*.mb.swatches
incrementalSave/
autosave/
*.autosave
*_tmp.ma
*_tmp.mb
mayaRenderLog.txt

# --- Blender ---
*.blend1
*.blend2
*.blend3
*.blend@
*_autosave.blend
/tmp/

# --- SideFX Houdini ---
backup/
*.hip.bak
*.hipnc.bak
*.hiplc.bak
*_bak*.hip
*_bak*.hipnc
*_bak*.hiplc
tex_cache/
sim_scratch/
*.bgeo.sc.tmp

# --- Foundry Nuke ---
*.nk~
*.nk.autosave
.nuke/
diskcache/
localcache/
viewer_cache/

# --- Adobe Photoshop ---
~$*.psd
~$*.psb
*.psd.tmp
*.psb.tmp
Photoshop Temp*
Adobe Photoshop AutoRecover/
```

## Inspecting and managing scratch space

### Check scratch status

`studiofs status <volume>` prints a dedicated **Scratch** section (both when the volume is mounted and when offline):

```bash
studiofs status myvolume
```

It reports:

- **Entries** and **Bytes** stored in local scratch
- **No longer matching rule**: scratch entries eligible for promotion
- **Over promote limit**: large scratch directories that exceed `STUDIOFS_SCRATCH_PROMOTE_ENTRY_LIMIT` (default `100000`), with the exact `studiofs scratch promote` command
- **Conflicts**: scratch entries renamed to `<name>.scratch-conflict-<n>` because a peer created a tracked file with the same name
- **Orphans**: scratch subtrees relocated outside the namespace because a peer or snapshot restore removed their tracked parent directory
- **Tracked matching rule**: existing tracked files that match a current `.studiofsignore` rule
- **Rule file warnings**: patterns in `.studiofsignore` that would match `.studiofsignore` itself

### `studiofs scratch` commands

All `studiofs scratch` commands work both through the control API of a mounted volume and offline directly against the local `redb` database when the volume is unmounted (pass `--volume <name>` for offline mode, and `--cache-dir <dir>` if you use a custom cache directory).

List scratch entries (optionally filtered to a path):

```bash
studiofs scratch list --volume myvolume
studiofs scratch list --volume myvolume renders/
```

List relocated orphaned scratch subtrees:

```bash
studiofs scratch list --volume myvolume --orphaned
```

Clean old or unwanted scratch entries (automatically skips files with open handles on mounted volumes):

```bash
studiofs scratch clean --volume myvolume --older-than 7d
studiofs scratch clean --volume myvolume --older-than 24h renders/
```

Manually promote a scratch file or directory into tracked storage (bypasses the background auto-promote entry limit):

```bash
studiofs scratch promote --volume myvolume -- renders/big_cache
studiofs scratch promote /mnt/studio/renders/shot.exr.tmp
```

Restore an orphaned scratch entry or subtree back into the scratch layer (using the `<timestamp>/<old_path>` ID from `list --orphaned`):

```bash
studiofs scratch restore --volume myvolume 1700000000000/renders/autosave.tmp
studiofs scratch restore --volume myvolume --to recovered/autosave.tmp 1700000000000/renders/autosave.tmp
```

### Consistency checks with `studiofs fsck`

`studiofs fsck <volume>` also verifies scratch table consistency — checking for orphaned scratch metadata rows, local scratch payloads without a metadata entry, and scratch entries whose local payload is missing:

```bash
studiofs fsck myvolume
studiofs fsck myvolume --repair
```
