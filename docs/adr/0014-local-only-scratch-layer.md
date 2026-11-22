# Paths Matched By `.studiofsignore` Live In A Local-Only Scratch Layer

Status: proposed (SFS-1068). Implementation is tracked in the Linear project
"[0.2.4] Scratch layer".

## Context

DCC applications and operating systems continuously create temporary files:
`*.autosave`, `*.tmp`, `*.blend1`, `.mayaSwatches/`, `ShaderCache/`, render
scratch directories, and AppleDouble `._*` sidecars. Every name outside the
hardcoded macOS and trash set goes through the full local-first pipeline:
`event_log` publication to PostgreSQL, S3 upload, and peer invalidation. That
adds churn and object-store cost for data no peer needs.

The only existing exclusion is `IgnoredEntryRegistry`
(`crates/studiofs-vfs/src/vfs/ignored_entries.rs`). It is an in-memory
metadata-only blackhole: writes only grow `attr.size`, reads return zeros, and
entries are detached after the last handle closes. It cannot keep autosave data.
Its rename handling also loses data silently: normal -> ignored unlinks the
source, and ignored -> normal unlinks the source without creating the target.

## Decision

### Rules

- Rules come from `.studiofsignore` files at the volume root and in any
  directory. They are ordinary tracked files and replicate like any file, so a
  studio shares one rule set.
- Syntax and precedence follow `.gitignore` exactly: `!` negation, trailing `/`
  for directories, leading `/` anchoring, `**`, deeper files override shallower
  ones, and a file cannot be re-included when a parent directory is excluded.
  Matching follows the volume's case policy (the folding used by
  `dentries_by_folded_name`).
- `._*` is a built-in, lowest-precedence scratch rule; `!._*` overrides it.
  `.Spotlight-V100`, `.fseventsd`, and trash components stay in the existing
  discard class.
- `.studiofsignore` files are always tracked. Patterns that would match them do
  not apply to them, and `status` warns about such patterns.
- Rules load in the background: after a local close or flush of a
  `.studiofsignore`, and after foreign apply of one. `.studiofsignore` payloads
  are pinned and limited to 64 KiB. Foreground operations use the last compiled
  rule set and never wait for S3 or PostgreSQL. Compiled rules persist in a
  local-only redb table, so a remount classifies immediately. The first mount
  of a volume on a client prefetches all `.studiofsignore` payloads during
  bootstrap with a bounded timeout; on timeout the mount proceeds and logs a
  warning.

### Classification

- An entry's class (tracked or scratch) is decided when it is created and is
  stored with the entry. Adding a rule never changes existing tracked entries,
  as in git; `status` reports tracked entries that match a rule.
- The whole subtree of a scratch directory is scratch.
- Removing a rule promotes scratch entries that no longer match and whose
  parent is tracked. A bounded, rate-limited background task does this; it is
  joined at shutdown and never blocks foreground callbacks. A directory that
  exceeds the promote limit stays scratch and is reported by `status`.

### Storage

- Scratch metadata lives in separate redb tables (`scratch_inodes`,
  `scratch_dentries`, `scratch_overlay`, `scratch_xattrs`). The publication path
  never reads them, so zero `event_log` rows is a structural guarantee.
- Scratch inode IDs come from the normal namespace allocator, so promotion
  keeps the inode number and open handles stay valid.
- Scratch payload goes through the normal writer stack into the local payload
  store with the same object format and object IDs, but is never submitted for
  upload. Disk-pressure eviction never removes it. Scratch data survives
  unmount, remount, and crashes on the creating client.
- There is no scratch quota and scratch does not count toward
  `logical_quota_bytes`. A scratch write fails with `ENOSPC` when the cached
  physical-space sample shows available space below the existing 10% reserve
  (`disk_pressure.rs`). Tracked writes keep their current contract.

### Namespace operations

- On the creating client, scratch entries behave like ordinary files: lookup,
  readdir, getattr, setattr, xattrs, symlinks, read, and write all work. Peers
  never see them. The read-only xattr `user.studiofs.scratch` marks them.
- Unlink of a scratch entry emits no event; its payload objects are freed after
  the last handle closes.
- `rmdir` of a tracked directory whose only children are scratch fails with
  `ENOTEMPTY`, consistent with the local `readdir`.
- **Promote** (rename scratch -> tracked, e.g. `shot.exr.tmp` -> `shot.exr`):
  unfrozen writer pages are frozen first, then one redb transaction moves the
  rows into tracked tables, appends create and rename/replace events, and marks
  the objects pending upload. Uploads are submitted after the commit. No payload
  is copied. Lock order is LocalDirty -> Namespace -> Upload. A directory
  promotes with its subtree in one transaction; above a configurable entry
  limit the rename fails with `EXDEV`. Set
  `STUDIOFS_SCRATCH_PROMOTE_ENTRY_LIMIT` on the mounting process to override
  the default of 100,000 entries, including the directory itself.
  Explicit promotion follows ordinary rename collision and replacement semantics.
- A tracked entry renamed to a matching name in a tracked parent stays tracked
  (a normal published rename).
- **Demote** (moving a tracked entry into a scratch directory): peers receive a
  removal that retains the old identity, and the entry moves to scratch tables.
  Demote is allowed only when the whole payload (whole subtree for
  directories) is local; otherwise the rename fails with `EXDEV` and the
  application falls back to copy and delete. Pending uploads are cancelled with
  pending identity preserved.
- Hardlinks across classes fail with `EXDEV`, except a link to a matching name
  in a tracked parent, which stays tracked.

### Conflicts and orphans

- When replay inserts a tracked entry under a name a local scratch entry holds,
  the tracked entry wins. The scratch entry is renamed locally to
  `<name>.scratch-conflict-<n>`, with a log warning and a `status` entry.
  A tracked directory meeting a scratch directory merges instead (ADR 0012):
  the scratch children move into the tracked directory and stay scratch,
  nested directories merge the same way, and other colliding children get the
  conflict suffix. Automatic promotion then promotes the children that no
  longer match a rule.
- Concurrent automatic promotion on several clients follows ADR 0012:
  directories merge, files resolve by last writer. Before promoting, a client
  checks its local base. If the name is taken by a tracked file, it renames the
  file to `<name>.scratch-conflict-<client>`; a tracked directory merges as on
  replay. The remaining race equals the ordinary concurrent-create race that
  ADR 0012 accepts.
- A pass runs only after the rules stay unchanged for a short quiet period, so
  an in-place rewrite of `.studiofsignore`, which loads an empty rule set
  between truncate and write, does not promote every scratch entry.
- When foreign apply or snapshot restore removes the tracked parent of scratch
  entries, they move to `<cache>/scratch-orphans/<timestamp>/<old path>`
  outside the namespace. `studiofs scratch list --orphaned` shows them and
  `studiofs scratch restore` brings them back. A peer rename of the parent
  carries scratch children with it.

### Tooling

- `status` reports scratch separately: entries, bytes, entries no longer
  matching a rule, conflicts, orphans, and entries over the promote limit.
  Scratch never counts as dirty or pending upload.
- `fsck` checks scratch table consistency. Local payload GC treats scratch
  objects as live. `janitor` is unaffected. Snapshots exclude scratch.
- The tooling design covers `studiofs scratch list|clean|promote|restore`
  through the mounted control API and offline against redb. The design for
  `clean` skips entries with open handles and supports `--older-than`.
- `studiofs scratch promote --volume <volume> -- <path>` works on a writable
  mount or offline and accepts a UTF-8 path relative to the volume root. It
  bypasses the background entry limit and fails atomically if a tracked entry
  already holds the target name. When the target stopped matching an ignore
  rule, descendants that still match remain scratch.

## Consequences

Temporary DCC files stop generating `event_log` rows, S3 PUTs, and peer
invalidations. Autosave data survives crashes, and atomic-save renames publish
the final file without copying. AppleDouble sidecars keep real data on the
creating client instead of reading back as zeros, and use disk until they are
cleaned.

A rule edit takes effect on each client only after background recompilation.
Classification is local, so clients can briefly disagree; because classes are
sticky, a disagreement affects only the creating client.

Moving a tracked file whose payload is not cached into a scratch directory
returns `EXDEV`, so applications copy instead of renaming.

Rename across the discard boundary (`.Spotlight-V100`, `.fseventsd`, trash
components) is fixed separately; this decision does not cover discard
semantics. Native WinFSP validation is a separate gate.
