# 23. Deleting Files: There Is No Magic Trash Can
> **Work in progress**

**By default, a StudioFS volume has no trash.** A new volume is created with trash retention set to 0 days, which means the trash is off. While it is off, “Move to Trash” or “Delete” in a file manager removes the file for good, just like `rm`.

An administrator can turn on a **recoverable trash** for the whole volume by giving it a retention period in whole days. Even then, the trash keeps only items that you explicitly move to the trash with the operating system's own command. It is not a backup, and it does not replace snapshots or independent copies.

## Is the trash on for this volume?

Ask your administrator, or check it yourself on a computer where the volume is discovered:

```bash
studiofs trash-days myvolume
```

The command prints the shared setting. `0` means the trash is off. A positive number is how many days a trashed item is kept.

## Turning the trash on, changing it, turning it off

The setting belongs to the whole volume and is stored in the shared PostgreSQL database, so every computer gets the same value. It can be set when the volume is created:

```bash
studiofs init myvolume ... --trash-days 7
```

or changed later, even while the volume is mounted:

```bash
studiofs trash-days myvolume 7    # keep trashed items for 7 days
studiofs trash-days myvolume 0    # stop keeping new items
```

The volume must be connected to PostgreSQL, and the database must be reachable when you run the command. Other computers pick up the change through normal background synchronization, not at the same moment. If the command reports a timeout, nothing is confirmed: run it again.

**Version requirement.** The trash needs volume format 6, which comes with StudioFS 0.2.3. An existing format-5 volume must first be upgraded on every computer with `studiofs upgrade` (see [chapter 27](27-maintenance-and-removing-a-volume.md)); the upgrade itself does not turn the trash on. Older StudioFS versions refuse to open format-6 metadata, and the shared database blocks them from publishing changes or deleting data. An old client therefore cannot silently ignore or destroy trashed items. Update every computer that uses the volume. This is checked against the StudioFS 0.2.2 metadata decoder; running an old 0.2.2 mount against an upgraded volume has not been tested, so update every computer before turning the trash on.

## What goes to the trash, and what does not

When the trash is on, only an explicit **Move to Trash** made by the operating system is kept:

- Windows: deleting to the Recycle Bin (for example, the Delete key in Explorer);
- Linux: `gio trash` or a file manager's “Move to Trash” that works through GIO/GVFS;
- macOS: Finder “Move to Trash”, but see the macOS limits below.

These operations **bypass the trash and delete permanently**, whatever the setting:

- `rm`, `rm -rf`, `del` and other direct deletions;
- Shift+Delete on Windows and “Delete permanently” in a file manager;
- saving over a file, and a rename that replaces an existing file.

Files, folders, hard links and files that are still open are kept the way the operating system moved them. A hard link that stays outside the trash keeps working, and restoring the trashed name brings back the same file, not a copy with a new identity.

## One trash for the whole volume

There is one trash per volume, shared by Windows, Linux and macOS clients. An item moved to the trash on one computer appears in the trash on the others after it is synchronized. Each operating system shows it in its own native layout. The per-user folder names you may see (for example `.Trash-1000`) only follow the local operating system's conventions; they are not separate private trashes.

## Who can see, restore and clear an item

Access is decided by StudioFS permissions on the item's **original parent folder**, as they are now, not as they were at deletion time:

- You see the item if you can currently access its original parent folder. If that access is removed, the item disappears from your trash view.
- If the original folder was moved, the item follows it and uses its current permissions. If the original folder was deleted or is itself in the trash, the item is hidden until that folder is restored. Creating a new folder with the same name does not give access.
- Restoring requires write access to the original parent folder and to the destination.
- Permanently deleting an item from the trash, including Clear or Empty Trash, requires write access to the original parent folder. Read-only access lets you see an item but not clear it.
- There is no administrator override. Items you cannot see are not cleared by you; they remain until they expire.

These are StudioFS permissions for the principal configured for the mount. All operating-system users who share one mount share that principal, so per-user trash folders do not isolate users from each other.

## Using the trash on each operating system

### Windows

The volume root gets a shared `$RECYCLE.BIN`. Deleted items appear in the Windows Recycle Bin, where you can restore them or delete them permanently.

- Restoring onto a name that already exists never overwrites it silently. If a restore does not finish, the item stays in the trash and you can try again.
- Clearing a trashed folder deletes it item by item, as on a local disk. If you cancel or the process stops halfway, the remaining items stay in the trash and can still be restored.
- If a trashed file's data is not cached on this computer, restore fails at first: Windows shows an interrupted-action message (error 31). StudioFS starts downloading the data in the background. Try the restore again a little later.

**What is proven:** Recycle Bin delete, listing, restore, Clear, partial Clear of a folder, name collision and the remote-only retry were checked through the Windows Shell programming interface on a local test volume. **Not proven:** clicking through Explorer's interface, and a Windows client sharing a volume with other clients through PostgreSQL.

### Linux

Each mount shows `.Trash-<uid>` at the volume root, with `files` (the trashed items) and `info` (a `.trashinfo` record for each item, containing the original path relative to the volume root). Long names may be shortened and duplicate names get a suffix; the original name stays in the `.trashinfo` record.

- Move to trash: `gio trash /path/to/mount/docs/report.txt`.
- List: look in `.Trash-<uid>/files`, or run `gio list trash:///`.
- Restore: `gio trash --restore trash:///…`, or move the item from `files` back to a free place inside the same mount. An occupied destination is refused and neither file changes.
- Delete permanently: `gio remove trash:///…`, delete the item from `files`, or delete its `.trashinfo` record. Deleting a folder child by child is permanent for each deleted child; a partly cleared folder restores only the children that remain.

Be careful with `gio trash --empty` and “Empty Trash” in a file manager: they empty the trash of **every** volume on that computer, not only StudioFS.

**What is proven:** `gio trash`, listing, restore and recursive Clear through the mounted folders, and listing, restore and per-item deletion of files through GVFS `trash:///`. These ran on real FUSE mounts of two StudioFS clients sharing PostgreSQL and S3 (both on one test machine), including a crash while S3 was unavailable, a fresh client with no local data, and payload cleanup by `janitor`. **Not proven:** Nautilus or another graphical file manager, the `gio trash --empty` command itself, a folder through `trash:///`, and GVFS while PostgreSQL or S3 is down.

### macOS

macOS support exists in the code (`.Trashes/<uid>` at the volume root) but has **not** been built or tested on a Mac. Do not rely on the trash on macOS clients yet.

## Working offline

Moving an item to the trash works without a network connection, and so does restoring it if its data is cached on this computer. An item trashed offline stays recoverable on that computer, including after a restart. Other computers see it only after its metadata is synchronized and its data is published.

While computers are offline, two of them can make opposite decisions about the same item. The first decision that reaches the shared database wins the trash entry, but **restored data is never lost**:

- if you restore an item offline while another computer permanently deletes it, or it expires, your restored file stays, with its content uploaded again as new data;
- if you permanently delete an item offline while another computer has already restored it, your deletion is dropped and the restored file stays.

## Retention period, expiry and storage

- A trashed item expires a whole number of days after it was first moved to the trash, using the volume's current period.
- Shortening the period can make existing items expire at once. Lengthening it extends existing items.
- Setting `0` stops keeping new items. Items already in the trash are not cleared; they still expire using the last positive period.
- Expiry runs in the background, roughly once a minute, on computers that have the volume mounted and connected to PostgreSQL. While no such computer is running, nothing expires.
- Trashed items keep using storage until they are cleared or expire. They are never cleared early to free space or because a volume is full.
- Clearing or expiry does not free S3 space immediately: the data is removed later by [`studiofs janitor`](27-maintenance-and-removing-a-volume.md), and data still referenced by a snapshot or a live hard link is kept.

## Known limitations

- `studiofs load` refuses to load into a target database that already holds trash records for that volume, so a failed or repeated load cannot reuse it. Load into a new, empty metadata database.
- If a restore is interrupted, its temporary copy is normally cleaned up later. If this computer's local metadata is then fully rebuilt from the shared database, that hidden copy is not cleaned up and keeps using storage.
- A restored item keeps its access and modification times. Its change time (ctime) and extended attributes are not preserved.
- If a computer crashes while **both** PostgreSQL and S3 are unavailable, the volume will not mount again on that computer until PostgreSQL is reachable. Local data, including trashed items, is kept; StudioFS refuses to serve until it can check the shared state.

## Before cleaning up a project

Whether or not the trash is on, prepare before a large deletion:

1. **Check the volume and full path.** Make sure you have selected the intended folder, not a neighboring project or the only copies of the originals.
2. **Agree on the deletion with your team.** This is a shared file space: the change will eventually reach other computers too. The copy a colleague sees on the same volume is not an independent backup.
3. **Check your data protection.** The snapshot you need must exist before deletion, and the backup must contain the required files and be readable. If the copy depends on changes being published, make sure it includes the latest version you need.
4. **If in doubt, postpone deletion.** With your team's agreement, you can move the material into an ordinary archive folder on the volume. This helps avoid rushing, but that folder still does not replace a backup.

To check trash behavior, use only a separate test volume and disposable test files. Do not use working material for this check.

## Snapshots and backups: prepare in advance

A snapshot records the state of a volume at a particular moment. It may help you return to the state before deletion if it was created in time and the required data remains available. A snapshot created after deletion will not bring back a file that is already missing from the recorded state.

You still need a separate backup: a snapshot is not an independent copy of all the data and does not protect against every storage problem. Before a major cleanup, try restoring the files you need to a safe, separate location. A “copy complete” message alone does not tell you whether the copy is usable.

We will cover preparing snapshots in the [next chapter](24-snapshots-a-time-machine-for-your-volume.md). Independent backups should be arranged separately for your infrastructure.

## If you have already deleted something you need

If the trash is on and you used Move to Trash, look in the trash first, on any computer that uses the volume. If the item is not there, it was deleted permanently, it has expired, or you no longer have access to its original folder. In that case recovery is not guaranteed without a snapshot or a backup made before the deletion.

---

[Contents](README.md) · [Українська](../uk/23-deleting-files-there-is-no-magic-trash-can.md) · [← Previous](22-settings-status-and-activity.md) · [Next →](24-snapshots-a-time-machine-for-your-volume.md)
