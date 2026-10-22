# 23. Deleting Files: There Is No Magic Trash Can

**In StudioFS, do not count on recovering a deleted file from the trash or Recycle Bin.** This applies to ordinary deletion in a file manager, even if it calls the action “Move to Trash.”

Before deleting anything, you need a separate, verified copy or a recovery method prepared in advance. StudioFS does not guarantee that you can get a file back after deletion.

## What actually happens?

On Windows, macOS, and Linux, a file manager may try to move a file into a special trash folder instead of deleting it directly. On an ordinary local disk, this often means you can still recover the file.

StudioFS behaves differently. When such an operation targets a recognized trash folder, StudioFS **deletes the source file instead of keeping it for recovery**. For a folder, this means deleting its contents, including nested folders. No file entry remains at the destination.

These special names include `.Trash-<uid>` and `.Trash` on Linux, `.Trashes` on macOS, and `$RECYCLE.BIN` on Windows. StudioFS hides the corresponding special folders at the volume root from file listings. Do not look there for a backup of deleted files.

The button label and confirmation shown by your file manager depend on the operating system. They do not change the rule: **the trash or Recycle Bin is not a recovery method for files on a StudioFS volume**.

## “Immediately”—on which computer?

Deletion first changes the local file space on the computer you are using. Publishing those changes and receiving them on other computers happen in the background. A file may therefore have disappeared on your computer while a colleague can still see it.

This is a synchronization delay, not a window for undoing the deletion. Do not expect disconnecting from the internet, remounting the volume, or opening an old file listing to bring deleted files back. Seeing a file name on another computer does not prove that the entire file can still be read there.

Likewise, a file disappearing from a listing does not mean that all its data in S3 has been physically erased or its space in the local cache freed at that very moment. But fragments remaining in storage or the cache **do not guarantee recovery**. These are internal StudioFS data, not a hidden trash folder.

## Before cleaning up a project

Suppose your team has finished an edit and wants to remove the folder of source material. Before deleting it:

1. **Check the volume and full path.** Make sure you have selected the intended folder, not a neighboring project or the only copies of the originals.
2. **Agree on the deletion with your team.** This is a shared file space: the change will eventually reach other computers too. The copy a colleague sees on the same volume is not an independent backup.
3. **Check your data protection.** The snapshot you need must exist before deletion, and the backup must contain the required files and be readable. If the copy depends on changes being published, make sure it includes the latest version you need.
4. **If in doubt, postpone deletion.** With your team's agreement, you can move the material into an ordinary archive folder on the volume. This helps avoid rushing, but that folder still does not replace a backup.

To check this behavior, use only a separate test volume and disposable test files. Do not use working material for this check.

## Snapshots and backups: prepare in advance

A snapshot records the state of a volume at a particular moment. It may help you return to the state before deletion if it was created in time and the required data remains available. A snapshot created after deletion will not bring back a file that is already missing from the recorded state.

You still need a separate backup: a snapshot is not an independent copy of all the data and does not protect against every storage problem. Before a major cleanup, try restoring the files you need to a safe, separate location. A “copy complete” message alone does not tell you whether the copy is usable.

We will cover preparing snapshots in the [next chapter](24-snapshots-a-time-machine-for-your-volume.md), and independent backups in [chapter 25](25-backups-even-time-machines-need-insurance.md).

## If you have already deleted something you need

Stop further cleanup and notify the person responsible for the volume. Record the path, the approximate time of deletion, and the computer where it happened: these details will help locate the appropriate snapshot or backup.

Check the copies you have: a snapshot from before deletion, backup storage, or an original saved separately. Restore to a separate location first so you can check the contents without overwriting the team's current files.

Do not clear the cache or manually change S3 objects or PostgreSQL records in an attempt to recover a file. Such actions can make further work with the data more difficult. If you have no verified copy, contact your administrator or support, but do not assume recovery is guaranteed.

---

[Contents](README.md) · [Українська](../uk/23-deleting-files-there-is-no-magic-trash-can.md) · [← Previous](22-settings-status-and-activity.md) · [Next →](24-snapshots-a-time-machine-for-your-volume.md)

[Linear source](https://linear.app/valandreev/document/23-deleting-files-there-is-no-magic-trash-can-f3a94106c3c4)
