# 17. Unplug Safely — studiofs umount

Work is done, the project is saved, and your laptop is practically asking to go into your bag. There is one step left: let StudioFS finish sending your changes and unmount the volume.

Clicking Save in your editor and being ready to shut down your computer are different things. Let us look at what to check between them so you can pick up tomorrow without surprises.

## Saved here does not mean available everywhere

StudioFS accepts changes locally and uploads file contents to S3 in the background. Shared metadata is also published asynchronously through PostgreSQL. So a file may already open on your computer while another computer still does not have its latest version.

Even closing a file or an application requesting `fsync` does not, by itself, confirm remote publication. And completing an upload to S3 is only part of the job: StudioFS also needs to publish the file version and receive metadata acknowledgement.

The best time to check is **while the volume is still mounted**. Once it is unmounted, its indicator can no longer help you wait for transfers to finish.

## Close your applications first

Save your project and wait for exports, copies, and other write operations to finish. Then close your editor, media players, and any other applications working with the volume. Autosave can write files too—even when you have already mentally left for home.

Close file manager windows showing the drive. If a terminal is currently in a folder on the volume, switch to your home folder or another local directory. This also helps avoid a volume being busy because of an open file or a current working directory.

## Wait for synchronization to be confirmed

When a graphical environment is available, StudioFS automatically tries to show an indicator in the system tray or menu bar on Windows, macOS, and Linux. Whether it is actually visible depends on the environment; the mount can work without it. If the indicator is available, check its state:

- **A red pulse** means local changes still need synchronization. Keep your computer running; restore access to S3, PostgreSQL, and the private network if needed.
- **An animated blue-and-yellow flag** appears when StudioFS has confirmed remote completion of data and metadata changes accepted by this mount. A fresh indicator tooltip may say `safe to shut down`.
- A starting, unavailable, or stale state **does not confirm completion**. The absence of a red pulse alone is not enough.

Check the indicator after closing your applications: a new write after your check means new synchronization work. Then unmount the volume using the normal command below.

Without the indicator, you can observe activity on the mounted volume. For example, on Windows:

```powershell
studiofs perf Z: --section upload --section metadata --count 5
```

On macOS or Linux, substitute your mount point:

```bash
studiofs perf "$HOME/StudioFS/myvolume" --section upload --section metadata --count 5
```

This is **observation, not a synchronization command**. An empty upload queue or a transfer rate of zero does not prove that all versions and metadata have been published. For an important handoff to another computer, also check the latest files there: open them and make sure you see the changes you need, rather than just familiar names.

If you cannot confirm completion, do not expect unmounting the volume itself to finish all transfers. Leave it mounted and investigate the delay using the [chapter on synchronization](19-sync-bad-wi-fi-and-other-surprises.md) or [diagnostics](29-logs-diagnostics-and-asking-for-help.md).

## Unmounting the volume

The command takes the **mount point**, not the volume name. For drive `Z:` on Windows:

```powershell
studiofs umount Z:
```

For a folder on macOS or Linux:

```bash
studiofs umount "$HOME/StudioFS/myvolume"
```

Replace the path with the one you used when mounting. Keep the quotes if it contains spaces. Notice the spelling: `umount`, with no `n` after the `u`.

Wait for the command to finish. A successful unmount removes access to the volume through that drive or folder. It does not delete the volume itself, its files in S3, or the local cache.

**A successful `umount` does not confirm that all changes are already in the cloud.** During a normal shutdown, StudioFS finishes local work and persists local metadata. By default, however, it does not wait for the upload queue to empty: unfinished uploads are cancelled so they can resume at the next startup. Even if an upload wait has been configured separately, it is time-limited and does not replace checking that publication is complete.

The correct sequence is therefore: **save and close applications → confirm synchronization → run `umount` → shut down the computer**.

## If something goes wrong

If the command reports that the volume is busy, check open applications, windows, and terminals again. If you lack permission, consider whether you started the mount with elevated privileges: unmounting may require a terminal with the same privileges.

Do not force-terminate the StudioFS process just to unmount faster. The command itself has a limited wait and may also resort to forced termination in some situations. If you see such a warning or an error, do not treat it as confirmation of synchronization, and keep the message for diagnostics.

If the internet goes down and you have to stop working, unmounting will not send local changes to an unreachable cloud. **Keep the local cache and client settings.** Once connectivity returns, mount the volume on the same computer with the same cache and wait for synchronization to finish. Recovery from local state depends on what was actually persisted; it is no substitute for a backup.

## Mounting again

There is no need to recreate the volume or run `discover` again after a normal unmount. Use the familiar `mount` command with the volume name and the mount point you want. For example, on Windows:

```powershell
studiofs mount myvolume Z:
```

On macOS or Linux:

```bash
studiofs mount myvolume "$HOME/StudioFS/myvolume"
```

Substitute your volume name and the path from the [chapter on mounting](14-mount-your-volume-studiofs-mount.md). After startup, let StudioFS resume any unfinished work. Then you can reopen your project—or first make sure yesterday's changes are already available to your colleague.

---

[Contents](README.md) · [Українська](../uk/17-unplug-safely-studiofs-umount.md) · [← Previous](16-connect-a-second-computer-studiofs-discover.md) · [Next →](18-working-with-real-creative-projects.md)

[Linear source](https://linear.app/valandreev/document/17-unplug-safely-studiofs-umount-777999ff6f1e)
