# 14. Mount Your Volume — studiofs mount

The volume is ready, but your editing application does not yet know where to find the media. Let us mount it in your computer's file system: in Windows it will appear as a drive with a letter, and on macOS or Linux as a folder. That is what `studiofs mount` does.

We will call our volume `myvolume` below. If you created yours under a different name, replace `myvolume` in the commands with that name. There is no need to create another volume here: `mount` connects one you have already prepared.

## Before you start

This computer needs StudioFS and the mounting software for your system: **WinFSP on Windows, macFUSE on macOS, or libfuse3 on Linux**. Installation instructions are in the earlier chapters.

You also need the volume's local configuration. After `studiofs init`, it is already on the computer where you created the volume. On another computer, first retrieve the existing volume's configuration using `studiofs discover`; we will cover that scenario separately. Running `init` again does not replace connecting to a shared volume.

For the first launch, check access to PostgreSQL and S3. If you reach the database through Tailscale or NetBird, turn on the VPN. StudioFS works with local metadata, but that does not mean a fresh client can obtain the whole shared file space without a network connection.

## Windows: choosing an available drive letter

Open PowerShell and run:

```powershell
studiofs mount myvolume Z:
```

`myvolume` is the volume name, and `Z:` is where applications will see it. Choose a letter that another drive or network connection is not already using. If `Z:` is taken, you could use `S:`, for example.

**On Windows, you must supply the drive letter explicitly.** For this command, a path such as `C:\Projects` cannot replace a drive letter. The volume name is not appended to `Z:`: the root of the mounted file space will be `Z:\`.

Open File Explorer and go to `Z:\`. A new volume may be empty—that is normal. It is not supposed to come with someone else's finished commercial, tempting as that might be as a starting point.

## macOS and Linux: choosing a folder

On macOS, use the `/Volumes/Studiofs` directory prepared in the previous chapter:

```bash
studiofs mount myvolume /Volumes/Studiofs
```

StudioFS appends the volume name to the supplied path unless its last component already matches that name. The actual mount point will be **`/Volumes/Studiofs/myvolume`**. On Linux, use a folder in your home directory:

```bash
studiofs mount myvolume "$HOME/StudioFS"
```

Both commands create a mount point using the volume name. Run the command for your system.

StudioFS tries to create the required directories itself. Choose a dedicated mount folder without your working files in it. On macOS, make sure you have write access to `/Volumes/Studiofs`; chapter 11 explains how to set it up. On Linux, use a folder your user can access.

On macOS, open Finder, press **⌘⇧G**, and enter `/Volumes/Studiofs/myvolume`. On Linux, open `$HOME/StudioFS/myvolume` in your file manager. The systems use different software: macOS uses macFUSE, and Linux uses libfuse3.

## Why has the terminal not returned to the prompt?

Because it is keeping the mount running. By default, `mount` runs **in the foreground**: the process stays active and keeps the terminal occupied. While it is running, open another terminal window or a file manager to check the drive.

You can also request this mode explicitly:

```powershell
studiofs mount myvolume Z: --foreground
```

On macOS or Linux, supply your path instead of `Z:`. This mode is useful for the first launch because you can see any problems immediately. Do not close the window expecting the drive to stay mounted.

For everyday work, there is **background mode**:

```powershell
studiofs mount myvolume Z: --detach
```

On macOS:

```bash
studiofs mount myvolume /Volumes/Studiofs -d
```

On Linux:

```bash
studiofs mount myvolume "$HOME/StudioFS" -d
```

`-d` is short for `--detach`. StudioFS starts a separate process, leaving you free to use the terminal again. You cannot pass `--foreground` and `--detach` together.

Background mode does not install a system service or configure automatic mounting after a reboot. Background process logs are stored in the `logs` directory under the client root, which defaults to `~/.studiofs/logs/`.

## Checking that you are looking at the volume

After launch, open the drive or folder and look at its contents. In a second terminal, you can run:

```powershell
Get-ChildItem Z:\
```

On macOS:

```bash
ls -la /Volumes/Studiofs/myvolume
```

On Linux:

```bash
ls -la "$HOME/StudioFS/myvolume"
```

To check writing, use File Explorer or your file manager to create a folder called `mount-check`, confirm that it appears, and delete it. An empty volume will be empty again afterward. If you deliberately mounted it with `--read-only`, creating the folder should be denied—that is the expected behavior.

On macOS and Linux, **the existence of a directory alone does not prove that the volume is mounted**: the directory may remain after unmounting. Make sure startup completed without an error and the mount process is still running. If the terminal reported a failed launch, do not start copying media into that folder: it could end up in an ordinary local directory.

## Saving a file and sharing it with the team happen separately

When an application saves a file to the mounted volume, StudioFS first accepts the changes locally. They are available immediately on that same mount. The contents are uploaded to S3 and shared metadata is published in the background; other computers see the result after synchronization.

So closing a file, saving successfully, or seeing the `mount-check` folder **does not confirm that publication for your colleagues is complete**. Do not turn off the computer straight after a large export just because your editing application has closed its progress window.

When a graphical environment is available, StudioFS automatically tries to show an indicator in the system tray or menu bar. The current implementation provides this for Windows, macOS, and Linux; whether it is actually visible depends on the graphical environment. You do not need a separate `--tray` option for `mount`. If the graphical indicator is unavailable, the mount can work through the command line.

A red pulse means local changes still need synchronization. An animated blue-and-yellow flag appears after confirmation that accepted changes and metadata have finished transferring. A missing indicator or stale status confirms no such thing. Even after confirmation, an application can write something else.

`--detach` alone does not guarantee that an indicator will appear. A background mount does not require one to work.

## Finishing your first session

Close applications that are using the volume. For a foreground mount, press **Ctrl+C** in its terminal and wait for the process to finish.

A background mount is stopped with a separate command. On Windows:

```powershell
studiofs umount Z:
```

On macOS, pass **the actual path, including the volume name**:

```bash
studiofs umount /Volumes/Studiofs/myvolume
```

On Linux:

```bash
studiofs umount "$HOME/StudioFS/myvolume"
```

Wait for the command to finish. We will cover unmounting and shutdown in more detail in a separate chapter; forcibly terminating the process should not become your usual way of removing the drive.

You now have a path to the shared file space that your applications can use. In the next chapter, we will put our first files there and check their journey from a local save to the cloud.

---

[Contents](README.md) · [Українська](../uk/14-mount-your-volume-studiofs-mount.md) · [← Previous](13-create-your-first-volume-studiofs-init.md) · [Next →](15-your-first-files-in-the-cloud.md)

[Linear source](https://linear.app/valandreev/document/14-mount-your-volume-studiofs-mount-65361de6b179)
