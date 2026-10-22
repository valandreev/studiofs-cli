# 15. Your First Files in the Cloud

The volume is mounted. Now let us try the everyday tasks: create a folder, copy a file, open it, rename it, and delete what we no longer need. We will start with a short text file you can afford to lose. That terabyte of source footage can take a coffee break for now.

## Find your drive or folder

Open the mounted volume in Windows File Explorer, Finder on macOS, or your Linux file manager. If you followed the examples in the previous chapter, this is `Z:\` on Windows, `/Volumes/Studiofs/myvolume` on macOS, or `$HOME/StudioFS/myvolume` on Linux. If you chose another path, use that one.

The mount must remain active. On macOS and Linux, the folder can still exist after the volume is unmounted, so its presence alone proves nothing. Check that `mount` started without an error and that its process is running. If it exited with an error, find the cause before copying files.

## A small file for your first test

In an ordinary local folder outside StudioFS, create a file called `hello.txt` using a plain text editor. Write something like “Hello from my first volume” in it. Save it as plain text, rather than a formatted document.

Use your file manager to create a folder called `first-test` on the mounted volume. If that name is already taken, choose another one: this exercise needs a separate new folder, rather than someone else's material.

Copy `hello.txt` into it. Make sure you **copy** it, so the original stays on your local drive. When you drag between windows, file managers may choose to copy or move depending on the circumstances; using Copy and Paste makes your intention clearer here.

Open the copied file from the volume and read the line. Then close it, open it again, and check that the same text is there. This is your first check of reading through the mount. It does not yet prove that the file has reached S3: StudioFS may be reading the newly written data from its local cache.

## Change the text and the name

Add a second line to the copy on the volume: “Now there is a change here.” Click Save, close the editor, and open the file again. Both lines should be there. The original file in your local folder should still have no second line, which also helps you check which copy you edited.

Close the file and use your file manager to rename it to `hello-again.txt`. Check that the old name is gone and that the file with the new name opens and contains both lines.

Now create a folder called `notes` inside `first-test` and move `hello-again.txt` into it. Open it from `notes`. To applications, these are ordinary file operations; you do not need a separate StudioFS command for every copy or rename.

If the editor or file manager reports an error, stop at that step. Read the message, check that the volume is mounted for writing, and make sure your local drive has enough space. Do not move straight to a large project in the hope that it will persuade the system to behave better.

## What is saved, and what is still being transferred?

After a write, the new data becomes available locally on this mount. StudioFS then transfers the contents to S3 and publishes updated metadata through PostgreSQL in the background. Another computer sees the new version after receiving those changes.

That is why **a successful save in the editor, closing the file, and finishing a copy do not mean synchronization is complete**. A new file may not yet appear for a colleague, and for an existing file, they may still see the previous published version for a while. Even finishing the transfer of the contents is only part of the journey: the metadata must be published too.

Nor should a local save be treated as a promise to survive every unexpected shutdown. The outcome depends on how the application writes and synchronizes the file, and on the StudioFS cache mode. For this first test, keep the mount running, wait for synchronization to finish, and do not delete the cache manually: it may hold the only copies of changes that have not yet been transferred.

## Check the status

In a second terminal, run:

```bash
studiofs status myvolume
```

Replace `myvolume` with your volume's name. The command shows information about the volume, local cache, and counters. For more detailed mount metrics, where available, use:

```bash
studiofs status myvolume --verbose
```

This displays status; it is not an “upload everything immediately” command. A successful `status` command or a zero in one counter does not confirm that all changes have been published. Missing or stale metrics cannot establish that either.

If your graphical environment shows the StudioFS indicator in the system tray or menu bar, a red pulse means local changes still need synchronization. An animated blue and yellow flag appears after confirmation that the data and metadata accepted by this mount have finished transferring. An absent indicator does not confirm completion.

An application can write again after that confirmation. Before shutting down your computer, close applications that use the volume and unmount it normally; we will cover that step separately in chapter 17. If synchronization does not finish, keep the computer running and check access to S3 and PostgreSQL, including the VPN if your database requires it.

## Delete only the test files

Once you have checked reading, editing, and renaming, delete `hello-again.txt` from `notes`. Check that it is gone, then delete the empty `notes` and `first-test` folders. Look at the name and path once more before confirming.

**Do not count on the system Recycle Bin or Trash.** Deletion on a mounted volume may be permanent, and the change propagates to the shared file space. That is why we practiced in a separate folder and kept the original text on the local drive. We will discuss deletion in more detail in chapter 23.

## Now try one small media file

If every step worked without errors, copy one small clip or image to the volume, keeping its original elsewhere. Open it in your working application. This helps you check your particular application, its version, and the way it handles files: success in a text editor does not guarantee identical behavior from every video editing, graphics, and 3D package on every platform.

When you first open data that is not in the local cache, StudioFS fetches the parts it needs from S3. A later read may be faster thanks to the cache, so one quick repeat viewing does not measure your cloud storage speed.

You have now checked the basic file operations on your computer. In the next chapter, we will connect a second computer and see how it obtains the shared file space and files that have already been published.

---

[Contents](README.md) · [Українська](../uk/15-your-first-files-in-the-cloud.md) · [← Previous](14-mount-your-volume-studiofs-mount.md) · [Next →](16-connect-a-second-computer-studiofs-discover.md)

[Linear source](https://linear.app/valandreev/document/15-your-first-files-in-the-cloud-03113b439cb0)
