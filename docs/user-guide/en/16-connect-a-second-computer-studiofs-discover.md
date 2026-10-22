# 16. Connect a Second Computer — studiofs discover

Your first computer already has a volume and some files. Now you want to open them on another machine: perhaps move from the studio workstation to your laptop at home, or give a colleague access to the project materials.

That is what `studiofs discover` is for. It finds an **existing volume in PostgreSQL** and prepares its local metadata on the new computer. The name sounds a little adventurous, but it will not search the entire internet for a lost drive: you need to know the server address and volume name.

**You do not need to run `studiofs init` again on the second computer.** The volume already exists. We are attaching another client to the same file space.

## Prepare the second computer

Install StudioFS and the mounting tools for your system: WinFSP on Windows, macFUSE on macOS, or libfuse3 on Linux. The steps are in chapters [10](10-install-studiofs-on-windows.md), [11](11-install-studiofs-on-macos.md), and [12](12-install-studiofs-on-linux.md).

Connect the computer to the same private Tailscale or NetBird network and check PostgreSQL access using [chapter 08](08-tailscale-netbird-a-private-path-to-postgresql.md). Joining the VPN is not enough on its own: the network and database rules must allow this client to connect. You also need access to S3, where the computer will fetch file contents.

Have the **exact name of the existing volume** and the connection string for the same PostgreSQL server ready. We will use `myvolume` below. Replace it with your own name if you chose a different one when creating the volume. This is not a new local alias: `discover` searches for the volume by the name you provide.

On the first computer, finish writing a small test file and give StudioFS time to publish the changes. PostgreSQL must have a valid metadata checkpoint available for the new client. Having files in S3 alone is not enough to connect.

## Fetch the local metadata

On the **second computer**, open a terminal or PowerShell. The general command format is:

```text
studiofs discover <volume-name> --meta "<PostgreSQL-connection-string>"
```

Replace the placeholders in angle brackets with your volume name and PostgreSQL connection string; do not type the brackets themselves. For example, the command might look like this:

```bash
studiofs discover myvolume --meta "postgres://studiofs:YOUR_PASSWORD@100.101.102.103:5432"
```

Replace the user, password, address, and port with your own. The address here is an example: use your server's VPN address or private hostname, rather than the first computer's `127.0.0.1`. On the second machine, `127.0.0.1` means that machine itself. StudioFS uses the `studiofs` database; check TLS requirements and connection parameters with the server administrator.

If the password contains special URI characters, encode them for the connection string. Quotes prevent the shell from splitting the string, but they do not replace that encoding. Do not post a command containing a real password in a chat or report: it may also remain in your terminal history.

During `discover`, StudioFS fetches the published checkpoint and volume settings from PostgreSQL and creates a local metadata database. **It does not download all your video, audio, and images to disk at once.** The client will read the contents of the files it needs from S3 as you work, using its own local cache.

This command has no separate `--access-key` or `--secret-key` options. S3 settings, including the stored access keys, arrive as part of the existing volume's configuration. A successful `discover` does not yet check whether those keys are valid or whether the second computer can read objects from S3. We will check that by opening a file after mounting. Only trusted users should have access to the metadata and the client's local state.

## Mount the discovered volume

`discover` prepares the client, but **does not mount a drive**. Once it finishes successfully, run `mount` as described in [chapter 14](14-mount-your-volume-studiofs-mount.md).

On Windows, choose an unused drive letter:

```powershell
studiofs mount myvolume Z:
```

On macOS:

```bash
studiofs mount myvolume /Volumes/Studiofs
```

On Linux:

```bash
studiofs mount myvolume "$HOME/StudioFS"
```

On Windows, open `Z:\`; on macOS, open `/Volumes/Studiofs/myvolume`; on Linux, open `$HOME/StudioFS/myvolume`. StudioFS appends the volume name to the parent folder you provide. The second computer can use a different drive letter or path from the first; it is still the same shared volume.

Run `discover` and `mount` **as the same user and with the same client root**. By default, local state lives in `~/.studiofs`. If you set `STUDIOFS_CLIENT_ROOT`, both commands must use the same value. Switching users, running through `sudo`, or changing this variable may cause `mount` to look for metadata somewhere else entirely.

By default, `mount` keeps running in the terminal. Leave that window open and check your files through a file manager or a second terminal window.

## Check changes in both directions

The new client starts from the checkpoint it received. After mounting, it catches up with later changes in the background. At first, the volume may be read-only: writing becomes available after the initial catch-up completes and the client obtains the required authority. This is not a reason to repeat `init`.

A small text file is enough for the first check:

1. On the first computer, create `hello-from-studio.txt` in the mounted volume, write a short line, and close the editor.
2. Give the first client time to publish the changes. On the second computer, refresh the file listing, wait for the file to appear, and open it. Check the actual text inside: seeing the name does not yet prove that the contents are available from S3.
3. Once the second client allows writing, create a different file there: `hello-from-home.txt`. Close the editor, then check that this file appears on the first computer and that its contents are correct.

Keep both computers connected to the network and both StudioFS processes running during this check. Do not edit the same file simultaneously: for now, we are checking that changes arrive; we will get to conflicts later.

Files **do not have to appear instantly**. Saving locally, uploading to S3, and another client receiving metadata are separate steps. There is no universal promise that “everything will be there in three seconds”: timing depends on the network, the amount of changes, and the state of the clients. That is why we start with a short text file rather than an export of an entire season of a TV series.

## If something goes wrong

**Volume not found.** Check the exact name, the PostgreSQL server, and whether the first client has published the metadata needed to connect. Do not create a volume with the same name on the second computer to “help the search.”

**Local metadata already exists.** `discover` refuses to overwrite an existing local database. If this client has been connected before, try a normal `mount` with the same name and client root. Do not delete local metadata blindly: it may contain changes that have not yet been published. A different name is not a workaround either: it changes the name of the volume being searched for on the server.

**Connection error.** Check the VPN, PostgreSQL port availability, credentials, and access rules. A successful network test does not confirm that the password is correct.

**Files are visible but will not open.** Check the second computer's access to S3, the validity of the keys, and whether publication has finished on the first computer. A successful PostgreSQL connection does not check that whole path.

**New changes do not appear.** Make sure both volumes are actually mounted and the clients have network access. Read the terminal messages; further steps are in the [troubleshooting chapter](28-troubleshooting-when-the-cat-stops-purring.md). You do not need to repeat `discover` after every save or normal restart.

After this check, you have two separate clients of one volume, each with its own metadata and cache. Next, we will learn how to shut them down without unnecessary adventures.

---

[Contents](README.md) · [Українська](../uk/16-connect-a-second-computer-studiofs-discover.md) · [← Previous](15-your-first-files-in-the-cloud.md) · [Next →](17-unplug-safely-studiofs-umount.md)

[Linear source](https://linear.app/valandreev/document/16-connect-a-second-computer-studiofs-discover-ee905bdac9dc)
