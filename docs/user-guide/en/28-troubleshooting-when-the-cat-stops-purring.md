# 28. Troubleshooting: When the Cat Stops Purring

The drive will not mount, a colleague cannot see yesterday's render, and the SSD has suddenly run out of space? Let us start with exactly what is failing. There is always time to reinstall everything. Changes that have not been uploaded yet are better kept safe first.

## First, three quick checks

In these examples, `myvolume` is your volume name. Replace it with your own.

```bash
studiofs --version
studiofs settings
studiofs status myvolume --verbose
```

The first command shows the client version, the second shows saved settings for future mounts, and the third shows volume status and available diagnostic metrics. `settings` without options changes nothing. Saved settings do not necessarily match the current mount: options may have been overridden at startup.

If you mounted the volume with a separate cache directory, specify the same **base directory**, without adding the UUID:

```powershell
studiofs status myvolume --verbose --cache-dir D:\StudioFSCache
```

Do not treat an empty queue or missing metrics as proof that synchronization has finished. Some information is available only from a running mount; a stale snapshot does not describe the current state.

**Until you know the cause, do not delete the cache or local database, or recreate the volume.** They may hold the only copies of changes that have not reached storage yet. `destroy`, metadata repair, and manually cleaning up internal directories are not the first steps in troubleshooting.

## The drive will not mount at all

Keep the full error message. The difference between “no local volume,” “driver failed to load,” and “server unavailable” matters more than it might seem at three in the morning.

* **Unknown command or option.** Check `studiofs mount --help` in the version you have installed. The current command has no `--tray` option: on Windows and macOS, StudioFS automatically tries to show an indicator when a graphical environment is available. Failure to start the indicator does not, by itself, stop the mount.
* **Volume not found.** Check the spelling of its name, your OS user account, and `STUDIOFS_CLIENT_ROOT`. On another computer, use `discover` to connect to an existing shared volume, as described in [chapter 16](16-connect-a-second-computer-studiofs-discover.md). The `status` message may suggest `init`, but that does not prove you need to create a new volume.
* **Drive letter or mount point already in use.** On Windows, choose an available letter, such as `Z:`. On macOS and Linux, check the directory and your user's permissions. If StudioFS is already running there, first close applications and unmount normally, following [chapter 17](17-unplug-safely-studiofs-umount.md). Do not delete lock files to get around an active mount.
* **The terminal does not return to the prompt.** This is normal: a regular `mount` runs in the foreground. Do not close it as though it were frozen. Use `--detach` for background operation.

To restart with more detailed logging, **after normally unmounting the previous mount**, you can use:

```powershell
studiofs --log-level debug mount myvolume Z:
```

On macOS or Linux, replace `Z:` with your directory. `--log-level` goes before `mount`. If you used a separate cache or other options, keep them in this command too.

## A WinFSP or FUSE error

### Windows

Check that **WinFSP** is installed, as well as StudioFS itself. Then consult the [installation instructions](10-install-studiofs-on-windows.md).

On the first mount, StudioFS may display a UAC prompt to configure WinFSP integration with Windows Mount Manager. If you declined it, the initial setup may not have finished. Interactive UAC is unavailable in service or CI sessions—an administrator must prepare the system beforehand. Subsequent regular mounts do not require you to keep running as administrator.

### macOS

You need **macFUSE** and the permissions its installation requires. If the system asks you to allow a component or restart, complete those steps following [chapter 11](11-install-studiofs-on-macos.md).

### Linux

The current backend uses **FUSE3**. Check the utility and kernel device:

```bash
fusermount3 --version
ls -l /dev/fuse
```

If the library cannot be found during the build, check the FUSE3 development packages following [chapter 12](12-install-studiofs-on-linux.md). If `/dev/fuse` is missing or inaccessible, the kernel module and permissions need checking. A container also needs permission to access FUSE on the host: installing a package inside the container does not guarantee this. Do not start by running everything as `root`.

## A PostgreSQL or VPN error

**Timeout**, **connection refused**, a DNS error, and **password authentication failed** mean different things.

1. Check that Tailscale or NetBird is connected and the server is reachable on the private network. A VPN status of “connected” does not guarantee that access rules allow PostgreSQL connections.
2. Compare the address and port with the server configuration. Check the firewall, VPN ACLs, and PostgreSQL itself, following [chapters 7](07-postgresql-the-brain-behind-the-files.md) and [8](08-tailscale-netbird-a-private-path-to-postgresql.md).
3. If the network works, check the username, password, database name, and server authentication rules. Do not expose a public PostgreSQL port as a way to “fix the VPN.”

If you have the `psql` client, you can test an actual connection from the workstation. Replace the address and username with your own; enter the password at the prompt:

```bash
psql -h VPN_SERVER_ADDRESS -p 5432 -U studiofs -d studiofs -W -c "SELECT 1;"
```

A response of `1` confirms that this user can access this database. It does not check S3 or guarantee all the permissions needed to initialize a volume.

Losing PostgreSQL does not necessarily stop an already mounted drive immediately: StudioFS works with local metadata. But exchanging changes between clients requires restoring connectivity.

## Folders are visible, but a file will not open

If cached files are readable but a new large file is not, check access to **S3**. Seeing a file name does not yet confirm that its contents are available.

Check the endpoint, bucket, region, and whether the keys are still valid. Look in the logs for DNS, TLS, timeout, or `AccessDenied` errors. Also check storage health, quotas, and any proxy in front of it. S3 can fail even when the VPN connection to PostgreSQL works: these are separate access paths.

Do not disable certificate verification or switch from HTTPS to HTTP at random. If the server address has changed, confirm the new address with the administrator. See [chapter 6](06-s3-a-home-for-your-files.md) for more on configuration.

A `Permission denied` error may also concern permissions on the file itself. Check the user and the `--principal` option if your team uses ACLs. Bucket permissions do not override permissions within the volume.

## Saving has finished, but a colleague sees the old file

In StudioFS, saving on one computer and publishing for others are separate stages. Closing a file does not confirm that every colleague has received the new version.

Start with **the computer where the changes were made**: is it on, is the mount running, and are S3 and PostgreSQL reachable? Check `status --verbose` and the logs for upload or publication retries. Then check connectivity on your colleague's computer.

If the indicator is available, a red pulse means local changes still need synchronization. The animated blue-and-yellow flag appears once remote completion has been confirmed for the data and metadata accepted by that mount. Missing, stale, or unknown status confirms no such thing. The author's indicator does not certify that a particular application on a colleague's computer has reread the file.

Keep the author's computer running and restore connectivity. Once synchronization is confirmed, ask your colleague to refresh the file list or reopen the file in their application. Save their own edits first. Do not overwrite the shared file with an old local copy to “push the update through.”

For more on delays and collaboration, see [chapters 19](19-sync-bad-wi-fi-and-other-surprises.md) and [21](21-two-people-one-file-collaboration-and-conflicts.md).

## The cache is full, or the SSD has run out of space

Check two things: the StudioFS cache limit and free space on the **physical disk** holding it. Increasing the limit on a full SSD is rather like writing “large” on a small suitcase.

By default, the client root is `~/.studiofs` (inside the user's home directory on Windows). Local metadata lives in `meta/<name>`, the default cache base directory is `meta/<name>/cache`, and data inside it is separated by volume UUID. `STUDIOFS_CLIENT_ROOT` changes the client root; `--cache-dir` or a saved `cache_dir` setting can place the cache on another disk.

StudioFS automatically removes cached data that is eligible for eviction. Changes that have not yet synchronized are another matter: deleting the cache manually may destroy them. Stop making large new writes, restore S3/network access, and free space by removing **other files you know you no longer need** from that disk.

Plan changes to the cache limit or location following [chapter 20](20-cache-why-is-the-cloud-using-my-ssd.md). A new `studiofs settings` value does not move the current cache or change a running mount. Do not point the client at an empty cache while the old one holds unfinished changes.

## A file has disappeared, or a folder is empty

First, make sure you are looking at the mounted drive, the correct volume, and the right folder. After unmounting, the ordinary directory used as the mount point may appear empty—this does not prove that files were deleted.

Ask the author exactly where they saved the file and whether synchronization finished. Check whether it was renamed or moved. Compare client versions and ask the administrator to confirm the volume identity: the same local name on two computers does not, by itself, prove they are connected to one shared drive.

If you suspect deletion or a conflict, stop making changes in that folder and preserve any available independent copies. Do not run `init` again, restore over the current volume, or start bulk copying without understanding the cause. Check backups with the administrator. StudioFS has no universal trash bin that will bring everything back with one click.

## When you need help

If the cause is still unclear, collect the StudioFS version, OS, incident time with time zone, launch command without secrets, exact error, and a short description of what you expected and what happened. Include the `status --verbose` output and the relevant log excerpt. For a background mount, logs are in `<client-root>/logs`; for a regular launch, check the terminal.

Before sharing, check the material and redact passwords, S3 keys, tokens, and full DSNs containing secrets. Do not send the entire `config.toml` or local database without a specific need. In the [next chapter](29-logs-diagnostics-and-asking-for-help.md), we will cover how to put together a useful diagnostic report. A cat may fall silent for many reasons; the exact error message narrows down the suspects better than reinstalling everything.

---

[Contents](README.md) · [Українська](../uk/28-troubleshooting-when-the-cat-stops-purring.md) · [← Previous](27-maintenance-and-removing-a-volume.md) · [Next →](29-logs-diagnostics-and-asking-for-help.md)

[Linear source](https://linear.app/valandreev/document/28-troubleshooting-when-the-cat-stops-purring-3a4d7bbb0f03)
