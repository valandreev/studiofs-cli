# 30. Glossary, Command Cheat Sheet and FAQ
> **Work in progress**

This chapter is here to help you look up a term, find a command, and get back to work. If you are setting up StudioFS for the first time, start by [choosing your setup](05-choose-your-setup.md) rather than copying every command in order.

## A short glossary

| Term | What it means in practice |
| --- | --- |
| **Volume** | A shared StudioFS file space with its own folder structure and settings. In the examples, its name is `projectfs`. |
| **Mounting** | Connecting a volume as a drive or directory that ordinary applications can use. |
| **Mount point** | Where the files appear: for example, `Z:` on Windows or a directory on macOS. It is not an S3 address. |
| **Client** | A computer and the StudioFS instance running on it to work with a volume. |
| **S3-compatible storage** | A service where StudioFS stores file contents in its own format. It can be a cloud service or your own storage. |
| **Bucket** | A container for objects in S3. You should not edit its contents as if it were an ordinary project folder. |
| **Endpoint** | The S3 service address the client uses to access storage. |
| **Metadata** | Information about file names, folders, sizes, and versions. The video itself or the contents of a scene are data, not metadata. |
| **PostgreSQL** | The shared metadata database through which clients learn about published changes. |
| **Local metadata database** | A working copy of metadata on the computer. The mount uses it to avoid contacting PostgreSQL for every file operation. |
| **Cache** | Local data on an SSD: downloaded parts of files and new changes that may still be waiting for upload. It is more than a folder of disposable temporary files. |
| **Synchronization** | Background data transfer and metadata propagation between clients. Saving in an application and having a colleague see the result happen at different times. |
| **Snapshot** | A captured state of the entire volume that you can open read-only and copy the files you need from. |
| **TTL / retention** | How long a snapshot is kept. In Snapshot v2 commands, the period starts when it becomes `Ready`. |
| **Backup** | An independent copy of the necessary data, metadata, and keys, with a tested recovery procedure. A snapshot alone is not enough. |

For more about how these parts fit together, see [How Does This Thing Work?](02-how-does-this-thing-work.md).

## Command cheat sheet

In the examples, `projectfs` is your volume name and `Z:` is an available Windows drive letter. On macOS, use a prepared directory instead. Check current platform support in the [requirements chapter](03-capabilities-limits-and-requirements.md) and the [Linux instructions](12-install-studiofs-on-linux.md).

| What you need | Command | Where to find details |
| --- | --- | --- |
| Check the version | `studiofs --version` | [Installing on Windows](10-install-studiofs-on-windows.md) |
| Find help | `studiofs --help` or `studiofs mount --help` | Help for your installed version |
| Mount a volume | `studiofs mount projectfs Z:` | [Mounting](14-mount-your-volume-studiofs-mount.md) |
| Set a cache limit for a mount | `studiofs mount projectfs Z: --cache-size 500GB` | [Cache](20-cache-why-is-the-cloud-using-my-ssd.md) |
| View status | `studiofs status projectfs` | [Settings, status, and activity](22-settings-status-and-activity.md) |
| Get more detailed status | `studiofs status projectfs --verbose` | The same chapter |
| View saved settings | `studiofs settings` | The same chapter |
| Save a default cache limit | `studiofs settings --cache-size 500GB` | The same chapter |
| Watch mount activity | `studiofs perf Z: --count 5` | The same chapter |
| Disconnect a drive | `studiofs umount Z:` | [Disconnecting safely](17-unplug-safely-studiofs-umount.md) |
| Create a snapshot kept for seven days | `studiofs snapshot projectfs create --ttl 7d` | [Snapshots](24-snapshots-a-time-machine-for-your-volume.md) |
| List snapshots | `studiofs snapshot projectfs list` | The same chapter |
| Check the snapshot schedule | `studiofs snapshot projectfs policy show` | The same chapter |
| Preview cleanup | `studiofs janitor projectfs --dry-run` | [Maintenance](27-maintenance-and-removing-a-volume.md) |

For a snapshot, copy its actual identifier from the list. For example, if it is `projectfs.2026-08-12T14-07-09.482Z`:

```powershell
studiofs snapshot projectfs inspect projectfs.2026-08-12T14-07-09.482Z
studiofs smount projectfs projectfs.2026-08-12T14-07-09.482Z Y:
```

`Y:` must be a separate, available drive letter. `smount` opens the snapshot read-only; when you are done, use `studiofs umount Y:`. Every `snapshot` command requires the explicit volume name and never infers a current volume. An unknown or invalid name produces an actionable error. `smount` also requires the volume name.

### Commands worth reading about before running

`studiofs init` creates a new volume and requires storage and PostgreSQL settings. A complete example with explanations of the parameters is in the [first-volume chapter](13-create-your-first-volume-studiofs-init.md). For an existing volume on another computer, use `studiofs discover` rather than another `init`: see [connecting a second computer](16-connect-a-second-computer-studiofs-discover.md).

`studiofs fsck projectfs` checks metadata and **may prompt you to repair it**. It is not guaranteed to be a passive report. The `--repair` flag allows findings to be repaired without interactive confirmation. Preparation and limitations are covered in the [maintenance chapter](27-maintenance-and-removing-a-volume.md).

`studiofs destroy projectfs` **destructively deletes the volume**; it does not unmount it. The `--local` option keeps remote data but deletes local state and can destroy changes that have not yet been uploaded. Do not use it to “refresh the cache.”

`studiofs dump projectfs --path-to-dump ./projectfs-metadata.json` exports part of the PostgreSQL metadata. This JSON does not contain the files themselves and is not a complete backup of the database or volume. `load` changes the target database; the migration steps and limits of both commands are described in the [metadata migration chapter](25-migrating-studiofs-metadata-to-new-postgresql-server.md).

## Frequently asked questions

### Do I need a constant internet connection?

Fetching missing data requires access to S3; propagating shared state requires access to PostgreSQL. Under certain conditions, cached data can be read during a connection outage, and local changes can wait for the network to return. This does not guarantee full offline operation. See the [chapter on bad Wi-Fi](19-sync-bad-wi-fi-and-other-surprises.md) for details.

### Where is the “Sync now” button?

Transfers happen in the background; there is no separate public `studiofs sync` command. Check `status`, activity, and errors. On Windows and macOS, `mount` automatically attempts to show a system tray or menu bar indicator when a graphical environment is available; no separate `--tray` flag is needed. If the indicator fails to start, the mount continues to work through the CLI.

### The file is saved. Can I shut down the computer?

First, save your work, let the application finish writing, and wait for changes to upload. A pulsing red indicator means synchronization is active—this is a good time to wait. An idle icon alone does not prove that every colleague has received the version they need. Close applications using the volume and run `umount`; if the command fails, find out why before shutting down. See [disconnecting safely](17-unplug-safely-studiofs-umount.md).

### Why is my SSD filling up if the files are in the cloud?

StudioFS keeps the data applications need close at hand and accumulates new changes before uploading them. Do not delete the cache manually: it may contain the only local copies of unpublished data. The [cache chapter](20-cache-why-is-the-cloud-using-my-ssd.md) explains how to assess space usage.

### I changed `settings`, but nothing behaves differently. Why?

`settings` saves defaults for future mounts rather than reconfiguring an already running process. Explicit `mount` parameters take priority. In the CLI, this command is called `settings`; there is no `studiofs prefs` command. See [chapter 22](22-settings-status-and-activity.md).

### Does a snapshot replace a backup?

No. A snapshot depends on the availability of the necessary metadata, objects, and keys for the same volume. It also does not automatically include everything still unpublished in the local cache. For disaster recovery, use independent backups with a tested recovery procedure.

### Do I need to run `init` or `discover` again after a connection outage?

An ordinary network reconnection does not require a new volume. Check the VPN, S3 and PostgreSQL availability, client status, and free disk space. If the mount has stopped, find out why and reconnect the existing volume with `mount`. `discover` prepares a client for an existing volume; repeating `init` does not repair synchronization. If the problem persists, move on to [diagnostics](29-logs-diagnostics-and-asking-for-help.md).

---

[Contents](README.md) · [Українська](../uk/30-glossary-command-cheat-sheet-and-faq.md) · [← Previous](29-logs-diagnostics-and-asking-for-help.md)
