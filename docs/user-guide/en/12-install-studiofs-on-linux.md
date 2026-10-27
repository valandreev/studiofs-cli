# 12. Install StudioFS on Linux
> **Work in progress**

On Linux, StudioFS mounts storage through **FUSE3**. This component lets the program present files from S3 as an ordinary folder: your editing application sees a path to the media, while StudioFS takes care of retrieving and saving it.

In this chapter, we will prepare the system, download StudioFS, and check that it starts. Creating a volume and actually mounting the storage come in the next chapters. The folder for our future drive will be empty for now—that is normal; the media has not arrived yet.

## Which Linux distribution should I use?

StudioFS has been tested on **Ubuntu 24.04** and **Rocky Linux 9.4**. It should work on other Linux distributions, but we have not tested them; system package names and requirements may differ.

You need administrator privileges to install system packages. Run StudioFS as your regular user.

## Installing system dependencies

### Ubuntu 24.04

Open a terminal and run:

```bash
sudo apt-get update
sudo apt-get install -y fuse3 libfuse3-3
```

These are system libraries and tools needed to run the program and mount FUSE3.

### Rocky Linux 9.4

Some required packages come from additional repositories. The commands below enable EPEL and CRB, which changes the system's package sources. On a workstation managed by your studio, coordinate this with your system administrator.

```bash
sudo dnf install -y dnf-plugins-core epel-release
sudo dnf config-manager --set-enabled crb
sudo dnf install -y fuse3 fuse3-libs
```

If a package cannot be found, first check repository availability and your system version. Do not replace FUSE3 with the older FUSE2: StudioFS uses version 3.

## Desktop indicator in 0.2.1

The Linux tray uses KSNI/StatusNotifierItem over your session D-Bus; GTK3, GLib, libxdo and AppIndicator libraries are no longer StudioFS prerequisites. A compatible desktop host, such as KDE Plasma, displays the icon. On GNOME, enable an AppIndicator/KStatusNotifierItem extension; tooltips may not be displayed. If the watcher starts later or restarts on the same bus, the backend registers again. Without a graphical session or session bus, mounting continues without the tray. An absent icon does not confirm that synchronization has finished. Older 0.2.0 binaries retain their GTK/AppIndicator requirements.

## Checking FUSE3

The checks are the same on both systems:

```bash
fusermount3 --version
ls -l /dev/fuse
```

The first command checks that the `fusermount3` utility is available. The second shows the kernel device used for mounting.

If `/dev/fuse` is missing on a regular Linux system, an administrator can try loading the module:

```bash
sudo modprobe fuse
```

Then check for the device again. In a container or restricted environment, installing packages alone is not enough: the host must provide access to `/dev/fuse` and allow FUSE mounts. On a managed server, its administrator needs to make that change.

## Downloading StudioFS

Open the [StudioFS releases page](https://github.com/valandreev/studiofs-cli/releases) and download the build for your system and architecture. Extract it if it is packaged in an archive. In the `install` command, replace `/path/to/studiofs` with the path to the downloaded executable:

```bash
mkdir -p "$HOME/.local/bin"
install -m 755 "/path/to/studiofs" "$HOME/.local/bin/studiofs"
export PATH="$HOME/.local/bin:$PATH"
studiofs --version
studiofs --help
```

The `export` command applies to the current terminal. To keep the path after closing it, add this line to your shell's configuration file if it is not already there. For Bash, this is usually `~/.bashrc`. Then open a new terminal and run `studiofs --version` again.

If you have installed StudioFS before, check which file is being launched:

```bash
command -v studiofs
```

In this example, we expect the path to `studiofs` in your `.local/bin` folder. A different path may mean that the terminal finds an older copy before the new one.

## Preparing a mount folder

Create `/mnt/StudioFS` and give it to your user so StudioFS can create mount points inside it:

```bash
sudo mkdir -p /mnt/StudioFS
sudo chown "$(id -un):$(id -gn)" /mnt/StudioFS
ls -ld /mnt/StudioFS
```

For a volume named `studio`, the actual mount point will be `/mnt/StudioFS/studio`: StudioFS appends the volume name to the directory you provide. Do not put media in it before mounting: ordinary local files will not automatically become volume files, and once the file system is mounted, it will hide the folder's existing contents.

Run StudioFS as the same user who will work with the media. There is no need to make the folder accessible to everyone with `chmod 777` or run the program through `sudo` to get past an unexplained access error. First check the folder's owner and access to `/dev/fuse`. These examples prepare a mount for your user; access from other accounts requires a separate permissions check.

## If something will not start

* **A system library is missing.** Make sure you installed the system packages listed above.
* **`studiofs: command not found`.** Run the file by its full path, `"$HOME/.local/bin/studiofs" --version`, and check your `PATH` settings.
* **A FUSE access error.** Check that `/dev/fuse` exists, its permissions, and any restrictions imposed by the environment. In a container, this is often a host configuration issue.

Successful `--version` and `--help` commands mean that the command-line program starts. They do not yet test a file system mount, PostgreSQL, or S3. We will check those when creating and mounting the first volume.

## What is next?

We now have the program, FUSE3, and a place for our future drive. In the next chapter, we will create the first volume with `studiofs init` and bring the PostgreSQL and S3 services prepared earlier together into one working storage system.

---

[Contents](README.md) · [Українська](../uk/12-install-studiofs-on-linux.md) · [← Previous](11-install-studiofs-on-macos.md) · [Next →](13-create-your-first-volume-studiofs-init.md)
