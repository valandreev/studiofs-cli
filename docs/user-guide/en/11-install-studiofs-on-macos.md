# 11. Install StudioFS on macOS

On a Mac, StudioFS starts in the terminal too. Once mounted, you will work with a folder in Finder, your editing application, or your compositor, but first you need to install two separate things: **macFUSE**, which helps macOS mount the filesystem, and **StudioFS** itself.

## macFUSE first

The main StudioFS option on macOS at present is **macFUSE**. That is what we will use below.

Download the installer from the [official macFUSE website](https://macfuse.github.io/) and follow its steps. If you already use Homebrew, you can install the same component with:

```bash
brew install --cask macfuse
```

### macOS permissions

Installing the package and allowing its system component to run are separate steps. macOS may ask you to approve loading the extension in **System Settings → Privacy & Security**, then restart your Mac. Names and locations vary between macOS versions; follow the installer messages and the [macFUSE instructions](https://github.com/macfuse/macfuse/wiki/Getting-Started).

Regular macFUSE 5.4 was tested on macOS 27 Golden Gate and macOS 26 Tahoe. On a Mac with Apple silicon, regular macFUSE may require permission for third-party kernel extensions through the recovery environment: **Startup Security Utility → Reduced Security → Allow user management of kernel extensions from identified developers**. This changes the computer's security policy. Follow the current macFUSE instructions for your particular system, and coordinate with your system administrator on a studio Mac. If company policy prohibits this step, do not try to bypass it with commands from a random forum.

## Downloading StudioFS

Open the [StudioFS releases page](https://github.com/valandreev/studiofs-cli/releases) and download the build for your Mac—**Apple silicon** or **Intel**. Extract it if it is packaged in an archive. In the `install` command, replace `/path/to/studiofs` with the path to the downloaded executable:

```bash
mkdir -p "$HOME/.local/bin"
install -m 755 "/path/to/studiofs" "$HOME/.local/bin/studiofs"
export PATH="$HOME/.local/bin:$PATH"
studiofs --version
studiofs --help
```

`export` applies to the current terminal session. To make the setting permanent, add that line to `~/.zshrc` if you use the default interactive zsh, then open a new terminal window. Check which copy of the program the system found:

```bash
command -v studiofs
studiofs --version
```

## Preparing a place to mount

A mount point for macFUSE will be `/Volumes/Studiofs/`. Since this is a system directory, create the folder with administrator privileges and give ownership to your user:

```bash
sudo mkdir -p /Volumes/Studiofs
sudo chown "$(id -un):$(id -gn)" /Volumes/Studiofs
ls -A /Volumes/Studiofs
```

The `ls` command should show no files. If the folder already contains files, choose another mount point name. Do not mount a volume over a directory containing your current project's material: mounting will hide its ordinary contents until you unmount.

In this example, the volume is called `myvolume`. StudioFS appends that name to `/Volumes/Studiofs/`, so the actual path will be `/Volumes/Studiofs/myvolume`. Keep the cache separately on a local SSD, outside the mounted volume.

## First run: what you can check now

If `studiofs --help` works, the executable starts. This is **not yet a test of mounting or macFUSE permissions**.

After creating a volume with `init` or connecting to an existing one with `discover`, the command will look like this:

```bash
studiofs mount myvolume /Volumes/Studiofs --foreground
```

This example assumes that the local client is already set up for the `myvolume` volume. If it is not, first follow Chapter 13 or [Chapter 16 on connecting a second computer](16-connect-a-second-computer-studiofs-discover.md). Do not create a new volume in place of the one your team invited you to join.

Leave the terminal running the mount open. In another window, you can open the folder in Finder:

```bash
open /Volumes/Studiofs/myvolume
```

Use test material for your first read and write checks, following Chapters 14–15. Before unmounting, close files and applications using the volume, then run:

```bash
studiofs umount /Volumes/Studiofs/myvolume
```

If macFUSE refuses to mount, revisit system permissions and restarting. If StudioFS reports a missing volume or a problem accessing PostgreSQL or S3, check the relevant settings: reinstalling the driver will not fix the database password.

---

[Contents](README.md) · [Українська](../uk/11-install-studiofs-on-macos.md) · [← Previous](10-install-studiofs-on-windows.md) · [Next →](12-install-studiofs-on-linux.md)

[Linear source](https://linear.app/valandreev/document/11-install-studiofs-on-macos-3e23441e6bbe)
