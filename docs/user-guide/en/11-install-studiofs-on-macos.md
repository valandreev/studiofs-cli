# 11. Install StudioFS on macOS

> **English translation draft awaiting review.** Not yet approved.

On a Mac, StudioFS starts in the terminal too. Once mounted, you will work with a folder in Finder, your editing application, or your compositor, but first you need to install two separate things: **macFUSE**, which helps macOS mount the filesystem, and **StudioFS** itself.

In this chapter, we will prepare the computer and check that the program starts. We will leave volume creation and full mount setup to [Chapter 13](13-create-your-first-volume-studiofs-init.md) and [Chapter 14](14-mount-your-volume-studiofs-mount.md). Having the program installed does not mean S3 and PostgreSQL are already configured. Unfortunately, they do not administer themselves.

## macFUSE first

The main StudioFS option on macOS at present is **regular macFUSE, or the legacy backend**. That is what we will use below. FSKit is a separate experimental implementation; we will discuss it at the end.

Download the installer from the [official macFUSE website](https://macfuse.github.io/) and follow its steps. If you already use Homebrew, you can install the same component with:

```bash
brew install --cask macfuse
```

This installs macFUSE, **not StudioFS**. Save your open projects before restarting if the installer asks you to do so.

### macOS permissions

Installing the package and allowing its system component to run are separate steps. macOS may ask you to approve loading the extension in **System Settings → Privacy & Security**, then restart your Mac. Names and locations vary between macOS versions; follow the installer messages and the [macFUSE instructions](https://github.com/macfuse/macfuse/wiki/Getting-Started).

On a Mac with Apple silicon, regular macFUSE may require permission for third-party kernel extensions through the recovery environment: **Startup Security Utility → Reduced Security → Allow user management of kernel extensions from identified developers**. This changes the computer's security policy. Follow the current macFUSE instructions for your particular system, and coordinate with your system administrator on a studio Mac. If company policy prohibits this step, do not try to bypass it with commands from a random forum.

A regular StudioFS installation does not require disabling SIP. Also, do not give every application Full Disk Access “just in case.” If macOS separately asks for terminal access to a protected folder, grant only the access needed, or choose a folder in your home directory for the first test.

## Where do you get StudioFS?

The current repository documents building from source. That is the route below: no invented StudioFS installer or Homebrew command that does not exist here. If your studio administrator has already supplied a verified executable for your Mac, use its path and skip to checking that it starts.

Building requires Git, the Xcode command-line tools, and Rust with Cargo. Install Apple's tools with:

```bash
xcode-select --install
```

If the system says they are already installed, you do not need to install them again. You can get Rust by following the [official instructions](https://www.rust-lang.org/tools/install). The current StudioFS `Cargo.toml` requires Rust **1.95 or later**. Check the tools:

```bash
rustc --version
cargo --version
```

Build the program on the Mac where you intend to run it: that saves you from having to choose between Apple silicon and Intel. In the terminal, go to your working directory and run:

```bash
git clone https://github.com/valandreev/studiofs-cli.git
cd studiofs-cli
cargo build --release -p studiofs
```

A studio team should agree on one version of the code in advance: the current repository branch changes, and compatibility between arbitrary builds is not guaranteed.

**This command produces `target/release/studiofs` inside the repository.** It is a command-line program, not an app in the Applications folder. Start by running it directly:

```bash
./target/release/studiofs --version
./target/release/studiofs --help
```

If you set your own `CARGO_TARGET_DIR`, look for the result in its `release` subdirectory.

Building requires real macFUSE headers, including `fuse_lowlevel.h`. The build looks for them through `pkg-config fuse` and in standard Homebrew directories. If you see an error about a missing header, check your macFUSE installation; Rust itself does not include it. Diagnostics through `pkg-config` require that tool, for example `brew install pkg-config` if you use Homebrew. A missing `MFMount.framework` is a different situation: it disables experimental FSKit, but does not replace or remove the requirements for the regular macFUSE build.

## Saving yourself the long path

You can leave the program in the build directory and always specify its full path. Or copy it into your own directory for commands:

```bash
mkdir -p "$HOME/.local/bin"
cp ./target/release/studiofs "$HOME/.local/bin/studiofs"
export PATH="$HOME/.local/bin:$PATH"
studiofs --help
```

`export` applies to the current terminal session. To make the setting permanent, add that line to `~/.zshrc` if you use the default interactive zsh, then open a new terminal window. Check which copy of the program the system found:

```bash
command -v studiofs
studiofs --version
```

After rebuilding, you need to update the copy in `~/.local/bin` separately. Otherwise, it is remarkably easy to spend an entire evening testing the new version with an old executable.

## Preparing a place to mount

A mount point is a local folder through which you will see the volume's files. For your first mount using regular macFUSE, an empty folder in your home directory is a convenient choice:

```bash
mkdir -p "$HOME/StudioFS/myvolume"
ls -A "$HOME/StudioFS/myvolume"
```

The second command should show no files. If it shows anything, choose another empty folder. Do not mount a volume over a directory containing your current project's material: mounting will hide its ordinary contents until you unmount.

In this example, the volume is called `myvolume`, and the final folder has the same name. That is deliberate: if the path you specify does not end with the volume name, StudioFS appends it automatically. Keep the cache separately on a local SSD, outside the mounted volume.

## First run: what you can check now

If `studiofs --help` works, the executable starts. This is **not yet a test of mounting or macFUSE permissions**.

After creating a volume with `init` or connecting to an existing one with `discover`, the command will look like this:

```bash
studiofs mount myvolume "$HOME/StudioFS/myvolume" --foreground
```

This example assumes that the local client is already set up for the `myvolume` volume. If it is not, first follow Chapter 13 or [Chapter 16 on connecting a second computer](16-connect-a-second-computer-studiofs-discover.md). Do not create a new volume in place of the one your team invited you to join.

Leave the terminal running the mount open. In another window, you can open the folder in Finder:

```bash
open "$HOME/StudioFS/myvolume"
```

Use test material for your first read and write checks, following Chapters 14–15. Before unmounting, close files and applications using the volume, then run:

```bash
studiofs umount "$HOME/StudioFS/myvolume"
```

If macFUSE refuses to mount, revisit system permissions and restarting. If StudioFS reports a missing volume or a problem accessing PostgreSQL or S3, check the relevant settings: reinstalling the driver will not fix the database password.

## What about FSKit?

FSKit in StudioFS remains **experimental and must be enabled separately**. It is not an automatic replacement for regular macFUSE, nor a promise of the same readiness for a production project. The implementation uses macFUSE's `MFMount.framework`; it requires macOS **15.4+**, macFUSE **5.3.3+**, an available framework, and a mount point under `/Volumes`. If the framework was unavailable at build time, selecting FSKit will return an unsupported-backend error.

For a regular installation, leave `STUDIOFS_MOUNT_BACKEND` unset. If you previously experimented in this terminal, return to the default path with:

```bash
unset STUDIOFS_MOUNT_BACKEND
```

It is better to experiment with FSKit separately, on a test volume, following the [technical documentation](../../platform-bridges.md#macos-backend-selection). You do not need it for your first mount using this guide.

## What next?

Your Mac is prepared when StudioFS starts, macFUSE is installed with the necessary system permissions, and you have chosen an empty folder for the volume. We will test the actual mount after setting up the volume. If you do not need Linux, you can go straight to [creating your first volume](13-create-your-first-volume-studiofs-init.md).

---

[Contents](README.md) · [Українська](../uk/11-install-studiofs-on-macos.md) · [← Previous](10-install-studiofs-on-windows.md) · [Next →](12-install-studiofs-on-linux.md)

[Linear source](https://linear.app/valandreev/document/11-install-studiofs-on-macos-3e23441e6bbe)
