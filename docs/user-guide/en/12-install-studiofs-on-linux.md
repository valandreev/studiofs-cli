# 12. Install StudioFS on Linux

On Linux, StudioFS mounts storage through **FUSE3**. This component lets the program present files from S3 as an ordinary folder: your editing application sees a path to the media, while StudioFS takes care of retrieving and saving it.

In this chapter, we will prepare the system, build the command-line program, and check that it starts. Creating a volume and actually mounting the storage come in the next chapters. The folder for our future drive will be empty for now—that is normal; the media has not arrived yet.

## Which Linux distribution should I use?

The repository has checks for **Ubuntu 24.04** and **Rocky Linux 9**. For testing a real mount on Rocky, the documentation requires **version 9.4 or later**. These are reference points for choosing a workstation system, rather than a promise that every distribution with a similar name has already been tested.

The commands below cover these two systems. On Debian, Fedora, Arch, and other distributions, the required packages and their names may differ; we do not yet have separate verified instructions for them here.

Building requires Git, a C/C++ compiler, build tools, `pkg-config`, and **Rust 1.95 or later** with Cargo. If you do not have Rust yet, install it using the [official Rust instructions](https://www.rust-lang.org/tools/install), then open a new terminal and check:

```bash
rustc --version
cargo --version
```

You will also need local disk space for the source code and build output, and an internet connection to download dependencies. Administrator privileges are needed for system packages; we will build and run the program as your regular user.

## Installing system dependencies

### Ubuntu 24.04

Open a terminal and run:

```bash
sudo apt-get update
sudo apt-get install -y build-essential git ca-certificates curl pkg-config \
  fuse3 libfuse3-dev libgtk-3-dev libxdo-dev
sudo apt-get install -y libappindicator3-dev || \
  sudo apt-get install -y libayatana-appindicator3-dev
```

`fuse3` provides mounting utilities, including `fusermount3`, while `libfuse3-dev` provides the files needed to build StudioFS. GTK and the indicator libraries are required by the desktop integration components included in the current program. Even if you plan to work mostly in the terminal, do not skip these build dependencies.

The last command first tries to install one indicator package, then tries an alternative if the first is unavailable. The repository's checks use the same choice.

### Rocky Linux 9.4 and later in the 9 series

Some required packages come from additional repositories. The commands below enable EPEL and CRB, which changes the system's package sources. On a workstation managed by your studio, coordinate this with your system administrator.

```bash
sudo dnf install -y dnf-plugins-core epel-release
sudo dnf config-manager --set-enabled crb
sudo dnf install -y ca-certificates curl gcc gcc-c++ git make \
  gtk3-devel libappindicator-gtk3-devel fuse3 fuse3-devel \
  libxdo-devel openssl-devel pkgconf-pkg-config
```

This list matches the dependencies in the Rocky verification script. If a package cannot be found, first check repository availability and your system version. Do not randomly replace FUSE3 with the older FUSE2: the current StudioFS Linux component specifically uses version 3.

## Checking FUSE3

The checks are the same on both systems:

```bash
pkg-config --modversion fuse3
fusermount3 --version
ls -l /dev/fuse
```

The first command should show the library version: building requires **libfuse3 3.2 or later**. The second checks that the `fusermount3` utility is available. The third shows the kernel device used for mounting.

If `/dev/fuse` is missing on a regular Linux system, an administrator can try loading the module:

```bash
sudo modprobe fuse
```

Then check for the device again. In a container or restricted environment, installing packages alone is not enough: the host must provide access to `/dev/fuse` and allow FUSE mounts. On a managed server, its administrator needs to make that change.

## Building StudioFS

For this installation method, we take the source code from the project's repository and build the program on your computer.

Go to the folder where you keep applications or source code, and run:

```bash
git clone https://github.com/valandreev/studiofs-cli.git
cd studiofs-cli
cargo build --release -p studiofs
```

The first build may take longer: Cargo downloads and compiles dependencies. Once it succeeds, the command-line program will be at `target/release/studiofs`. Check it before installing:

```bash
./target/release/studiofs --version
./target/release/studiofs --help
```

To use the same version across several workstations, build from a repository revision agreed with your team. The current development branch changes, so two builds made on different days may differ. Before updating important workstations, check compatibility on a test volume.

## Making the command available in the terminal

We will install the built file in your own user directory:

```bash
mkdir -p "$HOME/.local/bin"
install -m 755 target/release/studiofs "$HOME/.local/bin/studiofs"
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

Choose an empty folder in your home directory. For example, for a future volume named `studio`:

```bash
mkdir -p "$HOME/StudioFS/studio"
ls -ld "$HOME/StudioFS/studio"
```

The folder you create should belong to your user. Do not put media in it before mounting: ordinary local files will not automatically become volume files, and once the file system is mounted, it will hide the folder's existing contents.

Run StudioFS as the same user who will work with the media. There is no need to make the folder accessible to everyone with `chmod 777` or run the program through `sudo` to get past an unexplained access error. First check the folder's owner and access to `/dev/fuse`. These examples prepare a mount for your user; access from other accounts requires a separate permissions check.

## If something will not start

* **`pkg-config` cannot find `fuse3`.** Check that the development package is installed: `libfuse3-dev` on Ubuntu or `fuse3-devel` on Rocky. The `fusermount3` utility alone is not enough for building.
* **The build complains about the Rust version.** Compare `rustc --version` with the requirement for version 1.95 or later. Your system's Rust package may be older.
* **`studiofs: command not found`.** Run the file by its full path, `"$HOME/.local/bin/studiofs" --version`, and check your `PATH` settings.
* **A FUSE access error.** Check that `/dev/fuse` exists, its permissions, and any restrictions imposed by the environment. In a container, this is often a host configuration issue.

Successful `--version` and `--help` commands mean that the command-line program starts. They do not yet test a file system mount, PostgreSQL, or S3. We will check those when creating and mounting the first volume.

## What is next?

We now have the program, FUSE3, and a place for our future drive. In the next chapter, we will create the first volume with `studiofs init` and bring the PostgreSQL and S3 services prepared earlier together into one working storage system.

---

[Contents](README.md) · [Українська](../uk/12-install-studiofs-on-linux.md) · [← Previous](11-install-studiofs-on-macos.md) · [Next →](13-create-your-first-volume-studiofs-init.md)

[Linear source](https://linear.app/valandreev/document/12-install-studiofs-on-linux-24b19f5fa67c)
