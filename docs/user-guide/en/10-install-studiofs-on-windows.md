# 10. Install StudioFS on Windows

> **English translation draft awaiting editorial and technical review. Not yet approved.**

On an editing workstation, StudioFS has two parts: the `studiofs.exe` program and WinFSP, a Windows component that lets it present a volume as a drive. First, we will install both parts and check that the program starts. Connecting S3 and creating a volume come later: downloading the program alone will not make an empty drive appear.

Below, we use **PowerShell on Windows itself**, not inside WSL. The examples are for Windows x64. Support for other architectures in the code does not, by itself, confirm that a particular build is ready for your computer.

## WinFSP first

Visit the [official WinFSP website](https://winfsp.dev/), download the installer, and install the component. Windows may ask for administrator privileges because you are installing a filesystem driver. If the installer asks you to restart, do so before checking StudioFS. On a workstation with restricted permissions, it is best to ask an administrator to handle this step.

Keep the default installation location unless you have a reason to change it. StudioFS looks for the WinFSP library alongside its executable and in the standard `WinFsp\bin` folder under `Program Files (x86)` or `Program Files`. An x64 build needs `winfsp-x64.dll`. For example, with a default installation, you can check:

```powershell
Test-Path 'C:\Program Files (x86)\WinFsp\bin\winfsp-x64.dll'
```

`True` confirms that the file exists at that exact location, but does not yet verify that the driver works. If you see `False`, check the installation location and selected architecture. Copying a random DLL from the internet is no substitute for installing WinFSP.

## Where to get StudioFS

A prebuilt `studiofs.exe`, if the team provides one for your version, saves you the time needed to build it. Refer to the [StudioFS repository](https://github.com/valandreev/studiofs-cli) and its [releases page](https://github.com/valandreev/studiofs-cli/releases). Check the description of the specific release and the file's architecture.

The current repository documents building from source. We will follow that route below; if the team has provided a prebuilt binary for your version of Windows, skip ahead to checking that it starts.

To build StudioFS, you need [Git for Windows](https://git-scm.com/downloads/win), [Rust through rustup](https://www.rust-lang.org/tools/install), and Microsoft's C++ tools: **Build Tools for Visual Studio**, the **Desktop development with C++** workload, and the Windows SDK. Choose Rust with the MSVC toolchain for Windows. The current `Cargo.toml` requires **Rust 1.95 or newer**; for a different StudioFS version, check the requirement in that version's source code. When installing WinFSP for a source build, include the development tools—SDK / Developer—if the installer offers them separately.

After installing the tools, open a new PowerShell window and check:

```powershell
git --version
rustc --version
cargo --version
rustup show active-toolchain
```

On a typical x64 workstation, the active toolchain's name should contain `x86_64-pc-windows-msvc`. If a command is not found, fix the tools' installation or open a new terminal first.

In the folder where you keep source code, run:

```powershell
git clone https://github.com/valandreev/studiofs-cli.git
Set-Location .\studiofs-cli
cargo build --release -p studiofs --bin studiofs
```

This builds the StudioFS CLI and its dependencies. The first build downloads dependencies and may take longer than you would like with a project delivery looming. Do not continue if Cargo exits with an error. An error about `link.exe` usually points to missing or unavailable MSVC tools; errors about WinFSP libraries call for checking its installation and development tools.

After a successful build using Cargo's default output directory, the program will be here:

```powershell
.\target\release\studiofs.exe --version
.\target\release\studiofs.exe --help
```

The program itself should print its current version and help text. If you set a different `CARGO_TARGET_DIR` or an explicit target, the output path will differ.

## Avoid typing the full path every time

Create a separate folder in your user profile and copy the resulting file there. Run the following commands from the repository root after a successful build:

```powershell
$studiofsBin = Join-Path $env:LOCALAPPDATA 'Programs\StudioFS'
New-Item -ItemType Directory -Force -Path $studiofsBin | Out-Null
Copy-Item .\target\release\studiofs.exe -Destination $studiofsBin
& (Join-Path $studiofsBin 'studiofs.exe') --version
```

If you have a prebuilt file from the team, put it in the same folder. If the release includes additional files and separate instructions, follow those instructions.

In Windows settings, search for **“Edit environment variables for your account”**, open the user **Path** variable, and add the folder shown by `$studiofsBin`. Add the folder itself, without `studiofs.exe`, and preserve the existing entries. Changing your user Path does not require installing the program for all users.

Close PowerShell and open a new window. If you use Windows Terminal and a new tab retains the old environment, restart the entire Terminal application. Now check:

```powershell
Get-Command studiofs
studiofs --version
studiofs --help
```

`Get-Command` shows exactly which file will run. This helps if an older copy is still on the workstation. PowerShell also requires `.\` before a program name when running it from the current folder: use `.\studiofs.exe --help` or the full path. A quoted path needs the `&` operator, as in the example above.

## Permissions and the first run

After installing the driver, start with a regular PowerShell session under the same account you use for your editing application. Do not automatically switch to “Run as administrator” whenever an error appears: elevated and regular sessions may see mounted drives differently.

Your account needs write access to StudioFS's local state and cache folders. The cache also needs free space on a local drive. A protected system folder or network share is a poor choice for the first check. Access to S3 and PostgreSQL depends on separate credentials; Windows administrator privileges do not replace them.

If startup reports a missing WinFSP DLL, check its installation, the standard path, and the architecture match. If Windows blocks a downloaded file, check its source and contact your administrator according to your studio's policies. You do not need to change PowerShell Execution Policy to run `studiofs.exe`—that setting controls scripts and is not a way to install a driver.

**If help opens, the client is ready to configure.** This does not yet verify storage access or a real mount. If your workstation runs Windows, you can skip the macOS and Linux installation chapters and move on to [creating your first volume](13-create-your-first-volume-studiofs-init.md). If a colleague has already created the volume, see [connecting a second computer](16-connect-a-second-computer-studiofs-discover.md). For the mount, prepare an unused drive letter, such as `Z:`; we will cover the commands and verify operation in the [chapter on mounting](14-mount-your-volume-studiofs-mount.md).

---

[Contents](README.md) · [Українська](../uk/10-install-studiofs-on-windows.md) · [← Previous](09-check-your-infrastructure.md) · [Next →](11-install-studiofs-on-macos.md)

[Linear source](https://linear.app/valandreev/document/10-install-studiofs-on-windows-4a790cf2e80d)
