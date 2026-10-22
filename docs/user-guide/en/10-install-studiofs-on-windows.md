# 10. Install StudioFS on Windows

On an editing workstation, StudioFS has two parts: the `studiofs.exe` program and WinFSP, a Windows component that lets it present a volume as a drive. First, we will install both parts and check that the program starts. Connecting S3 and creating a volume come later: downloading the program alone will not make an empty drive appear.

Below, we use **PowerShell on Windows itself**, not inside WSL. The examples are for Windows x64. Support for other architectures in the code does not, by itself, confirm that a particular build is ready for your computer.

## WinFSP first

Download and install **WinFSP 2026 or newer** from the [official WinFSP GitHub releases](https://github.com/winfsp/winfsp/releases). StudioFS uses several newer WinFSP features, so earlier versions are not supported. Windows may ask for administrator privileges because you are installing a filesystem driver. If the installer asks you to restart, do so before checking StudioFS. On a workstation with restricted permissions, it is best to ask an administrator to handle this step.

## Downloading StudioFS

Open the [StudioFS releases page](https://github.com/valandreev/studiofs-cli/releases) and download the build for **Windows x64**. If it is packaged in an archive, extract it to a separate folder. Copy all release files to a StudioFS folder in your user profile:

```powershell
$studiofsBin = Join-Path $env:LOCALAPPDATA 'Programs\StudioFS'
New-Item -ItemType Directory -Force -Path $studiofsBin | Out-Null
Copy-Item 'C:\path\to\extracted\release\*' -Destination $studiofsBin -Recurse
& (Join-Path $studiofsBin 'studiofs.exe') --version
& (Join-Path $studiofsBin 'studiofs.exe') --help
```

In `Copy-Item`, replace `C:\path\to\extracted\release` with the path to the extracted release folder. Follow any separate release instructions.

## Avoid typing the full path every time

Add the StudioFS folder to your user Path:

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

**If help opens, the client is ready to configure.**

---

[Contents](README.md) · [Українська](../uk/10-install-studiofs-on-windows.md) · [← Previous](09-check-your-infrastructure.md) · [Next →](11-install-studiofs-on-macos.md)

[Linear source](https://linear.app/valandreev/document/10-install-studiofs-on-windows-4a790cf2e80d)
