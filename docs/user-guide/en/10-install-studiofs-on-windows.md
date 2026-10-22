# 10. Install StudioFS on Windows
> **Work in progress**

On an editing workstation, StudioFS consists of two parts: the `studiofs.exe` application and WinFSP—a Windows component that lets it present a volume as a drive. First, we will install both parts and check that the application starts. We will connect S3 and create a volume later: a blank drive will not appear just because we downloaded the application.

The steps below use **PowerShell on Windows itself**, not inside WSL. The examples target Windows x64. Support for other architectures in the code does not by itself confirm that a specific build is ready for your computer.

## Install WinFSP first

Download and install **WinFSP 2026 or later** from the [official WinFSP repository’s releases page](https://github.com/winfsp/winfsp/releases). StudioFS uses several newer WinFSP features, so earlier versions are not supported. Windows may ask for administrator privileges because this installs a file system driver. If the installer asks you to restart, do so before checking StudioFS. On a workstation with restricted permissions, ask an administrator to handle this step.

## Download StudioFS

Open the [StudioFS releases page](https://github.com/valandreev/studiofs-cli/releases) and download the **Windows x64** build. If it comes in an archive, extract it to a separate folder. Copy all release files to a StudioFS folder in your user profile:

```powershell
$studiofsBin = Join-Path $env:LOCALAPPDATA 'Programs\StudioFS'
New-Item -ItemType Directory -Force -Path $studiofsBin | Out-Null
Copy-Item 'C:\path\to\extracted\release\*' -Destination $studiofsBin -Recurse
& (Join-Path $studiofsBin 'studiofs.exe') --version
& (Join-Path $studiofsBin 'studiofs.exe') --help
```

In the `Copy-Item` command, replace `C:\path\to\extracted\release` with the path to the extracted release folder. If the release includes separate instructions, follow them.

## Avoid typing the full path every time

In Windows Settings, search for **“Edit environment variables for your account”**, open the user **Path** variable, and add the folder returned by the `$studiofsBin` command. Add the folder itself, without `studiofs.exe`, and keep the existing entries. Changing the user Path does not require installing the application for all users.

Close PowerShell and open a new window. If a new tab in Windows Terminal still has the old environment, restart the entire Terminal. Now check:

```powershell
Get-Command studiofs
studiofs --version
studiofs --help
```

`Get-Command` shows which file will run. This helps when an older copy is still on the workstation. PowerShell also requires `.\` before the name of an application in the current folder: use `.\studiofs.exe --help` or the full path. For a path in quotes, use the call operator `&`, as in the example above.

## Permissions and first launch

After installing the driver, start working from a regular PowerShell session under the account used by your editing application. Do not automatically switch to “Run as administrator” whenever something fails: elevated and regular sessions may see mounted drives differently.

The account needs write access to StudioFS’s local state and cache folders. The cache also needs free space on a local drive. A protected system folder or network share is a poor choice for the first check. Access to S3 and PostgreSQL is controlled by separate credentials; Windows administrator privileges do not replace them.

If startup reports that a WinFSP DLL is missing, check its installation, the standard path, and the architecture match. If Windows blocks the downloaded file, verify its source and contact your administrator according to your studio’s rules. You do not need to change the PowerShell Execution Policy to run `studiofs.exe`—that setting is for scripts, not for installing a driver.

**If help opens, the client is ready to configure.**

---

[Contents](README.md) · [Українська](../uk/10-install-studiofs-on-windows.md) · [← Previous](09-check-your-infrastructure.md) · [Next →](11-install-studiofs-on-macos.md)
