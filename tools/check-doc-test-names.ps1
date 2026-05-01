param(
    [string[]]$Docs = @()
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$CoreScript = Join-Path $ScriptDir "check-doc-test-names.py"
$ShellScript = Join-Path $ScriptDir "check-doc-test-names.sh"
$RunningOnWindows = if (Get-Variable IsWindows -ErrorAction SilentlyContinue) { $IsWindows } else { $true }

if (-not $RunningOnWindows -and (Test-Path -LiteralPath $ShellScript)) {
    & sh $ShellScript @Docs
    exit $LASTEXITCODE
}

$Python = Get-Command python -ErrorAction SilentlyContinue
if ($Python) {
    & $Python.Source $CoreScript @Docs
    exit $LASTEXITCODE
}

$Python3 = Get-Command python3 -ErrorAction SilentlyContinue
if ($Python3) {
    & $Python3.Source $CoreScript @Docs
    exit $LASTEXITCODE
}

$PyLauncher = Get-Command py -ErrorAction SilentlyContinue
if ($PyLauncher) {
    & $PyLauncher.Source -3 $CoreScript @Docs
    exit $LASTEXITCODE
}

throw "Python 3 is required to run check-doc-test-names."
