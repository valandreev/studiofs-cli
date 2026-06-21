param(
    [Parameter(Mandatory = $true)]
    [string]$MountPoint,
    [int]$Runs = 5,
    [string]$StudioFsPath = "studiofs",
    [string]$OutputRoot = "",
    [string]$BlockSize = "4MB",
    [string]$BigFileSize = "1GB",
    [string]$SmallFileSize = "128KB",
    [int]$SmallFileCount = 1000,
    [int]$Threads = 4
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RepoRoot {
    return (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\..")).Path
}

function New-UniqueDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    New-Item -ItemType Directory -Force -Path $Root | Out-Null
    for ($suffix = 0; $suffix -lt 1000; $suffix += 1) {
        $candidateName = if ($suffix -eq 0) { $Name } else { "$Name-$($suffix + 1)" }
        $candidate = Join-Path $Root $candidateName
        try {
            New-Item -ItemType Directory -Path $candidate -ErrorAction Stop | Out-Null
            return (Resolve-Path -LiteralPath $candidate).Path
        }
        catch {
            if (Test-Path -LiteralPath $candidate -PathType Container) {
                continue
            }
            throw
        }
    }

    throw "Could not create a unique benchmark artifact directory under $Root"
}

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Value, $utf8NoBom)
}

function Quote-CommandPart {
    param([Parameter(Mandatory = $true)][string]$Value)
    if ($Value -match '\s') {
        $escaped = $Value -replace '(\\*)"', '$1$1\"'
        $escaped = $escaped -replace '(\\+)$', '$1$1'
        return '"' + $escaped + '"'
    }
    return $Value
}

function Resolve-StudioFsCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot
    )

    $command = Get-Command $Path -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }

    if (Test-Path -LiteralPath $Path -PathType Leaf) {
        return (Resolve-Path -LiteralPath $Path).Path
    }

    if ($Path -eq "studiofs") {
        foreach ($candidate in @(
            (Join-Path $RepoRoot "target\release\studiofs.exe"),
            (Join-Path $RepoRoot "target\debug\studiofs.exe")
        )) {
            if (Test-Path -LiteralPath $candidate -PathType Leaf) {
                return (Resolve-Path -LiteralPath $candidate).Path
            }
        }
    }

    throw "Error: studiofs binary not found or not executable at: $Path"
}

function Invoke-OptionalCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$CommandArguments
    )

    try {
        $output = & $FilePath @CommandArguments 2>$null
        $exitCode = $LASTEXITCODE
        return [pscustomobject]@{
            success = $exitCode -eq 0
            output  = if ($exitCode -eq 0) { ($output | Out-String).Trim() } else { $null }
        }
    }
    catch {
        return [pscustomobject]@{
            success = $false
            output  = $null
        }
    }
}

if ($Runs -lt 1) {
    throw "-Runs must be >= 1"
}
if ($SmallFileCount -lt 1) {
    throw "-SmallFileCount must be >= 1"
}
if ($Threads -lt 1) {
    throw "-Threads must be >= 1"
}

$repoRoot = Get-RepoRoot
$StudioFsPath = Resolve-StudioFsCommand -Path $StudioFsPath -RepoRoot $repoRoot
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot "target\perf"
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$validationDir = New-UniqueDirectory -Root $OutputRoot -Name "windows-bench-validation-$timestamp"
$settings = [ordered]@{
    block_size       = $BlockSize
    big_file_size    = $BigFileSize
    small_file_size  = $SmallFileSize
    small_file_count = $SmallFileCount
    threads          = $Threads
    run_count        = $Runs
}
$gitCommitResult = Invoke-OptionalCommand -FilePath "git" -CommandArguments @("-C", $repoRoot, "rev-parse", "HEAD")
$studiofsVersionResult = Invoke-OptionalCommand -FilePath $StudioFsPath -CommandArguments @("--version")
$gitCommit = $gitCommitResult.output
$studiofsVersion = $studiofsVersionResult.output
$git_success = $gitCommitResult.success

$environmentJson = [pscustomobject][ordered]@{
    platform             = "Windows"
    os                   = [System.Environment]::OSVersion.VersionString
    mount_point          = $MountPoint
    git_commit           = if ($git_success) { $gitCommit } else { $null }
    studiofs_version     = $studiofsVersion
    benchmark_settings   = $settings
    artifact_layout_note = "Do not overwrite previous benchmark artifacts"
} | ConvertTo-Json -Depth 20
Write-Utf8NoBom -Path (Join-Path $validationDir "environment.json") -Value $environmentJson

$runDirs = @()
for ($run = 1; $run -le $Runs; $run += 1) {
    $runName = "run-{0:D3}" -f $run
    $runDir = Join-Path $validationDir $runName
    New-Item -ItemType Directory -Path $runDir | Out-Null
    $reportPath = Join-Path $runDir "report.json"
    $stdoutPath = Join-Path $runDir "stdout.txt"
    $commandPath = Join-Path $runDir "command.txt"

    $benchArgs = @(
        "bench",
        $MountPoint,
        "--block-size", $BlockSize,
        "--big-file-size", $BigFileSize,
        "--small-file-size", $SmallFileSize,
        "--small-file-count", $SmallFileCount.ToString(),
        "--threads", $Threads.ToString(),
        "--json-output", $reportPath
    )
    $commandLine = (Quote-CommandPart $StudioFsPath) + " " + (($benchArgs | ForEach-Object { Quote-CommandPart ([string]$_) }) -join " ")
    Write-Utf8NoBom -Path $commandPath -Value $commandLine

    Write-Host "Starting benchmark run $run of $Runs..."
    $oldEap = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $StudioFsPath @benchArgs > $stdoutPath 2>&1
        $exitCode = $LASTEXITCODE
    }
    catch {
        Write-Utf8NoBom -Path $stdoutPath -Value ($_ | Out-String)
        $exitCode = 1
    }
    finally {
        $ErrorActionPreference = $oldEap
    }
    if ($exitCode -ne 0) {
        throw "studiofs bench failed with exit code $exitCode. See $stdoutPath"
    }
    if (-not (Test-Path -LiteralPath $reportPath)) {
        throw "studiofs bench did not write report.json for $runName"
    }

    $runDirs += $runDir
}

$summaryPath = Join-Path $validationDir "summary.md"
$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("# StudioFS Windows benchmark validation")
$lines.Add("")
$lines.Add("- Platform: Windows")
$lines.Add("- Mount point: " + '`' + $MountPoint + '`')
$gitCommitText = if ($gitCommit) { '`' + $gitCommit + '`' } else { "N/A" }
$lines.Add("- Git commit: $gitCommitText")
$lines.Add("- Runs: $Runs; recommended run count is 5")
$lines.Add("- Benchmark settings: block size $BlockSize, big file size $BigFileSize, small file size $SmallFileSize, small file count $SmallFileCount, threads $Threads")
$lines.Add("")
$lines.Add("Compare medians across runs first, then inspect p95 and p99 latency for tail regressions.")
$lines.Add("")
$lines.Add("## Artifacts")
$lines.Add("")
foreach ($runDir in $runDirs) {
    $lines.Add("- " + '`' + $runDir + '`' + ": command.txt, stdout.txt, report.json")
}
Write-Utf8NoBom -Path $summaryPath -Value ($lines -join [Environment]::NewLine)

Write-Host "Benchmark validation artifacts: $validationDir"
