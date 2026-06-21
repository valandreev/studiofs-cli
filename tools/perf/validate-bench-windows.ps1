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
        if (-not (Test-Path -LiteralPath $candidate)) {
            New-Item -ItemType Directory -Path $candidate | Out-Null
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    throw "Could not create a unique benchmark artifact directory under $Root"
}

function Quote-CommandPart {
    param([Parameter(Mandatory = $true)][string]$Value)
    if ($Value -match '\s') {
        return '"' + ($Value -replace '"', '\"') + '"'
    }
    return $Value
}

if ($Runs -lt 1) {
    throw "-Runs must be >= 1"
}

$repoRoot = Get-RepoRoot
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
$gitCommit = (& git -C $repoRoot rev-parse HEAD 2>$null)
$studiofsVersion = (& $StudioFsPath --version 2>$null)

[pscustomobject][ordered]@{
    platform             = "Windows"
    os                   = [System.Environment]::OSVersion.VersionString
    mount_point          = $MountPoint
    git_commit           = if ($LASTEXITCODE -eq 0) { $gitCommit } else { $null }
    studiofs_version     = $studiofsVersion
    benchmark_settings   = $settings
    artifact_layout_note = "Do not overwrite previous benchmark artifacts"
} | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $validationDir "environment.json") -Encoding utf8

$runDirs = @()
for ($run = 1; $run -le $Runs; $run += 1) {
    $runName = "run-{0:D3}" -f $run
    $runDir = Join-Path $validationDir $runName
    New-Item -ItemType Directory -Path $runDir | Out-Null
    $reportPath = Join-Path $runDir "report.json"
    $stdoutPath = Join-Path $runDir "stdout.txt"
    $commandPath = Join-Path $runDir "command.txt"

    $args = @(
        "bench",
        $MountPoint,
        "--block-size", $BlockSize,
        "--big-file-size", $BigFileSize,
        "--small-file-size", $SmallFileSize,
        "--small-file-count", $SmallFileCount.ToString(),
        "--threads", $Threads.ToString(),
        "--json-output", $reportPath
    )
    $commandLine = (Quote-CommandPart $StudioFsPath) + " " + (($args | ForEach-Object { Quote-CommandPart ([string]$_) }) -join " ")
    Set-Content -LiteralPath $commandPath -Value $commandLine -Encoding utf8

    $output = & $StudioFsPath @args 2>&1
    $exitCode = $LASTEXITCODE
    $output | Out-String | Set-Content -LiteralPath $stdoutPath -Encoding utf8
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
$lines.Add("- Mount point: ``$MountPoint``")
$lines.Add("- Git commit: ``$gitCommit``")
$lines.Add("- Runs: $Runs; recommended run count is 5")
$lines.Add("- Benchmark settings: block size $BlockSize, big file size $BigFileSize, small file size $SmallFileSize, small file count $SmallFileCount, threads $Threads")
$lines.Add("")
$lines.Add("Compare medians across runs first, then inspect p95 and p99 latency for tail regressions.")
$lines.Add("")
$lines.Add("## Artifacts")
$lines.Add("")
foreach ($runDir in $runDirs) {
    $lines.Add("- ``$runDir``: command.txt, stdout.txt, report.json")
}
$lines -join [Environment]::NewLine | Set-Content -LiteralPath $summaryPath -Encoding utf8

Write-Host "Benchmark validation artifacts: $validationDir"
