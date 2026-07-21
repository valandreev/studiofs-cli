[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$StudioFsMountPoint,
    [Parameter(Mandatory = $true)]
    [string]$JuiceFsMountPoint,
    [Parameter(Mandatory = $true)]
    [string]$CurvineSource,
    [ValidateRange(3, 100)]
    [int]$Runs = 3,
    [ValidateRange(1, 1000000)]
    [int]$SmallFileCount = 10000,
    [ValidateRange(1, 1048576)]
    [int]$SmallFileSizeBytes = 4096,
    [Parameter(Mandatory = $true)]
    [string]$ColdPreparationCommand,
    [string]$StudioFsMountOptions = "",
    [string]$JuiceFsMountOptions = "",
    [string]$StorageNotes = "",
    [string]$AntivirusAndIndexingNotes = "",
    [string]$OutputRoot = "",
    [switch]$KeepWorkloads
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RepoRoot { (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\..")).Path }

function New-UniqueDirectory {
    param([string]$Root, [string]$Name)
    New-Item -ItemType Directory -Force -Path $Root | Out-Null
    for ($suffix = 0; $suffix -lt 1000; $suffix += 1) {
        $candidateName = if ($suffix -eq 0) { $Name } else { "$Name-$($suffix + 1)" }
        $candidate = Join-Path $Root $candidateName
        try {
            New-Item -ItemType Directory -Path $candidate -ErrorAction Stop | Out-Null
            return (Resolve-Path -LiteralPath $candidate).Path
        }
        catch {
            if (Test-Path -LiteralPath $candidate -PathType Container) { continue }
            throw
        }
    }
    throw "Could not create a unique directory under $Root"
}

function Write-Utf8NoBom {
    param([string]$Path, [string]$Value)
    [System.IO.File]::WriteAllText($Path, $Value, (New-Object System.Text.UTF8Encoding($false)))
}

function Get-MountInfo {
    param([string]$MountPoint, [string]$MountOptions)
    $resolved = (Resolve-Path -LiteralPath $MountPoint).Path
    $root = [System.IO.Path]::GetPathRoot($resolved)
    $volume = Get-Volume -DriveLetter $root.Substring(0, 1) -ErrorAction SilentlyContinue
    [pscustomobject][ordered]@{
        path          = $resolved
        filesystem    = if ($volume) { $volume.FileSystem } else { $null }
        size_bytes    = if ($volume) { $volume.Size } else { $null }
        free_bytes    = if ($volume) { $volume.SizeRemaining } else { $null }
        mount_options = $MountOptions
    }
}

function New-WorkloadRoot {
    param([string]$MountPath, [string]$Name)
    $root = New-UniqueDirectory -Root $MountPath -Name $Name
    $mountFullPath = [System.IO.Path]::GetFullPath($MountPath).TrimEnd('\')
    $rootFullPath = [System.IO.Path]::GetFullPath($root)
    if (-not $rootFullPath.StartsWith($mountFullPath + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to use workload directory outside mount point: $rootFullPath"
    }
    $root
}

function Remove-WorkloadRoot {
    param([string]$MountPath, [string]$WorkloadRoot)
    $mountFullPath = [System.IO.Path]::GetFullPath($MountPath).TrimEnd('\')
    $rootFullPath = [System.IO.Path]::GetFullPath($WorkloadRoot)
    if (-not $rootFullPath.StartsWith($mountFullPath + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove directory outside mount point: $rootFullPath"
    }
    Remove-Item -LiteralPath $rootFullPath -Recurse -Force
}

function Invoke-MetadataCreateWorkload {
    param(
        [string]$Filesystem,
        [string]$MountPath,
        [string]$Condition,
        [int]$Run,
        [string]$CurvineSourcePath,
        [int]$FileCount,
        [int]$FileSizeBytes
    )
    $workloadRoot = New-WorkloadRoot -MountPath $MountPath -Name (".sfs-612-{0}-{1}-{2:D3}-{3}" -f $Filesystem.ToLowerInvariant(), $Condition, $Run, [guid]::NewGuid().ToString("N"))
    try {
        $copyTimer = [System.Diagnostics.Stopwatch]::StartNew()
        & robocopy $CurvineSourcePath (Join-Path $workloadRoot "curvine") /E /MT:16 /R:0 /W:0 /NFL /NDL /NJH /NJS /NP | Out-Null
        $copyExitCode = $LASTEXITCODE
        if ($copyExitCode -gt 7) { throw "robocopy failed with exit code $copyExitCode" }
        $copyTimer.Stop()

        $smallFilesRoot = Join-Path $workloadRoot "small-files"
        New-Item -ItemType Directory -Path $smallFilesRoot | Out-Null
        $payload = New-Object byte[] $FileSizeBytes
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try {
            $rng.GetBytes($payload)
        }
        finally {
            $rng.Dispose()
        }
        $createTimer = [System.Diagnostics.Stopwatch]::StartNew()
        for ($index = 0; $index -lt $FileCount; $index += 1) {
            [System.IO.File]::WriteAllBytes((Join-Path $smallFilesRoot ("f-{0:D8}.bin" -f $index)), $payload)
        }
        $createTimer.Stop()

        [pscustomobject][ordered]@{
            filesystem       = $Filesystem
            condition        = $Condition
            run              = $Run
            curvine_copy_ms  = [math]::Round($copyTimer.Elapsed.TotalMilliseconds, 3)
            small_create_ms  = [math]::Round($createTimer.Elapsed.TotalMilliseconds, 3)
            total_ms         = [math]::Round(($copyTimer.Elapsed + $createTimer.Elapsed).TotalMilliseconds, 3)
            small_file_count = $FileCount
            small_file_bytes = $FileSizeBytes
        }
    }
    finally {
        if (-not $KeepWorkloads -and (Test-Path -LiteralPath $workloadRoot -PathType Container)) {
            try {
                Remove-WorkloadRoot -MountPath $MountPath -WorkloadRoot $workloadRoot
            }
            catch {
                Write-Warning ("Failed to clean up workload root {0}: {1}" -f $workloadRoot, $_.Exception.Message)
            }
        }
    }
}

function Get-Median {
    param([double[]]$Values)
    $ordered = @($Values | Sort-Object)
    $middle = [int][math]::Floor($ordered.Count / 2)
    if (($ordered.Count % 2) -eq 1) { return $ordered[$middle] }
    return ($ordered[$middle - 1] + $ordered[$middle]) / 2
}

$repoRoot = Get-RepoRoot
$curvine = (Resolve-Path -LiteralPath $CurvineSource).Path
if (-not (Test-Path -LiteralPath $curvine -PathType Container)) { throw "-CurvineSource must be a directory" }
if ([string]::IsNullOrWhiteSpace($ColdPreparationCommand)) { throw "-ColdPreparationCommand is required so cold-cache runs are controlled and reproducible" }

$studiofs = Get-MountInfo -MountPoint $StudioFsMountPoint -MountOptions $StudioFsMountOptions
$juicefs = Get-MountInfo -MountPoint $JuiceFsMountPoint -MountOptions $JuiceFsMountOptions
if ($studiofs.path -eq $juicefs.path) { throw "StudioFS and JuiceFS mount points must be different" }
if ([string]::IsNullOrWhiteSpace($OutputRoot)) { $OutputRoot = Join-Path $repoRoot "target\perf" }
$artifactDir = New-UniqueDirectory -Root $OutputRoot -Name ("sfs-612-windows-metadata-create-" + (Get-Date -Format "yyyyMMdd-HHmmss"))

$os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
$cpu = Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue | Select-Object -First 1
$environment = [pscustomobject][ordered]@{
    issue                         = "SFS-612"
    platform                      = "Windows"
    os                            = if ($os) { $os.Caption } else { [System.Environment]::OSVersion.VersionString }
    os_version                    = if ($os) { $os.Version } else { $null }
    cpu                           = if ($cpu) { $cpu.Name.Trim() } else { $null }
    logical_processors            = if ($cpu) { $cpu.NumberOfLogicalProcessors } else { $null }
    studiofs_mount                = $studiofs
    juicefs_mount                 = $juicefs
    curvine_source                = $curvine
    small_file_count              = $SmallFileCount
    small_file_size_bytes         = $SmallFileSizeBytes
    runs_per_condition            = $Runs
    cold_preparation_command      = $ColdPreparationCommand
    storage_notes                 = $StorageNotes
    antivirus_and_indexing_notes  = $AntivirusAndIndexingNotes
    git_commit                    = (& git -C $repoRoot rev-parse HEAD).Trim()
} | ConvertTo-Json -Depth 10
Write-Utf8NoBom -Path (Join-Path $artifactDir "environment.json") -Value $environment

$results = New-Object System.Collections.Generic.List[object]
foreach ($target in @(
    [pscustomobject]@{ filesystem = "StudioFS"; mount = $studiofs.path },
    [pscustomobject]@{ filesystem = "JuiceFS"; mount = $juicefs.path }
)) {
    for ($run = 1; $run -le $Runs; $run += 1) {
        Write-Host "[$($target.filesystem)] cold run $run of $Runs"
        & ([scriptblock]::Create($ColdPreparationCommand))
        if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) { throw "Cold preparation failed with exit code $LASTEXITCODE" }
        $results.Add((Invoke-MetadataCreateWorkload -Filesystem $target.filesystem -MountPath $target.mount -Condition "cold" -Run $run -CurvineSourcePath $curvine -FileCount $SmallFileCount -FileSizeBytes $SmallFileSizeBytes))

        Write-Host "[$($target.filesystem)] warm run $run of $Runs"
        $results.Add((Invoke-MetadataCreateWorkload -Filesystem $target.filesystem -MountPath $target.mount -Condition "warm" -Run $run -CurvineSourcePath $curvine -FileCount $SmallFileCount -FileSizeBytes $SmallFileSizeBytes))
    }
}

Write-Utf8NoBom -Path (Join-Path $artifactDir "results.json") -Value ($results | ConvertTo-Json -Depth 6)
$summary = New-Object System.Collections.Generic.List[string]
$summary.Add("# SFS-612 Windows metadata-create baseline")
$summary.Add("")
$summary.Add("| Filesystem | Cache state | Curvine copy median (ms) | 4 KiB create median (ms) | Total median (ms) |")
$summary.Add("|---|---|---:|---:|---:|")
foreach ($filesystem in @("StudioFS", "JuiceFS")) {
    foreach ($condition in @("cold", "warm")) {
        $group = @($results | Where-Object { $_.filesystem -eq $filesystem -and $_.condition -eq $condition })
        $summary.Add(("| {0} | {1} | {2:N3} | {3:N3} | {4:N3} |" -f $filesystem, $condition, (Get-Median ([double[]]$group.curvine_copy_ms)), (Get-Median ([double[]]$group.small_create_ms)), (Get-Median ([double[]]$group.total_ms))))
    }
}
$summary.Add("")
$summary.Add('`environment.json` records the machine, filesystems, mount options, cache preparation command, storage notes, and antivirus/indexing notes. `results.json` contains every raw run.')
Write-Utf8NoBom -Path (Join-Path $artifactDir "summary.md") -Value ($summary -join [Environment]::NewLine)
Write-Host "SFS-612 artifacts: $artifactDir"
