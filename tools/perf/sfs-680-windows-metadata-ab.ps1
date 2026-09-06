[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z]:$')]
    [string]$MountPoint,
    [string]$BaselineCommit = "3e7cf531",
    [string]$CandidateCommit = "",
    [ValidateRange(1, 10)]
    [int]$Runs = 3,
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [System.Globalization.CultureInfo]::InvariantCulture
[System.Threading.Thread]::CurrentThread.CurrentUICulture = [System.Globalization.CultureInfo]::InvariantCulture

$measurementColumns = @(
    "get_security_by_name",
    "open",
    "get_file_info",
    "read_directory",
    "cleanup",
    "close",
    "path_resolution",
    "metadata_lookup",
    "metadata_getattr",
    "acl_check",
    "readdir",
    "get_attrs_batch",
    "directory_buffer",
    "dentry_cache_hits",
    "dentry_cache_misses",
    "path_shortcut_cache_hits",
    "path_shortcut_cache_misses",
    "attr_read_cache_hits",
    "attr_read_cache_misses",
    "native_cache_enabled_open",
    "native_cache_disabled_open"
)
$numericMeasurementColumns = @("elapsed_ns") + $measurementColumns
$callbackColumns = @(
    "get_security_by_name",
    "open",
    "get_file_info",
    "read_directory",
    "cleanup",
    "close"
)

function Get-RepoRoot {
    (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\..")).Path
}

function Write-Utf8NoBom {
    param(
        [string]$Path,
        [string]$Value
    )
    [System.IO.File]::WriteAllText(
        $Path,
        $Value,
        (New-Object System.Text.UTF8Encoding($false))
    )
}

function New-UniqueDirectory {
    param(
        [string]$Root,
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
    throw "Could not create a unique directory under $Root"
}

function Get-DotEnvValue {
    param([string]$Path, [string]$Key)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required repository .env is missing: $Path"
    }
    $prefix = '^\s*' + [regex]::Escape($Key) + '\s*=\s*(.*)$'
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -notmatch $prefix) {
            continue
        }
        $value = $Matches[1].Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        return $value
    }
    throw "Required .env key is missing: $Key"
}

function Resolve-Commit {
    param([string]$RepoRoot, [string]$Commit)
    $resolved = (& git -C $RepoRoot rev-parse --verify "$Commit^{commit}").Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($resolved)) {
        throw "Commit does not resolve to a commit: $Commit"
    }
    $resolved
}

function Get-FileIdentity {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Expected file is missing: $Path"
    }
    $item = Get-Item -LiteralPath $Path
    $hash = (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    [pscustomobject][ordered]@{
        path                 = $item.FullName
        length_bytes         = [int64]$item.Length
        sha256               = $hash
        last_write_time_utc  = $item.LastWriteTimeUtc.ToString("o")
    }
}

function Get-SystemLoadObservation {
    $sampledAt = (Get-Date).ToUniversalTime()
    $processors = @(Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue)
    $loadValues = @(
        $processors |
            Where-Object { $null -ne $_.LoadPercentage } |
            ForEach-Object { [double]$_.LoadPercentage }
    )
    $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
    [pscustomobject][ordered]@{
        available                    = ($loadValues.Count -gt 0 -or $null -ne $os)
        sampled_at_utc               = $sampledAt.ToString("o")
        processor_load_percent      = if ($loadValues.Count -gt 0) {
            [math]::Round((($loadValues | Measure-Object -Average).Average), 3)
        }
        else { $null }
        free_physical_memory_kib     = if ($null -ne $os) {
            [int64]$os.FreePhysicalMemory
        }
        else { $null }
    }
}

function Start-ProcessHandle {
    param(
        [string]$FilePath,
        [string]$Worktree,
        [string[]]$Arguments
    )
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $FilePath
    $startInfo.WorkingDirectory = $Worktree
    $startInfo.Arguments = $Arguments -join " "
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "Could not start $FilePath in $Worktree"
    }
    [pscustomobject][ordered]@{
        process     = $process
        stdout_task = $process.StandardOutput.ReadToEndAsync()
        stderr_task = $process.StandardError.ReadToEndAsync()
    }
}

function Start-CargoProcess {
    param(
        [string]$Worktree,
        [string[]]$Arguments
    )
    Start-ProcessHandle -FilePath "cargo.exe" -Worktree $Worktree -Arguments $Arguments
}

function Complete-CargoProcess {
    param(
        [pscustomobject]$Handle,
        [string]$StdoutPath,
        [string]$StderrPath
    )
    $Handle.process.WaitForExit()
    $stdout = $Handle.stdout_task.GetAwaiter().GetResult()
    $stderr = $Handle.stderr_task.GetAwaiter().GetResult()
    Write-Utf8NoBom -Path $StdoutPath -Value $stdout
    Write-Utf8NoBom -Path $StderrPath -Value $stderr
    [pscustomobject][ordered]@{
        exit_code = [int]$Handle.process.ExitCode
        stdout    = $stdout
        stderr    = $stderr
    }
}

function Get-DescendantProcessIds {
    param([int]$RootProcessId)
    $parents = @{}
    foreach ($process in @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)) {
        $parents[[int]$process.ProcessId] = [int]$process.ParentProcessId
    }
    $ids = [System.Collections.Generic.HashSet[int]]::new()
    $queue = [System.Collections.Generic.Queue[int]]::new()
    $ids.Add($RootProcessId) | Out-Null
    $queue.Enqueue($RootProcessId)
    while ($queue.Count -gt 0) {
        $parentId = $queue.Dequeue()
        foreach ($entry in $parents.GetEnumerator()) {
            if ($entry.Value -eq $parentId -and $ids.Add([int]$entry.Key)) {
                $queue.Enqueue([int]$entry.Key)
            }
        }
    }
    [int[]]$ids
}

function Get-BenchmarkProcessIds {
    param(
        [int]$RootProcessId,
        [datetime]$StartedAt
    )
    $ids = [System.Collections.Generic.HashSet[int]]::new()
    $ids.Add($RootProcessId) | Out-Null
    foreach ($process in @(Get-Process -ErrorAction SilentlyContinue)) {
        if ($process.ProcessName -ne "cargo" -and
            $process.ProcessName -notlike "mount_sanity*" -and
            $process.ProcessName -notlike "studiofs*") {
            continue
        }
        try {
            if ($process.StartTime -ge $StartedAt) {
                $ids.Add([int]$process.Id) | Out-Null
            }
        }
        catch {
            # A short-lived process can disappear while its start time is read.
        }
    }
    foreach ($processId in @(Get-DescendantProcessIds -RootProcessId $RootProcessId)) {
        $ids.Add([int]$processId) | Out-Null
    }
    [int[]]$ids
}

function Invoke-CargoBuild {
    param(
        [string]$Worktree,
        [string]$Side,
        [string]$ArtifactDirectory
    )
    $stdoutPath = Join-Path $ArtifactDirectory ("{0}-build.stdout.log" -f $Side)
    $stderrPath = Join-Path $ArtifactDirectory ("{0}-build.stderr.log" -f $Side)
    $arguments = @(
        "test",
        "-p",
        "studiofs-winfsp",
        "--release",
        "--test",
        "mount_sanity",
        "--no-run",
        "--message-format=json"
    )
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    $handle = Start-CargoProcess -Worktree $Worktree -Arguments $arguments
    $cargo = $handle.process
    while (-not $cargo.HasExited) {
        Start-Sleep -Milliseconds 100
        $cargo.Refresh()
    }
    $completed = Complete-CargoProcess -Handle $handle -StdoutPath $stdoutPath -StderrPath $stderrPath
    $timer.Stop()
    if ($completed.exit_code -ne 0) {
        throw "$Side release build failed with cargo exit code $($completed.exit_code). See $stdoutPath and $stderrPath"
    }
    $artifacts = @()
    foreach ($line in @($completed.stdout -split "`r?`n")) {
        try {
            $message = $line | ConvertFrom-Json -ErrorAction Stop
        }
        catch {
            continue
        }
        if ($null -eq $message) {
            continue
        }
        $reason = $message.PSObject.Properties["reason"]
        $target = $message.PSObject.Properties["target"]
        $executable = $message.PSObject.Properties["executable"]
        $targetName = $null
        if ($null -ne $target -and $null -ne $target.Value) {
            $targetName = $target.Value.PSObject.Properties["name"]
        }
        if ($null -ne $reason -and
            $reason.Value -eq "compiler-artifact" -and
            $null -ne $targetName -and
            $targetName.Value -eq "mount_sanity" -and
            $null -ne $executable -and
            -not [string]::IsNullOrWhiteSpace($executable.Value)) {
            $artifacts += $executable.Value
        }
    }
    if ($artifacts.Count -eq 0) {
        throw "$Side release build did not expose the mount_sanity test executable. See $stdoutPath and $stderrPath"
    }
    $reportedExecutable = [string]$artifacts[$artifacts.Count - 1]
    $executablePath = if ([System.IO.Path]::IsPathRooted($reportedExecutable)) {
        [System.IO.Path]::GetFullPath($reportedExecutable)
    }
    else {
        [System.IO.Path]::GetFullPath((Join-Path $Worktree $reportedExecutable))
    }
    $executableIdentity = Get-FileIdentity -Path $executablePath
    [pscustomobject][ordered]@{
        side                 = $Side
        exit_code            = $completed.exit_code
        wall_ms              = [math]::Round($timer.Elapsed.TotalMilliseconds, 3)
        executable_identity  = $executableIdentity
        stdout_path          = $stdoutPath
        stderr_path          = $stderrPath
    }
}

function Invoke-CargoBenchmark {
    param(
        [string]$Worktree,
        [string]$Side,
        [int]$Run,
        [int]$Sequence,
        [pscustomobject]$ExpectedExecutable,
        [string]$ArtifactDirectory
    )
    $stdoutPath = Join-Path $ArtifactDirectory ("{0}-run-{1:D2}.stdout.log" -f $Side, $Run)
    $stderrPath = Join-Path $ArtifactDirectory ("{0}-run-{1:D2}.stderr.log" -f $Side, $Run)
    $arguments = @(
        "metadata_baseline::winfsp_metadata_baseline",
        "--ignored",
        "--exact",
        "--nocapture",
        "--test-threads=1"
    )
    $beforeIdentity = Get-FileIdentity -Path $ExpectedExecutable.path
    if ($beforeIdentity.sha256 -ne $ExpectedExecutable.sha256 -or
        $beforeIdentity.length_bytes -ne $ExpectedExecutable.length_bytes) {
        throw "$Side run $Run executable identity changed after build: $($ExpectedExecutable.path)"
    }
    $systemLoadBefore = Get-SystemLoadObservation
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    $startedAt = Get-Date
    $handle = Start-ProcessHandle -FilePath $ExpectedExecutable.path -Worktree $Worktree -Arguments $arguments
    $cargo = $handle.process
    $maxWorkingSet = [int64]0
    $cpuByProcessId = @{}
    while (-not $cargo.HasExited) {
        foreach ($processId in @(Get-BenchmarkProcessIds -RootProcessId $cargo.Id -StartedAt $startedAt)) {
            try {
                $process = Get-Process -Id $processId -ErrorAction Stop
                $maxWorkingSet = [math]::Max($maxWorkingSet, [int64]$process.WorkingSet64)
                $cpuByProcessId[$process.Id] = $process.TotalProcessorTime.TotalMilliseconds
            }
            catch {
                # A short-lived cargo/test child can exit between discovery and sampling.
            }
        }
        Start-Sleep -Milliseconds 100
        $cargo.Refresh()
    }
    $completed = Complete-CargoProcess -Handle $handle -StdoutPath $stdoutPath -StderrPath $stderrPath
    $timer.Stop()
    $systemLoadAfter = Get-SystemLoadObservation
    $stdout = $completed.stdout
    $stderr = $completed.stderr
    $combined = $stdout + [Environment]::NewLine + $stderr
    if ($completed.exit_code -ne 0) {
        throw "$Side run $Run failed with cargo exit code $($completed.exit_code). See $stdoutPath and $stderrPath"
    }
    $reports = [regex]::Matches($combined, 'metadata_baseline_report=(?<path>[^\r\n]+)')
    if ($reports.Count -eq 0) {
        throw "$Side run $Run did not report metadata_baseline_report. See $stdoutPath and $stderrPath"
    }
    $sourceCsv = $reports[$reports.Count - 1].Groups['path'].Value.Trim()
    if (-not (Test-Path -LiteralPath $sourceCsv -PathType Leaf)) {
        throw "$Side run $Run reported a missing CSV: $sourceCsv"
    }
    $sourceItem = Get-Item -LiteralPath $sourceCsv
    if ($sourceItem.LastWriteTimeUtc -lt $startedAt.ToUniversalTime()) {
        throw "$Side run $Run reported a stale CSV: $sourceCsv"
    }
    $afterIdentity = Get-FileIdentity -Path $ExpectedExecutable.path
    if ($afterIdentity.sha256 -ne $ExpectedExecutable.sha256 -or
        $afterIdentity.length_bytes -ne $ExpectedExecutable.length_bytes) {
        throw "$Side run $Run executable identity changed during benchmark: $($ExpectedExecutable.path)"
    }
    $csvPath = Join-Path $ArtifactDirectory ("{0}-run-{1:D2}.csv" -f $Side, $Run)
    Copy-Item -LiteralPath $sourceCsv -Destination $csvPath -Force
    $rows = @(Import-Csv -LiteralPath $csvPath)
    if ($rows.Count -ne 12) {
        throw "$Side run $Run produced $($rows.Count) CSV rows; expected 12"
    }
    $expectedColumns = @("pass", "workload", "operations", "elapsed_ns", "warmup") + $measurementColumns
    $legacyColumns = @("pass", "workload", "operations", "elapsed_ns") + $callbackColumns
    $actualColumns = @($rows[0].PSObject.Properties.Name)
    $hasSfs690Schema = $actualColumns.Count -eq $expectedColumns.Count -and
        ($actualColumns -join ',') -eq ($expectedColumns -join ',')
    $hasLegacySchema = $actualColumns.Count -eq $legacyColumns.Count -and
        ($actualColumns -join ',') -eq ($legacyColumns -join ',')
    if (-not $hasSfs690Schema -and -not $hasLegacySchema) {
        throw "$Side run $Run CSV has an unknown schema"
    }
    if ($hasLegacySchema -and $Side -ne "baseline") {
        throw "$Side run $Run emitted the legacy metadata schema; candidate instrumentation is required"
    }
    $instrumentationStatus = if ($hasSfs690Schema) { "sfs-690" } else { "legacy" }
    $expectedWorkloads = @("stat", "open_close", "negative_lookup", "enumeration")
    if (@($rows | Where-Object { [int]$_.operations -ne 1000 }).Count -ne 0) {
        throw "$Side run $Run contains a workload with operations other than 1000"
    }
    $actualWorkloads = @($rows | Select-Object -ExpandProperty workload -Unique | Sort-Object)
    if (($actualWorkloads -join ',') -ne (($expectedWorkloads | Sort-Object) -join ',')) {
        throw "$Side run $Run contains an unexpected workload set"
    }
    foreach ($workload in $expectedWorkloads) {
        $workloadRows = @($rows | Where-Object { $_.workload -eq $workload })
        if ($workloadRows.Count -ne 3) {
            throw "$Side run $Run does not contain exactly three passes for workload $workload"
        }
        $actualPasses = @($workloadRows | Select-Object -ExpandProperty pass -Unique | Sort-Object)
        if (($actualPasses -join ',') -ne "1,2,3") {
            throw "$Side run $Run contains an invalid pass set for workload $workload"
        }
    }
    if ($hasSfs690Schema -and @($rows | Where-Object { $_.warmup -ne "false" }).Count -ne 0) {
        throw "$Side run $Run contains an unexpected warm-up marker"
    }
    $columnsToValidate = if ($hasSfs690Schema) {
        $numericMeasurementColumns
    }
    else {
        @("elapsed_ns") + $callbackColumns
    }
    foreach ($column in $columnsToValidate) {
        if (@($rows | Where-Object { $_.$column -notmatch '^\d+$' }).Count -ne 0) {
            throw "$Side run $Run contains a non-integer measurement in column $column"
        }
    }
    [pscustomobject][ordered]@{
        side                    = $Side
        run                     = $Run
        sequence                = $Sequence
        run_label               = "{0}-run-{1:D2}" -f $Side, $Run
        instrumentation_status  = $instrumentationStatus
        exit_code               = $completed.exit_code
        wall_ms                 = [math]::Round($timer.Elapsed.TotalMilliseconds, 3)
        process_cpu_ms          = [math]::Round((@($cpuByProcessId.Values | Measure-Object -Sum).Sum), 3)
        max_working_set_bytes   = $maxWorkingSet
        executable_identity     = $afterIdentity
        system_load_before      = $systemLoadBefore
        system_load_after       = $systemLoadAfter
        csv_path                = $csvPath
        stdout_path             = $stdoutPath
        stderr_path             = $stderrPath
    }
}

function Get-Median {
    param([double[]]$Values)
    $ordered = @($Values | Sort-Object)
    if ($ordered.Count -eq 0) {
        throw "Cannot calculate a median for an empty set"
    }
    $middle = [int][math]::Floor($ordered.Count / 2)
    if (($ordered.Count % 2) -eq 1) {
        return $ordered[$middle]
    }
    return ($ordered[$middle - 1] + $ordered[$middle]) / 2
}

function Get-Quantile {
    param(
        [double[]]$Values,
        [ValidateRange(0, 1)]
        [double]$Probability
    )
    $ordered = @($Values | Sort-Object)
    if ($ordered.Count -eq 0) {
        throw "Cannot calculate a quantile for an empty set"
    }
    if ($ordered.Count -eq 1) {
        return $ordered[0]
    }
    $position = ($ordered.Count - 1) * $Probability
    $lower = [int][math]::Floor($position)
    $upper = [int][math]::Ceiling($position)
    if ($lower -eq $upper) {
        return $ordered[$lower]
    }
    $fraction = $position - $lower
    return $ordered[$lower] + (($ordered[$upper] - $ordered[$lower]) * $fraction)
}

function Get-StandardDeviation {
    param([double[]]$Values)
    if ($Values.Count -le 1) {
        return 0
    }
    $mean = ($Values | Measure-Object -Average).Average
    $sumSquares = [double]0
    foreach ($value in $Values) {
        $difference = $value - $mean
        $sumSquares += $difference * $difference
    }
    [math]::Sqrt($sumSquares / ($Values.Count - 1))
}

function Get-WorkloadSummary {
    param(
        [object[]]$RunRecords,
        [string]$Side,
        [string]$Workload
    )
    $passElapsed = New-Object System.Collections.Generic.List[double]
    $passMeasurements = @{}
    foreach ($name in $measurementColumns) {
        $passMeasurements[$name] = New-Object System.Collections.Generic.List[double]
    }
    $samples = @()
    foreach ($record in @($RunRecords | Where-Object { $_.side -eq $Side })) {
        foreach ($row in @(Import-Csv -LiteralPath $record.csv_path | Where-Object { $_.workload -eq $Workload })) {
            $passElapsed.Add([double]$row.elapsed_ns)
            $samples += [pscustomobject][ordered]@{
                run       = [int]$record.run
                sequence  = [int]$record.sequence
                pass      = [int]$row.pass
                elapsed_ns = [double]$row.elapsed_ns
            }
            foreach ($name in $passMeasurements.Keys) {
                $property = $row.PSObject.Properties[$name]
                if ($null -ne $property -and -not [string]::IsNullOrWhiteSpace($property.Value)) {
                    $passMeasurements[$name].Add([double]$property.Value)
                }
            }
        }
    }
    $elapsedValues = $passElapsed.ToArray()
    $q1 = Get-Quantile -Values $elapsedValues -Probability 0.25
    $q3 = Get-Quantile -Values $elapsedValues -Probability 0.75
    $iqr = $q3 - $q1
    $lowerFence = $q1 - (1.5 * $iqr)
    $upperFence = $q3 + (1.5 * $iqr)
    $outlierSamples = @(
        $samples |
            Where-Object { $_.elapsed_ns -lt $lowerFence -or $_.elapsed_ns -gt $upperFence } |
            ForEach-Object {
                [ordered]@{
                    run       = $_.run
                    sequence  = $_.sequence
                    pass      = $_.pass
                    elapsed_ns = $_.elapsed_ns
                }
            }
    )
    $elapsedMedian = Get-Median -Values $elapsedValues
    $elapsedStdDev = Get-StandardDeviation -Values $elapsedValues
    $result = [ordered]@{
        side                  = $Side
        workload              = $Workload
        instrumentation_status = (@($RunRecords | Where-Object { $_.side -eq $Side } | Select-Object -ExpandProperty instrumentation_status -Unique) -join ",")
        sample_count          = $elapsedValues.Count
        elapsed_ns_min        = [math]::Round((($elapsedValues | Measure-Object -Minimum).Minimum), 3)
        elapsed_ns_median     = [math]::Round($elapsedMedian, 3)
        elapsed_ns_p95        = [math]::Round((Get-Quantile -Values $elapsedValues -Probability 0.95), 3)
        elapsed_ns_max        = [math]::Round((($elapsedValues | Measure-Object -Maximum).Maximum), 3)
        elapsed_ns_stddev     = [math]::Round($elapsedStdDev, 3)
        elapsed_ns_cv_percent = if ($elapsedMedian -eq 0) { 0 } else { [math]::Round(($elapsedStdDev / $elapsedMedian) * 100, 3) }
        iqr_lower_fence       = [math]::Round($lowerFence, 3)
        iqr_upper_fence       = [math]::Round($upperFence, 3)
        outlier_count         = $outlierSamples.Count
        outliers              = $outlierSamples
    }
    foreach ($name in $passMeasurements.Keys) {
        $values = $passMeasurements[$name].ToArray()
        $result["${name}_median"] = if ($values.Count -eq 0) {
            $null
        }
        else {
            [math]::Round((Get-Median -Values $values), 3)
        }
    }
    [pscustomobject]$result
}

$repoRoot = Get-RepoRoot
if ([string]::IsNullOrWhiteSpace($CandidateCommit)) {
    $CandidateCommit = (& git -C $repoRoot rev-parse HEAD).Trim()
}
$baselineResolved = Resolve-Commit -RepoRoot $repoRoot -Commit $BaselineCommit
$candidateResolved = Resolve-Commit -RepoRoot $repoRoot -Commit $CandidateCommit
$mountLetter = $MountPoint.Substring(0, 1).ToUpperInvariant()
$drive = Get-PSDrive -Name $mountLetter -PSProvider FileSystem -ErrorAction SilentlyContinue
if ($null -ne $drive) {
    throw "Refusing assigned mount drive $MountPoint"
}
$envPath = Join-Path $repoRoot ".env"
$runMountTests = Get-DotEnvValue -Path $envPath -Key "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS"
$configuredMountPoint = Get-DotEnvValue -Path $envPath -Key "STUDIOFS_WINFSP_TEST_MOUNT_POINT"
if ($runMountTests -ne "1") {
    throw "Repository .env must set STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"
}
if ($configuredMountPoint.Trim().ToUpperInvariant() -ne $MountPoint.Trim().ToUpperInvariant()) {
    throw "-MountPoint must match STUDIOFS_WINFSP_TEST_MOUNT_POINT from .env"
}
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot "target\perf"
}
$artifactDirectory = New-UniqueDirectory -Root $OutputRoot -Name ("sfs-680-windows-metadata-ab-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
$targetRoot = (Resolve-Path -LiteralPath (Join-Path $repoRoot "target")).Path
$snapshot = Join-Path $targetRoot ("sfs-680-baseline-" + [System.Diagnostics.Process]::GetCurrentProcess().Id)
$snapshotArchive = Join-Path $artifactDirectory "baseline-source.zip"
if (Test-Path -LiteralPath $snapshot) {
    throw "Temporary baseline snapshot already exists: $snapshot"
}

$environmentNames = @(
    "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS",
    "STUDIOFS_WINFSP_TEST_MOUNT_POINT",
    "STUDIOFS_WINFSP_METADATA_CACHE_POLICY",
    "STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS",
    "STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS",
    "STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS",
    "STUDIOFS_WINFSP_SECURITY_TIMEOUT_MS",
    "STUDIOFS_WINFSP_STREAM_INFO_TIMEOUT_MS",
    "STUDIOFS_WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_MS"
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
}

$snapshotCreated = $false
try {
    # The managed workspace keeps .git read-only. A tracked-source archive gives
    # the baseline its own Cargo root without mutating Git's worktree metadata.
    & git -C $repoRoot archive --format=zip --output=$snapshotArchive $baselineResolved
    if ($LASTEXITCODE -ne 0) {
        throw "Could not archive baseline commit $baselineResolved"
    }
    Expand-Archive -LiteralPath $snapshotArchive -DestinationPath $snapshot
    $snapshotCreated = $true
    $env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS = $runMountTests
    $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT = $MountPoint.Trim().ToUpperInvariant()
    foreach ($name in @(
        "STUDIOFS_WINFSP_METADATA_CACHE_POLICY",
        "STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS",
        "STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS",
        "STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS",
        "STUDIOFS_WINFSP_SECURITY_TIMEOUT_MS",
        "STUDIOFS_WINFSP_STREAM_INFO_TIMEOUT_MS",
        "STUDIOFS_WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_MS"
    )) {
        Remove-Item -Path ("Env:" + $name) -ErrorAction SilentlyContinue
    }

    $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
    $cpu = Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue | Select-Object -First 1
    $runOrder = @()
    for ($run = 1; $run -le $Runs; $run += 1) {
        $firstSide = if (($run % 2) -eq 1) { "baseline" } else { "candidate" }
        $secondSide = if ($firstSide -eq "baseline") { "candidate" } else { "baseline" }
        $runOrder += [pscustomobject][ordered]@{
            sequence = $runOrder.Count + 1
            side     = $firstSide
            run      = $run
            label    = "{0}-run-{1:D2}" -f $firstSide, $run
        }
        $runOrder += [pscustomobject][ordered]@{
            sequence = $runOrder.Count + 1
            side     = $secondSide
            run      = $run
            label    = "{0}-run-{1:D2}" -f $secondSide, $run
        }
    }
    $runOrderLabels = @($runOrder | ForEach-Object { $_.label })
    $buildCommand = "cargo test -p studiofs-winfsp --release --test mount_sanity --no-run --message-format=json"
    $runnerCommand = "<built mount_sanity executable> metadata_baseline::winfsp_metadata_baseline --ignored --exact --nocapture --test-threads=1"
    $harnessCommand = "powershell -ExecutionPolicy Bypass -File .\tools\perf\sfs-680-windows-metadata-ab.ps1 -MountPoint $($MountPoint.Trim().ToUpperInvariant()) -BaselineCommit $baselineResolved -CandidateCommit $candidateResolved -Runs $Runs"
    $environment = [ordered]@{
        issue                  = "SFS-690"
        related_harness        = "SFS-680"
        instrumentation        = "SFS-690 opt-in metadata hot-path attribution"
        platform               = "Windows"
        mount_point            = $MountPoint.Trim().ToUpperInvariant()
        runs_per_side          = $Runs
        workload_definition    = "stat,open_close,negative_lookup,enumeration; 1000 operations; 3 passes per invocation"
        build_command          = $buildCommand
        command                = $runnerCommand
        harness_command        = $harnessCommand
        run_order              = $runOrderLabels
        warmup_passes          = 0
        warmup_status          = "none; all three passes are measured observations"
        measurement_columns    = @("elapsed_ns") + $measurementColumns
        baseline_commit        = $baselineResolved
        candidate_commit       = $candidateResolved
        baseline_policy        = "pre-SFS-678 defaults; policy and timeout overrides unset"
        candidate_policy       = "adaptive default; policy and timeout overrides unset"
        os                     = if ($os) { $os.Caption } else { [System.Environment]::OSVersion.VersionString }
        os_version             = if ($os) { $os.Version } else { $null }
        cpu                    = if ($cpu) { $cpu.Name.Trim() } else { $null }
        logical_processors     = if ($cpu) { $cpu.NumberOfLogicalProcessors } else { [Environment]::ProcessorCount }
        rustc_version          = (& rustc --version).Trim()
        cargo_version          = (& cargo --version).Trim()
        source_dotenv_keys     = "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS,STUDIOFS_WINFSP_TEST_MOUNT_POINT"
        resource_observation   = "cargo process tree CPU time and maximum sampled working set; supporting evidence, not a profiler"
        system_load_observation = "processor load and free physical memory sampled before and after each native run; unavailable fields are explicit nulls"
    }
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "environment.json") -Value ($environment | ConvertTo-Json -Depth 8)
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "command.txt") -Value ($harnessCommand + [Environment]::NewLine + "build: " + $buildCommand + [Environment]::NewLine + "run: " + $runnerCommand + [Environment]::NewLine + "run_order=" + ($runOrderLabels -join ","))

    $builds = @{}
    foreach ($side in @("baseline", "candidate")) {
        $sideWorktree = if ($side -eq "baseline") { $snapshot } else { $repoRoot }
        Write-Host ("[{0}] release build (excluded from workload timing)" -f $side)
        $builds[$side] = Invoke-CargoBuild -Worktree $sideWorktree -Side $side -ArtifactDirectory $artifactDirectory
    }
    $environment["binary_identity"] = [ordered]@{
        baseline  = $builds["baseline"].executable_identity
        candidate = $builds["candidate"].executable_identity
    }
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "environment.json") -Value ($environment | ConvertTo-Json -Depth 8)

    $records = @()
    foreach ($order in $runOrder) {
        $side = $order.side
        $sideWorktree = if ($side -eq "baseline") { $snapshot } else { $repoRoot }
        Write-Host ("[{0}] native metadata workload run {1} of {2} (sequence {3})" -f $side, $order.run, $Runs, $order.sequence)
        $records += Invoke-CargoBenchmark `
            -Worktree $sideWorktree `
            -Side $side `
            -Run $order.run `
            -Sequence $order.sequence `
            -ExpectedExecutable $builds[$side].executable_identity `
            -ArtifactDirectory $artifactDirectory
    }
    Write-Host "aggregation: workload summaries"
    $summaryRows = @()
    foreach ($side in @("baseline", "candidate")) {
        foreach ($workload in @("stat", "open_close", "negative_lookup", "enumeration")) {
            $summaryRows += Get-WorkloadSummary -RunRecords $records -Side $side -Workload $workload
        }
    }
    $instrumentationCoverage = [ordered]@{
        baseline  = (@($records | Where-Object { $_.side -eq "baseline" } | Select-Object -ExpandProperty instrumentation_status -Unique) -join ",")
        candidate = (@($records | Where-Object { $_.side -eq "candidate" } | Select-Object -ExpandProperty instrumentation_status -Unique) -join ",")
    }
    $environment["instrumentation_coverage"] = $instrumentationCoverage
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "environment.json") -Value ($environment | ConvertTo-Json -Depth 8)
    Write-Host "aggregation: JSON"
    $json = [ordered]@{
        issue                   = "SFS-690"
        run_order               = @($runOrder)
        instrumentation_coverage = $instrumentationCoverage
        builds                  = @($builds["baseline"], $builds["candidate"])
        runs                    = @($records)
        workloads               = @($summaryRows)
    }
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "results.json") -Value ($json | ConvertTo-Json -Depth 8)

    Write-Host "aggregation: Markdown"
    $markdown = New-Object System.Collections.Generic.List[string]
    $markdown.Add("# SFS-690 Windows metadata A/B benchmark")
    $markdown.Add("")
    $markdown.Add(("Baseline `{0}`; candidate `{1}`." -f $baselineResolved, $candidateResolved))
    $markdown.Add("")
    $markdown.Add(("Run order: `{0}`." -f ($runOrderLabels -join " -> ")))
    $markdown.Add(("Instrumentation coverage: baseline `{0}`; candidate `{1}`." -f $instrumentationCoverage.baseline, $instrumentationCoverage.candidate))
    $markdown.Add("")
    $markdown.Add("| Workload | Baseline median (ms) | Candidate median (ms) | Baseline p95 (ms) | Candidate p95 (ms) | Change | Outliers B/C |")
    $markdown.Add("|---|---:|---:|---:|---:|---:|---:|")
    foreach ($workload in @("stat", "open_close", "negative_lookup", "enumeration")) {
        $baseline = @($summaryRows | Where-Object { $_.side -eq "baseline" -and $_.workload -eq $workload })[0]
        $candidate = @($summaryRows | Where-Object { $_.side -eq "candidate" -and $_.workload -eq $workload })[0]
        $change = (($candidate.elapsed_ns_median - $baseline.elapsed_ns_median) / $baseline.elapsed_ns_median) * 100
        $markdown.Add(("| {0} | {1:N3} | {2:N3} | {3:N3} | {4:N3} | {5:+0.0;-0.0;0.0}% | {6}/{7} |" -f $workload, ($baseline.elapsed_ns_median / 1000000), ($candidate.elapsed_ns_median / 1000000), ($baseline.elapsed_ns_p95 / 1000000), ($candidate.elapsed_ns_p95 / 1000000), $change, $baseline.outlier_count, $candidate.outlier_count))
    }
    $markdown.Add("")
    $markdown.Add("| Side | Median wall time (ms) | Median process CPU (ms) | Median max working set (MiB) |")
    $markdown.Add("|---|---:|---:|---:|")
    foreach ($side in @("baseline", "candidate")) {
        $sideRecords = @($records | Where-Object { $_.side -eq $side })
        $markdown.Add(("| {0} | {1:N3} | {2:N3} | {3:N3} |" -f $side, (Get-Median -Values ([double[]]$sideRecords.wall_ms)), (Get-Median -Values ([double[]]$sideRecords.process_cpu_ms)), ((Get-Median -Values ([double[]]$sideRecords.max_working_set_bytes)) / 1MB)))
    }
    $markdown.Add("")
    $markdown.Add('Callback, VFS hot-path, cache, and native-cache medians are retained in `results.json`; raw CSVs preserve every pass and delta.')
    $markdown.Add('P95, sample standard deviation, coefficient of variation, and IQR outlier samples are retained per workload in `results.json`.')
    $markdown.Add(("Warm-up status: {0}. System-load observations are stored per run; unavailable CIM fields remain null." -f $environment.warmup_status))
    $markdown.Add("Enumeration timing includes correctness assertions, allocation and sorting. Close callbacks can cross workload boundaries.")
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "summary.md") -Value ($markdown -join [Environment]::NewLine)
    Write-Host "SFS-680 artifacts: $artifactDirectory"
}
catch {
    $details = $_ | Format-List * -Force | Out-String
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "harness-error.txt") -Value $details
    Write-Error $details
    throw
}
finally {
    Write-Host "cleanup: environment"
    foreach ($name in $environmentNames) {
        if ($null -eq $savedEnvironment[$name]) {
            Remove-Item -Path ("Env:" + $name) -ErrorAction SilentlyContinue
        }
        else {
            Set-Item -Path ("Env:" + $name) -Value $savedEnvironment[$name]
        }
    }
    if ($snapshotCreated) {
        Write-Host "cleanup: baseline snapshot"
        $resolvedSnapshot = [System.IO.Path]::GetFullPath($snapshot).TrimEnd('\')
        $resolvedTargetRoot = [System.IO.Path]::GetFullPath($targetRoot).TrimEnd('\')
        if (-not $resolvedSnapshot.StartsWith($resolvedTargetRoot + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove baseline snapshot outside target: $resolvedSnapshot"
        }
        Remove-Item -LiteralPath $resolvedSnapshot -Recurse -Force
        if (Test-Path -LiteralPath $snapshotArchive) {
            Remove-Item -LiteralPath $snapshotArchive -Force
        }
    }
    Write-Host "cleanup: complete"
}
