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
        "--no-run"
    )
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    $cargo = Start-Process -FilePath "cargo.exe" -ArgumentList $arguments -WorkingDirectory $Worktree `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
    while (-not $cargo.HasExited) {
        Start-Sleep -Milliseconds 100
        $cargo.Refresh()
    }
    $cargo.WaitForExit()
    $timer.Stop()
    if ($cargo.ExitCode -ne 0) {
        throw "$Side release build failed with cargo exit code $($cargo.ExitCode). See $stdoutPath and $stderrPath"
    }
    [pscustomobject][ordered]@{
        side      = $Side
        exit_code = $cargo.ExitCode
        wall_ms   = [math]::Round($timer.Elapsed.TotalMilliseconds, 3)
        stdout_path = $stdoutPath
        stderr_path = $stderrPath
    }
}

function Invoke-CargoBenchmark {
    param(
        [string]$Worktree,
        [string]$Side,
        [int]$Run,
        [string]$ArtifactDirectory
    )
    $stdoutPath = Join-Path $ArtifactDirectory ("{0}-run-{1:D2}.stdout.log" -f $Side, $Run)
    $stderrPath = Join-Path $ArtifactDirectory ("{0}-run-{1:D2}.stderr.log" -f $Side, $Run)
    $arguments = @(
        "test",
        "-p",
        "studiofs-winfsp",
        "--release",
        "--test",
        "mount_sanity",
        "metadata_baseline::winfsp_metadata_baseline",
        "--",
        "--ignored",
        "--exact",
        "--nocapture",
        "--test-threads=1"
    )
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    $startedAt = Get-Date
    $cargo = Start-Process -FilePath "cargo.exe" -ArgumentList $arguments -WorkingDirectory $Worktree `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
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
    $cargo.WaitForExit()
    $timer.Stop()
    $stdout = if (Test-Path -LiteralPath $stdoutPath) { Get-Content -Raw -LiteralPath $stdoutPath } else { "" }
    $stderr = if (Test-Path -LiteralPath $stderrPath) { Get-Content -Raw -LiteralPath $stderrPath } else { "" }
    $combined = $stdout + [Environment]::NewLine + $stderr
    if ($cargo.ExitCode -ne 0) {
        throw "$Side run $Run failed with cargo exit code $($cargo.ExitCode). See $stdoutPath and $stderrPath"
    }
    $reports = [regex]::Matches($combined, 'metadata_baseline_report=(?<path>[^\r\n]+)')
    if ($reports.Count -eq 0) {
        throw "$Side run $Run did not report metadata_baseline_report. See $stdoutPath and $stderrPath"
    }
    $sourceCsv = $reports[$reports.Count - 1].Groups['path'].Value.Trim()
    if (-not (Test-Path -LiteralPath $sourceCsv -PathType Leaf)) {
        throw "$Side run $Run reported a missing CSV: $sourceCsv"
    }
    $csvPath = Join-Path $ArtifactDirectory ("{0}-run-{1:D2}.csv" -f $Side, $Run)
    Copy-Item -LiteralPath $sourceCsv -Destination $csvPath -Force
    $rows = @(Import-Csv -LiteralPath $csvPath)
    if ($rows.Count -ne 12) {
        throw "$Side run $Run produced $($rows.Count) CSV rows; expected 12"
    }
    $expectedWorkloads = @("stat", "open_close", "negative_lookup", "enumeration")
    if (@($rows | Where-Object { [int]$_.operations -ne 1000 }).Count -ne 0) {
        throw "$Side run $Run contains a workload with operations other than 1000"
    }
    $actualWorkloads = @($rows | Select-Object -ExpandProperty workload -Unique | Sort-Object)
    if (($actualWorkloads -join ',') -ne (($expectedWorkloads | Sort-Object) -join ',')) {
        throw "$Side run $Run contains an unexpected workload set"
    }
    [pscustomobject][ordered]@{
        side                    = $Side
        run                     = $Run
        exit_code               = $cargo.ExitCode
        wall_ms                 = [math]::Round($timer.Elapsed.TotalMilliseconds, 3)
        process_cpu_ms          = [math]::Round((@($cpuByProcessId.Values | Measure-Object -Sum).Sum), 3)
        max_working_set_bytes   = $maxWorkingSet
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

function Get-WorkloadSummary {
    param(
        [object[]]$RunRecords,
        [string]$Side,
        [string]$Workload
    )
    $passElapsed = New-Object System.Collections.Generic.List[double]
    $passCallbacks = @{
        get_security_by_name = New-Object System.Collections.Generic.List[double]
        open                 = New-Object System.Collections.Generic.List[double]
        get_file_info        = New-Object System.Collections.Generic.List[double]
        read_directory       = New-Object System.Collections.Generic.List[double]
        cleanup              = New-Object System.Collections.Generic.List[double]
        close                = New-Object System.Collections.Generic.List[double]
    }
    foreach ($record in @($RunRecords | Where-Object { $_.side -eq $Side })) {
        foreach ($row in @(Import-Csv -LiteralPath $record.csv_path | Where-Object { $_.workload -eq $Workload })) {
            $passElapsed.Add([double]$row.elapsed_ns)
            foreach ($name in $passCallbacks.Keys) {
                $passCallbacks[$name].Add([double]$row.$name)
            }
        }
    }
    $result = [ordered]@{
        side     = $Side
        workload = $Workload
        elapsed_ns_median = [math]::Round((Get-Median -Values $passElapsed.ToArray()), 3)
    }
    foreach ($name in $passCallbacks.Keys) {
        $result["${name}_median"] = [math]::Round((Get-Median -Values $passCallbacks[$name].ToArray()), 3)
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
    $environment = [ordered]@{
        issue                  = "SFS-680"
        platform               = "Windows"
        mount_point            = $MountPoint.Trim().ToUpperInvariant()
        runs_per_side          = $Runs
        workload_definition    = "stat,open_close,negative_lookup,enumeration; 1000 operations; 3 passes per invocation"
        command                = "cargo test -p studiofs-winfsp --release --test mount_sanity metadata_baseline::winfsp_metadata_baseline -- --ignored --exact --nocapture --test-threads=1"
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
    }
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "environment.json") -Value ($environment | ConvertTo-Json -Depth 8)
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "command.txt") -Value $environment.command

    $records = @()
    foreach ($side in @("baseline", "candidate")) {
        $sideWorktree = if ($side -eq "baseline") { $snapshot } else { $repoRoot }
        Write-Host ("[{0}] release build (excluded from workload timing)" -f $side)
        Invoke-CargoBuild -Worktree $sideWorktree -Side $side -ArtifactDirectory $artifactDirectory | Out-Null
        for ($run = 1; $run -le $Runs; $run += 1) {
            Write-Host ("[{0}] native metadata workload run {1} of {2}" -f $side, $run, $Runs)
            $records += Invoke-CargoBenchmark -Worktree $sideWorktree -Side $side -Run $run -ArtifactDirectory $artifactDirectory
        }
    }
    Write-Host "aggregation: workload summaries"
    $summaryRows = @()
    foreach ($side in @("baseline", "candidate")) {
        foreach ($workload in @("stat", "open_close", "negative_lookup", "enumeration")) {
            $summaryRows += Get-WorkloadSummary -RunRecords $records -Side $side -Workload $workload
        }
    }
    Write-Host "aggregation: JSON"
    $json = [ordered]@{
        runs = @($records)
        workloads = @($summaryRows)
    }
    Write-Utf8NoBom -Path (Join-Path $artifactDirectory "results.json") -Value ($json | ConvertTo-Json -Depth 8)

    Write-Host "aggregation: Markdown"
    $markdown = New-Object System.Collections.Generic.List[string]
    $markdown.Add("# SFS-680 Windows metadata A/B benchmark")
    $markdown.Add("")
    $markdown.Add(("Baseline `{0}`; candidate `{1}`." -f $baselineResolved, $candidateResolved))
    $markdown.Add("")
    $markdown.Add("| Workload | Baseline median (ms) | Candidate median (ms) | Change |")
    $markdown.Add("|---|---:|---:|---:|")
    foreach ($workload in @("stat", "open_close", "negative_lookup", "enumeration")) {
        $baseline = @($summaryRows | Where-Object { $_.side -eq "baseline" -and $_.workload -eq $workload })[0]
        $candidate = @($summaryRows | Where-Object { $_.side -eq "candidate" -and $_.workload -eq $workload })[0]
        $change = (($candidate.elapsed_ns_median - $baseline.elapsed_ns_median) / $baseline.elapsed_ns_median) * 100
        $markdown.Add(("| {0} | {1:N3} | {2:N3} | {3:+0.0;-0.0;0.0}% |" -f $workload, ($baseline.elapsed_ns_median / 1000000), ($candidate.elapsed_ns_median / 1000000), $change))
    }
    $markdown.Add("")
    $markdown.Add("| Side | Median wall time (ms) | Median process CPU (ms) | Median max working set (MiB) |")
    $markdown.Add("|---|---:|---:|---:|")
    foreach ($side in @("baseline", "candidate")) {
        $sideRecords = @($records | Where-Object { $_.side -eq $side })
        $markdown.Add(("| {0} | {1:N3} | {2:N3} | {3:N3} |" -f $side, (Get-Median -Values ([double[]]$sideRecords.wall_ms)), (Get-Median -Values ([double[]]$sideRecords.process_cpu_ms)), ((Get-Median -Values ([double[]]$sideRecords.max_working_set_bytes)) / 1MB)))
    }
    $markdown.Add("")
    $markdown.Add('Callback medians are retained in `results.json`; the raw CSVs preserve every pass and callback-entry delta.')
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
