[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$StudiofsBin,

    [Parameter(Mandatory = $true)]
    [string]$E2eBin,

    [string]$OutputRoot = (Join-Path $PSScriptRoot "..\..\target\perf"),

    [ValidateRange(1, 20)]
    [int]$Runs = 3,

    [string]$SelectedCase,

    [switch]$ConstrainPostgres,

    [switch]$PreviewOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scenario = "mounted-large-history-cold-bootstrap-convergence"
$replayBatches = 128
$timestamp = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ")
$runRoot = Join-Path $OutputRoot ("sfs-718-pg-sync-{0}-{1}" -f $timestamp, $PID)
$e2eRoot = Join-Path $runRoot "e2e-runs"
$null = New-Item -ItemType Directory -Path $runRoot -Force
$null = New-Item -ItemType Directory -Path $e2eRoot -Force

function Resolve-ExistingFile {
    param([string]$PathValue, [string]$Label)

    $resolved = Resolve-Path -LiteralPath $PathValue -ErrorAction SilentlyContinue
    if ($null -eq $resolved -or -not (Test-Path -LiteralPath $resolved.Path -PathType Leaf)) {
        throw "$Label does not exist: $PathValue"
    }
    return $resolved.Path
}

function Convert-ToBytes {
    param([string]$Value)

    $match = [regex]::Match($Value.Trim(), '^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$')
    if (-not $match.Success) {
        return $null
    }
    $number = [double]$match.Groups[1].Value
    $multiplier = switch ($match.Groups[2].Value.ToUpperInvariant()) {
        "B" { 1 }
        "KB" { 1024 }
        "KIB" { 1024 }
        "MB" { 1024 * 1024 }
        "MIB" { 1024 * 1024 }
        "GB" { 1024 * 1024 * 1024 }
        "GIB" { 1024 * 1024 * 1024 }
        "TB" { 1024L * 1024 * 1024 * 1024 }
        "TIB" { 1024L * 1024 * 1024 * 1024 }
        default { return $null }
    }
    return [int64][math]::Round($number * $multiplier)
}

function Get-Percentile {
    param([object[]]$Values, [double]$Percentile)

    $numbers = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ } | Sort-Object)
    if ($numbers.Count -eq 0) {
        return $null
    }
    $index = [math]::Ceiling($numbers.Count * $Percentile) - 1
    $index = [math]::Max(0, [math]::Min($index, $numbers.Count - 1))
    return $numbers[[int]$index]
}

function Get-Maximum {
    param([object[]]$Values)

    $numbers = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($numbers.Count -eq 0) {
        return $null
    }
    return ($numbers | Measure-Object -Maximum).Maximum
}

function Get-ProcessTreeIds {
    param([int]$RootPid)

    $ids = [System.Collections.Generic.HashSet[int]]::new()
    [void]$ids.Add($RootPid)
    $processes = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    $changed = $true
    while ($changed) {
        $changed = $false
        foreach ($processInfo in $processes) {
            $parent = [int]$processInfo.ParentProcessId
            $child = [int]$processInfo.ProcessId
            if ($ids.Contains($parent) -and $ids.Add($child)) {
                $changed = $true
            }
        }
    }
    return @($ids)
}

function Get-ProcessTreeWorkingSetBytes {
    param([int]$RootPid)

    $total = [int64]0
    foreach ($processId in @(Get-ProcessTreeIds -RootPid $RootPid)) {
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($null -ne $process) {
            $process.Refresh()
            $total += [int64]$process.WorkingSet64
        }
    }
    return $total
}

function Get-PostgresContainerId {
    param([string]$RunId)

    $container = docker ps -q `
        --filter "label=studiofs.e2e.run=$RunId" `
        --filter "label=studiofs.e2e.service=postgres" 2>$null |
        Select-Object -First 1
    if ($null -eq $container) {
        return $null
    }
    return $container.ToString().Trim()
}

function Get-PostgresMemorySample {
    param([string]$ContainerId)

    if ([string]::IsNullOrWhiteSpace($ContainerId)) {
        return $null
    }
    $line = docker stats --no-stream --format "{{.Name}}|{{.MemUsage}}" $ContainerId 2>$null |
        Select-Object -First 1
    if ($null -eq $line) {
        return $null
    }
    $parts = $line.ToString().Split('|', 2)
    if ($parts.Count -ne 2) {
        return $null
    }
    $usage = $parts[1].Split('/', 2)[0].Trim()
    $bytes = Convert-ToBytes $usage
    if ($null -eq $bytes) {
        return $null
    }
    return [ordered]@{
        container_id = $ContainerId
        memory_usage = $usage
        memory_bytes = [int64]$bytes
    }
}

function Quote-ProcessArgument {
    param([string]$Value)

    if ($Value -notmatch '[\s"]') {
        return $Value
    }
    return '"' + $Value.Replace('"', '\"') + '"'
}

function Get-ScenarioSummaryPath {
    param([string]$RunId)

    return Join-Path $e2eRoot "$RunId\artifacts\scenarios\$scenario\large-history-cold-bootstrap-summary.json"
}

function Get-ScenarioStatusPath {
    param([string]$RunId)

    return Join-Path $e2eRoot "$RunId\artifacts\scenarios\$scenario\large-history-status.ndjson"
}

function Get-UniquePages {
    param([object[]]$Entries)

    $pages = @{}
    foreach ($entry in $Entries) {
        foreach ($pageName in @("current_page", "last_page")) {
            $page = $entry.sample.$pageName
            if ($null -eq $page) {
                continue
            }
            $key = "{0}:{1}" -f $page.first_batch_version, $page.last_batch_version
            if (-not $pages.ContainsKey($key) -or
                ($pages[$key].total_duration_us -eq 0 -and $page.total_duration_us -gt 0)) {
                $pages[$key] = $page
            }
        }
    }
    return @($pages.Values)
}

function Get-CellMetrics {
    param(
        [string]$RunId,
        [int]$ExitCode,
        [int64]$MaxWorkingSetBytes,
        [int64]$MaxPostgresMemoryBytes,
        [string]$ScenarioReportPath
    )

    $summaryPath = Get-ScenarioSummaryPath -RunId $RunId
    $statusPath = Get-ScenarioStatusPath -RunId $RunId
    $summary = $null
    $pages = @()
    if (Test-Path -LiteralPath $summaryPath) {
        $summary = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
    }
    if (Test-Path -LiteralPath $statusPath) {
        $entries = @(Get-Content -LiteralPath $statusPath | Where-Object { $_.Trim() } |
            ForEach-Object { $_ | ConvertFrom-Json })
        $pages = @(Get-UniquePages -Entries $entries)
    }

    $queryValues = @($pages | ForEach-Object { $_.query_duration_us })
    $applyValues = @($pages | ForEach-Object { $_.apply_duration_us })
    $totalValues = @($pages | ForEach-Object { $_.total_duration_us })
    $checkpoint = if ($null -ne $summary -and $null -ne $summary.large_history) {
        $summary.large_history
    } else {
        $null
    }
    return [ordered]@{
        run_id = $RunId
        exit_code = $ExitCode
        scenario_report = $ScenarioReportPath
        scenario_summary = if (Test-Path -LiteralPath $summaryPath) { $summaryPath } else { $null }
        page_count = if ($null -ne $checkpoint) { $checkpoint.observed_page_count } else { $pages.Count }
        event_rows = if ($null -ne $checkpoint) { $checkpoint.observed_event_rows } else { $null }
        event_bytes = if ($null -ne $checkpoint) { $checkpoint.observed_event_bytes } else { $null }
        scenario_wall_duration_ms = if ($null -ne $checkpoint) { $checkpoint.wall_duration_ms } else { $null }
        page_response_bytes_max = Get-Maximum -Values @($pages | ForEach-Object { $_.response_bytes })
        page_batch_count_max = Get-Maximum -Values @($pages | ForEach-Object { $_.batch_count })
        page_event_count_max = Get-Maximum -Values @($pages | ForEach-Object { $_.event_count })
        query_p50_us = Get-Percentile -Values $queryValues -Percentile 0.50
        query_p95_us = Get-Percentile -Values $queryValues -Percentile 0.95
        apply_p50_us = Get-Percentile -Values $applyValues -Percentile 0.50
        apply_p95_us = Get-Percentile -Values $applyValues -Percentile 0.95
        total_p50_us = Get-Percentile -Values $totalValues -Percentile 0.50
        total_p95_us = Get-Percentile -Values $totalValues -Percentile 0.95
        checkpoint_payload_bytes = if ($null -ne $checkpoint) { $checkpoint.checkpoint_payload_bytes } else { $null }
        checkpoint_build_duration_us = if ($null -ne $checkpoint) { $checkpoint.checkpoint_build_duration_us } else { $null }
        checkpoint_publish_duration_us = if ($null -ne $checkpoint) { $checkpoint.checkpoint_publish_duration_us } else { $null }
        checkpoint_total_duration_us = if ($null -ne $checkpoint) { $checkpoint.checkpoint_total_duration_us } else { $null }
        client_process_tree_peak_working_set_bytes = $MaxWorkingSetBytes
        postgres_peak_memory_bytes = $MaxPostgresMemoryBytes
        telemetry_complete = ($pages.Count -gt 0 -and $queryValues.Count -eq $pages.Count -and
            $applyValues.Count -eq $pages.Count -and $totalValues.Count -eq $pages.Count)
        scenario_passed = ($ExitCode -eq 0 -and $null -ne $checkpoint -and $checkpoint.final_read_write_converged -eq $true)
    }
}

$StudiofsBin = Resolve-ExistingFile -PathValue $StudiofsBin -Label "StudioFS binary"
$E2eBin = Resolve-ExistingFile -PathValue $E2eBin -Label "studiofs-e2e binary"

$cases = @(
    [pscustomobject]@{ name = "page-batches-8"; page_batches = 8; page_bytes = $null; checkpoint_version_lag = 10000; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "page-batches-32"; page_batches = 32; page_bytes = $null; checkpoint_version_lag = 10000; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "page-batches-64"; page_batches = 64; page_bytes = $null; checkpoint_version_lag = 10000; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "page-bytes-1m"; page_batches = 64; page_bytes = 1048576; checkpoint_version_lag = 10000; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "page-bytes-2m"; page_batches = 64; page_bytes = 2097152; checkpoint_version_lag = 10000; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "checkpoint-lag-32"; page_batches = 64; page_bytes = $null; checkpoint_version_lag = 32; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "checkpoint-bytes-1m"; page_batches = 64; page_bytes = $null; checkpoint_version_lag = 10000; checkpoint_event_bytes = 1048576; checkpoint_age_secs = 3600 },
    [pscustomobject]@{ name = "checkpoint-age-1s"; page_batches = 64; page_bytes = $null; checkpoint_version_lag = 10000; checkpoint_event_bytes = 67108864; checkpoint_age_secs = 1 }
)
if (-not [string]::IsNullOrWhiteSpace($SelectedCase)) {
    $selectedCaseNames = @($SelectedCase -split ',' |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ })
    $availableCases = @($cases | ForEach-Object { $_.name })
    $unknownCases = @($selectedCaseNames | Where-Object { $availableCases -notcontains $_ })
    if ($unknownCases.Count -gt 0) {
        throw "Unknown SFS-718 case(s): $($unknownCases -join ', ')"
    }
    $requestedCases = [System.Collections.Generic.HashSet[string]]::new(
        [StringComparer]::OrdinalIgnoreCase
    )
    foreach ($caseName in $selectedCaseNames) {
        [void]$requestedCases.Add($caseName)
    }
    $cases = @($cases.Where({ $requestedCases.Contains($_.name) }))
}

$matrix = [ordered]@{
    schema_version = 1
    scenario = $scenario
    replay_batches = $replayBatches
    runs = $Runs
    constrain_postgres = [bool]$ConstrainPostgres
    studiofs_binary = $StudiofsBin
    e2e_binary = $E2eBin
    git_commit = (& git -C (Get-Location).Path rev-parse HEAD).Trim()
    cases = $cases
}
$matrixPath = Join-Path $runRoot "matrix.json"
$matrix | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $matrixPath -Encoding utf8

if ($PreviewOnly) {
    $matrix | ConvertTo-Json -Depth 8
    Write-Host "Preview written to $matrixPath"
    exit 0
}

$environmentKeys = @(
    "STUDIOFS_E2E_LARGE_HISTORY_PAGE_BATCHES",
    "STUDIOFS_E2E_LARGE_HISTORY_PAGE_BYTES",
    "STUDIOFS_E2E_LARGE_HISTORY_REPLAY_BATCHES",
    "STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG",
    "STUDIOFS_PG_CHECKPOINT_MAX_EVENT_BYTES",
    "STUDIOFS_PG_CHECKPOINT_MAX_AGE_SECS"
)
$originalEnvironment = @{}
foreach ($key in $environmentKeys) {
    $originalEnvironment[$key] = [Environment]::GetEnvironmentVariable($key, "Process")
}

$results = [System.Collections.Generic.List[object]]::new()
try {
    foreach ($matrixCase in $cases) {
        for ($iteration = 1; $iteration -le $Runs; $iteration++) {
            $runId = "sfs718-{0}-{1:D2}-{2:D2}" -f $matrixCase.name, $iteration, (Get-Random -Minimum 1000 -Maximum 9999)
            $cellDir = Join-Path $runRoot ("{0}\run-{1:D2}" -f $matrixCase.name, $iteration)
            $null = New-Item -ItemType Directory -Path $cellDir -Force
            $reportPath = Join-Path $cellDir "report.json"
            $stdoutPath = Join-Path $cellDir "stdout.txt"
            $stderrPath = Join-Path $cellDir "stderr.txt"
            $samplesPath = Join-Path $cellDir "memory-samples.ndjson"
            $commandPath = Join-Path $cellDir "command.txt"

            foreach ($key in $environmentKeys) {
                Remove-Item -LiteralPath "Env:$key" -ErrorAction SilentlyContinue
            }
            $env:STUDIOFS_E2E_LARGE_HISTORY_PAGE_BATCHES = [string]$matrixCase.page_batches
            $env:STUDIOFS_E2E_LARGE_HISTORY_REPLAY_BATCHES = [string]$replayBatches
            $env:STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG = [string]$matrixCase.checkpoint_version_lag
            $env:STUDIOFS_PG_CHECKPOINT_MAX_EVENT_BYTES = [string]$matrixCase.checkpoint_event_bytes
            $env:STUDIOFS_PG_CHECKPOINT_MAX_AGE_SECS = [string]$matrixCase.checkpoint_age_secs
            if ($null -ne $matrixCase.page_bytes) {
                $env:STUDIOFS_E2E_LARGE_HISTORY_PAGE_BYTES = [string]$matrixCase.page_bytes
            }

            $arguments = @(
                "run", $scenario,
                "--root", $e2eRoot,
                "--run-id", $runId,
                "--studiofs-bin", $StudiofsBin,
                "--infra", "compose",
                "--retain", "always",
                "--report", $reportPath,
                "--seed", "718"
            )
            $argumentLine = ($arguments | ForEach-Object { Quote-ProcessArgument $_ }) -join " "
            @(
                "& `"$E2eBin`" $argumentLine",
                "STUDIOFS_E2E_LARGE_HISTORY_PAGE_BATCHES=$($matrixCase.page_batches)",
                "STUDIOFS_E2E_LARGE_HISTORY_PAGE_BYTES=$($matrixCase.page_bytes)",
                "STUDIOFS_E2E_LARGE_HISTORY_REPLAY_BATCHES=$replayBatches",
                "STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG=$($matrixCase.checkpoint_version_lag)",
                "STUDIOFS_PG_CHECKPOINT_MAX_EVENT_BYTES=$($matrixCase.checkpoint_event_bytes)",
                "STUDIOFS_PG_CHECKPOINT_MAX_AGE_SECS=$($matrixCase.checkpoint_age_secs)",
                "constrain_postgres=$ConstrainPostgres"
            ) | Set-Content -LiteralPath $commandPath -Encoding utf8

            Write-Host ("[{0}/{1}] {2} run {3}" -f $matrixCase.name, $Runs, $matrixCase.name, $iteration)
            $process = Start-Process -FilePath $E2eBin -ArgumentList $argumentLine -NoNewWindow `
                -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
            $maxWorkingSetBytes = [int64]0
            $maxPostgresMemoryBytes = [int64]0
            $postgresContainerId = $null
            $postgresConstrained = $false
            $sampleStream = [System.IO.StreamWriter]::new($samplesPath, $false)
            try {
                while (-not $process.HasExited) {
                    $process.Refresh()
                    $workingSetBytes = Get-ProcessTreeWorkingSetBytes -RootPid $process.Id
                    $maxWorkingSetBytes = [math]::Max($maxWorkingSetBytes, $workingSetBytes)
                    if ([string]::IsNullOrWhiteSpace($postgresContainerId)) {
                        $postgresContainerId = Get-PostgresContainerId -RunId $runId
                    }
                    if ($ConstrainPostgres -and -not $postgresConstrained -and
                        -not [string]::IsNullOrWhiteSpace($postgresContainerId)) {
                        docker update --cpus 1 --memory 6g --memory-swap 6g $postgresContainerId | Out-Null
                        $postgresConstrained = $true
                    }
                    $postgresSample = Get-PostgresMemorySample -ContainerId $postgresContainerId
                    if ($null -ne $postgresSample) {
                        $maxPostgresMemoryBytes = [math]::Max($maxPostgresMemoryBytes, [int64]$postgresSample.memory_bytes)
                    }
                    $sample = [ordered]@{
                        timestamp_unix_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
                        runner_working_set_bytes = $workingSetBytes
                        postgres = $postgresSample
                    }
                    $sampleStream.WriteLine(($sample | ConvertTo-Json -Compress -Depth 5))
                    $sampleStream.Flush()
                    Start-Sleep -Milliseconds 250
                }
                $process.Refresh()
                $maxWorkingSetBytes = [math]::Max($maxWorkingSetBytes, (Get-ProcessTreeWorkingSetBytes -RootPid $process.Id))
                $process.WaitForExit()
            } finally {
                $sampleStream.Dispose()
            }
            $exitCode = $process.ExitCode
            $metrics = Get-CellMetrics `
                -RunId $runId `
                -ExitCode $exitCode `
                -MaxWorkingSetBytes $maxWorkingSetBytes `
                -MaxPostgresMemoryBytes $maxPostgresMemoryBytes `
                -ScenarioReportPath $reportPath
            $metrics["case"] = $matrixCase.name
            $metrics["iteration"] = $iteration
            $metrics["page_batches"] = $matrixCase.page_batches
            $metrics["page_bytes"] = $matrixCase.page_bytes
            $metrics["checkpoint_version_lag"] = $matrixCase.checkpoint_version_lag
            $metrics["checkpoint_event_bytes"] = $matrixCase.checkpoint_event_bytes
            $metrics["checkpoint_age_secs"] = $matrixCase.checkpoint_age_secs
            $metrics["postgres_container_id"] = $postgresContainerId
            $metrics["postgres_constrained"] = $postgresConstrained
            $metricsPath = Join-Path $cellDir "metrics.json"
            $metrics | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $metricsPath -Encoding utf8
            $results.Add($metrics)
            if (-not $metrics.scenario_passed) {
                Write-Warning "SFS-718 cell failed; retained artifacts: $cellDir"
            }
        }
    }
} finally {
    foreach ($key in $environmentKeys) {
        $value = $originalEnvironment[$key]
        if ($null -eq $value) {
            Remove-Item -LiteralPath "Env:$key" -ErrorAction SilentlyContinue
        } else {
            Set-Item -Path "Env:$key" -Value $value
        }
    }
}

$resultsPath = Join-Path $runRoot "results.json"
$results | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $resultsPath -Encoding utf8
$passed = @($results | Where-Object { $_["scenario_passed"] -eq $true -and $_["telemetry_complete"] -eq $true })
$summary = [ordered]@{
    schema_version = 1
    run_root = $runRoot
    matrix = $matrixPath
    results = $resultsPath
    total_cells = $results.Count
    passed_cells = $passed.Count
    failed_cells = $results.Count - $passed.Count
    page_query_p95_us = Get-Percentile -Values @($passed | ForEach-Object { $_.query_p95_us }) -Percentile 0.95
    page_apply_p95_us = Get-Percentile -Values @($passed | ForEach-Object { $_.apply_p95_us }) -Percentile 0.95
    page_total_p95_us = Get-Percentile -Values @($passed | ForEach-Object { $_.total_p95_us }) -Percentile 0.95
    client_process_tree_peak_working_set_bytes = Get-Maximum -Values @($passed | ForEach-Object { $_["client_process_tree_peak_working_set_bytes"] })
    postgres_peak_memory_bytes = Get-Maximum -Values @($passed | ForEach-Object { $_["postgres_peak_memory_bytes"] })
}
$summaryPath = Join-Path $runRoot "summary.json"
$summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $summaryPath -Encoding utf8
$summary | ConvertTo-Json -Depth 10
if ($summary.failed_cells -gt 0) {
    throw "SFS-718 benchmark matrix failed; inspect $runRoot"
}
