Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-StudioFsPath {
    param(
        [string]$ExplicitPath
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitPath)) {
        return (Resolve-Path -LiteralPath $ExplicitPath).Path
    }

    $repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
    $debugPath = Join-Path $repoRoot "target\debug\studiofs.exe"
    $releasePath = Join-Path $repoRoot "target\release\studiofs.exe"

    if (Test-Path -LiteralPath $debugPath) {
        return $debugPath
    }
    if (Test-Path -LiteralPath $releasePath) {
        return $releasePath
    }

    throw "Could not find studiofs.exe under target\debug or target\release."
}

function New-PerfRunDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputRoot,
        [Parameter(Mandatory = $true)]
        [string]$ScenarioName
    )

    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $safeName = ($ScenarioName -replace '[^A-Za-z0-9._-]', '-').Trim('-')
    if ([string]::IsNullOrWhiteSpace($safeName)) {
        $safeName = "perf-run"
    }

    $runDir = Join-Path $OutputRoot "$safeName-$timestamp"
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    return (Resolve-Path -LiteralPath $runDir).Path
}

function Get-DoubleValues {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Values
    )

    return @($Values | ForEach-Object { [double]$_ })
}

function Get-Mean {
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$Values
    )

    if ($Values.Count -eq 0) {
        return 0.0
    }

    $sum = 0.0
    foreach ($value in $Values) {
        $sum += $value
    }
    return $sum / $Values.Count
}

function Get-Percentile {
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$Values,
        [Parameter(Mandatory = $true)]
        [double]$Percentile
    )

    if ($Values.Count -eq 0) {
        return 0.0
    }

    $sorted = @($Values | Sort-Object)
    if ($sorted.Count -eq 1) {
        return $sorted[0]
    }

    $position = (($Percentile / 100.0) * ($sorted.Count - 1))
    $lowerIndex = [int][Math]::Floor($position)
    $upperIndex = [int][Math]::Ceiling($position)
    if ($lowerIndex -eq $upperIndex) {
        return $sorted[$lowerIndex]
    }

    $fraction = $position - $lowerIndex
    return $sorted[$lowerIndex] + (($sorted[$upperIndex] - $sorted[$lowerIndex]) * $fraction)
}

function Get-StandardDeviation {
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$Values
    )

    if ($Values.Count -le 1) {
        return 0.0
    }

    $mean = Get-Mean -Values $Values
    $sumSquares = 0.0
    foreach ($value in $Values) {
        $delta = $value - $mean
        $sumSquares += ($delta * $delta)
    }

    return [Math]::Sqrt($sumSquares / $Values.Count)
}

function Convert-BytesPerSecondToMiB {
    param(
        [double]$BytesPerSecond
    )

    return $BytesPerSecond / 1MB
}

function Get-WindowSummaries {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Operations,
        [Parameter(Mandatory = $true)]
        [int]$WindowOperationCount
    )

    $windowSize = [Math]::Max($WindowOperationCount, 1)
    $windows = @()
    $windowIndex = 0

    for ($offset = 0; $offset -lt $Operations.Count; $offset += $windowSize) {
        $endExclusive = [Math]::Min($offset + $windowSize, $Operations.Count)
        $chunk = @()
        for ($index = $offset; $index -lt $endExclusive; $index++) {
            $chunk += $Operations[$index]
        }

        $bytes = 0.0
        $elapsedMs = 0.0
        $maxLatencyMs = 0.0
        foreach ($operation in $chunk) {
            $bytes += [double]$operation.bytes
            $elapsedMs += [double]$operation.elapsed_ms
            $maxLatencyMs = [Math]::Max($maxLatencyMs, [double]$operation.elapsed_ms)
        }

        $throughputBytesPerSec = 0.0
        if ($elapsedMs -gt 0.0) {
            $throughputBytesPerSec = ($bytes * 1000.0) / $elapsedMs
        }

        $windows += [pscustomobject]@{
            window_index              = $windowIndex
            first_operation_index     = [int64]$chunk[0].operation_index
            last_operation_index      = [int64]$chunk[$chunk.Count - 1].operation_index
            operation_count           = $chunk.Count
            bytes                     = [int64]$bytes
            elapsed_ms                = [Math]::Round($elapsedMs, 3)
            throughput_mib_per_sec    = [Math]::Round((Convert-BytesPerSecondToMiB $throughputBytesPerSec), 3)
            max_operation_latency_ms  = [Math]::Round($maxLatencyMs, 3)
        }

        $windowIndex += 1
    }

    return @($windows)
}

function Get-SmoothnessSummary {
    param(
        [Parameter(Mandatory = $true)]
        [object]$PhaseReport,
        [Parameter(Mandatory = $true)]
        [int]$WindowOperationCount
    )

    $operations = @($PhaseReport.operations_log)
    if ($operations.Count -eq 0) {
        return [pscustomobject]@{
            operation_log_present              = $false
            window_operation_count             = [Math]::Max($WindowOperationCount, 1)
            operation_count                    = 0
            window_count                       = 0
            throughput_cv                      = 0.0
            windows_below_80pct_of_median      = 0
            windows_below_60pct_of_median      = 0
            slowest_window_percent_of_median   = 0.0
            stall_count                        = 0
            worst_operations                   = @()
            windows                            = @()
        }
    }

    $latencies = Get-DoubleValues -Values @($operations | ForEach-Object { $_.elapsed_ms })
    $windowSummaries = @(Get-WindowSummaries -Operations $operations -WindowOperationCount $WindowOperationCount)
    $windowThroughputs = Get-DoubleValues -Values @($windowSummaries | ForEach-Object { $_.throughput_mib_per_sec })
    $windowMedian = Get-Percentile -Values $windowThroughputs -Percentile 50
    $windowStdDev = Get-StandardDeviation -Values $windowThroughputs
    $windowMean = Get-Mean -Values $windowThroughputs
    $throughputCv = if ($windowMean -gt 0.0) { $windowStdDev / $windowMean } else { 0.0 }

    $latencyP50 = Get-Percentile -Values $latencies -Percentile 50
    $stallThresholdMs = [Math]::Max(($latencyP50 * 3.0), [double]$PhaseReport.p95_ms)
    $worstOperations = @(
        $operations |
            Sort-Object -Property @{ Expression = { [double]$_.elapsed_ms }; Descending = $true } |
            Select-Object -First 10 |
            ForEach-Object {
                [pscustomobject]@{
                    operation_index           = [int64]$_.operation_index
                    worker_id                 = [int]$_.worker_id
                    target                    = [string]$_.target
                    bytes                     = [int64]$_.bytes
                    elapsed_ms                = [Math]::Round([double]$_.elapsed_ms, 3)
                    throughput_mib_per_sec    = [Math]::Round((Convert-BytesPerSecondToMiB ([double]$_.throughput_bytes_per_sec)), 3)
                }
            }
    )

    $below80 = 0
    $below60 = 0
    foreach ($window in $windowSummaries) {
        if ($windowMedian -gt 0.0) {
            if ($window.throughput_mib_per_sec -lt ($windowMedian * 0.8)) {
                $below80 += 1
            }
            if ($window.throughput_mib_per_sec -lt ($windowMedian * 0.6)) {
                $below60 += 1
            }
        }
    }

    $slowestPercentOfMedian = 0.0
    if ($windowSummaries.Count -gt 0 -and $windowMedian -gt 0.0) {
        $slowestWindow = $windowSummaries | Sort-Object throughput_mib_per_sec | Select-Object -First 1
        $slowestPercentOfMedian = ($slowestWindow.throughput_mib_per_sec / $windowMedian) * 100.0
    }

    $stallCount = @($operations | Where-Object { [double]$_.elapsed_ms -ge $stallThresholdMs }).Count

    return [pscustomobject]@{
        operation_log_present            = $true
        window_operation_count           = [Math]::Max($WindowOperationCount, 1)
        operation_count                  = $operations.Count
        window_count                     = $windowSummaries.Count
        throughput_cv                    = [Math]::Round($throughputCv, 4)
        window_throughput_mib_per_sec    = [pscustomobject]@{
            min  = [Math]::Round((Get-Percentile -Values $windowThroughputs -Percentile 0), 3)
            p50  = [Math]::Round((Get-Percentile -Values $windowThroughputs -Percentile 50), 3)
            p95  = [Math]::Round((Get-Percentile -Values $windowThroughputs -Percentile 95), 3)
            max  = [Math]::Round((Get-Percentile -Values $windowThroughputs -Percentile 100), 3)
            avg  = [Math]::Round($windowMean, 3)
        }
        windows_below_80pct_of_median    = $below80
        windows_below_60pct_of_median    = $below60
        slowest_window_percent_of_median = [Math]::Round($slowestPercentOfMedian, 2)
        stall_threshold_ms               = [Math]::Round($stallThresholdMs, 3)
        stall_count                      = $stallCount
        worst_operations                 = $worstOperations
        windows                          = $windowSummaries
    }
}

function New-PhaseAggregate {
    param(
        [Parameter(Mandatory = $true)]
        [object]$PhaseReport
    )

    return [pscustomobject]@{
        operations             = [int64]$PhaseReport.operations
        bytes                  = [int64]$PhaseReport.bytes
        elapsed_ms             = [Math]::Round([double]$PhaseReport.elapsed_ms, 3)
        avg_latency_ms         = [Math]::Round([double]$PhaseReport.avg_latency_ms, 3)
        p50_ms                 = [Math]::Round([double]$PhaseReport.p50_ms, 3)
        p95_ms                 = [Math]::Round([double]$PhaseReport.p95_ms, 3)
        p99_ms                 = [Math]::Round([double]$PhaseReport.p99_ms, 3)
        throughput_mib_per_sec = [Math]::Round((Convert-BytesPerSecondToMiB ([double]$PhaseReport.throughput_bytes_per_sec)), 3)
    }
}

function New-SignalSummary {
    param(
        [Parameter(Mandatory = $true)]
        [object]$BenchmarkReport
    )

    $signals = [ordered]@{}

    $cachePipeline = $BenchmarkReport.PSObject.Properties["cache_pipeline"]
    if ($null -ne $cachePipeline -and $null -ne $cachePipeline.Value) {
        $signals.cache_pipeline = [pscustomobject]@{
            upload_queue_pending_depth_peak = $cachePipeline.Value.upload_queue.pending_depth_peak
            blocking_queue_wait_max_ms      = [Math]::Round([double]$cachePipeline.Value.blocking_tasks.total.queue_wait_max_ms, 3)
            blocking_execution_max_ms       = [Math]::Round([double]$cachePipeline.Value.blocking_tasks.total.execution_max_ms, 3)
        }
    }

    $mountedMemory = $BenchmarkReport.PSObject.Properties["mounted_memory"]
    if ($null -ne $mountedMemory -and $null -ne $mountedMemory.Value) {
        $signals.mounted_memory = [pscustomobject]@{
            rss_bytes_peak          = $mountedMemory.Value.rss_bytes_peak
            dirty_bytes_peak        = $mountedMemory.Value.dirty_bytes_peak
            dirty_limit_bytes       = $mountedMemory.Value.dirty_limit_bytes
        }
    }

    $mountedWritePathWindow = $BenchmarkReport.PSObject.Properties["mounted_write_path_window"]
    if ($null -ne $mountedWritePathWindow -and $null -ne $mountedWritePathWindow.Value) {
        $beforeSnapshot = $mountedWritePathWindow.Value.before
        $afterSnapshot = $mountedWritePathWindow.Value.after
        $signals.mounted_write_path_window = [pscustomobject]@{
            before_total_dirty_bytes = if ($null -ne $beforeSnapshot) { $beforeSnapshot.dirty_pressure.total_dirty_bytes } else { $null }
            after_total_dirty_bytes  = if ($null -ne $afterSnapshot) { $afterSnapshot.dirty_pressure.total_dirty_bytes } else { $null }
            before_dirty_limit_bytes = if ($null -ne $beforeSnapshot) { $beforeSnapshot.dirty_pressure.dirty_limit_bytes } else { $null }
            after_dirty_limit_bytes  = if ($null -ne $afterSnapshot) { $afterSnapshot.dirty_pressure.dirty_limit_bytes } else { $null }
        }
    }

    $localFirstRead = $BenchmarkReport.PSObject.Properties["local_first_read_comparison"]
    if ($null -ne $localFirstRead -and $null -ne $localFirstRead.Value) {
        $signals.local_first_read = @(
            $localFirstRead.Value.phases |
                ForEach-Object {
                    [pscustomobject]@{
                        phase                   = $_.phase
                        target_state_observed   = [bool]$_.target_state_observed
                        valid_for_target        = [bool]$_.valid_for_target
                        wait_ms                 = [Math]::Round([double]$_.wait_ms, 3)
                        throughput_mib_per_sec  = [Math]::Round((Convert-BytesPerSecondToMiB ([double]$_.results.throughput_bytes_per_sec)), 3)
                        p95_ms                  = [Math]::Round([double]$_.results.p95_ms, 3)
                    }
                }
        )
    }

    $profile = $BenchmarkReport.PSObject.Properties["profile"]
    if ($null -ne $profile -and $null -ne $profile.Value) {
        $signals.profile_artifacts = @(
            $profile.Value.artifacts |
                ForEach-Object {
                    [pscustomobject]@{
                        kind              = $_.kind
                        path              = $_.path
                        bytes             = $_.bytes
                        sent_to_endpoint  = [bool]$_.sent_to_endpoint
                    }
                }
        )
    }

    return [pscustomobject]$signals
}

function Build-DevbenchPerfSummary {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Scenario,
        [Parameter(Mandatory = $true)]
        [object]$BenchmarkReport,
        [Parameter(Mandatory = $true)]
        [string]$RunDirectory,
        [Parameter(Mandatory = $true)]
        [string]$BenchmarkJsonPath,
        [Parameter(Mandatory = $true)]
        [string]$StdoutPath
    )

    $focusPhase = [string]$Scenario.FocusPhase
    $phaseReport = $BenchmarkReport.$focusPhase
    if ($null -eq $phaseReport) {
        throw "Benchmark report does not contain phase '$focusPhase'."
    }

    return [pscustomobject][ordered]@{
        scenario = [pscustomobject][ordered]@{
            name                   = [string]$Scenario.Name
            focus_phase            = $focusPhase
            target                 = [string]$Scenario.Target
            studiofs_path          = [string]$Scenario.StudioFsPath
            run_directory          = $RunDirectory
            benchmark_json_path    = $BenchmarkJsonPath
            stdout_path            = $StdoutPath
            preview_only           = [bool]$Scenario.PreviewOnly
        }
        benchmark = [pscustomobject][ordered]@{
            mode                   = [string]$BenchmarkReport.mode
            started_at             = [string]$BenchmarkReport.started_at
            completed_at           = [string]$BenchmarkReport.completed_at
            total_elapsed_ms       = [Math]::Round([double]$BenchmarkReport.total_elapsed_ms, 3)
            block_size_bytes       = [int64]$BenchmarkReport.block_size
            read_size_bytes        = [int64]$BenchmarkReport.read_size
            workload_repeat        = [int]$BenchmarkReport.workload_repeat
            sequential_streams     = [bool]$BenchmarkReport.sequential_streams
            concurrency            = [int]$BenchmarkReport.concurrency
            io_depth               = [int]$BenchmarkReport.io_depth
            warmup_runs            = [int]$BenchmarkReport.warmup_runs
            sizes                  = [string]$BenchmarkReport.sizes
            phase                  = [pscustomobject]@{
                aggregate          = New-PhaseAggregate -PhaseReport $phaseReport
                smoothness         = Get-SmoothnessSummary -PhaseReport $phaseReport -WindowOperationCount ([int]$Scenario.WindowOperationCount)
            }
        }
        signals = New-SignalSummary -BenchmarkReport $BenchmarkReport
    }
}

function Format-DevbenchPerfMarkdown {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Summary
    )

    $aggregate = $Summary.benchmark.phase.aggregate
    $smoothness = $Summary.benchmark.phase.smoothness
    $lines = New-Object System.Collections.Generic.List[string]
    $tick = [char]96

    $lines.Add("# $($Summary.scenario.name)")
    $lines.Add("")
    $lines.Add("- Focus phase: " + $tick + [string]$Summary.scenario.focus_phase + $tick)
    $lines.Add("- Target: " + $tick + [string]$Summary.scenario.target + $tick)
    $lines.Add("- Mode: " + $tick + [string]$Summary.benchmark.mode + $tick)
    $lines.Add("- Run directory: " + $tick + [string]$Summary.scenario.run_directory + $tick)
    $lines.Add("")
    $lines.Add("## Aggregate")
    $lines.Add("")
    $lines.Add("- Operations: $($aggregate.operations)")
    $lines.Add("- Bytes: $($aggregate.bytes)")
    $lines.Add("- Throughput: $($aggregate.throughput_mib_per_sec) MiB/s")
    $lines.Add("- Avg latency: $($aggregate.avg_latency_ms) ms")
    $lines.Add("- P95 latency: $($aggregate.p95_ms) ms")
    $lines.Add("- P99 latency: $($aggregate.p99_ms) ms")
    $lines.Add("")
    $lines.Add("## Smoothness")
    $lines.Add("")

    if (-not $smoothness.operation_log_present) {
        $lines.Add("- Operation log not present in benchmark JSON.")
    }
    else {
        $windowThroughput = $smoothness.window_throughput_mib_per_sec
        $lines.Add("- Windows: $($smoothness.window_count) (window size $($smoothness.window_operation_count) ops)")
        $lines.Add("- Window throughput avg/p50/p95: $($windowThroughput.avg) / $($windowThroughput.p50) / $($windowThroughput.p95) MiB/s")
        $lines.Add("- Window throughput min/max: $($windowThroughput.min) / $($windowThroughput.max) MiB/s")
        $lines.Add("- Throughput CV: $($smoothness.throughput_cv)")
        $lines.Add("- Slowest window vs median: $($smoothness.slowest_window_percent_of_median)%")
        $lines.Add("- Windows below 80% of median: $($smoothness.windows_below_80pct_of_median)")
        $lines.Add("- Windows below 60% of median: $($smoothness.windows_below_60pct_of_median)")
        $lines.Add("- Stall threshold: $($smoothness.stall_threshold_ms) ms")
        $lines.Add("- Stall count: $($smoothness.stall_count)")
    }

    $profileArtifacts = $Summary.signals.PSObject.Properties["profile_artifacts"]
    if ($null -ne $profileArtifacts -and $null -ne $profileArtifacts.Value -and $profileArtifacts.Value.Count -gt 0) {
        $lines.Add("")
        $lines.Add("## Profile Artifacts")
        $lines.Add("")
        foreach ($artifact in $profileArtifacts.Value) {
            $lines.Add("- $($artifact.kind): " + $tick + [string]$artifact.path + $tick + " ($($artifact.bytes) bytes)")
        }
    }

    $mountedWritePathWindow = $Summary.signals.PSObject.Properties["mounted_write_path_window"]
    if ($null -ne $mountedWritePathWindow -and $null -ne $mountedWritePathWindow.Value) {
        $window = $mountedWritePathWindow.Value
        $lines.Add("")
        $lines.Add("## Mounted Write Path Window")
        $lines.Add("")
        $lines.Add("- Dirty bytes before/after: $($window.before_total_dirty_bytes) / $($window.after_total_dirty_bytes)")
        $lines.Add("- Dirty limit before/after: $($window.before_dirty_limit_bytes) / $($window.after_dirty_limit_bytes)")
    }

    return ($lines -join [Environment]::NewLine)
}

function Invoke-DevbenchPerfScenario {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Scenario
    )

    $repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
    $studiofsPath = Resolve-StudioFsPath -ExplicitPath $Scenario.StudioFsPath
    $outputRoot = $Scenario.OutputRoot
    if ([string]::IsNullOrWhiteSpace($outputRoot)) {
        $outputRoot = Join-Path $repoRoot "target\perf"
    }

    New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
    $runDirectory = New-PerfRunDirectory -OutputRoot $outputRoot -ScenarioName $Scenario.Name
    $benchmarkJsonPath = Join-Path $runDirectory "benchmark.json"
    $summaryJsonPath = Join-Path $runDirectory "summary.json"
    $summaryMarkdownPath = Join-Path $runDirectory "summary.md"
    $stdoutPath = Join-Path $runDirectory "stdout.txt"
    $commandPath = Join-Path $runDirectory "command.txt"

    $operationCount = [Math]::Max([int]$Scenario.WorkloadRepeat, 1)
    $smallFileCount = [Math]::Max($operationCount - 1, 1)
    $args = @(
        "devbench",
        [string]$Scenario.Target,
        "--block-size", [string]$Scenario.BlockSize,
        "--big-file-size", [string]$Scenario.Sizes,
        "--small-file-size", [string]$Scenario.Sizes,
        "--small-file-count", $smallFileCount.ToString(),
        "--threads", ([int]$Scenario.Concurrency).ToString(),
        "--json-output", $benchmarkJsonPath
    )

    $displayArgs = @()
    foreach ($arg in $args) {
        if ($arg -match '\s') {
            $displayArgs += '"' + $arg + '"'
        }
        else {
            $displayArgs += $arg
        }
    }
    $commandLine = '"' + $studiofsPath + '" ' + ($displayArgs -join ' ')
    Set-Content -LiteralPath $commandPath -Value $commandLine -Encoding utf8

    if ([bool]$Scenario.PreviewOnly) {
        return [pscustomobject]@{
            scenario_name       = $Scenario.Name
            studiofs_path       = $studiofsPath
            run_directory       = $runDirectory
            benchmark_json_path = $benchmarkJsonPath
            summary_json_path   = $summaryJsonPath
            summary_md_path     = $summaryMarkdownPath
            command_path        = $commandPath
            command_line        = $commandLine
            preview_only        = $true
        }
    }

    $output = & $studiofsPath @args 2>&1
    $exitCode = $LASTEXITCODE
    $stdoutText = ($output | Out-String)
    Set-Content -LiteralPath $stdoutPath -Value $stdoutText -Encoding utf8
    if ($exitCode -ne 0) {
        throw "studiofs devbench failed with exit code ${exitCode}. See $stdoutPath"
    }

    if (-not (Test-Path -LiteralPath $benchmarkJsonPath)) {
        throw "Expected benchmark JSON was not written: $benchmarkJsonPath"
    }

    $benchmarkReport = Get-Content -LiteralPath $benchmarkJsonPath -Raw | ConvertFrom-Json
    $Scenario.StudioFsPath = $studiofsPath
    $summary = Build-DevbenchPerfSummary `
        -Scenario $Scenario `
        -BenchmarkReport $benchmarkReport `
        -RunDirectory $runDirectory `
        -BenchmarkJsonPath $benchmarkJsonPath `
        -StdoutPath $stdoutPath

    $summary | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $summaryJsonPath -Encoding utf8
    $markdown = Format-DevbenchPerfMarkdown -Summary $summary
    Set-Content -LiteralPath $summaryMarkdownPath -Value $markdown -Encoding utf8

    return [pscustomobject]@{
        summary            = $summary
        run_directory      = $runDirectory
        benchmark_json     = $benchmarkJsonPath
        summary_json       = $summaryJsonPath
        summary_markdown   = $summaryMarkdownPath
        stdout_path        = $stdoutPath
        command_path       = $commandPath
    }
}
