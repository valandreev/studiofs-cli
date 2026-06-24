param(
    [Parameter(Mandatory = $true)]
    [string]$Target,

    [string]$StudioFsPath,
    [string]$OutputRoot,
    [string]$ScenarioName = "write-jitter",
    [string]$BlockSize = "16MB",
    [string]$ReadSize = "16MB",
    [string]$ItemSize = "16MB",
    [int]$OperationCount = 128,
    [int]$Concurrency = 1,
    [int]$IoDepth = 1,
    [int]$WarmupRuns = 0,
    [int]$WindowOperationCount = 8,
    [ValidateSet("sequential", "random", "mixed")]
    [string]$AccessPattern = "sequential",
    [string]$L1Limit = "2GB",
    [bool]$SequentialStreams = $true,
    [switch]$DisableProfile,
    [switch]$PreviewOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

$scenario = @{
    Name                 = $ScenarioName
    FocusPhase           = "writes"
    Target               = $Target
    StudioFsPath         = $StudioFsPath
    OutputRoot           = $OutputRoot
    BlockSize            = $BlockSize
    ReadSize             = $ReadSize
    Sizes                = $ItemSize
    WorkloadRepeat       = [Math]::Max($OperationCount, 1)
    Concurrency          = [Math]::Max($Concurrency, 1)
    IoDepth              = [Math]::Max($IoDepth, 1)
    WarmupRuns           = [Math]::Max($WarmupRuns, 0)
    AccessPattern        = $AccessPattern
    L1Limit              = $L1Limit
    SequentialStreams    = $SequentialStreams
    Profile              = -not $DisableProfile
    WindowOperationCount = [Math]::Max($WindowOperationCount, 1)
    PreviewOnly          = [bool]$PreviewOnly
}

$result = Invoke-DevbenchPerfScenario -Scenario $scenario

if ($PreviewOnly) {
    Write-Host ""
    Write-Host "Preview only."
    Write-Host "Run directory   : $($result.run_directory)"
    Write-Host "Command file    : $($result.command_path)"
    Write-Host "Command         :"
    Write-Host "  $($result.command_line)"
    Write-Host ""
    exit 0
}

$aggregate = $result.summary.benchmark.phase.aggregate
$smoothness = $result.summary.benchmark.phase.smoothness

Write-Host ""
Write-Host "Write jitter benchmark complete."
Write-Host "Run directory   : $($result.run_directory)"
Write-Host "Benchmark JSON  : $($result.benchmark_json)"
Write-Host "Summary JSON    : $($result.summary_json)"
Write-Host "Summary Markdown: $($result.summary_markdown)"
Write-Host ""
Write-Host "Key metrics:"
Write-Host "  Throughput           : $($aggregate.throughput_mib_per_sec) MiB/s"
Write-Host "  P95 latency          : $($aggregate.p95_ms) ms"
Write-Host "  Throughput CV        : $($smoothness.throughput_cv)"
Write-Host "  Windows < 80% median : $($smoothness.windows_below_80pct_of_median)"
Write-Host "  Stall count          : $($smoothness.stall_count)"
Write-Host ""
