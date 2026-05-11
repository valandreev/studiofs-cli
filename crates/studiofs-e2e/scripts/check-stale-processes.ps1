$ErrorActionPreference = 'Stop'

$names = @('studiofs.exe', 'studiofs-e2e.exe')
$selfPidValue = $env:STUDIOFS_E2E_SELF_PID
if ([string]::IsNullOrWhiteSpace($selfPidValue)) {
    $selfPidValue = $env:STUDIOFS_E2E_BASELINE_SELF_PID
}
if ([string]::IsNullOrWhiteSpace($selfPidValue)) {
    $selfPidValue = '0'
}
$selfPid = [int]$selfPidValue
$runnerRoot = $env:STUDIOFS_E2E_RUNNER_ROOT
if ([string]::IsNullOrWhiteSpace($runnerRoot)) {
    $runnerRoot = $env:STUDIOFS_E2E_BASELINE_ROOT
}
$cleanStale = $env:STUDIOFS_E2E_CLEAN_STALE_PROCESSES -eq '1'

if ([string]::IsNullOrWhiteSpace($runnerRoot)) {
    throw 'STUDIOFS_E2E_RUNNER_ROOT is required'
}

$rootPattern = [regex]::Escape($runnerRoot)

Get-CimInstance Win32_Process |
    Where-Object {
        $cmd = $_.CommandLine -as [string]
        $names -contains $_.Name -and
            $_.ProcessId -ne $selfPid -and
            ($cmd -match $rootPattern -or $cmd -match '__mounted-chaos-op')
    } |
    ForEach-Object {
        if ($cleanStale) {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
            "stopped $($_.ProcessId) $($_.Name) $($_.CommandLine)"
        } else {
            "$($_.ProcessId) $($_.Name) $($_.CommandLine)"
        }
    }
